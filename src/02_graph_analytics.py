# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Analisando com Grafos
# MAGIC **Workshop: Utilizando grafos para PLD** · Parte 2 de 3
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | ⏱️ Duração | ~60 min |
# MAGIC | 🎯 Objetivo | Encontrar padrões de lavagem na **estrutura** do grafo, unificar identidades e medir a centralidade de cada loja |
# MAGIC | 📥 Entrada | `gold.grafo_vertices` / `gold.grafo_arestas` e `silver.registros_cadastro` (notebook 01) |
# MAGIC | 📤 Saída | `silver.identidade`, `gold.grafo_*_resolvido` e `gold.features_grafo_loja` (entrada do notebook 03) |
# MAGIC
# MAGIC ### Roteiro
# MAGIC 1. **Motifs** — descrever o formato de uma relação suspeita e deixar o GraphFrames achar todos os casos
# MAGIC 2. **Resolução de identidade** — Splink (Fellegi-Sunter) + `connectedComponents`
# MAGIC 3. **PageRank e métricas de centralidade** — graus, componentes, comunidades, triângulos, distância até fraude, agregados de vizinhos
# MAGIC
# MAGIC > ⚙️ Requer cluster **Dedicated** com Databricks Runtime **ML** (GraphFrames) e as bibliotecas `splink` + JAR de UDFs do Splink instaladas (ver README).

# COMMAND ----------

# MAGIC %md ## 0. Configuração

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM

CATALOGO = "cielo_pld"
BRONZE, SILVER, GOLD = f"{CATALOGO}.bronze", f"{CATALOGO}.silver", f"{CATALOGO}.gold"
spark.sql(f"USE CATALOG {CATALOGO}")

def salvar(df, tabela, comentario):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")
    print(f"✔ {tabela}: {spark.table(tabela).count():,} linhas")

V = spark.table(f"{GOLD}.grafo_vertices").cache()
E = spark.table(f"{GOLD}.grafo_arestas").cache()
g = GraphFrame(V, E)
print(f"Grafo: {V.count():,} vértices | {E.count():,} arestas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Motifs — encontrando padrões na estrutura
# MAGIC Um *motif* descreve o **formato** de uma relação: `(a)-[e1]->(c); (b)-[e2]->(c)` significa "dois vértices `a` e `b` apontando para o mesmo `c`".
# MAGIC O `g.find()` devolve um DataFrame com **todos** os trechos do grafo que se encaixam — e depois filtramos pelos atributos.
# MAGIC
# MAGIC > 💡 **Dica de performance:** filtre as arestas **antes** do `find()` (só as relações que importam para o padrão). Motifs sobre o grafo inteiro
# MAGIC > fazem *self-joins* em vértices com milhares de vizinhos.
# MAGIC
# MAGIC ### 1.1 Conta compartilhada por lojas diferentes
# MAGIC Lojas **sem vínculo societário** (raízes de CNPJ diferentes) que liquidam na mesma conta. Filiais do mesmo grupo dividindo conta é normal — por isso o filtro `a.nu_raiz <> b.nu_raiz`.

# COMMAND ----------

g_liquida = GraphFrame(V, E.filter("relacao = 'liquida_em'"))
padrao = g_liquida.find("(a)-[e1]->(c); (b)-[e2]->(c)")
conta_compartilhada = padrao.filter("a.tipo = 'LOJA' AND b.tipo = 'LOJA' AND a.id < b.id AND a.nu_raiz <> b.nu_raiz")
display(conta_compartilhada.select(F.col("a.nome").alias("loja_a"), F.col("a.fl_fraude").alias("fraude_a"),
                                   F.col("b.nome").alias("loja_b"), F.col("b.fl_fraude").alias("fraude_b"), F.col("c.id").alias("conta")).limit(20))
print(f"Pares de lojas de raízes diferentes na mesma conta: {conta_compartilhada.count():,}")

# COMMAND ----------

# MAGIC %md ### 1.2 Sócio em comum com loja já fraudada

# COMMAND ----------

g_socio = GraphFrame(V, E.filter("relacao = 'socio'"))
socio_comum = (g_socio.find("(a)-[e1]->(s); (b)-[e2]->(s)")
    .filter("a.tipo = 'LOJA' AND b.tipo = 'LOJA' AND a.id <> b.id AND a.fl_fraude = 0 AND b.fl_fraude = 1"))
display(socio_comum.select(F.col("a.nome").alias("loja_sem_alerta"), F.col("s.nome").alias("socio"), F.col("b.nome").alias("loja_fraudada")).limit(20))
print(f"Lojas sem alerta que compartilham sócio com loja fraudada: {socio_comum.select('a.id').distinct().count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Ciclos de transações (A → B → C → A)
# MAGIC Dinheiro que "dá a volta" e retorna à origem é um clássico de **layering**. Usamos só PIX de valor alto (≥ R$ 10 mil) e exigimos ordem temporal plausível.

# COMMAND ----------

g_pix_alto = GraphFrame(V.filter("tipo = 'DOC'"), E.filter("relacao = 'pix' AND valor >= 10000"))
ciclos = (g_pix_alto.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
    .filter("a.id < b.id AND a.id < c.id")
    .filter("e1.ts_min <= e2.ts_max AND e2.ts_min <= e3.ts_max"))
display(ciclos.select(F.col("a.nome").alias("a"), F.col("b.nome").alias("b"), F.col("c.nome").alias("c"),
                      F.round("e1.valor", 2).alias("a_para_b"), F.round("e2.valor", 2).alias("b_para_c"), F.round("e3.valor", 2).alias("c_para_a")).limit(20))
print(f"Ciclos de 3 saltos: {ciclos.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 1
# MAGIC Escreva o motif de um **ciclo de 4 saltos** `(a)->(b)->(c)->(d)->(a)` em `g_pix_alto` e conte quantos existem.
# MAGIC *Dica: para não contar o mesmo ciclo 4 vezes (uma por rotação), exija que `a` seja o menor id.*

# COMMAND ----------

# ✏️ seu código aqui
# ciclos4 = g_pix_alto.find("...")

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

ciclos4 = (g_pix_alto.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)")
    .filter("a.id < b.id AND a.id < c.id AND a.id < d.id AND a.id <> c.id AND b.id <> d.id"))
print(f"Ciclos de 4 saltos: {ciclos4.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 "A Nova Loja" — ligada a várias lojas por atributos diferentes
# MAGIC Lembra do slide da visão tabular? A Nova Loja divide **telefone e sócio** com a Loja A, **endereço** com a Loja B e **conta** com a Loja C.
# MAGIC Juntamos os atributos que a loja tem **diretamente** (sócio, conta de liquidação, endereço) e **via titular** — um motif de 2 saltos
# MAGIC `(loja)-[titular]->(dono); (dono)-[...]->(atributo)` — e procuramos lojas que compartilham atributos.

# COMMAND ----------

REL_LOJA = ["socio", "liquida_em", "localizada_em"]
REL_TITULAR = ["recebe_em", "paga_com", "tem_telefone", "tem_email", "usa_dispositivo"]

def atributos_da_loja(V, E, max_lojas_por_atributo=50):
    direto = (GraphFrame(V, E.filter(F.col("relacao").isin(REL_LOJA))).find("(l)-[r]->(x)").filter("l.tipo = 'LOJA'")
              .select(F.col("l.id").alias("loja"), F.col("l.nu_raiz").alias("raiz"), F.col("x.id").alias("atributo"), F.col("r.relacao").alias("via")))
    titular = (GraphFrame(V, E.filter(F.col("relacao").isin(["titular"] + REL_TITULAR))).find("(l)-[t]->(d); (d)-[r]->(x)")
               .filter("l.tipo = 'LOJA' AND t.relacao = 'titular' AND r.relacao <> 'titular'")
               .select(F.col("l.id").alias("loja"), F.col("l.nu_raiz").alias("raiz"), F.col("x.id").alias("atributo"), F.concat(F.lit("titular→"), F.col("r.relacao")).alias("via")))
    la = direto.unionByName(titular).distinct()
    lojas_por_attr = la.groupBy("atributo").agg(F.countDistinct("loja").alias("_n"))
    return la.join(lojas_por_attr.filter(f"_n <= {max_lojas_por_atributo}"), "atributo").drop("_n")   # remove super-nós genéricos

def pares_de_lojas(la):
    return (la.alias("a").join(la.alias("b"), "atributo").filter("a.loja < b.loja AND a.raiz <> b.raiz")
              .groupBy(F.col("a.loja").alias("loja_a"), F.col("b.loja").alias("loja_b"))
              .agg(F.countDistinct("atributo").alias("qt_atributos"), F.array_sort(F.collect_set("a.via")).alias("vias")))

pares_antes = pares_de_lojas(atributos_da_loja(V, E)).cache()
nova_loja = (pares_antes.select(F.col("loja_a").alias("loja"), "loja_b", "vias").unionByName(pares_antes.select(F.col("loja_b").alias("loja"), F.col("loja_a").alias("loja_b"), "vias"))
    .groupBy("loja").agg(F.countDistinct("loja_b").alias("qt_lojas_ligadas"), F.array_sort(F.array_distinct(F.flatten(F.collect_list("vias")))).alias("vias"))
    .join(V.select(F.col("id").alias("loja"), "nome", "fl_fraude"), "loja").orderBy(F.desc("qt_lojas_ligadas")))
display(nova_loja.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 O problema: a mesma pessoa em vértices diferentes
# MAGIC A padronização do notebook 01 resolve máscara e zeros à esquerda, mas **não resolve um dígito digitado errado**: o documento continua inválido
# MAGIC e vira **outro vértice**. Veja pares de vértices `DOC` com o **mesmo nome** e documentos a **1 dígito** de distância:

# COMMAND ----------

docs = V.filter("tipo = 'DOC' AND nome IS NOT NULL").select("id", "nome", "fl_doc_valido")
mesmo_nome = (docs.alias("x").join(docs.alias("y"), "nome").filter("x.id < y.id")
              .filter(F.levenshtein(F.col("x.id"), F.col("y.id")) == 1)
              .select("nome", F.col("x.id").alias("vertice_1"), F.col("x.fl_doc_valido").alias("valido_1"), F.col("y.id").alias("vertice_2"), F.col("y.fl_doc_valido").alias("valido_2")))
display(mesmo_nome.limit(10))
qt_invalidos = V.filter("tipo = 'DOC' AND fl_doc_valido = 0").count()
print(f"Vértices DOC com DV inválido: {qt_invalidos:,} | pares 'mesmo nome, 1 dígito de diferença': {mesmo_nome.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Resolução de identidade com Splink
# MAGIC | Passo | O que acontece |
# MAGIC |---|---|
# MAGIC | 01 · Padronizar | feito no notebook 01 (`silver.registros_cadastro`) |
# MAGIC | 02 · Comparar com Splink | *linkage* probabilístico **Fellegi-Sunter** no Spark: compara documento, nome, endereço, telefone e e-mail tolerando erros e dá uma **probabilidade de match** para cada par |
# MAGIC | 03 · Agrupar | pares aprovados viram **arestas**; `connectedComponents` do GraphFrames dá um id único por grupo — a **identidade resolvida** |
# MAGIC
# MAGIC **Fellegi-Sunter em 3 linhas:** para cada campo, o Splink estima `m` = P(campo concorda | mesma pessoa) e `u` = P(campo concorda | pessoas diferentes).
# MAGIC Cada concordância soma `log2(m/u)` ao *match weight*; cada discordância subtrai. A soma vira probabilidade.
# MAGIC
# MAGIC **Blocking:** comparar todos com todos (n²) é inviável — só comparamos pares que coincidem em **documento**, **telefone** ou **nome**.

# COMMAND ----------

# MAGIC %md ### 2.1 Os registros

# COMMAND ----------

registros = spark.table(f"{SILVER}.registros_cadastro").select("unique_id", "cpf_cnpj", "nome", "telefone", "email", "endereco").cache()
print(f"{registros.count():,} registros | {registros.select('cpf_cnpj').distinct().count():,} documentos distintos")
display(registros.filter("telefone is not null and endereco is not null").limit(10))

# COMMAND ----------

# MAGIC %md ### 2.2 Configuração do modelo

# COMMAND ----------

from splink import SparkAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl

settings = SettingsCreator(
    link_type="dedupe_only",
    unique_id_column_name="unique_id",
    blocking_rules_to_generate_predictions=[
        block_on("cpf_cnpj"),
        block_on("telefone"),
        block_on("nome"),                                     # recupera documentos com dígito trocado
    ],
    comparisons=[
        cl.LevenshteinAtThresholds("cpf_cnpj", [1]),          # 1 dígito de diferença = provável erro de digitação
        cl.JaroWinklerAtThresholds("nome", [0.95, 0.88]),
        cl.LevenshteinAtThresholds("endereco", [2, 5]),
        cl.ExactMatch("telefone"),
        cl.ExactMatch("email"),
    ],
)
linker = Linker(registros, settings, SparkAPI(spark_session=spark, break_lineage_method="persist"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Treinando os parâmetros (sem rótulos!)
# MAGIC 1. **Probabilidade *a priori*** de dois registros aleatórios serem a mesma pessoa
# MAGIC 2. **`u`** por amostragem aleatória de pares (quase todos são pessoas diferentes)
# MAGIC 3. **`m`** por *Expectation-Maximisation* — em dois "blocos", para que cada campo seja estimado quando **não** está sendo usado como bloco

# COMMAND ----------

linker.training.estimate_probability_two_random_records_match([block_on("cpf_cnpj")], recall=0.9)
linker.training.estimate_u_using_random_sampling(max_pairs=2e6)
linker.training.estimate_parameters_using_expectation_maximisation(block_on("cpf_cnpj"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("nome"))

# COMMAND ----------

try:
    displayHTML(linker.visualisations.match_weights_chart().to_html())
except Exception as ex:
    print(f"(gráfico indisponível: {ex})")

# COMMAND ----------

# MAGIC %md ### 2.4 Probabilidade de match para cada par

# COMMAND ----------

pares = linker.inference.predict(threshold_match_probability=0.01).as_spark_dataframe().cache()
display(pares.select((F.floor(F.col("match_probability") * 10) / 10).alias("faixa_probabilidade")).groupBy("faixa_probabilidade").count().orderBy("faixa_probabilidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC Dois casos que mostram o valor do modelo probabilístico:
# MAGIC * **Documento com dígito trocado, mesmo nome** → match (era um vértice "fantasma")
# MAGIC * **Mesmo telefone, pessoas diferentes** → *não* é match. Quem divide o telefone com o controlador é um **elo do grafo**, não a mesma identidade!

# COMMAND ----------

cols = ["match_probability", "cpf_cnpj_l", "cpf_cnpj_r", "nome_l", "nome_r", "telefone_l", "telefone_r"]
display(pares.filter("cpf_cnpj_l <> cpf_cnpj_r AND match_probability >= 0.9").select(*cols).limit(10))

# COMMAND ----------

display(pares.filter("telefone_l = telefone_r AND cpf_cnpj_l <> cpf_cnpj_r AND match_probability < 0.5").select(*cols).orderBy("telefone_l").limit(10))

# COMMAND ----------

# MAGIC %md ### 2.5 Agrupar: pares aprovados → arestas → `connectedComponents`

# COMMAND ----------

LIMIAR = 0.9
arestas_match = pares.filter(F.col("match_probability") >= LIMIAR).selectExpr("unique_id_l AS src", "unique_id_r AS dst")
g_match = GraphFrame(registros.select(F.col("unique_id").alias("id")), arestas_match)
# algorithm="graphx" dispensa diretório de checkpoint
grupos = g_match.connectedComponents(algorithm="graphx").withColumnRenamed("id", "unique_id")

registro_id = registros.join(grupos, "unique_id")
identidade = (registro_id.groupBy("cpf_cnpj").agg(F.mode("component").alias("component"))
    .join(registro_id.groupBy("component").agg(
        F.concat(F.lit("IDN:"), F.col("component").cast("string")).alias("id_identidade"),
        F.mode(F.col("nome")).alias("nm_identidade"),
        F.countDistinct("cpf_cnpj").alias("qt_documentos"),
        F.array_sort(F.collect_set("cpf_cnpj")).alias("documentos")), "component")
    .select("cpf_cnpj", "id_identidade", "nm_identidade", "qt_documentos", "documentos"))
salvar(identidade, f"{SILVER}.identidade", "De-para documento -> identidade resolvida (Splink + connectedComponents)")
identidade = spark.table(f"{SILVER}.identidade")

display(identidade.agg(F.count("*").alias("documentos"), F.countDistinct("id_identidade").alias("identidades"),
                       F.countDistinct(F.when(F.col("qt_documentos") > 1, F.col("id_identidade"))).alias("identidades_com_2+_documentos")))
display(identidade.filter("qt_documentos > 1").select("id_identidade", "nm_identidade", "documentos").distinct().limit(10))
# controle de qualidade: identidades que juntaram 2+ documentos VÁLIDOS (possível over-merge — deve ser raro)
validos = spark.table(f"{SILVER}.registros_cadastro").filter("fl_doc_valido").select("cpf_cnpj").distinct()
print("Identidades com 2+ documentos válidos:", identidade.join(validos, "cpf_cnpj").groupBy("id_identidade").count().filter("count > 1").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 2
# MAGIC Mude o `LIMIAR` para **0.5** e depois para **0.99**. O que acontece com o número de identidades com 2+ documentos?
# MAGIC Em PLD, o que é pior: **juntar** duas pessoas diferentes ou **não juntar** a mesma pessoa?

# COMMAND ----------

# ✏️ seu código aqui
# for limiar in [0.5, 0.9, 0.99]: ...

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

for limiar in [0.5, 0.9, 0.99]:
    cc_l = GraphFrame(registros.select(F.col("unique_id").alias("id")),
                      pares.filter(F.col("match_probability") >= limiar).selectExpr("unique_id_l AS src", "unique_id_r AS dst")).connectedComponents(algorithm="graphx")
    n = registros.join(cc_l.withColumnRenamed("id", "unique_id"), "unique_id").groupBy("component").agg(F.countDistinct("cpf_cnpj").alias("d")).filter("d > 1").count()
    print(f"limiar {limiar}: {n:,} identidades com 2+ documentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Grafo resolvido
# MAGIC Cada vértice `DOC:<documento>` é substituído pela sua identidade `IDN:<id>`. Arestas que caem no mesmo par são somadas.

# COMMAND ----------

mapa = identidade.select(F.concat(F.lit("DOC:"), "cpf_cnpj").alias("id_doc"), "id_identidade")
def resolve(col):
    return F.coalesce(F.col(f"_{col}"), F.col(col))
E_res = (E.join(mapa.toDF("src", "_src"), "src", "left").join(mapa.toDF("dst", "_dst"), "dst", "left")
    .select(resolve("src").alias("src"), resolve("dst").alias("dst"), "relacao", "qt", "valor", "ts_min", "ts_max")
    .filter("src <> dst")
    .groupBy("src", "dst", "relacao").agg(F.sum("qt").alias("qt"), F.sum("valor").alias("valor"), F.min("ts_min").alias("ts_min"), F.max("ts_max").alias("ts_max")))
v_idn = (identidade.groupBy("id_identidade").agg(F.first("nm_identidade").alias("nome"), F.min("cpf_cnpj").alias("doc"), F.max("qt_documentos").alias("qt_documentos"))
    .select(F.col("id_identidade").alias("id"), F.lit("IDENTIDADE").alias("tipo"), "nome", F.lit(None).cast("bigint").alias("nu_ec"),
            F.when(F.length("doc") == 14, F.substring("doc", 1, 8)).otherwise(F.col("doc")).alias("nu_raiz"),
            F.lit(0).alias("fl_fraude"), F.lit(None).cast("double").alias("pc_chargeback"), F.lit(None).cast("int").alias("fl_doc_valido")))
V_res = V.filter("tipo <> 'DOC'").unionByName(v_idn).unionByName(V.filter("tipo = 'DOC'").join(mapa.withColumnRenamed("id_doc", "id"), "id", "left_anti"))
salvar(V_res, f"{GOLD}.grafo_vertices_resolvido", "Vértices do grafo PLD após a resolução de identidade")
salvar(E_res, f"{GOLD}.grafo_arestas_resolvido", "Arestas do grafo PLD após a resolução de identidade")
V_res, E_res = spark.table(f"{GOLD}.grafo_vertices_resolvido").cache(), spark.table(f"{GOLD}.grafo_arestas_resolvido").cache()
g_res = GraphFrame(V_res, E_res)

# COMMAND ----------

# MAGIC %md **Antes × depois:** a resolução reduz vértices e **recupera ligações** entre lojas que estavam escondidas atrás de documentos digitados errado.

# COMMAND ----------

pares_depois = pares_de_lojas(atributos_da_loja(V_res, E_res)).cache()
display(spark.createDataFrame([
    ("vértices pessoa/empresa", V.filter("tipo = 'DOC'").count(), V_res.filter("tipo IN ('IDENTIDADE', 'DOC')").count()),
    ("pares de lojas (raízes diferentes) ligadas por atributo", pares_antes.count(), pares_depois.count())],
    "metrica string, antes long, depois long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. PageRank e métricas de centralidade
# MAGIC Cada algoritmo devolve um **DataFrame** que vira coluna de feature no notebook 03.
# MAGIC
# MAGIC | Algoritmo | Feature | Sinal de risco |
# MAGIC |---|---|---|
# MAGIC | `degrees` | nº de contas, sócios e lojas ligados | conta usada por muitas lojas |
# MAGIC | `pageRank` | centralidade na rede | vértices que concentram o fluxo |
# MAGIC | `connectedComponents` | tamanho do grupo | grupo com fraude confirmada |
# MAGIC | `labelPropagation` | comunidade | comunidade com alta taxa de chargeback |
# MAGIC | `triangleCount` | laços fechados | lojas que se interligam |
# MAGIC | `shortestPaths` | distância até fraude confirmada | proximidade de casos conhecidos |
# MAGIC | `aggregateMessages` | agregados dos vizinhos | % de vizinhos com chargeback |
# MAGIC | `find` (motifs) | flags de padrão | conta compartilhada, sócio em comum, ciclos |
# MAGIC
# MAGIC Usamos dois sub-grafos: **vínculos** (cadastro e atributos — quem se parece com quem) e **fluxo** (PIX — para onde vai o dinheiro).

# COMMAND ----------

REL_VINCULO = ["titular", "socio", "liquida_em", "localizada_em", "recebe_em", "paga_com", "tem_telefone", "tem_email", "usa_dispositivo", "entrega_em"]
E_v0 = E_res.filter(F.col("relacao").isin(REL_VINCULO))
grau_attr = E_v0.groupBy("dst").agg(F.count("*").alias("_n"))
ok = V_res.filter(F.col("tipo").isin("LOJA", "IDENTIDADE", "DOC")).select(F.col("id").alias("dst")) \
    .unionByName(grau_attr.filter("_n BETWEEN 2 AND 100").select("dst"))       # atributo isolado não liga ninguém; super-nó é genérico
E_vinc = E_v0.join(ok, "dst", "left_semi").cache()
g_vinc = GraphFrame(V_res, E_vinc)
g_fluxo = GraphFrame(V_res.filter("tipo IN ('IDENTIDADE', 'DOC')"), E_res.filter("relacao = 'pix'"))

def simetrico(e):
    """shortestPaths/PPR seguem o sentido src->dst; para 'proximidade' precisamos dos dois sentidos."""
    return e.unionByName(e.select(F.col("dst").alias("src"), F.col("src").alias("dst"), *[c for c in e.columns if c not in ("src", "dst")]))

lojas = V_res.filter("tipo = 'LOJA'").select(F.col("id").alias("loja"), "nu_ec", "fl_fraude", "pc_chargeback")
titular = E_res.filter("relacao = 'titular'").select(F.col("src").alias("loja"), F.col("dst").alias("dono"))

# COMMAND ----------

# MAGIC %md ### 3.1 `degrees` — quantas contas, sócios e lojas estão ligados

# COMMAND ----------

f_grau = (g_res.degrees.withColumnRenamed("id", "loja").withColumnRenamed("degree", "grau")
    .join(E_res.filter("relacao IN ('liquida_em', 'socio')").groupBy(F.col("src").alias("loja"))
          .agg(F.countDistinct(F.when(F.col("relacao") == "liquida_em", F.col("dst"))).alias("qt_contas_liquidacao"),
               F.countDistinct(F.when(F.col("relacao") == "socio", F.col("dst"))).alias("qt_socios")), "loja", "left"))
contas_multi = (g_liquida.inDegrees.join(V.filter("tipo = 'CONTA'").select("id"), "id").orderBy(F.desc("inDegree")))
display(contas_multi.limit(10))

# COMMAND ----------

# MAGIC %md ### 3.2 `pageRank` — quem concentra o fluxo de dinheiro

# COMMAND ----------

pr = g_fluxo.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "nome", "pagerank")
display(pr.orderBy(F.desc("pagerank")).limit(15))
f_pr = (titular.join(pr.select(F.col("id").alias("dono"), "pagerank"), "dono", "left")
        .join(g_fluxo.inDegrees.select(F.col("id").alias("dono"), F.col("inDegree").alias("qt_pagadores_pix")), "dono", "left")
        .join(g_fluxo.outDegrees.select(F.col("id").alias("dono"), F.col("outDegree").alias("qt_destinos_pix")), "dono", "left")
        .groupBy("loja").agg(F.max("pagerank").alias("pagerank"), F.max("qt_pagadores_pix").alias("qt_pagadores_pix"), F.max("qt_destinos_pix").alias("qt_destinos_pix")))

# COMMAND ----------

# MAGIC %md ### 3.3 `connectedComponents` — tamanho do grupo e fraude confirmada no grupo

# COMMAND ----------

cc = g_vinc.connectedComponents(algorithm="graphx").select("id", "tipo", "nu_raiz", "fl_fraude", "component").cache()
comp = cc.groupBy("component").agg(F.count("*").alias("tam_componente"), F.sum((F.col("tipo") == "LOJA").cast("int")).alias("qt_lojas_componente"),
                                   F.countDistinct(F.when(F.col("tipo") == "LOJA", F.col("nu_raiz"))).alias("qt_raizes_componente"),
                                   F.sum("fl_fraude").alias("qt_fraude_componente"))
f_cc = (cc.filter("tipo = 'LOJA'").select(F.col("id").alias("loja"), "component", "fl_fraude").join(comp, "component")
        .select("loja", F.col("component").alias("id_componente"), "tam_componente", "qt_lojas_componente", "qt_raizes_componente",
                (F.col("qt_fraude_componente") - F.col("fl_fraude")).alias("qt_outras_lojas_fraude_componente")))
display(comp.filter("qt_raizes_componente >= 2").orderBy(F.desc("qt_fraude_componente"), F.desc("qt_lojas_componente")).limit(10))

# COMMAND ----------

# MAGIC %md Visualizando um componente suspeito (lojas de raízes diferentes, com fraude confirmada — **borda vermelha**):

# COMMAND ----------

import networkx as nx, matplotlib.pyplot as plt
alvo = (comp.filter("qt_raizes_componente >= 3 AND qt_fraude_componente >= 1 AND tam_componente <= 300").orderBy(F.desc("qt_fraude_componente")).first()
        or comp.filter("tam_componente <= 300").orderBy(F.desc("qt_lojas_componente")).first())["component"]
vs = cc.filter(F.col("component") == alvo).select("id", "tipo", "fl_fraude").toPandas()
es = E_vinc.join(cc.filter(F.col("component") == alvo).select(F.col("id").alias("src")), "src", "left_semi").select("src", "dst", "relacao").toPandas()
G = nx.Graph(); G.add_nodes_from(vs.id); G.add_edges_from(zip(es.src, es.dst))
cores = {"LOJA": "#FF3621", "IDENTIDADE": "#1B3139", "DOC": "#1B3139", "CONTA": "#00A972", "TELEFONE": "#8B5CF6", "EMAIL": "#94A3B8",
         "ENDERECO": "#F59E0B", "DISPOSITIVO": "#0EA5E9", "CARTAO": "#CBD5E1"}
tipo, fraude = dict(zip(vs.id, vs.tipo)), dict(zip(vs.id, vs.fl_fraude))
fig, ax = plt.subplots(figsize=(13, 9))
pos = nx.spring_layout(G, seed=7, k=0.5)
nx.draw_networkx_edges(G, pos, alpha=0.25, ax=ax)
nx.draw_networkx_nodes(G, pos, node_color=[cores.get(tipo[n], "#333") for n in G.nodes], node_size=[520 if tipo[n] == "LOJA" else 110 for n in G.nodes],
                       edgecolors=["red" if fraude[n] == 1 else "white" for n in G.nodes], linewidths=3, ax=ax)
for t, cor in cores.items():
    ax.scatter([], [], c=cor, label=t)
ax.legend(loc="upper left", fontsize=8); ax.set_title("Componente de vínculos com lojas de raízes diferentes"); ax.axis("off")
display(fig)

# COMMAND ----------

# MAGIC %md ### 3.4 `labelPropagation` — comunidades (vínculos + fluxo PIX) e taxa de chargeback

# COMMAND ----------

g_com = GraphFrame(V_res, E_vinc.unionByName(E_res.filter("relacao = 'pix'")))
com = g_com.labelPropagation(maxIter=5).select("id", "tipo", "fl_fraude", "pc_chargeback", F.col("label").alias("comunidade"))
com_lojas = com.filter("tipo = 'LOJA'")
stats_com = com_lojas.groupBy("comunidade").agg(F.count("*").alias("_n"), F.sum("fl_fraude").alias("_fraude"), F.sum("pc_chargeback").alias("_chrg"))
f_com = (com_lojas.join(stats_com, "comunidade")
         .select(F.col("id").alias("loja"), F.col("_n").alias("qt_lojas_comunidade"),
                 F.when(F.col("_n") > 1, (F.col("_chrg") - F.col("pc_chargeback")) / (F.col("_n") - 1)).otherwise(0.0).alias("chargeback_medio_comunidade"),
                 (F.col("_fraude") - F.col("fl_fraude")).alias("qt_outras_lojas_fraude_comunidade")))
display(stats_com.filter("_n >= 3").orderBy(F.desc("_fraude")).limit(10))

# COMMAND ----------

# MAGIC %md ### 3.5 `triangleCount` — lojas que se interligam

# COMMAND ----------

g_lojas = GraphFrame(lojas.withColumnRenamed("loja", "id"), pares_depois.select(F.col("loja_a").alias("src"), F.col("loja_b").alias("dst"), "qt_atributos"))
f_tri = g_lojas.triangleCount().select(F.col("id").alias("loja"), F.col("count").alias("qt_triangulos"))
display(f_tri.join(lojas, "loja").groupBy("fl_fraude").agg(F.avg("qt_triangulos").alias("media_triangulos"), F.count("*").alias("lojas")))

# COMMAND ----------

# MAGIC %md ### 3.6 `shortestPaths` — distância até uma loja com fraude confirmada

# COMMAND ----------

fraudadas = [r.id for r in V_res.filter("tipo = 'LOJA' AND fl_fraude = 1").select("id").collect()]
sp = GraphFrame(V_res, simetrico(E_vinc)).shortestPaths(landmarks=fraudadas)
f_dist = (sp.filter("tipo = 'LOJA'")
    .select(F.col("id").alias("loja"), F.map_filter("distances", lambda k, v: k != F.col("id")).alias("d"))      # ignora a distância 0 até ela mesma
    .select("loja", F.coalesce(F.array_min(F.map_values("d")), F.lit(99)).alias("dist_fraude")))
display(f_dist.join(lojas, "loja").groupBy("fl_fraude", "dist_fraude").count().orderBy("fl_fraude", "dist_fraude"))

# COMMAND ----------

# MAGIC %md ### 3.7 `aggregateMessages` — o que os vizinhos dizem (chargeback e fraude)

# COMMAND ----------

g_lojas_sim = GraphFrame(lojas.withColumnRenamed("loja", "id"),
                         simetrico(pares_depois.select(F.col("loja_a").alias("src"), F.col("loja_b").alias("dst"))))
f_viz = (g_lojas_sim.aggregateMessages(
            F.count(AM.msg).alias("qt_lojas_vizinhas"),
            sendToDst=F.lit(1))
         .join(g_lojas_sim.aggregateMessages(F.avg(AM.msg).alias("pct_vizinhos_chargeback_alto"), sendToDst=(AM.src["pc_chargeback"] > 1.0).cast("double")), "id")
         .join(g_lojas_sim.aggregateMessages(F.avg(AM.msg).alias("pct_vizinhos_fraude"), sendToDst=AM.src["fl_fraude"].cast("double")), "id")
         .withColumnRenamed("id", "loja"))
display(f_viz.join(lojas, "loja").groupBy("fl_fraude").agg(*[F.round(F.avg(c), 3).alias(c) for c in ["qt_lojas_vizinhas", "pct_vizinhos_chargeback_alto", "pct_vizinhos_fraude"]]))

# COMMAND ----------

# MAGIC %md ### 3.8 Flags de motif por loja

# COMMAND ----------

la_res = atributos_da_loja(V_res, E_res)
conta_comp = (la_res.filter("via IN ('liquida_em', 'titular→recebe_em')").alias("a").join(la_res.filter("via IN ('liquida_em', 'titular→recebe_em')").alias("b"), "atributo")
              .filter("a.loja <> b.loja AND a.raiz <> b.raiz").select(F.col("a.loja").alias("loja")).distinct().withColumn("fl_conta_compartilhada", F.lit(1)))
socio_fraude = (GraphFrame(V_res, E_res.filter("relacao = 'socio'")).find("(a)-[e1]->(s); (b)-[e2]->(s)")
                .filter("a.id <> b.id AND b.fl_fraude = 1").select(F.col("a.id").alias("loja")).distinct().withColumn("fl_socio_comum_fraude", F.lit(1)))
ciclos_res = (GraphFrame(V_res.filter("tipo IN ('IDENTIDADE', 'DOC')"), E_res.filter("relacao = 'pix' AND valor >= 10000"))
              .find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)").filter("a.id <> b.id AND b.id <> c.id AND a.id <> c.id"))
f_ciclo = (ciclos_res.select(F.col("a.id").alias("dono")).groupBy("dono").count()
           .join(titular, "dono").groupBy("loja").agg(F.sum("count").alias("qt_ciclos_pix")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 3 — PageRank personalizado
# MAGIC O `parallelPersonalizedPageRank` faz o "passeio aleatório" **recomeçar sempre em lojas com fraude confirmada**: quem fica com PageRank alto
# MAGIC está estruturalmente perto de casos conhecidos. Calcule-o no grafo de vínculos (simétrico) com `sourceIds=fraudadas[:20]` e veja as lojas
# MAGIC **sem alerta** com maior pontuação.

# COMMAND ----------

# ✏️ seu código aqui
# ppr = GraphFrame(V_res, simetrico(E_vinc)).parallelPersonalizedPageRank(resetProbability=0.15, sourceIds=..., maxIter=10)

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

from pyspark.ml.functions import vector_to_array
ppr = GraphFrame(V_res, simetrico(E_vinc)).parallelPersonalizedPageRank(resetProbability=0.15, sourceIds=fraudadas[:20], maxIter=10)
display(ppr.vertices.filter("tipo = 'LOJA' AND fl_fraude = 0")
        .select("id", "nome", F.aggregate(vector_to_array("pageranks"), F.lit(0.0), lambda acc, x: acc + x).alias("ppr_fraude"))
        .orderBy(F.desc("ppr_fraude")).limit(10))

# COMMAND ----------

# MAGIC %md ### 3.9 Tabela de features de grafo por loja → `gold.features_grafo_loja`

# COMMAND ----------

features = (lojas.select("loja", "nu_ec")
    .join(f_grau, "loja", "left").join(f_pr, "loja", "left").join(f_cc, "loja", "left").join(f_com, "loja", "left")
    .join(f_tri, "loja", "left").join(f_dist, "loja", "left").join(f_viz, "loja", "left")
    .join(conta_comp, "loja", "left").join(socio_fraude, "loja", "left").join(f_ciclo, "loja", "left")
    .fillna(0).withColumn("dist_fraude", F.when(F.col("dist_fraude") == 0, 99).otherwise(F.col("dist_fraude")))
    .drop("loja"))
salvar(features, f"{GOLD}.features_grafo_loja", "Features de grafo por loja (graus, PageRank, componentes, comunidades, triângulos, distância à fraude, vizinhos e motifs)")
display(spark.table(f"{GOLD}.features_grafo_loja").join(spark.table(f"{SILVER}.loja").select("nu_ec", "fl_fraude"), "nu_ec").groupBy("fl_fraude")
        .agg(*[F.round(F.avg(c), 3).alias(c) for c in ["grau", "pagerank", "qt_lojas_componente", "qt_triangulos", "dist_fraude", "pct_vizinhos_fraude",
                                                       "fl_conta_compartilhada", "fl_socio_comum_fraude", "qt_ciclos_pix"]]))

# COMMAND ----------

# MAGIC %md ✅ **Checkpoint**

# COMMAND ----------

f = spark.table(f"{GOLD}.features_grafo_loja")
assert f.count() == spark.table(f"{SILVER}.loja").count(), "deve haver 1 linha por loja"
assert f.filter("qt_lojas_componente > 1").count() > 0
print("✔ Features de grafo prontas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC * **Motifs** acharam contas compartilhadas, sócios em comum com lojas fraudadas e ciclos de PIX — sem escrever joins à mão.
# MAGIC * **Splink** unificou menções da mesma pessoa/empresa (inclusive com dígito trocado) sem confundir quem só **divide** um telefone.
# MAGIC * **PageRank, graus, componentes, comunidades, triângulos, distância e vizinhos** viraram colunas em `gold.features_grafo_loja`.
# MAGIC
# MAGIC ➡️ **Próximo:** `03_graph_ml` — treinar o modelo com essas features, avaliar e implantar.
