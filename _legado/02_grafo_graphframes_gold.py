# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Grafo de relacionamentos PLD com GraphFrames → Gold
# MAGIC
# MAGIC **Por que grafos em PLD?** A lavagem de dinheiro raramente aparece numa transação isolada: ela está na **estrutura** —
# MAGIC ECs de CNPJs diferentes que liquidam na mesma conta, laranjas que usam o mesmo dispositivo do controlador, PIX que "dão a volta"
# MAGIC e retornam à origem. Um modelo tabular olha cada EC sozinho; o grafo olha **com quem** ele se relaciona.
# MAGIC
# MAGIC | Etapa | Algoritmo GraphFrames | Pergunta de PLD |
# MAGIC |---|---|---|
# MAGIC | 1 | construção de vértices/arestas | Quem se conecta a quem (EC, pessoa/empresa, conta, dispositivo, IP, telefone, e-mail, chave PIX, endereço, cartão)? |
# MAGIC | 2 | `degrees`, *motif finding* `find()` | Quais atributos são compartilhados por ECs de **raízes CNPJ diferentes**? |
# MAGIC | 3 | `connectedComponents()` | Quais "clusters" de identidade reúnem vários ECs sem vínculo societário? |
# MAGIC | 4 | `labelPropagation()` | Quais comunidades de relacionamento (incluindo fluxo PIX) existem? |
# MAGIC | 5 | `pageRank()` / `parallelPersonalizedPageRank()` | Quem é central no fluxo de dinheiro? Quem está "perto" de ECs já listados? |
# MAGIC | 6 | `find()` ciclos + `aggregateMessages` | Há PIX em ciclo (A→B→C→D→A)? Quem recebe e repassa quase tudo (*pass-through*)? |
# MAGIC | 7 | `triangleCount()` | ECs em triângulos de compartilhamento (coesão típica de anéis) |
# MAGIC | 8 | `shortestPaths()` / `bfs()` | A quantos saltos um EC está de um EC da lista negativa? Qual o caminho? |
# MAGIC
# MAGIC Resultado: tabelas Gold `grafo_vertices`, `grafo_arestas`, `ec_features_grafo`, `alerta_componente`, `pix_ciclos`.
# MAGIC
# MAGIC > Requer cluster clássico com **Databricks Runtime ML** (GraphFrames incluso).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from pyspark.ml.functions import vector_to_array
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM


def sv(t): return spark.table(f"{SILVER}.{t}")
def salvar(df, nome, comentario):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{nome}")
    spark.sql(f"COMMENT ON TABLE {GOLD}.{nome} IS '{comentario}'")
    print(f"{GOLD}.{nome}: {spark.table(f'{GOLD}.{nome}').count():,} linhas")

ec, ent, ea = sv("ec_cadastro"), sv("entidade"), sv("entidade_atributo")
crt, pix, antp, frd, lst = sv("trns_cartao"), sv("trns_pix"), sv("trns_antecipacao"), sv("fraude_reportada"), sv("lista_monitoramento")

neg_ec = (lst.filter("tp_lista = 'NEGATIVA' and tp_objeto = 'EC' and fl_ativo")
             .select(F.col("cd_objeto").cast("bigint").alias("nu_ec")).distinct().withColumn("fl_lista_negativa", F.lit(1)))
ATRIBUTOS = ["CONTA", "DISPOSITIVO", "IP", "TELEFONE", "EMAIL", "ENDERECO", "CHAVE_PIX", "CARTAO"]

# COMMAND ----------

# MAGIC %md ## 1. Construção do grafo (vértices e arestas)

# COMMAND ----------

v_ec = (ec.join(neg_ec, "nu_ec", "left")
    .select(F.concat(F.lit("EC:"), F.col("nu_ec")).alias("id"), F.lit("EC").alias("tipo"), F.col("nm_ec").alias("rotulo"), "nu_ec",
            F.coalesce("nu_raiz_cnpj", "nu_doc_titular").alias("nu_raiz"), F.coalesce("fl_lista_negativa", F.lit(0)).alias("fl_lista_negativa")))
v_ent = ent.select(F.concat(F.lit("ENT:"), F.col("id_entidade")).alias("id"), F.concat(F.lit("ENTIDADE_"), F.col("tp_pessoa")).alias("tipo"),
                   F.col("nm_canonico").alias("rotulo"), F.lit(None).cast("bigint").alias("nu_ec"),
                   F.coalesce("nu_raiz_cnpj", "nu_doc").alias("nu_raiz"), F.lit(0).alias("fl_lista_negativa"))

def aresta(df, src, dst, rel, vl=None, ts="ts_oprc"):
    agg = [F.count("*").alias("qt"), (F.sum(vl) if vl else F.lit(None).cast("double")).alias("vl_total"),
           (F.min(ts) if ts else F.lit(None).cast("timestamp")).alias("ts_min"), (F.max(ts) if ts else F.lit(None).cast("timestamp")).alias("ts_max")]
    return (df.select(src.alias("src"), dst.alias("dst"), *( [F.col(vl)] if vl else []), *( [F.col(ts)] if ts else []))
              .filter("src is not null and dst is not null").groupBy("src", "dst").agg(*agg).withColumn("relationship", F.lit(rel)))

attr_id = lambda tipo, col: F.concat(F.lit(f"{tipo}:"), F.col(col))
e_titular = aresta(ec, F.concat(F.lit("EC:"), F.col("nu_ec")), F.concat(F.lit("ENT:"), F.col("id_entidade_titular")), "TITULAR", ts=None)
e_ent_attr = (ea.select(F.concat(F.lit("ENT:"), F.col("id_entidade")).alias("src"), F.concat_ws(":", "tp_atributo", "vl_atributo").alias("dst"),
                        F.col("qt_ocorrencias").alias("qt"), F.lit(None).cast("double").alias("vl_total"),
                        F.col("ts_primeira_ocorrencia").alias("ts_min"), F.col("ts_ultima_ocorrencia").alias("ts_max"),
                        F.concat(F.lit("TEM_"), F.col("tp_atributo")).alias("relationship")))
e_liquida = aresta(antp, F.concat(F.lit("EC:"), F.col("nu_ec")), attr_id("CONTA", "ds_conta_domicilio"), "LIQUIDA_EM", "vl_trns")
e_endereco = aresta(ec, F.concat(F.lit("EC:"), F.col("nu_ec")), attr_id("ENDERECO", "ds_endereco_norm"), "LOCALIZADO_EM", ts=None)
e_ip = aresta(crt.filter("fl_cnp"), F.concat(F.lit("EC:"), F.col("nu_ec")), attr_id("IP", "ds_ip"), "IP_TRANSACAO", "vl_trns")
e_compra = aresta(crt, attr_id("CARTAO", "nu_token_cartao"), F.concat(F.lit("EC:"), F.col("nu_ec")), "COMPRA", "vl_trns")
e_pix = aresta(pix.filter("fl_aprovada"), F.concat(F.lit("ENT:"), F.col("id_entidade_pagador")), F.concat(F.lit("ENT:"), F.col("id_entidade_recebedor")), "PIX", "vl_trns")

arestas = (e_titular.unionByName(e_ent_attr).unionByName(e_liquida).unionByName(e_endereco).unionByName(e_ip)
           .unionByName(e_compra).unionByName(e_pix).filter("src <> dst"))
v_attr = (arestas.select(F.col("dst").alias("id")).union(arestas.select(F.col("src").alias("id"))).distinct()
          .filter(~F.col("id").startswith("EC:") & ~F.col("id").startswith("ENT:"))
          .select("id", F.split("id", ":", 2)[0].alias("tipo"), F.split("id", ":", 2)[1].alias("rotulo"),
                  F.lit(None).cast("bigint").alias("nu_ec"), F.lit(None).cast("string").alias("nu_raiz"), F.lit(0).alias("fl_lista_negativa")))
vertices = v_ec.unionByName(v_ent).unionByName(v_attr)

salvar(vertices, "grafo_vertices", "Vértices do grafo PLD: EC, entidades PF/PJ e atributos (conta, dispositivo, IP, telefone, e-mail, endereço, chave PIX, cartão)")
salvar(arestas, "grafo_arestas", "Arestas do grafo PLD: titularidade, atributos, liquidação, IP, compras com cartão e fluxo PIX")
V, E = spark.table(f"{GOLD}.grafo_vertices").cache(), spark.table(f"{GOLD}.grafo_arestas").cache()
g = GraphFrame(V, E)

# COMMAND ----------

display(V.groupBy("tipo").count().orderBy(F.desc("count")))

# COMMAND ----------

display(E.groupBy("relationship").agg(F.count("*").alias("arestas"), F.sum("qt").alias("eventos"), F.round(F.sum("vl_total"), 2).alias("valor_total")).orderBy(F.desc("arestas")))

# COMMAND ----------

# MAGIC %md ## 2. Graus e atributos compartilhados
# MAGIC ### 2.1 `degrees` — atributos que conectam muitas entidades

# COMMAND ----------

graus = g.degrees.join(V.select("id", "tipo"), "id")
display(graus.filter(F.col("tipo").isin(ATRIBUTOS)).groupBy("tipo")
        .agg(F.count("*").alias("vertices"), F.sum((F.col("degree") >= 2).cast("int")).alias("compartilhados_grau_2+"),
             F.expr("percentile(degree, 0.99)").alias("grau_p99"), F.max("degree").alias("grau_max")).orderBy("tipo"))

# COMMAND ----------

# MAGIC %md ### 2.2 Grafo de **identidade** (sem compras e sem PIX)
# MAGIC Mantemos só ligações "fortes" (titularidade + atributos). Atributos com grau 1 não conectam ninguém e *super-nós* (grau > 30) tendem a ser genéricos — ambos são removidos.

# COMMAND ----------

# IP_TRANSACAO fica fora: em transações CNP o IP é do comprador — ligá-lo ao EC encadearia todo comprador a todo EC onde comprou.
# O compartilhamento de IP entre compradores (laranjas) continua representado por TEM_IP.
REL_IDENT = ["TITULAR", "LIQUIDA_EM", "LOCALIZADO_EM"] + [f"TEM_{a}" for a in ATRIBUTOS]
E_id0 = E.filter(F.col("relationship").isin(REL_IDENT))
grau_attr = E_id0.select(F.col("dst").alias("id")).groupBy("id").count()
attr_ok = grau_attr.join(V.filter(F.col("tipo").isin(ATRIBUTOS)).select("id"), "id").filter("count between 2 and 30").select("id")
E_ident = E_id0.filter("relationship = 'TITULAR'").unionByName(
    E_id0.filter("relationship <> 'TITULAR'").join(attr_ok.withColumnRenamed("id", "dst"), "dst", "left_semi"))
V_ident = V.filter(F.col("tipo").isin("EC", "ENTIDADE_PF", "ENTIDADE_PJ")).unionByName(V.join(attr_ok, "id", "left_semi"))
g_ident = GraphFrame(V_ident.cache(), E_ident.cache())
print(f"Grafo de identidade: {g_ident.vertices.count():,} vértices, {g_ident.edges.count():,} arestas")

def simetrico(e):
    """GraphFrames percorre arestas no sentido src->dst. Para 'ligação' (associação) precisamos dos dois sentidos."""
    return e.unionByName(e.select(F.col("dst").alias("src"), F.col("src").alias("dst"), *[c for c in e.columns if c not in ("src", "dst")]))
g_ident_sim = GraphFrame(g_ident.vertices, simetrico(E_ident).cache())

# COMMAND ----------

# MAGIC %md ### 2.3 *Motif finding* — atributos de EC diretos ou via titular: `(ec)-[TITULAR]->(ent); (ent)-[TEM_*]->(atributo)`

# COMMAND ----------

ec_attr_direto = (g_ident.find("(e)-[r]->(a)")
    .filter("e.tipo = 'EC' and r.relationship <> 'TITULAR'")
    .select(F.col("e.nu_ec").alias("nu_ec"), F.col("e.nu_raiz").alias("nu_raiz"), F.col("a.id").alias("id_atributo"), F.col("a.tipo").alias("tp_atributo")))
ec_attr_titular = (g_ident.find("(e)-[t]->(n); (n)-[r]->(a)")
    .filter("e.tipo = 'EC' and t.relationship = 'TITULAR'")
    .select(F.col("e.nu_ec").alias("nu_ec"), F.col("e.nu_raiz").alias("nu_raiz"), F.col("a.id").alias("id_atributo"), F.col("a.tipo").alias("tp_atributo")))
ec_attr = ec_attr_direto.unionByName(ec_attr_titular).distinct().cache()

pares = (ec_attr.alias("x").join(ec_attr.alias("y"), "id_atributo")
    .filter("x.nu_ec < y.nu_ec")
    .groupBy(F.col("x.nu_ec").alias("ec_a"), F.col("y.nu_ec").alias("ec_b"))
    .agg(F.countDistinct("id_atributo").alias("qt_atributos"), F.array_sort(F.collect_set("x.tp_atributo")).alias("tipos"),
         F.max((F.col("x.nu_raiz") != F.col("y.nu_raiz")).cast("int")).alias("fl_raiz_diferente"))).cache()
display(pares.groupBy("fl_raiz_diferente").agg(F.count("*").alias("pares_de_ec"), F.avg("qt_atributos").alias("media_atributos_compartilhados")))

# COMMAND ----------

# MAGIC %md Pares de ECs **sem vínculo societário** (raízes diferentes) que compartilham 2+ atributos — forte indício de operador oculto:

# COMMAND ----------

nomes = ec.select(F.col("nu_ec"), "nm_ec", "nu_raiz_cnpj", "nm_ramo")
display(pares.filter("fl_raiz_diferente = 1 and qt_atributos >= 2").orderBy(F.desc("qt_atributos"))
        .join(nomes.toDF("ec_a", "nm_ec_a", "raiz_a", "ramo_a"), "ec_a").join(nomes.toDF("ec_b", "nm_ec_b", "raiz_b", "ramo_b"), "ec_b")
        .select("ec_a", "nm_ec_a", "raiz_a", "ec_b", "nm_ec_b", "raiz_b", "qt_atributos", "tipos").limit(30))

# COMMAND ----------

# MAGIC %md ## 3. `connectedComponents()` — clusters de identidade

# COMMAND ----------

# algorithm="graphx": não exige checkpoint dir (checkpoint de RDD não é suportado em UC Volumes)
cc = g_ident.connectedComponents(algorithm="graphx").select("id", "tipo", "nu_ec", "nu_raiz", "fl_lista_negativa", "component").cache()
taxa_fraude_ec = (crt.groupBy("nu_ec").agg(F.count("*").alias("qt_trns"))
    .join(frd.groupBy("nu_ec").agg(F.count("*").alias("qt_fraude")), "nu_ec", "left").fillna(0, ["qt_fraude"]))

comp = (cc.groupBy("component").agg(
        F.count("*").alias("qt_vertices"),
        F.sum((F.col("tipo") == "EC").cast("int")).alias("qt_ecs"),
        F.countDistinct(F.when(F.col("tipo") == "EC", F.col("nu_raiz"))).alias("qt_raizes_distintas"),
        F.sum(F.col("tipo").startswith("ENTIDADE").cast("int")).alias("qt_entidades"),
        F.sum(F.col("tipo").isin(ATRIBUTOS).cast("int")).alias("qt_atributos"),
        F.sum("fl_lista_negativa").alias("qt_ecs_lista_negativa"))
    .join(cc.filter("tipo = 'EC'").join(taxa_fraude_ec, "nu_ec", "left").groupBy("component")
            .agg(F.sum("qt_fraude").alias("qt_fraudes"), F.sum("qt_trns").alias("qt_trns_cartao")), "component", "left"))
display(comp.groupBy(F.when(F.col("qt_ecs") == 0, "0 ECs").when(F.col("qt_ecs") == 1, "1 EC").when(F.col("qt_ecs") <= 3, "2-3 ECs").otherwise("4+ ECs").alias("faixa"))
        .agg(F.count("*").alias("componentes"), F.sum("qt_ecs").alias("ecs"), F.sum("qt_ecs_lista_negativa").alias("ecs_lista_negativa")).orderBy("faixa"))

# COMMAND ----------

# MAGIC %md Componentes com **vários ECs de raízes diferentes** ranqueados por risco (score de alerta simples e explicável):

# COMMAND ----------

alerta = (comp.filter("qt_ecs >= 2 and qt_raizes_distintas >= 2")
    .withColumn("pct_lista_negativa", F.col("qt_ecs_lista_negativa") / F.col("qt_ecs"))
    .withColumn("taxa_fraude", F.col("qt_fraudes") / F.greatest(F.col("qt_trns_cartao"), F.lit(1)))
    .withColumn("score_alerta", F.round(F.log2(F.col("qt_raizes_distintas") + 1) * 2 + F.col("pct_lista_negativa") * 5
                                         + F.least(F.col("taxa_fraude") * 100, F.lit(5)) + F.log2(F.col("qt_entidades") + 1), 2))
    .orderBy(F.desc("score_alerta")))
salvar(alerta, "alerta_componente", "Componentes conexos do grafo de identidade com múltiplos ECs de raízes CNPJ distintas, ranqueados por risco")
display(spark.table(f"{GOLD}.alerta_componente").limit(20))

# COMMAND ----------

# MAGIC %md ### 3.1 Visualização do componente mais suspeito

# COMMAND ----------

import networkx as nx, matplotlib.pyplot as plt
top = spark.table(f"{GOLD}.alerta_componente").filter("qt_vertices <= 400").orderBy(F.desc("score_alerta")).first()["component"]
vs = cc.filter(F.col("component") == top).select("id", "tipo", "fl_lista_negativa").toPandas()
ids = set(vs.id)
es = g_ident.edges.filter(F.col("src").isin(list(ids)) & F.col("dst").isin(list(ids))).select("src", "dst", "relationship").toPandas()
G = nx.Graph(); G.add_nodes_from(vs.id); G.add_edges_from(zip(es.src, es.dst))
cores = {"EC": "#1f77b4", "ENTIDADE_PF": "#2ca02c", "ENTIDADE_PJ": "#17becf", "CONTA": "#ff7f0e", "DISPOSITIVO": "#9467bd",
         "IP": "#8c564b", "TELEFONE": "#e377c2", "EMAIL": "#7f7f7f", "ENDERECO": "#bcbd22", "CHAVE_PIX": "#aec7e8", "CARTAO": "#c5b0d5"}
tipo = dict(zip(vs.id, vs.tipo)); neg = dict(zip(vs.id, vs.fl_lista_negativa))
fig, ax = plt.subplots(figsize=(14, 10))
pos = nx.spring_layout(G, seed=7, k=0.6)
nx.draw_networkx_edges(G, pos, alpha=0.3, ax=ax)
nx.draw_networkx_nodes(G, pos, node_color=[cores.get(tipo[n], "#333") for n in G.nodes],
                       node_size=[500 if tipo[n] == "EC" else 120 for n in G.nodes],
                       edgecolors=["red" if neg[n] == 1 else "white" for n in G.nodes], linewidths=3, ax=ax)
nx.draw_networkx_labels(G, pos, {n: n.split(":")[0] if tipo[n] != "EC" else n for n in G.nodes if tipo[n] == "EC"}, font_size=8, ax=ax)
for t, c in cores.items():
    ax.scatter([], [], c=c, label=t)
ax.scatter([], [], facecolors="none", edgecolors="red", linewidths=3, s=200, label="EC em lista negativa")
ax.legend(loc="upper left", fontsize=8); ax.set_title(f"Componente {top} — ECs de raízes distintas ligados por atributos compartilhados"); ax.axis("off")
display(fig)

# COMMAND ----------

# MAGIC %md ## 4. `labelPropagation()` — comunidades (identidade + fluxo PIX)

# COMMAND ----------

E_risco = E_ident.unionByName(E.filter("relationship = 'PIX'"))
V_risco = V_ident.unionByName(V.join(E.filter("relationship = 'PIX'").select(F.col("src").alias("id")).union(E.filter("relationship = 'PIX'").select(F.col("dst").alias("id"))).distinct(), "id", "left_semi")).distinct()
g_risco = GraphFrame(V_risco.cache(), E_risco.cache())
lpa = g_risco.labelPropagation(maxIter=5).select("id", "tipo", "nu_ec", "fl_lista_negativa", F.col("label").alias("comunidade")).cache()
com = lpa.groupBy("comunidade").agg(F.count("*").alias("qt_vertices"), F.sum((F.col("tipo") == "EC").cast("int")).alias("qt_ecs"),
                                    F.sum("fl_lista_negativa").alias("qt_ecs_lista_negativa"))
display(com.filter("qt_ecs >= 2").orderBy(F.desc("qt_ecs_lista_negativa"), F.desc("qt_ecs")).limit(20))

# COMMAND ----------

# MAGIC %md ## 5. Centralidade no fluxo de dinheiro
# MAGIC ### 5.1 `pageRank()` no grafo de PIX entre entidades

# COMMAND ----------

g_pix = GraphFrame(V.filter(F.col("tipo").startswith("ENTIDADE")), E.filter("relationship = 'PIX'"))
pr = g_pix.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "rotulo", "tipo", F.col("pagerank"))
display(pr.orderBy(F.desc("pagerank")).limit(15))

# COMMAND ----------

# MAGIC %md ### 5.2 `parallelPersonalizedPageRank()` — proximidade de ECs em lista negativa
# MAGIC O "passeio aleatório" recomeça sempre em um EC já listado: vértices com PPR alto estão estruturalmente próximos de casos conhecidos
# MAGIC (*guilt by association*). Para evitar vazamento, a contribuição do **próprio** EC como fonte é removida.

# COMMAND ----------

fontes = [r.id for r in V_risco.filter("tipo = 'EC' and fl_lista_negativa = 1").select("id").collect()]
g_risco_sim = GraphFrame(g_risco.vertices, simetrico(E_risco).cache())
ppr = g_risco_sim.parallelPersonalizedPageRank(resetProbability=0.15, sourceIds=fontes, maxIter=10).vertices
idx_fonte = F.create_map(*[x for i, s in enumerate(fontes) for x in (F.lit(s), F.lit(i))])
ppr_ec = (ppr.filter("tipo = 'EC'").withColumn("arr", vector_to_array("pageranks"))
    .withColumn("i_self", idx_fonte[F.col("id")])
    .select("nu_ec", F.expr("aggregate(arr, 0D, (acc, x) -> acc + x)").alias("ppr_total"),
            F.when(F.col("i_self").isNotNull(), F.element_at("arr", F.col("i_self") + 1)).otherwise(F.lit(0.0)).alias("ppr_self"))
    .select("nu_ec", (F.col("ppr_total") - F.col("ppr_self")).alias("ppr_lista_negativa")))
display(ppr_ec.join(neg_ec, "nu_ec", "left").fillna(0, ["fl_lista_negativa"]).groupBy("fl_lista_negativa")
        .agg(F.expr("percentile(ppr_lista_negativa, array(0.5, 0.9, 0.99))").alias("p50_p90_p99"), F.count("*").alias("ecs")))

# COMMAND ----------

# MAGIC %md ## 6. Tipologias de fluxo PIX
# MAGIC ### 6.1 *Motif finding* — ciclos de 4 saltos `(a)->(b)->(c)->(d)->(a)` com valores altos (≥ R$ 10 mil)

# COMMAND ----------

E_pix_alto = E.filter("relationship = 'PIX' and vl_total >= 10000")
g_pix_alto = GraphFrame(V.filter(F.col("tipo").startswith("ENTIDADE")), E_pix_alto)
ciclos = (g_pix_alto.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)")
    .filter("a.id < b.id and a.id < c.id and a.id < d.id and a.id <> c.id and b.id <> d.id")
    .filter("e1.ts_min <= e2.ts_max and e2.ts_min <= e3.ts_max and e3.ts_min <= e4.ts_max")
    .select(F.array("a.id", "b.id", "c.id", "d.id").alias("entidades"), F.array("a.rotulo", "b.rotulo", "c.rotulo", "d.rotulo").alias("nomes"),
            F.array("e1.vl_total", "e2.vl_total", "e3.vl_total", "e4.vl_total").alias("valores"),
            F.least("e1.ts_min", "e2.ts_min", "e3.ts_min", "e4.ts_min").alias("ts_inicio"),
            F.greatest("e1.ts_max", "e2.ts_max", "e3.ts_max", "e4.ts_max").alias("ts_fim")))
ciclos3 = (g_pix_alto.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
    .filter("a.id < b.id and a.id < c.id")
    .select(F.array("a.id", "b.id", "c.id").alias("entidades"), F.array("a.rotulo", "b.rotulo", "c.rotulo").alias("nomes"),
            F.array("e1.vl_total", "e2.vl_total", "e3.vl_total").alias("valores"),
            F.least("e1.ts_min", "e2.ts_min", "e3.ts_min").alias("ts_inicio"), F.greatest("e1.ts_max", "e2.ts_max", "e3.ts_max").alias("ts_fim")))
todos_ciclos = ciclos.withColumn("qt_saltos", F.lit(4)).unionByName(ciclos3.withColumn("qt_saltos", F.lit(3))) \
    .withColumn("vl_min_ciclo", F.array_min("valores")).withColumn("id_ciclo", F.sha2(F.concat_ws("|", "entidades"), 256).substr(1, 12))
salvar(todos_ciclos, "pix_ciclos", "Ciclos de PIX de alto valor detectados por motif finding (dinheiro que retorna à origem)")
display(spark.table(f"{GOLD}.pix_ciclos").orderBy(F.desc("vl_min_ciclo")).limit(20))

# COMMAND ----------

# MAGIC %md ### 6.2 `aggregateMessages` — entrada x saída por entidade (*pass-through* / conta de passagem)

# COMMAND ----------

fluxo = g_pix.aggregateMessages(
    F.sum(AM.msg).alias("vl_entrada"), sendToDst=AM.edge["vl_total"]).join(
    g_pix.aggregateMessages(F.sum(AM.msg).alias("vl_saida"), sendToSrc=AM.edge["vl_total"]), "id", "full").fillna(0)
fluxo = (fluxo.join(g_pix.inDegrees, "id", "left").join(g_pix.outDegrees, "id", "left").fillna(0)
    .withColumn("pct_repasse", F.when((F.col("vl_entrada") > 0) & (F.col("vl_saida") > 0),
                                      F.least("vl_entrada", "vl_saida") / F.greatest("vl_entrada", "vl_saida")).otherwise(0.0)))
display(fluxo.filter("inDegree >= 10 and pct_repasse > 0.7").join(V.select("id", "rotulo", "tipo"), "id").orderBy(F.desc("vl_entrada")).limit(20))

# COMMAND ----------

# MAGIC %md ## 7. `triangleCount()` no grafo EC–EC de compartilhamento

# COMMAND ----------

g_ec = GraphFrame(V.filter("tipo = 'EC'").select("id", "nu_ec"),
                  pares.select(F.concat(F.lit("EC:"), "ec_a").alias("src"), F.concat(F.lit("EC:"), "ec_b").alias("dst"), "qt_atributos"))
tri = g_ec.triangleCount().select("nu_ec", F.col("count").alias("qt_triangulos"))
display(tri.join(neg_ec, "nu_ec", "left").fillna(0, ["fl_lista_negativa"]).groupBy("fl_lista_negativa")
        .agg(F.avg("qt_triangulos").alias("media_triangulos"), F.sum((F.col("qt_triangulos") > 0).cast("int")).alias("ecs_em_triangulo"), F.count("*").alias("ecs")))

# COMMAND ----------

# MAGIC %md ## 8. `shortestPaths()` e `bfs()` — distância até a lista negativa

# COMMAND ----------

sp = g_ident_sim.shortestPaths(landmarks=fontes)
dist_ec = (sp.filter("tipo = 'EC'")
    .select("nu_ec", "id", F.map_filter("distances", lambda k, v: k != F.col("id")).alias("d"))
    .select("nu_ec", F.array_min(F.map_values("d")).alias("dist_min_lista_negativa")))
display(dist_ec.join(neg_ec, "nu_ec", "left").fillna(0, ["fl_lista_negativa"])
        .groupBy("fl_lista_negativa", F.coalesce(F.col("dist_min_lista_negativa").cast("string"), F.lit("sem caminho")).alias("distancia")).count()
        .orderBy("fl_lista_negativa", "distancia"))

# COMMAND ----------

# MAGIC %md Caminho (`bfs`) de um EC **fora da lista** até o EC listado mais próximo — a evidência que o analista de PLD leva para o dossiê:

# COMMAND ----------

alvo = dist_ec.join(neg_ec, "nu_ec", "left_anti").filter("dist_min_lista_negativa between 2 and 6").orderBy("dist_min_lista_negativa").first()
if alvo:
    caminho = g_ident_sim.bfs(fromExpr=f"id = 'EC:{alvo.nu_ec}'", toExpr="tipo = 'EC' and fl_lista_negativa = 1", maxPathLength=6)
    display(caminho.limit(3))

# COMMAND ----------

# MAGIC %md ## 9. Features de grafo por EC → `gold.ec_features_grafo`
# MAGIC Dois grupos de features:
# MAGIC * **Estruturais** (não usam rótulos): grau, atributos compartilhados, tamanho do componente, triângulos, PageRank, ciclos, *pass-through*.
# MAGIC * **Risco de vizinhança** (usam rótulos de *outros* ECs, sem o próprio): % de ECs listados no componente, PPR da lista negativa, distância mínima e taxa de fraude da vizinhança.

# COMMAND ----------

ec_ids = ec.select("nu_ec", "id_entidade_titular").withColumn("id", F.concat(F.lit("EC:"), F.col("nu_ec"))) \
           .withColumn("id_ent", F.concat(F.lit("ENT:"), F.col("id_entidade_titular")))
grau_ec = g.degrees.withColumnRenamed("degree", "grau_total")
grau_ident = g_ident.degrees.withColumnRenamed("degree", "grau_identidade")
compart = (pares.select(F.col("ec_a").alias("nu_ec"), "ec_b", "fl_raiz_diferente", "qt_atributos")
           .unionByName(pares.select(F.col("ec_b").alias("nu_ec"), F.col("ec_a").alias("ec_b"), "fl_raiz_diferente", "qt_atributos"))
           .groupBy("nu_ec").agg(F.countDistinct("ec_b").alias("qt_ecs_compartilham_atributo"),
                                 F.sum("fl_raiz_diferente").alias("qt_ecs_raiz_diferente_compartilham"),
                                 F.max("qt_atributos").alias("max_atributos_compartilhados_par")))
qt_attr_comp = ec_attr.groupBy("nu_ec").agg(F.countDistinct("id_atributo").alias("qt_atributos_compartilhados"),
                                             F.countDistinct(F.when(F.col("tp_atributo") == "CONTA", F.col("id_atributo"))).alias("qt_contas_compartilhadas"),
                                             F.countDistinct(F.when(F.col("tp_atributo") == "DISPOSITIVO", F.col("id_atributo"))).alias("qt_dispositivos_compartilhados"))
cc_ec = (cc.filter("tipo = 'EC'").select("nu_ec", "component", "fl_lista_negativa")
    .join(comp.select("component", "qt_ecs", "qt_raizes_distintas", "qt_entidades", "qt_ecs_lista_negativa", "qt_fraudes", "qt_trns_cartao"), "component")
    .join(taxa_fraude_ec, "nu_ec", "left").fillna(0, ["qt_fraude", "qt_trns"])
    .select("nu_ec", "component", F.col("qt_ecs").alias("qt_ecs_componente"), F.col("qt_raizes_distintas").alias("qt_raizes_componente"),
            F.col("qt_entidades").alias("qt_entidades_componente"),
            F.when(F.col("qt_ecs") > 1, (F.col("qt_ecs_lista_negativa") - F.col("fl_lista_negativa")) / (F.col("qt_ecs") - 1)).otherwise(0.0).alias("pct_lista_negativa_componente"),
            F.when(F.col("qt_trns_cartao") - F.col("qt_trns") > 0, (F.col("qt_fraudes") - F.col("qt_fraude")) / (F.col("qt_trns_cartao") - F.col("qt_trns"))).otherwise(0.0).alias("taxa_fraude_componente")))
com_ec = lpa.filter("tipo = 'EC'").select("nu_ec", "comunidade", "fl_lista_negativa").join(com, "comunidade") \
    .select("nu_ec", F.col("qt_vertices").alias("qt_vertices_comunidade"), F.col("qt_ecs").alias("qt_ecs_comunidade"),
            F.when(F.col("qt_ecs") > 1, (F.col("qt_ecs_lista_negativa") - F.col("fl_lista_negativa")) / (F.col("qt_ecs") - 1)).otherwise(0.0).alias("pct_lista_negativa_comunidade"))
pr_tit = pr.select(F.col("id").alias("id_ent"), F.col("pagerank").alias("pagerank_pix_titular"))
fluxo_tit = fluxo.select(F.col("id").alias("id_ent"), F.col("inDegree").alias("qt_pagadores_pix_titular"), F.col("outDegree").alias("qt_destinatarios_pix_titular"),
                         "pct_repasse", F.col("vl_entrada").alias("vl_pix_entrada_titular"), F.col("vl_saida").alias("vl_pix_saida_titular"))
ciclos_ent = spark.table(f"{GOLD}.pix_ciclos").select(F.explode("entidades").alias("id_ent"), "id_ciclo").groupBy("id_ent").agg(F.countDistinct("id_ciclo").alias("qt_ciclos_pix"))

feat = (ec_ids.join(grau_ec, "id", "left").join(grau_ident, "id", "left")
    .join(compart, "nu_ec", "left").join(qt_attr_comp, "nu_ec", "left").join(cc_ec, "nu_ec", "left").join(com_ec, "nu_ec", "left")
    .join(tri, "nu_ec", "left").join(pr_tit, "id_ent", "left").join(fluxo_tit, "id_ent", "left").join(ciclos_ent, "id_ent", "left")
    .join(ppr_ec, "nu_ec", "left").join(dist_ec, "nu_ec", "left")
    .withColumn("dist_min_lista_negativa", F.coalesce("dist_min_lista_negativa", F.lit(99)))
    .drop("id", "id_ent", "id_entidade_titular")
    .fillna(0)
    .withColumn("dt_referencia", F.current_date()))
salvar(feat, "ec_features_grafo", "Features de grafo por EC (estruturais e de risco de vizinhança) para o modelo de risco PLD")
display(spark.table(f"{GOLD}.ec_features_grafo").join(neg_ec, "nu_ec", "left").fillna(0, ["fl_lista_negativa"]).groupBy("fl_lista_negativa")
        .agg(*[F.round(F.avg(c), 3).alias(c) for c in ["qt_atributos_compartilhados", "qt_ecs_raiz_diferente_compartilham", "qt_ecs_componente",
                                                       "qt_triangulos", "qt_ciclos_pix", "pct_repasse", "pct_lista_negativa_componente", "ppr_lista_negativa"]]))
