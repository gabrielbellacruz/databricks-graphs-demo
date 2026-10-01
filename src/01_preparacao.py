# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Preparando os Dados
# MAGIC **Workshop: Utilizando grafos para PLD** · Parte 1 de 3
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | ⏱️ Duração | ~40 min |
# MAGIC | 🎯 Objetivo | Sair de dados brutos e inconsistentes (Bronze) para um **grafo pronto para análise** (Gold) |
# MAGIC | 📥 Entrada | `cielo_pld.bronze` — réplica sintética do HUB de risco (`tbciar_re_hub_dado_risc_cred`) e das tabelas Lynx (`tbciar_tr_*`) |
# MAGIC | 📤 Saída | `cielo_pld.silver.*` (dados padronizados) e `cielo_pld.gold.grafo_vertices` / `grafo_arestas` |
# MAGIC
# MAGIC ### Roteiro
# MAGIC 1. **EDA** — o que tem nas fontes e por que a visão tabular não basta
# MAGIC 2. **Padronização** de documentos, telefones e endereços (o "gargalo" do KYC)
# MAGIC 3. **Construção de vértices e arestas** no formato do GraphFrames
# MAGIC
# MAGIC > 💡 Durante o notebook você encontrará blocos **✏️ Exercício** (opcionais) seguidos da **✅ Solução**. O notebook roda inteiro com *Run all*.

# COMMAND ----------

# MAGIC %md ## 0. Configuração

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
import pandas as pd, re

CATALOGO = "cielo_pld"
BRONZE, SILVER, GOLD = f"{CATALOGO}.bronze", f"{CATALOGO}.silver", f"{CATALOGO}.gold"
spark.sql(f"USE CATALOG {CATALOGO}")
for s in (SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

def bronze(t): return spark.table(f"{BRONZE}.{t}")

def salvar(df, tabela, comentario):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")
    print(f"✔ {tabela}: {spark.table(tabela).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. EDA — Análise exploratória da Bronze
# MAGIC ### 1.1 As fontes
# MAGIC | Fonte | Tabela | O que traz para PLD |
# MAGIC |---|---|---|
# MAGIC | HUB de risco (RDC) | `tbciar_re_hub_dado_risc_cred` | Cadastro mensal do EC: documento, endereço, faturamento, chargeback, sócio PEP, alteração societária |
# MAGIC | Lynx · Cartões | `tbciar_tr_lynx_trns_crto` | Transações com dados do portador (CPF, telefone, e-mail, endereço de entrega) |
# MAGIC | Lynx · PIX | `tbciar_tr_lynx_trns_pix` | Pagador × recebedor, contas, chaves PIX, dispositivo |
# MAGIC | Lynx · RAD0 | `tbciar_tr_lynx_trns_antp_rcbv` | Antecipações: conta de domicílio e responsável pela maquininha (sócio) |
# MAGIC | Lynx · Fraude / Listas | `tbciar_tr_frde`, `tbciar_tr_lynx_lsta_*` | Fraudes reportadas e listas positivas/negativas |

# COMMAND ----------

tabelas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {BRONZE}").collect()]
display(spark.createDataFrame([(t, bronze(t).count(), len(bronze(t).columns)) for t in tabelas], "tabela string, linhas long, colunas int")
        .orderBy(F.desc("linhas")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Uma loja, várias grafias
# MAGIC O HUB tem um snapshot por mês. Veja como **o mesmo EC** aparece com documento e endereço escritos de formas diferentes ao longo dos meses —
# MAGIC e cada sistema (cartões, PIX, RAD0) tem o seu próprio formato.

# COMMAND ----------

hub_b = bronze("tbciar_re_hub_dado_risc_cred")
variacoes = (hub_b.groupBy("NU_EC").agg(F.countDistinct("NU_DCMT").alias("grafias_documento"), F.countDistinct("DC_ENDR").alias("grafias_endereco"))
                  .orderBy(F.desc("grafias_endereco"), F.desc("grafias_documento")))
exemplo = variacoes.first()["NU_EC"]
display(hub_b.filter(F.col("NU_EC") == exemplo).select("CD_MES", "NU_EC", "NM_EC", "NU_DCMT", "DC_ENDR", "NM_UF").orderBy("CD_MES"))

# COMMAND ----------

# MAGIC %md ### 1.3 Documentos (CPF/CNPJ): um campo, muitos formatos

# COMMAND ----------

def padrao_doc(c):
    return (F.when(F.col(c).isNull(), "nulo")
             .when(F.col(c).rlike(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"), "CPF com máscara")
             .when(F.col(c).rlike(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$"), "CNPJ com máscara")
             .when(F.col(c).rlike(r"^\d{11}$|^\d{14}$"), "só dígitos (11/14)")
             .when(F.col(c).rlike(r"^\d+$"), "só dígitos, zeros à esquerda perdidos")
             .otherwise("outro (espaços etc.)"))

fontes_doc = [("HUB · NU_DCMT", hub_b, "NU_DCMT"),
              ("PIX · pagador", bronze("tbciar_tr_lynx_trns_pix"), "nu_pix_cpf_cnpj_pgdr"),
              ("PIX · recebedor", bronze("tbciar_tr_lynx_trns_pix"), "nu_cpf_cnpj_recr"),
              ("Cartão · portador PLD", bronze("tbciar_tr_lynx_trns_crto"), "nu_oprc_cpf_ptdr_pld"),
              ("RAD0 · responsável maquininha", bronze("tbciar_tr_lynx_trns_antp_rcbv"), "nu_cnpj_cpf_mqnt")]
pad = None
for nome, df, c in fontes_doc:
    d = df.select(F.lit(nome).alias("fonte"), padrao_doc(c).alias("formato"))
    pad = d if pad is None else pad.unionByName(d)
display(pad.groupBy("fonte").pivot("formato").count().fillna(0))

# COMMAND ----------

# MAGIC %md ### 1.4 Telefones e endereços

# COMMAND ----------

crto_b = bronze("tbciar_tr_lynx_trns_crto")
display(crto_b.filter("nu_celr is not null").select("nu_ddd", "nu_celr", "nu_celr_ptdr_ecommerce").limit(10))

# COMMAND ----------

display(hub_b.select(F.split("DC_ENDR", " ")[0].alias("tipo_logradouro")).groupBy("tipo_logradouro").count().orderBy(F.desc("count")).limit(20))

# COMMAND ----------

# MAGIC %md ### 1.5 Duplicatas de arquivos reprocessados

# COMMAND ----------

display(crto_b.groupBy("id_unco").count().filter("count > 1").agg(F.count("*").alias("transacoes_duplicadas"))
        .crossJoin(crto_b.filter(F.col("nm_arqv_orgm").contains("REPROC")).agg(F.count("*").alias("linhas_de_arquivos_REPROC"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Primeiro sinal de PLD: fracionamento (*structuring*)
# MAGIC Concentração anormal de transações logo **abaixo de R$ 5 mil e R$ 10 mil** — limites que costumam disparar controles.

# COMMAND ----------

display(crto_b.filter("vl_trns between 3000 and 11000")
        .select((F.floor(F.col("vl_trns") / 250) * 250).alias("faixa_valor")).groupBy("faixa_valor").count().orderBy("faixa_valor"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 1
# MAGIC Quantos ECs aparecem com **mais de uma grafia de endereço** no HUB? E com mais de uma grafia de documento?
# MAGIC *Dica: use o DataFrame `variacoes` criado acima.*

# COMMAND ----------

# ✏️ seu código aqui
# variacoes.filter(...).count()

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

display(variacoes.agg(F.sum((F.col("grafias_endereco") > 1).cast("int")).alias("ecs_com_2+_enderecos"),
                      F.sum((F.col("grafias_documento") > 1).cast("int")).alias("ecs_com_2+_documentos"),
                      F.count("*").alias("total_ecs")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Padronização de documentos, telefones e endereços
# MAGIC > *"Inconsistências no cadastro comprometem toda a cadeia analítica"* — se o mesmo CPF vira dois valores diferentes, o grafo cria **dois nós**
# MAGIC > e o elo entre o laranja e o controlador se perde.
# MAGIC
# MAGIC Regras que vamos aplicar em **todas** as fontes:
# MAGIC
# MAGIC | Campo | Regra | Exemplo |
# MAGIC |---|---|---|
# MAGIC | CPF/CNPJ | só dígitos → completa zeros → decide CPF×CNPJ pelo **dígito verificador** | `1.222.333/0001-81` → `01222333000181` |
# MAGIC | Telefone | só dígitos → remove `55` → adiciona DDD → formato E.164 | `(11) 98812-4471` → `+5511988124471` |
# MAGIC | Endereço | maiúsculas, sem acento, sem CEP/`Nº`/pontuação, abreviações expandidas | `R. Augusta, Nº 1200` → `RUA AUGUSTA 1200` |
# MAGIC | Nome / e-mail | maiúsculas sem acento / minúsculas sem espaços | `  Maria  Conceição ` → `MARIA CONCEICAO` |
# MAGIC
# MAGIC Documentos que **continuam inválidos** (dígito trocado) não são descartados: eles serão resolvidos no notebook 02 com **Splink**.

# COMMAND ----------

# MAGIC %md ### 2.1 Documentos — CPF/CNPJ com validação do dígito verificador

# COMMAND ----------

def dv_valido(d):
    if len(d) == 11:
        if d == d[0] * 11: return False
        r = sum(int(d[i]) * (10 - i) for i in range(9)) % 11; v1 = 0 if r < 2 else 11 - r
        r = sum(int(d[i]) * (11 - i) for i in range(10)) % 11; v2 = 0 if r < 2 else 11 - r
        return d[9:] == f"{v1}{v2}"
    if len(d) == 14:
        p1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]; p2 = [6] + p1
        r = sum(int(a) * b for a, b in zip(d[:12], p1)) % 11; v1 = 0 if r < 2 else 11 - r
        r = sum(int(a) * b for a, b in zip(d[:13], p2)) % 11; v2 = 0 if r < 2 else 11 - r
        return d[12:] == f"{v1}{v2}"
    return False

@F.pandas_udf("struct<doc:string, tp_pessoa:string, valido:boolean>")
def padroniza_documento(s: pd.Series) -> pd.DataFrame:
    out = []
    for x in s:
        d = re.sub(r"\D", "", str(x)) if x is not None and not pd.isna(x) else ""
        if not d:
            out.append((None, None, None)); continue
        candidatos = [d.zfill(14)] if len(d) > 11 else [d.zfill(11), d.zfill(14)]
        ok = next((c for c in candidatos if dv_valido(c)), None)
        doc = ok or candidatos[0]
        out.append((doc, "PF" if len(doc) == 11 else "PJ", ok is not None))
    return pd.DataFrame(out, columns=["doc", "tp_pessoa", "valido"])

testes_doc = spark.createDataFrame([("123.456.789-09",), ("12345678909",), ("11.222.333/0001-81",), ("1222333000181",), (" 12345678900 ",)], "bruto string")
display(testes_doc.select("bruto", padroniza_documento("bruto").alias("p")).select("bruto", "p.*"))

# COMMAND ----------

# MAGIC %md ### 2.2 Telefones — formato E.164

# COMMAND ----------

def padroniza_telefone(col, ddd=None):
    d = F.regexp_replace(col, r"\D", "")
    d = F.when((F.length(d) >= 12) & d.startswith("55"), F.substring(d, 3, 20)).otherwise(d)
    if ddd is not None:
        d = F.when(F.length(d).isin(8, 9), F.concat(F.regexp_replace(ddd, r"\D", ""), d)).otherwise(d)
    return F.when(F.length(d).isin(10, 11), F.concat(F.lit("+55"), d))

testes_tel = spark.createDataFrame([("(11) 98812-4471", None), ("5511988124471", None), ("988124471", "11"), ("+55 21 3123-4567", None), ("123", None)], "bruto string, ddd string")
display(testes_tel.select("bruto", "ddd", padroniza_telefone(F.col("bruto"), F.col("ddd")).alias("telefone_e164")))

# COMMAND ----------

# MAGIC %md ### 2.3 Endereços

# COMMAND ----------

_ACENTOS, _SEM = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ", "AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn"
TIPOS_LOGRADOURO = {"R": "RUA", "AV": "AVENIDA", "TV": "TRAVESSA", "AL": "ALAMEDA", "PCA": "PRACA", "ROD": "RODOVIA",
                    "EST": "ESTRADA", "LAD": "LADEIRA", "VD": "VIADUTO", "PQ": "PARQUE", "JD": "JARDIM", "LGO": "LARGO"}

def sem_acento_maiusculo(col):
    return F.upper(F.translate(col, _ACENTOS, _SEM))

def padroniza_endereco(col):
    e = sem_acento_maiusculo(col)
    e = F.regexp_replace(e, r"\s*-?\s*CEP\s*[\d.\-]+", "")          # remove CEP
    e = F.regexp_replace(e, r"\bN\s*[º°O]?\.?\s*(?=\d)", "")          # remove "Nº" / "N." antes do número
    e = F.regexp_replace(e, r"[^A-Z0-9 ]", " ")                       # pontuação
    e = F.trim(F.regexp_replace(e, r"\s+", " "))
    for abrev, completo in TIPOS_LOGRADOURO.items():                  # abreviação só no início (tipo de logradouro)
        e = F.regexp_replace(e, rf"^{abrev}\b", completo)
    return F.when(e != "", e)

def padroniza_nome(col):
    e = F.regexp_replace(sem_acento_maiusculo(col), r"[^A-Z0-9 ]", " ")
    return F.when(F.trim(e) != "", F.trim(F.regexp_replace(e, r"\s+", " ")))

def padroniza_email(col):
    e = F.lower(F.trim(col))
    return F.when(e.rlike(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"), e)

testes_end = spark.createDataFrame([("R. Augusta, Nº 1200",), ("RUA AUGUSTA 1200",), ("Av. Brasil, 455 - CEP 01310-100",), ("avenida brasil, 455",), ("Pça da Sé, 1",)], "bruto string")
display(testes_end.select("bruto", padroniza_endereco(F.col("bruto")).alias("endereco_padronizado")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 2
# MAGIC O endereço `"Tv. das Flores, 10"` vira `"TRAVESSA DAS FLORES 10"`? E `"Rod. Anhanguera km 20"`?
# MAGIC Teste a função `padroniza_endereco` com os seus próprios exemplos — e, se quiser, acrescente uma abreviação nova em `TIPOS_LOGRADOURO`.

# COMMAND ----------

# ✏️ seu código aqui
# display(spark.createDataFrame([("Tv. das Flores, 10",)], "bruto string").select(padroniza_endereco(F.col("bruto"))))

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

display(spark.createDataFrame([("Tv. das Flores, 10",), ("Rod. Anhanguera km 20",), ("Jd. Paulista, Nº 7",)], "bruto string")
        .select("bruto", padroniza_endereco(F.col("bruto")).alias("padronizado")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Aplicando a padronização nas fontes → Silver
# MAGIC Além de padronizar, removemos **duplicatas de reprocessamento** (fica a carga mais recente) e **transações de teste**.

# COMMAND ----------

def dedup(df, chave, ordem="dt_crga"):
    w = W.partitionBy(*([chave] if isinstance(chave, str) else chave)).orderBy(F.desc(ordem))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def conta(ispb, agencia, numero):
    """Chave única de conta bancária: ISPB|agência|conta (sem zeros à esquerda)."""
    return F.concat_ws("|", F.col(ispb), F.col(agencia).cast("int").cast("string"), F.col(numero).cast("bigint").cast("string"))

def para_timestamp(data, hora):
    d = F.coalesce(F.try_to_timestamp(F.col(data).cast("string"), F.lit("yyyyMMdd")), F.try_to_timestamp(F.col(data).cast("string"), F.lit("yyyy-MM-dd")))
    h = F.lpad(F.regexp_replace(F.col(hora).cast("string"), ":", ""), 6, "0")
    return d + F.make_dt_interval(F.lit(0), h.substr(1, 2).cast("int"), h.substr(3, 2).cast("int"), h.substr(5, 2).cast("int"))

def tipo_chave(col):
    return (F.when(col.startswith("+55"), "TELEFONE").when(col.contains("@"), "EMAIL")
             .when(col.rlike(r"^[0-9a-f]{32}$"), "EVP").when(col.rlike(r"^\d{11}$|^\d{14}$"), "DOCUMENTO"))

# COMMAND ----------

# MAGIC %md **HUB de risco (mensal) e cadastro da loja (último snapshot)**

# COMMAND ----------

hub_s = (dedup(hub_b, ["CD_MES", "NU_EC"], "DT_CRGA")
    .withColumn("_doc", padroniza_documento("NU_DCMT"))
    .withColumn("NU_DCMT_PADRAO", F.col("_doc.doc")).withColumn("TP_PESSOA", F.col("_doc.tp_pessoa")).withColumn("FL_DOC_VALIDO", F.col("_doc.valido"))
    .withColumn("NM_UF", F.coalesce(F.element_at(F.create_map(*[F.lit(x) for kv in {"SAO PAULO": "SP", "RIO DE JANEIRO": "RJ", "MINAS GERAIS": "MG",
        "PARANA": "PR", "RIO GRANDE DO SUL": "RS", "BAHIA": "BA", "PERNAMBUCO": "PE", "CEARA": "CE", "DISTRITO FEDERAL": "DF", "GOIAS": "GO",
        "SANTA CATARINA": "SC", "AMAZONAS": "AM", "PARA": "PA"}.items() for x in kv]), F.upper(F.trim("NM_UF"))), F.upper(F.trim("NM_UF"))))
    .withColumn("DC_ENDR_PADRAO", padroniza_endereco(F.expr("regexp_replace(DC_ENDR, concat(',\\\\s*', NM_MNCP, '.*$'), '')")))
    .withColumn("VL_FTRM", F.when(F.col("VL_FTRM") >= 0, F.col("VL_FTRM")))
    .drop("_doc"))
salvar(hub_s, f"{SILVER}.hub_risco_mensal", "HUB de risco mensal deduplicado, com documento, UF e endereço padronizados")

# COMMAND ----------

LISTAS = {f"tbciar_tr_lynx_lsta_{t}_{o}": (t, o) for t in ["psit", "ngto", "psit_atzd"] for o in ["crto", "clnt", "ec"]}
lst = None
for nome, (t, o) in LISTAS.items():
    d = (dedup(bronze(nome), ["id_objt_prcp", "dt_etra_lsta"])
         .select(F.lit({"psit": "POSITIVA", "ngto": "NEGATIVA", "psit_atzd": "POSITIVA AUTORIZADA"}[t]).alias("tp_lista"),
                 F.lit({"crto": "CARTAO", "clnt": "CLIENTE", "ec": "EC"}[o]).alias("tp_objeto"),
                 F.when(F.lit(o) == "clnt", F.lpad(F.col("id_objt_prcp").cast("string"), 11, "0")).otherwise(F.col("id_objt_prcp").cast("string")).alias("cd_objeto"),
                 F.coalesce(F.try_to_timestamp("dt_etra_lsta", F.lit("yyyy-MM-dd HH:mm:ss")), F.try_to_timestamp("dt_etra_lsta", F.lit("dd/MM/yyyy HH:mm"))).alias("ts_entrada"),
                 F.to_timestamp("dt_sada_lsta").alias("ts_saida")))
    lst = d if lst is None else lst.unionByName(d)
lst = lst.withColumn("fl_ativo", F.col("ts_saida").isNull() | (F.col("ts_saida") > F.current_timestamp()))
salvar(lst, f"{SILVER}.lista_monitoramento", "União das 9 listas Lynx (positiva/negativa/positiva autorizada x cartão/cliente/EC) com status ativo")

lojas_fraude = (spark.table(f"{SILVER}.lista_monitoramento").filter("tp_lista = 'NEGATIVA' and tp_objeto = 'EC' and fl_ativo")
                .select(F.col("cd_objeto").cast("bigint").alias("nu_ec")).distinct().withColumn("fl_fraude", F.lit(1)))

hub_hist = spark.table(f"{SILVER}.hub_risco_mensal")
loja = (hub_hist.withColumn("_rn", F.row_number().over(W.partitionBy("NU_EC").orderBy(F.desc("CD_MES")))).filter("_rn = 1")
    .select(F.col("NU_EC").alias("nu_ec"), padroniza_nome("NM_EC").alias("nm_loja"), padroniza_nome("NM_CLNT").alias("nm_razao_social"),
            F.col("NU_DCMT_PADRAO").alias("nu_doc"), F.col("TP_PESSOA").alias("tp_pessoa"), F.col("FL_DOC_VALIDO").alias("fl_doc_valido"),
            F.coalesce(F.when(F.col("TP_PESSOA") == "PJ", F.substring("NU_DCMT_PADRAO", 1, 8)), F.col("NU_DCMT_PADRAO")).alias("nu_raiz"),
            F.col("TIPO_CLNT").alias("tp_cliente"), F.col("NM_UF").alias("sg_uf"), F.col("NM_MNCP").alias("nm_municipio"),
            F.col("DC_ENDR_PADRAO").alias("ds_endereco"), F.col("CD_RMAT").alias("cd_ramo"), F.col("NM_RMAT").alias("nm_ramo"),
            F.col("QT_DIAS_AFLC").alias("qt_dias_afiliacao"), F.coalesce("VL_FTRM", F.lit(0.0)).alias("vl_faturamento"),
            F.coalesce("PC_CHRG", F.lit(0.0)).alias("pc_chargeback"), F.col("IN_SOCO_PEP").alias("in_socio_pep"),
            F.col("IN_ALTR_QDRO_SCTR").alias("in_alteracao_societaria"))
    .join(lojas_fraude, "nu_ec", "left").fillna(0, ["fl_fraude"]))
salvar(loja, f"{SILVER}.loja", "Cadastro padronizado da loja (EC) no último snapshot do HUB + flag de fraude confirmada (lista negativa ativa)")

# COMMAND ----------

# MAGIC %md **Transações de cartão, PIX e antecipações (RAD0)**

# COMMAND ----------

cartao = (dedup(crto_b, "id_unco")
    .filter("coalesce(vl_tste, 0) = 0 and upper(coalesce(nm_estn, '')) not like '%TESTE%' and vl_trns > 0.01")
    .withColumn("_doc", padroniza_documento(F.coalesce("nu_oprc_cpf_ptdr_pld", "nu_cpf_ptdr_ecommerce")))
    .select("id_unco", "qt_cnar_ordm", para_timestamp("dt_oprc", "hr_oprc").alias("ts_transacao"), F.col("id_estn").alias("nu_ec"),
            "vl_trns", (F.col("in_acto") == "1").alias("fl_aprovada"), (F.col("in_etnc") == "1").alias("fl_cnp"),
            F.col("cd_tipo_crto").alias("tp_cartao"), F.col("nu_crto_tken").alias("nu_token_cartao"), "nu_score_totl",
            F.col("id_rgra_pld").isNotNull().alias("fl_regra_pld"), F.col("cd_pais_crto"),
            F.col("_doc.doc").alias("nu_doc_portador"),
            padroniza_nome(F.coalesce("tx_oprc_nm_ptdr_pld", "nm_ptdr_crto_ecommerce", "nm_ptdr_crto_chip")).alias("nm_portador"),
            F.coalesce(padroniza_telefone(F.col("nu_celr_ptdr_ecommerce")), padroniza_telefone(F.col("nu_celr"), F.col("nu_ddd"))).alias("nu_telefone_portador"),
            padroniza_email(F.col("tx_emal_ptdr_ecommerce")).alias("ds_email_portador"),
            padroniza_endereco(F.col("tx_endr_ptdr_ecommerce")).alias("ds_endereco_portador"),
            padroniza_endereco(F.col("tx_endr_enta_ecommerce")).alias("ds_endereco_entrega"))
    .withColumn("fl_madrugada", F.hour("ts_transacao").between(0, 4))
    .withColumn("fl_abaixo_limite", F.col("vl_trns").between(9000, 9999.99) | F.col("vl_trns").between(4500, 4999.99))
    .withColumn("fl_valor_redondo", (F.col("vl_trns") >= 500) & (F.col("vl_trns") % 500 == 0))
    .withColumn("fl_pre_pago", F.col("tp_cartao") == "PRE-PAGO"))
salvar(cartao, f"{SILVER}.trns_cartao", "Transações Lynx de cartão: deduplicadas, sem testes, com portador padronizado")

pix_b = bronze("tbciar_tr_lynx_trns_pix")
pix = (dedup(pix_b, "id_unco_pix")
    .withColumn("_pag", padroniza_documento("nu_pix_cpf_cnpj_pgdr")).withColumn("_rec", padroniza_documento("nu_cpf_cnpj_recr"))
    .select("id_unco_pix", para_timestamp("dt_oprc", "hr_oprc").alias("ts_transacao"), F.col("id_estn").alias("nu_ec"),
            F.col("cd_tipo_oprc_pix").alias("tp_operacao"), "vl_trns", (F.col("in_acto") == "1").alias("fl_aprovada"),
            F.col("_pag.doc").alias("nu_doc_pagador"), padroniza_nome("tx_pix_nm_pgdr").alias("nm_pagador"),
            conta("nu_pix_ispb_pgdr", "nu_pix_agnc_pgdr", "nu_pix_cnta_pgdr").alias("ds_conta_pagador"),
            F.col("tx_chve_pix_pgdr").alias("ds_chave_pagador"), padroniza_email(F.col("tx_usro_pix")).alias("ds_email_pagador"),
            F.col("_rec.doc").alias("nu_doc_recebedor"), padroniza_nome("nm_recr").alias("nm_recebedor"),
            conta("nu_ispb_recr", "nu_agnc_recr", "nu_cnta_recr").alias("ds_conta_recebedor"), F.col("tx_chve_pix_recr").alias("ds_chave_recebedor"),
            F.col("id_dspi").alias("id_dispositivo"),
            F.datediff(F.to_date(F.col("dt_oprc").cast("string"), "yyyyMMdd"), F.to_date("dt_crca_cnta", "yyyyMMdd")).alias("qt_dias_conta"),
            (F.col("qt_d90_stooge_acc_mrca_frde_us") + F.col("qt_d90_frde_acc_mrca_frde_us")).alias("qt_marcacoes_laranja_90d"))
    .withColumn("tp_chave_pagador", tipo_chave(F.col("ds_chave_pagador"))).withColumn("tp_chave_recebedor", tipo_chave(F.col("ds_chave_recebedor"))))
salvar(pix, f"{SILVER}.trns_pix", "Transações Lynx PIX: deduplicadas, pagador e recebedor padronizados")

antp = (dedup(bronze("tbciar_tr_lynx_trns_antp_rcbv"), "id_unco_lynx")
    .withColumn("_dom", padroniza_documento(F.col("nu_cnpj_rspe_dmcl").cast("string"))).withColumn("_resp", padroniza_documento("nu_cnpj_cpf_mqnt"))
    .select("id_unco_lynx", para_timestamp("dt_antp", "hr_antp").alias("ts_transacao"), F.col("id_estn").cast("bigint").alias("nu_ec"),
            F.regexp_replace(F.when(F.col("vl_trns").contains(","), F.regexp_replace("vl_trns", r"\.", "")).otherwise(F.col("vl_trns")), ",", ".").cast("double").alias("vl_trns"),
            (F.col("in_acto") == "1").alias("fl_aprovada"), F.col("_dom.doc").alias("nu_doc_domicilio"), F.col("_resp.doc").alias("nu_doc_responsavel"),
            conta("nu_ispb", "nu_agnc", "nu_cnta").alias("ds_conta_domicilio")))
salvar(antp, f"{SILVER}.trns_antecipacao", "Antecipações RAD0: valor convertido (vírgula decimal), conta de domicílio e responsável pela maquininha padronizados")

frd = (dedup(bronze("tbciar_tr_frde"), "id_unco_lynx")
       .select("id_unco_lynx", F.to_timestamp("dh_trns").alias("ts_transacao"), F.to_timestamp("dh_reporte").alias("ts_reporte"), "cd_tipo_rsps_frde")
       .join(spark.table(f"{SILVER}.trns_cartao").select(F.col("id_unco").alias("id_unco_lynx"), "nu_ec", "nu_token_cartao"), "id_unco_lynx", "left"))
salvar(frd, f"{SILVER}.fraude_reportada", "Transações reportadas como fraude, ligadas ao EC e ao cartão")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Registros de cadastro (entrada do Splink no notebook 02)
# MAGIC Cada **menção** a uma pessoa/empresa em qualquer fonte vira um registro com os campos que usaremos para comparar identidades:
# MAGIC `cpf_cnpj`, `nome`, `telefone`, `email`, `endereco`. Menções idênticas são agrupadas (`qt_ocorrencias`).

# COMMAND ----------

c, p, a = (spark.table(f"{SILVER}.{t}") for t in ["trns_cartao", "trns_pix", "trns_antecipacao"])
ch = lambda col_chave, col_tipo, tipo: F.when(F.col(col_tipo) == tipo, F.col(col_chave))

def reg(df, fonte, doc, nome=None, tel=None, email=None, end=None):
    n = lambda x: x if x is not None else F.lit(None).cast("string")
    return df.select(F.lit(fonte).alias("fonte"), doc.alias("cpf_cnpj"), n(nome).alias("nome"), n(tel).alias("telefone"),
                     n(email).alias("email"), n(end).alias("endereco"))

registros = (reg(hub_hist, "HUB_TITULAR", F.col("NU_DCMT_PADRAO"), padroniza_nome("NM_CLNT"), end=F.col("DC_ENDR_PADRAO"))
    .unionByName(reg(c, "CARTAO_PORTADOR", F.col("nu_doc_portador"), F.col("nm_portador"), F.col("nu_telefone_portador"), F.col("ds_email_portador"), F.col("ds_endereco_portador")))
    .unionByName(reg(p, "PIX_PAGADOR", F.col("nu_doc_pagador"), F.col("nm_pagador"),
                     padroniza_telefone(ch("ds_chave_pagador", "tp_chave_pagador", "TELEFONE")), F.col("ds_email_pagador")))
    .unionByName(reg(p, "PIX_RECEBEDOR", F.col("nu_doc_recebedor"), F.col("nm_recebedor"),
                     padroniza_telefone(ch("ds_chave_recebedor", "tp_chave_recebedor", "TELEFONE")), padroniza_email(ch("ds_chave_recebedor", "tp_chave_recebedor", "EMAIL"))))
    .unionByName(reg(a, "RAD0_DOMICILIO", F.col("nu_doc_domicilio")))
    .unionByName(reg(a, "RAD0_RESPONSAVEL", F.col("nu_doc_responsavel")))
    .filter("cpf_cnpj is not null")
    .groupBy("cpf_cnpj", "nome", "telefone", "email", "endereco")
    .agg(F.array_sort(F.collect_set("fonte")).alias("fontes"), F.count("*").alias("qt_ocorrencias"))
    .withColumn("unique_id", F.xxhash64("cpf_cnpj", "nome", "telefone", "email", "endereco"))
    .withColumn("fl_doc_valido", padroniza_documento("cpf_cnpj").getField("valido")))
salvar(registros.select("unique_id", "cpf_cnpj", "fl_doc_valido", "nome", "telefone", "email", "endereco", "fontes", "qt_ocorrencias"),
       f"{SILVER}.registros_cadastro", "Menções de pessoas/empresas em todas as fontes (entrada da resolução de identidade com Splink)")
display(spark.table(f"{SILVER}.registros_cadastro").groupBy("fl_doc_valido").count())

# COMMAND ----------

# MAGIC %md ✅ **Checkpoint** — as tabelas Silver devem existir e ter dados:

# COMMAND ----------

for t in ["hub_risco_mensal", "loja", "trns_cartao", "trns_pix", "trns_antecipacao", "fraude_reportada", "lista_monitoramento", "registros_cadastro"]:
    assert spark.table(f"{SILVER}.{t}").count() > 0, f"{t} vazia"
assert spark.table(f"{SILVER}.trns_cartao").filter("ts_transacao is null").count() == 0, "datas não convertidas"
print("✔ Silver ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construção de vértices e arestas
# MAGIC O GraphFrames representa o grafo com **dois DataFrames**: `vertices` (coluna `id`) e `edges` (colunas `src`, `dst`). Qualquer outra coluna vira atributo —
# MAGIC ou seja, montamos o grafo **direto das tabelas Delta**, sem banco de grafos separado.
# MAGIC
# MAGIC | Vértice | `id` | De onde vem |
# MAGIC |---|---|---|
# MAGIC | LOJA | `LOJA:<nu_ec>` | HUB |
# MAGIC | DOC (pessoa/empresa) | `DOC:<cpf_cnpj padronizado>` | todas as fontes |
# MAGIC | CONTA | `CONTA:<ispb\|agência\|conta>` | RAD0, PIX |
# MAGIC | TELEFONE · EMAIL · ENDERECO | `TEL:` · `EMAIL:` · `END:` | cartões, PIX (chaves), HUB |
# MAGIC | DISPOSITIVO · CARTAO | `DISP:` · `CARTAO:` | PIX, cartões |
# MAGIC
# MAGIC | Aresta (`relacao`) | src → dst | Sinal |
# MAGIC |---|---|---|
# MAGIC | `titular` | LOJA → DOC | quem é o dono da loja |
# MAGIC | `socio` | LOJA → DOC | responsável pela maquininha (RAD0) |
# MAGIC | `liquida_em` | LOJA → CONTA | conta de domicílio das antecipações |
# MAGIC | `localizada_em` | LOJA → ENDERECO | endereço cadastral |
# MAGIC | `recebe_em` / `paga_com` | DOC → CONTA | contas usadas no PIX |
# MAGIC | `tem_telefone` / `tem_email` / `usa_dispositivo` / `entrega_em` / `portador` | DOC → atributo | dados de contato e meios de pagamento |
# MAGIC | `pix` | DOC → DOC | fluxo de dinheiro (qtd, valor) |
# MAGIC | `compra` | CARTAO → LOJA | transações de cartão (qtd, valor) |

# COMMAND ----------

loja_s = spark.table(f"{SILVER}.loja")
pref = lambda p, col: F.concat(F.lit(p), col)

def arestas(df, src, dst, relacao, valor=None, ts="ts_transacao"):
    return (df.select(src.alias("src"), dst.alias("dst"), (F.col(valor) if valor else F.lit(None).cast("double")).alias("_v"),
                      (F.col(ts) if ts else F.lit(None).cast("timestamp")).alias("_t"))
              .filter("src is not null and dst is not null and src <> dst")
              .groupBy("src", "dst").agg(F.count("*").alias("qt"), F.sum("_v").alias("valor"), F.min("_t").alias("ts_min"), F.max("_t").alias("ts_max"))
              .withColumn("relacao", F.lit(relacao)))

E = (arestas(loja_s, pref("LOJA:", F.col("nu_ec")), pref("DOC:", F.col("nu_doc")), "titular", ts=None)
    .unionByName(arestas(a.join(loja_s.select("nu_ec", "nu_doc"), "nu_ec").filter("nu_doc_responsavel <> nu_doc"),
                         pref("LOJA:", F.col("nu_ec")), pref("DOC:", F.col("nu_doc_responsavel")), "socio"))
    .unionByName(arestas(a, pref("LOJA:", F.col("nu_ec")), pref("CONTA:", F.col("ds_conta_domicilio")), "liquida_em", "vl_trns"))
    .unionByName(arestas(loja_s, pref("LOJA:", F.col("nu_ec")), pref("END:", F.col("ds_endereco")), "localizada_em", ts=None))
    .unionByName(arestas(p, pref("DOC:", F.col("nu_doc_recebedor")), pref("CONTA:", F.col("ds_conta_recebedor")), "recebe_em", "vl_trns"))
    .unionByName(arestas(p, pref("DOC:", F.col("nu_doc_pagador")), pref("CONTA:", F.col("ds_conta_pagador")), "paga_com", "vl_trns"))
    .unionByName(arestas(c, pref("DOC:", F.col("nu_doc_portador")), pref("TEL:", F.col("nu_telefone_portador")), "tem_telefone"))
    .unionByName(arestas(p, pref("DOC:", F.col("nu_doc_recebedor")), pref("TEL:", padroniza_telefone(ch("ds_chave_recebedor", "tp_chave_recebedor", "TELEFONE"))), "tem_telefone"))
    .unionByName(arestas(p, pref("DOC:", F.col("nu_doc_pagador")), pref("TEL:", padroniza_telefone(ch("ds_chave_pagador", "tp_chave_pagador", "TELEFONE"))), "tem_telefone"))
    .unionByName(arestas(c, pref("DOC:", F.col("nu_doc_portador")), pref("EMAIL:", F.col("ds_email_portador")), "tem_email"))
    .unionByName(arestas(p, pref("DOC:", F.col("nu_doc_pagador")), pref("EMAIL:", F.col("ds_email_pagador")), "tem_email"))
    .unionByName(arestas(p, pref("DOC:", F.col("nu_doc_recebedor")), pref("EMAIL:", padroniza_email(ch("ds_chave_recebedor", "tp_chave_recebedor", "EMAIL"))), "tem_email"))
    .unionByName(arestas(p, pref("DOC:", F.col("nu_doc_pagador")), pref("DISP:", F.col("id_dispositivo")), "usa_dispositivo"))
    .unionByName(arestas(c, pref("DOC:", F.col("nu_doc_portador")), pref("END:", F.col("ds_endereco_entrega")), "entrega_em"))
    .unionByName(arestas(c, pref("DOC:", F.col("nu_doc_portador")), pref("CARTAO:", F.col("nu_token_cartao")), "portador"))
    .unionByName(arestas(c, pref("CARTAO:", F.col("nu_token_cartao")), pref("LOJA:", F.col("nu_ec")), "compra", "vl_trns"))
    .unionByName(arestas(p.filter("fl_aprovada"), pref("DOC:", F.col("nu_doc_pagador")), pref("DOC:", F.col("nu_doc_recebedor")), "pix", "vl_trns"))
    .groupBy("src", "dst", "relacao").agg(F.sum("qt").alias("qt"), F.sum("valor").alias("valor"), F.min("ts_min").alias("ts_min"), F.max("ts_max").alias("ts_max")))
salvar(E, f"{GOLD}.grafo_arestas", "Arestas do grafo PLD (antes da resolução de identidade)")
E = spark.table(f"{GOLD}.grafo_arestas")

# COMMAND ----------

nomes_doc = (spark.table(f"{SILVER}.registros_cadastro").groupBy("cpf_cnpj")
             .agg(F.expr("max_by(nome, CASE WHEN nome IS NOT NULL THEN qt_ocorrencias ELSE -1 END)").alias("nome"),
                  F.max(F.col("fl_doc_valido").cast("int")).alias("fl_doc_valido")))
ids = E.select(F.col("src").alias("id")).union(E.select(F.col("dst").alias("id"))).distinct()
v_loja = loja_s.select(pref("LOJA:", F.col("nu_ec")).alias("id"), F.lit("LOJA").alias("tipo"), F.col("nm_loja").alias("nome"), "nu_ec", "nu_raiz",
                       "fl_fraude", "pc_chargeback", F.lit(None).cast("int").alias("fl_doc_valido"))
v_outros = (ids.join(v_loja.select("id"), "id", "left_anti")
    .withColumn("tipo", F.element_at(F.create_map(*[F.lit(x) for x in ["DOC", "DOC", "CONTA", "CONTA", "TEL", "TELEFONE", "EMAIL", "EMAIL",
                                                                       "END", "ENDERECO", "DISP", "DISPOSITIVO", "CARTAO", "CARTAO"]]), F.split("id", ":")[0]))
    .withColumn("valor", F.expr("substring(id, instr(id, ':') + 1)"))
    .join(nomes_doc.withColumnRenamed("cpf_cnpj", "valor"), "valor", "left")
    .select("id", "tipo", F.coalesce("nome", "valor").alias("nome"), F.lit(None).cast("bigint").alias("nu_ec"),
            F.when(F.col("tipo") == "DOC", F.when(F.length("valor") == 14, F.substring("valor", 1, 8)).otherwise(F.col("valor"))).alias("nu_raiz"),
            F.lit(0).alias("fl_fraude"), F.lit(None).cast("double").alias("pc_chargeback"), "fl_doc_valido"))
salvar(v_loja.unionByName(v_outros), f"{GOLD}.grafo_vertices", "Vértices do grafo PLD (antes da resolução de identidade)")
V = spark.table(f"{GOLD}.grafo_vertices")

# COMMAND ----------

# MAGIC %md ### 3.1 O grafo no GraphFrames

# COMMAND ----------

from graphframes import GraphFrame
g = GraphFrame(V, E)
display(g.vertices.groupBy("tipo").count().orderBy(F.desc("count")))

# COMMAND ----------

display(g.edges.groupBy("relacao").agg(F.count("*").alias("arestas"), F.sum("qt").alias("eventos"), F.round(F.sum("valor"), 2).alias("valor_total")).orderBy(F.desc("arestas")))

# COMMAND ----------

# MAGIC %md
# MAGIC Vizinhança de uma loja **com fraude confirmada** — o que a visão tabular não mostra numa linha só:

# COMMAND ----------

loja_exemplo = V.filter("tipo = 'LOJA' and fl_fraude = 1").orderBy("id").first()["id"]
display(g.edges.filter((F.col("src") == loja_exemplo) | (F.col("dst") == loja_exemplo)).filter("relacao <> 'compra'")
        .join(V.select(F.col("id").alias("dst"), F.col("tipo").alias("tipo_destino"), F.col("nome").alias("nome_destino")), "dst"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 3
# MAGIC Use `g.degrees` para descobrir **qual tipo de vértice tem o maior grau médio** e quais são as 5 contas (`tipo = 'CONTA'`) com mais conexões.

# COMMAND ----------

# ✏️ seu código aqui
# g.degrees.join(V, "id")...

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

graus = g.degrees.join(V.select("id", "tipo", "nome"), "id")
display(graus.groupBy("tipo").agg(F.round(F.avg("degree"), 2).alias("grau_medio"), F.max("degree").alias("grau_max")).orderBy(F.desc("grau_medio")))
display(graus.filter("tipo = 'CONTA'").orderBy(F.desc("degree")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint**

# COMMAND ----------

assert V.filter("tipo = 'LOJA'").count() > 0 and E.filter("relacao = 'pix'").count() > 0
assert E.join(V, E.src == V.id, "left_anti").count() == 0, "aresta com src sem vértice"
assert E.join(V, E.dst == V.id, "left_anti").count() == 0, "aresta com dst sem vértice"
print(f"✔ Grafo pronto: {V.count():,} vértices e {E.count():,} arestas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC * Exploramos as fontes e vimos o problema: **o mesmo cliente com várias grafias**, duplicatas e formatos mistos.
# MAGIC * Padronizamos **documentos (com DV), telefones (E.164) e endereços** em todas as fontes → `silver.*`.
# MAGIC * Montamos o grafo de **lojas, pessoas/empresas, contas, telefones, endereços, dispositivos e cartões** → `gold.grafo_vertices` / `gold.grafo_arestas`.
# MAGIC
# MAGIC ➡️ **Próximo:** `02_graph_analytics` — motifs, resolução de identidade com Splink e PageRank.
