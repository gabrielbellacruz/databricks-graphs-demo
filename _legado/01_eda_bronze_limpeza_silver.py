# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · EDA na Bronze → Limpeza → Resolução de Identidade na Silver
# MAGIC
# MAGIC **Objetivo PLD:** antes de montar o grafo precisamos garantir que *a mesma pessoa/empresa* seja reconhecida em todas as fontes
# MAGIC (HUB de risco, cartões, PIX, antecipações e listas). Um CPF com máscara num sistema e sem zeros à esquerda em outro — ou com um dígito trocado —
# MAGIC vira dois nós diferentes e **quebra o elo** que liga o laranja ao controlador.
# MAGIC
# MAGIC Etapas:
# MAGIC 1. **EDA** da camada Bronze: volumetria, nulos, duplicatas, formatos inconsistentes, sinais de PLD.
# MAGIC 2. **Funções de limpeza** reutilizáveis (documentos com validação de DV, nomes, telefones, e-mails, datas, valores).
# MAGIC 3. **Resolução de identidade**: determinística (documento normalizado + DV) → *fuzzy* (nome + distância de Levenshtein no documento).
# MAGIC 4. **Silver**: tabelas limpas, tipadas, deduplicadas e ligadas à entidade resolvida.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
import pandas as pd, re

def bz(t): return spark.table(f"{BRONZE}.{t}")

# COMMAND ----------

# MAGIC %md ## 1. EDA — Camada Bronze
# MAGIC ### 1.1 Inventário

# COMMAND ----------

tabelas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {BRONZE}").collect()]
inv = [(t, bz(t).count(), len(bz(t).columns)) for t in tabelas]
display(spark.createDataFrame(inv, "tabela string, linhas long, colunas int").orderBy(F.desc("linhas")))

# COMMAND ----------

# MAGIC %md ### 1.2 Perfil de nulos — layout configurável da Lynx
# MAGIC O layout Lynx é configurável por canal: muitas colunas `id_cau_*` chegam vazias. Colunas 100% nulas não carregam informação e serão descartadas na Silver.

# COMMAND ----------

def perfil_nulos(df, nome):
    n = df.count()
    agg = df.select([(F.sum(F.col(c).isNull().cast("int")) / n * 100).alias(c) for c in df.columns]).first().asDict()
    return spark.createDataFrame([(nome, c, float(v)) for c, v in agg.items()], "tabela string, coluna string, pct_nulo double")

nulos = perfil_nulos(bz("tbciar_tr_lynx_trns_crto"), "crto").unionByName(perfil_nulos(bz("tbciar_tr_lynx_trns_pix"), "pix")) \
    .unionByName(perfil_nulos(bz("tbciar_tr_lynx_trns_antp_rcbv"), "antp")).unionByName(perfil_nulos(bz("tbciar_re_hub_dado_risc_cred"), "hub"))
nulos.cache()
display(nulos.groupBy("tabela").agg(F.count("*").alias("colunas"), F.sum((F.col("pct_nulo") == 100).cast("int")).alias("colunas_100pct_nulas"),
                                     F.sum(((F.col("pct_nulo") > 0) & (F.col("pct_nulo") < 100)).cast("int")).alias("colunas_parcialmente_nulas")))

# COMMAND ----------

display(nulos.filter("pct_nulo > 0 and pct_nulo < 100").orderBy("tabela", F.desc("pct_nulo")))

# COMMAND ----------

# MAGIC %md ### 1.3 Duplicatas por reprocessamento de arquivos

# COMMAND ----------

def dups(df, chave, nome):
    return (df.groupBy(chave).count().filter("count > 1")
              .agg(F.count("*").alias("chaves_duplicadas"), F.sum(F.col("count") - 1).alias("linhas_excedentes"))
              .withColumn("tabela", F.lit(nome)).withColumn("chave", F.lit(",".join(chave) if isinstance(chave, list) else chave)))

display(dups(bz("tbciar_tr_lynx_trns_crto"), "id_unco", "trns_crto")
        .unionByName(dups(bz("tbciar_tr_lynx_trns_pix"), "id_unco_pix", "trns_pix"))
        .unionByName(dups(bz("tbciar_tr_lynx_trns_antp_rcbv"), "id_unco_lynx", "trns_antp_rcbv"))
        .unionByName(dups(bz("tbciar_tr_frde"), "id_unco_lynx", "frde"))
        .unionByName(dups(bz("tbciar_re_hub_dado_risc_cred"), ["CD_MES", "NU_EC"], "hub"))
        .select("tabela", "chave", "chaves_duplicadas", "linhas_excedentes"))

# COMMAND ----------

display(bz("tbciar_tr_lynx_trns_crto").groupBy(F.col("nm_arqv_orgm").contains("REPROC").alias("arquivo_reprocessado")).count())

# COMMAND ----------

# MAGIC %md ### 1.4 Qualidade de documentos (CPF/CNPJ)
# MAGIC O mesmo documento aparece em formatos diferentes entre fontes — o principal obstáculo para ligar entidades.

# COMMAND ----------

def padrao_doc(c):
    return (F.when(F.col(c).isNull(), "NULO")
             .when(F.col(c).rlike(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"), "CPF COM MASCARA")
             .when(F.col(c).rlike(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$"), "CNPJ COM MASCARA")
             .when(F.col(c).rlike(r"^\d{11}$"), "11 DIGITOS")
             .when(F.col(c).rlike(r"^\d{14}$"), "14 DIGITOS")
             .when(F.col(c).rlike(r"^\d+$"), "SO DIGITOS, TAMANHO ERRADO (zeros perdidos)")
             .otherwise("OUTRO"))

fontes_doc = [("hub", "tbciar_re_hub_dado_risc_cred", "NU_DCMT"), ("pix_pagador", "tbciar_tr_lynx_trns_pix", "nu_pix_cpf_cnpj_pgdr"),
              ("pix_recebedor", "tbciar_tr_lynx_trns_pix", "nu_cpf_cnpj_recr"), ("pix_estab", "tbciar_tr_lynx_trns_pix", "nu_dcmt_estn"),
              ("crto_pld", "tbciar_tr_lynx_trns_crto", "nu_oprc_cpf_ptdr_pld"), ("crto_ecommerce", "tbciar_tr_lynx_trns_crto", "nu_cpf_ptdr_ecommerce"),
              ("antp_maquininha", "tbciar_tr_lynx_trns_antp_rcbv", "nu_cnpj_cpf_mqnt")]
pad = None
for f, t, c in fontes_doc:
    d = bz(t).select(F.lit(f).alias("fonte"), padrao_doc(c).alias("padrao"))
    pad = d if pad is None else pad.unionByName(d)
display(pad.groupBy("fonte", "padrao").count().orderBy("fonte", F.desc("count")))

# COMMAND ----------

# MAGIC %md ### 1.5 Formatos mistos de datas, UFs e valores

# COMMAND ----------

crto = bz("tbciar_tr_lynx_trns_crto")
display(crto.select(F.when(F.col("dt_oprc").rlike(r"^\d{8}$"), "aaaammdd").when(F.col("dt_oprc").rlike(r"^\d{4}-\d{2}-\d{2}$"), "aaaa-mm-dd")
                    .otherwise("outro").alias("formato_dt_oprc")).groupBy("formato_dt_oprc").count())

# COMMAND ----------

display(bz("tbciar_re_hub_dado_risc_cred").groupBy("NM_UF").count().orderBy(F.desc("count")))

# COMMAND ----------

display(bz("tbciar_tr_lynx_trns_antp_rcbv").select(
    F.when(F.col("vl_trns").rlike(r"^\d{1,3}(\.\d{3})*,\d{2}$"), "pt-BR (1.234,56)")
     .when(F.col("vl_trns").rlike(r"^\d+\.\d{2}$"), "en (1234.56)").otherwise("outro").alias("formato_vl_trns")).groupBy("formato_vl_trns").count())

# COMMAND ----------

display(crto.select((F.col("vl_tste") > 0).alias("transacao_teste"), F.upper("nm_estn").contains("TESTE").alias("nome_teste")).groupBy("transacao_teste", "nome_teste").count())

# COMMAND ----------

# MAGIC %md ### 1.6 Primeiros sinais de PLD na Bronze
# MAGIC **Fracionamento (structuring):** concentração anormal de transações logo abaixo de R$ 5 mil e R$ 10 mil.

# COMMAND ----------

display(crto.filter("vl_trns between 3000 and 11000")
        .select((F.floor(F.col("vl_trns") / 250) * 250).alias("faixa_valor")).groupBy("faixa_valor").count().orderBy("faixa_valor"))

# COMMAND ----------

display(crto.select(F.substring("hr_oprc", 1, 2).cast("int").alias("hora"), F.col("tx_prfl_cptn")).groupBy("hora")
        .agg(F.count("*").alias("transacoes"), F.avg(F.col("tx_prfl_cptn").isin("P11", "P12").cast("int")).alias("pct_perfil_atipico")).orderBy("hora"))

# COMMAND ----------

# MAGIC %md **Listas de monitoramento:** parte das inserções já expirou (`dt_sada_lsta` preenchida) e há datas em dois formatos.

# COMMAND ----------

LISTAS = {f"tbciar_tr_lynx_lsta_{t}_{o}": (t, o) for t in ["psit", "ngto", "psit_atzd"] for o in ["crto", "clnt", "ec"]}
lst = None
for nome, (t, o) in LISTAS.items():
    d = bz(nome).select(F.lit(t).alias("tipo"), F.lit(o).alias("objeto"), F.col("dt_sada_lsta").isNull().alias("ativo"))
    lst = d if lst is None else lst.unionByName(d)
display(lst.groupBy("tipo", "objeto").agg(F.count("*").alias("insercoes"), F.sum(F.col("ativo").cast("int")).alias("ativas")).orderBy("objeto", "tipo"))

# COMMAND ----------

# MAGIC %md ## 2. Funções de limpeza

# COMMAND ----------

def _dv_ok(d):
    if len(d) == 11:
        if d == d[0] * 11: return False
        r = sum(int(d[i]) * (10 - i) for i in range(9)) % 11; v1 = 0 if r < 2 else 11 - r
        r = sum(int(d[i]) * (11 - i) for i in range(10)) % 11; v2 = 0 if r < 2 else 11 - r
        return d[9] == str(v1) and d[10] == str(v2)
    if len(d) == 14:
        w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]; w2 = [6] + w1
        r = sum(int(a) * b for a, b in zip(d[:12], w1)) % 11; v1 = 0 if r < 2 else 11 - r
        r = sum(int(a) * b for a, b in zip(d[:13], w2)) % 11; v2 = 0 if r < 2 else 11 - r
        return d[12] == str(v1) and d[13] == str(v2)
    return False

@F.pandas_udf("struct<doc:string,tp_pessoa:string,valido:boolean>")
def resolve_doc(s: pd.Series) -> pd.DataFrame:
    """Remove máscara, recompõe zeros à esquerda e decide CPF x CNPJ pelo dígito verificador."""
    out = []
    for x in s:
        d = re.sub(r"\D", "", str(x)) if x is not None and not pd.isna(x) else ""
        if not d:
            out.append((None, None, None)); continue
        cands = [d.zfill(14)] if len(d) > 11 else [d.zfill(11), d.zfill(14)]
        ok = next((c for c in cands if _dv_ok(c)), None)
        c = ok or cands[0]
        out.append((c, "PF" if len(c) == 11 else "PJ", ok is not None))
    return pd.DataFrame(out, columns=["doc", "tp_pessoa", "valido"])

_ACC, _SEM = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ", "AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn"

def norm_nome(c):
    c = F.upper(F.translate(F.col(c) if isinstance(c, str) else c, _ACC, _SEM))
    return F.trim(F.regexp_replace(F.regexp_replace(c, r"[^A-Z0-9 ]", " "), r"\s+", " "))

def chave_nome(c):
    """Nome normalizado sem sufixos societários — usado como bloco na resolução fuzzy."""
    return F.trim(F.regexp_replace(F.regexp_replace(norm_nome(c), r"\b(LTDA|ME|EPP|EIRELI|SA|S A|SLU|MEI|CIA|E FILHOS)\b", ""), r"\s+", " "))

def norm_tel(c, ddd=None):
    d = F.regexp_replace(F.col(c), r"\D", "")
    d = F.when((F.length(d) >= 12) & d.startswith("55"), F.expr("substring(regexp_replace({0}, '\\\\D', ''), 3)".format(c))).otherwise(d)
    if ddd is not None:
        d = F.when(F.length(d) <= 9, F.concat(F.col(ddd), d)).otherwise(d)
    return F.when(F.length(d) >= 10, d)

def norm_email(c): return F.lower(F.trim(F.col(c)))

def para_ts(data, hora=None, fmts=("yyyyMMdd", "yyyy-MM-dd")):
    data = F.col(data) if isinstance(data, str) else data
    d = F.coalesce(*[F.try_to_timestamp(data, F.lit(f)) for f in fmts])
    if hora is None:
        return d
    h = F.lpad(F.regexp_replace(F.col(hora).cast("string"), ":", ""), 6, "0")
    return d + F.make_dt_interval(F.lit(0), h.substr(1, 2).cast("int"), h.substr(3, 2).cast("int"), h.substr(5, 2).cast("int"))

def valor_br(c):
    s = F.trim(F.col(c))
    return F.when(s.contains(","), F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")).otherwise(s).cast("double")

def conta_key(ispb, ag, cc):
    return F.concat_ws("|", F.col(ispb), F.col(ag).cast("int").cast("string"), F.col(cc).cast("bigint").cast("string"))

def dedup(df, chave, ordem="dt_crga"):
    w = W.partitionBy(*([chave] if isinstance(chave, str) else chave)).orderBy(F.desc(ordem))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def salvar(df, nome, comentario):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.{nome}")
    spark.sql(f"COMMENT ON TABLE {SILVER}.{nome} IS '{comentario}'")
    print(f"{SILVER}.{nome}: {spark.table(f'{SILVER}.{nome}').count():,} linhas")

UF_MAP = F.create_map(*[F.lit(x) for kv in {"SAO PAULO": "SP", "RIO DE JANEIRO": "RJ", "MINAS GERAIS": "MG", "PARANA": "PR",
    "RIO GRANDE DO SUL": "RS", "BAHIA": "BA", "PERNAMBUCO": "PE", "CEARA": "CE", "DISTRITO FEDERAL": "DF", "GOIAS": "GO",
    "SANTA CATARINA": "SC", "AMAZONAS": "AM", "PARA": "PA"}.items() for x in kv])

# teste rápido
display(spark.createDataFrame([("123.456.789-09",), ("12345678909",), ("11.222.333/0001-81",), ("11222333000181",), ("1222333000181",), ("12345678900",)], "raw string")
        .select("raw", resolve_doc("raw").alias("r")).select("raw", "r.*"))

# COMMAND ----------

# MAGIC %md ## 3. Resolução de identidade
# MAGIC ### 3.1 Coleta de todas as menções de documento (com o nome que acompanha cada menção)

# COMMAND ----------

pix_b, crto_b, antp_b, hub_b = bz("tbciar_tr_lynx_trns_pix"), bz("tbciar_tr_lynx_trns_crto"), bz("tbciar_tr_lynx_trns_antp_rcbv"), bz("tbciar_re_hub_dado_risc_cred")

def mencao(df, fonte, doc_col, nome_col=None):
    nome = F.col(nome_col) if nome_col else F.lit(None).cast("string")
    return df.select(F.lit(fonte).alias("fonte"), F.col(doc_col).cast("string").alias("doc_bruto"), nome.alias("nome_bruto"))

mencoes = (mencao(hub_b, "HUB_TITULAR_EC", "NU_DCMT", "NM_CLNT")
    .unionByName(mencao(pix_b, "PIX_PAGADOR", "nu_pix_cpf_cnpj_pgdr", "tx_pix_nm_pgdr"))
    .unionByName(mencao(pix_b, "PIX_RECEBEDOR", "nu_cpf_cnpj_recr", "nm_recr"))
    .unionByName(mencao(pix_b, "PIX_RECEBEDOR", "nu_cpf_cnpj", "nm_recr"))
    .unionByName(mencao(pix_b, "PIX_ESTABELECIMENTO", "nu_dcmt_estn"))
    .unionByName(mencao(crto_b, "CARTAO_PORTADOR_PLD", "nu_oprc_cpf_ptdr_pld", "tx_oprc_nm_ptdr_pld"))
    .unionByName(mencao(crto_b, "CARTAO_PORTADOR_ECOMMERCE", "nu_cpf_ptdr_ecommerce", "nm_ptdr_crto_ecommerce"))
    .unionByName(mencao(antp_b, "ANTP_DOMICILIO", "nu_cnpj_rspe_dmcl"))
    .unionByName(mencao(antp_b, "ANTP_MAQUININHA", "nu_cnpj_cpf_mqnt"))
    .filter(F.col("doc_bruto").isNotNull())
    .groupBy("fonte", "doc_bruto", "nome_bruto").count()
    .withColumn("r", resolve_doc("doc_bruto"))
    .select("fonte", "doc_bruto", "nome_bruto", F.col("count").alias("qt"), F.col("r.doc").alias("doc_norm"),
            F.col("r.tp_pessoa").alias("tp_pessoa"), F.col("r.valido").alias("doc_valido"), chave_nome("nome_bruto").alias("nome_chave")))
mencoes.cache()
display(mencoes.groupBy("fonte").agg(F.sum("qt").alias("mencoes"), F.countDistinct("doc_bruto").alias("docs_brutos_distintos"),
                                     F.countDistinct("doc_norm").alias("docs_normalizados_distintos"),
                                     F.sum(F.when(~F.col("doc_valido"), F.col("qt")).otherwise(0)).alias("mencoes_dv_invalido")).orderBy("fonte"))

# COMMAND ----------

# MAGIC %md ### 3.2 Passo determinístico: documento normalizado + DV válido
# MAGIC ### 3.3 Passo *fuzzy*: documento com DV inválido → mesmo nome (bloco) + Levenshtein(documento) ≤ 2

# COMMAND ----------

validos = mencoes.filter("doc_valido")
nomes_validos = validos.filter("nome_chave is not null and nome_chave <> ''").select("doc_norm", "nome_chave").distinct()

invalidos = mencoes.filter("not doc_valido").select("doc_norm", "tp_pessoa", "nome_chave").distinct()
candidatos = (invalidos.alias("i").join(nomes_validos.alias("v"), "nome_chave")
    .withColumn("dist", F.levenshtein(F.col("i.doc_norm"), F.col("v.doc_norm")))
    .filter("dist <= 2")
    .withColumn("_rn", F.row_number().over(W.partitionBy(F.col("i.doc_norm")).orderBy("dist")))
    .filter("_rn = 1")
    .select(F.col("i.doc_norm").alias("doc_norm"), F.col("v.doc_norm").alias("doc_final"), "dist", "nome_chave"))

mapa_doc = (mencoes.select("doc_norm", "doc_valido").distinct()
    .join(candidatos.select("doc_norm", "doc_final", "dist"), "doc_norm", "left")
    .select("doc_norm",
            F.when(F.col("doc_valido"), F.col("doc_norm")).otherwise(F.coalesce("doc_final", "doc_norm")).alias("doc_final"),
            F.when(F.col("doc_valido"), "DETERMINISTICO_DV")
             .when(F.col("doc_final").isNotNull(), "FUZZY_NOME_LEVENSHTEIN")
             .otherwise("NAO_RESOLVIDO").alias("metodo_resolucao"),
            "dist")
    .dropDuplicates(["doc_norm"]))
salvar(mapa_doc, "mapa_resolucao_documento", "De-para de documento normalizado para documento resolvido (resolução de identidade)")
mapa_doc = spark.table(f"{SILVER}.mapa_resolucao_documento")
display(mapa_doc.groupBy("metodo_resolucao").count())

# COMMAND ----------

display(candidatos.limit(20))

# COMMAND ----------

# MAGIC %md ### 3.4 Entidade mestre (`silver.entidade`)

# COMMAND ----------

mencoes_res = mencoes.join(mapa_doc.select("doc_norm", "doc_final", "metodo_resolucao"), "doc_norm")
entidade = (mencoes_res.groupBy("doc_final").agg(
        F.first("tp_pessoa").alias("tp_pessoa"),
        F.mode(F.when(F.col("nome_chave") != "", F.col("nome_chave"))).alias("nm_canonico"),
        F.countDistinct("nome_chave").alias("qt_variacoes_nome"),
        F.countDistinct("doc_bruto").alias("qt_variacoes_documento"),
        F.array_sort(F.collect_set("fonte")).alias("fontes"),
        F.sum("qt").alias("qt_mencoes"),
        F.max((F.col("metodo_resolucao") == "FUZZY_NOME_LEVENSHTEIN").cast("int")).alias("fl_resolvida_fuzzy"),
        F.min((F.col("metodo_resolucao") != "NAO_RESOLVIDO").cast("int")).alias("fl_doc_valido"))
    .withColumn("id_entidade", F.concat(F.lit("ENT_"), F.substring(F.sha2("doc_final", 256), 1, 16)))
    .withColumn("nu_raiz_cnpj", F.when(F.col("tp_pessoa") == "PJ", F.substring("doc_final", 1, 8)))
    .withColumn("qt_fontes", F.size("fontes"))
    .withColumnRenamed("doc_final", "nu_doc")
    .select("id_entidade", "nu_doc", "tp_pessoa", "nu_raiz_cnpj", "nm_canonico", "qt_variacoes_nome", "qt_variacoes_documento",
            "fontes", "qt_fontes", "qt_mencoes", "fl_resolvida_fuzzy", "fl_doc_valido"))
salvar(entidade, "entidade", "Entidade mestre (pessoa/empresa) resolvida a partir de todas as fontes")
ent = spark.table(f"{SILVER}.entidade")

# COMMAND ----------

display(ent.agg(F.count("*").alias("entidades"), F.sum("qt_mencoes").alias("mencoes"),
                F.sum((F.col("qt_variacoes_documento") > 1).cast("int")).alias("entidades_com_multiplas_grafias_doc"),
                F.sum("fl_resolvida_fuzzy").alias("entidades_com_mencao_resolvida_por_fuzzy"),
                F.sum((F.col("qt_fontes") > 1).cast("int")).alias("entidades_em_multiplas_fontes")))

# COMMAND ----------

display(ent.orderBy(F.desc("qt_variacoes_documento"), F.desc("qt_fontes")).limit(20))

# COMMAND ----------

# MAGIC %md ## 4. Silver — tabelas limpas e ligadas à entidade

# COMMAND ----------

doc_para_ent = mapa_doc.join(ent.select(F.col("nu_doc").alias("doc_final"), "id_entidade"), "doc_final").select("doc_norm", "doc_final", "id_entidade")

def liga_entidade(df, col_bruta, prefixo):
    """Resolve o documento bruto -> (doc resolvido, id_entidade)."""
    m = doc_para_ent.select(F.col("doc_norm").alias(f"_{prefixo}_n"), F.col("doc_final").alias(f"nu_doc_{prefixo}"),
                            F.col("id_entidade").alias(f"id_entidade_{prefixo}"))
    return (df.withColumn(f"_{prefixo}_n", resolve_doc(F.col(col_bruta).cast("string")).getField("doc"))
              .join(m, f"_{prefixo}_n", "left").drop(f"_{prefixo}_n"))

# COMMAND ----------

# MAGIC %md ### 4.1 HUB de risco de crédito (mensal) e cadastro de EC

# COMMAND ----------

hub_s = (dedup(hub_b, ["CD_MES", "NU_EC"], "DT_CRGA")
    .withColumn("NM_UF", F.coalesce(UF_MAP[F.upper(F.trim("NM_UF"))], F.upper(F.trim("NM_UF"))))
    .withColumn("VL_FTRM", F.when(F.col("VL_FTRM") >= 0, F.col("VL_FTRM")))
    .withColumn("NM_EC", norm_nome("NM_EC")).withColumn("NM_CLNT", norm_nome("NM_CLNT"))
    .withColumn("DC_ENDR_NORM", norm_nome(F.split("DC_ENDR", " - CEP")[0]))
    .transform(lambda d: liga_entidade(d, "NU_DCMT", "titular"))
    .withColumn("NU_DCMT", F.col("nu_doc_titular"))
    .withColumn("NU_DCMT_RAIZ", F.when(F.length("NU_DCMT") == 14, F.substring("NU_DCMT", 1, 8))))
salvar(hub_s, "hub_risco_credito", "HUB de risco de crédito mensal limpo e deduplicado, com documento resolvido")

w_ult = W.partitionBy("NU_EC").orderBy(F.desc("CD_MES"))
hub_hist = spark.table(f"{SILVER}.hub_risco_credito")
ec_cad = (hub_hist.withColumn("_rn", F.row_number().over(w_ult)).filter("_rn = 1").drop("_rn")
    .select(F.col("NU_EC").alias("nu_ec"), F.col("NM_EC").alias("nm_ec"), F.col("NM_CLNT").alias("nm_clnt"),
            F.col("NU_DCMT").alias("nu_doc_titular"), "id_entidade_titular", F.col("NU_DCMT_RAIZ").alias("nu_raiz_cnpj"),
            F.col("NM_GRPO_ECNC").alias("nm_grupo_economico"), F.col("TIPO_CLNT").alias("tp_cliente"), F.col("DC_TIPO_EMPA").alias("tp_empresa"),
            F.col("NM_UF").alias("sg_uf"), F.col("NM_MNCP").alias("nm_municipio"), F.col("DC_ENDR_NORM").alias("ds_endereco_norm"),
            F.col("CD_RMAT").alias("cd_ramo"), F.col("NM_RMAT").alias("nm_ramo"), F.col("DT_AFLC").alias("dt_afiliacao"),
            F.col("QT_DIAS_AFLC").alias("qt_dias_afiliacao"), F.col("CD_BNCO_AGNC").alias("cd_banco"), F.col("CD_AGNC").alias("cd_agencia"),
            F.col("NM_BNCO").alias("nm_banco"), F.col("IN_SOCO_PEP").alias("in_socio_pep"), F.col("IN_ALTR_QDRO_SCTR").alias("in_alteracao_societaria"),
            F.col("DC_STCO_SOCO_RCTA").alias("ds_situacao_socio_receita"), F.col("CD_MES").alias("cd_mes_ultimo_snapshot")))
salvar(ec_cad, "ec_cadastro", "Cadastro do Estabelecimento Comercial (último snapshot do HUB)")

# COMMAND ----------

# MAGIC %md ### 4.2 Transações de cartão

# COMMAND ----------

crto_s = (dedup(crto_b, "id_unco")
    .filter("coalesce(vl_tste, 0) = 0 and upper(coalesce(nm_estn, '')) not like '%TESTE%' and vl_trns > 0.01")
    .withColumn("ts_oprc", para_ts("dt_oprc", "hr_oprc"))
    .withColumn("doc_portador_bruto", F.coalesce("nu_oprc_cpf_ptdr_pld", "nu_cpf_ptdr_ecommerce"))
    .transform(lambda d: liga_entidade(d, "doc_portador_bruto", "portador"))
    .select("id_unco", "qt_cnar_ordm", "ts_oprc", F.col("id_estn").alias("nu_ec"), norm_nome("nm_estn").alias("nm_estn"),
            F.col("cd_setr_atvd").alias("cd_mcc"), F.col("sg_uf"), "vl_trns", (F.col("in_acto") == "1").alias("fl_aprovada"),
            (F.col("in_etnc") == "1").alias("fl_cnp"), F.col("cd_tipo_crto").alias("tp_cartao"), F.col("cd_bndr").alias("nm_bandeira"),
            F.col("nu_crto_tken").alias("nu_token_cartao"), "cd_bin6_crto", F.col("nu_prcl").cast("int").alias("qt_parcelas"),
            "nu_score_totl", "nu_score_rede_nerl", "nu_score_rgra", "nu_score_rgra_pld", "id_rgra_pld",
            F.col("id_rgra").cast("bigint").alias("id_rgra"), "cd_stas_frde", F.col("tx_prfl_cptn").alias("cd_perfil_comportamental"),
            F.col("cd_pais_crto"), "nu_doc_portador", "id_entidade_portador",
            norm_nome(F.coalesce("tx_oprc_nm_ptdr_pld", "nm_ptdr_crto_ecommerce", "nm_ptdr_crto_chip")).alias("nm_portador"),
            norm_email("tx_emal_ptdr_ecommerce").alias("ds_email_portador"),
            F.coalesce(norm_tel("nu_celr_ptdr_ecommerce"), norm_tel("nu_celr", "nu_ddd")).alias("nu_telefone_portador"),
            F.col("tx_endr_ip").alias("ds_ip"), norm_nome("tx_endr_enta_ecommerce").alias("ds_endereco_entrega"),
            F.to_date(F.col("dt_reporte_frde"), "yyyyMMdd").alias("dt_reporte_fraude"), "cd_tipo_frde")
    .withColumn("hr", F.hour("ts_oprc"))
    .withColumn("fl_madrugada", F.col("hr").between(0, 4))
    .withColumn("fl_valor_redondo", (F.col("vl_trns") >= 500) & (F.col("vl_trns") % 500 == 0))
    .withColumn("fl_abaixo_limite", F.col("vl_trns").between(9000, 9999.99) | F.col("vl_trns").between(4500, 4999.99))
    .withColumn("fl_pre_pago", F.col("tp_cartao") == "PRE-PAGO")
    .drop("hr"))
salvar(crto_s, "trns_cartao", "Transações Lynx canal cartões - limpas, deduplicadas, sem testes e com portador resolvido")

# COMMAND ----------

# MAGIC %md ### 4.3 Transações PIX

# COMMAND ----------

pix_s = (dedup(pix_b, "id_unco_pix")
    .withColumn("ts_oprc", para_ts(F.col("dt_oprc").cast("string"), "hr_oprc"))
    .withColumn("doc_recr_bruto", F.when(resolve_doc("nu_cpf_cnpj_recr").getField("valido"), F.col("nu_cpf_cnpj_recr")).otherwise(F.col("nu_cpf_cnpj")))
    .transform(lambda d: liga_entidade(d, "nu_pix_cpf_cnpj_pgdr", "pagador"))
    .transform(lambda d: liga_entidade(d, "doc_recr_bruto", "recebedor"))
    .select("id_unco_pix", "ts_oprc", F.col("id_estn").alias("nu_ec"), norm_nome("nm_estn_pix").alias("nm_estn"),
            F.col("cd_tipo_oprc_pix").alias("tp_operacao"), "vl_trns", (F.col("in_acto") == "1").alias("fl_aprovada"),
            "nu_score_totl", F.col("id_rgra").cast("bigint").alias("id_rgra"),
            "nu_doc_pagador", "id_entidade_pagador", norm_nome("tx_pix_nm_pgdr").alias("nm_pagador"),
            conta_key("nu_pix_ispb_pgdr", "nu_pix_agnc_pgdr", "nu_pix_cnta_pgdr").alias("ds_conta_pagador"),
            F.col("tx_chve_pix_pgdr").alias("ds_chave_pix_pagador"), norm_email("tx_usro_pix").alias("ds_email_usuario_pix"),
            "nu_doc_recebedor", "id_entidade_recebedor", norm_nome("nm_recr").alias("nm_recebedor"),
            conta_key("nu_ispb_recr", "nu_agnc_recr", "nu_cnta_recr").alias("ds_conta_recebedor"),
            F.col("tx_chve_pix_recr").alias("ds_chave_pix_recebedor"), F.col("cd_tipo_chve_pix").alias("tp_chave_pix_recebedor"),
            F.col("id_dspi").alias("id_dispositivo"), F.col("tx_geolocalizacao_dspi").alias("ds_geolocalizacao"),
            F.datediff(F.col("ts_oprc"), F.to_date("dt_crca_cnta", "yyyyMMdd")).alias("qt_dias_conta"),
            F.datediff(F.col("ts_oprc"), F.to_date("dt_crca_chve", "yyyyMMdd")).alias("qt_dias_chave"),
            (F.col("qt_d90_stooge_acc_mrca_frde_us") + F.col("qt_d90_frde_acc_mrca_frde_us")).alias("qt_marcacoes_laranja_fraude_90d"),
            F.col("tx_pix_mrca_frde").alias("fl_marcacao_fraude")))
salvar(pix_s, "trns_pix", "Transações Lynx canal PIX - limpas, deduplicadas, pagador e recebedor resolvidos")

# COMMAND ----------

# MAGIC %md ### 4.4 Antecipações / recebíveis (RAD0)

# COMMAND ----------

antp_s = (dedup(antp_b, "id_unco_lynx")
    .withColumn("ts_oprc", para_ts(F.col("dt_antp").cast("string"), "hr_antp"))
    .transform(lambda d: liga_entidade(d, "nu_cnpj_rspe_dmcl", "domicilio"))
    .select("id_unco_lynx", "ts_oprc", F.col("id_estn").cast("bigint").alias("nu_ec"), valor_br("vl_trns").alias("vl_trns"),
            (F.col("in_acto") == "1").alias("fl_aprovada"), "nu_score_totl", F.col("id_rgra").cast("bigint").alias("id_rgra"),
            "nu_doc_domicilio", "id_entidade_domicilio", conta_key("nu_ispb", "nu_agnc", "nu_cnta").alias("ds_conta_domicilio"),
            F.col("cd_bnco").alias("cd_banco"), F.col("tx_arnj_pgmn").alias("cd_arranjo")))
salvar(antp_s, "trns_antecipacao", "Transações Lynx canal antecipações/recebíveis (RAD0) - valores convertidos e deduplicados")

# COMMAND ----------

# MAGIC %md ### 4.5 Fraude reportada, respostas a alertas, regras acionadas e listas

# COMMAND ----------

crto_ref = spark.table(f"{SILVER}.trns_cartao").select("id_unco", "qt_cnar_ordm", "nu_ec", "nu_token_cartao", "id_entidade_portador")

frde_s = (dedup(bz("tbciar_tr_frde"), "id_unco_lynx")
    .select(F.to_timestamp("dh_trns").alias("ts_transacao"), F.to_timestamp("dh_reporte").alias("ts_reporte"), "cd_tipo_rsps_frde",
            (F.col("in_match") == 0).alias("fl_match"),
            F.when(F.col("cd_tipo_reporte") == 3, "LYNX WEB").when(F.col("cd_tipo_reporte") == 1, "EXTERNO").alias("ds_origem_reporte"),
            "id_unco_lynx", "id_trns_orgm")
    .join(crto_ref.withColumnRenamed("id_unco", "id_unco_lynx").drop("qt_cnar_ordm"), "id_unco_lynx", "left"))
salvar(frde_s, "fraude_reportada", "Transações reportadas como fraude, ligadas ao EC, cartão e portador")

rsps_s = (dedup(bz("tbciar_tr_lynx_rsps"), ["id_unco_lynx", "dh_rsps"])
    .select("id_unco_lynx", F.to_timestamp("dh_rsps").alias("ts_resposta"), "cd_tipo_rsps_frde",
            F.element_at(F.create_map(*[F.lit(x) for x in ["1", "FRAUDE CONFIRMADA", "2", "LEGITIMA", "3", "SUSPEITA PLD", "4", "SEM CONTATO"]]),
                         F.col("cd_tipo_rsps_frde")).alias("ds_resposta"),
            F.col("cd_usro").alias("cd_analista"))
    .join(crto_ref.withColumnRenamed("id_unco", "id_unco_lynx").drop("qt_cnar_ordm"), "id_unco_lynx", "left"))
salvar(rsps_s, "resposta_alerta", "Respostas de analistas aos alertas Lynx")

TIPO_REGRA = {"pre": "EMISSOR", "pos": "AUTORIZACAO EMISSOR", "pad": "AUTORIZACAO CIELO", "can": "AUTORIZACAO CANCELAMENTO",
              "ros": "CANCELAMENTO STAR/CAN", "rcl": "SCORE PAGAMENTOS WPY/PIX", "acl": "AUTORIZACAO PAGAMENTOS WPY/PIX",
              "bkr": "SCORE ARV/RA D0", "ant": "AUTORIZACAO ARV/RA D0", "lol": "COMERCIOS TEMPO REAL", "lof": "COMERCIOS NAO TEMPO REAL",
              "spg": "RECEBIVEIS", "rml": "PLD", "rsl": "PARCEIRO"}
rgra_s = (dedup(bz("tbciar_tr_lynx_rgra"), ["nu_ordm_rgra", "id_rgra"])
    .select(F.to_timestamp("dt_hr_envio_rgra").alias("ts_disparo"), "nu_ordm_rgra", "id_rgra", "nu_vrso_rgra", "cd_tipo_rgra",
            F.create_map(*[F.lit(x) for kv in TIPO_REGRA.items() for x in kv])[F.col("cd_tipo_rgra")].alias("ds_categoria_regra"),
            (F.col("cd_tipo_rgra") == "rml").alias("fl_regra_pld"))
    .join(crto_ref.withColumnRenamed("qt_cnar_ordm", "nu_ordm_rgra"), "nu_ordm_rgra", "left"))
salvar(rgra_s, "regra_acionada", "Disparos de regras Lynx com categoria decodificada, ligados à transação de cartão")

# COMMAND ----------

lst_s = None
for nome, (t, o) in LISTAS.items():
    d = (dedup(bz(nome), ["id_objt_prcp", "dt_etra_lsta"])
         .select(F.lit({"psit": "POSITIVA", "ngto": "NEGATIVA", "psit_atzd": "POSITIVA AUTORIZADA"}[t]).alias("tp_lista"),
                 F.lit({"crto": "CARTAO", "clnt": "CLIENTE", "ec": "EC"}[o]).alias("tp_objeto"), "id_lsta",
                 F.when(F.lit(o) == "clnt", F.lpad(F.col("id_objt_prcp").cast("string"), 11, "0"))
                  .otherwise(F.col("id_objt_prcp").cast("string")).alias("cd_objeto"),
                 F.col("id_objt_adcn").alias("ds_objeto_adicional"),
                 F.coalesce(F.try_to_timestamp("dt_etra_lsta", F.lit("yyyy-MM-dd HH:mm:ss")), F.try_to_timestamp("dt_etra_lsta", F.lit("dd/MM/yyyy HH:mm"))).alias("ts_entrada"),
                 F.to_timestamp("dt_sada_lsta").alias("ts_saida"), F.col("cd_usro").alias("cd_analista")))
    lst_s = d if lst_s is None else lst_s.unionByName(d)
lst_s = lst_s.withColumn("fl_ativo", F.col("ts_saida").isNull() | (F.col("ts_saida") > F.current_timestamp()))
lst_s = lst_s.join(ent.select(F.col("nu_doc").alias("cd_objeto"), F.col("id_entidade").alias("id_entidade_cliente")), "cd_objeto", "left")
salvar(lst_s, "lista_monitoramento", "União das 9 listas Lynx (positiva/negativa/positiva autorizada x cartão/cliente/EC) com status ativo")

# COMMAND ----------

# MAGIC %md ### 4.6 Atributos das entidades (e-mail, telefone, conta, dispositivo, chave PIX, cartão, endereço, IP)
# MAGIC Esses atributos serão os **nós de ligação** do grafo: duas entidades/ECs que compartilham uma conta ou um dispositivo ficam a 2 saltos de distância.

# COMMAND ----------

c, p, a, h = (spark.table(f"{SILVER}.{t}") for t in ["trns_cartao", "trns_pix", "trns_antecipacao", "ec_cadastro"])

def attr(df, ent_col, tipo, val_col, fonte, ts_col="ts_oprc"):
    return (df.filter(F.col(ent_col).isNotNull() & F.col(val_col).isNotNull() & (F.col(val_col) != ""))
              .select(F.col(ent_col).alias("id_entidade"), F.lit(tipo).alias("tp_atributo"), F.col(val_col).cast("string").alias("vl_atributo"),
                      F.lit(fonte).alias("fonte"), F.col(ts_col).alias("ts") if ts_col else F.lit(None).cast("timestamp").alias("ts")))

atributos = (attr(c, "id_entidade_portador", "EMAIL", "ds_email_portador", "CARTAO")
    .unionByName(attr(c, "id_entidade_portador", "TELEFONE", "nu_telefone_portador", "CARTAO"))
    .unionByName(attr(c, "id_entidade_portador", "CARTAO", "nu_token_cartao", "CARTAO"))
    .unionByName(attr(c, "id_entidade_portador", "IP", "ds_ip", "CARTAO"))
    .unionByName(attr(c, "id_entidade_portador", "ENDERECO", "ds_endereco_entrega", "CARTAO"))
    .unionByName(attr(p, "id_entidade_pagador", "DISPOSITIVO", "id_dispositivo", "PIX"))
    .unionByName(attr(p, "id_entidade_pagador", "CONTA", "ds_conta_pagador", "PIX"))
    .unionByName(attr(p, "id_entidade_pagador", "CHAVE_PIX", "ds_chave_pix_pagador", "PIX"))
    .unionByName(attr(p, "id_entidade_pagador", "EMAIL", "ds_email_usuario_pix", "PIX"))
    .unionByName(attr(p, "id_entidade_recebedor", "CONTA", "ds_conta_recebedor", "PIX"))
    .unionByName(attr(p, "id_entidade_recebedor", "CHAVE_PIX", "ds_chave_pix_recebedor", "PIX"))
    .unionByName(attr(a, "id_entidade_domicilio", "CONTA", "ds_conta_domicilio", "ANTECIPACAO"))
    .unionByName(attr(h, "id_entidade_titular", "ENDERECO", "ds_endereco_norm", "HUB", None))
    .withColumn("vl_atributo", F.when(F.col("tp_atributo") == "EMAIL", F.lower("vl_atributo")).otherwise(F.col("vl_atributo")))
    .groupBy("id_entidade", "tp_atributo", "vl_atributo")
    .agg(F.array_sort(F.collect_set("fonte")).alias("fontes"), F.count("*").alias("qt_ocorrencias"),
         F.min("ts").alias("ts_primeira_ocorrencia"), F.max("ts").alias("ts_ultima_ocorrencia")))
salvar(atributos, "entidade_atributo", "Atributos de contato/meio de pagamento por entidade resolvida")

# COMMAND ----------

ea = spark.table(f"{SILVER}.entidade_atributo")
compart = ea.groupBy("tp_atributo", "vl_atributo").agg(F.countDistinct("id_entidade").alias("qt_entidades"))
display(compart.groupBy("tp_atributo").agg(F.count("*").alias("valores_distintos"),
                                           F.sum((F.col("qt_entidades") > 1).cast("int")).alias("valores_compartilhados"),
                                           F.max("qt_entidades").alias("max_entidades_por_valor")).orderBy("tp_atributo"))

# COMMAND ----------

# MAGIC %md ## 5. Resumo da Silver

# COMMAND ----------

resumo = [(t.tableName, spark.table(f"{SILVER}.{t.tableName}").count()) for t in spark.sql(f"SHOW TABLES IN {SILVER}").collect()]
display(spark.createDataFrame(resumo, "tabela string, linhas long"))
