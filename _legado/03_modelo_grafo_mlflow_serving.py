# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Modelo de risco PLD com features de grafo → MLflow → Unity Catalog → Model Serving
# MAGIC
# MAGIC **Pergunta de negócio:** *quais ECs têm perfil de lavagem de dinheiro e ainda não estão na lista negativa?*
# MAGIC
# MAGIC * **Rótulo:** EC com inserção **ativa** na lista negativa de EC Lynx (`silver.lista_monitoramento`).
# MAGIC * **Experimento controlado** (mesmo algoritmo, mesmos dados, só muda o conjunto de features):
# MAGIC
# MAGIC | Run | Features |
# MAGIC |---|---|
# MAGIC | `A_tabular` | cadastro/risco (HUB) + comportamento transacional (cartão, PIX, antecipação) |
# MAGIC | `B_tabular_grafo_estrutural` | A + features estruturais do grafo (não usam rótulos) |
# MAGIC | `C_tabular_grafo_completo` | B + risco de vizinhança (rótulos de *outros* ECs: componente, comunidade, PPR, distância) |
# MAGIC
# MAGIC * **Validação sem vazamento:** divisão treino/teste **por componente conexo** — ECs do mesmo anel nunca ficam dos dois lados.
# MAGIC * O melhor modelo é registrado no **Unity Catalog** (alias `champion`), usado em *batch scoring* (Gold) e publicado em um endpoint de **Model Serving**.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import mlflow, numpy as np, pandas as pd, matplotlib.pyplot as plt, xgboost as xgb, sklearn, shap
from pyspark.sql import functions as F, Window as W
from sklearn.model_selection import GroupShuffleSplit
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_curve
from mlflow.models import infer_signature

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)
def sv(t): return spark.table(f"{SILVER}.{t}")
def gd(t): return spark.table(f"{GOLD}.{t}")

# COMMAND ----------

# MAGIC %md ## 1. Montagem do dataset (1 linha por EC)
# MAGIC ### 1.1 Features tabulares — HUB (último mês + tendência)

# COMMAND ----------

hub = sv("hub_risco_credito")
ult = hub.withColumn("_rn", F.row_number().over(W.partitionBy("NU_EC").orderBy(F.desc("CD_MES")))).filter("_rn = 1")
tend = hub.groupBy("NU_EC").agg(F.avg("VL_FTRM").alias("vl_ftrm_medio_6m"), F.min("CD_MES").alias("cd_mes_ini"),
                                F.max("VL_FTRM").alias("vl_ftrm_max_6m"), F.stddev("VL_FTRM").alias("vl_ftrm_desvio_6m"))
f_hub = (ult.join(tend, "NU_EC").select(
    F.col("NU_EC").alias("nu_ec"),
    F.coalesce("VL_FTRM", F.lit(0.0)).alias("vl_faturamento"),
    (F.coalesce("VL_FTRM", F.lit(0.0)) / F.greatest("vl_ftrm_medio_6m", F.lit(1.0))).alias("razao_fat_ultimo_vs_media"),
    (F.coalesce("vl_ftrm_desvio_6m", F.lit(0.0)) / F.greatest("vl_ftrm_medio_6m", F.lit(1.0))).alias("cv_faturamento_6m"),
    F.coalesce("PC_FTRM_CNP", F.lit(0.0)).alias("pc_faturamento_cnp"),
    (F.coalesce("VL_FTRM_CNP_LINK", F.lit(0.0)) / F.greatest("VL_FTRM", F.lit(1.0))).alias("pc_faturamento_link"),
    F.coalesce("PC_CHRG", F.lit(0.0)).alias("pc_chargeback"), F.coalesce("PC_CANC", F.lit(0.0)).alias("pc_cancelamento"),
    F.coalesce("VL_TCKT_MEDO", F.lit(0.0)).alias("vl_ticket_medio"), F.col("QT_DIAS_AFLC").cast("double").alias("qt_dias_afiliacao"),
    F.col("IN_SOCO_PEP").cast("double").alias("in_socio_pep"), F.col("IN_ALTR_QDRO_SCTR").cast("double").alias("in_alteracao_societaria"),
    (F.coalesce("VL_ANTP_ARV", F.lit(0.0)) / F.greatest("VL_FTRM", F.lit(1.0))).alias("pc_antecipacao_arv"),
    F.coalesce("PC_CNCN_ARV", F.lit(0.0)).alias("pc_concentracao_arv"),
    F.col("VL_RSRV").isNotNull().cast("double").alias("fl_reserva"),
    (F.coalesce("VL_DBTO_PNDT_TOTL", F.lit(0.0)) > 0).cast("double").alias("fl_debito_pendente"),
    (F.col("DC_ALRT_CPTO") != "SEM ALERTA").cast("double").alias("fl_alerta_comportamental"),
    (F.col("DC_STCO_SOCO_RCTA") != "REGULAR").cast("double").alias("fl_socio_irregular_receita"),
    (F.col("TIPO_CLNT") == "E-COMMERCE").cast("double").alias("fl_ecommerce"), (F.col("TIPO_CLNT") == "SUB").cast("double").alias("fl_subadquirente"),
    (F.col("CD_RMAT") >= 11).cast("double").alias("fl_mcc_alto_risco_pld"),
    (F.col("DC_CNAL_AFLC") == "DIGITAL").cast("double").alias("fl_afiliacao_digital")))

# COMMAND ----------

# MAGIC %md ### 1.2 Features transacionais (cartão, PIX, antecipação)

# COMMAND ----------

f_crt = sv("trns_cartao").groupBy("nu_ec").agg(
    F.count("*").cast("double").alias("qt_trns_cartao"), F.avg(F.col("fl_madrugada").cast("int")).alias("pct_trns_madrugada"),
    F.avg(F.col("fl_abaixo_limite").cast("int")).alias("pct_trns_abaixo_limite"), F.avg(F.col("fl_valor_redondo").cast("int")).alias("pct_trns_valor_redondo"),
    F.avg(F.col("fl_pre_pago").cast("int")).alias("pct_trns_pre_pago"), F.avg(F.col("fl_cnp").cast("int")).alias("pct_trns_cnp"),
    F.avg("nu_score_totl").alias("score_lynx_medio"), F.avg(F.col("id_rgra_pld").isNotNull().cast("int")).alias("pct_trns_regra_pld"),
    F.avg((F.col("cd_pais_crto") != "076").cast("int")).alias("pct_cartao_estrangeiro"),
    (F.countDistinct("nu_token_cartao") / F.count("*")).alias("razao_cartoes_distintos"))
f_pix = sv("trns_pix").groupBy("nu_ec").agg(
    F.count("*").cast("double").alias("qt_pix"), F.sum("vl_trns").alias("vl_pix_total"),
    F.avg(F.col("qt_marcacoes_laranja_fraude_90d")).alias("media_marcacoes_laranja_pagador"),
    F.avg((F.col("qt_dias_conta") < 90).cast("int")).alias("pct_pix_conta_recente"))
f_antp = sv("trns_antecipacao").groupBy("nu_ec").agg(
    F.count("*").cast("double").alias("qt_antecipacoes"), F.countDistinct("ds_conta_domicilio").cast("double").alias("qt_contas_domicilio_antecipacao"))

# COMMAND ----------

# MAGIC %md ### 1.3 Features de grafo (Gold) e rótulo

# COMMAND ----------

GRAFO_ESTRUTURAL = ["grau_total", "grau_identidade", "qt_ecs_compartilham_atributo", "qt_ecs_raiz_diferente_compartilham", "max_atributos_compartilhados_par",
                    "qt_atributos_compartilhados", "qt_contas_compartilhadas", "qt_dispositivos_compartilhados", "qt_ecs_componente", "qt_raizes_componente",
                    "qt_entidades_componente", "qt_vertices_comunidade", "qt_ecs_comunidade", "qt_triangulos", "pagerank_pix_titular",
                    "qt_pagadores_pix_titular", "qt_destinatarios_pix_titular", "pct_repasse", "vl_pix_entrada_titular", "vl_pix_saida_titular", "qt_ciclos_pix"]
GRAFO_VIZINHANCA = ["pct_lista_negativa_componente", "pct_lista_negativa_comunidade", "ppr_lista_negativa", "dist_min_lista_negativa", "taxa_fraude_componente"]
f_grafo = gd("ec_features_grafo").select("nu_ec", "component", *GRAFO_ESTRUTURAL, *GRAFO_VIZINHANCA)

rotulo = (sv("lista_monitoramento").filter("tp_lista = 'NEGATIVA' and tp_objeto = 'EC' and fl_ativo")
          .select(F.col("cd_objeto").cast("bigint").alias("nu_ec")).distinct().withColumn("label", F.lit(1)))

base = (sv("ec_cadastro").select("nu_ec", "nm_ec", "nm_ramo").join(f_hub, "nu_ec", "left").join(f_crt, "nu_ec", "left")
        .join(f_pix, "nu_ec", "left").join(f_antp, "nu_ec", "left").join(f_grafo, "nu_ec", "left").join(rotulo, "nu_ec", "left").fillna(0))
pdf = base.toPandas()
TABULAR = [c for c in f_hub.columns + f_crt.columns + f_pix.columns + f_antp.columns if c != "nu_ec"]
CONJUNTOS = {"A_tabular": TABULAR, "B_tabular_grafo_estrutural": TABULAR + GRAFO_ESTRUTURAL,
             "C_tabular_grafo_completo": TABULAR + GRAFO_ESTRUTURAL + GRAFO_VIZINHANCA}
print(f"ECs: {len(pdf):,} | positivos: {pdf.label.sum()} ({pdf.label.mean():.2%}) | features: tabular={len(TABULAR)}, "
      f"estrutural={len(GRAFO_ESTRUTURAL)}, vizinhança={len(GRAFO_VIZINHANCA)}")

# COMMAND ----------

# MAGIC %md ## 2. Split por componente conexo (sem vazamento entre membros do mesmo anel)

# COMMAND ----------

gss = GroupShuffleSplit(n_splits=20, test_size=0.3, random_state=42)
grupos = pdf["component"].astype(str).values
# escolhe o split cuja taxa de positivos no teste é mais próxima da global
melhor = min(gss.split(pdf, pdf.label, grupos), key=lambda s: abs(pdf.label.iloc[s[1]].mean() - pdf.label.mean()))
tr, te = pdf.iloc[melhor[0]], pdf.iloc[melhor[1]]
assert not set(tr.component) & set(te.component)
print(f"treino: {len(tr):,} ({tr.label.mean():.2%} pos) | teste: {len(te):,} ({te.label.mean():.2%} pos)")

# COMMAND ----------

# MAGIC %md ## 3. Treino e comparação no MLflow

# COMMAND ----------

PARAMS = dict(n_estimators=400, max_depth=4, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, min_child_weight=2,
              reg_lambda=1.0, eval_metric="aucpr", random_state=42, n_jobs=-1)
TOP_K = 100

def metricas(y, p):
    ordem = np.argsort(-p); top = ordem[:TOP_K]
    return {"roc_auc": roc_auc_score(y, p), "pr_auc": average_precision_score(y, p),
            f"precision_at_{TOP_K}": y[top].mean(), f"recall_at_{TOP_K}": y[top].sum() / max(y.sum(), 1)}

resultados, modelos = {}, {}
with mlflow.start_run(run_name="comparacao_features_grafo") as pai:
    mlflow.set_tags({"projeto": "cielo-pld-grafos", "rotulo": "lista_negativa_ec_ativa", "split": "GroupShuffleSplit(component)"})
    for nome, feats in CONJUNTOS.items():
        with mlflow.start_run(run_name=nome, nested=True):
            spw = (tr.label == 0).sum() / max((tr.label == 1).sum(), 1)
            m = xgb.XGBClassifier(**PARAMS, scale_pos_weight=spw)
            m.fit(tr[feats], tr.label, eval_set=[(te[feats], te.label)], verbose=False)
            p = m.predict_proba(te[feats])[:, 1]
            met = metricas(te.label.values, p)
            mlflow.log_params({**PARAMS, "scale_pos_weight": round(spw, 2), "n_features": len(feats), "conjunto": nome})
            mlflow.log_metrics(met)
            mlflow.log_dict({"features": feats}, "features.json")
            imp = pd.Series(m.feature_importances_, index=feats).sort_values().tail(20)
            fig, ax = plt.subplots(figsize=(8, 7)); imp.plot.barh(ax=ax, color="#1f77b4"); ax.set_title(f"Importância (gain) — {nome}")
            plt.tight_layout(); mlflow.log_figure(fig, "importancia_features.png"); plt.close(fig)
            resultados[nome], modelos[nome] = met, (m, feats, p)
    comp = pd.DataFrame(resultados).T
    mlflow.log_table(comp.reset_index().rename(columns={"index": "conjunto"}), "comparacao.json")
    fig, ax = plt.subplots(figsize=(7, 5))
    for nome, (_, _, p) in modelos.items():
        pr_, rc_, _ = precision_recall_curve(te.label.values, p)
        ax.plot(rc_, pr_, label=f"{nome} (AP={resultados[nome]['pr_auc']:.3f})")
    ax.set_xlabel("Recall"); ax.set_ylabel("Precision"); ax.set_title("Curva Precision-Recall (teste)"); ax.legend()
    mlflow.log_figure(fig, "curva_pr_comparacao.png")
display(comp.round(4).reset_index().rename(columns={"index": "conjunto"}))

# COMMAND ----------

display(fig)

# COMMAND ----------

# MAGIC %md ### 3.1 Explicabilidade (SHAP) do melhor modelo

# COMMAND ----------

CAMPEAO = comp["pr_auc"].idxmax()
m_best, feats_best, _ = modelos[CAMPEAO]
print(f"Campeão: {CAMPEAO}")
try:
    sv_te = shap.TreeExplainer(m_best).shap_values(te[feats_best])
except Exception as ex:  # fallback: contribuições nativas do XGBoost (mesmos valores SHAP)
    print(f"shap.TreeExplainer indisponível ({type(ex).__name__}); usando pred_contribs do XGBoost")
    sv_te = m_best.get_booster().predict(xgb.DMatrix(te[feats_best]), pred_contribs=True)[:, :-1]
plt.figure()
shap.summary_plot(sv_te, te[feats_best], max_display=20, show=False)
fig_shap = plt.gcf(); fig_shap.set_size_inches(10, 8); plt.tight_layout()
display(fig_shap)

# COMMAND ----------

# MAGIC %md ## 4. Registro no Unity Catalog
# MAGIC O modelo final é treinado com **todos** os ECs e empacotado como `pyfunc`, devolvendo `score_pld` e `faixa_risco` — contrato estável para o endpoint.

# COMMAND ----------

class ModeloRiscoPLD(mlflow.pyfunc.PythonModel):
    def __init__(self, modelo, features, cortes):
        self.modelo, self.features, self.cortes = modelo, features, cortes
    def predict(self, context, model_input, params=None):
        X = model_input[self.features].astype(float)
        p = self.modelo.predict_proba(X)[:, 1]
        faixa = np.where(p >= self.cortes["alto"], "ALTO", np.where(p >= self.cortes["medio"], "MEDIO", "BAIXO"))
        return pd.DataFrame({"score_pld": p, "faixa_risco": faixa})

from sklearn.model_selection import GroupKFold
spw = (pdf.label == 0).sum() / max((pdf.label == 1).sum(), 1)
# scores out-of-fold (GroupKFold por componente): cada EC é pontuado por um modelo que não viu ele nem o seu anel.
# São esses scores que vão para o batch scoring e definem os cortes de faixa — sem isso o modelo "decora" a lista negativa.
oof = np.zeros(len(pdf))
for i_tr, i_te in GroupKFold(n_splits=5).split(pdf, pdf.label, grupos):
    m_cv = xgb.XGBClassifier(**PARAMS, scale_pos_weight=spw).fit(pdf.iloc[i_tr][feats_best], pdf.label.iloc[i_tr])
    oof[i_te] = m_cv.predict_proba(pdf.iloc[i_te][feats_best])[:, 1]
print(f"OOF: ROC-AUC={roc_auc_score(pdf.label, oof):.3f} | PR-AUC={average_precision_score(pdf.label, oof):.3f}")
cortes = {"alto": float(np.quantile(oof, 0.95)), "medio": float(np.quantile(oof, 0.85))}
final = xgb.XGBClassifier(**PARAMS, scale_pos_weight=spw).fit(pdf[feats_best], pdf.label)
wrapper = ModeloRiscoPLD(final, feats_best, cortes)
exemplo = pdf[feats_best].head(5).astype(float)
assinatura = infer_signature(exemplo, wrapper.predict(None, exemplo))

with mlflow.start_run(run_name=f"final_{CAMPEAO}") as run_final:
    mlflow.log_params({**PARAMS, "conjunto": CAMPEAO, "n_features": len(feats_best), "corte_alto": cortes["alto"], "corte_medio": cortes["medio"]})
    mlflow.log_metrics({**{f"teste_{k}": v for k, v in resultados[CAMPEAO].items()},
                        "oof_roc_auc": roc_auc_score(pdf.label, oof), "oof_pr_auc": average_precision_score(pdf.label, oof)})
    mlflow.log_figure(fig_shap, "shap_summary.png")
    info = mlflow.pyfunc.log_model(
        artifact_path="modelo", python_model=wrapper, signature=assinatura, input_example=exemplo,
        registered_model_name=MODEL_NAME,
        # pandas em faixa (não fixo): a versão do cluster (1.5.3) não tem wheel para o Python 3.12 da imagem de serving
        pip_requirements=[f"xgboost=={xgb.__version__}", f"scikit-learn=={sklearn.__version__}", "pandas>=2.0,<3", f"numpy=={np.__version__}"])

client = mlflow.MlflowClient()
versao = info.registered_model_version
client.set_registered_model_alias(MODEL_NAME, "champion", versao)
client.update_registered_model(MODEL_NAME, description="Score de risco PLD por Estabelecimento Comercial usando features tabulares e de grafo (GraphFrames). Demo Cielo - dados sintéticos.")
client.update_model_version(MODEL_NAME, versao, description=f"Conjunto {CAMPEAO} | PR-AUC teste={resultados[CAMPEAO]['pr_auc']:.3f} | ROC-AUC teste={resultados[CAMPEAO]['roc_auc']:.3f}")
print(f"Registrado {MODEL_NAME} v{versao} (alias champion)")

# COMMAND ----------

# MAGIC %md ## 5. Batch scoring → `gold.ec_score_pld`
# MAGIC O resultado mais valioso para o time de PLD: ECs de **alto risco que ainda não estão na lista negativa**.
# MAGIC
# MAGIC Para a base atual usamos os scores **out-of-fold** (cada EC pontuado por um modelo que não o viu no treino) — é a forma honesta de
# MAGIC revelar ECs suspeitos que ainda não estão listados. O modelo `champion` (treinado com todos os ECs) é o que atende novos ECs/meses via endpoint.

# COMMAND ----------

champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
_ = champion.predict(pdf[feats_best].head(5).astype(float))   # sanidade do modelo registrado
faixa_oof = np.where(oof >= cortes["alto"], "ALTO", np.where(oof >= cortes["medio"], "MEDIO", "BAIXO"))
out = pdf[["nu_ec", "nm_ec", "nm_ramo", "component", "label"]].assign(score_pld=oof, faixa_risco=faixa_oof)
out["rank_risco"] = out.score_pld.rank(ascending=False, method="first").astype(int)
out["fl_novo_suspeito"] = ((out.label == 0) & (out.faixa_risco == "ALTO")).astype(int)
sdf = (spark.createDataFrame(out).withColumnRenamed("label", "fl_lista_negativa").withColumnRenamed("component", "id_componente")
       .withColumn("versao_modelo", F.lit(str(versao))).withColumn("ts_score", F.current_timestamp()))
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.ec_score_pld")
spark.sql(f"COMMENT ON TABLE {GOLD}.ec_score_pld IS 'Score de risco PLD por EC (modelo {MODEL_NAME}@champion)'")
display(spark.table(f"{GOLD}.ec_score_pld").groupBy("faixa_risco", "fl_lista_negativa").count().orderBy("faixa_risco", "fl_lista_negativa"))

# COMMAND ----------

novos = (spark.table(f"{GOLD}.ec_score_pld").filter("fl_novo_suspeito = 1")
         .join(gd("ec_features_grafo").select(F.col("nu_ec"), "qt_ecs_componente", "qt_raizes_componente", "pct_lista_negativa_componente",
                                              "qt_contas_compartilhadas", "qt_ciclos_pix", "dist_min_lista_negativa"), "nu_ec")
         .orderBy("rank_risco"))
display(novos.limit(25))

# COMMAND ----------

# MAGIC %md ## 6. Deploy — Model Serving (endpoint REST, *scale to zero*)

# COMMAND ----------

from datetime import timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()
cfg = EndpointCoreConfigInput(name=ENDPOINT_NAME, served_entities=[ServedEntityInput(
    entity_name=MODEL_NAME, entity_version=str(versao), workload_size="Small", scale_to_zero_enabled=True)])
existentes = {e.name for e in w.serving_endpoints.list()}
if ENDPOINT_NAME in existentes:
    w.serving_endpoints.update_config_and_wait(name=ENDPOINT_NAME, served_entities=cfg.served_entities, timeout=timedelta(minutes=45))
else:
    w.serving_endpoints.create_and_wait(name=ENDPOINT_NAME, config=cfg, timeout=timedelta(minutes=45))
ep = w.serving_endpoints.get(ENDPOINT_NAME)
print(ep.state)

# COMMAND ----------

# MAGIC %md ### 6.1 Chamada ao endpoint (mesmo contrato que um sistema de monitoramento PLD usaria)

# COMMAND ----------

amostra = pdf.sort_values("label", ascending=False).head(3)[feats_best].astype(float)
resp = w.serving_endpoints.query(name=ENDPOINT_NAME, dataframe_records=amostra.to_dict(orient="records"))
display(pd.concat([pdf.loc[amostra.index, ["nu_ec", "nm_ec", "label"]].reset_index(drop=True), pd.DataFrame(resp.predictions)], axis=1))

# COMMAND ----------

# MAGIC %md Exemplo de chamada externa (REST):
# MAGIC ```bash
# MAGIC curl -X POST "$DATABRICKS_HOST/serving-endpoints/cielo-pld-risco-ec/invocations" \
# MAGIC   -H "Authorization: Bearer $DATABRICKS_TOKEN" -H "Content-Type: application/json" \
# MAGIC   -d '{"dataframe_records": [{"vl_faturamento": 250000.0, "pct_trns_abaixo_limite": 0.31, "qt_contas_compartilhadas": 2, "...": 0}]}'
# MAGIC ```
