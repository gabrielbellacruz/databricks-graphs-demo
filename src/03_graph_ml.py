# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Aplicando ML
# MAGIC **Workshop: Utilizando grafos para PLD** · Parte 3 de 3
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | ⏱️ Duração | ~45 min (+ ~15 min de build do endpoint) |
# MAGIC | 🎯 Objetivo | Medir quanto as **features de grafo** melhoram a detecção, gerar a **fila de investigação priorizada** e **implantar** o modelo |
# MAGIC | 📥 Entrada | `silver.*` (notebook 01) e `gold.features_grafo_loja` (notebook 02) |
# MAGIC | 📤 Saída | experimento MLflow, modelo `cielo_pld.gold.modelo_risco_pld_ec@champion`, `gold.fila_investigacao_pld` e o endpoint `cielo-pld-risco-ec` |
# MAGIC
# MAGIC ### Por que ML depois dos motifs?
# MAGIC * **A fraude se organiza em rede** — o risco de uma loja depende de com quem ela se conecta.
# MAGIC * **Regras só pegam o padrão conhecido** — motifs acham o que sabemos descrever; o modelo aprende combinações que nenhuma regra cobre.
# MAGIC * **Fila priorizada** — um score por loja ordena os ~5 mil casos/mês; o analista começa pelos de maior risco.
# MAGIC * **Risco atualizado depois do onboarding** — quando o grafo muda (nova conta, novo sócio), o score é recalculado.
# MAGIC
# MAGIC ### Roteiro
# MAGIC 1. Features de grafo + features tabulares → **XGBoost** (tabular × tabular + grafo, comparado no MLflow)
# MAGIC 2. **Avaliação** do modelo
# MAGIC 3. **Implantação**: fila de investigação, Unity Catalog e Model Serving
# MAGIC
# MAGIC > ℹ️ Usamos **XGBoost** (e não Spark ML) porque modelos Spark ML não podem ser publicados no Model Serving. As features continuam calculadas no Spark/GraphFrames;
# MAGIC > como há 1 linha por loja, o treino cabe tranquilamente em pandas.

# COMMAND ----------

# MAGIC %md ## 0. Configuração

# COMMAND ----------

import mlflow, numpy as np, pandas as pd, matplotlib.pyplot as plt, xgboost as xgb, sklearn, shap
from pyspark.sql import functions as F, Window as W
from sklearn.model_selection import GroupShuffleSplit, GroupKFold
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_curve, confusion_matrix
from mlflow.models import infer_signature

CATALOGO = "cielo_pld"
SILVER, GOLD = f"{CATALOGO}.silver", f"{CATALOGO}.gold"
spark.sql(f"USE CATALOG {CATALOGO}")
USUARIO = spark.sql("select current_user()").first()[0]
EXPERIMENTO = f"/Users/{USUARIO}/pld-grafos/workshop_graph_ml"
MODELO = f"{GOLD}.modelo_risco_pld_ec"
ENDPOINT = "cielo-pld-risco-ec"

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENTO)
def sv(t): return spark.table(f"{SILVER}.{t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Features de grafo + features tabulares
# MAGIC ### 1.1 Features tabulares — o que o time já usa (cadastro, HUB e transações)

# COMMAND ----------

hub = sv("hub_risco_mensal")
ultimo = hub.withColumn("_rn", F.row_number().over(W.partitionBy("NU_EC").orderBy(F.desc("CD_MES")))).filter("_rn = 1")
historico = hub.groupBy("NU_EC").agg(F.avg("VL_FTRM").alias("_media"), F.stddev("VL_FTRM").alias("_desvio"))
f_hub = (ultimo.join(historico, "NU_EC").select(
    F.col("NU_EC").alias("nu_ec"),
    F.coalesce("VL_FTRM", F.lit(0.0)).alias("faturamento"),
    (F.coalesce("VL_FTRM", F.lit(0.0)) / F.greatest("_media", F.lit(1.0))).alias("razao_faturamento_vs_media"),
    (F.coalesce("_desvio", F.lit(0.0)) / F.greatest("_media", F.lit(1.0))).alias("cv_faturamento"),
    F.coalesce("PC_FTRM_CNP", F.lit(0.0)).alias("pc_faturamento_cnp"),
    F.coalesce("PC_CHRG", F.lit(0.0)).alias("pc_chargeback"), F.coalesce("PC_CANC", F.lit(0.0)).alias("pc_cancelamento"),
    F.coalesce("VL_TCKT_MEDO", F.lit(0.0)).alias("ticket_medio"), F.col("QT_DIAS_AFLC").cast("double").alias("dias_afiliacao"),
    (F.coalesce("VL_ANTP_ARV", F.lit(0.0)) / F.greatest("VL_FTRM", F.lit(1.0))).alias("pc_antecipacao"),
    F.col("IN_SOCO_PEP").cast("double").alias("socio_pep"), F.col("IN_ALTR_QDRO_SCTR").cast("double").alias("alteracao_societaria"),
    (F.col("DC_ALRT_CPTO") != "SEM ALERTA").cast("double").alias("alerta_comportamental"),
    (F.col("DC_STCO_SOCO_RCTA") != "REGULAR").cast("double").alias("socio_irregular_receita"),
    (F.col("TIPO_CLNT") == "E-COMMERCE").cast("double").alias("ecommerce"),
    (F.col("CD_RMAT") >= 11).cast("double").alias("mcc_alto_risco")))
f_cartao = sv("trns_cartao").groupBy("nu_ec").agg(
    F.count("*").cast("double").alias("qt_trns_cartao"),
    *[F.avg(F.col(c).cast("int")).alias(f"pct_{c[3:]}") for c in ["fl_madrugada", "fl_abaixo_limite", "fl_valor_redondo", "fl_pre_pago", "fl_cnp", "fl_regra_pld"]],
    F.avg("nu_score_totl").alias("score_lynx_medio"), F.avg((F.col("cd_pais_crto") != "076").cast("int")).alias("pct_cartao_estrangeiro"))
f_pix = sv("trns_pix").groupBy("nu_ec").agg(F.count("*").cast("double").alias("qt_pix"), F.avg("qt_marcacoes_laranja_90d").alias("marcacoes_laranja_pagadores"),
                                            F.avg((F.col("qt_dias_conta") < 90).cast("int")).alias("pct_pix_conta_recente"))
f_antp = sv("trns_antecipacao").groupBy("nu_ec").agg(F.count("*").cast("double").alias("qt_antecipacoes"))

# COMMAND ----------

# MAGIC %md ### 1.2 Features de grafo (notebook 02) e rótulo

# COMMAND ----------

f_grafo = spark.table(f"{GOLD}.features_grafo_loja")
base = (sv("loja").select("nu_ec", "nm_loja", "nm_ramo", F.col("fl_fraude").alias("fraude"))
        .join(f_hub, "nu_ec", "left").join(f_cartao, "nu_ec", "left").join(f_pix, "nu_ec", "left").join(f_antp, "nu_ec", "left")
        .join(f_grafo, "nu_ec", "left").fillna(0))
pdf = base.toPandas()

TABULARES = [c for c in f_hub.columns + f_cartao.columns + f_pix.columns + f_antp.columns if c != "nu_ec"]
GRAFO = [c for c in f_grafo.columns if c not in ("nu_ec", "id_componente")]
# estas features usam o rótulo de OUTRAS lojas (vizinhança) — veremos no exercício quanto elas pesam
GRAFO_ROTULO_VIZINHOS = ["qt_outras_lojas_fraude_componente", "qt_outras_lojas_fraude_comunidade", "dist_fraude", "pct_vizinhos_fraude", "fl_socio_comum_fraude"]
print(f"{len(pdf):,} lojas | fraude: {pdf.fraude.sum()} ({pdf.fraude.mean():.1%}) | {len(TABULARES)} features tabulares + {len(GRAFO)} de grafo")
display(pdf.groupby("fraude")[["pct_abaixo_limite", "pct_madrugada", "qt_lojas_componente", "pagerank", "qt_triangulos", "qt_ciclos_pix", "fl_conta_compartilhada"]].mean().round(3).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Treino × teste **por componente**
# MAGIC Se lojas do **mesmo anel** caem no treino e no teste, o modelo "cola" — aprende o anel e parece ótimo no teste.
# MAGIC Dividimos pelo `id_componente` do grafo de vínculos: um anel inteiro fica de um lado só.

# COMMAND ----------

grupos = pdf["id_componente"].astype(str).values
splits = GroupShuffleSplit(n_splits=20, test_size=0.3, random_state=42).split(pdf, pdf.fraude, grupos)
i_tr, i_te = min(splits, key=lambda s: abs(pdf.fraude.iloc[s[1]].mean() - pdf.fraude.mean()))   # split com proporção de fraude mais próxima da global
treino, teste = pdf.iloc[i_tr], pdf.iloc[i_te]
assert not set(treino.id_componente) & set(teste.id_componente)
print(f"treino: {len(treino):,} lojas ({treino.fraude.mean():.1%} fraude) | teste: {len(teste):,} lojas ({teste.fraude.mean():.1%} fraude)")

# COMMAND ----------

# MAGIC %md ### 1.4 XGBoost: tabular × tabular + grafo (registrado no MLflow)

# COMMAND ----------

PARAMS = dict(n_estimators=300, max_depth=4, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, min_child_weight=2, eval_metric="aucpr", random_state=42)
TOP_K = 100

def avaliar(y, p):
    top = np.argsort(-p)[:TOP_K]
    return {"roc_auc": roc_auc_score(y, p), "pr_auc": average_precision_score(y, p),
            f"precisao_top{TOP_K}": y[top].mean(), f"recall_top{TOP_K}": y[top].sum() / max(y.sum(), 1)}

def treinar(nome, features):
    peso = (treino.fraude == 0).sum() / max(treino.fraude.sum(), 1)          # compensa o desbalanceamento
    with mlflow.start_run(run_name=nome, nested=True):
        m = xgb.XGBClassifier(**PARAMS, scale_pos_weight=peso).fit(treino[features], treino.fraude)
        p = m.predict_proba(teste[features])[:, 1]
        met = avaliar(teste.fraude.values, p)
        mlflow.log_params({**PARAMS, "n_features": len(features)}); mlflow.log_metrics(met); mlflow.log_dict({"features": features}, "features.json")
        imp = pd.Series(m.feature_importances_, index=features).sort_values().tail(15)
        fig, ax = plt.subplots(figsize=(7, 6)); imp.plot.barh(ax=ax, color="#FF3621"); ax.set_title(f"Importância — {nome}"); plt.tight_layout()
        mlflow.log_figure(fig, "importancia.png"); plt.close(fig)
    return m, features, p, met

resultados = {}
with mlflow.start_run(run_name="tabular_vs_grafo"):
    resultados["A_tabular"] = treinar("A_tabular", TABULARES)
    resultados["B_tabular_grafo"] = treinar("B_tabular_grafo", TABULARES + GRAFO)
display(pd.DataFrame({k: v[3] for k, v in resultados.items()}).T.round(3).reset_index().rename(columns={"index": "modelo"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 1
# MAGIC As features em `GRAFO_ROTULO_VIZINHOS` usam a informação de fraude de **outras** lojas (o "culpado por associação").
# MAGIC Treine um modelo **C** com `TABULARES + GRAFO` **sem** elas. Quanto do ganho vem só da **estrutura** do grafo?

# COMMAND ----------

# ✏️ seu código aqui
# with mlflow.start_run(run_name="exercicio"):
#     resultados["C_..."] = treinar("C_...", [...])

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

with mlflow.start_run(run_name="exercicio_estrutura"):
    resultados["C_tabular_grafo_estrutural"] = treinar("C_tabular_grafo_estrutural", TABULARES + [c for c in GRAFO if c not in GRAFO_ROTULO_VIZINHOS])
display(pd.DataFrame({k: v[3] for k, v in resultados.items()}).T.round(3).reset_index().rename(columns={"index": "modelo"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Avaliação do modelo
# MAGIC Com ~5% de fraude, **acurácia engana** (um modelo que diz "ninguém é fraude" acerta 95%). Olhamos para:
# MAGIC * **PR-AUC** — qualidade da ordenação focada na classe rara
# MAGIC * **Precisão no top 100** — "se o analista abrir os 100 primeiros casos da fila, quantos são fraude?"
# MAGIC * **Recall no top 100** — "que fração das fraudes está nos 100 primeiros?"

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
for nome, (_, _, p, met) in resultados.items():
    pr_, rc_, _ = precision_recall_curve(teste.fraude, p)
    axes[0].plot(rc_, pr_, label=f"{nome} (PR-AUC={met['pr_auc']:.2f})")
    ordem = np.argsort(-p); acum = np.cumsum(teste.fraude.values[ordem]) / teste.fraude.sum()
    axes[1].plot(np.arange(1, len(p) + 1), acum, label=nome)
axes[0].set(xlabel="Recall", ylabel="Precisão", title="Curva Precisão × Recall (teste)"); axes[0].legend()
axes[1].axvline(TOP_K, color="gray", ls="--"); axes[1].set(xlabel="Nº de lojas investigadas (ordem do score)", ylabel="% das fraudes encontradas", title="Fila de investigação"); axes[1].legend()
plt.tight_layout(); display(fig)

# COMMAND ----------

m_b, feats_b, p_b, _ = resultados["B_tabular_grafo"]
corte = np.quantile(p_b, 0.95)
cm = confusion_matrix(teste.fraude, (p_b >= corte).astype(int))
display(pd.DataFrame(cm, index=["real: não fraude", "real: fraude"], columns=["previsto: não fraude", "previsto: fraude"]).reset_index())

# COMMAND ----------

# MAGIC %md **Explicabilidade (SHAP):** quais sinais empurram o score para cima?

# COMMAND ----------

try:
    valores_shap = shap.TreeExplainer(m_b).shap_values(teste[feats_b])
except Exception as ex:          # fallback: contribuições nativas do XGBoost (mesmos valores SHAP)
    valores_shap = m_b.get_booster().predict(xgb.DMatrix(teste[feats_b]), pred_contribs=True)[:, :-1]
plt.figure(); shap.summary_plot(valores_shap, teste[feats_b], max_display=15, show=False)
fig_shap = plt.gcf(); fig_shap.set_size_inches(10, 7); plt.tight_layout(); display(fig_shap)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Implantação
# MAGIC ### 3.1 Fila de investigação priorizada → `gold.fila_investigacao_pld`
# MAGIC Para pontuar a base **atual** sem "colar", usamos scores *out-of-fold*: cada loja é pontuada por um modelo que **não a viu** (nem o seu anel) no treino.
# MAGIC Os **novos suspeitos** são lojas de alto risco que ainda **não** estão na lista negativa.

# COMMAND ----------

FEATURES = feats_b
peso = (pdf.fraude == 0).sum() / max(pdf.fraude.sum(), 1)
oof = np.zeros(len(pdf))
for i_tr_k, i_te_k in GroupKFold(n_splits=5).split(pdf, pdf.fraude, grupos):
    m_k = xgb.XGBClassifier(**PARAMS, scale_pos_weight=peso).fit(pdf.iloc[i_tr_k][FEATURES], pdf.fraude.iloc[i_tr_k])
    oof[i_te_k] = m_k.predict_proba(pdf.iloc[i_te_k][FEATURES])[:, 1]
cortes = {"ALTO": float(np.quantile(oof, 0.95)), "MEDIO": float(np.quantile(oof, 0.85))}
print(f"OOF  ROC-AUC={roc_auc_score(pdf.fraude, oof):.3f} | PR-AUC={average_precision_score(pdf.fraude, oof):.3f}")

modelo_final = xgb.XGBClassifier(**PARAMS, scale_pos_weight=peso).fit(pdf[FEATURES], pdf.fraude)
contrib = modelo_final.get_booster().predict(xgb.DMatrix(pdf[FEATURES]), pred_contribs=True)[:, :-1]
principais = [", ".join(np.array(FEATURES)[np.argsort(-c)[:3]]) for c in contrib]          # 3 sinais que mais elevaram o score

fila = pdf[["nu_ec", "nm_loja", "nm_ramo", "fraude", "id_componente"]].assign(
    score_pld=oof, faixa_risco=np.where(oof >= cortes["ALTO"], "ALTO", np.where(oof >= cortes["MEDIO"], "MEDIO", "BAIXO")), principais_sinais=principais)
fila["posicao_fila"] = fila.score_pld.rank(ascending=False, method="first").astype(int)
fila["fl_novo_suspeito"] = ((fila.fraude == 0) & (fila.faixa_risco == "ALTO")).astype(int)
(spark.createDataFrame(fila).withColumnRenamed("fraude", "fl_fraude_confirmada").withColumn("ts_score", F.current_timestamp())
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.fila_investigacao_pld"))
display(spark.table(f"{GOLD}.fila_investigacao_pld").groupBy("faixa_risco", "fl_fraude_confirmada").count().orderBy("faixa_risco", "fl_fraude_confirmada"))

# COMMAND ----------

display(spark.table(f"{GOLD}.fila_investigacao_pld").filter("fl_novo_suspeito = 1").orderBy("posicao_fila")
        .select("posicao_fila", "nu_ec", "nm_loja", "nm_ramo", F.round("score_pld", 3).alias("score_pld"), "principais_sinais").limit(25))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Registro no Unity Catalog
# MAGIC Empacotamos o modelo como `pyfunc`: recebe as features da loja e devolve **score** e **faixa de risco** — um contrato estável para quem consome.

# COMMAND ----------

class ModeloRiscoPLD(mlflow.pyfunc.PythonModel):
    def __init__(self, modelo, features, cortes):
        self.modelo, self.features, self.cortes = modelo, features, cortes
    def predict(self, context, model_input, params=None):
        p = self.modelo.predict_proba(model_input[self.features].astype(float))[:, 1]
        faixa = np.where(p >= self.cortes["ALTO"], "ALTO", np.where(p >= self.cortes["MEDIO"], "MEDIO", "BAIXO"))
        return pd.DataFrame({"score_pld": p, "faixa_risco": faixa})

wrapper = ModeloRiscoPLD(modelo_final, FEATURES, cortes)
exemplo = pdf[FEATURES].head(5).astype(float)
with mlflow.start_run(run_name="modelo_final") as run:
    mlflow.log_params({**PARAMS, "n_features": len(FEATURES), **{f"corte_{k}": v for k, v in cortes.items()}})
    mlflow.log_metrics({"oof_roc_auc": roc_auc_score(pdf.fraude, oof), "oof_pr_auc": average_precision_score(pdf.fraude, oof),
                        **{f"teste_{k}": v for k, v in resultados["B_tabular_grafo"][3].items()}})
    mlflow.log_figure(fig_shap, "shap.png")
    info = mlflow.pyfunc.log_model(
        artifact_path="modelo", python_model=wrapper, input_example=exemplo, signature=infer_signature(exemplo, wrapper.predict(None, exemplo)),
        registered_model_name=MODELO,
        # pandas em faixa: a versão do cluster (1.5.3) não tem wheel para o Python 3.12 da imagem de serving
        pip_requirements=[f"xgboost=={xgb.__version__}", f"scikit-learn=={sklearn.__version__}", "pandas>=2.0,<3", f"numpy=={np.__version__}"])

cliente = mlflow.MlflowClient()
versao = info.registered_model_version
cliente.set_registered_model_alias(MODELO, "champion", versao)
cliente.update_registered_model(MODELO, description="Score de risco PLD por loja (EC) com features tabulares e de grafo (GraphFrames + Splink). Workshop Cielo - dados sintéticos.")
print(f"✔ {MODELO} versão {versao} → alias @champion")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Model Serving
# MAGIC Um endpoint REST com *scale to zero*: o onboarding (ou o monitoramento contínuo) chama o modelo sempre que o grafo de uma loja muda.
# MAGIC ⏳ O primeiro build do container leva ~10–15 min.

# COMMAND ----------

from datetime import timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()
entidade = [ServedEntityInput(entity_name=MODELO, entity_version=str(versao), workload_size="Small", scale_to_zero_enabled=True)]
if ENDPOINT in {e.name for e in w.serving_endpoints.list()}:
    w.serving_endpoints.update_config_and_wait(name=ENDPOINT, served_entities=entidade, timeout=timedelta(minutes=45))
else:
    w.serving_endpoints.create_and_wait(name=ENDPOINT, config=EndpointCoreConfigInput(name=ENDPOINT, served_entities=entidade), timeout=timedelta(minutes=45))
print(w.serving_endpoints.get(ENDPOINT).state)

# COMMAND ----------

# MAGIC %md ### 3.4 Consultando o endpoint: a "Nova Loja"

# COMMAND ----------

suspeito = fila[fila.fl_novo_suspeito == 1].sort_values("posicao_fila").iloc[0]
registro = pdf.loc[pdf.nu_ec == suspeito.nu_ec, FEATURES].astype(float)
resposta = w.serving_endpoints.query(name=ENDPOINT, dataframe_records=registro.to_dict(orient="records"))
print(f"Loja {suspeito.nm_loja} (EC {suspeito.nu_ec}) →", resposta.predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercício 2 — *e se* a loja não tivesse vínculos?
# MAGIC Zere as features de grafo do `registro` (mantendo as tabulares) e consulte o endpoint de novo. O que acontece com o score?

# COMMAND ----------

# ✏️ seu código aqui
# sem_grafo = registro.copy(); sem_grafo[GRAFO] = 0 ...

# COMMAND ----------

# MAGIC %md ✅ **Solução**

# COMMAND ----------

sem_grafo = registro.copy()
sem_grafo[[c for c in GRAFO if c in sem_grafo.columns]] = 0.0
sem_grafo["dist_fraude"] = 99.0
print("com vínculos:", w.serving_endpoints.query(name=ENDPOINT, dataframe_records=registro.to_dict(orient="records")).predictions)
print("sem vínculos:", w.serving_endpoints.query(name=ENDPOINT, dataframe_records=sem_grafo.to_dict(orient="records")).predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC Chamada externa (REST):
# MAGIC ```bash
# MAGIC curl -X POST "$DATABRICKS_HOST/serving-endpoints/cielo-pld-risco-ec/invocations" \
# MAGIC   -H "Authorization: Bearer $DATABRICKS_TOKEN" -H "Content-Type: application/json" \
# MAGIC   -d '{"dataframe_records": [{"faturamento": 250000.0, "pct_abaixo_limite": 0.31, "qt_lojas_componente": 12, "...": 0}]}'
# MAGIC ```
# MAGIC
# MAGIC ## Resumo
# MAGIC * As **features de grafo** somadas às tabulares melhoram a ordenação da fila (PR-AUC e precisão no top 100) — o ganho vem da **estrutura** e da **vizinhança**.
# MAGIC * A **fila de investigação** traz lojas de alto risco que **ainda não estão na lista negativa**, com os principais sinais de cada uma.
# MAGIC * O modelo está versionado no **Unity Catalog** (`@champion`) e publicado em **Model Serving**.
# MAGIC
# MAGIC ### Próximos passos
# MAGIC * Agendar os notebooks 01 → 03 num **Lakeflow Job** para recalcular o grafo e a fila diariamente.
# MAGIC * Usar **dados reais** (`prd.aura.*`, `prd.re_aura.*`) e o rótulo de fraude confirmada da área.
# MAGIC * Servir as features de grafo de uma **Online Feature Store** para pontuar lojas novas no onboarding em tempo real.
