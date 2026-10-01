# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração compartilhada — Demo PLD Cielo (Grafos)
# MAGIC
# MAGIC Arquitetura medalhão no catálogo `cielo_pld`:
# MAGIC | Camada | Schema | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | Bronze | `cielo_pld.bronze` | Réplica sintética das tabelas `prd.aura.*` (Lynx) e `prd.re_aura.tbciar_re_hub_dado_risc_cred`, com os mesmos nomes, colunas e tipos do catálogo de dados — inclusive a "sujeira" típica de origem |
# MAGIC | Silver | `cielo_pld.silver` | Dados limpos, tipados, deduplicados + **resolução de identidade** (entidades e atributos) |
# MAGIC | Gold | `cielo_pld.gold` | Grafo de relacionamentos (vértices/arestas), features de grafo, alertas de comunidades e scores do modelo |
# MAGIC
# MAGIC **Compute:** cluster clássico *Dedicated* com **Databricks Runtime 16.4 LTS ML** (já inclui GraphFrames).

# COMMAND ----------

CATALOG = "cielo_pld"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

USER = spark.sql("select current_user()").first()[0]
EXPERIMENT_PATH = f"/Users/{USER}/pld-grafos/exp_pld_risco_ec"
MODEL_NAME = f"{CATALOG}.gold.modelo_risco_pld_ec"
ENDPOINT_NAME = "cielo-pld-risco-ec"

spark.sql(f"USE CATALOG {CATALOG}")
print(f"Bronze: {BRONZE} | Silver: {SILVER} | Gold: {GOLD}")
