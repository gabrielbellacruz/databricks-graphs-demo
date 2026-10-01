# Workshop — Utilizando grafos para PLD (Cielo)

Material prático do workshop **"Utilizando grafos para PLD — Graph Analytics e ML"** (deck: *Grafos para PLD*), com dados **100% sintéticos**
que seguem os catálogos em `docs/`:

- `Catalogo_Dados_RDC_20260928.pdf` → `prd.re_aura.tbciar_re_hub_dado_risc_cred`
- `Referencia_Analiticos_Lynx_Databricks.pdf` → tabelas Lynx `prd.aura.*` (cartões, PIX, RAD0, fraude, alertas, regras e 9 listas)

## Os 3 notebooks

| # | Notebook | Conteúdo |
|---|---|---|
| 01 | `notebooks/01_preparacao.ipynb` | EDA · padronização de documentos (DV), telefones (E.164) e endereços · construção de vértices e arestas |
| 02 | `notebooks/02_graph_analytics.ipynb` | Motifs (conta compartilhada, sócio em comum com loja fraudada, ciclos) · resolução de identidade com **Splink** + `connectedComponents` · PageRank e métricas de centralidade |
| 03 | `notebooks/03_graph_ml.ipynb` | Features de grafo + **XGBoost** (tabular × grafo no MLflow) · avaliação (PR-AUC, precisão no top 100, SHAP) · implantação (fila de investigação, Unity Catalog, Model Serving) |

Cada notebook tem explicação curta de cada conceito (ligada ao slide correspondente), exercícios opcionais **✏️** com **✅ solução** e células de **checkpoint**.
Todos rodam com *Run all*.

> **Spark ML × XGBoost:** o deck (slide 15) mostra `VectorAssembler` + `GBTClassifier`. O notebook 03 usa XGBoost porque modelos Spark ML não podem ser
> publicados no Model Serving (o container do endpoint não tem JVM). As features continuam sendo calculadas no Spark/GraphFrames.

## Ambiente

| Item | Valor |
|---|---|
| Workspace | `dbc-97a20602-e668.cloud.databricks.com` (id 7474647724744831) — profile CLI `gabriel-bella-cruz-classic-sandbox` |
| Catálogo | `cielo_pld`: `bronze` (fonte, somente leitura), `silver`, `gold` (compartilhados — workshop conduzido pelo instrutor) |
| Compute | cluster `pld-grafos-ml` — DBR **16.4 LTS ML**, acesso **Dedicated** (GraphFrames exige; em serverless os algoritmos falham) |
| Bibliotecas do cluster | PyPI `splink==4.0.17` + JAR `/Volumes/cielo_pld/bronze/libs/scala-udf-similarity-0.2.2_spark3.jar` (UDFs `jaro_winkler` etc. do Splink) |
| Notebooks no workspace | `/Users/gabriel.bellamartini@databricks.com/pld-grafos/` |
| Modelo UC / Endpoint | `cielo_pld.gold.modelo_risco_pld_ec@champion` / `cielo-pld-risco-ec` |

## App de investigação (`app/`)

Databricks App **`cielo-pld-grafos`** (AppKit, DAB em `app/databricks.yml`) para o analista de PLD: fila de risco, rede de vínculos em grafo e Genie Agent
"Cielo PLD · Investigação em grafos" (`01f1bdacc8141798b11a226a3eb97cac`). URL: https://cielo-pld-grafos-7474647724744831.aws.databricksapps.com — ver `app/README.md`.

## Setup do instrutor (uma vez, antes do workshop)

1. Cluster DBR 16.4 LTS ML (Dedicated) com as duas bibliotecas acima instaladas.
2. Rodar `_instrutor/00_gerar_dados_bronze.py` — gera a Bronze sintética com **exatamente** as colunas/tipos dos catálogos e as tipologias de PLD
   (anéis de lojas com conta/endereço/telefone/dispositivo e **sócio** em comum, fracionamento, PIX em ciclo de 3 e 4 saltos, laranjas, antecipação agressiva).
3. Executar os notebooks 01 → 03 uma vez para validar (o 03 cria/atualiza o endpoint, ~15 min).

Para rodar via CLI: `scripts/run_nb.sh <notebook>` (ex.: `01_preparacao`, `_instrutor/00_gerar_dados_bronze`). Os fontes editáveis estão em `src/*.py`
(formato Databricks); os `.ipynb` em `notebooks/` são exportados do workspace.

## Premissas sobre os dados

- **Sócio**: o catálogo não tem tabela de quadro societário; usamos o **responsável pela maquininha** (`nu_cnpj_cpf_mqnt`, RAD0) como sócio/responsável da loja.
- Os 9 layouts de lista seguem `tbciar_tr_lynx_lsta_{psit|ngto|psit_atzd}_{crto|clnt|ec}` (o documento cita só `psit_crto` e `ngto_ec`).
- Campos de controle de carga Lynx (sem nome no documento): `dt_crga`, `nm_arqv_orgm`, `dt_prtc`.
- `tbciar_tr_lynx_rgra` não tem chave de transação no layout; `nu_ordm_rgra` = `qt_cnar_ordm` da transação de cartão.
- Rótulo `fraude` = loja com inserção **ativa** na lista negativa de EC.
