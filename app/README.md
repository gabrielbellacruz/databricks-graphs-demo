# App `cielo-pld-grafos` — Investigação PLD em grafos

Databricks App (AppKit · React + Node) para o analista interno de PLD/prevenção a fraude da Cielo, empacotado como **DAB** (`databricks.yml`).

| Tela | Rota | O que mostra |
|---|---|---|
| Fila de risco | `/` | KPIs (novos suspeitos, risco ALTO, fraude confirmada, grupos suspeitos), fila priorizada com filtros e principais sinais traduzidos, risco ALTO por ramo |
| Rede de vínculos | `/rede?ec=<EC>` | Ficha da loja + grafo interativo (dono, sócio, contas, telefones, e-mails, endereço, dispositivos, lojas conectadas e PIX ≥ R$ 5 mil) + lista de lojas conectadas |
| Genie Agent | `/genie` | Chat com o Genie Agent "Cielo PLD · Investigação em grafos": SQL gerado visível, status, aviso de IA e perguntas sugeridas; o botão "Explicar com o Genie" das outras telas abre o chat já com a pergunta da loja |

## Dados e identidade

- Consultas em `config/queries/*.sql` (plugin `analytics`) sobre `cielo_pld.gold` / `cielo_pld.silver`, executadas pelo **service principal** do App.
- O **Genie** roda **em nome do usuário** (OBO, escopo `dashboards.genie`) — o plugin `genie()` do AppKit é sempre OBO. Cada analista precisa de
  `CAN_RUN` no Genie Agent e `SELECT` nas tabelas usadas por ele.
- Recursos declarados no `databricks.yml` (permissões concedidas ao service principal no deploy): SQL warehouse (`CAN_USE`), Genie Agent (`CAN_RUN`) e as
  11 tabelas (`SELECT`).

## Genie Agent

Configuração versionada em `genie/build_space.py` → `genie/genie_space.json` (descrições e sinônimos por coluna, joins, filtros, medidas, exemplos SQL
validados e instruções). Para atualizar o Agent depois de editar o script:

```bash
python3 genie/build_space.py
databricks genie update-space 01f1bdacc8141798b11a226a3eb97cac \
  --json "{\"serialized_space\": $(jq -c . genie/genie_space.json | jq -Rs .)}" --profile gabriel-bella-cruz-classic-sandbox
```

## Desenvolvimento e deploy

```bash
npm install                     # usa registry.npmjs.org (.npmrc) — o proxy npm interno não é acessível pelo container do App
npm run dev                     # local, com .env (DATABRICKS_CONFIG_PROFILE, DATABRICKS_WAREHOUSE_ID, DATABRICKS_GENIE_SPACE_ID)
databricks apps validate --profile gabriel-bella-cruz-classic-sandbox
databricks apps deploy -t default --profile gabriel-bella-cruz-classic-sandbox
```

> Se um deploy falhar na instalação de pacotes e o seguinte acusar `ENOTEMPTY` em `node_modules`, rode `databricks apps stop cielo-pld-grafos` e faça o deploy de novo (container limpo).
