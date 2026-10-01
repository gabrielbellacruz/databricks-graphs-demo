#!/bin/zsh
# uso: scripts/run_nb.sh <caminho_relativo_no_workspace>   ex.: 01_preparacao | _instrutor/00_gerar_dados_bronze
# importa src/*.py e _instrutor/*.py para o workspace e executa um notebook no cluster do workshop
P=gabriel-bella-cruz-classic-sandbox
DIR=/Users/gabriel.bellamartini@databricks.com/pld-grafos
CLUSTER=0930-183645-w5etqb5s
ROOT=$(cd "$(dirname "$0")/.." && pwd)
databricks workspace mkdirs $DIR/_instrutor --profile $P
for f in $ROOT/src/*.py; do databricks workspace import $DIR/$(basename $f .py) --file $f --language PYTHON --format SOURCE --overwrite --profile $P; done
for f in $ROOT/_instrutor/*.py; do databricks workspace import $DIR/_instrutor/$(basename $f .py) --file $f --language PYTHON --format SOURCE --overwrite --profile $P; done
NB=$1
RUN=$(databricks jobs submit --no-wait --profile $P --json "{\"run_name\":\"pld-$(basename $NB)\",\"tasks\":[{\"task_key\":\"t\",\"existing_cluster_id\":\"$CLUSTER\",\"notebook_task\":{\"notebook_path\":\"$DIR/$NB\"}}]}" -o json | jq -r .run_id)
echo "run_id=$RUN"
while true; do
  S=$(databricks jobs get-run $RUN --profile $P -o json)
  LS=$(echo $S | jq -r .state.life_cycle_state)
  [[ $LS == TERMINATED || $LS == INTERNAL_ERROR || $LS == SKIPPED ]] && break
  sleep 20
done
echo $S | jq -r '.state.result_state, .run_page_url'
TID=$(echo $S | jq -r '.tasks[0].run_id')
databricks jobs get-run-output $TID --profile $P -o json > /tmp/run_out_$RUN.json
python3 -c "
import json,re
d=json.loads(open('/tmp/run_out_$RUN.json').read(),strict=False)
if d.get('error'):
    print('ERROR:', d['error'][:1500])
    t=re.sub(r'\x1b\[[0-9;]*m','',d.get('error_trace',''))
    print('\n'.join([l for l in t.splitlines() if not l.strip().startswith('at ')][-25:])[:3000])"
