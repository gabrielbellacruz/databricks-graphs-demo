-- KPIs da fila de investigação PLD (1 linha)
SELECT
  COUNT(*)                                                    AS qt_lojas,
  SUM(CASE WHEN f.faixa_risco = 'ALTO' THEN 1 ELSE 0 END)    AS qt_alto,
  SUM(f.fl_novo_suspeito)                                     AS qt_novos_suspeitos,
  SUM(f.fl_fraude_confirmada)                                 AS qt_fraude_confirmada,
  COUNT(DISTINCT CASE WHEN g.qt_raizes_componente >= 2 AND g.qt_lojas_componente >= 3 THEN g.id_componente END) AS qt_grupos_suspeitos,
  CAST(MAX(f.ts_score) AS STRING)                             AS ts_atualizacao
FROM cielo_pld.gold.fila_investigacao_pld f
JOIN cielo_pld.gold.features_grafo_loja g ON g.nu_ec = f.nu_ec
