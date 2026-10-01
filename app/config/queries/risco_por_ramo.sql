-- Lojas em risco ALTO por ramo: novos suspeitos x fraude já confirmada
SELECT
  f.nm_ramo                                                        AS ramo,
  SUM(CASE WHEN f.fl_novo_suspeito = 1 THEN 1 ELSE 0 END)          AS novos_suspeitos,
  SUM(CASE WHEN f.fl_fraude_confirmada = 1 THEN 1 ELSE 0 END)      AS fraude_confirmada
FROM cielo_pld.gold.fila_investigacao_pld f
WHERE f.faixa_risco = 'ALTO'
GROUP BY f.nm_ramo
ORDER BY novos_suspeitos + fraude_confirmada DESC
