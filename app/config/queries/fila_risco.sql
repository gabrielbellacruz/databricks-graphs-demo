-- Fila de investigação priorizada (top 200 com filtros)
-- @param faixa STRING = ALTO
-- @param somente_novos STRING = N
-- @param busca STRING
SELECT
  f.posicao_fila,
  f.nu_ec,
  f.nm_loja,
  f.nm_ramo,
  l.sg_uf,
  ROUND(f.score_pld, 4)            AS score_pld,
  f.faixa_risco,
  f.fl_fraude_confirmada,
  f.fl_novo_suspeito,
  f.principais_sinais,
  g.qt_lojas_componente,
  g.fl_conta_compartilhada,
  g.fl_socio_comum_fraude,
  g.qt_ciclos_pix,
  g.dist_fraude
FROM cielo_pld.gold.fila_investigacao_pld f
JOIN cielo_pld.silver.loja l ON l.nu_ec = f.nu_ec
JOIN cielo_pld.gold.features_grafo_loja g ON g.nu_ec = f.nu_ec
WHERE (:faixa = '' OR f.faixa_risco = :faixa)
  AND (:somente_novos = 'N' OR f.fl_novo_suspeito = 1)
  AND (:busca = '' OR LOWER(f.nm_loja) LIKE CONCAT('%', LOWER(:busca), '%') OR CAST(f.nu_ec AS STRING) LIKE CONCAT(:busca, '%'))
ORDER BY f.posicao_fila
LIMIT 200
