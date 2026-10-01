-- Ficha da loja: score, cadastro e métricas de grafo
-- @param nu_ec BIGINT = 2112816701
SELECT
  f.nu_ec, f.nm_loja, l.nm_razao_social, l.tp_pessoa, l.nm_ramo, l.sg_uf, l.nm_municipio, l.ds_endereco,
  l.qt_dias_afiliacao, ROUND(l.vl_faturamento, 2) AS vl_faturamento, ROUND(l.pc_chargeback, 2) AS pc_chargeback,
  l.in_socio_pep, l.in_alteracao_societaria,
  f.posicao_fila, ROUND(f.score_pld, 4) AS score_pld, f.faixa_risco, f.fl_fraude_confirmada, f.fl_novo_suspeito, f.principais_sinais,
  g.grau, ROUND(g.pagerank, 4) AS pagerank, g.qt_lojas_componente, g.qt_raizes_componente, g.qt_outras_lojas_fraude_componente,
  g.qt_triangulos, g.dist_fraude, ROUND(g.pct_vizinhos_fraude, 3) AS pct_vizinhos_fraude, g.fl_conta_compartilhada,
  g.fl_socio_comum_fraude, g.qt_ciclos_pix, g.qt_contas_liquidacao, g.qt_socios
FROM cielo_pld.gold.fila_investigacao_pld f
JOIN cielo_pld.silver.loja l ON l.nu_ec = f.nu_ec
JOIN cielo_pld.gold.features_grafo_loja g ON g.nu_ec = f.nu_ec
WHERE f.nu_ec = :nu_ec
