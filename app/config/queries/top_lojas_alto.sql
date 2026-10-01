-- Lojas de risco ALTO para o seletor da visão de grafo
SELECT f.nu_ec, f.nm_loja, ROUND(f.score_pld, 3) AS score_pld, f.fl_novo_suspeito, f.fl_fraude_confirmada
FROM cielo_pld.gold.fila_investigacao_pld f
WHERE f.faixa_risco = 'ALTO'
ORDER BY f.posicao_fila
LIMIT 60
