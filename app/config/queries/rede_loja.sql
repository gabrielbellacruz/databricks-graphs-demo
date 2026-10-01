-- Rede de vínculos de uma loja (até 3 saltos) + fluxo PIX entre as pessoas/empresas da rede
-- @param nu_ec BIGINT = 2112816701
WITH vinculo AS (
  SELECT src, dst, relacao, qt, valor
  FROM cielo_pld.gold.grafo_arestas_resolvido
  WHERE relacao IN ('titular', 'socio', 'liquida_em', 'localizada_em', 'recebe_em', 'paga_com', 'tem_telefone', 'tem_email', 'usa_dispositivo')
),
grau AS (SELECT dst, COUNT(*) AS n FROM vinculo GROUP BY dst),
nivel1 AS (   -- dono, sócio, conta de liquidação e endereço da loja
  SELECT v.* FROM vinculo v WHERE v.src = CONCAT('LOJA:', CAST(:nu_ec AS STRING))
),
nivel2 AS (   -- contas, telefones, e-mails e dispositivos do dono e do sócio
  SELECT v.* FROM vinculo v JOIN nivel1 n ON v.src = n.dst AND n.relacao IN ('titular', 'socio')
),
atributos AS (SELECT dst AS id FROM nivel1 UNION SELECT dst FROM nivel2),
nivel3 AS (   -- quem mais compartilha esses atributos (ignora super-nós genéricos)
  SELECT v.* FROM vinculo v
  JOIN atributos a ON v.dst = a.id
  JOIN grau g ON g.dst = v.dst AND g.n <= 30
  WHERE v.src <> CONCAT('LOJA:', CAST(:nu_ec AS STRING))
),
nivel4 AS (   -- lojas cujo dono/sócio apareceu no nível 3
  SELECT v.* FROM vinculo v
  WHERE v.relacao IN ('titular', 'socio') AND v.dst IN (SELECT src FROM nivel3)
),
arestas AS (SELECT * FROM nivel1 UNION SELECT * FROM nivel2 UNION SELECT * FROM nivel3 UNION SELECT * FROM nivel4),
nos AS (SELECT src AS id FROM arestas UNION SELECT dst FROM arestas),
pix AS (
  SELECT e.src, e.dst, e.relacao, e.qt, e.valor
  FROM cielo_pld.gold.grafo_arestas_resolvido e
  WHERE e.relacao = 'pix' AND e.valor >= 5000
    AND e.src IN (SELECT id FROM nos) AND e.dst IN (SELECT id FROM nos)
),
todas AS (SELECT * FROM arestas UNION ALL SELECT * FROM pix)
SELECT
  t.src, vs.tipo AS src_tipo, vs.nome AS src_nome, COALESCE(vs.fl_fraude, 0) AS src_fraude, fs.score_pld AS src_score, fs.faixa_risco AS src_faixa,
  t.dst, vd.tipo AS dst_tipo, vd.nome AS dst_nome, COALESCE(vd.fl_fraude, 0) AS dst_fraude, fd.score_pld AS dst_score, fd.faixa_risco AS dst_faixa,
  t.relacao, t.qt, ROUND(t.valor, 2) AS valor
FROM todas t
JOIN cielo_pld.gold.grafo_vertices_resolvido vs ON vs.id = t.src
JOIN cielo_pld.gold.grafo_vertices_resolvido vd ON vd.id = t.dst
LEFT JOIN cielo_pld.gold.fila_investigacao_pld fs ON fs.nu_ec = vs.nu_ec
LEFT JOIN cielo_pld.gold.fila_investigacao_pld fd ON fd.nu_ec = vd.nu_ec
LIMIT 600
