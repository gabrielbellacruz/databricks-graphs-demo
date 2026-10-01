// Tradução das features do modelo (coluna principais_sinais) para a linguagem do analista de PLD
type Sinal = { rotulo: string; grafo: boolean };

const SINAIS: Record<string, Sinal> = {
  // cadastro / HUB
  faturamento: { rotulo: 'Faturamento', grafo: false },
  razao_faturamento_vs_media: { rotulo: 'Faturamento acima da média histórica', grafo: false },
  cv_faturamento: { rotulo: 'Faturamento instável', grafo: false },
  pc_faturamento_cnp: { rotulo: 'Concentração em venda não presencial', grafo: false },
  pc_chargeback: { rotulo: 'Chargeback alto', grafo: false },
  pc_cancelamento: { rotulo: 'Cancelamentos altos', grafo: false },
  ticket_medio: { rotulo: 'Ticket médio atípico', grafo: false },
  dias_afiliacao: { rotulo: 'Afiliação recente', grafo: false },
  pc_antecipacao: { rotulo: 'Antecipação agressiva', grafo: false },
  socio_pep: { rotulo: 'Sócio PEP', grafo: false },
  alteracao_societaria: { rotulo: 'Alteração societária', grafo: false },
  alerta_comportamental: { rotulo: 'Alerta comportamental', grafo: false },
  socio_irregular_receita: { rotulo: 'Sócio irregular na Receita', grafo: false },
  ecommerce: { rotulo: 'E-commerce', grafo: false },
  mcc_alto_risco: { rotulo: 'Ramo de alto risco PLD', grafo: false },
  // transações
  qt_trns_cartao: { rotulo: 'Volume de transações', grafo: false },
  pct_madrugada: { rotulo: 'Transações de madrugada', grafo: false },
  pct_abaixo_limite: { rotulo: 'Fracionamento abaixo de limites', grafo: false },
  pct_valor_redondo: { rotulo: 'Valores redondos', grafo: false },
  pct_pre_pago: { rotulo: 'Cartões pré-pagos', grafo: false },
  pct_cnp: { rotulo: 'Cartão não presente', grafo: false },
  pct_regra_pld: { rotulo: 'Regras PLD acionadas', grafo: false },
  score_lynx_medio: { rotulo: 'Score Lynx elevado', grafo: false },
  pct_cartao_estrangeiro: { rotulo: 'Cartões estrangeiros', grafo: false },
  qt_pix: { rotulo: 'Volume de PIX', grafo: false },
  marcacoes_laranja_pagadores: { rotulo: 'Pagadores marcados como laranja', grafo: false },
  pct_pix_conta_recente: { rotulo: 'PIX de contas recém-abertas', grafo: false },
  qt_antecipacoes: { rotulo: 'Muitas antecipações', grafo: false },
  // grafo
  grau: { rotulo: 'Muitas conexões', grafo: true },
  qt_contas_liquidacao: { rotulo: 'Várias contas de liquidação', grafo: true },
  qt_socios: { rotulo: 'Vários sócios/responsáveis', grafo: true },
  pagerank: { rotulo: 'Central no fluxo de dinheiro', grafo: true },
  qt_pagadores_pix: { rotulo: 'Muitos pagadores PIX', grafo: true },
  qt_destinos_pix: { rotulo: 'Muitos destinos PIX', grafo: true },
  tam_componente: { rotulo: 'Grupo de vínculos grande', grafo: true },
  qt_lojas_componente: { rotulo: 'Várias lojas no mesmo grupo', grafo: true },
  qt_raizes_componente: { rotulo: 'Lojas de CNPJs diferentes ligadas', grafo: true },
  qt_outras_lojas_fraude_componente: { rotulo: 'Grupo com fraude confirmada', grafo: true },
  qt_lojas_comunidade: { rotulo: 'Comunidade grande', grafo: true },
  chargeback_medio_comunidade: { rotulo: 'Comunidade com chargeback alto', grafo: true },
  qt_outras_lojas_fraude_comunidade: { rotulo: 'Comunidade com fraude confirmada', grafo: true },
  qt_triangulos: { rotulo: 'Lojas interligadas (triângulos)', grafo: true },
  dist_fraude: { rotulo: 'Perto de loja fraudada', grafo: true },
  qt_lojas_vizinhas: { rotulo: 'Muitas lojas vizinhas', grafo: true },
  pct_vizinhos_chargeback_alto: { rotulo: 'Vizinhos com chargeback alto', grafo: true },
  pct_vizinhos_fraude: { rotulo: 'Vizinhos com fraude', grafo: true },
  fl_conta_compartilhada: { rotulo: 'Conta compartilhada com outra loja', grafo: true },
  fl_socio_comum_fraude: { rotulo: 'Sócio em comum com loja fraudada', grafo: true },
  qt_ciclos_pix: { rotulo: 'Ciclos de PIX', grafo: true },
};

export function traduzSinais(principais: string | null | undefined): Sinal[] {
  if (!principais) return [];
  return principais
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => SINAIS[s] ?? { rotulo: s, grafo: false });
}
