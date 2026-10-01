// Tipos e vocabulário do grafo de vínculos (compartilhados entre o componente e as páginas)
export type LinhaRede = {
  src: string; src_tipo: string; src_nome: string; src_fraude: number; src_score: number | null; src_faixa: string | null;
  dst: string; dst_tipo: string; dst_nome: string; dst_fraude: number; dst_score: number | null; dst_faixa: string | null;
  relacao: string; qt: number; valor: number | null;
};

export type No = { id: string; tipo: string; nome: string; fraude: number; score: number | null; faixa: string | null };
export type Aresta = { source: string; target: string; relacao: string; qt: number; valor: number | null };

export const TIPOS: Record<string, { rotulo: string; token: string; raio: number }> = {
  LOJA: { rotulo: 'Loja', token: '--foreground', raio: 7 },
  IDENTIDADE: { rotulo: 'Pessoa / empresa', token: '--chart-cat-1', raio: 5 },
  DOC: { rotulo: 'Pessoa / empresa', token: '--chart-cat-1', raio: 5 },
  CONTA: { rotulo: 'Conta bancária', token: '--chart-cat-2', raio: 4 },
  TELEFONE: { rotulo: 'Telefone', token: '--chart-cat-3', raio: 3.5 },
  EMAIL: { rotulo: 'E-mail', token: '--chart-cat-4', raio: 3.5 },
  ENDERECO: { rotulo: 'Endereço', token: '--chart-cat-5', raio: 3.5 },
  DISPOSITIVO: { rotulo: 'Dispositivo', token: '--chart-cat-6', raio: 3.5 },
};

export const RELACOES: Record<string, string> = {
  titular: 'é titular de', socio: 'tem como sócio/responsável', liquida_em: 'liquida na conta', localizada_em: 'está no endereço',
  recebe_em: 'recebe PIX na conta', paga_com: 'paga PIX com a conta', tem_telefone: 'usa o telefone', tem_email: 'usa o e-mail',
  usa_dispositivo: 'usa o dispositivo', pix: 'enviou PIX para',
};
