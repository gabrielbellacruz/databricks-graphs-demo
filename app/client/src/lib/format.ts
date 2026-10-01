// Valores numéricos podem chegar como string (BIGINT/DECIMAL) — sempre converter antes de formatar
export const num = (v: number | string | null | undefined): number => (v === null || v === undefined || v === '' ? 0 : Number(v));

const inteiro = new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 0 });
const moeda = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 });
const pct = new Intl.NumberFormat('pt-BR', { style: 'percent', maximumFractionDigits: 1 });
const moedaCompacta = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', notation: 'compact', maximumFractionDigits: 1 });

export const fmtInteiro = (v: number | string | null | undefined) => inteiro.format(num(v));
export const fmtMoeda = (v: number | string | null | undefined) => moeda.format(num(v));
/** R$ 230 mil, R$ 1,2 mi — para rótulos curtos (grafo) */
export const fmtMoedaCompacta = (v: number | string | null | undefined) => moedaCompacta.format(num(v));
export const fmtPct = (v: number | string | null | undefined) => pct.format(num(v));
export const fmtScore = (v: number | string | null | undefined) => num(v).toFixed(3).replace('.', ',');

export function fmtDataHora(v: string | null | undefined): string {
  if (!v) return '—';
  const d = new Date(v.replace(' ', 'T'));
  return Number.isNaN(d.getTime()) ? v : d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
}
