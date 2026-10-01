// Tipos e vocabulário do grafo de vínculos + montagem da "visão simples" (compartilhados entre o componente e as páginas)
import { fmtMoedaCompacta, fmtScore, num } from '@/lib/format';

export type LinhaRede = {
  src: string; src_tipo: string; src_nome: string; src_fraude: number; src_score: number | null; src_faixa: string | null;
  dst: string; dst_tipo: string; dst_nome: string; dst_fraude: number; dst_score: number | null; dst_faixa: string | null;
  relacao: string; qt: number; valor: number | null;
};

/** fraude = liga a loja com fraude confirmada · alerta = vínculo compartilhado / PIX na rede · normal = exclusivo da loja */
export type Nivel = 'fraude' | 'alerta' | 'normal';

export const TIPOS: Record<string, { rotulo: string; token: string }> = {
  LOJA: { rotulo: 'Loja', token: '--foreground' },
  GRUPO: { rotulo: 'Pessoas sem loja', token: '--muted-foreground' },
  IDENTIDADE: { rotulo: 'Pessoa / empresa', token: '--chart-cat-1' },
  DOC: { rotulo: 'Pessoa / empresa', token: '--chart-cat-1' },
  CONTA: { rotulo: 'Conta bancária', token: '--chart-cat-2' },
  TELEFONE: { rotulo: 'Telefone', token: '--chart-cat-3' },
  EMAIL: { rotulo: 'E-mail', token: '--chart-cat-4' },
  ENDERECO: { rotulo: 'Endereço', token: '--chart-cat-5' },
  DISPOSITIVO: { rotulo: 'Dispositivo', token: '--chart-cat-6' },
};

export const RELACOES: Record<string, string> = {
  titular: 'titular', socio: 'sócio/responsável', liquida_em: 'liquida na conta', localizada_em: 'está no endereço',
  recebe_em: 'recebe PIX na conta', paga_com: 'paga PIX com a conta', tem_telefone: 'usa o telefone', tem_email: 'usa o e-mail',
  usa_dispositivo: 'usa o dispositivo', pix: 'enviou PIX para',
};

/** Pessoas que não são titular/sócio de nenhuma loja da rede viram um único nó */
export const GRUPO_SEM_LOJA = 'GRUPO:sem-loja';

export type NoVisao = {
  id: string;
  tipo: string;
  titulo: string;
  subtitulo: string;
  nivel: Nivel;
  raiz: boolean;
  fraude: boolean;
  faixa: string | null;
  /** lojas que usam o atributo (atributos) */
  lojas: string[];
  /** nomes das pessoas sem loja (atributo usado por elas, ou o próprio grupo) */
  pessoasSemLoja: string[];
  /** o que a loja compartilha com a rede (lojas): "Conta bancária", "PIX"... */
  compartilha: string[];
  detalhes: [string, string][];
  posicao: { x: number; y: number };
};

export type ArestaVisao = {
  id: string;
  source: string;
  target: string;
  tipo: 'vinculo' | 'pix';
  nivel: Nivel;
  rotulo: string;
  /** espessura do traço */
  peso: number;
  bidirecional: boolean;
  ciclo: boolean;
};

export type Sinal = { id: string; nivel: 'fraude' | 'alerta'; texto: string; alvo: string };

export type VisaoRede = { nos: NoVisao[]; arestas: ArestaVisao[]; sinais: Sinal[]; ocultos: number };

type Vertice = { id: string; tipo: string; nome: string; fraude: boolean; score: number | null; faixa: string | null };

const PESSOA = new Set(['IDENTIDADE', 'DOC']);
const ORDEM_NIVEL: Record<Nivel, number> = { fraude: 0, alerta: 1, normal: 2 };

// Layout em duas colunas: lojas (e o grupo sem loja) à esquerda, vínculos à direita; PIX em arcos à esquerda das lojas
const X_VINCULOS = 640;
const DY_LOJA = 76;
const DY_VINCULO = 62;

const plural = (n: number, um: string, varios: string) => `${n} ${n === 1 ? um : varios}`;

function tituloAtributo(v: Vertice): string {
  if (v.tipo === 'CONTA') {
    const [, agencia, conta] = v.nome.split('|');
    return agencia && conta ? `Conta ${agencia} / ${conta}` : `Conta ${v.nome}`;
  }
  if (v.tipo === 'DISPOSITIVO') return `Dispositivo ${v.nome.slice(0, 8)}`;
  if (v.tipo === 'TELEFONE') {
    const m = /^\+55(\d{2})(\d{4,5})(\d{4})$/.exec(v.nome);
    return m ? `+55 ${m[1]} ${m[2]}-${m[3]}` : v.nome;
  }
  return v.nome;
}

/**
 * Monta a visão simples da rede de uma loja:
 * - donos e sócios são projetados na loja (o dispositivo do titular vira "titular usa o dispositivo" na loja);
 * - um vínculo é suspeito quando o atributo (conta, dispositivo, telefone, e-mail, endereço, sócio) é usado por mais de uma loja/pessoa;
 * - vermelho quando o atributo também é usado por loja com fraude confirmada (arestas da loja investigada e da loja fraudada);
 * - PIX ≥ R$ 5 mil agregado por par, com sentido do fluxo líquido e marcação de ciclos de 3 saltos.
 * `completo` inclui também os vínculos exclusivos (não compartilhados) das lojas.
 */
export function montarVisao(linhas: LinhaRede[], raiz: string, completo: boolean): VisaoRede {
  const vert = new Map<string, Vertice>();
  const add = (id: string, tipo: string, nome: string, fraude: number, score: number | null, faixa: string | null) => {
    if (!vert.has(id)) vert.set(id, { id, tipo, nome, fraude: num(fraude) === 1, score: score === null || score === undefined ? null : num(score), faixa: faixa || null });
  };
  for (const l of linhas) {
    add(l.src, l.src_tipo, l.src_nome, l.src_fraude, l.src_score, l.src_faixa);
    add(l.dst, l.dst_tipo, l.dst_nome, l.dst_fraude, l.dst_score, l.dst_faixa);
  }
  const ehLoja = (id: string) => vert.get(id)?.tipo === 'LOJA';
  const nome = (id: string) => (id === GRUPO_SEM_LOJA ? 'pessoas sem loja' : vert.get(id)?.nome ?? id);
  const fraudes = new Set([...vert.values()].filter((v) => v.tipo === 'LOJA' && v.fraude).map((v) => v.id));

  // Quem é titular/sócio de quais lojas
  const lojasDaPessoa = new Map<string, Map<string, string>>();
  for (const l of linhas) {
    if ((l.relacao === 'titular' || l.relacao === 'socio') && ehLoja(l.src)) {
      const m = lojasDaPessoa.get(l.dst) ?? new Map<string, string>();
      if (m.get(l.src) !== 'titular') m.set(l.src, l.relacao === 'titular' ? 'titular' : 'sócio');
      lojasDaPessoa.set(l.dst, m);
    }
  }
  const entidades = (id: string): string[] => {
    if (ehLoja(id)) return [id];
    const lojas = lojasDaPessoa.get(id);
    return lojas && lojas.size > 0 ? [...lojas.keys()] : [GRUPO_SEM_LOJA];
  };
  const principal = (id: string): string => {
    const e = entidades(id);
    return e.includes(raiz) ? raiz : [...e].sort()[0];
  };

  // Vínculos projetados entidade (loja ou grupo) -> atributo
  type Vinculo = { source: string; target: string; relacoes: Set<string> };
  const vinculos = new Map<string, Vinculo>();
  const uso = new Map<string, { lojas: Set<string>; semLoja: Set<string> }>();
  for (const l of linhas) {
    if (l.relacao === 'pix') continue;
    const base = RELACOES[l.relacao] ?? l.relacao;
    for (const origem of entidades(l.src)) {
      const chave = `${origem}>${l.dst}`;
      const v = vinculos.get(chave) ?? { source: origem, target: l.dst, relacoes: new Set<string>() };
      const papel = lojasDaPessoa.get(l.src)?.get(origem);
      v.relacoes.add(ehLoja(l.src) || !papel ? base : `${papel} ${base}`);
      vinculos.set(chave, v);
      const u = uso.get(l.dst) ?? { lojas: new Set<string>(), semLoja: new Set<string>() };
      if (origem === GRUPO_SEM_LOJA) u.semLoja.add(l.src);
      else u.lojas.add(origem);
      uso.set(l.dst, u);
    }
  }
  const nivelAtributo = (id: string): Nivel => {
    const u = uso.get(id);
    if (!u || u.lojas.size + u.semLoja.size < 2) return 'normal';
    return [...u.lojas].some((x) => fraudes.has(x)) ? 'fraude' : 'alerta';
  };

  // PIX: ciclos de 3 saltos entre pessoas e fluxo agregado por par de entidades
  const pix = linhas.filter((l) => l.relacao === 'pix');
  const saidas = new Map<string, Set<string>>();
  for (const l of pix) saidas.set(l.src, (saidas.get(l.src) ?? new Set<string>()).add(l.dst));
  const emCiclo = new Set<string>();
  const ciclos = new Set<string>();
  for (const [a, bs] of saidas) {
    for (const b of bs) {
      for (const c of saidas.get(b) ?? []) {
        if (c === a || !saidas.get(c)?.has(a)) continue;
        emCiclo.add(`${a}>${b}`).add(`${b}>${c}`).add(`${c}>${a}`);
        ciclos.add([a, b, c].sort().join('|'));
      }
    }
  }
  type Fluxo = { a: string; b: string; ab: number; ba: number; qt: number; ciclo: boolean };
  const fluxos = new Map<string, Fluxo>();
  const enviado = new Map<string, number>();
  const recebido = new Map<string, number>();
  let pixInterno = 0;
  let pixTotal = 0;
  for (const l of pix) {
    const s = principal(l.src);
    const t = principal(l.dst);
    const valor = num(l.valor);
    pixTotal += valor;
    if (s === t) {
      if (s === GRUPO_SEM_LOJA) pixInterno += valor;
      continue;
    }
    enviado.set(s, (enviado.get(s) ?? 0) + valor);
    recebido.set(t, (recebido.get(t) ?? 0) + valor);
    const [a, b] = s < t ? [s, t] : [t, s];
    const f = fluxos.get(`${a}~${b}`) ?? { a, b, ab: 0, ba: 0, qt: 0, ciclo: false };
    if (s === a) f.ab += valor;
    else f.ba += valor;
    f.qt += num(l.qt);
    if (emCiclo.has(`${l.src}>${l.dst}`)) f.ciclo = true;
    fluxos.set(`${a}~${b}`, f);
  }

  // Quais atributos entram na visão
  const atributos = [...uso.keys()].filter((id) => nivelAtributo(id) !== 'normal' || (completo && (uso.get(id)?.lojas.size ?? 0) > 0));
  const visiveis = new Set(atributos);
  const ocultos = [...uso.keys()].filter((id) => !visiveis.has(id) && (uso.get(id)?.lojas.size ?? 0) > 0).length;
  const vinculosVisiveis = [...vinculos.values()].filter((v) => visiveis.has(v.target));
  const temGrupo = vinculosVisiveis.some((v) => v.source === GRUPO_SEM_LOJA) || [...fluxos.values()].some((f) => f.a === GRUPO_SEM_LOJA || f.b === GRUPO_SEM_LOJA);

  // Coluna das lojas: investigada, fraudes confirmadas, maior score; grupo sem loja no fim
  const lojas = [...vert.values()].filter((v) => v.tipo === 'LOJA').sort((x, y) =>
    Number(y.id === raiz) - Number(x.id === raiz) || Number(y.fraude) - Number(x.fraude) || (y.score ?? 0) - (x.score ?? 0));
  const coluna = [...lojas.map((v) => v.id), ...(temGrupo ? [GRUPO_SEM_LOJA] : [])];
  const linhaDe = new Map(coluna.map((id, i) => [id, i]));

  // Coluna dos vínculos: compartilhados antes dos exclusivos, ordenados pelo baricentro das lojas ligadas (menos cruzamentos)
  const baricentro = (id: string) => {
    const ligados = vinculosVisiveis.filter((v) => v.target === id).map((v) => linhaDe.get(v.source) ?? 0);
    return ligados.reduce((s, x) => s + x, 0) / Math.max(1, ligados.length);
  };
  atributos.sort((x, y) => Number(nivelAtributo(x) === 'normal') - Number(nivelAtributo(y) === 'normal')
    || baricentro(x) - baricentro(y) || ORDEM_NIVEL[nivelAtributo(x)] - ORDEM_NIVEL[nivelAtributo(y)]);
  const alturaLojas = (coluna.length - 1) * DY_LOJA;
  const alturaVinculos = (atributos.length - 1) * DY_VINCULO;
  const y0Lojas = Math.max(0, (alturaVinculos - alturaLojas) / 2);
  const y0Vinculos = Math.max(0, (alturaLojas - alturaVinculos) / 2);

  const semLoja = new Set<string>();
  for (const l of linhas) for (const id of [l.src, l.dst]) if (PESSOA.has(vert.get(id)?.tipo ?? '') && entidades(id)[0] === GRUPO_SEM_LOJA) semLoja.add(id);
  const tiposCompartilhados = (loja: string) => {
    const t = new Set(vinculosVisiveis.filter((v) => v.source === loja && nivelAtributo(v.target) !== 'normal').map((v) => {
      const tipo = vert.get(v.target)?.tipo ?? '';
      return PESSOA.has(tipo) ? 'Titular/sócio' : TIPOS[tipo]?.rotulo ?? '';
    }));
    if ([...fluxos.values()].some((f) => f.a === loja || f.b === loja)) t.add('PIX');
    return [...t].filter(Boolean);
  };

  const nos: NoVisao[] = lojas.map((v) => {
    const pessoas = [...lojasDaPessoa.entries()].filter(([, m]) => m.has(v.id)).map(([p, m]) => `${nome(p)} (${m.get(v.id) ?? ''})`);
    const detalhes: [string, string][] = [['EC', v.id.replace('LOJA:', '')]];
    if (v.score !== null) detalhes.push(['Score PLD', `${fmtScore(v.score)} · ${v.faixa ?? '—'}`]);
    if (pessoas.length) detalhes.push(['Titular / sócio', pessoas.join(', ')]);
    if (enviado.get(v.id)) detalhes.push(['PIX enviado na rede', fmtMoedaCompacta(enviado.get(v.id))]);
    if (recebido.get(v.id)) detalhes.push(['PIX recebido na rede', fmtMoedaCompacta(recebido.get(v.id))]);
    return {
      id: v.id, tipo: 'LOJA', titulo: v.nome,
      subtitulo: `EC ${v.id.replace('LOJA:', '')}${v.score !== null ? ` · score ${fmtScore(v.score)}` : ''}`,
      nivel: v.fraude ? 'fraude' : 'normal', raiz: v.id === raiz, fraude: v.fraude, faixa: v.faixa,
      lojas: [], pessoasSemLoja: [], compartilha: tiposCompartilhados(v.id), detalhes,
      posicao: { x: 0, y: y0Lojas + (linhaDe.get(v.id) ?? 0) * DY_LOJA },
    };
  });
  if (temGrupo) {
    const nomes = [...semLoja].map(nome).sort();
    const detalhes: [string, string][] = [['Pessoas', nomes.slice(0, 12).join(', ') + (nomes.length > 12 ? ` e mais ${nomes.length - 12}` : '')]];
    if (pixInterno) detalhes.push(['PIX entre elas', fmtMoedaCompacta(pixInterno)]);
    nos.push({
      id: GRUPO_SEM_LOJA, tipo: 'GRUPO', titulo: plural(nomes.length, 'pessoa sem loja', 'pessoas sem loja'),
      subtitulo: 'usam os mesmos dispositivos, contas ou PIX', nivel: 'alerta', raiz: false, fraude: false, faixa: null,
      lojas: [], pessoasSemLoja: nomes, compartilha: [], detalhes,
      posicao: { x: 0, y: y0Lojas + (linhaDe.get(GRUPO_SEM_LOJA) ?? 0) * DY_LOJA },
    });
  }
  atributos.forEach((id, i) => {
    const v = vert.get(id);
    if (!v) return;
    const u = uso.get(id) ?? { lojas: new Set<string>(), semLoja: new Set<string>() };
    const comFraude = [...u.lojas].filter((x) => fraudes.has(x)).length;
    const partes = [plural(u.lojas.size, 'loja', 'lojas')];
    if (comFraude) partes.push(`${comFraude} com fraude`);
    if (u.semLoja.size) partes.push(plural(u.semLoja.size, 'pessoa sem loja', 'pessoas sem loja'));
    const detalhes: [string, string][] = [['Tipo', TIPOS[v.tipo]?.rotulo ?? v.tipo], ['Valor', v.nome]];
    if (v.tipo === 'CONTA') detalhes.push(['ISPB', v.nome.split('|')[0] ?? '—']);
    nos.push({
      id, tipo: v.tipo, titulo: PESSOA.has(v.tipo) ? v.nome : tituloAtributo(v),
      subtitulo: u.lojas.size + u.semLoja.size < 2 ? 'exclusivo desta loja' : partes.join(' · '),
      nivel: nivelAtributo(id), raiz: false, fraude: false, faixa: null,
      lojas: [...u.lojas], pessoasSemLoja: [...u.semLoja].map(nome).sort(), compartilha: [], detalhes,
      posicao: { x: X_VINCULOS, y: y0Vinculos + i * DY_VINCULO },
    });
  });

  const arestas: ArestaVisao[] = vinculosVisiveis.map((v) => {
    const na = nivelAtributo(v.target);
    const nivel: Nivel = na === 'fraude' && (fraudes.has(v.source) || v.source === raiz) ? 'fraude' : na === 'normal' ? 'normal' : 'alerta';
    const n = uso.get(v.target)?.semLoja.size ?? 0;
    const relacoes = [...v.relacoes];
    const rotulo = v.source === GRUPO_SEM_LOJA ? `${plural(n, 'pessoa sem loja', 'pessoas sem loja')}: ${relacoes.join(' · ')}` : relacoes.join(' · ');
    return { id: `v:${v.source}>${v.target}`, source: v.source, target: v.target, tipo: 'vinculo', nivel, rotulo, peso: nivel === 'fraude' ? 2.5 : nivel === 'alerta' ? 1.75 : 1, bidirecional: false, ciclo: false };
  });
  for (const f of fluxos.values()) {
    const [source, target] = f.ab >= f.ba ? [f.a, f.b] : [f.b, f.a];
    const valor = f.ab + f.ba;
    const bidirecional = f.ab > 0 && f.ba > 0;
    arestas.push({
      id: `p:${f.a}~${f.b}`, source, target, tipo: 'pix',
      nivel: fraudes.has(f.a) || fraudes.has(f.b) ? 'fraude' : 'alerta',
      rotulo: `PIX ${fmtMoedaCompacta(valor)} · ${f.qt}x${bidirecional ? ' · ida e volta' : ''}${f.ciclo ? ' · ciclo' : ''}`,
      peso: 1.25 + Math.min(3, Math.max(0, Math.log10(valor / 5000))), bidirecional, ciclo: f.ciclo,
    });
  }

  // Sinais em linguagem de negócio, do mais grave para o menos grave
  const sinais: (Sinal & { daRaiz: boolean })[] = [];
  for (const id of atributos) {
    const v = vert.get(id);
    const u = uso.get(id);
    const nivel = nivelAtributo(id);
    if (!v || !u || nivel === 'normal') continue;
    const comFraude = [...u.lojas].filter((x) => fraudes.has(x)).length;
    const quem = [plural(u.lojas.size, 'loja', 'lojas'), u.semLoja.size ? plural(u.semLoja.size, 'pessoa sem loja', 'pessoas sem loja') : ''].filter(Boolean).join(' e ');
    const verbo = PESSOA.has(v.tipo) ? 'é titular ou sócio de' : v.tipo === 'CONTA' ? 'usada por' : 'usado por';
    const titulo = PESSOA.has(v.tipo) ? v.nome : `${TIPOS[v.tipo]?.rotulo ?? v.tipo} ${tituloAtributo(v).replace(/^(Conta|Dispositivo) /, '')}`;
    sinais.push({
      id: `s:${id}`, nivel, alvo: id, daRaiz: u.lojas.has(raiz),
      texto: `${titulo} ${verbo} ${quem}${comFraude ? ` — ${comFraude} com fraude confirmada` : ''}`,
    });
  }
  const fluxosFraude = [...fluxos.values()].filter((f) => fraudes.has(f.a) || fraudes.has(f.b)).sort((x, y) => y.ab + y.ba - (x.ab + x.ba));
  for (const f of fluxosFraude.slice(0, 3)) {
    sinais.push({
      id: `s:${f.a}~${f.b}`, nivel: 'fraude', alvo: fraudes.has(f.a) ? f.a : f.b, daRaiz: f.a === raiz || f.b === raiz,
      texto: `PIX entre ${nome(f.a)} e ${nome(f.b)}: ${fmtMoedaCompacta(f.ab + f.ba)} em ${plural(f.qt, 'transferência', 'transferências')} (loja com fraude confirmada)`,
    });
  }
  if (ciclos.size) {
    sinais.push({ id: 's:ciclos', nivel: 'alerta', alvo: raiz, daRaiz: true, texto: `${plural(ciclos.size, 'ciclo', 'ciclos')} de PIX em 3 saltos (A → B → C → A) entre pessoas da rede` });
  }
  if (pixTotal) {
    sinais.push({ id: 's:pix', nivel: 'alerta', alvo: raiz, daRaiz: true, texto: `${fmtMoedaCompacta(pixTotal)} em PIX ≥ R$ 5 mil circulando entre titulares, sócios e pessoas da rede` });
  }
  sinais.sort((x, y) => ORDEM_NIVEL[x.nivel] - ORDEM_NIVEL[y.nivel] || Number(y.daRaiz) - Number(x.daRaiz));

  return { nos, arestas, sinais: sinais.map(({ daRaiz: _daRaiz, ...s }) => s), ocultos };
}
