import { useEffect, useMemo, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { RELACOES, TIPOS } from '@/lib/grafo';
import type { Aresta, LinhaRede, No } from '@/lib/grafo';

/** Resolve tokens de tema (CSS custom properties) para cores que o canvas entende — respeita tema claro/escuro. */
function useTokens(nomes: string[]) {
  const chave = nomes.join('|');
  return useMemo(() => {
    const estilo = getComputedStyle(document.documentElement);
    return Object.fromEntries(chave.split('|').map((n) => [n, estilo.getPropertyValue(n).trim() || 'gray'])) as Record<string, string>;
  }, [chave]);
}

export function GrafoRede({ linhas, raiz, onSelecionarLoja, altura = 560 }: {
  linhas: LinhaRede[];
  raiz: string;
  onSelecionarLoja: (nuEc: string) => void;
  altura?: number;
}) {
  const caixa = useRef<HTMLDivElement>(null);
  const [largura, setLargura] = useState(800);
  useEffect(() => {
    if (!caixa.current) return;
    const obs = new ResizeObserver(([e]) => setLargura(Math.max(320, e.contentRect.width)));
    obs.observe(caixa.current);
    return () => obs.disconnect();
  }, []);

  const tokens = useTokens(['--primary', '--destructive', '--warning', '--muted-foreground', '--border',
    ...Object.values(TIPOS).map((t) => t.token)]);

  const dados = useMemo(() => {
    const nos = new Map<string, No>();
    const add = (id: string, tipo: string, nome: string, fraude: number, score: number | null, faixa: string | null) => {
      if (!nos.has(id)) nos.set(id, { id, tipo, nome, fraude: Number(fraude), score: score === null ? null : Number(score), faixa });
    };
    const links: Aresta[] = [];
    for (const l of linhas) {
      add(l.src, l.src_tipo, l.src_nome, l.src_fraude, l.src_score, l.src_faixa);
      add(l.dst, l.dst_tipo, l.dst_nome, l.dst_fraude, l.dst_score, l.dst_faixa);
      links.push({ source: l.src, target: l.dst, relacao: l.relacao, qt: Number(l.qt), valor: l.valor === null ? null : Number(l.valor) });
    }
    return { nodes: [...nos.values()], links };
  }, [linhas]);

  const corNo = (n: No) => {
    if (n.tipo !== 'LOJA') return tokens[TIPOS[n.tipo]?.token ?? '--muted-foreground'];
    if (n.id === raiz) return tokens['--primary'];
    if (n.fraude === 1) return tokens['--destructive'];
    if (n.faixa === 'ALTO') return tokens['--warning'];
    return tokens['--foreground'];
  };

  return (
    <div ref={caixa} className="w-full overflow-hidden rounded-md border" style={{ height: altura }}>
      <ForceGraph2D<No, Aresta>
        graphData={dados}
        width={largura}
        height={altura}
        cooldownTicks={120}
        nodeRelSize={1}
        nodeLabel={(n) => `${TIPOS[n.tipo]?.rotulo ?? n.tipo}: ${n.nome}${n.score !== null ? ` · score ${n.score.toFixed(3)}` : ''}${n.fraude === 1 ? ' · FRAUDE CONFIRMADA' : ''}`}
        linkLabel={(l) => `${RELACOES[l.relacao] ?? l.relacao}${l.valor ? ` · R$ ${Math.round(l.valor).toLocaleString('pt-BR')}` : ''}${l.qt > 1 ? ` · ${l.qt}x` : ''}`}
        linkColor={(l) => (l.relacao === 'pix' ? tokens['--warning'] : tokens['--border'])}
        linkWidth={(l) => (l.relacao === 'pix' ? 1.5 : 1)}
        linkLineDash={(l) => (l.relacao === 'pix' ? [4, 2] : null)}
        linkDirectionalArrowLength={(l) => (l.relacao === 'pix' ? 4 : 0)}
        linkDirectionalArrowRelPos={1}
        onNodeClick={(n) => { if (n.tipo === 'LOJA') onSelecionarLoja(n.id.replace('LOJA:', '')); }}
        nodeCanvasObject={(n, ctx, escala) => {
          const raio = (TIPOS[n.tipo]?.raio ?? 3) * (n.id === raiz ? 1.6 : 1);
          ctx.beginPath();
          ctx.arc(n.x ?? 0, n.y ?? 0, raio, 0, 2 * Math.PI);
          ctx.fillStyle = corNo(n);
          ctx.fill();
          if (n.tipo === 'LOJA' && (n.fraude === 1 || n.id === raiz)) {
            ctx.lineWidth = 1.5;
            ctx.strokeStyle = n.fraude === 1 ? tokens['--destructive'] : tokens['--primary'];
            ctx.stroke();
          }
          if (n.tipo === 'LOJA' && (escala > 1.4 || n.id === raiz)) {
            ctx.font = `${Math.max(10 / escala, 3)}px sans-serif`;
            ctx.textAlign = 'center';
            ctx.fillStyle = tokens['--muted-foreground'];
            ctx.fillText(n.nome.slice(0, 28), n.x ?? 0, (n.y ?? 0) + raio + 10 / escala);
          }
        }}
        nodePointerAreaPaint={(n, cor, ctx) => {
          ctx.fillStyle = cor;
          ctx.beginPath();
          ctx.arc(n.x ?? 0, n.y ?? 0, (TIPOS[n.tipo]?.raio ?? 3) + 2, 0, 2 * Math.PI);
          ctx.fill();
        }}
      />
    </div>
  );
}

/** Legenda com as mesmas cores (tokens) usadas no canvas. */
export function LegendaGrafo() {
  const itens: [string, string][] = [
    ['--primary', 'Loja investigada'], ['--destructive', 'Loja com fraude confirmada'], ['--warning', 'Loja em risco ALTO'],
    ['--foreground', 'Outra loja'], ['--chart-cat-1', 'Pessoa / empresa'], ['--chart-cat-2', 'Conta bancária'],
    ['--chart-cat-3', 'Telefone'], ['--chart-cat-4', 'E-mail'], ['--chart-cat-5', 'Endereço'], ['--chart-cat-6', 'Dispositivo'],
  ];
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
      {itens.map(([token, rotulo]) => (
        <span key={rotulo} className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: `var(${token})` }} />
          {rotulo}
        </span>
      ))}
      <span className="flex items-center gap-1.5">
        <span className="inline-block w-5 border-t-2 border-dashed border-warning" /> PIX ≥ R$ 5 mil
      </span>
    </div>
  );
}
