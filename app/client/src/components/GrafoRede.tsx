import { createContext, createElement, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import {
  Background,
  BackgroundVariant,
  BaseEdge,
  ControlButton,
  Controls,
  EdgeLabelRenderer,
  Handle,
  Panel,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useNodesInitialized,
  useNodesState,
  useReactFlow,
} from '@xyflow/react';
import type { Edge, EdgeProps, EdgeTypes, Node, NodeProps, NodeTypes } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Badge, Button, Label, Switch } from '@databricks/appkit-ui/react';
import { Landmark, Mail, MapPin, Maximize, Network, Phone, Smartphone, Store, UserRound, Users, X } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { TIPOS, montarVisao } from '@/lib/grafo';
import type { ArestaVisao, LinhaRede, Nivel, NoVisao, VisaoRede } from '@/lib/grafo';
import { cn } from '@/lib/utils';

type NoFluxo = Node<NoVisao, 'loja' | 'grupo' | 'atributo'>;
type ArestaFluxo = Edge<ArestaVisao, 'vinculo' | 'pix'>;

const COR: Record<Nivel, string> = { fraude: 'var(--destructive)', alerta: 'var(--warning)', normal: 'var(--muted-foreground)' };
const ICONES: Record<string, LucideIcon> = {
  LOJA: Store, GRUPO: Users, IDENTIDADE: UserRound, DOC: UserRound, CONTA: Landmark, TELEFONE: Phone, EMAIL: Mail, ENDERECO: MapPin, DISPOSITIVO: Smartphone,
};
// Alças invisíveis: o grafo é só leitura (sem criar conexões)
const ALCA = { opacity: 0, width: 6, height: 6, minWidth: 0, minHeight: 0, border: 0 };

/** Nó/arestas em foco (hover ou clique) — os demais ficam esmaecidos */
type Destaque = { ativo: string | null; porAresta: boolean; vizinhos: Set<string>; arestas: Set<string>; rotulos: boolean };
const SEM_DESTAQUE: Destaque = { ativo: null, porAresta: false, vizinhos: new Set(), arestas: new Set(), rotulos: false };
const DestaqueCtx = createContext<Destaque>(SEM_DESTAQUE);

function useEsmaecido(id: string) {
  const d = useContext(DestaqueCtx);
  return d.ativo !== null && !d.vizinhos.has(id);
}

function Icone({ tipo, cor }: { tipo: string; cor: string }) {
  return (
    <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full" style={{ color: cor, backgroundColor: `color-mix(in oklch, ${cor} 14%, transparent)` }}>
      {createElement(ICONES[tipo] ?? Network, { className: 'h-4 w-4' })}
    </span>
  );
}

function AlcasLoja() {
  return (
    <>
      <Handle type="target" position={Position.Left} id="pix-t" isConnectable={false} style={ALCA} />
      <Handle type="source" position={Position.Left} id="pix-s" isConnectable={false} style={ALCA} />
      <Handle type="source" position={Position.Right} id="v" isConnectable={false} style={ALCA} />
    </>
  );
}

/** Faixa de risco em tom neutro/âmbar — no grafo o vermelho fica reservado para fraude confirmada */
function FaixaNo({ fraude, faixa }: { fraude: boolean; faixa: string | null }) {
  if (fraude) return <Badge variant="destructive">Fraude</Badge>;
  if (faixa === 'ALTO') return <Badge variant="outline" className="border-warning text-warning">ALTO</Badge>;
  if (faixa === 'MEDIO') return <Badge variant="outline">MÉDIO</Badge>;
  return faixa ? <Badge variant="secondary">{faixa}</Badge> : null;
}

function NoLoja({ id, data }: NodeProps<NoFluxo>) {
  const esmaecido = useEsmaecido(id);
  const cor = data.fraude ? 'var(--destructive)' : data.raiz ? 'var(--primary)' : 'var(--foreground)';
  return (
    <div
      className={cn('relative w-80 cursor-pointer rounded-lg border bg-card px-3 py-2 shadow-sm transition-opacity duration-200', esmaecido && 'opacity-25')}
      style={{
        borderColor: data.raiz ? 'var(--primary)' : data.fraude ? 'var(--destructive)' : undefined,
        borderWidth: data.raiz || data.fraude ? 2 : 1,
        boxShadow: data.raiz ? '0 0 0 4px color-mix(in oklch, var(--primary) 18%, transparent)' : undefined,
      }}
    >
      <AlcasLoja />
      {data.raiz && (
        <span className="absolute -top-2.5 left-3 rounded bg-primary px-1.5 text-[10px] font-medium leading-4 text-primary-foreground">Loja investigada</span>
      )}
      <div className="flex items-center gap-2">
        <Icone tipo="LOJA" cor={cor} />
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-medium leading-tight" title={data.titulo}>{data.titulo}</div>
          <div className="mt-0.5 flex items-center justify-between gap-2">
            <span className="truncate text-xs text-muted-foreground">{data.subtitulo}</span>
            <FaixaNo fraude={data.fraude} faixa={data.faixa} />
          </div>
        </div>
      </div>
    </div>
  );
}

function NoGrupo({ id, data }: NodeProps<NoFluxo>) {
  const esmaecido = useEsmaecido(id);
  return (
    <div className={cn('w-80 cursor-pointer rounded-lg border border-dashed bg-card px-3 py-2 shadow-sm transition-opacity duration-200', esmaecido && 'opacity-25')}
      style={{ borderColor: 'var(--warning)' }}>
      <AlcasLoja />
      <div className="flex items-center gap-2">
        <Icone tipo="GRUPO" cor="var(--warning)" />
        <div className="min-w-0">
          <div className="truncate text-sm font-medium leading-tight">{data.titulo}</div>
          <div className="truncate text-xs text-muted-foreground">{data.subtitulo}</div>
        </div>
      </div>
    </div>
  );
}

function NoAtributo({ id, data }: NodeProps<NoFluxo>) {
  const esmaecido = useEsmaecido(id);
  return (
    <div
      className={cn('flex w-[19rem] cursor-pointer items-center gap-2 rounded-full border bg-card py-1.5 pl-1.5 pr-4 shadow-sm transition-opacity duration-200',
        data.nivel === 'normal' && 'opacity-75', esmaecido && 'opacity-25')}
      style={{ borderColor: data.nivel === 'normal' ? undefined : COR[data.nivel], borderWidth: data.nivel === 'normal' ? 1 : 1.5 }}
    >
      <Handle type="target" position={Position.Left} isConnectable={false} style={ALCA} />
      <Icone tipo={data.tipo} cor={`var(${TIPOS[data.tipo]?.token ?? '--muted-foreground'})`} />
      <div className="min-w-0">
        <div className="truncate text-xs font-medium" title={data.titulo}>{data.titulo}</div>
        <div className="truncate text-[11px]" style={{ color: data.nivel === 'fraude' ? 'var(--destructive)' : 'var(--muted-foreground)' }}>{data.subtitulo}</div>
      </div>
    </div>
  );
}

/** Rótulo da aresta; `ancora` define de que lado do ponto ele fica (antes = à esquerda, depois = à direita) */
function Rotulo({ x, y, nivel, texto, ancora = 'centro' }: { x: number; y: number; nivel: Nivel; texto: string; ancora?: 'antes' | 'centro' | 'depois' }) {
  const dx = ancora === 'antes' ? 'calc(-100% - 6px)' : ancora === 'depois' ? '6px' : '-50%';
  return (
    <EdgeLabelRenderer>
      <div
        className="nodrag nopan pointer-events-none absolute z-10 max-w-48 rounded-md border bg-popover px-2 py-1 text-[11px] leading-tight text-popover-foreground shadow-sm"
        style={{ transform: `translate(${dx}, -50%) translate(${x}px, ${y}px)`, borderColor: nivel === 'normal' ? undefined : COR[nivel] }}
      >
        {texto}
      </div>
    </EdgeLabelRenderer>
  );
}

/** Afastamento horizontal do arco de PIX (proporcional à distância vertical entre as lojas) */
const curvaPix = (dy: number) => Math.min(260, 80 + Math.abs(dy) * 0.35);

type Ponto = [number, number];
function pontoCubica(t: number, [x0, y0]: Ponto, [x1, y1]: Ponto, [x2, y2]: Ponto, [x3, y3]: Ponto) {
  const u = 1 - t;
  const [a, b, c, e] = [u * u * u, 3 * u * u * t, 3 * u * t * t, t * t * t];
  return { x: a * x0 + b * x1 + c * x2 + e * x3, y: a * y0 + b * y1 + c * y2 + e * y3 };
}

function estiloAresta(d: Destaque, id: string, a: ArestaVisao) {
  const ativa = d.arestas.has(id);
  const opacidade = d.ativo !== null && !ativa ? 0.06 : a.nivel === 'normal' ? 0.4 : 0.9;
  return { ativa, estilo: { stroke: COR[a.nivel], strokeWidth: ativa ? a.peso + 1 : a.peso, opacity: opacidade, transition: 'opacity 150ms, stroke-width 150ms' } };
}

/**
 * Vínculo loja/grupo → atributo (curva da coluna das lojas para a coluna dos vínculos).
 * Com um nó em foco, o rótulo vai para a ponta oposta — cada rótulo fica ao lado do atributo/loja a que se refere.
 */
function ArestaVinculo({ id, source, target, sourceX, sourceY, targetX, targetY, data }: EdgeProps<ArestaFluxo>) {
  const d = useContext(DestaqueCtx);
  if (!data) return null;
  const o = Math.max(40, Math.abs(targetX - sourceX) / 2);
  const pontos: [Ponto, Ponto, Ponto, Ponto] = [[sourceX, sourceY], [sourceX + o, sourceY], [targetX - o, targetY], [targetX, targetY]];
  const path = `M ${sourceX},${sourceY} C ${sourceX + o},${sourceY} ${targetX - o},${targetY} ${targetX},${targetY}`;
  const [t, ancora] = d.porAresta ? [0.5, 'centro' as const] : d.ativo === source && d.ativo !== target ? [0.85, 'antes' as const] : [0.15, 'depois' as const];
  const p = pontoCubica(t, ...pontos);
  const { ativa, estilo } = estiloAresta(d, id, data);
  return (
    <>
      <BaseEdge id={id} path={path} interactionWidth={16} style={estilo} />
      {ativa && d.rotulos && <Rotulo x={p.x} y={p.y} nivel={data.nivel} texto={data.rotulo} ancora={ancora} />}
    </>
  );
}

/** PIX entre lojas (via titulares/sócios) e pessoas sem loja: arco à esquerda da coluna, tracejado, seta no sentido do dinheiro */
function ArestaPix({ id, sourceX, sourceY, targetX, targetY, data, markerEnd, markerStart }: EdgeProps<ArestaFluxo>) {
  const d = useContext(DestaqueCtx);
  if (!data) return null;
  const curva = curvaPix(targetY - sourceY);
  const path = `M ${sourceX},${sourceY} C ${sourceX - curva},${sourceY} ${targetX - curva},${targetY} ${targetX},${targetY}`;
  const { ativa, estilo } = estiloAresta(d, id, data);
  return (
    <>
      <BaseEdge id={id} path={path} markerEnd={markerEnd} markerStart={markerStart} interactionWidth={14} style={{ ...estilo, strokeDasharray: '6 4' }} />
      {ativa && d.rotulos && <Rotulo x={(sourceX + targetX) / 2 - curva * 0.75} y={(sourceY + targetY) / 2} nivel={data.nivel} texto={data.rotulo} />}
    </>
  );
}

const TIPOS_NO: NodeTypes = { loja: NoLoja, grupo: NoGrupo, atributo: NoAtributo };
const TIPOS_ARESTA: EdgeTypes = { vinculo: ArestaVinculo, pix: ArestaPix };
const ORDEM_DESENHO: Record<Nivel, number> = { normal: 0, alerta: 1, fraude: 2 };

/** Pontas de seta (uma por nível) referenciadas pelas arestas de PIX */
function Setas() {
  return (
    <svg aria-hidden className="absolute h-0 w-0">
      <defs>
        {(['fraude', 'alerta', 'normal'] as const).map((n) => (
          <marker key={n} id={`pld-seta-${n}`} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="10" markerHeight="10" markerUnits="userSpaceOnUse" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" style={{ fill: COR[n] }} />
          </marker>
        ))}
      </defs>
    </svg>
  );
}

/** Inspetor ao lado do grafo: a loja investigada por padrão, ou o nó clicado */
function Detalhe({ no, emFoco, altura, porId, onFocar, onFechar, onSelecionarLoja }: {
  no: NoVisao;
  emFoco: boolean;
  altura: number;
  porId: Map<string, NoVisao>;
  onFocar: (id: string) => void;
  onFechar: () => void;
  onSelecionarLoja: (nuEc: string) => void;
}) {
  const cor = no.tipo === 'LOJA' ? (no.fraude ? 'var(--destructive)' : 'var(--primary)') : no.tipo === 'GRUPO' ? 'var(--warning)' : `var(${TIPOS[no.tipo]?.token ?? '--muted-foreground'})`;
  return (
    <aside className="space-y-3 overflow-y-auto rounded-md border bg-card p-3 text-sm" style={{ maxHeight: altura }}>
      <p className="text-xs font-medium text-muted-foreground">{emFoco ? (TIPOS[no.tipo]?.rotulo ?? no.tipo) : 'Loja investigada'}</p>
      <div className="flex items-start gap-2">
        <Icone tipo={no.tipo} cor={cor} />
        <div className="min-w-0 flex-1">
          <div className="font-medium leading-tight">{no.titulo}</div>
          <div className="text-xs" style={{ color: no.nivel === 'fraude' ? 'var(--destructive)' : 'var(--muted-foreground)' }}>{no.subtitulo}</div>
        </div>
        {emFoco && (
          <Button variant="ghost" size="icon" className="h-7 w-7" aria-label="Voltar para a loja investigada" title="Voltar para a loja investigada" onClick={onFechar}>
            <X className="h-4 w-4" />
          </Button>
        )}
      </div>
      <dl className="space-y-1.5">
        {no.detalhes.map(([k, v]) => (
          <div key={k}><dt className="text-xs text-muted-foreground">{k}</dt><dd className="break-words">{v}</dd></div>
        ))}
        {no.compartilha.length > 0 && (
          <div><dt className="text-xs text-muted-foreground">Compartilha com a rede</dt><dd>{no.compartilha.join(', ')}</dd></div>
        )}
      </dl>
      {no.lojas.length > 0 && (
        <div className="space-y-1">
          <p className="text-xs text-muted-foreground">Lojas que usam</p>
          {no.lojas.map((id) => porId.get(id)).filter((l): l is NoVisao => l !== undefined).map((l) => (
            <button key={l.id} type="button" onClick={() => onFocar(l.id)} className="flex w-full items-center justify-between gap-2 rounded-md px-1.5 py-1 text-left text-xs hover:bg-muted">
              <span className="truncate">{l.titulo}{l.raiz ? ' (investigada)' : ''}</span>
              <FaixaNo fraude={l.fraude} faixa={l.faixa} />
            </button>
          ))}
        </div>
      )}
      {no.tipo !== 'GRUPO' && no.pessoasSemLoja.length > 0 && (
        <p className="text-xs text-muted-foreground">
          Também usado por {no.pessoasSemLoja.length} pessoa{no.pessoasSemLoja.length > 1 ? 's' : ''} sem loja: {no.pessoasSemLoja.slice(0, 6).join(', ')}{no.pessoasSemLoja.length > 6 ? '…' : ''}
        </p>
      )}
      {no.tipo === 'LOJA' && !no.raiz && (
        <Button size="sm" className="w-full" onClick={() => onSelecionarLoja(no.id.replace('LOJA:', ''))}>
          <Network className="mr-2 h-4 w-4" /> Abrir a rede desta loja
        </Button>
      )}
      {!emFoco && <p className="text-xs text-muted-foreground">Clique em qualquer nó do grafo para ver os detalhes dele aqui.</p>}
    </aside>
  );
}

function Legenda() {
  const linha = (cor: string, tracejado: boolean, texto: string) => (
    <span key={texto} className="flex items-center gap-1.5">
      <svg width="28" height="8" aria-hidden><line x1="0" y1="4" x2="28" y2="4" style={{ stroke: cor, strokeWidth: 2.5, strokeDasharray: tracejado ? '6 4' : undefined }} /></svg>
      {texto}
    </span>
  );
  return (
    <div className="space-y-1.5 text-xs text-muted-foreground">
      <div className="flex flex-wrap gap-x-5 gap-y-1">
        {linha(COR.fraude, false, 'Vínculo com loja com fraude confirmada')}
        {linha(COR.alerta, false, 'Vínculo compartilhado entre lojas/pessoas')}
        {linha(COR.alerta, true, 'PIX ≥ R$ 5 mil (seta = sentido do dinheiro; vermelho = com loja fraudada)')}
        {linha(COR.normal, false, 'Vínculo exclusivo da loja')}
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1">
        {['CONTA', 'DISPOSITIVO', 'TELEFONE', 'EMAIL', 'ENDERECO', 'IDENTIDADE'].map((t) => (
          <span key={t} className="flex items-center gap-1" style={{ color: `var(${TIPOS[t]?.token ?? '--muted-foreground'})` }}>
            {createElement(ICONES[t] ?? Network, { className: 'h-3.5 w-3.5' })}
            <span className="text-muted-foreground">{TIPOS[t]?.rotulo}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

function Fluxo({ visao, completo, onCompleto, onSelecionarLoja, altura }: {
  visao: VisaoRede;
  completo: boolean;
  onCompleto: (v: boolean) => void;
  onSelecionarLoja: (nuEc: string) => void;
  altura: number;
}) {
  const { fitView, fitBounds, getNodes, getNodesBounds } = useReactFlow();
  const iniciais = useMemo<NoFluxo[]>(() => visao.nos.map((n) => ({
    id: n.id, type: n.tipo === 'LOJA' ? 'loja' : n.tipo === 'GRUPO' ? 'grupo' : 'atributo', position: n.posicao, data: n,
  })), [visao]);
  const [nodes, , onNodesChange] = useNodesState(iniciais);
  const edges = useMemo<ArestaFluxo[]>(() => [...visao.arestas]
    .sort((a, b) => ORDEM_DESENHO[a.nivel] - ORDEM_DESENHO[b.nivel])
    .map((a) => ({
      id: a.id, source: a.source, target: a.target, type: a.tipo, data: a,
      sourceHandle: a.tipo === 'pix' ? 'pix-s' : 'v',
      targetHandle: a.tipo === 'pix' ? 'pix-t' : null,
      animated: a.tipo === 'pix' && (a.nivel === 'fraude' || a.ciclo),
      markerEnd: a.tipo === 'pix' ? `pld-seta-${a.nivel}` : undefined,
      markerStart: a.tipo === 'pix' && a.bidirecional ? `pld-seta-${a.nivel}` : undefined,
    })), [visao]);

  const porId = useMemo(() => new Map(visao.nos.map((n) => [n.id, n])), [visao]);
  const vizinhanca = useMemo(() => {
    const m = new Map<string, { vizinhos: Set<string>; arestas: Set<string> }>();
    const de = (id: string) => m.get(id) ?? m.set(id, { vizinhos: new Set([id]), arestas: new Set() }).get(id);
    for (const a of visao.arestas) {
      for (const [x, y] of [[a.source, a.target], [a.target, a.source]]) {
        const v = de(x);
        v?.vizinhos.add(y);
        v?.arestas.add(a.id);
      }
    }
    return m;
  }, [visao]);

  // Enquadra nós + arcos de PIX (o fitView padrão só considera os nós) e reenquadra quando o card muda de largura
  const margemArcos = useMemo(() => {
    const y = new Map(visao.nos.map((n) => [n.id, n.posicao.y]));
    return Math.max(0, ...visao.arestas.filter((a) => a.tipo === 'pix').map((a) => 0.75 * curvaPix((y.get(a.target) ?? 0) - (y.get(a.source) ?? 0)) + 90));
  }, [visao]);
  const enquadrar = useCallback(() => {
    const b = getNodesBounds(getNodes());
    const x = Math.min(b.x, -margemArcos);
    void fitBounds({ x, y: b.y, width: b.x + b.width - x, height: b.height }, { padding: 0.1 });
  }, [fitBounds, getNodes, getNodesBounds, margemArcos]);
  const caixa = useRef<HTMLDivElement>(null);
  const inicializados = useNodesInitialized();
  useEffect(() => {
    if (!inicializados || !caixa.current) return;
    const obs = new ResizeObserver(() => enquadrar());
    obs.observe(caixa.current);
    return () => obs.disconnect();
  }, [inicializados, enquadrar]);

  const [foco, setFoco] = useState<string | null>(null);
  const [sobreNo, setSobreNo] = useState<string | null>(null);
  const [sobreAresta, setSobreAresta] = useState<string | null>(null);
  const destaque = useMemo<Destaque>(() => {
    const aresta = sobreAresta ? visao.arestas.find((a) => a.id === sobreAresta) : undefined;
    if (aresta) return { ativo: aresta.source, porAresta: true, vizinhos: new Set([aresta.source, aresta.target]), arestas: new Set([aresta.id]), rotulos: true };
    const ativo = sobreNo ?? foco;
    const v = ativo ? vizinhanca.get(ativo) : undefined;
    if (!ativo) return SEM_DESTAQUE;
    return { ativo, porAresta: false, vizinhos: v?.vizinhos ?? new Set([ativo]), arestas: v?.arestas ?? new Set(), rotulos: (v?.arestas.size ?? 0) <= 12 };
  }, [foco, sobreNo, sobreAresta, visao, vizinhanca]);

  const focar = (id: string) => {
    setFoco(id);
    const alvo = [...(vizinhanca.get(id)?.vizinhos ?? [id])].map((x) => ({ id: x }));
    void fitView({ nodes: alvo, duration: 500, padding: 0.3, maxZoom: 1.2 });
  };
  const noFoco = foco ? porId.get(foco) : undefined;
  const inspecionado = noFoco ?? visao.nos.find((n) => n.raiz);

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-xs text-muted-foreground">
          Passe o mouse para destacar os vínculos · clique para ver detalhes · duplo clique numa loja abre a rede dela · arraste para reorganizar
        </p>
        <div className="flex items-center gap-2">
          <Switch id="vinculos-exclusivos" checked={completo} onCheckedChange={onCompleto} />
          <Label htmlFor="vinculos-exclusivos" className="text-xs font-normal">
            Mostrar vínculos exclusivos{!completo && visao.ocultos > 0 ? ` (${visao.ocultos} ocultos)` : ''}
          </Label>
        </div>
      </div>
      <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_18rem]">
      <div ref={caixa} className="relative w-full overflow-hidden rounded-md border bg-background" style={{ height: altura }}>
        <Setas />
        <DestaqueCtx.Provider value={destaque}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            nodeTypes={TIPOS_NO}
            edgeTypes={TIPOS_ARESTA}
            colorMode="system"
            fitView
            fitViewOptions={{ padding: 0.12 }}
            minZoom={0.2}
            maxZoom={1.75}
            nodesConnectable={false}
            elementsSelectable={false}
            edgesFocusable={false}
            deleteKeyCode={null}
            zoomOnDoubleClick={false}
            onNodeMouseEnter={(_, n) => setSobreNo(n.id)}
            onNodeMouseLeave={() => setSobreNo(null)}
            onEdgeMouseEnter={(_, e) => setSobreAresta(e.id)}
            onEdgeMouseLeave={() => setSobreAresta(null)}
            onNodeClick={(_, n) => setFoco((f) => (f === n.id ? null : n.id))}
            onNodeDoubleClick={(_, n) => { if (n.data.tipo === 'LOJA' && !n.data.raiz) onSelecionarLoja(n.id.replace('LOJA:', '')); }}
            onPaneClick={() => setFoco(null)}
          >
            <Background variant={BackgroundVariant.Dots} gap={18} size={1} />
            <Controls showInteractive={false} showFitView={false} position="bottom-left">
              <ControlButton onClick={enquadrar} title="Enquadrar a rede" aria-label="Enquadrar a rede"><Maximize className="h-3 w-3" /></ControlButton>
            </Controls>
            {visao.arestas.length === 0 && (
              <Panel position="top-center" className="max-w-md rounded-md border bg-card p-3 text-center text-sm shadow-sm">
                Nenhum vínculo compartilhado: a loja não divide conta, sócio, telefone, e-mail, endereço ou dispositivo com outras lojas ou pessoas,
                nem troca PIX ≥ R$ 5 mil com a rede.
                {!completo && visao.ocultos > 0 && (
                  <Button variant="link" size="sm" onClick={() => onCompleto(true)}>Ver os {visao.ocultos} vínculos exclusivos</Button>
                )}
              </Panel>
            )}
          </ReactFlow>
        </DestaqueCtx.Provider>
      </div>
      {inspecionado && (
        <Detalhe no={inspecionado} emFoco={noFoco !== undefined} altura={altura} porId={porId} onFocar={focar} onFechar={() => setFoco(null)} onSelecionarLoja={onSelecionarLoja} />
      )}
      </div>
      <Legenda />
      {visao.sinais.length > 0 && (
        <div className="space-y-1.5">
          <p className="text-xs font-medium text-muted-foreground">Por que a rede chama atenção — clique para localizar no grafo</p>
          <div className="grid gap-1.5 md:grid-cols-2">
            {visao.sinais.slice(0, 8).map((s) => (
              <button key={s.id} type="button" onClick={() => focar(s.alvo)}
                className={cn('flex items-start gap-2 rounded-md border px-2 py-1.5 text-left text-xs hover:bg-muted', foco === s.alvo && 'bg-muted')}>
                <span className="mt-1 h-2 w-2 shrink-0 rounded-full" style={{ backgroundColor: COR[s.nivel] }} />
                <span>{s.texto}</span>
              </button>
            ))}
          </div>
          {visao.sinais.length > 8 && <p className="text-xs text-muted-foreground">e mais {visao.sinais.length - 8} {visao.sinais.length - 8 === 1 ? 'sinal' : 'sinais'}</p>}
        </div>
      )}
    </div>
  );
}

/**
 * Visão simples da rede de vínculos: lojas à esquerda, o que elas compartilham à direita e PIX em arcos.
 * Destaca em vermelho o que liga a loja a fraude confirmada e em âmbar o que é compartilhado.
 */
export function GrafoRede({ linhas, raiz, onSelecionarLoja, altura }: {
  linhas: LinhaRede[];
  raiz: string;
  onSelecionarLoja: (nuEc: string) => void;
  altura?: number;
}) {
  const [completo, setCompleto] = useState(false);
  const visao = useMemo(() => montarVisao(linhas, raiz, completo), [linhas, raiz, completo]);
  // Altura acompanha a coluna mais longa (lojas ou vínculos), para a rede não ficar minúscula
  const colunaLojas = visao.nos.filter((n) => n.tipo === 'LOJA' || n.tipo === 'GRUPO').length;
  const alturaGrafo = altura ?? Math.min(780, Math.max(520, 100 + Math.max(colunaLojas * 66, (visao.nos.length - colunaLojas) * 54)));
  return (
    <ReactFlowProvider key={`${raiz}|${linhas.length}|${String(completo)}`}>
      <Fluxo visao={visao} completo={completo} onCompleto={setCompleto} onSelecionarLoja={onSelecionarLoja} altura={alturaGrafo} />
    </ReactFlowProvider>
  );
}
