import { useMemo, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Separator,
  Skeleton,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import { MessageSquareText } from 'lucide-react';
import { ErroConsulta, FaixaBadge, Sinais, StatusLoja } from '@/components/Indicadores';
import { GrafoRede } from '@/components/GrafoRede';
import { montarVisao } from '@/lib/grafo';
import type { LinhaRede } from '@/lib/grafo';
import { fmtInteiro, fmtMoeda, fmtPct, fmtScore, num } from '@/lib/format';
import { perguntaExplicar } from '@/lib/genie';

const SEM_PARAMS = {};

function Seletor({ ec, onEc }: { ec: string; onEc: (v: string) => void }) {
  const { data, loading } = useAnalyticsQuery('top_lojas_alto', SEM_PARAMS);
  const [digitado, setDigitado] = useState('');
  return (
    <div className="flex flex-wrap items-end gap-4">
      <div className="space-y-1">
        <Label htmlFor="loja">Loja em risco ALTO</Label>
        {loading ? <Skeleton className="h-9 w-80" /> : (
          <Select value={ec} onValueChange={onEc}>
            <SelectTrigger id="loja" className="w-80"><SelectValue placeholder="Escolha uma loja" /></SelectTrigger>
            <SelectContent>
              {(data ?? []).map((l) => (
                <SelectItem key={l.nu_ec} value={String(l.nu_ec)}>
                  {l.nm_loja} · EC {l.nu_ec} · {fmtScore(l.score_pld)}{num(l.fl_novo_suspeito) === 1 ? ' · novo suspeito' : ''}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
      </div>
      <form className="flex items-end gap-2" onSubmit={(e) => { e.preventDefault(); if (/^\d+$/.test(digitado.trim())) onEc(digitado.trim()); }}>
        <div className="space-y-1">
          <Label htmlFor="ec">ou informe o número do EC</Label>
          <Input id="ec" className="w-44" inputMode="numeric" placeholder="ex.: 2112816701" value={digitado} onChange={(e) => setDigitado(e.target.value)} />
        </div>
        <Button type="submit" variant="outline">Ver rede</Button>
      </form>
    </div>
  );
}

function Ficha({ ec }: { ec: string }) {
  const navigate = useNavigate();
  const params = useMemo(() => ({ nu_ec: sql.bigint(ec) }), [ec]);
  const { data, loading, error } = useAnalyticsQuery('detalhe_loja', params);
  const l = data?.[0];
  if (error) return <ErroConsulta erro={error} />;
  if (loading) return <Card><CardContent className="space-y-3 pt-6">{Array.from({ length: 6 }, (_, i) => <Skeleton key={i} className="h-5 w-full" />)}</CardContent></Card>;
  if (!l) return <Empty><EmptyHeader><EmptyTitle>EC {ec} não encontrado</EmptyTitle><EmptyDescription>Confira o número do estabelecimento.</EmptyDescription></EmptyHeader></Empty>;
  const metricas: [string, string][] = [
    ['Lojas no mesmo grupo', fmtInteiro(l.qt_lojas_componente)],
    ['CNPJs diferentes no grupo', fmtInteiro(l.qt_raizes_componente)],
    ['Outras lojas fraudadas no grupo', fmtInteiro(l.qt_outras_lojas_fraude_componente)],
    ['Distância até loja fraudada', num(l.dist_fraude) >= 99 ? 'sem caminho' : `${l.dist_fraude} saltos`],
    ['Vizinhos com fraude', fmtPct(l.pct_vizinhos_fraude)],
    ['Triângulos', fmtInteiro(l.qt_triangulos)],
    ['Ciclos de PIX', fmtInteiro(l.qt_ciclos_pix)],
    ['Contas de liquidação', fmtInteiro(l.qt_contas_liquidacao)],
  ];
  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-start justify-between gap-2">
          <div>
            <CardTitle>{l.nm_loja}</CardTitle>
            <CardDescription>EC {l.nu_ec} · {l.nm_razao_social} · {l.nm_ramo} · {l.nm_municipio}/{l.sg_uf}</CardDescription>
          </div>
          <div className="flex items-center gap-2"><FaixaBadge faixa={l.faixa_risco} /><StatusLoja fraude={l.fl_fraude_confirmada} novo={l.fl_novo_suspeito} /></div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4 text-sm">
        <div className="flex items-baseline gap-3">
          <span className="text-3xl font-semibold tabular-nums">{fmtScore(l.score_pld)}</span>
          <span className="text-muted-foreground">score PLD · posição {fmtInteiro(l.posicao_fila)} na fila</span>
        </div>
        <div>
          <p className="mb-1 text-xs font-medium text-muted-foreground">Principais sinais do modelo</p>
          <Sinais principais={l.principais_sinais} />
        </div>
        <Separator />
        <dl className="grid grid-cols-2 gap-x-4 gap-y-2">
          {metricas.map(([k, v]) => (
            <div key={k}><dt className="text-xs text-muted-foreground">{k}</dt><dd className="tabular-nums">{v}</dd></div>
          ))}
        </dl>
        <Separator />
        <dl className="grid grid-cols-2 gap-x-4 gap-y-2">
          <div><dt className="text-xs text-muted-foreground">Faturamento (último mês)</dt><dd className="tabular-nums">{fmtMoeda(l.vl_faturamento)}</dd></div>
          <div><dt className="text-xs text-muted-foreground">Chargeback</dt><dd className="tabular-nums">{num(l.pc_chargeback).toFixed(2).replace('.', ',')}%</dd></div>
          <div><dt className="text-xs text-muted-foreground">Dias desde a afiliação</dt><dd className="tabular-nums">{fmtInteiro(l.qt_dias_afiliacao)}</dd></div>
          <div><dt className="text-xs text-muted-foreground">Sócio PEP / alteração societária</dt><dd>{num(l.in_socio_pep) ? 'Sim' : 'Não'} / {num(l.in_alteracao_societaria) ? 'Sim' : 'Não'}</dd></div>
        </dl>
        <Button className="w-full" onClick={() => void navigate(`/genie?pergunta=${encodeURIComponent(perguntaExplicar(l.nm_loja, l.nu_ec))}`)}>
          <MessageSquareText className="mr-2 h-4 w-4" /> Explicar com o Genie Agent
        </Button>
      </CardContent>
    </Card>
  );
}

function Rede({ ec, onEc }: { ec: string; onEc: (v: string) => void }) {
  const params = useMemo(() => ({ nu_ec: sql.bigint(ec) }), [ec]);
  const { data, loading, error } = useAnalyticsQuery('rede_loja', params);
  const linhas = useMemo(() => (data ?? []) as LinhaRede[], [data]);
  const raiz = `LOJA:${ec}`;
  // Mesma visão do grafo: lojas já ordenadas por fraude confirmada e score, com o que cada uma compartilha com a rede
  const conectadas = useMemo(() => montarVisao(linhas, raiz, false).nos.filter((n) => n.tipo === 'LOJA' && !n.raiz), [linhas, raiz]);

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>
            {loading ? 'Rede de vínculos' : conectadas.length > 0
              ? `Ligada a ${conectadas.length} outras lojas — ${conectadas.filter((c) => c.fraude).length} com fraude confirmada`
              : 'Nenhuma outra loja ligada por atributos compartilhados'}
          </CardTitle>
          <CardDescription>
            O que a loja divide com outras lojas e pessoas (conta, sócio, dispositivo, telefone, e-mail, endereço) e o PIX entre elas.
            Donos e sócios aparecem dentro da loja. Fonte: gold.grafo_*_resolvido
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {error && <ErroConsulta erro={error} />}
          {loading && <Skeleton className="h-[600px] w-full" />}
          {!loading && !error && linhas.length === 0 && (
            <Empty><EmptyHeader><EmptyTitle>Sem vínculos para este EC</EmptyTitle><EmptyDescription>A loja não tem atributos no grafo resolvido.</EmptyDescription></EmptyHeader></Empty>
          )}
          {!loading && !error && linhas.length > 0 && <GrafoRede linhas={linhas} raiz={raiz} onSelecionarLoja={onEc} />}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Lojas conectadas</CardTitle>
          <CardDescription>Ordenadas por fraude confirmada e score · clique para abrir a rede da loja</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
          {loading && Array.from({ length: 5 }, (_, i) => <Skeleton key={i} className="h-12 w-full" />)}
          {!loading && conectadas.length === 0 && <p className="text-sm text-muted-foreground">Nenhuma loja conectada a 3 saltos.</p>}
          {conectadas.map((c) => (
            <button key={c.id} type="button" onClick={() => onEc(c.id.replace('LOJA:', ''))}
              className="w-full rounded-md border p-2 text-left hover:bg-muted">
              <div className="flex items-center justify-between gap-2">
                <span className="truncate font-medium">{c.titulo}</span>
                {c.fraude ? <Badge variant="destructive">Fraude</Badge> : <FaixaBadge faixa={c.faixa} />}
              </div>
              <div className="text-xs text-muted-foreground">
                {c.subtitulo}{c.compartilha.length > 0 ? ` · compartilha ${c.compartilha.join(', ')}` : ''}
              </div>
            </button>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}

export function RedePage() {
  const [busca, setBusca] = useSearchParams();
  const { data: top } = useAnalyticsQuery('top_lojas_alto', SEM_PARAMS);
  const ec = busca.get('ec') ?? (top?.find((l) => num(l.fl_novo_suspeito) === 1)?.nu_ec?.toString() ?? '');
  const onEc = (v: string) => setBusca({ ec: v });
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold tracking-tight">Rede de vínculos</h2>
        <p className="text-sm text-muted-foreground">O que a visão tabular não mostra numa linha só: com quem a loja divide conta, sócio, telefone, endereço e dispositivo.</p>
      </div>
      <Seletor ec={ec} onEc={onEc} />
      {!ec ? <Skeleton className="h-96 w-full" /> : (
        <div className="grid gap-6 2xl:grid-cols-4">
          <div className="2xl:col-span-1"><Ficha ec={ec} /></div>
          <div className="2xl:col-span-3"><Rede ec={ec} onEc={onEc} /></div>
        </div>
      )}
    </div>
  );
}
