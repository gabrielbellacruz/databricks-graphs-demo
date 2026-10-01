import type { ReactNode } from 'react';
import { Alert, AlertDescription, AlertTitle, Badge, Card, CardContent, CardDescription, CardHeader, CardTitle, Skeleton } from '@databricks/appkit-ui/react';
import { AlertTriangle, Network } from 'lucide-react';
import { traduzSinais } from '@/lib/sinais';

/** Card de KPI composto de primitivas: valor + unidade/contexto + fonte/atualização. */
export function Kpi({ titulo, valor, contexto, rodape, carregando, destaque }: {
  titulo: string;
  valor: ReactNode;
  contexto?: ReactNode;
  rodape?: ReactNode;
  carregando?: boolean;
  destaque?: 'destructive' | 'warning';
}) {
  const cor = destaque === 'destructive' ? 'text-destructive' : destaque === 'warning' ? 'text-warning' : 'text-foreground';
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{titulo}</CardDescription>
        {carregando ? <Skeleton className="h-8 w-24" /> : <CardTitle className={`text-3xl tabular-nums ${cor}`}>{valor}</CardTitle>}
      </CardHeader>
      <CardContent className="space-y-1 text-xs text-muted-foreground">
        {contexto && <p>{contexto}</p>}
        {rodape && <p>{rodape}</p>}
      </CardContent>
    </Card>
  );
}

export function FaixaBadge({ faixa }: { faixa: string | null | undefined }) {
  if (faixa === 'ALTO') return <Badge variant="destructive">ALTO</Badge>;
  if (faixa === 'MEDIO') return <Badge variant="outline" className="border-warning text-warning">MÉDIO</Badge>;
  return <Badge variant="secondary">{faixa ?? '—'}</Badge>;
}

export function StatusLoja({ fraude, novo }: { fraude: number | string; novo: number | string }) {
  if (Number(fraude) === 1) return <Badge variant="destructive">Fraude confirmada</Badge>;
  if (Number(novo) === 1) return <Badge variant="outline" className="border-warning text-warning">Novo suspeito</Badge>;
  return <span className="text-xs text-muted-foreground">—</span>;
}

/** Principais sinais do modelo, traduzidos; sinais vindos do grafo levam o ícone de rede. */
export function Sinais({ principais }: { principais: string | null | undefined }) {
  const sinais = traduzSinais(principais);
  if (!sinais.length) return <span className="text-xs text-muted-foreground">—</span>;
  return (
    <div className="flex flex-wrap gap-1">
      {sinais.map((s) => (
        <Badge key={s.rotulo} variant="secondary" className="gap-1 font-normal">
          {s.grafo && <Network className="h-3 w-3" aria-label="sinal do grafo" />}
          {s.rotulo}
        </Badge>
      ))}
    </div>
  );
}

export function ErroConsulta({ erro }: { erro: unknown }) {
  return (
    <Alert variant="destructive">
      <AlertTriangle className="h-4 w-4" />
      <AlertTitle>Não foi possível carregar os dados</AlertTitle>
      <AlertDescription>{String(erro)} — tente novamente em alguns segundos (o SQL warehouse pode estar iniciando).</AlertDescription>
    </Alert>
  );
}
