import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import {
  BarChart,
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
  Skeleton,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import { MessageSquareText, Network } from 'lucide-react';
import { ErroConsulta, FaixaBadge, Kpi, Sinais, StatusLoja } from '@/components/Indicadores';
import { fmtDataHora, fmtInteiro, fmtScore, num } from '@/lib/format';
import { perguntaExplicar } from '@/lib/genie';

const POR_PAGINA = 15;
const SEM_PARAMS = {};

function Kpis() {
  const { data, loading, error } = useAnalyticsQuery('kpis_risco', SEM_PARAMS);
  const k = data?.[0];
  const fonte = `Fonte: gold.fila_investigacao_pld · atualizado em ${fmtDataHora(k?.ts_atualizacao)}`;
  if (error) return <ErroConsulta erro={error} />;
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <Kpi titulo="Novos suspeitos" carregando={loading} destaque="warning" valor={fmtInteiro(k?.qt_novos_suspeitos)}
        contexto="lojas em risco ALTO que ainda não estão na lista negativa" rodape={fonte} />
      <Kpi titulo="Lojas em risco ALTO" carregando={loading} valor={fmtInteiro(k?.qt_alto)}
        contexto={`de ${fmtInteiro(k?.qt_lojas)} lojas pontuadas (top 5% do score)`} rodape={fonte} />
      <Kpi titulo="Fraude confirmada" carregando={loading} destaque="destructive" valor={fmtInteiro(k?.qt_fraude_confirmada)}
        contexto="lojas com inserção ativa na lista negativa Lynx" rodape="Fonte: silver.lista_monitoramento" />
      <Kpi titulo="Grupos suspeitos" carregando={loading} valor={fmtInteiro(k?.qt_grupos_suspeitos)}
        contexto="grupos com 3+ lojas de CNPJs diferentes ligadas por conta, sócio, telefone, endereço ou dispositivo"
        rodape="Fonte: gold.features_grafo_loja (connectedComponents)" />
    </div>
  );
}

function Fila() {
  const navigate = useNavigate();
  const [faixa, setFaixa] = useState('ALTO');
  const [somenteNovos, setSomenteNovos] = useState(true);
  const [texto, setTexto] = useState('');
  const [busca, setBusca] = useState('');
  const [pagina, setPagina] = useState(0);

  useEffect(() => {
    const t = setTimeout(() => { setBusca(texto.trim()); setPagina(0); }, 400);
    return () => clearTimeout(t);
  }, [texto]);

  const params = useMemo(
    () => ({
      faixa: sql.string(faixa === 'todas' ? '' : faixa),
      somente_novos: sql.string(somenteNovos ? 'S' : 'N'),
      busca: sql.string(busca),
    }),
    [faixa, somenteNovos, busca],
  );
  const { data, loading, error } = useAnalyticsQuery('fila_risco', params);
  const linhas = data ?? [];
  const paginas = Math.max(1, Math.ceil(linhas.length / POR_PAGINA));
  const visiveis = linhas.slice(pagina * POR_PAGINA, (pagina + 1) * POR_PAGINA);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Fila de investigação</CardTitle>
        <CardDescription>Ordenada pelo score do modelo (XGBoost com features de grafo). Clique numa loja para ver a rede de vínculos.</CardDescription>
        <div className="flex flex-wrap items-end gap-4 pt-2">
          <div className="space-y-1">
            <Label htmlFor="faixa">Faixa de risco</Label>
            <Select value={faixa} onValueChange={(v) => { setFaixa(v); setPagina(0); }}>
              <SelectTrigger id="faixa" className="w-36"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="todas">Todas</SelectItem>
                <SelectItem value="ALTO">Alto</SelectItem>
                <SelectItem value="MEDIO">Médio</SelectItem>
                <SelectItem value="BAIXO">Baixo</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="flex items-center gap-2 pb-2">
            <Switch id="novos" checked={somenteNovos} onCheckedChange={(v) => { setSomenteNovos(v); setPagina(0); }} />
            <Label htmlFor="novos">Só novos suspeitos (fora da lista negativa)</Label>
          </div>
          <div className="space-y-1">
            <Label htmlFor="busca">Buscar loja ou EC</Label>
            <Input id="busca" className="w-56" placeholder="nome ou número do EC" value={texto} onChange={(e) => setTexto(e.target.value)} />
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {error && <ErroConsulta erro={error} />}
        {loading && <div className="space-y-2">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-10 w-full" />)}</div>}
        {!loading && !error && linhas.length === 0 && (
          <Empty>
            <EmptyHeader>
              <EmptyTitle>Nenhuma loja com esses filtros</EmptyTitle>
              <EmptyDescription>Desligue “só novos suspeitos” ou escolha outra faixa de risco.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        )}
        {!loading && !error && linhas.length > 0 && (
          <>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-12 text-right">#</TableHead>
                  <TableHead>Loja</TableHead>
                  <TableHead className="text-right">Score</TableHead>
                  <TableHead>Faixa</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Principais sinais</TableHead>
                  <TableHead className="text-right">Lojas no grupo</TableHead>
                  <TableHead className="w-32" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {visiveis.map((l) => (
                  <TableRow key={l.nu_ec} className="cursor-pointer" onClick={() => void navigate(`/rede?ec=${l.nu_ec}`)}>
                    <TableCell className="text-right tabular-nums text-muted-foreground">{l.posicao_fila}</TableCell>
                    <TableCell>
                      <div className="font-medium">{l.nm_loja}</div>
                      <div className="text-xs text-muted-foreground">EC {l.nu_ec} · {l.nm_ramo} · {l.sg_uf}</div>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{fmtScore(l.score_pld)}</TableCell>
                    <TableCell><FaixaBadge faixa={l.faixa_risco} /></TableCell>
                    <TableCell><StatusLoja fraude={l.fl_fraude_confirmada} novo={l.fl_novo_suspeito} /></TableCell>
                    <TableCell className="max-w-md"><Sinais principais={l.principais_sinais} /></TableCell>
                    <TableCell className="text-right tabular-nums">{fmtInteiro(l.qt_lojas_componente)}</TableCell>
                    <TableCell>
                      <div className="flex justify-end gap-1">
                        <Button size="icon" variant="ghost" title="Ver rede de vínculos" onClick={(e) => { e.stopPropagation(); void navigate(`/rede?ec=${l.nu_ec}`); }}>
                          <Network className="h-4 w-4" />
                        </Button>
                        <Button size="icon" variant="ghost" title="Explicar com o Genie" onClick={(e) => {
                          e.stopPropagation();
                          void navigate(`/genie?pergunta=${encodeURIComponent(perguntaExplicar(l.nm_loja, l.nu_ec))}`);
                        }}>
                          <MessageSquareText className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <div className="flex items-center justify-between pt-3 text-sm text-muted-foreground">
              <span>{fmtInteiro(linhas.length)} lojas{linhas.length >= 200 ? ' (mostrando as 200 primeiras da fila)' : ''}</span>
              <div className="flex items-center gap-2">
                <Button variant="outline" size="sm" disabled={pagina === 0} onClick={() => setPagina((p) => p - 1)}>Anterior</Button>
                <span className="tabular-nums">{pagina + 1} / {paginas}</span>
                <Button variant="outline" size="sm" disabled={pagina + 1 >= paginas} onClick={() => setPagina((p) => p + 1)}>Próxima</Button>
              </div>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

export function FilaPage() {
  const { data } = useAnalyticsQuery('kpis_risco', SEM_PARAMS);
  const novos = data?.[0] ? num(data[0].qt_novos_suspeitos) : null;
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold tracking-tight">
          {novos === null ? 'Fila de risco PLD' : `${fmtInteiro(novos)} lojas de alto risco ainda não estão na lista negativa`}
        </h2>
        <p className="text-sm text-muted-foreground">Priorize a investigação pelas lojas com maior score — os sinais com ícone de rede vêm do grafo de vínculos.</p>
      </div>
      <Kpis />
      <div className="grid gap-6 xl:grid-cols-3">
        <div className="xl:col-span-2"><Fila /></div>
        <Card>
          <CardHeader>
            <CardTitle>Risco ALTO por ramo de atividade</CardTitle>
            <CardDescription>Lojas em risco ALTO: novos suspeitos × fraude já confirmada. Fonte: gold.fila_investigacao_pld</CardDescription>
          </CardHeader>
          <CardContent>
            <BarChart queryKey="risco_por_ramo" parameters={SEM_PARAMS} xKey="ramo" yKey={['novos_suspeitos', 'fraude_confirmada']}
              orientation="horizontal" stacked showLegend colorPalette="categorical" height={520} ariaLabel="Lojas em risco alto por ramo" />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
