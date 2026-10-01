import { useEffect, useMemo, useRef } from 'react';
import { useSearchParams } from 'react-router';
import {
  Alert,
  AlertDescription,
  AlertTitle,
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
  GenieChatInput,
  GenieChatMessageList,
  Spinner,
  useGenieChat,
} from '@databricks/appkit-ui/react';
import { AlertTriangle, RotateCcw, ShieldCheck } from 'lucide-react';
import { useIdentidade } from '@/lib/identidade';

const PERGUNTAS = [
  'Quais são as 10 lojas com maior score de risco que ainda não estão na lista negativa?',
  'Quais lojas compartilham conta de liquidação com lojas que têm fraude confirmada?',
  'Quais ramos de atividade concentram mais lojas em risco ALTO?',
  'Quais pessoas recebem mais PIX de lojas em risco ALTO?',
  'Quantas lojas em risco ALTO têm sócio em comum com uma loja fraudada?',
];

export function GeniePage() {
  const { messages, status, error, sendMessage, reset } = useGenieChat({ alias: 'default' });
  const eu = useIdentidade();
  const [params, setParams] = useSearchParams();
  const enviado = useRef(false);

  // Pergunta vinda do botão "Explicar com o Genie" (fila ou rede): envia uma única vez numa conversa nova
  useEffect(() => {
    const pergunta = params.get('pergunta');
    if (!pergunta || enviado.current || status === 'loading-history' || status === 'streaming') return;
    enviado.current = true;
    if (messages.length > 0) reset();
    sendMessage(pergunta);
    const p = new URLSearchParams(params);
    p.delete('pergunta');
    setParams(p, { replace: true });
  }, [params, status, messages.length, reset, sendMessage, setParams]);

  const ultimoSql = useMemo(() => {
    for (const m of [...messages].reverse())
      for (const a of m.attachments ?? []) if (a.query?.query) return a.query;
    return null;
  }, [messages]);

  const ocupado = status === 'streaming' || status === 'loading-history';

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-2xl font-semibold tracking-tight">Genie Agent PLD</h2>
          <p className="text-sm text-muted-foreground">Pergunte em português sobre a fila de risco, as transações, o grafo de vínculos e as listas — o Genie gera o SQL e explica o resultado.</p>
        </div>
        <Button variant="outline" onClick={reset} disabled={ocupado}><RotateCcw className="mr-2 h-4 w-4" />Nova conversa</Button>
      </div>

      <Alert>
        <ShieldCheck className="h-4 w-4" />
        <AlertTitle className="flex flex-wrap items-center gap-2">
          Você está conectado como <Badge variant="secondary">{eu?.email ?? 'usuário autenticado'}</Badge>
        </AlertTitle>
        <AlertDescription>
          As consultas do Genie rodam com o <strong>service principal do App</strong> (permissões do App no Unity Catalog, não as suas) sobre as tabelas
          do catálogo <code>cielo_pld</code>. Ambiente de demonstração com dados sintéticos.
        </AlertDescription>
      </Alert>

      <div className="grid gap-6 xl:grid-cols-3">
        <Card className="flex flex-col xl:col-span-2" style={{ height: 680 }}>
          <CardContent className="flex min-h-0 flex-1 flex-col gap-3 pt-6">
            {messages.length === 0 && status === 'idle' ? (
              <Empty className="flex-1">
                <EmptyHeader>
                  <EmptyTitle>Comece com uma pergunta</EmptyTitle>
                  <EmptyDescription>Escolha um exemplo ao lado ou escreva a sua. Para explicar uma loja específica, use o botão “Explicar com o Genie” na fila ou na rede.</EmptyDescription>
                </EmptyHeader>
              </Empty>
            ) : (
              <GenieChatMessageList messages={messages} status={status} className="min-h-0 flex-1" />
            )}
            {status === 'streaming' && (
              <p className="flex items-center gap-2 text-sm text-muted-foreground"><Spinner className="h-4 w-4" /> O Genie está analisando os dados…</p>
            )}
            {status === 'error' && (
              <Alert variant="destructive">
                <AlertTriangle className="h-4 w-4" />
                <AlertTitle>O Genie não conseguiu responder</AlertTitle>
                <AlertDescription>{error ?? 'Reformule a pergunta ou tente novamente.'}</AlertDescription>
              </Alert>
            )}
            <GenieChatInput onSend={sendMessage} disabled={ocupado} placeholder="Ex.: por que a loja EC 2112816701 está com risco alto?" />
            <p className="text-xs text-muted-foreground">
              Resposta gerada por IA a partir dos dados via Genie — confira o SQL gerado antes de usar a conclusão numa investigação.
            </p>
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Perguntas sugeridas</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              {PERGUNTAS.map((q) => (
                <Button key={q} variant="outline" className="h-auto justify-start whitespace-normal text-left" disabled={ocupado} onClick={() => sendMessage(q)}>
                  {q}
                </Button>
              ))}
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>SQL gerado</CardTitle>
              <CardDescription>{ultimoSql?.title ?? 'Como a última resposta foi calculada'}</CardDescription>
            </CardHeader>
            <CardContent>
              {ultimoSql?.query
                ? <pre className="max-h-80 overflow-auto whitespace-pre-wrap rounded-md bg-muted p-3 text-xs">{ultimoSql.query}</pre>
                : <p className="text-sm text-muted-foreground">Nenhuma consulta ainda.</p>}
              {ultimoSql?.description && <p className="pt-2 text-xs text-muted-foreground">{ultimoSql.description}</p>}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
