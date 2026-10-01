import { useEffect, useState } from 'react';

export type Identidade = { email: string | null; user: string | null };

/** Analista logado, a partir dos cabeçalhos x-forwarded-* que a plataforma injeta (rota /api/whoami do servidor). */
export function useIdentidade(): Identidade | null {
  const [eu, setEu] = useState<Identidade | null>(null);
  useEffect(() => {
    fetch('/api/whoami')
      .then((r) => (r.ok ? (r.json() as Promise<Identidade>) : null))
      .then(setEu)
      .catch(() => setEu(null));
  }, []);
  return eu;
}
