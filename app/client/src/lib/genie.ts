/** Pergunta padrão do botão "Explicar com o Genie" (fila e rede). */
export function perguntaExplicar(nome: string, ec: number | string) {
  return `Explique por que a loja ${nome} (EC ${ec}) está com risco PLD alto: quais sinais de cadastro, transações e vínculos no grafo mais pesam, e com quais outras lojas ela está conectada?`;
}
