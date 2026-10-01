"""Gera genie/genie_space.json — configuração (serialized_space v2) do Genie Agent de PLD.

Uso:  python3 genie/build_space.py   (rodar a partir de app/)
"""
import hashlib
import json
from pathlib import Path

C = "cielo_pld"


def hid(chave: str) -> str:
    """ID determinístico (32 hex minúsculo) — mantém o arquivo estável entre execuções."""
    return hashlib.md5(chave.encode()).hexdigest()


def col(nome, descricao=None, sinonimos=None, entidade=False, formato=False, excluir=False):
    c = {"column_name": nome}
    if descricao:
        c["description"] = [descricao]
    if sinonimos:
        c["synonyms"] = sinonimos
    if entidade:
        c["enable_entity_matching"] = True
    if formato:
        c["enable_format_assistance"] = True
    if excluir:
        c["exclude"] = True
    return c


EC = col("nu_ec", "Número do Estabelecimento Comercial (EC) — identificador da loja.", ["EC", "estabelecimento", "número do EC", "loja"])

TABELAS = {
    f"{C}.gold.fila_investigacao_pld": [
        EC,
        col("nm_loja", "Nome fantasia da loja.", ["nome da loja", "estabelecimento"], entidade=True),
        col("nm_ramo", "Ramo de atividade da loja.", ["ramo", "segmento", "MCC"], entidade=True, formato=True),
        col("fl_fraude_confirmada", "1 = loja com inserção ativa na lista negativa de EC (fraude/PLD confirmada pela área).", ["fraude confirmada", "lista negativa", "fraudada"]),
        col("id_componente", "Grupo de vínculos (componente conexo do grafo). Lojas com o mesmo id_componente estão ligadas por conta, sócio, telefone, e-mail, endereço ou dispositivo.", ["grupo", "anel", "rede", "componente"]),
        col("score_pld", "Score de risco PLD do modelo XGBoost (0 a 1, out-of-fold). Quanto maior, maior a prioridade de investigação.", ["score", "risco", "probabilidade"]),
        col("faixa_risco", "Faixa do score: ALTO (top 5%), MEDIO (top 15%), BAIXO.", ["faixa", "nível de risco"], entidade=True, formato=True),
        col("principais_sinais", "Três features do modelo que mais elevaram o score da loja (nomes técnicos separados por vírgula, ex.: fl_conta_compartilhada = conta compartilhada com outra loja).", ["motivos", "sinais", "explicação"]),
        col("posicao_fila", "Posição na fila de investigação (1 = maior risco).", ["ranking", "posição"]),
        col("fl_novo_suspeito", "1 = loja em risco ALTO que ainda NÃO está na lista negativa (novo suspeito a investigar).", ["novo suspeito", "não listada"]),
        col("ts_score", "Data/hora do cálculo do score."),
    ],
    f"{C}.gold.features_grafo_loja": [
        EC,
        col("grau", "Número de conexões da loja no grafo."),
        col("qt_contas_liquidacao", "Quantidade de contas de liquidação (domicílio) usadas pela loja."),
        col("qt_socios", "Quantidade de sócios/responsáveis pela maquininha."),
        col("pagerank", "PageRank do titular no fluxo de PIX — centralidade no fluxo de dinheiro.", ["centralidade"]),
        col("qt_pagadores_pix", "Pessoas/empresas distintas que enviaram PIX ao titular."),
        col("qt_destinos_pix", "Pessoas/empresas distintas que receberam PIX do titular."),
        col("id_componente", "Grupo de vínculos (mesmo que na fila).", ["grupo", "anel"]),
        col("tam_componente", "Total de vértices (lojas, pessoas, contas, telefones...) no grupo de vínculos."),
        col("qt_lojas_componente", "Quantidade de lojas no mesmo grupo de vínculos.", ["lojas no grupo"]),
        col("qt_raizes_componente", "Quantidade de raízes de CNPJ diferentes no grupo (lojas sem vínculo societário ligadas entre si)."),
        col("qt_outras_lojas_fraude_componente", "Outras lojas com fraude confirmada no mesmo grupo (exclui a própria loja)."),
        col("qt_lojas_comunidade", "Lojas na mesma comunidade (labelPropagation sobre vínculos + PIX)."),
        col("chargeback_medio_comunidade", "Chargeback médio (%) das outras lojas da comunidade."),
        col("qt_outras_lojas_fraude_comunidade", "Outras lojas com fraude confirmada na comunidade."),
        col("qt_triangulos", "Triângulos no grafo loja-loja (lojas interligadas)."),
        col("dist_fraude", "Distância (em saltos no grafo de vínculos) até a loja com fraude confirmada mais próxima; 99 = sem caminho.", ["distância até fraude"]),
        col("qt_lojas_vizinhas", "Lojas que compartilham pelo menos um atributo com esta loja."),
        col("pct_vizinhos_chargeback_alto", "Fração das lojas vizinhas com chargeback acima de 1%."),
        col("pct_vizinhos_fraude", "Fração das lojas vizinhas com fraude confirmada."),
        col("fl_conta_compartilhada", "1 = liquida ou recebe PIX numa conta usada por loja de outra raiz de CNPJ.", ["conta compartilhada"]),
        col("fl_socio_comum_fraude", "1 = tem sócio/responsável em comum com loja fraudada.", ["sócio em comum"]),
        col("qt_ciclos_pix", "Ciclos de PIX de alto valor (A→B→C→A) envolvendo o titular.", ["ciclos", "layering"]),
    ],
    f"{C}.silver.loja": [
        EC,
        col("nm_loja", "Nome fantasia.", entidade=True),
        col("nm_razao_social", "Razão social (ou nome do titular PF)."),
        col("nu_doc", "CPF/CNPJ padronizado do titular (só dígitos)."),
        col("tp_pessoa", "PF ou PJ.", entidade=True),
        col("fl_doc_valido", "Documento passou na validação do dígito verificador."),
        col("nu_raiz", "Raiz do CNPJ (8 dígitos) ou CPF — lojas com a mesma raiz pertencem ao mesmo grupo societário."),
        col("tp_cliente", "Tipo de cliente: E-COMMERCE, BALCAO ou SUB.", entidade=True),
        col("sg_uf", "UF da loja.", ["estado", "UF"], entidade=True),
        col("nm_municipio", "Município.", entidade=True),
        col("ds_endereco", "Endereço padronizado."),
        col("nm_ramo", "Ramo de atividade.", entidade=True),
        col("qt_dias_afiliacao", "Dias desde a afiliação à Cielo."),
        col("vl_faturamento", "Faturamento do último mês (R$)."),
        col("pc_chargeback", "Percentual de chargeback do último mês."),
        col("in_socio_pep", "1 = sócio pessoa politicamente exposta.", ["PEP"]),
        col("in_alteracao_societaria", "1 = alteração recente do quadro societário."),
        col("fl_fraude", "1 = fraude confirmada (lista negativa de EC ativa).", ["fraude confirmada"]),
        col("cd_ramo", excluir=True),
    ],
    f"{C}.silver.trns_pix": [
        col("ts_transacao", "Data/hora do PIX (dados de 2026-08-01 a 2026-09-30)."),
        EC,
        col("tp_operacao", "PAGAMENTO (cliente paga a loja), TRANSFERENCIA (loja/pessoa transfere) ou SAQUE.", entidade=True),
        col("vl_trns", "Valor do PIX (R$).", ["valor"]),
        col("fl_aprovada", "PIX aprovado."),
        col("nu_doc_pagador", "CPF/CNPJ do pagador."), col("nm_pagador", "Nome do pagador."),
        col("nu_doc_recebedor", "CPF/CNPJ do recebedor."), col("nm_recebedor", "Nome do recebedor."),
        col("ds_conta_pagador", "Conta do pagador (ISPB|agência|conta)."), col("ds_conta_recebedor", "Conta do recebedor (ISPB|agência|conta)."),
        col("id_dispositivo", "Dispositivo usado pelo pagador."),
        col("qt_dias_conta", "Idade da conta do pagador em dias."),
        col("qt_marcacoes_laranja_90d", "Marcações de conta laranja/fraude do pagador nos últimos 90 dias (DICT).", ["laranja"]),
        col("tp_chave_pagador", entidade=True), col("tp_chave_recebedor", entidade=True),
        col("id_unco_pix", excluir=True), col("ds_chave_pagador", excluir=True), col("ds_chave_recebedor", excluir=True), col("ds_email_pagador", excluir=True),
    ],
    f"{C}.silver.trns_cartao": [
        col("ts_transacao", "Data/hora da transação de cartão."), EC,
        col("vl_trns", "Valor (R$)."), col("tp_cartao", "CREDITO, DEBITO ou PRE-PAGO.", entidade=True),
        col("fl_abaixo_limite", "Valor logo abaixo de R$ 5 mil / R$ 10 mil (fracionamento).", ["fracionamento", "structuring"]),
        col("fl_madrugada", "Transação entre 0h e 4h."), col("fl_valor_redondo", "Valor redondo (múltiplo de R$ 500)."),
        col("fl_regra_pld", "Acionou regra PLD da Lynx."), col("fl_cnp", "Cartão não presente."),
        col("nu_doc_portador", "CPF do portador."), col("nm_portador", "Nome do portador."),
        col("qt_cnar_ordm", excluir=True), col("id_unco", excluir=True), col("nu_telefone_portador", excluir=True), col("ds_email_portador", excluir=True),
        col("ds_endereco_portador", excluir=True), col("ds_endereco_entrega", excluir=True),
    ],
    f"{C}.silver.trns_antecipacao": [
        col("ts_transacao", "Data/hora da antecipação de recebíveis (RAD0)."), EC,
        col("vl_trns", "Valor antecipado (R$)."),
        col("nu_doc_responsavel", "CPF/CNPJ do responsável pela maquininha (sócio).", ["sócio"]),
        col("ds_conta_domicilio", "Conta de domicílio onde o valor foi liquidado."),
        col("id_unco_lynx", excluir=True),
    ],
    f"{C}.gold.grafo_vertices_resolvido": [
        col("id", "Id do vértice: LOJA:<EC>, IDN:<identidade>, CONTA:<ispb|ag|conta>, TEL:, EMAIL:, END:, DISP:, CARTAO:."),
        col("tipo", "LOJA, IDENTIDADE (pessoa/empresa resolvida), CONTA, TELEFONE, EMAIL, ENDERECO, DISPOSITIVO ou CARTAO.", entidade=True),
        col("nome", "Nome da loja/pessoa ou valor do atributo."), col("nu_ec", "EC (só para vértices LOJA)."),
        col("fl_fraude", "1 = loja com fraude confirmada."),
    ],
    f"{C}.gold.grafo_arestas_resolvido": [
        col("src", "Vértice de origem (grafo_vertices_resolvido.id)."), col("dst", "Vértice de destino (grafo_vertices_resolvido.id)."),
        col("relacao", "titular (loja→dono), socio (loja→sócio), liquida_em (loja→conta), localizada_em (loja→endereço), recebe_em / paga_com (pessoa→conta PIX), tem_telefone, tem_email, usa_dispositivo, entrega_em, portador (pessoa→cartão), compra (cartão→loja), pix (pessoa→pessoa, fluxo de dinheiro).", ["vínculo", "relação"], entidade=True),
        col("qt", "Quantidade de eventos na relação."), col("valor", "Valor total (R$) — PIX, compras e antecipações."),
    ],
    f"{C}.silver.identidade": [
        col("cpf_cnpj", "Documento padronizado (pode ser uma grafia com dígito trocado)."),
        col("id_identidade", "Identidade resolvida (Splink + connectedComponents): documentos com o mesmo id são a mesma pessoa/empresa."),
        col("nm_identidade", "Nome mais frequente da identidade."), col("qt_documentos", "Quantos documentos distintos foram unificados nesta identidade."),
    ],
    f"{C}.silver.lista_monitoramento": [
        col("tp_lista", "NEGATIVA, POSITIVA ou POSITIVA AUTORIZADA.", entidade=True),
        col("tp_objeto", "EC, CLIENTE ou CARTAO.", entidade=True),
        col("cd_objeto", "EC (texto) quando tp_objeto = EC; CPF quando CLIENTE; token quando CARTAO."),
        col("fl_ativo", "Inserção ativa (sem data de saída ou saída futura)."),
    ],
    f"{C}.silver.fraude_reportada": [
        EC, col("ts_transacao", "Data/hora da transação fraudada."), col("ts_reporte", "Data/hora do reporte de fraude."),
        col("cd_tipo_rsps_frde", "Código do tipo de fraude reportada."), col("id_unco_lynx", excluir=True), col("nu_token_cartao", excluir=True),
    ],
}

JOINS = [
    ("fila_loja", "gold.fila_investigacao_pld", "fila_investigacao_pld", "silver.loja", "loja", "`fila_investigacao_pld`.`nu_ec` = `loja`.`nu_ec`", "ONE_TO_ONE"),
    ("fila_features", "gold.fila_investigacao_pld", "fila_investigacao_pld", "gold.features_grafo_loja", "features_grafo_loja", "`fila_investigacao_pld`.`nu_ec` = `features_grafo_loja`.`nu_ec`", "ONE_TO_ONE"),
    ("loja_pix", "silver.loja", "loja", "silver.trns_pix", "trns_pix", "`loja`.`nu_ec` = `trns_pix`.`nu_ec`", "ONE_TO_MANY"),
    ("loja_cartao", "silver.loja", "loja", "silver.trns_cartao", "trns_cartao", "`loja`.`nu_ec` = `trns_cartao`.`nu_ec`", "ONE_TO_MANY"),
    ("loja_antp", "silver.loja", "loja", "silver.trns_antecipacao", "trns_antecipacao", "`loja`.`nu_ec` = `trns_antecipacao`.`nu_ec`", "ONE_TO_MANY"),
    ("loja_fraude", "silver.loja", "loja", "silver.fraude_reportada", "fraude_reportada", "`loja`.`nu_ec` = `fraude_reportada`.`nu_ec`", "ONE_TO_MANY"),
    ("loja_identidade", "silver.loja", "loja", "silver.identidade", "identidade", "`loja`.`nu_doc` = `identidade`.`cpf_cnpj`", "MANY_TO_ONE"),
    ("aresta_vertice", "gold.grafo_arestas_resolvido", "grafo_arestas_resolvido", "gold.grafo_vertices_resolvido", "grafo_vertices_resolvido", "`grafo_arestas_resolvido`.`src` = `grafo_vertices_resolvido`.`id`", "MANY_TO_ONE"),
]

F = f"{C}.gold.fila_investigacao_pld"
G = f"{C}.gold.features_grafo_loja"
L = f"{C}.silver.loja"
A = f"{C}.gold.grafo_arestas_resolvido"
VX = f"{C}.gold.grafo_vertices_resolvido"
PIX = f"{C}.silver.trns_pix"

EXEMPLOS = [
    ("novos_suspeitos", "Quais são as lojas de maior risco que ainda não estão na lista negativa?",
     f"SELECT fila_investigacao_pld.posicao_fila, fila_investigacao_pld.nu_ec, fila_investigacao_pld.nm_loja, fila_investigacao_pld.nm_ramo, ROUND(fila_investigacao_pld.score_pld, 3) AS score_pld, fila_investigacao_pld.principais_sinais FROM {F} fila_investigacao_pld WHERE fila_investigacao_pld.fl_novo_suspeito = 1 ORDER BY fila_investigacao_pld.posicao_fila LIMIT 10",
     "Ranking de novos suspeitos: filtre fl_novo_suspeito = 1 e ordene por posicao_fila."),
    ("explicar_loja", "Por que a loja EC 2112816701 está com risco alto?",
     f"SELECT fila_investigacao_pld.nu_ec, fila_investigacao_pld.nm_loja, fila_investigacao_pld.posicao_fila, ROUND(fila_investigacao_pld.score_pld, 3) AS score_pld, fila_investigacao_pld.faixa_risco, fila_investigacao_pld.principais_sinais, loja.nm_ramo, loja.qt_dias_afiliacao, loja.pc_chargeback, loja.in_socio_pep, loja.in_alteracao_societaria, features_grafo_loja.qt_lojas_componente, features_grafo_loja.qt_raizes_componente, features_grafo_loja.qt_outras_lojas_fraude_componente, features_grafo_loja.fl_conta_compartilhada, features_grafo_loja.fl_socio_comum_fraude, features_grafo_loja.qt_ciclos_pix, features_grafo_loja.dist_fraude, features_grafo_loja.pct_vizinhos_fraude FROM {F} fila_investigacao_pld JOIN {L} loja ON loja.nu_ec = fila_investigacao_pld.nu_ec JOIN {G} features_grafo_loja ON features_grafo_loja.nu_ec = fila_investigacao_pld.nu_ec WHERE fila_investigacao_pld.nu_ec = 2112816701",
     "Para explicar o risco de uma loja: troque o EC e combine score, principais_sinais, cadastro e features de grafo."),
    ("conta_compartilhada_fraude", "Quais lojas compartilham conta de liquidação com lojas que têm fraude confirmada?",
     f"SELECT DISTINCT va.nu_ec AS nu_ec, va.nome AS loja, vb.nu_ec AS nu_ec_fraudada, vb.nome AS loja_fraudada, a1.dst AS conta FROM {A} a1 JOIN {A} a2 ON a1.dst = a2.dst AND a1.src <> a2.src JOIN {VX} va ON va.id = a1.src JOIN {VX} vb ON vb.id = a2.src WHERE a1.relacao = 'liquida_em' AND a2.relacao = 'liquida_em' AND va.fl_fraude = 0 AND vb.fl_fraude = 1 ORDER BY loja LIMIT 50",
     "Atributo compartilhado entre lojas: auto-join de grafo_arestas_resolvido pelo dst (conta/telefone/endereço) filtrando relacao."),
    ("socio_comum_fraude", "Quais lojas têm sócio em comum com uma loja fraudada?",
     f"SELECT DISTINCT va.nu_ec AS nu_ec, va.nome AS loja, vs.nome AS socio, vb.nu_ec AS nu_ec_fraudada, vb.nome AS loja_fraudada FROM {A} a1 JOIN {A} a2 ON a1.dst = a2.dst AND a1.src <> a2.src JOIN {VX} va ON va.id = a1.src JOIN {VX} vb ON vb.id = a2.src JOIN {VX} vs ON vs.id = a1.dst WHERE a1.relacao = 'socio' AND a2.relacao = 'socio' AND va.fl_fraude = 0 AND vb.fl_fraude = 1 LIMIT 50",
     "Sócio em comum: mesma lógica de atributo compartilhado com relacao = 'socio'."),
    ("pix_recebedores_alto", "Quem mais recebe PIX de lojas em risco ALTO?",
     f"SELECT trns_pix.nu_doc_recebedor, trns_pix.nm_recebedor, COUNT(*) AS qt_pix, ROUND(SUM(trns_pix.vl_trns), 2) AS vl_total, COUNT(DISTINCT trns_pix.nu_ec) AS qt_lojas FROM {PIX} trns_pix JOIN {F} fila_investigacao_pld ON fila_investigacao_pld.nu_ec = trns_pix.nu_ec WHERE fila_investigacao_pld.faixa_risco = 'ALTO' AND trns_pix.tp_operacao = 'TRANSFERENCIA' AND trns_pix.fl_aprovada GROUP BY trns_pix.nu_doc_recebedor, trns_pix.nm_recebedor ORDER BY vl_total DESC LIMIT 20",
     "Saídas de dinheiro das lojas: tp_operacao = 'TRANSFERENCIA' (loja como pagadora)."),
    ("grupo_loja", "Quais outras lojas estão no mesmo grupo de vínculos da loja EC 2112816701?",
     f"SELECT f2.nu_ec, f2.nm_loja, f2.faixa_risco, ROUND(f2.score_pld, 3) AS score_pld, f2.fl_fraude_confirmada FROM {F} f1 JOIN {F} f2 ON f2.id_componente = f1.id_componente AND f2.nu_ec <> f1.nu_ec WHERE f1.nu_ec = 2112816701 ORDER BY f2.fl_fraude_confirmada DESC, f2.score_pld DESC",
     "Mesmo grupo/anel = mesmo id_componente."),
]

PERGUNTAS = [
    "Quais são as 10 lojas com maior score de risco que ainda não estão na lista negativa?",
    "Quais lojas compartilham conta de liquidação com lojas que têm fraude confirmada?",
    "Quais ramos de atividade concentram mais lojas em risco ALTO?",
    "Quais pessoas recebem mais PIX de lojas em risco ALTO?",
    "Quantas lojas em risco ALTO têm sócio em comum com uma loja fraudada?",
]

INSTRUCOES = """## PURPOSE
- Apoiar analistas de PLD e prevenção a fraude da Cielo a priorizar e explicar lojas (EC) suspeitas usando o score do modelo e o grafo de vínculos.

## DISAMBIGUATION
- "loja", "EC" e "estabelecimento" referem-se a nu_ec.
- "risco alto" = faixa_risco ALTO; "novo suspeito" = fl_novo_suspeito = 1; "fraude confirmada" = lista negativa de EC ativa (fl_fraude_confirmada ou loja.fl_fraude).
- "grupo", "anel" ou "rede" da loja = lojas com o mesmo id_componente.

## DATA QUALITY NOTES
- principais_sinais traz nomes técnicos de features; ao responder, traduza para linguagem de negócio (ex.: fl_conta_compartilhada = conta compartilhada com outra loja; dist_fraude = proximidade de loja fraudada; dias_afiliacao = afiliação recente).
- dist_fraude = 99 significa que não há caminho até loja fraudada.
- Transações (PIX, cartão, antecipação) cobrem 2026-08-01 a 2026-09-30. Dados sintéticos de demonstração.

## CONSTRAINTS
- O score é um indício para priorizar a investigação, não uma prova de lavagem: nunca afirme que a loja cometeu crime.

## Instructions you must follow when providing summaries
- Ao explicar uma loja, cite o score, a posição na fila, os principais sinais traduzidos e os vínculos com outras lojas (grupo, conta, sócio, ciclos de PIX).
- Arredonde valores em reais sem centavos e scores com 3 casas decimais."""


def montar():
    tabelas = [{"identifier": t, "column_configs": sorted(cols, key=lambda c: c["column_name"])} for t, cols in sorted(TABELAS.items())]
    joins = sorted([
        {"id": hid(f"join-{k}"), "left": {"identifier": f"{C}.{lt}", "alias": la}, "right": {"identifier": f"{C}.{rt}", "alias": ra},
         "sql": [cond, f"--rt=FROM_RELATIONSHIP_TYPE_{tipo}--"]}
        for k, lt, la, rt, ra, cond, tipo in JOINS], key=lambda x: x["id"])
    exemplos = sorted([{"id": hid(f"ex-{k}"), "question": [q], "sql": [s], "usage_guidance": [u]} for k, q, s, u in EXEMPLOS], key=lambda x: x["id"])
    snippets = {
        "filters": sorted([
            {"id": hid("f-alto"), "display_name": "Lojas em risco ALTO", "sql": ["fila_investigacao_pld.faixa_risco = 'ALTO'"], "synonyms": ["risco alto", "alto risco"]},
            {"id": hid("f-novo"), "display_name": "Novos suspeitos", "sql": ["fila_investigacao_pld.fl_novo_suspeito = 1"], "synonyms": ["não listadas", "fora da lista negativa"]},
            {"id": hid("f-fraude"), "display_name": "Fraude confirmada", "sql": ["fila_investigacao_pld.fl_fraude_confirmada = 1"], "synonyms": ["lista negativa", "fraudadas"]},
        ], key=lambda x: x["id"]),
        "measures": sorted([
            {"id": hid("m-lojas"), "alias": "qt_lojas", "display_name": "Quantidade de lojas", "sql": ["COUNT(DISTINCT fila_investigacao_pld.nu_ec)"], "synonyms": ["número de lojas", "quantas lojas"]},
            {"id": hid("m-pix"), "alias": "vl_pix", "display_name": "Valor de PIX (R$)", "sql": ["SUM(trns_pix.vl_trns)"], "synonyms": ["volume PIX", "valor PIX"]},
        ], key=lambda x: x["id"]),
    }
    return {
        "version": 2,
        "config": {"sample_questions": sorted([{"id": hid(f"q-{q}"), "question": [q]} for q in PERGUNTAS], key=lambda x: x["id"])},
        "data_sources": {"tables": tabelas},
        "instructions": {
            "text_instructions": [{"id": hid("instrucoes"), "content": [INSTRUCOES]}],
            "example_question_sqls": exemplos,
            "join_specs": joins,
            "sql_snippets": snippets,
        },
    }


if __name__ == "__main__":
    destino = Path(__file__).with_name("genie_space.json")
    destino.write_text(json.dumps(montar(), ensure_ascii=False, indent=1))
    print(f"✔ {destino} ({len(INSTRUCOES)} caracteres de instruções, {len(TABELAS)} tabelas, {len(EXEMPLOS)} exemplos SQL)")
