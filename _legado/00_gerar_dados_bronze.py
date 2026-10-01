# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Geração de dados sintéticos — camada Bronze
# MAGIC
# MAGIC Gera dados **100% sintéticos** que seguem exatamente o catálogo de dados:
# MAGIC
# MAGIC | Documento de referência | Tabela original | Tabela bronze |
# MAGIC |---|---|---|
# MAGIC | Catálogo RDC | `prd.re_aura.tbciar_re_hub_dado_risc_cred` | `cielo_pld.bronze.tbciar_re_hub_dado_risc_cred` |
# MAGIC | Lynx §1 (9 variações) | `prd.aura.tbciar_tr_lynx_lsta_*` | `cielo_pld.bronze.tbciar_tr_lynx_lsta_{psit,ngto,psit_atzd}_{crto,clnt,ec}` |
# MAGIC | Lynx §2 | `prd.aura.tbciar_tr_frde` | `cielo_pld.bronze.tbciar_tr_frde` |
# MAGIC | Lynx §3 | `prd.aura.tbciar_tr_lynx_rsps` | `cielo_pld.bronze.tbciar_tr_lynx_rsps` |
# MAGIC | Lynx §4 | `prd.aura.tbciar_tr_lynx_rgra` | `cielo_pld.bronze.tbciar_tr_lynx_rgra` |
# MAGIC | Lynx §5.1 | `prd.aura.tbciar_tr_lynx_trns_crto` | `cielo_pld.bronze.tbciar_tr_lynx_trns_crto` |
# MAGIC | Lynx §5.2 | `prd.aura.tbciar_tr_lynx_trns_pix` | `cielo_pld.bronze.tbciar_tr_lynx_trns_pix` |
# MAGIC | Lynx §5.3 | `prd.aura.tbciar_tr_lynx_trns_antp_rcbv` | `cielo_pld.bronze.tbciar_tr_lynx_trns_antp_rcbv` |
# MAGIC
# MAGIC Nas tabelas Lynx são acrescentados os **campos de controle de carga** citados no documento (data de carga, nome do arquivo de origem e data de partição) como `dt_crga`, `nm_arqv_orgm`, `dt_prtc`.
# MAGIC
# MAGIC ### Tipologias de PLD embutidas nos dados (para a demo)
# MAGIC * **Anéis de ECs** com CNPJs de raízes diferentes, mas que compartilham conta de liquidação, endereço, telefone, dispositivo e IP (operador oculto / "laranjas").
# MAGIC * **Autotransação / cartões de laranjas** comprando nos ECs do anel, com valores fracionados logo abaixo de limites (R$ 5 mil / R$ 10 mil), valores redondos, madrugada, CNP e cartões pré-pagos.
# MAGIC * **PIX em ciclo** (EC A → laranja → EC B → controlador → EC A), *fan-in* de laranjas e *pass-through* para o controlador.
# MAGIC * **Antecipação agressiva (ARV)** com troca de conta de domicílio.
# MAGIC * Sinais cadastrais: afiliação recente, crescimento abrupto de faturamento, alteração societária, sócio PEP, MCC de alto risco.
# MAGIC
# MAGIC Apenas **parte** dos ECs dos anéis está na lista negativa: o objetivo da demo é mostrar que o grafo encontra os demais.
# MAGIC
# MAGIC ### "Sujeira" proposital na Bronze
# MAGIC Documentos com/sem máscara e sem zeros à esquerda, dígitos trocados, nomes com caixa/acentos inconsistentes, UF por extenso, datas em formatos mistos, valores com vírgula decimal (`vl_trns` string no RAD0), arquivos reprocessados (duplicatas), transações de teste e entradas de lista já expiradas.

# COMMAND ----------

# MAGIC %pip install faker==30.8.2 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import numpy as np, pandas as pd, random, hashlib, unicodedata
from faker import Faker
from pyspark.sql import functions as F

SEED = 42
rng = np.random.default_rng(SEED)
random.seed(SEED)
fake = Faker("pt_BR")
Faker.seed(SEED)

DT_INI = pd.Timestamp("2026-08-01")          # janela diária Lynx
N_DIAS = 60                                   # 2026-08-01 .. 2026-09-29
MESES = [202603, 202604, 202605, 202606, 202607, 202608]   # snapshots mensais do HUB
N_EC, N_PESSOAS, N_ANEIS = 4000, 40000, 40
N_CARTAO_NORMAL, N_PIX_NORMAL = 200_000, 100_000

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

# MAGIC %md ## 1. Layouts (colunas e tipos exatamente como no catálogo)

# COMMAND ----------

def S(spec):
    return [tuple(c.split()) for c in spec.replace("\n", " ").split(",") if c.strip()]

def R(fmt, a, b, typ="string"):
    return [(fmt.format(i), typ) for i in range(a, b + 1)]

CTRL_LYNX = S("dt_crga string, nm_arqv_orgm string, dt_prtc bigint")

LAYOUT_LSTA = S("id_lsta bigint, id_objt_prcp bigint, id_objt_adcn string, id_usro bigint, cd_usro string, dt_etra_lsta string, dt_sada_lsta string")
LAYOUT_FRDE = S("dh_trns string, dh_reporte string, cd_tipo_rsps_frde bigint, in_match bigint, cd_tipo_reporte bigint, id_unco_lynx string, id_trns_orgm string")
LAYOUT_RSPS = S("id_unco_lynx string, dh_rsps string, cd_tipo_rsps_frde string, id_usro string, cd_usro string")
LAYOUT_RGRA = S("dt_hr_envio_rgra string, nu_ordm_rgra bigint, id_rgra bigint, nu_vrso_rgra string, cd_tipo_rgra string")

LAYOUT_CRTO = (
    S("""dt_ordm string, qt_cnar_ordm bigint, nu_crto string, nu_crto_tken string, id_estn bigint, dt_oprc string, hr_oprc string,
    in_etnc string, vl_trns double, in_acto string, cd_pais_crto string, cd_pais_estn string, in_cred string, cd_stas_frde string,
    nu_score_totl bigint, nu_score_rede_nerl bigint, nu_score_rgra bigint, cd_estn string, nu_vrso_rgra string, id_rgra string,
    id_entd string, sg_uf string, dt_rsps_alrt string, dt_reporte_frde string, cd_tipo_frde string, tx_prfl_cptn string, id_host bigint,
    id_cau_0002 string, id_rgra_atzc string, tx_acao_rgra_atzc string, id_cau_0005 string, id_unco string, id_rgra_estn bigint,
    nu_score_rgra_estn bigint, vl_tste double, cd_rsps_rgra_atzc string, tx_evnt_poc_extr string, id_cau_0012 string, id_mdlo bigint,
    id_cau_0014 string, id_cau_0015 string, tx_evnt_poc_extr_3 bigint, id_cau_0017 string, vl_perd_espa double, nu_prob_frde bigint,
    id_rgra_estn_onln bigint, nu_score_rgra_estn_onln bigint, id_rgra_estn_ofln string, nu_score_rgra_ec_onln bigint, cd_bndr string,
    in_cred_dbto string, nu_score_rede_nerl_estc bigint, vl_perd_espa_estc double, nu_prob_frde_estc bigint, nu_score_rede_nerl_dnmc bigint,
    vl_perd_espa_dnmc double, nu_prob_frde_dnmc bigint, tx_mdlo_vncd bigint, nu_score_nerl_locl_ofln bigint, nu_score_totl_locl_ofln bigint,
    in_tipo_mdlo_locl_ofln string, id_cau_0036 string, id_cau_0037 string, id_rgra_pld string, nu_score_rgra_pld bigint, id_cau_0040 string,
    id_cau_0041 string, in_blqo_crto string, in_blqo_estn string, cd_tipo_crto string, cd_rsps string, cd_setr_atvd string""")
    + R("id_cau_cnl_{:04d}", 4, 6)
    + S("""cd_modo_etra_dado string, cd_orgm_atzc string, tx_pos string, tx_cpcd_trmn bigint, id_cau_cnl_0011 string, in_parc string,
    tx_tid string, cd_vl_fixo string, cd_vl_fixo_0002 string, cd_vl_fixo_otro string, id_cau_cnl_0017 string, cd_vl_fixo_exta string,
    cd_srvc string, cd_srvc_otro string, cd_srvc_exta string, in_fallback string, tx_cam string, tx_eci_verifiedbyvisa string, tx_cdc string,
    in_anlc string, tx_quem_respondeu string, cd_vl_fixo_e string, cd_sgmt string, nm_estn string, nu_prcl string, in_snha string,
    in_cvv2 string, tx_tvr string, nu_trmn string, tx_meio_pgmn bigint, cd_tipo_pgmn string, in_rcre string, id_rgra_host string,
    cd_cnfm string, cd_tipo_trmn string, cd_checkout string, tx_xls string, id_cau_cnl_0044 string, id_cau_cnl_0045 string,
    in_cvv2_vald string, cd_stas_lynx_onln string, tx_usro_lynx_onln string, vl_trco double, nu_pnpd string, vl_agrodebito double,
    cd_atzc string, nu_nsu string, cd_stas_cncl string, vl_ftrm_acmo_cred double, vl_ftrm_acmo_dbto double, cd_tipo_cnal string,
    nu_ddd string, nu_celr string, in_atzc_pari string, tx_usro_cncl string, tx_ec_opra string, cd_tipo_crto_bndr string, cd_bin6_crto string""")
    + R("id_cau_cnl_{:04d}", 65, 91)
    + S("tx_cter_dgtl string, id_rede string, cd_tipo_slct_ecommerce_cdst string, tk_crpg string, nu_atc string")
    + R("id_cau_cnl_{:04d}", 97, 113)
    + S("tx_endr_ip string, cd_tipo_cncl string, cd_tipo_cnal_cncl string, in_pnpd_lio string, in_rsps string, in_qr_code string, in_ewallet string, dt_cptr string")
    + R("id_cau_{:04d}", 122, 125)
    + S("""nm_ptdr_crto_chip string, nm_ctgr_ecommerce string, nm_ptdr_crto_ecommerce string, nu_cpf_ptdr_ecommerce string,
    tx_endr_ptdr_ecommerce string, nu_cep_ptdr_ecommerce string, nu_celr_ptdr_ecommerce string, dt_nscm_ptdr_ecommerce string,
    tx_emal_ptdr_ecommerce string, tx_endr_enta_ecommerce string, tx_prdt_ecommerce string, tx_foma_pgmn_ecommerce string,
    nu_prcl_ecommerce string, tx_subestabelecimento_mcc bigint, tx_subestabelecimento_endr string, tx_subestabelecimento_cide string,
    sg_subestabelecimento_estd string, nu_subestabelecimento_cep string, nu_subestabelecimento_tlfn string, tx_trnp_tipo_mdlo string,
    tx_trnp_rego_mdlo string, cd_tipo_slct_ecommerce_trns string, nu_cnpj_subestabelecimento string, in_aplc_ecommerce string,
    id_cau_0150 string, tk_mac_mcrd string, tx_ip_brsp string, vl_lmte double, vl_usdo_lmte double, vl_sald_lmte double, dh_lmte string,
    id_crto_vrtl string, dc_soft_descriptor string, nu_par_number string, id_moda string, tk_cncl string, in_clnt_etng string,
    cd_oprc_mdld string, tk_oprc_crto_tokenizado string, nu_oprc_cpf_ptdr_pld string, tx_oprc_nm_ptdr_pld string, tx_oprc_prcr string,
    tx_oprc_app string, tx_oprc_stma_oprl string, nu_oprc_vrso_sistemaoperacional string, tx_oprc_tmpo_trns string, nu_oprc_indi_ngto_anfr string""")
)

_TAGS = ["ideologicalfalsehoodtags_frde", "stooge_acc_mrca_frde", "frde_acc_mrca_frde", "othertags_frde",
         "frde_unknowntags_frde", "totalspi_frde_mrca_frde", "distinctpsp_frde_mrca_frde"]
_P3 = ["d90", "m12", "m60"]

def _escopo(sfx):
    spi = "dh_dthrulteventspius" if sfx == "us" else "dh_dth_rulteventspiks"
    spi_qt = "trns_spi_us" if sfx == "us" else "trns_spiks"
    cols = [(spi, "string")] + [(f"qt_{p}_{spi_qt}", "bigint") for p in _P3]
    cols += [(f"dh_dth_rulteventtags_frde_{sfx}", "string")]
    cols += [(f"qt_{p}_{t}_{sfx}", "bigint") for t in _TAGS for p in _P3]
    cols += [(f"dh_dth_rulteventinfractions_notif_{sfx}", "string"),
             (f"tx_open_notif_infractions_notif_{sfx}", "string"),
             (f"tx_openpsp_notif_infractions_notif_{sfx}", "string")]
    cols += [(f"qt_{p}_rejected_notif_infractions_notif_{sfx}", "bigint") for p in _P3]
    cols += [(f"dh_dth_rultevent_cnta_{sfx}", "string")]
    if sfx == "us":
        cols += [("qt_registered_cnta_cnta_us", "bigint")]
    else:
        cols += [(f"qt_{p}_distinct_cnta_cnta_ks", "bigint") for p in _P3]
    return cols

LAYOUT_PIX = (
    S("""dt_ordm string, nu_crto string, nu_crto_id string, id_estn bigint, dt_oprc bigint, hr_oprc bigint, id_eltn string, vl_trns double,
    in_acto string, id_pais string, cd_pais_estn string, cd_cred string, cd_stts string, nu_score_totl bigint, nu_score_rede_nerl bigint,
    nu_score_rgra bigint, cd_estn string, nu_vrso_rgra string, id_rgra string, id_entd string, id_uf string, dt_rsps string, dr_rprt string,
    id_tipo_frde string, tx_prfl_cptn string, nu_cpf_cnpj_recr string, nu_vrso_lyot string, id_0001_cau string, id_0002_cau string,
    id_rgra_atzc string, tx_acao_rgra_atzc bigint""")
    + R("id_{:04d}_cau", 5, 39)
    + S("id_cnta string, cd_tipo_pgmn string")
    + R("id_{:04d}_cau", 42, 70)
    + S("""cd_tipo_chve_pix string, nm_razo_socl_pix string, dt_crca_cnta string, hr_crca_cnta string, nm_fnts_detn string,
    dt_crca_chve string, hr_crca_chve string, dt_pose_chve string, hr_pose_chve string, dt_inco_claim string, hr_inco_claim string,
    dt_ulto_anfr string, hr_ulto_anfr string, qt_trns_3d bigint, qt_trns_30d bigint, qt_trns_6m bigint, qt_frde_rpto_3d bigint,
    qt_frde_rpto_30d bigint, qt_frde_rpto_6m bigint, qt_frde_cfmd_3d bigint, qt_frde_cfmd_30d bigint, qt_frde_cfmd_6m bigint,
    id_dspi string, tx_geolocalizacao_dspi string, tx_fatr_autc bigint, tx_usro_pix string, tx_atlc_chve bigint, tx_claim_chve bigint,
    nu_atlc_lmte string, nu_cnta_detn string, cd_tipo_oprc_pix string, qt_cd_trns_pix bigint, nm_estn_pix string, nu_dcmt_estn string,
    cd_tipo_pesa string, qt_stas_trns_pix bigint, cd_tipo_trne string, tx_chve_pix_pgdr string, id_unco_pix string, nu_ispb_recr string,
    nu_cnta_recr string, nu_agnc_recr string, cd_tipo_cnta_recr string, cd_tipo_bnfr_recr string, nu_cpf_cnpj string, nm_recr string,
    tx_chve_pix_recr string, vl_cpra double, tx_rsps_pgdr string, id_tx_pix string, nu_ispb_agnt_saqe string, cd_modo_agnt string,
    vl_espe double, cd_tipo_pix string, cd_erro_pix string, cd_rsps string""")
    + _escopo("us") + _escopo("ks")
    + S("""tx_pix_tipo_saqe bigint, nu_pix_ispb_pgdr string, nu_pix_cnta_pgdr string, nu_pix_agnc_pgdr string, nu_pix_tipo_cnta_pgdr string,
    tx_pix_tipo_bnfr_pgdr string, nu_pix_cpf_cnpj_pgdr string, tx_pix_nm_pgdr string, cd_pix_mdld_estn string, tx_pix_flag_dvlc bigint,
    tx_pix_mrca_frde bigint, tx_pix_resc_trns bigint""")
)

LAYOUT_ANTP = (
    S("""id_unco_lynx string, dt_antp bigint, hr_antp string, id_estn string, cd_pais_estn bigint, cd_estn string, vl_trns string,
    in_acto string, cd_cred double, cd_stts string, nu_score_totl bigint, nu_score_rede_nerl bigint, nu_score_rgra bigint,
    nu_vrso_rgra bigint, id_rgra string, cd_tipo_objt_qualificado string, dr_rsps double, dt_rprt string, id_tipo_frde string,
    id_rgra_atzc string, tx_acao_rgra_atzc bigint, id_lote bigint, nu_cnpj_rspe_dmcl bigint, cd_tipo_pesa string, nu_cnpj_cpf_mqnt string,
    cd_tipo_cnta string, nu_agnc string, nu_cnta string, cd_bnco string, nu_ispb string, nu_cnta_pgmn string, tx_arnj_pgmn string,
    tx_asoc_crto string, qt_envo_bnco string, id_unco string, cd_rsps string, nu_dgto_agnc_cnta string, nu_dgto_cnta_pgmn string,
    nu_vrso_lyot string, dh_etra_webservice string, id_bhvr string, cd_bnco_exta string, id_crto string, id_crto_exta string""")
    + [(f"id_{i:04d}_cau", "string") for i in
       [7, 8, 10, 13, 16, 18, 19, 20, 21, 22, 23, 24, 25, 33, 36, 37, 40, 1, 2, 5, 6, 9, 11, 12, 14, 15, 17,
        26, 27, 28, 29, 30, 31, 32, 34, 35, 38, 39] + list(range(41, 70))]
)

LAYOUT_HUB = S("""CD_MES bigint, NU_EC bigint, NM_EC string, NM_CLNT string, NU_CDFR bigint, NM_CDFR string, IN_EC_MTRZ int, NU_DCMT string,
NU_DCMT_RAIZ string, NM_GRPO_ECNC string, TIPO_CLNT string, DC_TIPO_EMPA string, DC_SUB_ADQT string,
NM_MTRZ_SGMT string, NM_MTRZ_SUB_SGMT string, CD_SGMT_CMRL bigint, NM_SGTO_CMRL string,
NM_UF string, NM_MNCP string, DC_ENDR string, CD_RMAT int, NM_RMAT string,
DT_AFLC timestamp, QT_DIAS_AFLC int, CD_STCO_ATVD int, DC_STCO_ATVD string, IN_ATIV_CDST int, IN_TIPO_ATVO int, IN_SADA_CLNT int,
DC_CNAL_AFLC string, DT_PRIM_TRNS date, DT_ULTM_TRNS date, QT_DIAS_INTV bigint, DC_SITU_ATVD string, DC_SITU_ATVD_ULTM_MES string,
DC_STAS_CLIE string, DC_STAS_CLIE_ULTM_MES string,
CD_BNCO_AGNC int, CD_AGNC int, NM_BNCO string, NU_CNCR int,
VL_FTRM double, VL_FTRM_FSCO double, VL_FTRM_ONLN double, VL_FTRM_DBTO double, VL_FTRM_CRED double, VL_FTRM_PARC double,
VL_FTRM_CP double, VL_FTRM_CNP double, VL_FTRM_CNP_LINK double, VL_FTRM_CNP_ECOM double, PC_FTRM_CP double, PC_FTRM_CNP double,
QT_TRNS_TOTL int, QT_TRNS_FSCO int, QT_TRNS_ONLN int, QT_TRNS_DBTO int, QT_TRNS_CRED int, QT_TRNS_PARC int, QT_TRNS_CHRG int,
QT_TRNS_CNCL int, QT_TRNS_CP bigint, QT_TRNS_CNP bigint,
VL_RCTA_BRTA double, VL_ITCM_RMAT double, VL_TCKT_MEDO double, VL_MRGM_CRBC_AJSD double,
VL_ANTP_RBRP double, VL_ANTP_ARV double, VL_RCTA_ANTP_ARV double, VL_RCTA_ANTP_RBRP double, FL_VLME_ANTP_ARV string,
FL_RCTA_ANTP string, FL_VLOR_ANTE_FNIL bigint,
VL_BRTO_CHRG double, VL_LQDO_CHRG double, PC_CHRG double, PC_CHRG_HIST double, VL_TMPO_MEDO_LQDC_CHRG double,
VL_HSTR_CHRG_SETR double, VL_LQDO_CHRG_FRDE double, VL_LQDO_CHRG_ATZC double, VL_LQDO_CHRG_ERRO_PRSM double, VL_LQDO_CHRG_DSCD_CMRL double,
VL_BRTO_CNCL double, VL_LQDO_CNCL double, PC_CANC double, PC_CANC_HIST double, VL_TMPO_MEDO_LQDC_CANC double, VL_LQDO_CHRG_CNCL_TOTL double,
VL_CBRN double, TIPO_CBRN string, VL_DBTO_PNDT_TOTL double, VL_DBTO_PNDT_ALGL double, VL_DBTO_PNDT_AJST double,
VL_DBTO_PNDT_CANC double, VL_DBTO_PNDT_CHRG double, DT_DBTO_PNDT date,
VL_AGND_LVRE_MRCD double, VL_AGND_LVRE_CILO double, VL_AGND_LVRE_VRSU_TOTL double, VL_AGND_NEGC_MRCD double, VL_AGND_NEGC_CILO double,
VL_SALD_PARA_RCBM_RBRP_TCD0 double, VL_SALD_PARA_RCBM_ARV double, VL_SALD_PARA_RCBM_TOTL double, VL_SALD_PARA_RCBM_30D double,
VL_SALD_PARA_RCBM_60D double, VL_SALD_PARA_RCBM_90D double, VL_SALD_PARA_RCBM_MAIR_90D double, PC_CNCN_RBRP_TCD0 double,
PC_CNCN_ARV double, PC_CNCN_TOTL double,
QT_PRZO_DFRT_DIA int, QT_PRZO_DFRT_MES int, IN_CLNT_DFRT int, VL_DFRT double, VL_EXPS_DFRT double, QT_DIAS_DFRT bigint, FL_CLNT_DRFT bigint,
VL_INDM_30 double, VL_INDM_31_60 double, VL_INDM_61_90 double, VL_INDM_90 double, PC_INDM_30 double, PC_INDM_31_60 double,
PC_INDM_61_90 double, PC_INDM_90 double, FL_INDM_90 string, FL_EXPS_RISC_CRED string,
PC_RSRV double, DC_MTDO_RSRV string, VL_RSRV double, VL_LMTE_CRED double, DC_GRAU_ALVG string,
IN_ELGB_PRDT_TCD0 int, IN_ELGB_PRDT_TCD1 int, IN_ELGB_PRDT_ARV int, IN_ELGB_PRDT_RBRP int, FL_ACTE_OFRT_TCD0_FNIL bigint,
FL_ACTE_OFRT_TCD1_FNIL bigint, FL_ELGB_TCD0_FNIL bigint, FL_ELGB_TCD1_FNIL bigint, FL_PROD_HBIL_TCD0_FNIL bigint, FL_PROD_HBIL_TCD1_FNIL bigint,
IN_ALTR_QDRO_SCTR int, DT_ALTR_QDRO_SCTR date, IN_SOCO_PEP int, DC_PNDT_TRBR_EMPA string, DC_STCO_SOCO_RCTA string,
IN_MRTO_EMPA_SETR int, DC_RCMN_OPRL string,
DC_ALRT_CPTO string, DC_ALRT_CPTO_FTRM string, NM_VRVL_ANML string, DC_MDNC_STAS_CPTO string,
DT_CRGA date, DT_PRTC_CD_MES bigint""")

print({k: len(v) for k, v in dict(HUB=LAYOUT_HUB, CRTO=LAYOUT_CRTO, PIX=LAYOUT_PIX, ANTP=LAYOUT_ANTP).items()})

# COMMAND ----------

# MAGIC %md ## 2. Funções utilitárias (documentos válidos, formatação e "sujeira")

# COMMAND ----------

def _dv(digits, pesos):
    r = (digits * pesos).sum(1) % 11
    return np.where(r < 2, 0, 11 - r)

def gerar_cpfs(n):
    d = rng.integers(0, 10, size=(n, 9))
    d = np.column_stack([d, _dv(d, np.arange(10, 1, -1))])
    d = np.column_stack([d, _dv(d, np.arange(11, 1, -1))])
    return np.array(["".join(map(str, x)) for x in d])

def gerar_cnpjs(raizes8, filiais):
    base = np.array([list(r) + list(f"{f:04d}") for r, f in zip(raizes8, filiais)], dtype=int)
    base = np.column_stack([base, _dv(base, np.array([5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]))])
    base = np.column_stack([base, _dv(base, np.array([6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]))])
    return np.array(["".join(map(str, x)) for x in base])

def mascara(doc):
    if len(doc) == 11:
        return f"{doc[:3]}.{doc[3:6]}.{doc[6:9]}-{doc[9:]}"
    return f"{doc[:2]}.{doc[2:5]}.{doc[5:8]}/{doc[8:12]}-{doc[12:]}"

def sujar_doc(docs, p_mask=0.2, p_zero=0.05, p_typo=0.0):
    out = []
    for d in docs:
        if d is None:
            out.append(None); continue
        r = random.random()
        if r < p_typo:                      # dígito trocado (será resolvido por nome na Silver)
            i = random.randrange(len(d)); d = d[:i] + str((int(d[i]) + random.randint(1, 9)) % 10) + d[i + 1:]
        elif r < p_typo + p_mask:
            d = mascara(d)
        elif r < p_typo + p_mask + p_zero:
            d = d.lstrip("0")
        out.append(d)
    return out

def sem_acento(s):
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()

def sujar_nome(nomes, p=0.25):
    out = []
    for n in nomes:
        r = random.random()
        if n is None or r >= p: out.append(n)
        elif r < p * 0.35: out.append(n.lower())
        elif r < p * 0.7: out.append(sem_acento(n).upper())
        else: out.append("  " + n.title() + " ")
    return out

def hexid(n, k=32):
    return np.array([hashlib.md5(f"{SEED}-{k}-{i}-{random.random()}".encode()).hexdigest()[:k] for i in range(n)])

def escolhe(opcoes, n, p=None):
    return rng.choice(np.array(opcoes, dtype=object), size=n, p=p)

def timestamps(n, madrugada=0.0):
    dias = rng.integers(0, N_DIAS, n)
    p_h = np.array([1, .5, .3, .3, .3, .5, 1, 2, 4, 6, 7, 7, 8, 7, 7, 7, 7, 7, 7, 6, 5, 4, 3, 2], float); p_h /= p_h.sum()
    horas = rng.choice(24, n, p=p_h)
    noite = rng.random(n) < madrugada
    horas = np.where(noite, rng.integers(0, 5, n), horas)
    seg = rng.integers(0, 3600, n)
    return DT_INI + pd.to_timedelta(dias, "D") + pd.to_timedelta(horas * 3600 + seg, "s")

def to_bronze(pdf, layout, nome, ctrl=True, particao=None, comentario=""):
    """Converte pandas -> Spark e força exatamente o layout (ordem/tipos); colunas não geradas ficam nulas."""
    sdf = spark.createDataFrame(pdf)
    cols = layout + (CTRL_LYNX if ctrl else [])
    exprs = [(F.col(c).cast(t) if c in sdf.columns else F.lit(None).cast(t)).alias(c) for c, t in cols]
    out = sdf.select(*exprs)
    w = out.write.mode("overwrite").option("overwriteSchema", "true")
    if particao:
        w = w.partitionBy(particao)
    w.saveAsTable(f"{BRONZE}.{nome}")
    if comentario:
        spark.sql(f"COMMENT ON TABLE {BRONZE}.{nome} IS '{comentario}'")
    n = spark.table(f"{BRONZE}.{nome}").count()
    print(f"{nome}: {n:,} linhas, {len(cols)} colunas")

def ctrl_cols(pdf, ts, prefixo, p_dup=0.0):
    """Campos de controle de carga Lynx + duplicatas de arquivos reprocessados."""
    d = pd.to_datetime(ts)
    pdf["dt_prtc"] = d.strftime("%Y%m%d").astype(int).values
    pdf["dt_crga"] = (d + pd.Timedelta(days=1)).strftime("%Y-%m-%d").values
    pdf["nm_arqv_orgm"] = [f"LYNX_{prefixo}_{x}.csv.gz" for x in d.strftime("%Y%m%d")]
    if p_dup > 0:
        dup = pdf.sample(frac=p_dup, random_state=SEED).copy()
        dup["dt_crga"] = (pd.to_datetime(dup["dt_crga"]) + pd.Timedelta(days=2)).dt.strftime("%Y-%m-%d")
        dup["nm_arqv_orgm"] = dup["nm_arqv_orgm"].str.replace(".csv.gz", "_REPROC.csv.gz", regex=False)
        pdf = pd.concat([pdf, dup], ignore_index=True)
    return pdf

# COMMAND ----------

# MAGIC %md ## 3. Universo de entidades: pessoas, cartões, ECs e anéis

# COMMAND ----------

UFS = [("SP", "São Paulo", "11", .30), ("RJ", "Rio de Janeiro", "21", .12), ("MG", "Belo Horizonte", "31", .10),
       ("PR", "Curitiba", "41", .06), ("RS", "Porto Alegre", "51", .06), ("BA", "Salvador", "71", .06),
       ("PE", "Recife", "81", .05), ("CE", "Fortaleza", "85", .05), ("DF", "Brasília", "61", .05),
       ("GO", "Goiânia", "62", .04), ("SC", "Florianópolis", "48", .04), ("AM", "Manaus", "92", .03),
       ("PA", "Belém", "91", .04)]
UF_NOME = {"SP": "SAO PAULO", "RJ": "RIO DE JANEIRO", "MG": "MINAS GERAIS", "PR": "PARANA", "RS": "RIO GRANDE DO SUL",
           "BA": "BAHIA", "PE": "PERNAMBUCO", "CE": "CEARA", "DF": "DISTRITO FEDERAL", "GO": "GOIAS",
           "SC": "SANTA CATARINA", "AM": "AMAZONAS", "PA": "PARA"}
p_uf = np.array([u[3] for u in UFS]); p_uf /= p_uf.sum()

# (CD_RMAT, NM_RMAT, MCC, ticket médio, alto risco PLD)
RAMOS = [(1, "SUPERMERCADOS", 5411, 120, 0), (2, "RESTAURANTES", 5812, 85, 0), (3, "VAREJO DIVERSO", 5999, 150, 0),
         (4, "FARMACIAS", 5912, 90, 0), (5, "VESTUARIO", 5651, 180, 0), (6, "ELETRONICOS", 5732, 900, 0),
         (7, "HOTEIS", 7011, 650, 0), (8, "SERVICOS MEDICOS", 8011, 350, 0), (9, "TRANSPORTE", 4121, 45, 0),
         (10, "SERVICOS PESSOAIS", 7299, 110, 0), (11, "JOALHERIAS", 5944, 2500, 1), (12, "REVENDA DE VEICULOS", 5521, 9000, 1),
         (13, "JOGOS E APOSTAS", 7995, 400, 1), (14, "CAMBIO E CRIPTOATIVOS", 6051, 3000, 1), (15, "ENTIDADES E ONGS", 8398, 250, 1)]
p_ramo_normal = np.array([14, 14, 12, 8, 9, 6, 4, 6, 7, 8, 2, 2, 3, 1, 4], float); p_ramo_normal /= p_ramo_normal.sum()
p_ramo_anel = np.array([2, 3, 6, 1, 4, 8, 2, 2, 2, 6, 14, 12, 18, 14, 6], float); p_ramo_anel /= p_ramo_anel.sum()

BANCOS = [(1, "BANCO DO BRASIL", "00000000"), (237, "BRADESCO", "60746948"), (341, "ITAU UNIBANCO", "60701190"),
          (104, "CAIXA ECONOMICA FEDERAL", "00360305"), (33, "SANTANDER", "90400888"), (260, "NU PAGAMENTOS", "18236120"),
          (77, "BANCO INTER", "00416968"), (336, "C6 BANK", "31872495"), (290, "PAGSEGURO", "08561701"), (380, "PICPAY", "22896431")]
p_banco_normal = np.array([16, 18, 20, 12, 14, 7, 5, 3, 3, 2], float); p_banco_normal /= p_banco_normal.sum()
p_banco_anel = np.array([2, 3, 3, 2, 2, 20, 18, 18, 16, 16], float); p_banco_anel /= p_banco_anel.sum()

# ---------------- Pessoas (portadores, pagadores PIX, sócios, laranjas) ----------------
pes = pd.DataFrame({"cpf": gerar_cpfs(N_PESSOAS)}).drop_duplicates("cpf").reset_index(drop=True)
N_PESSOAS = len(pes)
pes["nome"] = [fake.name().upper() for _ in range(N_PESSOAS)]
uf_idx = rng.choice(len(UFS), N_PESSOAS, p=p_uf)
pes["uf"] = [UFS[i][0] for i in uf_idx]; pes["cidade"] = [UFS[i][1] for i in uf_idx]; pes["ddd"] = [UFS[i][2] for i in uf_idx]
pes["celular"] = [f"9{rng.integers(10**7, 10**8)}" for _ in range(N_PESSOAS)]
_dom = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br", "uol.com.br", "icloud.com"]
pes["email"] = [f"{sem_acento(n.split()[0]).lower()}.{sem_acento(n.split()[-1]).lower()}{i}@{random.choice(_dom)}" for i, n in enumerate(pes.nome)]   # único por pessoa
pes["dt_nasc"] = pd.to_datetime(rng.integers(pd.Timestamp("1955-01-01").value // 10**9, pd.Timestamp("2006-01-01").value // 10**9, N_PESSOAS), unit="s").strftime("%Y-%m-%d")
pes["endereco"] = [fake.street_address().upper() for _ in range(N_PESSOAS)]
pes["cep"] = [f"{rng.integers(10**7, 10**8 - 1):08d}" for _ in range(N_PESSOAS)]
pes["banco"] = rng.choice(len(BANCOS), N_PESSOAS, p=p_banco_normal)
pes["agencia"] = rng.integers(1, 9999, N_PESSOAS); pes["conta"] = rng.integers(10000, 99999999, N_PESSOAS)
pes["papel"] = "COMUM"; pes["anel"] = -1

# ---------------- Cartões ----------------
BANDEIRAS = [("VISA", "4", .42), ("MASTERCARD", "5", .38), ("ELO", "636368", .14), ("AMEX", "37", .03), ("HIPERCARD", "606282", .03)]
n_crt = 1 + (rng.random(N_PESSOAS) < 0.35).astype(int)
crt = pd.DataFrame({"pessoa": np.repeat(np.arange(N_PESSOAS), n_crt)})
b_idx = rng.choice(len(BANDEIRAS), len(crt), p=[b[2] for b in BANDEIRAS])
crt["bandeira"] = [BANDEIRAS[i][0] for i in b_idx]
crt["bin6"] = [(BANDEIRAS[i][1] + "".join(map(str, rng.integers(0, 10, 6))))[:6] for i in b_idx]
crt["final4"] = [f"{x:04d}" for x in rng.integers(0, 10000, len(crt))]
crt["token"] = [f"9{x:015d}" for x in rng.choice(10**15, len(crt), replace=False)]
crt["tipo"] = escolhe(["CREDITO", "DEBITO", "PRE-PAGO"], len(crt), [.62, .33, .05])
crt["pan"] = crt.bin6 + "******" + crt.final4
cartoes_por_pessoa = crt.groupby("pessoa").indices

# ---------------- ECs ----------------
n_pj = int(N_EC * 0.78)
tamanhos = []                                   # filiais por raiz CNPJ
while sum(tamanhos) < n_pj:
    tamanhos.append(1 if rng.random() < 0.85 else int(rng.integers(2, 7)))
tamanhos[-1] -= sum(tamanhos) - n_pj
raizes = [f"{x:08d}" for x in rng.choice(10**8, len(tamanhos), replace=False)]
ec_raiz = np.repeat(raizes, tamanhos)
ec_filial = np.concatenate([np.arange(1, t + 1) for t in tamanhos])
docs_pj = gerar_cnpjs(ec_raiz, ec_filial)
docs_pf = gerar_cpfs(N_EC - n_pj)

ec = pd.DataFrame({
    "nu_ec": rng.choice(np.arange(1_000_000_000, 2_999_999_999), N_EC, replace=False),
    "doc": np.concatenate([docs_pj, docs_pf]),
    "tp_pessoa": ["PJ"] * n_pj + ["PF"] * (N_EC - n_pj),
    "raiz": list(ec_raiz) + [None] * (N_EC - n_pj),
    "filial": list(ec_filial) + [None] * (N_EC - n_pj),
})
grp_size = pd.Series(ec.raiz).map(pd.Series(ec.raiz).value_counts())
ec["multi"] = grp_size.fillna(1).values > 1
ec["anel"] = -1

# anéis: somente ECs de raiz única (o elo entre eles é oculto, não societário)
elegiveis = ec.index[~ec.multi].to_numpy()
rng.shuffle(elegiveis)
pos, aneis = 0, []
for a in range(N_ANEIS):
    k = int(rng.integers(4, 11))
    membros = elegiveis[pos:pos + k]; pos += k
    ec.loc[membros, "anel"] = a
    aneis.append(membros)
em_anel = ec.anel >= 0

ramo_idx = np.where(em_anel, rng.choice(len(RAMOS), N_EC, p=p_ramo_anel), rng.choice(len(RAMOS), N_EC, p=p_ramo_normal))
ec["cd_rmat"] = [RAMOS[i][0] for i in ramo_idx]; ec["nm_rmat"] = [RAMOS[i][1] for i in ramo_idx]
ec["mcc"] = [RAMOS[i][2] for i in ramo_idx]; ec["ticket"] = [RAMOS[i][3] for i in ramo_idx]
ec["mcc_risco"] = [RAMOS[i][4] for i in ramo_idx]
uf_idx = rng.choice(len(UFS), N_EC, p=p_uf)
ec["uf"] = [UFS[i][0] for i in uf_idx]; ec["municipio"] = [UFS[i][1] for i in uf_idx]; ec["ddd"] = [UFS[i][2] for i in uf_idx]

# nomes: filiais do mesmo grupo compartilham razão social
razao_por_raiz = {r: fake.company().upper() for r in set(raizes)}
ec["nm_clnt"] = [razao_por_raiz[r] if r else None for r in ec.raiz]
pf_mask = ec.tp_pessoa == "PF"
ec.loc[pf_mask, "nm_clnt"] = [fake.name().upper() for _ in range(pf_mask.sum())]
ec["nm_ec"] = [f"{fake.last_name().upper()} {nm.split()[0]}" for nm in ec.nm_rmat]
ec["tipo_clnt"] = np.where(em_anel, escolhe(["E-COMMERCE", "BALCAO", "SUB"], N_EC, [.55, .35, .10]),
                           escolhe(["E-COMMERCE", "BALCAO", "SUB"], N_EC, [.18, .78, .04]))
ec["tipo_empa"] = np.where(pf_mask, "MEI", escolhe(["LTDA", "EIRELI", "S/A", "SLU", "EPP"], N_EC, [.55, .08, .07, .2, .1]))
ec["porte"] = np.where(em_anel, rng.lognormal(11.3, 0.7, N_EC), rng.lognormal(10.6, 1.2, N_EC))   # faturamento mensal R$
dias_aflc = np.where(em_anel, rng.integers(40, 300, N_EC), rng.integers(60, 5000, N_EC))
ec["dt_aflc"] = (pd.Timestamp("2026-09-30") - pd.to_timedelta(dias_aflc, "D")).normalize()
ec["endereco"] = [fake.street_address().upper() for _ in range(N_EC)]
ec["cep"] = [f"{x:08d}" for x in rng.integers(10**7, 10**8 - 1, N_EC)]
ec["telefone"] = [f"{d}{rng.integers(3000_0000, 3999_9999)}" for d in ec.ddd]
ec["email"] = [f"contato@{sem_acento(n.split()[0]).lower()}{i:04d}.com.br" for i, n in enumerate(ec.nm_ec)]   # único por EC
b_idx = np.where(em_anel, rng.choice(len(BANCOS), N_EC, p=p_banco_anel), rng.choice(len(BANCOS), N_EC, p=p_banco_normal))
ec["banco"] = b_idx; ec["agencia"] = rng.integers(1, 9999, N_EC); ec["conta"] = rng.integers(10000, 99999999, N_EC)
ec["ip"] = [fake.ipv4_public() for _ in range(N_EC)]
ec["dispositivo"] = hexid(N_EC, 24)
ec["pep"] = np.where(em_anel, rng.random(N_EC) < 0.18, rng.random(N_EC) < 0.01).astype(int)
ec["alt_societaria"] = np.where(em_anel, rng.random(N_EC) < 0.55, rng.random(N_EC) < 0.04).astype(int)
ec["uso_arv"] = np.where(em_anel, rng.random(N_EC) < 0.92, rng.random(N_EC) < 0.25)

# filiais do mesmo grupo: mesma conta de liquidação (compartilhamento legítimo)
for r, idx in ec[ec.multi].groupby("raiz").groups.items():
    i0 = idx[0]
    ec.loc[idx, ["banco", "agencia", "conta"]] = ec.loc[i0, ["banco", "agencia", "conta"]].values

# ---------------- Anéis: controladores, laranjas e atributos compartilhados ----------------
livres = rng.permutation(N_PESSOAS)
ptr = 0
anel_info = []
for a, membros in enumerate(aneis):
    n_ctrl, n_lar = int(rng.integers(1, 3)), int(rng.integers(6, 16))
    ctrl = livres[ptr:ptr + n_ctrl]; ptr += n_ctrl
    lar = livres[ptr:ptr + n_lar]; ptr += n_lar
    pes.loc[ctrl, "papel"] = "CONTROLADOR"; pes.loc[lar, "papel"] = "LARANJA"
    pes.loc[np.concatenate([ctrl, lar]), "anel"] = a
    # contas digitais compartilhadas
    contas = [(int(rng.choice(len(BANCOS), p=p_banco_anel)), int(rng.integers(1, 9999)), int(rng.integers(10000, 99999999))) for _ in range(2)]
    ips = [fake.ipv4_public() for _ in range(3)]
    dsp = list(hexid(2, 24))
    end_anel, tel_anel = fake.street_address().upper(), f"{ec.loc[membros[0], 'ddd']}9{rng.integers(10**7, 10**8)}"
    for m in membros:
        if rng.random() < 0.75: ec.loc[m, ["banco", "agencia", "conta"]] = contas[int(rng.integers(0, 2))]
        if rng.random() < 0.5: ec.loc[m, "endereco"] = end_anel
        if rng.random() < 0.45: ec.loc[m, "telefone"] = tel_anel
        if rng.random() < 0.7: ec.loc[m, "ip"] = random.choice(ips)
        if rng.random() < 0.7: ec.loc[m, "dispositivo"] = random.choice(dsp)
    # laranjas compartilham e-mail/telefone/endereço de entrega
    for l in lar:
        if rng.random() < 0.4: pes.loc[l, "celular"] = pes.loc[ctrl[0], "celular"]
        if rng.random() < 0.3: pes.loc[l, ["banco", "agencia", "conta"]] = contas[0]
    # laranjas usam cartões pré-pagos
    for l in lar:
        for ci in cartoes_por_pessoa.get(l, []):
            if rng.random() < 0.5: crt.loc[ci, "tipo"] = "PRE-PAGO"
    anel_info.append(dict(membros=membros, ctrl=ctrl, lar=lar, contas=contas, ips=ips, dsp=dsp, end=end_anel))

print(f"ECs: {N_EC:,} | em anéis: {em_anel.sum()} | pessoas: {N_PESSOAS:,} | cartões: {len(crt):,}")

# COMMAND ----------

# MAGIC %md ## 4. HUB de risco de crédito mensal — `tbciar_re_hub_dado_risc_cred`

# COMMAND ----------

hub = ec.assign(key=1).merge(pd.DataFrame({"CD_MES": MESES, "k_mes": range(len(MESES)), "key": 1}), on="key").drop(columns="key")
hub["mes_ini"] = pd.to_datetime(hub.CD_MES.astype(str) + "01")
hub = hub[hub.dt_aflc < hub.mes_ini + pd.offsets.MonthEnd(0)]            # só após afiliação
hub = hub[rng.random(len(hub)) > 0.02].reset_index(drop=True)             # ECs ausentes em alguns meses
n = len(hub); anel_h = (hub.anel >= 0).values
u = lambda a, b: rng.uniform(a, b, n)

meses_desde = ((hub.mes_ini - hub.dt_aflc).dt.days / 30).clip(lower=0).values
cresc = np.where(anel_h, 1 + 0.9 * np.minimum(meses_desde, 6), 1.0)      # crescimento abrupto nos anéis
fat = hub.porte.values * cresc * u(0.8, 1.2)
sem_fat = rng.random(n) < 0.06
fat = np.where(sem_fat, np.nan, fat)
ecom = (hub.tipo_clnt != "BALCAO").values
pc_onl = np.where(ecom, u(0.75, 0.98), u(0.0, 0.12))
pc_cred = u(0.5, 0.85)
ticket = hub.ticket.values * u(0.7, 1.4) * np.where(anel_h, 2.2, 1.0)
qt = np.nan_to_num(fat / ticket).astype(int)
pc_chrg = np.where(anel_h, u(0.8, 3.5), rng.beta(1.2, 60, n) * 100)
pc_canc = np.where(anel_h, u(1.0, 4.0), rng.beta(2, 120, n) * 100)
arv = np.where(hub.uso_arv.values, fat * np.where(anel_h, u(0.65, 0.95), u(0.1, 0.5)), 0)
rbrp = np.where(rng.random(n) < 0.15, fat * u(0.05, 0.3), 0)
saldo = np.nan_to_num(fat) * u(0.8, 2.8)
tem_rsrv = np.where(anel_h, rng.random(n) < 0.3, rng.random(n) < 0.05)
tem_debito = rng.random(n) < np.where(anel_h, 0.25, 0.07)
inad = rng.random(n) < np.where(anel_h, 0.2, 0.05)
dfrt = rng.random(n) < 0.1
fat0 = np.nan_to_num(fat)

H = pd.DataFrame({
    "CD_MES": hub.CD_MES, "NU_EC": hub.nu_ec, "NM_EC": sujar_nome(list(hub.nm_ec), 0.1), "NM_CLNT": sujar_nome(list(hub.nm_clnt), 0.15),
    "NU_CDFR": np.where(hub.multi, pd.to_numeric(hub.raiz, errors="coerce").fillna(0).astype("int64") % 900000 + 100000, np.nan),
    "NM_CDFR": np.where(hub.multi, "REDE " + hub.nm_clnt.fillna("").str.split().str[0], None),
    "IN_EC_MTRZ": np.where(hub.filial.fillna(1) == 1, 1, 0),
    "NU_DCMT": sujar_doc(list(hub.doc), p_mask=0.15, p_zero=0.04),
    "NU_DCMT_RAIZ": np.where(hub.tp_pessoa == "PJ", hub.doc.str[:8], None),
    "NM_GRPO_ECNC": np.where(hub.multi, "GRUPO " + hub.nm_clnt.fillna("").str.split().str[0], None),
    "TIPO_CLNT": hub.tipo_clnt, "DC_TIPO_EMPA": hub.tipo_empa,
    "DC_SUB_ADQT": np.where(hub.tipo_clnt == "SUB", escolhe(["SUB", "MKP", "FCI", "Wallet", "CBPS", "SUB,MKP"], n), None),
    "NM_MTRZ_SGMT": np.select([hub.porte > 5e5, hub.porte > 8e4, hub.tp_pessoa == "PF"], ["GRANDES CONTAS", "EMPRESAS", "EMPREENDEDOR"], "VAREJO"),
    "NM_MTRZ_SUB_SGMT": escolhe(["VAREJO A", "VAREJO B", "EMPRESAS I", "EMPRESAS II", "EMPREENDEDOR DIGITAL"], n),
    "CD_SGMT_CMRL": hub.cd_rmat.values * 10 + 1, "NM_SGTO_CMRL": hub.nm_rmat,
    "NM_UF": np.where(rng.random(n) < 0.04, hub.uf.map(UF_NOME), np.where(rng.random(n) < 0.03, hub.uf.str.lower(), hub.uf)),
    "NM_MNCP": hub.municipio.str.upper(), "DC_ENDR": hub.endereco + ", " + hub.municipio.str.upper() + " - CEP " + hub.cep,
    "CD_RMAT": hub.cd_rmat, "NM_RMAT": hub.nm_rmat,
    "DT_AFLC": hub.dt_aflc.dt.strftime("%Y-%m-%d %H:%M:%S"),
    "QT_DIAS_AFLC": ((hub.mes_ini + pd.offsets.MonthEnd(0)) - hub.dt_aflc).dt.days,
    "CD_STCO_ATVD": np.where(sem_fat, 2, 1), "DC_STCO_ATVD": np.where(sem_fat, "INATIVO", "ATIVO"),
    "IN_ATIV_CDST": np.where(sem_fat, 0, 1), "IN_TIPO_ATVO": (rng.random(n) < 0.9).astype(int), "IN_SADA_CLNT": (rng.random(n) < 0.01).astype(int),
    "DC_CNAL_AFLC": np.where(anel_h, escolhe(["DIGITAL", "SUBADQUIRENTE", "PARCEIRO BANCARIO"], n, [.7, .2, .1]),
                             escolhe(["LOJA", "TELEVENDAS", "PARCEIRO BANCARIO", "DIGITAL", "SUBADQUIRENTE"], n, [.3, .15, .3, .2, .05])),
    "DT_PRIM_TRNS": (hub.dt_aflc + pd.to_timedelta(rng.integers(1, 20, n), "D")).dt.strftime("%Y-%m-%d"),
    "DT_ULTM_TRNS": (hub.mes_ini + pd.offsets.MonthEnd(0) - pd.to_timedelta(np.where(sem_fat, rng.integers(31, 120, n), rng.integers(0, 3, n)), "D")).dt.strftime("%Y-%m-%d"),
    "QT_DIAS_INTV": rng.integers(0, 4, n),
    "DC_SITU_ATVD": np.where(sem_fat, "INATIVO 30", "ATIVO"), "DC_SITU_ATVD_ULTM_MES": escolhe(["ATIVO", "ATIVO", "ATIVO", "INATIVO 30"], n),
    "DC_STAS_CLIE": np.where(meses_desde < 3, "NOVO", escolhe(["RECORRENTE", "RECORRENTE", "EM RISCO", "CHURN"], n, [.6, .25, .1, .05])),
    "DC_STAS_CLIE_ULTM_MES": escolhe(["RECORRENTE", "NOVO", "EM RISCO"], n, [.75, .15, .1]),
    "CD_BNCO_AGNC": [BANCOS[i][0] for i in hub.banco], "CD_AGNC": hub.agencia, "NM_BNCO": [BANCOS[i][1] for i in hub.banco],
    "NU_CNCR": rng.integers(100000, 999999, n),
    "VL_FTRM": np.where(rng.random(n) < 0.003, -1.0, fat),
    "VL_FTRM_FSCO": fat * (1 - pc_onl), "VL_FTRM_ONLN": fat * pc_onl, "VL_FTRM_DBTO": fat * (1 - pc_cred), "VL_FTRM_CRED": fat * pc_cred,
    "VL_FTRM_PARC": fat * pc_cred * u(0.1, 0.5), "VL_FTRM_CP": fat * (1 - pc_onl), "VL_FTRM_CNP": fat * pc_onl,
    "VL_FTRM_CNP_LINK": fat * pc_onl * np.where(anel_h, u(0.4, 0.8), u(0.1, 0.4)), "VL_FTRM_CNP_ECOM": fat * pc_onl * u(0.2, 0.6),
    "PC_FTRM_CP": np.where(sem_fat, np.nan, (1 - pc_onl) * 100), "PC_FTRM_CNP": np.where(sem_fat, np.nan, pc_onl * 100),
    "QT_TRNS_TOTL": qt, "QT_TRNS_FSCO": (qt * (1 - pc_onl)).astype(int), "QT_TRNS_ONLN": (qt * pc_onl).astype(int),
    "QT_TRNS_DBTO": (qt * (1 - pc_cred)).astype(int), "QT_TRNS_CRED": (qt * pc_cred).astype(int), "QT_TRNS_PARC": (qt * pc_cred * 0.3).astype(int),
    "QT_TRNS_CHRG": (qt * pc_chrg / 100).astype(int), "QT_TRNS_CNCL": (qt * pc_canc / 100).astype(int),
    "QT_TRNS_CP": (qt * (1 - pc_onl)).astype(int), "QT_TRNS_CNP": (qt * pc_onl).astype(int),
    "VL_RCTA_BRTA": fat * u(0.015, 0.032), "VL_ITCM_RMAT": fat * u(0.007, 0.011), "VL_TCKT_MEDO": np.where(sem_fat, np.nan, ticket),
    "VL_MRGM_CRBC_AJSD": fat * u(0.002, 0.012),
    "VL_ANTP_RBRP": rbrp, "VL_ANTP_ARV": arv, "VL_RCTA_ANTP_ARV": arv * u(0.015, 0.035), "VL_RCTA_ANTP_RBRP": rbrp * u(0.01, 0.03),
    "FL_VLME_ANTP_ARV": np.where(np.nan_to_num(arv) > 0, "S", "N"), "FL_RCTA_ANTP": np.where(np.nan_to_num(arv + rbrp) > 0, "S", "N"),
    "FL_VLOR_ANTE_FNIL": (np.nan_to_num(arv + rbrp) > 0).astype(int),
    "VL_BRTO_CHRG": fat0 * pc_chrg / 100, "VL_LQDO_CHRG": fat0 * pc_chrg / 100 * 0.8, "PC_CHRG": pc_chrg,
    "PC_CHRG_HIST": pc_chrg * u(0.6, 1.3), "VL_TMPO_MEDO_LQDC_CHRG": u(15, 75), "VL_HSTR_CHRG_SETR": hub.mcc_risco.values * 1.2 + u(0.1, 0.6),
    "VL_LQDO_CHRG_FRDE": fat0 * pc_chrg / 100 * 0.8 * 0.5, "VL_LQDO_CHRG_ATZC": fat0 * pc_chrg / 100 * 0.8 * 0.2,
    "VL_LQDO_CHRG_ERRO_PRSM": fat0 * pc_chrg / 100 * 0.8 * 0.1, "VL_LQDO_CHRG_DSCD_CMRL": fat0 * pc_chrg / 100 * 0.8 * 0.2,
    "VL_BRTO_CNCL": fat0 * pc_canc / 100, "VL_LQDO_CNCL": fat0 * pc_canc / 100 * 0.95, "PC_CANC": pc_canc, "PC_CANC_HIST": pc_canc * u(0.7, 1.2),
    "VL_TMPO_MEDO_LQDC_CANC": u(1, 20), "VL_LQDO_CHRG_CNCL_TOTL": fat0 * (pc_chrg * 0.8 + pc_canc * 0.95) / 100,
    "VL_CBRN": np.where(tem_debito, u(200, 20000), 0), "TIPO_CBRN": np.where(tem_debito, escolhe(["ALUGUEL POS", "AJUSTE", "CHARGEBACK", "CANCELAMENTO"], n), None),
    "VL_DBTO_PNDT_TOTL": np.where(tem_debito, u(500, 50000), 0),
    "VL_DBTO_PNDT_ALGL": np.where(tem_debito, u(0, 800), 0), "VL_DBTO_PNDT_AJST": np.where(tem_debito, u(0, 3000), 0),
    "VL_DBTO_PNDT_CANC": np.where(tem_debito, u(0, 8000), 0), "VL_DBTO_PNDT_CHRG": np.where(tem_debito, u(0, 20000), 0),
    "DT_DBTO_PNDT": np.where(tem_debito, (hub.mes_ini + pd.to_timedelta(rng.integers(0, 28, n), "D")).dt.strftime("%Y-%m-%d"), None),
    "VL_AGND_LVRE_MRCD": fat0 * u(0, 0.8), "VL_AGND_LVRE_CILO": fat0 * u(0.3, 1.5), "VL_AGND_NEGC_MRCD": fat0 * np.where(anel_h, u(0.3, 1.2), u(0, 0.3)),
    "VL_AGND_NEGC_CILO": fat0 * u(0, 0.4),
    "VL_SALD_PARA_RCBM_RBRP_TCD0": saldo * 0.1, "VL_SALD_PARA_RCBM_ARV": saldo * 0.2, "VL_SALD_PARA_RCBM_TOTL": saldo,
    "VL_SALD_PARA_RCBM_30D": saldo * 0.45, "VL_SALD_PARA_RCBM_60D": saldo * 0.25, "VL_SALD_PARA_RCBM_90D": saldo * 0.15,
    "VL_SALD_PARA_RCBM_MAIR_90D": saldo * 0.15, "PC_CNCN_RBRP_TCD0": u(0, 20), "PC_CNCN_ARV": np.where(anel_h, u(40, 90), u(0, 40)), "PC_CNCN_TOTL": u(10, 95),
    "QT_PRZO_DFRT_DIA": np.where(dfrt, escolhe([30, 60, 90], n), 0), "QT_PRZO_DFRT_MES": np.where(dfrt, escolhe([1, 2, 3], n), 0),
    "IN_CLNT_DFRT": dfrt.astype(int), "VL_DFRT": np.where(dfrt, fat0 * u(0.05, 0.3), 0), "VL_EXPS_DFRT": np.where(dfrt, fat0 * u(0.01, 0.1), 0),
    "QT_DIAS_DFRT": np.where(dfrt, rng.integers(1, 90, n), 0), "FL_CLNT_DRFT": dfrt.astype(int),
    "VL_INDM_30": np.where(inad, u(100, 5000), 0), "VL_INDM_31_60": np.where(inad, u(0, 4000), 0),
    "VL_INDM_61_90": np.where(inad, u(0, 3000), 0), "VL_INDM_90": np.where(inad & (rng.random(n) < 0.5), u(100, 20000), 0),
    "PC_INDM_30": np.where(inad, u(0, 5), 0), "PC_INDM_31_60": np.where(inad, u(0, 4), 0), "PC_INDM_61_90": np.where(inad, u(0, 3), 0),
    "PC_INDM_90": np.where(inad, u(0, 10), 0),
    "PC_RSRV": np.where(tem_rsrv, u(5, 40), np.nan), "DC_MTDO_RSRV": np.where(tem_rsrv, escolhe(["PERCENTUAL FATURAMENTO", "VALOR FIXO", "RETENCAO AGENDA"], n), None),
    "VL_RSRV": np.where(tem_rsrv, fat0 * u(0.05, 0.4), np.nan),
    "VL_LMTE_CRED": fat0 * u(0.3, 2.0), "DC_GRAU_ALVG": escolhe(["BAIXO", "MEDIO", "ALTO"], n, [.6, .3, .1]),
    "IN_ELGB_PRDT_TCD0": (rng.random(n) < 0.6).astype(int), "IN_ELGB_PRDT_TCD1": (rng.random(n) < 0.5).astype(int),
    "IN_ELGB_PRDT_ARV": (rng.random(n) < 0.7).astype(int), "IN_ELGB_PRDT_RBRP": (rng.random(n) < 0.4).astype(int),
    "FL_ACTE_OFRT_TCD0_FNIL": (rng.random(n) < 0.3).astype(int), "FL_ACTE_OFRT_TCD1_FNIL": (rng.random(n) < 0.2).astype(int),
    "FL_ELGB_TCD0_FNIL": (rng.random(n) < 0.6).astype(int), "FL_ELGB_TCD1_FNIL": (rng.random(n) < 0.5).astype(int),
    "FL_PROD_HBIL_TCD0_FNIL": (rng.random(n) < 0.4).astype(int), "FL_PROD_HBIL_TCD1_FNIL": (rng.random(n) < 0.3).astype(int),
    "IN_ALTR_QDRO_SCTR": hub.alt_societaria,
    "DT_ALTR_QDRO_SCTR": np.where(hub.alt_societaria == 1, (hub.dt_aflc + pd.to_timedelta(rng.integers(5, 40, n), "D")).dt.strftime("%Y-%m-%d"), None),
    "IN_SOCO_PEP": hub.pep, "DC_PNDT_TRBR_EMPA": escolhe(["SEM PENDENCIA", "COM PENDENCIA"], n, [.92, .08]),
    "DC_STCO_SOCO_RCTA": np.where(anel_h, escolhe(["REGULAR", "PENDENTE DE REGULARIZACAO", "SUSPENSA"], n, [.6, .3, .1]),
                                  escolhe(["REGULAR", "PENDENTE DE REGULARIZACAO", "SUSPENSA", "CANCELADA"], n, [.93, .05, .015, .005])),
    "IN_MRTO_EMPA_SETR": (rng.random(n) < 0.03).astype(int),
    "DC_RCMN_OPRL": escolhe(["MANTER", "MONITORAR", "REVISAR LIMITES", "BLOQUEAR ANTECIPACAO"], n, [.75, .15, .07, .03]),
    "DC_ALRT_CPTO": np.where(anel_h & (rng.random(n) < 0.6), escolhe(["AUMENTO ABRUPTO DE FATURAMENTO", "CONCENTRACAO CNP", "TRANSACOES MADRUGADA", "TICKET ATIPICO"], n),
                             np.where(rng.random(n) < 0.05, escolhe(["AUMENTO ABRUPTO DE FATURAMENTO", "TICKET ATIPICO"], n), "SEM ALERTA")),
    "DC_ALRT_CPTO_FTRM": np.where(anel_h & (cresc > 2), "FATURAMENTO ACIMA DO ESPERADO", "SEM ALERTA"),
    "NM_VRVL_ANML": np.where(anel_h & (rng.random(n) < 0.5), escolhe(["VL_FTRM", "QT_TRNS_CNP", "VL_TCKT_MEDO", "PC_CHRG"], n), None),
    "DC_MDNC_STAS_CPTO": np.where(anel_h, escolhe(["PIORA", "ESTAVEL"], n, [.6, .4]), escolhe(["ESTAVEL", "PIORA", "MELHORA"], n, [.8, .1, .1])),
    "DT_CRGA": (hub.mes_ini + pd.offsets.MonthEnd(0) + pd.Timedelta(days=5)).dt.strftime("%Y-%m-%d"),
    "DT_PRTC_CD_MES": hub.CD_MES,
})
H["NU_CDFR"] = pd.array(H["NU_CDFR"], dtype="Int64")
# reprocessamento: ~1% das linhas carregadas novamente com DT_CRGA posterior e VL_FTRM ajustado
dup = H.sample(frac=0.01, random_state=SEED).copy()
dup["DT_CRGA"] = (pd.to_datetime(dup["DT_CRGA"]) + pd.Timedelta(days=3)).dt.strftime("%Y-%m-%d")
dup["VL_FTRM"] = dup["VL_FTRM"] * 1.01
H = pd.concat([H, dup], ignore_index=True)

to_bronze(H, LAYOUT_HUB, "tbciar_re_hub_dado_risc_cred", ctrl=False, particao="DT_PRTC_CD_MES",
          comentario="Tabela de risco de crédito com indicadores de risco de crédito de todos os clientes (sintético - réplica de prd.re_aura.tbciar_re_hub_dado_risc_cred)")

# COMMAND ----------

# MAGIC %md ## 5. Transações Lynx — canal Cartões `tbciar_tr_lynx_trns_crto`

# COMMAND ----------

REGRAS = pd.DataFrame({"id_rgra": np.arange(1000, 1150)})
REGRAS["cd_tipo_rgra"] = escolhe(["pre", "pos", "pad", "can", "ros", "rcl", "acl", "bkr", "ant", "lol", "lof", "spg", "rml", "rsl"], 150,
                                 [.08, .1, .14, .05, .03, .08, .06, .04, .04, .12, .06, .04, .12, .04])
REGRAS["nu_vrso_rgra"] = [f"v{rng.integers(1, 6)}.{rng.integers(0, 10)}" for _ in range(150)]
regras_por_tipo = REGRAS.groupby("cd_tipo_rgra").id_rgra.apply(np.array).to_dict()
regra_versao = dict(zip(REGRAS.id_rgra, REGRAS.nu_vrso_rgra))

ec_anel_idx = ec.index[em_anel].to_numpy()
w = (ec.porte / ec.porte.sum()).values

# transações normais
n1 = N_CARTAO_NORMAL
t1 = pd.DataFrame({"ec_i": rng.choice(N_EC, n1, p=w), "p_i": rng.integers(0, N_PESSOAS, n1), "anel_tx": 0})
t1["vl"] = np.round(rng.lognormal(np.log(ec.ticket.values[t1.ec_i]), 0.8), 2)
t1["ts"] = timestamps(n1, 0.02)
# transações dos anéis: laranjas/controladores comprando nos ECs do anel
linhas = []
for info in anel_info:
    k = int(rng.integers(250, 650))
    compradores = np.concatenate([info["lar"], info["ctrl"]])
    df_ = pd.DataFrame({"ec_i": rng.choice(info["membros"], k),
                        "p_i": np.where(rng.random(k) < 0.8, rng.choice(compradores, k), rng.integers(0, N_PESSOAS, k)), "anel_tx": 1})
    tipo_vl = rng.random(k)
    df_["vl"] = np.select([tipo_vl < 0.3, tipo_vl < 0.5, tipo_vl < 0.7],
                          [rng.uniform(9500, 9999.99, k), rng.uniform(4700, 4999.99, k), rng.choice([1000., 2000., 3000., 5000.], k)],
                          np.round(rng.lognormal(7.5, 0.6, k), 2))
    df_["ts"] = timestamps(k, 0.45)
    df_["ip_anel"] = rng.choice(info["ips"], k)
    df_["end_anel"] = info["end"]
    linhas.append(df_)
tc = pd.concat([t1] + linhas, ignore_index=True).sort_values("ts").reset_index(drop=True)
tc["vl"] = tc.vl.round(2)
N = len(tc)
tc["c_i"] = [random.choice(cartoes_por_pessoa[p]) for p in tc.p_i]
E, P, C = ec.loc[tc.ec_i].reset_index(drop=True), pes.loc[tc.p_i].reset_index(drop=True), crt.loc[tc.c_i].reset_index(drop=True)
anel_tx = tc.anel_tx.values == 1
cnp = np.where(anel_tx, rng.random(N) < 0.8, np.where(E.tipo_clnt.values == "BALCAO", rng.random(N) < 0.06, rng.random(N) < 0.85))

score_nn = np.clip(np.where(anel_tx, rng.normal(640, 150, N), rng.normal(260, 140, N)), 0, 999).astype(int)
score_rg = np.clip(np.where(anel_tx, rng.normal(560, 180, N), rng.normal(180, 120, N)), 0, 999).astype(int)
score = ((score_nn * 0.6 + score_rg * 0.4)).astype(int)
score_pld = np.clip(np.where(anel_tx, rng.normal(620, 160, N), rng.normal(150, 110, N)), 0, 999).astype(int)
aprov = rng.random(N) < np.where(score > 800, 0.55, 0.94)
tem_regra = rng.random(N) < np.where(anel_tx, 0.55, 0.12)
tipo_regra = np.where(anel_tx, escolhe(["rml", "lol", "pad", "pos"], N, [.45, .25, .2, .1]),
                      escolhe(["pad", "pos", "pre", "lol", "lof", "rml", "can"], N, [.3, .2, .12, .15, .1, .05, .08]))
id_rgra = np.array([rng.choice(regras_por_tipo[t]) for t in tipo_regra])
id_rgra = np.where(tem_regra, id_rgra, 0)
parc = np.where(C.tipo.values == "CREDITO", np.where(rng.random(N) < 0.3, rng.integers(2, 13, N), 1), 1)
ts = pd.to_datetime(tc.ts)
dt_str = ts.dt.strftime("%Y%m%d").values
teste = rng.random(N) < 0.005
ip = np.where(anel_tx & tc.get("ip_anel").notna().values, tc.get("ip_anel").fillna("").values,
              [fake.ipv4_public() if c else None for c in cnp])
ip = np.where(cnp, ip, None)
end_entrega = np.where(anel_tx & (rng.random(N) < 0.6), tc.end_anel.fillna("").values, P.endereco.values)
cpf_pld_raw = np.where(rng.random(N) < 0.65, P.cpf.values, None)

T = pd.DataFrame({
    "dt_ordm": dt_str, "qt_cnar_ordm": np.arange(500_000_000, 500_000_000 + N), "nu_crto": C.pan, "nu_crto_tken": C.token,
    "id_estn": E.nu_ec, "dt_oprc": np.where(rng.random(N) < 0.03, ts.dt.strftime("%Y-%m-%d").values, dt_str),
    "hr_oprc": ts.dt.strftime("%H%M%S").values, "in_etnc": np.where(cnp, "1", "0"),
    "vl_trns": np.where(teste, 0.01, tc.vl.values), "in_acto": np.where(aprov, "1", "0"),
    "cd_pais_crto": np.where(rng.random(N) < np.where(anel_tx, 0.06, 0.015), escolhe(["840", "032", "600", "724"], N), "076"),
    "cd_pais_estn": "076", "in_cred": np.where(C.tipo.values == "DEBITO", "0", "1"),
    "cd_stas_frde": np.select([score > 850, score > 700, score > 550], ["BLOQUEADA", "EM ANALISE", "ALERTA"], "NORMAL"),
    "nu_score_totl": score, "nu_score_rede_nerl": score_nn, "nu_score_rgra": score_rg,
    "cd_estn": [f"{x:015d}" for x in E.nu_ec], "nu_vrso_rgra": np.where(tem_regra, [regra_versao.get(r, None) for r in id_rgra], None),
    "id_rgra": np.where(tem_regra, id_rgra.astype(str), None),
    "id_entd": ["E" + hashlib.md5(c.encode()).hexdigest()[:12].upper() for c in P.cpf], "sg_uf": E.uf,
    "tx_prfl_cptn": np.where(anel_tx, escolhe(["P11", "P12"], N), escolhe([f"P{i:02d}" for i in range(1, 11)], N)),
    "id_host": rng.integers(1, 5, N), "id_rgra_atzc": np.where(rng.random(N) < 0.2, rng.integers(1, 80, N).astype(str), None),
    "tx_acao_rgra_atzc": np.where(aprov, "APROVAR", escolhe(["NEGAR", "REVISAR"], N)),
    "id_unco": hexid(N, 32), "id_rgra_estn": np.where(rng.random(N) < 0.1, rng.integers(1, 50, N), 0),
    "nu_score_rgra_estn": rng.integers(0, 500, N), "vl_tste": np.where(teste, np.round(rng.uniform(1, 5, N), 2), 0.0),
    "cd_rsps_rgra_atzc": np.where(aprov, "00", "05"), "id_mdlo": escolhe([7, 8], N), "vl_perd_espa": np.round(tc.vl.values * score / 1000 * 0.1, 2),
    "nu_prob_frde": (score / 10).astype(int), "cd_bndr": C.bandeira, "in_cred_dbto": np.where(C.tipo.values == "DEBITO", "0", "1"),
    "nu_score_rede_nerl_estc": np.clip(score_nn + rng.integers(-50, 50, N), 0, 999), "vl_perd_espa_estc": np.round(tc.vl.values * score_nn / 1e4, 2),
    "nu_prob_frde_estc": (score_nn / 10).astype(int), "nu_score_rede_nerl_dnmc": np.clip(score_nn + rng.integers(-80, 80, N), 0, 999),
    "vl_perd_espa_dnmc": np.round(tc.vl.values * score_nn / 1.1e4, 2), "nu_prob_frde_dnmc": (score_nn / 11).astype(int),
    "tx_mdlo_vncd": escolhe([1, 2], N), "id_rgra_pld": np.where(score_pld > 600, np.char.add("PLD", rng.integers(10, 60, N).astype(str)), None),
    "nu_score_rgra_pld": score_pld, "in_blqo_crto": np.where(score > 900, "S", "N"), "in_blqo_estn": "N",
    "cd_tipo_crto": C.tipo, "cd_rsps": np.where(aprov, "00", escolhe(["05", "51", "59", "57"], N)), "cd_setr_atvd": E.mcc.astype(str),
    "cd_modo_etra_dado": np.where(cnp, "010", escolhe(["051", "071", "090", "801"], N, [.55, .38, .04, .03])),
    "cd_orgm_atzc": escolhe(["EMISSOR", "STAND-IN"], N, [.98, .02]), "tx_pos": np.where(cnp, "59", "00"), "tx_cpcd_trmn": np.where(cnp, 0, rng.integers(1, 6, N)),
    "in_parc": np.where(parc > 1, "S", "N"), "tx_tid": np.where(cnp, None, [f"{x:08d}" for x in rng.integers(0, 10**8, N)]),
    "cd_vl_fixo": "0", "cd_vl_fixo_0002": "0", "cd_vl_fixo_otro": "0", "cd_vl_fixo_exta": "0", "cd_vl_fixo_e": "E",
    "cd_srvc": escolhe(["201", "101", "221"], N), "in_fallback": np.where(rng.random(N) < 0.01, "S", "N"),
    "tx_eci_verifiedbyvisa": np.where(cnp, escolhe(["05", "06", "07"], N, [.6, .2, .2]), None), "in_anlc": "N",
    "tx_quem_respondeu": escolhe(["EMISSOR", "LYNX"], N, [.9, .1]), "cd_sgmt": E.nm_rmat,
    "nm_estn": sujar_nome(list(E.nm_ec), 0.2), "nu_prcl": parc.astype(str), "in_snha": np.where(cnp, "N", "S"),
    "in_cvv2": np.where(cnp, "S", "N"), "tx_tvr": np.where(cnp, None, "0000008000"), "nu_trmn": np.where(cnp, None, rng.integers(1, 20, N).astype(str)),
    "tx_meio_pgmn": np.where(cnp, 2, 1), "cd_tipo_pgmn": np.select([C.tipo.values == "DEBITO", parc > 1], ["DEBITO", "CREDITO PARCELADO"], "CREDITO A VISTA"),
    "in_rcre": np.where(rng.random(N) < 0.03, "S", "N"),
    "cd_tipo_trmn": np.where(cnp, escolhe(["ECOMMERCE", "LINK PAGAMENTO"], N, [.6, .4]), escolhe(["POS", "LIO", "TEF", "MPOS"], N, [.5, .2, .15, .15])),
    "in_cvv2_vald": np.where(cnp, "S", "N"), "cd_stas_lynx_onln": np.where(score > 700, "REVISAR", "OK"),
    "cd_atzc": [f"{x:06d}" for x in rng.integers(0, 10**6, N)], "nu_nsu": [f"{x:012d}" for x in rng.choice(10**12, N, replace=False)],
    "cd_stas_cncl": np.where(rng.random(N) < 0.015, "CANCELADA", None),
    "cd_tipo_cnal": np.where(cnp, escolhe(["ECOMMERCE", "LINK PAGAMENTO"], N), "POS"),
    "nu_ddd": np.where(cnp, P.ddd.values, None),
    "nu_celr": np.where(cnp, [f"({d}) {c[:5]}-{c[5:]}" if random.random() < 0.3 else c for d, c in zip(P.ddd, P.celular)], None),
    "cd_tipo_crto_bndr": C.bandeira + " " + C.tipo, "cd_bin6_crto": C.bin6,
    "tx_cter_dgtl": np.where(rng.random(N) < 0.12, escolhe(["APPLE PAY", "GOOGLE PAY", "SAMSUNG PAY"], N), None),
    "id_rede": escolhe(["CIELO"], N), "tx_endr_ip": ip,
    "in_pnpd_lio": np.where(cnp, "N", "S"), "in_rsps": "S", "in_qr_code": np.where(rng.random(N) < 0.04, "S", "N"),
    "in_ewallet": np.where(rng.random(N) < 0.1, "S", "N"), "dt_cptr": dt_str,
    "nm_ptdr_crto_chip": np.where(~cnp, P.nome.values, None), "nm_ctgr_ecommerce": np.where(cnp, E.nm_rmat.values, None),
    "nm_ptdr_crto_ecommerce": np.where(cnp, sujar_nome(list(P.nome), 0.3), None),
    "nu_cpf_ptdr_ecommerce": np.where(cnp, sujar_doc(list(P.cpf), p_mask=0.4, p_zero=0.05, p_typo=0.01), None),
    "tx_endr_ptdr_ecommerce": np.where(cnp, P.endereco.values, None), "nu_cep_ptdr_ecommerce": np.where(cnp, P.cep.values, None),
    "nu_celr_ptdr_ecommerce": np.where(cnp, ("55" + P.ddd + P.celular).values, None), "dt_nscm_ptdr_ecommerce": np.where(cnp, P.dt_nasc.values, None),
    "tx_emal_ptdr_ecommerce": np.where(cnp, [e.upper() if random.random() < 0.2 else e for e in P.email], None),
    "tx_endr_enta_ecommerce": np.where(cnp, end_entrega, None),
    "tx_prdt_ecommerce": np.where(cnp, escolhe(["ELETRONICOS", "GIFT CARD", "CREDITOS DE JOGO", "VESTUARIO", "SERVICOS", "JOIAS"], N), None),
    "tx_foma_pgmn_ecommerce": np.where(cnp, escolhe(["CARTAO", "CARTAO TOKENIZADO"], N), None), "nu_prcl_ecommerce": np.where(cnp, parc.astype(str), None),
    "tx_subestabelecimento_mcc": np.where(E.tipo_clnt.values == "SUB", E.mcc.values, None),
    "tx_subestabelecimento_cide": np.where(E.tipo_clnt.values == "SUB", E.municipio.values, None),
    "sg_subestabelecimento_estd": np.where(E.tipo_clnt.values == "SUB", E.uf.values, None),
    "nu_subestabelecimento_tlfn": np.where(E.tipo_clnt.values == "SUB", E.telefone.values, None),
    "nu_cnpj_subestabelecimento": np.where(E.tipo_clnt.values == "SUB", E.doc.values, None),
    "in_aplc_ecommerce": np.where(cnp, "S", "N"), "tx_ip_brsp": np.where(cnp & (rng.random(N) < 0.3), ip, None),
    "dc_soft_descriptor": ("CIELO*" + E.nm_ec.str[:14]).values, "in_clnt_etng": np.where(rng.random(N) < 0.01, "S", "N"),
    "cd_oprc_mdld": np.where(cnp, "CNP", "CP"), "tk_oprc_crto_tokenizado": np.where(rng.random(N) < 0.2, "S", "N"),
    "nu_oprc_cpf_ptdr_pld": sujar_doc(list(cpf_pld_raw), p_mask=0.25), "tx_oprc_nm_ptdr_pld": np.where(pd.notna(cpf_pld_raw), P.nome.values, None),
    "tx_oprc_app": np.where(cnp, escolhe(["APP LOJA", "SITE", "CHECKOUT CIELO"], N), None),
    "tx_oprc_stma_oprl": np.where(cnp, escolhe(["ANDROID", "IOS", "WINDOWS"], N, [.6, .3, .1]), None),
    "nu_oprc_vrso_sistemaoperacional": np.where(cnp, escolhe(["13", "14", "15", "17.4", "18.1"], N), None),
    "tx_oprc_tmpo_trns": rng.integers(150, 4000, N).astype(str), "nu_oprc_indi_ngto_anfr": rng.integers(0, 100, N).astype(str),
})
T["nm_estn"] = np.where(teste, "TESTE HOMOLOGACAO", T["nm_estn"])
T = ctrl_cols(T, ts.values, "TRJ", p_dup=0.015)
# guardamos chaves para as tabelas relacionadas (fraude, alertas, regras)
tc_ref = pd.DataFrame({"id_unco": T.id_unco[:N].values, "nu_nsu": T.nu_nsu[:N].values, "ts": ts.values, "score": score,
                       "anel": anel_tx, "id_rgra": id_rgra, "tem_regra": tem_regra, "ordem": T.qt_cnar_ordm[:N].values,
                       "token": C.token.values, "cpf": P.cpf.values, "ec_i": tc.ec_i.values})

# COMMAND ----------

# fraude reportada (§2) e resposta ao alerta (§3) — antes de gravar cartões para refletir dt_reporte_frde
p_frd = np.where(tc_ref.anel, 0.07, np.where(tc_ref.score > 750, 0.03, 0.0015))
frd = tc_ref[rng.random(N) < p_frd].copy()
frd["dh_rep"] = pd.to_datetime(frd.ts) + pd.to_timedelta(rng.integers(1, 40 * 24, len(frd)), "h")
frd = frd[frd.dh_rep < pd.Timestamp("2026-09-30")]
FRD = pd.DataFrame({
    "dh_trns": pd.to_datetime(frd.ts).dt.strftime("%Y-%m-%d %H:%M:%S").values, "dh_reporte": frd.dh_rep.dt.strftime("%Y-%m-%d %H:%M:%S").values,
    "cd_tipo_rsps_frde": escolhe([1, 2, 3, 4, 5, 6], len(frd), [.35, .2, .15, .1, .1, .1]),
    "in_match": np.where(rng.random(len(frd)) < 0.9, 0, 1), "cd_tipo_reporte": escolhe([3, 1], len(frd), [.4, .6]),
    "id_unco_lynx": frd.id_unco.values, "id_trns_orgm": frd.nu_nsu.values,
})
FRD = ctrl_cols(FRD, frd.dh_rep.values, "FRAUDE", p_dup=0.01)
to_bronze(FRD, LAYOUT_FRDE, "tbciar_tr_frde", comentario="Relatório das transações reportadas como fraude (sintético)")

frd_map = dict(zip(frd.id_unco, frd.dh_rep.dt.strftime("%Y%m%d")))
T["dt_reporte_frde"] = T.id_unco.map(frd_map)
T["cd_tipo_frde"] = np.where(T.dt_reporte_frde.notna(), escolhe(["CARTAO CLONADO", "AUTOFRAUDE", "CNP NAO RECONHECIDA", "LAVAGEM"], len(T)), None)

alr = tc_ref[tc_ref.score > 700].copy()
alr = alr[rng.random(len(alr)) < 0.8]
alr["dh"] = pd.to_datetime(alr.ts) + pd.to_timedelta(rng.integers(5, 72 * 60, len(alr)), "m")
analistas = [f"{fake.first_name().lower()}.{fake.last_name().lower()}" for _ in range(25)]
ids_analista = rng.integers(10000, 99999, 25)
ai = rng.integers(0, 25, len(alr))
RSP = pd.DataFrame({
    "id_unco_lynx": alr.id_unco.values, "dh_rsps": alr.dh.dt.strftime("%Y-%m-%d %H:%M:%S").values,
    "cd_tipo_rsps_frde": np.where(alr.anel, escolhe(["1", "3", "2"], len(alr), [.35, .45, .2]), escolhe(["1", "2", "3", "4"], len(alr), [.1, .75, .05, .1])),
    "id_usro": ids_analista[ai].astype(str), "cd_usro": [f"{analistas[i]}@cielo.com.br" for i in ai],
})
rsp_map = dict(zip(alr.id_unco, alr.dh.dt.strftime("%Y%m%d")))
T["dt_rsps_alrt"] = T.id_unco.map(rsp_map)
RSP = ctrl_cols(RSP, alr.dh.values, "RESPOSTA", p_dup=0.01)
to_bronze(RSP, LAYOUT_RSPS, "tbciar_tr_lynx_rsps", comentario="Respostas dadas aos alertas de fraude gerados (sintético)")

to_bronze(T, LAYOUT_CRTO, "tbciar_tr_lynx_trns_crto", comentario="Transações Lynx - canal Cartões (TRJ) (sintético)")

# regra acionada (§4): um disparo por transação com regra; nu_ordm_rgra = qt_cnar_ordm da transação
rg = tc_ref[tc_ref.tem_regra]
extra = rg[rng.random(len(rg)) < np.where(rg.anel, 0.6, 0.15)]           # segundo disparo
rg_all = pd.concat([rg.assign(seq=0), extra.assign(seq=1, id_rgra=rng.choice(REGRAS.id_rgra, len(extra)))])
dh = pd.to_datetime(rg_all.ts) + pd.to_timedelta(rng.integers(0, 3, len(rg_all)) + rg_all.seq.values, "s")
RGR = pd.DataFrame({"dt_hr_envio_rgra": dh.dt.strftime("%Y-%m-%d %H:%M:%S").values, "nu_ordm_rgra": rg_all.ordem.values,
                    "id_rgra": rg_all.id_rgra.values, "nu_vrso_rgra": [regra_versao[r] for r in rg_all.id_rgra],
                    "cd_tipo_rgra": REGRAS.set_index("id_rgra").loc[rg_all.id_rgra, "cd_tipo_rgra"].values})
RGR = ctrl_cols(RGR, dh.values, "REGRA")
to_bronze(RGR, LAYOUT_RGRA, "tbciar_tr_lynx_rgra", comentario="Regras acionadas pelas transações processadas (sintético)")

# COMMAND ----------

# MAGIC %md ## 6. Transações Lynx — canal PIX `tbciar_tr_lynx_trns_pix`

# COMMAND ----------

def chave_pix(tipo, doc, email, cel):
    return {"CPF": doc, "CNPJ": doc, "EMAIL": email, "TELEFONE": f"+55{cel}", "EVP": hashlib.md5((doc or "").encode()).hexdigest()}[tipo]

pix_rows = []
def add_pix(ec_i, pag, rec, vl, ts_, tipo_op, anel_flag, dsp=None):
    """pag/rec: dicts com doc, nome, banco, agencia, conta, email, cel, tp"""
    pix_rows.append((ec_i, pag, rec, vl, ts_, tipo_op, anel_flag, dsp))

def P_(i):
    r = pes.loc[i]; return dict(doc=r.cpf, nome=r.nome, banco=r.banco, ag=r.agencia, cc=r.conta, email=r.email, cel=r.ddd + r.celular, tp="PF", dsp=None)
def E_(i):
    r = ec.loc[i]; return dict(doc=r.doc, nome=r.nm_clnt, banco=r.banco, ag=r.agencia, cc=r.conta, email=r.email, cel=r.telefone, tp=r.tp_pessoa, dsp=r.dispositivo)

# normais — pagamentos de pessoas para ECs (70%), ECs pagando fornecedores/pessoas (25%), saque/troco (5%)
ec_pix = rng.choice(N_EC, N_PIX_NORMAL, p=w)
tp_op = escolhe(["PAGAMENTO", "TRANSFERENCIA", "SAQUE"], N_PIX_NORMAL, [.7, .25, .05])
pp = rng.integers(0, N_PESSOAS, N_PIX_NORMAL)
ts_pix = timestamps(N_PIX_NORMAL, 0.03)
vl_pix = np.round(rng.lognormal(4.6, 1.1, N_PIX_NORMAL), 2)
P_cache, E_cache = {}, {}
def gp(i):
    if i not in P_cache: P_cache[i] = P_(i)
    return P_cache[i]
def ge(i):
    if i not in E_cache: E_cache[i] = E_(i)
    return E_cache[i]
for k in range(N_PIX_NORMAL):
    e_, p_ = ge(ec_pix[k]), gp(pp[k])
    if tp_op[k] == "TRANSFERENCIA":
        add_pix(ec_pix[k], e_, p_, vl_pix[k] * 3, ts_pix[k], "TRANSFERENCIA", 0, e_["dsp"])
    else:
        add_pix(ec_pix[k], p_, e_, vl_pix[k], ts_pix[k], tp_op[k], 0)

# anéis: fan-in de laranjas -> EC, pass-through EC -> controlador, ciclos EC A -> laranja -> EC B -> controlador -> EC A
for info in anel_info:
    mem, lar, ctrl, dsp = info["membros"], info["lar"], info["ctrl"], info["dsp"]
    for _ in range(int(rng.integers(120, 260))):                     # fan-in
        e_i, l_i = rng.choice(mem), rng.choice(lar)
        t0 = DT_INI + pd.Timedelta(seconds=int(rng.integers(0, N_DIAS * 86400)))
        v = float(np.round(rng.uniform(800, 4990), 2))
        l = dict(gp(l_i)); l["dsp"] = random.choice(dsp) if rng.random() < 0.6 else None
        add_pix(e_i, l, ge(e_i), v, t0, "PAGAMENTO", 1, l["dsp"])
        if rng.random() < 0.55:                                      # pass-through em poucas horas
            c = gp(rng.choice(ctrl))
            add_pix(e_i, ge(e_i), c, float(np.round(v * rng.uniform(0.85, 0.98), 2)), t0 + pd.Timedelta(minutes=int(rng.integers(20, 600))), "TRANSFERENCIA", 1, random.choice(dsp))
    for _ in range(int(rng.integers(8, 20))):                         # ciclos
        a, b = rng.choice(mem, 2, replace=False)
        l_i, c_i = rng.choice(lar), rng.choice(ctrl)
        t0 = DT_INI + pd.Timedelta(seconds=int(rng.integers(0, (N_DIAS - 2) * 86400)))
        v = float(np.round(rng.uniform(15000, 80000), 2))
        add_pix(a, ge(a), gp(l_i), v, t0, "TRANSFERENCIA", 1, random.choice(dsp))
        add_pix(b, gp(l_i), ge(b), round(v * 0.97, 2), t0 + pd.Timedelta(hours=int(rng.integers(1, 12))), "PAGAMENTO", 1, random.choice(dsp))
        add_pix(b, ge(b), gp(c_i), round(v * 0.94, 2), t0 + pd.Timedelta(hours=int(rng.integers(12, 30))), "TRANSFERENCIA", 1, random.choice(dsp))
        add_pix(a, gp(c_i), ge(a), round(v * 0.91, 2), t0 + pd.Timedelta(hours=int(rng.integers(30, 48))), "PAGAMENTO", 1, random.choice(dsp))

M = len(pix_rows)
ec_i = np.array([r[0] for r in pix_rows]); pag = [r[1] for r in pix_rows]; rec = [r[2] for r in pix_rows]
vl = np.array([r[3] for r in pix_rows]); tsp = pd.to_datetime([r[4] for r in pix_rows]); tipo = np.array([r[5] for r in pix_rows], dtype=object)
anel_p = np.array([r[6] for r in pix_rows]) == 1; dsp = np.array([r[7] for r in pix_rows], dtype=object)
E = ec.loc[ec_i].reset_index(drop=True)
tp_chave = escolhe(["CPF", "CNPJ", "EMAIL", "TELEFONE", "EVP"], M, [.3, .2, .15, .2, .15])
tp_chave_rec = np.array([("CNPJ" if r["tp"] == "PJ" else "CPF") if t in ("CPF", "CNPJ") else t for r, t in zip(rec, tp_chave)], dtype=object)
tp_chave_pag = np.array([("CNPJ" if p["tp"] == "PJ" else "CPF") if t in ("CPF", "CNPJ") else t for p, t in zip(pag, escolhe(["CPF", "EMAIL", "TELEFONE", "EVP"], M))], dtype=object)
score_p = np.clip(np.where(anel_p, rng.normal(600, 160, M), rng.normal(220, 130, M)), 0, 999).astype(int)
aprov_p = rng.random(M) < np.where(score_p > 850, 0.6, 0.97)
cpfs_laranja = set(pes.loc[pes.papel == "LARANJA", "cpf"])
lar_mark = np.array([a and p["doc"] in cpfs_laranja for p, a in zip(pag, anel_p)])
dt_crca_cnta = (tsp - pd.to_timedelta(np.where(anel_p, rng.integers(5, 120, M), rng.integers(200, 3000, M)), "D"))
dt_crca_chve = (tsp - pd.to_timedelta(np.where(anel_p, rng.integers(1, 60, M), rng.integers(30, 1500, M)), "D"))
tem_regra_p = rng.random(M) < np.where(anel_p, 0.5, 0.08)
q = lambda lam_n, lam_a: np.where(anel_p, rng.poisson(lam_a, M), rng.poisson(lam_n, M))
stooge = np.where(lar_mark & (rng.random(M) < 0.45), rng.integers(1, 4, M), 0)

X = pd.DataFrame({
    "dt_ordm": tsp.strftime("%Y%m%d"), "id_estn": E.nu_ec, "dt_oprc": tsp.strftime("%Y%m%d").astype(int), "hr_oprc": tsp.strftime("%H%M%S").astype(int),
    "id_eltn": "1", "vl_trns": vl, "in_acto": np.where(aprov_p, "1", "0"), "id_pais": "BR", "cd_pais_estn": "076",
    "cd_stts": np.where(aprov_p, "LIQUIDADA", "REJEITADA"), "nu_score_totl": score_p,
    "nu_score_rede_nerl": np.clip(score_p + rng.integers(-60, 60, M), 0, 999), "nu_score_rgra": np.clip(score_p + rng.integers(-100, 100, M), 0, 999),
    "cd_estn": E.nu_ec.astype(str), "id_rgra": np.where(tem_regra_p, rng.choice(np.concatenate([regras_por_tipo["rml"], regras_por_tipo["acl"], regras_por_tipo["rcl"]]), M).astype(str), None),
    "id_entd": ["E" + hashlib.md5(p["doc"].encode()).hexdigest()[:12].upper() for p in pag], "id_uf": E.uf,
    "tx_prfl_cptn": np.where(anel_p, "PX9", escolhe(["PX1", "PX2", "PX3", "PX4"], M)),
    "nu_cpf_cnpj_recr": sujar_doc([r["doc"] for r in rec], p_mask=0.2, p_zero=0.05, p_typo=0.008), "nu_vrso_lyot": "3.2",
    "tx_acao_rgra_atzc": np.where(aprov_p, 1, 2), "id_cnta": [f"{p['banco']}-{p['ag']}-{p['cc']}" for p in pag],
    "cd_tipo_pgmn": np.where(tipo == "PAGAMENTO", "QRCODE", "CHAVE"),
    "cd_tipo_chve_pix": tp_chave_rec, "nm_razo_socl_pix": np.where([r["tp"] == "PJ" for r in rec], [r["nome"] for r in rec], None),
    "dt_crca_cnta": dt_crca_cnta.strftime("%Y%m%d"), "hr_crca_cnta": dt_crca_cnta.strftime("%H%M%S"),
    "nm_fnts_detn": np.where([r["tp"] != "PF" for r in rec], E.nm_ec.values, None),
    "dt_crca_chve": dt_crca_chve.strftime("%Y%m%d"), "hr_crca_chve": dt_crca_chve.strftime("%H%M%S"),
    "dt_pose_chve": dt_crca_chve.strftime("%Y%m%d"), "hr_pose_chve": dt_crca_chve.strftime("%H%M%S"),
    "dt_ulto_anfr": tsp.strftime("%Y%m%d"), "hr_ulto_anfr": tsp.strftime("%H%M%S"),
    "qt_trns_3d": q(2, 9), "qt_trns_30d": q(12, 60), "qt_trns_6m": q(60, 220),
    "qt_frde_rpto_3d": q(0.01, 0.3), "qt_frde_rpto_30d": q(0.05, 1.2), "qt_frde_rpto_6m": q(0.1, 2.5),
    "qt_frde_cfmd_3d": q(0.005, 0.15), "qt_frde_cfmd_30d": q(0.02, 0.6), "qt_frde_cfmd_6m": q(0.05, 1.2),
    "id_dspi": np.where(pd.notna(dsp), dsp, hexid(M, 24)),
    "tx_geolocalizacao_dspi": [f"{-23.5 + rng.normal(0, 3):.4f},{-46.6 + rng.normal(0, 3):.4f}" for _ in range(M)],
    "tx_fatr_autc": escolhe([1, 2, 3], M), "tx_usro_pix": [p["email"] for p in pag], "tx_atlc_chve": q(0.1, 1.5), "tx_claim_chve": q(0.02, 0.6),
    "nu_cnta_detn": [str(r["cc"]) for r in rec], "cd_tipo_oprc_pix": tipo, "qt_cd_trns_pix": rng.integers(1, 9, M),
    "nm_estn_pix": sujar_nome(list(E.nm_ec), 0.15), "nu_dcmt_estn": sujar_doc(list(E.doc), p_mask=0.3),
    "cd_tipo_pesa": [r["tp"] for r in rec], "qt_stas_trns_pix": np.where(aprov_p, 1, 9),
    "cd_tipo_trne": np.where(tipo == "SAQUE", "SAQUE", np.where(tipo == "PAGAMENTO", "COMPRA", "TRANSFERENCIA")),
    "tx_chve_pix_pgdr": [chave_pix(t, p["doc"], p["email"], p["cel"]) for t, p in zip(tp_chave_pag, pag)],
    "id_unco_pix": ["E" + "".join(random.choices("0123456789", k=8)) + t.strftime("%Y%m%d%H%M") + hexid(1, 11)[0] for t in tsp],
    "nu_ispb_recr": [BANCOS[r["banco"]][2] for r in rec], "nu_cnta_recr": [str(r["cc"]) for r in rec], "nu_agnc_recr": [f"{r['ag']:04d}" for r in rec],
    "cd_tipo_cnta_recr": escolhe(["CACC", "SVGS", "TRAN"], M, [.7, .1, .2]), "cd_tipo_bnfr_recr": [r["tp"] for r in rec],
    "nu_cpf_cnpj": [r["doc"] for r in rec], "nm_recr": sujar_nome([r["nome"] for r in rec], 0.3),
    "tx_chve_pix_recr": [chave_pix(t, r["doc"], r["email"], r["cel"]) for t, r in zip(tp_chave_rec, rec)],
    "vl_cpra": np.where(tipo == "PAGAMENTO", vl, np.nan), "tx_rsps_pgdr": np.where(aprov_p, "ACCP", "RJCT"),
    "id_tx_pix": hexid(M, 25), "nu_ispb_agnt_saqe": np.where(tipo == "SAQUE", E.banco.map(lambda b: BANCOS[b][2]).values, None),
    "cd_modo_agnt": np.where(tipo == "SAQUE", escolhe(["AGTEC", "AGTOT", "AGPSS"], M), None),
    "vl_espe": np.where(tipo == "SAQUE", np.round(vl * 0.5, 2), np.nan), "cd_tipo_pix": np.where(tipo == "SAQUE", "PIX SAQUE", "PIX"),
    "cd_erro_pix": np.where(aprov_p, None, escolhe(["AB03", "AC03", "AM04", "BE01"], M)), "cd_rsps": np.where(aprov_p, "00", "05"),
    "dh_dthrulteventspius": tsp.strftime("%Y-%m-%d %H:%M:%S"),
    "qt_d90_trns_spi_us": q(40, 150), "qt_m12_trns_spi_us": q(300, 700), "qt_m60_trns_spi_us": q(1200, 900),
    "qt_d90_stooge_acc_mrca_frde_us": stooge, "qt_m12_stooge_acc_mrca_frde_us": stooge + np.where(stooge > 0, rng.integers(0, 3, M), 0),
    "qt_m60_stooge_acc_mrca_frde_us": stooge + np.where(stooge > 0, rng.integers(0, 5, M), 0),
    "qt_d90_frde_acc_mrca_frde_us": q(0.01, 0.4), "qt_m12_frde_acc_mrca_frde_us": q(0.03, 0.9), "qt_m60_frde_acc_mrca_frde_us": q(0.05, 1.3),
    "qt_d90_totalspi_frde_mrca_frde_us": q(0.02, 0.8), "qt_m12_totalspi_frde_mrca_frde_us": q(0.05, 1.8), "qt_m60_totalspi_frde_mrca_frde_us": q(0.1, 2.5),
    "qt_d90_distinctpsp_frde_mrca_frde_us": q(0.01, 0.5), "qt_m12_distinctpsp_frde_mrca_frde_us": q(0.02, 1.0), "qt_m60_distinctpsp_frde_mrca_frde_us": q(0.03, 1.4),
    "qt_registered_cnta_cnta_us": q(1.5, 5),
    "qt_d90_trns_spiks": q(30, 120), "qt_m12_trns_spiks": q(250, 500), "qt_m60_trns_spiks": q(900, 700),
    "qt_d90_distinct_cnta_cnta_ks": q(1, 6), "qt_m12_distinct_cnta_cnta_ks": q(1.5, 10), "qt_m60_distinct_cnta_cnta_ks": q(2, 14),
    "tx_pix_tipo_saqe": np.where(tipo == "SAQUE", 1, 0),
    "nu_pix_ispb_pgdr": [BANCOS[p["banco"]][2] for p in pag], "nu_pix_cnta_pgdr": [str(p["cc"]) for p in pag],
    "nu_pix_agnc_pgdr": [f"{p['ag']:04d}" for p in pag], "nu_pix_tipo_cnta_pgdr": "CACC", "tx_pix_tipo_bnfr_pgdr": [p["tp"] for p in pag],
    "nu_pix_cpf_cnpj_pgdr": sujar_doc([p["doc"] for p in pag], p_mask=0.25, p_zero=0.05, p_typo=0.008),
    "tx_pix_nm_pgdr": sujar_nome([p["nome"] for p in pag], 0.3), "cd_pix_mdld_estn": np.where(E.tipo_clnt.values == "E-COMMERCE", "ONLINE", "PRESENCIAL"),
    "tx_pix_flag_dvlc": (rng.random(M) < 0.01).astype(int), "tx_pix_mrca_frde": (rng.random(M) < np.where(anel_p, 0.05, 0.002)).astype(int),
    "tx_pix_resc_trns": (rng.random(M) < 0.005).astype(int),
})
X = ctrl_cols(X, tsp.values, "PIX", p_dup=0.012)
to_bronze(X, LAYOUT_PIX, "tbciar_tr_lynx_trns_pix", comentario="Transações Lynx - canal PIX (sintético)")

# COMMAND ----------

# MAGIC %md ## 7. Transações Lynx — Antecipações / Recebíveis (RAD0) `tbciar_tr_lynx_trns_antp_rcbv`

# COMMAND ----------

n_antp = np.where(ec.uso_arv, np.where(em_anel, rng.poisson(14, N_EC), rng.poisson(3, N_EC)), 0)
ai = np.repeat(np.arange(N_EC), n_antp)
K = len(ai)
E = ec.loc[ai].reset_index(drop=True)
anel_a = (E.anel >= 0).values
ts_a = timestamps(K, 0.1)
vl_a = np.round(E.porte.values / 30 * rng.uniform(2, 12, K) * np.where(anel_a, 2.0, 1.0), 2)
# troca de domicílio bancário: ECs de anel às vezes antecipam para a conta de outro membro/laranja
troca = anel_a & (rng.random(K) < 0.25)
bco, ag, cc = E.banco.values.copy(), E.agencia.values.copy(), E.conta.values.copy()
for j in np.where(troca)[0]:
    c = random.choice(anel_info[E.anel[j]]["contas"]); bco[j], ag[j], cc[j] = c
score_a = np.clip(np.where(anel_a, rng.normal(560, 150, K), rng.normal(200, 120, K)), 0, 999).astype(int)
aprov_a = rng.random(K) < np.where(score_a > 800, 0.6, 0.96)
vl_txt = [f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if random.random() < 0.6 else f"{v:.2f}" for v in vl_a]

A = pd.DataFrame({
    "id_unco_lynx": hexid(K, 32), "dt_antp": ts_a.strftime("%Y%m%d").astype(int), "hr_antp": ts_a.strftime("%H:%M:%S"),
    "id_estn": [f"{x:012d}" for x in E.nu_ec], "cd_pais_estn": 76, "cd_estn": E.nu_ec.astype(str), "vl_trns": vl_txt,
    "in_acto": np.where(aprov_a, "1", "0"), "cd_cred": 1.0, "cd_stts": np.where(aprov_a, "EFETIVADA", "RECUSADA"),
    "nu_score_totl": score_a, "nu_score_rede_nerl": np.clip(score_a + rng.integers(-50, 50, K), 0, 999), "nu_score_rgra": np.clip(score_a + rng.integers(-90, 90, K), 0, 999),
    "nu_vrso_rgra": rng.integers(1, 6, K), "id_rgra": np.where(rng.random(K) < np.where(anel_a, 0.5, 0.1),
                                                                 rng.choice(np.concatenate([regras_por_tipo["bkr"], regras_por_tipo["ant"], regras_por_tipo["spg"]]), K).astype(str), None),
    "cd_tipo_objt_qualificado": "EC", "tx_acao_rgra_atzc": np.where(aprov_a, 1, 2), "id_lote": rng.integers(10**6, 10**7, K),
    "nu_cnpj_rspe_dmcl": [int(d) for d in E.doc], "cd_tipo_pesa": E.tp_pessoa,
    "nu_cnpj_cpf_mqnt": sujar_doc(list(E.doc), p_mask=0.5), "cd_tipo_cnta": escolhe(["CC", "CP", "PG"], K, [.8, .05, .15]),
    "nu_agnc": [f"{a:04d}" for a in ag], "nu_cnta": [str(c) for c in cc], "cd_bnco": [f"{BANCOS[b][0]:03d}" for b in bco],
    "nu_ispb": [BANCOS[b][2] for b in bco], "nu_cnta_pgmn": np.where(rng.random(K) < 0.2, [str(c) for c in cc], None),
    "tx_arnj_pgmn": escolhe(["VCC", "MCC", "ECC", "VDC", "MCD"], K), "tx_asoc_crto": escolhe(["VISA", "MASTERCARD", "ELO"], K),
    "qt_envo_bnco": rng.integers(1, 3, K).astype(str), "id_unco": hexid(K, 20), "cd_rsps": np.where(aprov_a, "00", "05"),
    "nu_dgto_agnc_cnta": rng.integers(0, 10, K).astype(str), "nu_vrso_lyot": "2.1",
    "dh_etra_webservice": (ts_a - pd.to_timedelta(rng.integers(1, 30, K), "s")).strftime("%Y-%m-%d %H:%M:%S"),
    "cd_bnco_exta": [f"{BANCOS[b][0]:03d}" for b in bco],
})
A = ctrl_cols(A, ts_a.values, "RAD0", p_dup=0.01)
to_bronze(A, LAYOUT_ANTP, "tbciar_tr_lynx_trns_antp_rcbv", comentario="Transações Lynx - canal Antecipações / Recebíveis RAD0 (sintético)")

# COMMAND ----------

# MAGIC %md ## 8. Inserção em Lista — 9 variações `tbciar_tr_lynx_lsta_*`

# COMMAND ----------

def lista(nome, id_lsta, objetos, adicionais, datas_ent, p_saida=0.12, comentario=""):
    n = len(objetos)
    ui = rng.integers(0, 25, n)
    ent = pd.to_datetime(datas_ent)
    saiu = rng.random(n) < p_saida
    sai = ent + pd.to_timedelta(rng.integers(5, 40, n), "D")
    fmt_ent = [d.strftime("%d/%m/%Y %H:%M") if random.random() < 0.05 else d.strftime("%Y-%m-%d %H:%M:%S") for d in ent]
    L = pd.DataFrame({"id_lsta": id_lsta, "id_objt_prcp": objetos, "id_objt_adcn": adicionais, "id_usro": ids_analista[ui],
                      "cd_usro": [f"{analistas[i]}@cielo.com.br" for i in ui], "dt_etra_lsta": fmt_ent,
                      "dt_sada_lsta": np.where(saiu & (sai < pd.Timestamp("2026-09-30")), sai.strftime("%Y-%m-%d %H:%M:%S"), None)})
    L = ctrl_cols(L, ent.values, f"LISTA_{id_lsta}")
    to_bronze(L, LAYOUT_LSTA, nome, comentario=comentario)

def datas(n, ini="2026-03-01", fim="2026-09-28"):
    a, b = pd.Timestamp(ini).value // 10**9, pd.Timestamp(fim).value // 10**9
    return pd.to_datetime(rng.integers(a, b, n), unit="s")

conta_str = lambda r: f"{BANCOS[r.banco][0]:03d}-{r.agencia:04d}-{r.conta}"

# EC negativa: ~55% dos ECs dos anéis + ruído de ECs comuns (alguns já expirados)
neg_ec = np.concatenate([rng.choice(ec_anel_idx, int(len(ec_anel_idx) * 0.55), replace=False),
                         rng.choice(ec.index[~em_anel], 35, replace=False)])
lista("tbciar_tr_lynx_lsta_ngto_ec", 6, ec.loc[neg_ec, "nu_ec"].values, [conta_str(r) for r in ec.loc[neg_ec].itertuples()], datas(len(neg_ec)),
      comentario="Lista Negativa - Estabelecimento Comercial (sintético)")
pos_ec = ec[~em_anel].sort_values("porte", ascending=False).index[:120]
lista("tbciar_tr_lynx_lsta_psit_ec", 3, ec.loc[pos_ec, "nu_ec"].values, None, datas(len(pos_ec)), 0.05, "Lista Positiva - Estabelecimento Comercial (sintético)")
aut_ec = rng.choice(ec.index[~em_anel], 40, replace=False)
lista("tbciar_tr_lynx_lsta_psit_atzd_ec", 9, ec.loc[aut_ec, "nu_ec"].values, None, datas(40), 0.1, "Lista Positiva Autorizada - Estabelecimento Comercial (sintético)")

# clientes (CPF como bigint)
lar_all = pes.index[pes.papel == "LARANJA"].to_numpy(); ctrl_all = pes.index[pes.papel == "CONTROLADOR"].to_numpy()
neg_cl = np.concatenate([rng.choice(lar_all, int(len(lar_all) * 0.3), replace=False), rng.choice(ctrl_all, int(len(ctrl_all) * 0.3), replace=False),
                         rng.choice(pes.index[pes.papel == "COMUM"], 80, replace=False)])
lista("tbciar_tr_lynx_lsta_ngto_clnt", 5, pes.loc[neg_cl, "cpf"].astype("int64").values, pes.loc[neg_cl, "celular"].values, datas(len(neg_cl)),
      comentario="Lista Negativa - Cliente (sintético)")
pos_cl = rng.choice(pes.index[pes.papel == "COMUM"], 300, replace=False)
lista("tbciar_tr_lynx_lsta_psit_clnt", 2, pes.loc[pos_cl, "cpf"].astype("int64").values, None, datas(300), 0.05, "Lista Positiva - Cliente (sintético)")
aut_cl = rng.choice(pes.index[pes.papel == "COMUM"], 80, replace=False)
lista("tbciar_tr_lynx_lsta_psit_atzd_clnt", 8, pes.loc[aut_cl, "cpf"].astype("int64").values, None, datas(80), 0.1, "Lista Positiva Autorizada - Cliente (sintético)")

# cartões (token como bigint): negativa = cartões com fraude reportada
tok_frd = pd.Series(tc_ref.loc[tc_ref.id_unco.isin(frd.id_unco), "token"].unique())
tok_frd = tok_frd.sample(frac=0.7, random_state=SEED)
lista("tbciar_tr_lynx_lsta_ngto_crto", 4, tok_frd.astype("int64").values, None, datas(len(tok_frd), "2026-08-05"), comentario="Lista Negativa - Cartão (sintético)")
pos_crt = crt.sample(400, random_state=SEED)
lista("tbciar_tr_lynx_lsta_psit_crto", 1, pos_crt.token.astype("int64").values, None, datas(400), 0.05, "Lista Positiva - Cartão (sintético)")
aut_crt = crt.sample(100, random_state=SEED + 1)
lista("tbciar_tr_lynx_lsta_psit_atzd_crto", 7, aut_crt.token.astype("int64").values, None, datas(100), 0.1, "Lista Positiva Autorizada - Cartão (sintético)")

# COMMAND ----------

# MAGIC %md ## 9. Resumo da camada Bronze

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {BRONZE}"))
resumo = [(t.tableName, spark.table(f"{BRONZE}.{t.tableName}").count(), len(spark.table(f"{BRONZE}.{t.tableName}").columns))
          for t in spark.sql(f"SHOW TABLES IN {BRONZE}").collect()]
display(spark.createDataFrame(resumo, "tabela string, linhas long, colunas int"))
