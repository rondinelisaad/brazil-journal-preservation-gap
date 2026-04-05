#!/usr/bin/env bash
set -euo pipefail

PYTHON="${PYTHON:-python3}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RAW_DIR="${ROOT_DIR}/data/raw"
PROCESSED_DIR="${ROOT_DIR}/data/processed"
LOG_DIR="${ROOT_DIR}/logs"

TITLEDB_XML="${TITLEDB_XML:-}"
OPENALEX_BRUTO="${OPENALEX_BRUTO:-${RAW_DIR}/openalex_BR_2000_2024_tratado.csv}"
DOAJ_BRUTO="${DOAJ_BRUTO:-${RAW_DIR}/doaj_brasil_extraido_do_json.csv}"
LATINDEX_BRUTO="${LATINDEX_BRUTO:-${RAW_DIR}/latindex-journals-brasileiros-v2.csv}"
REVIEW_FILE="${REVIEW_FILE:-${PROCESSED_DIR}/revisao_manual_periodicos_residuais.csv}"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$1"
}

die() {
  printf '[%s] ERRO: %s\n' "$(timestamp)" "$1" >&2
  exit 1
}

run_step() {
  local step_name="$1"
  local logfile="$2"
  shift 2

  log "Iniciando: ${step_name}"
  "$@" > "${logfile}" 2>&1
  log "Concluído: ${step_name}"
}

usage() {
  cat <<EOF
Uso:
  TITLEDB_XML=/caminho/para/titledb.xml ./run_pipeline.sh

Variáveis opcionais:
  PYTHON
  OPENALEX_BRUTO
  DOAJ_BRUTO
  LATINDEX_BRUTO
  REVIEW_FILE
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

[[ -n "${TITLEDB_XML}" ]] || die "Defina TITLEDB_XML com o caminho do titledb.xml"
[[ -f "${TITLEDB_XML}" ]] || die "Arquivo TITLEDB_XML não encontrado: ${TITLEDB_XML}"

[[ -f "${OPENALEX_BRUTO}" ]] || die "Arquivo OpenAlex bruto não encontrado: ${OPENALEX_BRUTO}"
[[ -f "${DOAJ_BRUTO}" ]] || die "Arquivo DOAJ bruto não encontrado: ${DOAJ_BRUTO}"
[[ -f "${LATINDEX_BRUTO}" ]] || die "Arquivo Latindex bruto não encontrado: ${LATINDEX_BRUTO}"

mkdir -p "${RAW_DIR}" "${PROCESSED_DIR}" "${LOG_DIR}"

log "Pipeline iniciado"

# =========================================================
# 1) INGESTÃO
# =========================================================
run_step \
  "01_ingestion/parser_titledb_cariniana.py" \
  "${LOG_DIR}/01_parser_titledb_cariniana.log" \
  "${PYTHON}" "${ROOT_DIR}/scripts/01_ingestion/parser_titledb_cariniana.py" \
  "${TITLEDB_XML}" \
  --outdir "${PROCESSED_DIR}"

run_step \
  "01_ingestion/preparar_bases_reais_cariniana.py" \
  "${LOG_DIR}/02_preparar_bases_reais_cariniana.log" \
  "${PYTHON}" "${ROOT_DIR}/scripts/01_ingestion/preparar_bases_reais_cariniana.py" \
  --openalex-bruto "${OPENALEX_BRUTO}" \
  --doaj-bruto "${DOAJ_BRUTO}" \
  --latindex-bruto "${LATINDEX_BRUTO}" \
  --outdir "${PROCESSED_DIR}"

# =========================================================
# 2) GERAÇÃO DA BASE
# =========================================================
[[ -f "${PROCESSED_DIR}/preservacao_titledb.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/preservacao_titledb.csv"

run_step \
  "03_base_generation/gerar_periodicos_base.py" \
  "${LOG_DIR}/03_gerar_periodicos_base.log" \
  "${PYTHON}" "${ROOT_DIR}/scripts/03_base_generation/gerar_periodicos_base.py" \
  --input "${PROCESSED_DIR}/preservacao_titledb.csv" \
  --outdir "${PROCESSED_DIR}"

# =========================================================
# 3) VALIDAÇÃO DE URL / INFRAESTRUTURA
# =========================================================
if [[ -f "${PROCESSED_DIR}/periodicos_base.csv" ]]; then
  run_step \
    "05_validation/validador_urls_cariniana.py (periodicos_base.csv)" \
    "${LOG_DIR}/04_validador_urls_cariniana.log" \
    "${PYTHON}" "${ROOT_DIR}/scripts/05_validation/validador_urls_cariniana.py" \
    "${PROCESSED_DIR}/periodicos_base.csv" \
    --outdir "${PROCESSED_DIR}"
elif [[ -f "${PROCESSED_DIR}/preservacao_titledb.csv" ]]; then
  run_step \
    "05_validation/validador_urls_cariniana.py (preservacao_titledb.csv)" \
    "${LOG_DIR}/04_validador_urls_cariniana.log" \
    "${PYTHON}" "${ROOT_DIR}/scripts/05_validation/validador_urls_cariniana.py" \
    "${PROCESSED_DIR}/preservacao_titledb.csv" \
    --source-system titledb \
    --outdir "${PROCESSED_DIR}"
else
  die "Nem periodicos_base.csv nem preservacao_titledb.csv foram encontrados para validação de URLs"
fi

# =========================================================
# 4) RECONCILIAÇÃO
# =========================================================
[[ -f "${PROCESSED_DIR}/periodicos_base.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/periodicos_base.csv"
[[ -f "${PROCESSED_DIR}/editorial_openalex.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/editorial_openalex.csv"
[[ -f "${PROCESSED_DIR}/indexacao_doaj.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/indexacao_doaj.csv"
[[ -f "${PROCESSED_DIR}/indexacao_latindex.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/indexacao_latindex.csv"
[[ -f "${PROCESSED_DIR}/infraestrutura_url.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/infraestrutura_url.csv"
[[ -f "${PROCESSED_DIR}/preservacao_titledb.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/preservacao_titledb.csv"

run_step \
  "02_reconciliation/reconciliador_cariniana.py" \
  "${LOG_DIR}/05_reconciliador_cariniana.log" \
  "${PYTHON}" "${ROOT_DIR}/scripts/02_reconciliation/reconciliador_cariniana.py" \
  --periodicos "${PROCESSED_DIR}/periodicos_base.csv" \
  --openalex "${PROCESSED_DIR}/editorial_openalex.csv" \
  --doaj "${PROCESSED_DIR}/indexacao_doaj.csv" \
  --latindex "${PROCESSED_DIR}/indexacao_latindex.csv" \
  --infra "${PROCESSED_DIR}/infraestrutura_url.csv" \
  --titledb "${PROCESSED_DIR}/preservacao_titledb.csv" \
  --outdir "${PROCESSED_DIR}"

# =========================================================
# 5) CLASSIFICAÇÃO
# =========================================================
run_step \
  "04_classification/classificador_recalibrado.py" \
  "${LOG_DIR}/06_classificador_recalibrado.log" \
  "${PYTHON}" "${ROOT_DIR}/scripts/04_classification/classificador_recalibrado.py"

run_step \
  "04_classification/qualificar_categorias.py" \
  "${LOG_DIR}/07_qualificar_categorias.log" \
  "${PYTHON}" "${ROOT_DIR}/scripts/04_classification/qualificar_categorias.py"

# =========================================================
# 6) AMOSTRAGEM PARA VALIDAÇÃO MANUAL
# =========================================================
[[ -f "${PROCESSED_DIR}/diagnostico_risco.csv" ]] || die "Arquivo ausente: ${PROCESSED_DIR}/diagnostico_risco.csv"

run_step \
  "05_validation/gerar_amostra_validacao_manual.py" \
  "${LOG_DIR}/08_gerar_amostra_validacao_manual.log" \
  "${PYTHON}" "${ROOT_DIR}/scripts/05_validation/gerar_amostra_validacao_manual.py" \
  --input "${PROCESSED_DIR}/diagnostico_risco.csv" \
  --outdir "${PROCESSED_DIR}" \
  --n-a 30 \
  --n-b 30 \
  --seed 42

# =========================================================
# 7) REVISÃO MANUAL RESIDUAL (CONDICIONAL)
# =========================================================
if [[ -f "${REVIEW_FILE}" && -f "${PROCESSED_DIR}/periodicos_base_corrigido.csv" ]]; then
  run_step \
    "05_validation/aplicar_revisao_periodicos_residuais.py" \
    "${LOG_DIR}/09_aplicar_revisao_periodicos_residuais.log" \
    "${PYTHON}" "${ROOT_DIR}/scripts/05_validation/aplicar_revisao_periodicos_residuais.py" \
    --periodicos "${PROCESSED_DIR}/periodicos_base_corrigido.csv" \
    --revisao "${REVIEW_FILE}" \
    --outdir "${PROCESSED_DIR}"
else
  log "Revisão residual não executada."
  log "Motivo: arquivo de revisão manual ausente (${REVIEW_FILE}) ou periodicos_base_corrigido.csv não encontrado."
fi

log "Pipeline concluído com sucesso"

#=========================================================
#PIPELINE FINALIZADO
#=========================================================
#Saídas esperadas em: ${PROCESSED_DIR}
#
#Arquivos principais:
#- preservacao_titledb.csv
#- periodicos_base.csv
#- editorial_openalex.csv
#- indexacao_doaj.csv
#- indexacao_latindex.csv
#- infraestrutura_url.csv
#- periodicos_enriquecido.csv
#- diagnostico_risco.csv
#- auditoria_match.csv
#- periodicos_base_corrigido_v3.csv
#- mapa_periodicos_consolidados_v3.csv
#- planilha_validacao_manual.csv

#Logs:
#- ${LOG_DIR}

#ATENÇÃO:
#A validação manual continua sendo parcialmente não automatizável.
#=========================================================
