#!/usr/bin/env bash
#
# subir-raw.sh — sobe os 10 CSVs de ERP e CRM para o Volume raw do Unity Catalog.
#
# Origem:  dados/erp e dados/crm (na RAIZ do repositório, dois níveis acima
#          deste script: rotaperfume/scripts/ -> repo/dados).
# Destino: /Volumes/lakehouse_rouper/bronze/raw/{erp,crm}
#
# Detalhe importante: `databricks fs cp` exige o esquema `dbfs:` no destino,
# MESMO sendo um Volume do UC. O caminho continua sendo /Volumes/...
#
# Pré-requisito: o Volume bronze.raw precisa existir antes. Rode
# scripts/criar-catalogo.sh primeiro.
#
# Uso:  bash scripts/subir-raw.sh <profile>
#   ex: bash scripts/subir-raw.sh dbc-8f60023a-5d91

set -euo pipefail

PROFILE="${1:?informe o profile como primeiro argumento, ex: dbc-8f60023a-5d91}"
CATALOG="lakehouse_rouper"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DADOS_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)/dados"

if [[ ! -d "${DADOS_DIR}/erp" || ! -d "${DADOS_DIR}/crm" ]]; then
  echo "ERRO: não encontrei os dados em '${DADOS_DIR}' (esperado dados/erp e dados/crm)." >&2
  echo "      Confirme que a pasta dados/ está na raiz do repositório." >&2
  exit 1
fi

DEST_BASE="dbfs:/Volumes/${CATALOG}/bronze/raw"

for sistema in erp crm; do
  echo "==> Subindo dados/${sistema} -> ${DEST_BASE}/${sistema}"
  databricks fs cp --recursive --overwrite \
    "${DADOS_DIR}/${sistema}" "${DEST_BASE}/${sistema}" \
    --profile "$PROFILE"
done

echo "==> OK. CSVs no Volume. Confira com:"
echo "    databricks fs ls ${DEST_BASE}/erp --profile ${PROFILE}"
echo "    databricks fs ls ${DEST_BASE}/crm --profile ${PROFILE}"
