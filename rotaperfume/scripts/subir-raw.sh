#!/usr/bin/env bash
#
# subir-raw.sh — sobe os 10 CSVs crus para o Volume raw do Unity Catalog.
#
# Roda DEPOIS do `databricks bundle deploy`: o volume bronze.raw é criado pelo bundle
# (resources/catalogo.yml), e arquivo só sobe em volume que já existe.
#
# ATENÇÃO ao esquema `dbfs:` no destino — `databricks fs cp` o exige MESMO sendo um
# Volume do UC (o caminho lógico é /Volumes/..., mas a CLI quer o prefixo dbfs:).
#
# Uso:  bash scripts/subir-raw.sh <profile>
set -euo pipefail

PROFILE="${1:?uso: bash scripts/subir-raw.sh <profile>}"

# Precisa bater com o default de var.catalog em databricks.yml.
CATALOG="lakehouse_rotaperfume"
VOL="dbfs:/Volumes/${CATALOG}/bronze/raw"

# dados/ fica na RAIZ do repositório — é a entrada do projeto (input versionado),
# não uma saída do pipeline.
REPO_ROOT="$(git rev-parse --show-toplevel)"
DADOS="${REPO_ROOT}/dados"

if [[ ! -d "${DADOS}/erp" || ! -d "${DADOS}/crm" ]]; then
  echo "✗ não encontrei ${DADOS}/erp e ${DADOS}/crm." >&2
  echo "  dados/ é a entrada do projeto e deveria estar versionada na raiz do repo." >&2
  exit 1
fi

for sistema in erp crm; do
  echo "→ subindo ${sistema}/ para ${VOL}/${sistema} (profile ${PROFILE})…"
  databricks fs cp --recursive --overwrite \
    "${DADOS}/${sistema}" "${VOL}/${sistema}" \
    --profile "${PROFILE}"
done

echo "✓ CSVs no volume. Confira:  databricks fs ls ${VOL}/erp --profile ${PROFILE}"
