#!/usr/bin/env bash
#
# criar-catalogo.sh — cria o catálogo do projeto, por SQL.
#
# POR QUE ISSO NÃO ESTÁ NO BUNDLE (resources/catalogo.yml tem só schemas e volume):
# este metastore usa Default Storage. Nessa configuração a API do Unity Catalog RECUSA
# criar catálogo, porque ela exige um MANAGED LOCATION que a conta não tem:
#     Error: Metastore storage root URL does not exist.
#            Default Storage is enabled in your account. (400 INVALID_STATE)
# O comando SQL `CREATE CATALOG IF NOT EXISTS` funciona. Por isso o catálogo nasce aqui,
# fora do bundle, e este script tem que rodar ANTES do `databricks bundle deploy` — os
# schemas do catalogo.yml precisam do catálogo já existente.
#
# Roda em serverless SQL: `aitools tools query` escolhe sozinho o warehouse padrão do
# workspace. Não há warehouse fixo (este workspace é todo serverless).
#
# Uso:  bash scripts/criar-catalogo.sh <profile>
set -euo pipefail

PROFILE="${1:?uso: bash scripts/criar-catalogo.sh <profile>}"

# Precisa bater com o default de var.catalog em databricks.yml.
CATALOG="lakehouse_rotaperfume"

echo "→ criando (se não existir) o catálogo ${CATALOG} via SQL, profile ${PROFILE}…"
databricks experimental aitools tools query \
  "CREATE CATALOG IF NOT EXISTS ${CATALOG}" \
  --profile "${PROFILE}"

echo "✓ catálogo ${CATALOG} pronto. Agora rode: databricks bundle deploy --target dev --profile ${PROFILE}"
