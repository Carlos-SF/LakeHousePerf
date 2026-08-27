#!/usr/bin/env bash
#
# criar-catalogo.sh — garante o catálogo, os schemas da medalhão e o Volume raw.
#
# POR QUE ISTO É UM SCRIPT E NÃO ESTÁ NO BUNDLE:
#   1. No Free Edition o Default Storage está ligado, e nessa configuração a API
#      do Unity Catalog RECUSA criar catálogo — ela exige um MANAGED LOCATION que
#      a conta gratuita não tem:
#        Error: Metastore storage root URL does not exist.
#               Default Storage is enabled in your account. (400 INVALID_STATE)
#      O comando SQL, por outro lado, funciona.
#   2. O target dev do bundle usa `mode: development`, que prefixa o NOME dos
#      recursos com [dev seu_usuario] — inclusive SCHEMAS e VOLUMES do UC. Se
#      declarássemos bronze/silver/gold/raw no bundle, eles virariam
#      dev_carlos_bronze e o caminho /Volumes/lakehouse_rouper/bronze/raw
#      deixaria de existir. Por isso o "catálogo como código" mora aqui, fora do
#      bundle, com nomes fixos.
#
# ESTADO ATUAL: o catálogo lakehouse_rouper e os schemas bronze/silver/gold já
# existem neste workspace. Tudo aqui é IF NOT EXISTS (idempotente) — a rede de
# segurança. O objeto realmente novo é o Volume bronze.raw.
#
# Uso:  bash scripts/criar-catalogo.sh <profile>
#   ex: bash scripts/criar-catalogo.sh dbc-8f60023a-5d91

set -euo pipefail

PROFILE="${1:?informe o profile como primeiro argumento, ex: dbc-8f60023a-5d91}"
CATALOG="lakehouse_rouper"

run_sql() {
  echo "-- SQL: $1"
  databricks experimental aitools tools query "$1" --profile "$PROFILE"
}

echo "==> Garantindo catálogo, schemas e Volume raw em '$CATALOG' (profile: $PROFILE)"

run_sql "CREATE CATALOG IF NOT EXISTS ${CATALOG}
         COMMENT 'Lakehouse rotaperfume — dados de ERP e CRM em camadas medalhão.'"

run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.bronze
         COMMENT 'Camada bronze: dados brutos ingeridos 1:1 da origem (raw -> tabelas).'"

run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.silver
         COMMENT 'Camada silver: dados limpos, tipados e conformados.'"

run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.gold
         COMMENT 'Camada gold: modelos de consumo e métricas de negócio.'"

# O Volume MANAGED guarda o CSV exatamente como saiu do ERP/CRM, byte por byte.
run_sql "CREATE VOLUME IF NOT EXISTS ${CATALOG}.bronze.raw
         COMMENT 'Arquivos crus (CSV) de ERP e CRM, como chegaram da origem.'"

echo "==> OK. Catálogo, schemas e Volume ${CATALOG}.bronze.raw prontos."
