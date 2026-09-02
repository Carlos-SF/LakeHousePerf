-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver — clientes: limpo, tipado, deduplicado, com contrato
-- MAGIC
-- MAGIC A bronze preservou a sujeira de propósito. Aqui a conversão é escrita **uma vez**:
-- MAGIC
-- MAGIC - **cnpj** vem em três formatos (puro, pontuado, com espaço em volta). Normaliza para
-- MAGIC   14 dígitos: `trim` → `regexp_replace` tira não-dígito → `lpad` com zero à esquerda.
-- MAGIC   **Nunca** vira número (senão o zero à esquerda de 309 CNPJs some, calado).
-- MAGIC - **razao_social** com caixa e espaçamento inconsistentes → `initcap` + colapsa espaço.
-- MAGIC - **data_cadastro** em ISO e `dd/MM/yyyy` misturados → `coalesce` de dois `try_to_date`
-- MAGIC   (ANSI mode: `to_date` sobre data malformada **aborta** a query, não vira NULL).
-- MAGIC - **dedup**: 40 CNPJs têm dois `cliente_id`. `row_number()` por CNPJ mantém o cadastro
-- MAGIC   **mais antigo**; o id descartado fica em `cliente_ids_duplicados` (os pedidos antigos
-- MAGIC   ainda apontam para ele). 3.040 → 3.000.
-- MAGIC - **ativo**: `'S'/'N'` → boolean.
-- MAGIC
-- MAGIC Contrato ao final: `length(cnpj) = 14` e `data_cadastro IS NOT NULL`.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.clientes AS
WITH base AS (
  SELECT
    CAST(cliente_id AS BIGINT)                                                     AS cliente_id,
    lpad(regexp_replace(trim(cnpj), '[^0-9]', ''), 14, '0')                        AS cnpj,
    initcap(regexp_replace(trim(razao_social), '\\s+', ' '))                       AS razao_social,
    segmento,
    cidade,
    uf,
    bairro,
    coalesce(try_to_date(data_cadastro), try_to_date(data_cadastro, 'dd/MM/yyyy')) AS data_cadastro,
    (ativo = 'S')                                                                  AS ativo
  FROM lakehouse_rotaperfume.bronze.clientes
),
ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY cnpj ORDER BY data_cadastro ASC, cliente_id ASC) AS ordem,
    collect_list(cliente_id) OVER (PARTITION BY cnpj)                                 AS todos_ids
  FROM base
)
SELECT
  cliente_id,
  cnpj,
  razao_social,
  segmento,
  cidade,
  uf,
  bairro,
  data_cadastro,
  ativo,
  array_except(todos_ids, array(cliente_id))                       AS cliente_ids_duplicados,
  current_timestamp()                                              AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.clientes)     AS _linhas_origem
FROM ranked
WHERE ordem = 1;

-- COMMAND ----------

COMMENT ON TABLE lakehouse_rotaperfume.silver.clientes IS
  'Silver de clientes: CNPJ normalizado para 14 dígitos, razão social padronizada, datas convertidas dos dois formatos e deduplicação por CNPJ (mantém o cadastro mais antigo). 3.040 → 3.000.';

COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.cnpj IS
  'CNPJ normalizado: trim + só dígitos + lpad(14). Mantido como texto para preservar zero à esquerda.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.razao_social IS
  'Padronizada com initcap e colapso de espaço duplo.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.data_cadastro IS
  'coalesce de try_to_date (ISO e dd/MM/yyyy). try_to_date porque ANSI mode aborta em data malformada.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.ativo IS
  'Convertido de S/N para boolean.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.clientes.cliente_ids_duplicados IS
  'IDs de cadastros duplicados descartados na dedup por CNPJ — os pedidos antigos ainda apontam para eles.';

-- COMMAND ----------

ALTER TABLE lakehouse_rotaperfume.silver.clientes
  ADD CONSTRAINT cnpj_14 CHECK (length(cnpj) = 14);

-- COMMAND ----------

ALTER TABLE lakehouse_rotaperfume.silver.clientes
  ADD CONSTRAINT data_cadastro_nn CHECK (data_cadastro IS NOT NULL);
