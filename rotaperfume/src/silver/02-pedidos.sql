-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver — pedidos: tipado, com o cancelado zerado e o contrato
-- MAGIC
-- MAGIC - **data_pedido** em ISO e `dd/MM/yyyy` misturados → `coalesce` de dois `try_to_date`.
-- MAGIC - **valor_total** é texto → `CAST` para `DECIMAL(18,2)`.
-- MAGIC - **pedido cancelado** tem valor zerado sem flag clara → cria `cancelado` (boolean) a
-- MAGIC   partir do `status`, e `valor_liquido` = 0 quando cancelado, `valor_total` caso contrário.
-- MAGIC - **ano** e **mes** a partir da data.
-- MAGIC
-- MAGIC Contrato: `data_pedido IS NOT NULL` e `NOT cancelado OR valor_liquido = 0`.
-- MAGIC A regra intuitiva `valor_liquido >= 0` **falharia** em 135 pedidos com devolução
-- MAGIC (valor negativo legítimo). A regra certa é: **pedido cancelado tem que ter valor ZERO**.
-- MAGIC Nenhuma linha é descartada: cancelado fica, com flag — o faturamento não pode mudar.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.pedidos AS
WITH base AS (
  SELECT
    CAST(pedido_id AS BIGINT)                                                    AS pedido_id,
    CAST(cliente_id AS BIGINT)                                                   AS cliente_id,
    CAST(vendedor_id AS BIGINT)                                                  AS vendedor_id,
    coalesce(try_to_date(data_pedido), try_to_date(data_pedido, 'dd/MM/yyyy'))   AS data_pedido,
    canal,
    status,
    (status = 'Cancelado')                                                       AS cancelado,
    CAST(valor_total AS DECIMAL(18, 2))                                          AS valor_total
  FROM lakehouse_rotaperfume.bronze.pedidos
)
SELECT
  pedido_id,
  cliente_id,
  vendedor_id,
  data_pedido,
  canal,
  status,
  cancelado,
  valor_total,
  CASE WHEN cancelado THEN CAST(0.00 AS DECIMAL(18, 2)) ELSE valor_total END   AS valor_liquido,
  year(data_pedido)                                                            AS ano,
  month(data_pedido)                                                           AS mes,
  current_timestamp()                                                          AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.pedidos)                  AS _linhas_origem
FROM base;

-- COMMAND ----------

COMMENT ON TABLE lakehouse_rotaperfume.silver.pedidos IS
  'Silver de pedidos: data convertida dos dois formatos, valor_total tipado, cancelado sinalizado e valor_liquido zerado no cancelado. Nenhuma linha descartada — o faturamento não muda.';

COMMENT ON COLUMN lakehouse_rotaperfume.silver.pedidos.data_pedido IS
  'coalesce de try_to_date (ISO e dd/MM/yyyy). ANSI mode aborta em data malformada, por isso try_.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.pedidos.cancelado IS
  'Derivada de status = Cancelado — o cancelado vinha com valor zerado sem flag clara.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.pedidos.valor_liquido IS
  'Zero quando cancelado, valor_total caso contrário. Some com o faturamento da bronze (status <> Cancelado).';

-- COMMAND ----------

ALTER TABLE lakehouse_rotaperfume.silver.pedidos
  ADD CONSTRAINT data_pedido_nn CHECK (data_pedido IS NOT NULL);

-- COMMAND ----------

-- A regra certa NÃO é valor_liquido >= 0 (135 pedidos têm valor negativo legítimo, com
-- devolução). É: pedido cancelado tem que ter valor ZERO.
ALTER TABLE lakehouse_rotaperfume.silver.pedidos
  ADD CONSTRAINT pedido_cancelado_zerado CHECK (NOT cancelado OR valor_liquido = 0);
