-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver — produtos e itens do pedido
-- MAGIC
-- MAGIC **produtos** primeiro (tipos certos, `data_lancamento` com `try_to_date`, `ativo` boolean),
-- MAGIC porque **itens_pedido** faz join nele para marcar SKU descontinuado.
-- MAGIC
-- MAGIC **itens_pedido — a decisão da noite:** quantidade negativa é **DEVOLUÇÃO**, não erro.
-- MAGIC Não se descarta a linha (descartar infla o faturamento em mais de um milhão) e não se
-- MAGIC deixa sem flag (poluiria toda soma). Sinaliza-se: `devolucao` (boolean) e
-- MAGIC `quantidade_abs` (int) — e a análise decide se quer o bruto ou o líquido.
-- MAGIC
-- MAGIC Contrato de itens_pedido: `quantidade_abs > 0`.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.produtos AS
SELECT
  sku,
  descricao,
  categoria,
  marca,
  nota_olfativa,
  CAST(preco_tabela AS DECIMAL(18, 2))                                                    AS preco_tabela,
  CAST(custo_unitario AS DECIMAL(18, 2))                                                  AS custo_unitario,
  unidade,
  (ativo = 'S')                                                                           AS ativo,
  coalesce(try_to_date(data_lancamento), try_to_date(data_lancamento, 'dd/MM/yyyy'))      AS data_lancamento,
  current_timestamp()                                                                     AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.produtos)                            AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.produtos;

-- COMMAND ----------

COMMENT ON TABLE lakehouse_rotaperfume.silver.produtos IS
  'Silver de produtos: preços tipados, data_lancamento convertida (pode ser nula na origem) e ativo em boolean.';

COMMENT ON COLUMN lakehouse_rotaperfume.silver.produtos.ativo IS
  'Convertido de S/N para boolean. É a base de sku_descontinuado em itens_pedido.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.produtos.data_lancamento IS
  'coalesce de try_to_date; vazio na origem vira NULL, sem abortar (ANSI mode).';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.itens_pedido AS
SELECT
  CAST(i.item_id AS BIGINT)                                                     AS item_id,
  CAST(i.pedido_id AS BIGINT)                                                   AS pedido_id,
  i.sku,
  CAST(i.quantidade AS INT)                                                     AS quantidade,
  CAST(i.preco_praticado AS DECIMAL(18, 2))                                     AS preco_praticado,
  CAST(i.desconto_pct AS DECIMAL(9, 4))                                         AS desconto_pct,
  CAST(i.valor_bruto AS DECIMAL(18, 2))                                         AS valor_bruto,
  (CAST(i.quantidade AS INT) < 0)                                              AS devolucao,
  abs(CAST(i.quantidade AS INT))                                               AS quantidade_abs,
  COALESCE(NOT p.ativo, false)                                                 AS sku_descontinuado,
  current_timestamp()                                                          AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.itens_pedido)             AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.itens_pedido i
LEFT JOIN lakehouse_rotaperfume.silver.produtos p
  ON i.sku = p.sku;

-- COMMAND ----------

COMMENT ON TABLE lakehouse_rotaperfume.silver.itens_pedido IS
  'Silver de itens: quantidade negativa preservada como DEVOLUÇÃO (com flag), tipos certos e marca de SKU descontinuado via join com produtos. Nenhuma linha descartada.';

COMMENT ON COLUMN lakehouse_rotaperfume.silver.itens_pedido.devolucao IS
  'quantidade < 0 é devolução, não erro. Sinalizada em vez de descartada, para não inflar o faturamento.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.itens_pedido.quantidade_abs IS
  'Módulo da quantidade — sempre positiva, base da constraint.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.itens_pedido.sku_descontinuado IS
  'TRUE quando o produto do SKU não está mais ativo (join com silver.produtos).';

-- COMMAND ----------

ALTER TABLE lakehouse_rotaperfume.silver.itens_pedido
  ADD CONSTRAINT quantidade_abs_pos CHECK (quantidade_abs > 0);
