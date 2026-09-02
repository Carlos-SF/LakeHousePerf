-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold — dimensões conformadas
-- MAGIC
-- MAGIC Quatro dimensões, lendo **só da silver**. Conformadas = as mesmas chaves e
-- MAGIC atributos valem para todos os marts, então os números fecham igual em qualquer um.
-- MAGIC
-- MAGIC - **dim_cliente** — 1 linha por cliente (silver.clientes já deduplicada em 3.000),
-- MAGIC   com o histórico de compras agregado (primeiro/último pedido, total, receita,
-- MAGIC   dias sem comprar).
-- MAGIC - **dim_produto** — 1 linha por SKU, com custo e preço para o cálculo de margem no fato.
-- MAGIC - **dim_vendedor** — 1 linha por vendedor, com a meta que o mart comercial compara.
-- MAGIC - **dim_calendario** — 1 linha por dia dos 24 meses, com `mes_pico_setor` (abril,
-- MAGIC   junho e outubro), a sazonalidade do setor de perfumaria.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_cliente AS
WITH historico AS (
  SELECT
    cliente_id,
    MIN(data_pedido)      AS data_primeiro_pedido,
    MAX(data_pedido)      AS data_ultimo_pedido,
    COUNT(*)              AS total_pedidos,
    SUM(valor_liquido)    AS receita_acumulada
  FROM lakehouse_rotaperfume.silver.pedidos
  GROUP BY cliente_id
)
SELECT
  c.cliente_id,
  c.razao_social,
  c.segmento,
  c.cidade,
  c.uf,
  c.data_cadastro,
  h.data_primeiro_pedido,
  h.data_ultimo_pedido,
  COALESCE(h.total_pedidos, 0)                        AS total_pedidos,
  COALESCE(h.receita_acumulada, CAST(0.00 AS DECIMAL(18, 2))) AS receita_acumulada,
  datediff(current_date(), h.data_ultimo_pedido)      AS dias_sem_comprar,
  current_timestamp()                                 AS _processado_em
FROM lakehouse_rotaperfume.silver.clientes c
LEFT JOIN historico h ON h.cliente_id = c.cliente_id;

COMMENT ON TABLE lakehouse_rotaperfume.gold.dim_cliente IS
  'Dimensão cliente: uma linha por cliente (já deduplicado na silver) com o histórico de compras agregado.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_cliente.receita_acumulada IS
  'Soma do valor_liquido de todos os pedidos do cliente (cancelado conta zero). Receita histórica, não do período.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_cliente.dias_sem_comprar IS
  'Dias entre hoje e o último pedido. NULL para cliente que nunca comprou — sinaliza recência para retenção.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_produto AS
SELECT
  sku,
  descricao,
  marca,
  categoria,
  nota_olfativa,
  custo_unitario,
  preco_tabela,
  data_lancamento,
  (NOT ativo)          AS descontinuado,
  current_timestamp()  AS _processado_em
FROM lakehouse_rotaperfume.silver.produtos;

COMMENT ON TABLE lakehouse_rotaperfume.gold.dim_produto IS
  'Dimensão produto: uma linha por SKU, com custo e preço de tabela — a base do cálculo de margem no fato.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_produto.descontinuado IS
  'TRUE quando o produto não está mais ativo. Continua no catálogo para explicar vendas históricas do SKU.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_vendedor AS
SELECT
  vendedor_id,
  nome,
  regiao,
  uf,
  meta_mensal,
  ativo,
  current_timestamp()  AS _processado_em
FROM lakehouse_rotaperfume.silver.vendedores;

COMMENT ON TABLE lakehouse_rotaperfume.gold.dim_vendedor IS
  'Dimensão vendedor: uma linha por vendedor, com a meta mensal que o mart comercial usa para atingimento.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.dim_calendario AS
WITH limites AS (
  SELECT
    date_trunc('month', MIN(data_pedido)) AS ini,
    MAX(data_pedido)                      AS fim
  FROM lakehouse_rotaperfume.silver.pedidos
),
dias AS (
  SELECT explode(sequence(CAST(ini AS DATE), CAST(fim AS DATE), interval 1 day)) AS data
  FROM limites
)
SELECT
  data,
  year(data)   AS ano,
  month(data)  AS mes,
  CASE month(data)
    WHEN 1 THEN 'Janeiro'  WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
    WHEN 4 THEN 'Abril'    WHEN 5 THEN 'Maio'       WHEN 6 THEN 'Junho'
    WHEN 7 THEN 'Julho'    WHEN 8 THEN 'Agosto'     WHEN 9 THEN 'Setembro'
    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro'  WHEN 12 THEN 'Dezembro'
  END          AS nome_mes,
  quarter(data) AS trimestre,
  CASE dayofweek(data)
    WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Segunda' WHEN 3 THEN 'Terça'
    WHEN 4 THEN 'Quarta'  WHEN 5 THEN 'Quinta'  WHEN 6 THEN 'Sexta'
    WHEN 7 THEN 'Sábado'
  END          AS dia_semana,
  (month(data) IN (4, 6, 10)) AS mes_pico_setor,
  current_timestamp()         AS _processado_em
FROM dias;

COMMENT ON TABLE lakehouse_rotaperfume.gold.dim_calendario IS
  'Dimensão calendário: uma linha por dia do período com dados, com atributos de ano/mês/trimestre e o pico do setor.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.dim_calendario.mes_pico_setor IS
  'TRUE em abril, junho e outubro — os meses de pico de venda de perfumaria (Dia das Mães, Namorados, Dia das Crianças/Natal antecipado).';
