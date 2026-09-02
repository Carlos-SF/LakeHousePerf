-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold — marts por diretoria
-- MAGIC
-- MAGIC Um mart por consumidor, o que muda entre eles é a **dimensão dominante** e as
-- MAGIC **métricas** — nunca a tabela base. Os dois de venda saem do MESMO `fato_vendas`
-- MAGIC (por isso somam o mesmo R$ 102.303.828,05: é o significado de "conformado").
-- MAGIC
-- MAGIC - **mart_vendas_por_vendedor** — comercial: vendedor × mês, com atingimento de meta.
-- MAGIC - **mart_produto_performance** — produto: SKU × mês, com margem % e curva ABC.
-- MAGIC - **mart_financeiro_recebimento** — financeiro: mês de vencimento. Sai de
-- MAGIC   `silver.pagamentos` (recebimento é conceito de pagamento, não está no fato de venda).

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.mart_vendas_por_vendedor AS
SELECT
  f.vendedor_id,
  v.nome,
  f.ano,
  f.mes,
  ROUND(SUM(f.receita), 2)                                           AS receita,
  ROUND(SUM(f.margem), 2)                                            AS margem,
  v.meta_mensal                                                      AS meta,
  ROUND(SUM(f.receita) / NULLIF(v.meta_mensal, 0), 4)               AS atingimento,
  COUNT(DISTINCT f.cliente_id)                                       AS clientes_atendidos,
  ROUND(SUM(f.receita) / NULLIF(COUNT(DISTINCT f.pedido_id), 0), 2) AS ticket_medio,
  current_timestamp()                                                AS _processado_em
FROM lakehouse_rotaperfume.gold.fato_vendas f
JOIN lakehouse_rotaperfume.gold.dim_vendedor v ON v.vendedor_id = f.vendedor_id
GROUP BY f.vendedor_id, v.nome, f.ano, f.mes, v.meta_mensal;

COMMENT ON TABLE lakehouse_rotaperfume.gold.mart_vendas_por_vendedor IS
  'Mart comercial: receita e margem por vendedor e mês, com atingimento de meta, clientes atendidos e ticket médio. Some o mesmo total do fato.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_vendas_por_vendedor.atingimento IS
  'Receita do mês dividida pela meta mensal. 1,0 = bateu a meta; abaixo de 1,0 = ficou aquém.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_vendas_por_vendedor.ticket_medio IS
  'Receita dividida por número de pedidos distintos — valor médio por pedido, não por item.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.mart_produto_performance AS
WITH por_sku_mes AS (
  SELECT
    sku, marca, categoria, ano, mes,
    ROUND(SUM(receita), 2) AS receita,
    ROUND(SUM(margem), 2)  AS margem,
    SUM(quantidade)        AS quantidade
  FROM lakehouse_rotaperfume.gold.fato_vendas
  GROUP BY sku, marca, categoria, ano, mes
),
receita_por_sku AS (
  SELECT sku, SUM(receita) AS receita_sku
  FROM lakehouse_rotaperfume.gold.fato_vendas
  GROUP BY sku
),
abc AS (
  SELECT
    sku,
    -- fração da receita acumulada (do maior para o menor) sobre o total
    SUM(receita_sku) OVER (ORDER BY receita_sku DESC, sku)
      / SUM(receita_sku) OVER ()  AS receita_acum_pct
  FROM receita_por_sku
),
classe AS (
  SELECT
    sku,
    CASE WHEN receita_acum_pct <= 0.80 THEN 'A'
         WHEN receita_acum_pct <= 0.95 THEN 'B'
         ELSE 'C' END AS curva_abc
  FROM abc
)
SELECT
  m.sku,
  m.marca,
  m.categoria,
  m.ano,
  m.mes,
  m.receita,
  m.margem,
  ROUND(100 * m.margem / NULLIF(m.receita, 0), 1) AS margem_pct,
  m.quantidade,
  cl.curva_abc,
  current_timestamp()                             AS _processado_em
FROM por_sku_mes m
JOIN classe cl ON cl.sku = m.sku;

COMMENT ON TABLE lakehouse_rotaperfume.gold.mart_produto_performance IS
  'Mart de produto: receita, margem e margem % por SKU e mês, com a curva ABC do SKU (por receita acumulada). Some o mesmo total do fato.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_produto_performance.curva_abc IS
  'Classe ABC do SKU por receita acumulada: A = top 80% da receita, B = próximos 15%, C = últimos 5%. Propriedade do SKU, repetida em cada mês.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_produto_performance.margem_pct IS
  'Margem sobre receita, em %. Kit Presente ~33% (pior), Óleo Concentrado ~50% (melhor).';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.mart_financeiro_recebimento AS
SELECT
  year(data_vencimento)   AS ano_vencimento,
  month(data_vencimento)  AS mes_vencimento,
  ROUND(SUM(valor_liquido), 2)                                                            AS valor_a_receber,
  ROUND(SUM(valor_liquido) FILTER (WHERE data_pagamento IS NOT NULL), 2)                  AS recebido,
  ROUND(AVG(datediff(data_pagamento, data_vencimento)) FILTER (WHERE data_pagamento IS NOT NULL), 1) AS atraso_medio_dias,
  ROUND(SUM(valor - valor_liquido), 2)                                                    AS custo_taxa,
  current_timestamp()                                                                     AS _processado_em
FROM lakehouse_rotaperfume.silver.pagamentos
GROUP BY year(data_vencimento), month(data_vencimento);

COMMENT ON TABLE lakehouse_rotaperfume.gold.mart_financeiro_recebimento IS
  'Mart financeiro: por mês de vencimento, quanto há a receber, quanto foi recebido, o atraso médio e o custo de taxa. Sai de silver.pagamentos.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_financeiro_recebimento.atraso_medio_dias IS
  'Média de dias entre vencimento e pagamento, só sobre pagamentos já feitos. Negativo = pago adiantado.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.mart_financeiro_recebimento.custo_taxa IS
  'Valor retido em taxas (valor bruto menos valor líquido) — o custo financeiro do meio de pagamento.';
