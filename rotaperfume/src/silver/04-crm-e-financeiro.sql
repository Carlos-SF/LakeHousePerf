-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver — CRM e financeiro (vendedores, carteira, oportunidades, visitas, pagamentos, estoque)
-- MAGIC
-- MAGIC Seis tabelas de apoio, limpas e tipadas. Duas decisões de negócio:
-- MAGIC
-- MAGIC - **carteira**: existe vendedor **desligado** com carteira ainda vigente. Não se
-- MAGIC   conserta o dado — cria-se `vigente` (respeita `data_fim` **e** `data_desligamento`)
-- MAGIC   e `orfao_vendedor_desligado`, que **expõe** o problema para o gestor.
-- MAGIC - **oportunidades**: as etapas na origem são `'Fechado ganho'` e `'Fechado perdido'`
-- MAGIC   (não 'Ganha'/'Perdida'). O CASE respeita o valor real da origem.
-- MAGIC - **estoque**: `ruptura` é **recalculada** de `saldo = 0` (não confia no texto de origem).
-- MAGIC
-- MAGIC `vendedores` é criada primeiro porque `carteira` faz join nela.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.vendedores AS
SELECT
  CAST(vendedor_id AS BIGINT)                                                          AS vendedor_id,
  nome,
  regiao,
  uf,
  coalesce(try_to_date(data_admissao), try_to_date(data_admissao, 'dd/MM/yyyy'))       AS data_admissao,
  coalesce(try_to_date(data_desligamento), try_to_date(data_desligamento, 'dd/MM/yyyy')) AS data_desligamento,
  CAST(meta_mensal AS DECIMAL(18, 2))                                                  AS meta_mensal,
  (data_desligamento IS NULL OR trim(data_desligamento) = '')                          AS ativo,
  current_timestamp()                                                                  AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.vendedores)                       AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.vendedores;

COMMENT ON TABLE lakehouse_rotaperfume.silver.vendedores IS
  'Silver de vendedores: datas convertidas e ativo (sem data_desligamento). Base da carteira órfã.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.vendedores.ativo IS
  'TRUE quando não há data_desligamento — o vendedor ainda está na empresa.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.carteira AS
WITH c AS (
  SELECT
    CAST(carteira_id AS BIGINT)                                             AS carteira_id,
    CAST(cliente_id AS BIGINT)                                              AS cliente_id,
    CAST(vendedor_id AS BIGINT)                                             AS vendedor_id,
    coalesce(try_to_date(data_inicio), try_to_date(data_inicio, 'dd/MM/yyyy')) AS data_inicio,
    coalesce(try_to_date(data_fim), try_to_date(data_fim, 'dd/MM/yyyy'))    AS data_fim
  FROM lakehouse_rotaperfume.bronze.carteira
)
SELECT
  c.carteira_id,
  c.cliente_id,
  c.vendedor_id,
  c.data_inicio,
  c.data_fim,
  -- carteira aberta (data_fim não passou) E vendedor ainda na empresa
  (c.data_fim IS NULL AND v.data_desligamento IS NULL)                     AS vigente,
  -- EXPÕE o problema: carteira aberta, mas o vendedor foi desligado
  (c.data_fim IS NULL AND v.data_desligamento IS NOT NULL)                 AS orfao_vendedor_desligado,
  current_timestamp()                                                      AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.carteira)             AS _linhas_origem
FROM c
LEFT JOIN lakehouse_rotaperfume.silver.vendedores v
  ON c.vendedor_id = v.vendedor_id;

COMMENT ON TABLE lakehouse_rotaperfume.silver.carteira IS
  'Silver de carteira: vigente respeita data_fim e o desligamento do vendedor; orfao_vendedor_desligado expõe carteira aberta de vendedor já desligado. O dado não é consertado, é sinalizado.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.carteira.vigente IS
  'Carteira sem data_fim E vendedor sem data_desligamento.';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.carteira.orfao_vendedor_desligado IS
  'Carteira sem data_fim, mas vendedor com data_desligamento — o problema exposto ao gestor.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.oportunidades AS
SELECT
  CAST(oportunidade_id AS BIGINT)                                                       AS oportunidade_id,
  CAST(cliente_id AS BIGINT)                                                            AS cliente_id,
  CAST(vendedor_id AS BIGINT)                                                           AS vendedor_id,
  origem,
  coalesce(try_to_date(data_abertura), try_to_date(data_abertura, 'dd/MM/yyyy'))        AS data_abertura,
  etapa,
  -- as etapas reais da origem são 'Fechado ganho' e 'Fechado perdido'
  (etapa = 'Fechado ganho')                                                             AS ganha,
  (etapa = 'Fechado perdido')                                                           AS perdida,
  CAST(probabilidade_pct AS INT)                                                        AS probabilidade_pct,
  CAST(valor_estimado AS DECIMAL(18, 2))                                                AS valor_estimado,
  coalesce(try_to_date(data_fechamento), try_to_date(data_fechamento, 'dd/MM/yyyy'))    AS data_fechamento,
  CAST(ciclo_dias AS INT)                                                               AS ciclo_dias,
  motivo_perda,
  current_timestamp()                                                                   AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.oportunidades)                     AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.oportunidades;

COMMENT ON TABLE lakehouse_rotaperfume.silver.oportunidades IS
  'Silver de oportunidades: datas/valores tipados e ganha/perdida derivadas das etapas reais da origem (Fechado ganho / Fechado perdido).';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.oportunidades.ganha IS
  'Derivada de etapa = Fechado ganho (o valor real da origem, não Ganha).';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.oportunidades.perdida IS
  'Derivada de etapa = Fechado perdido (o valor real da origem, não Perdida).';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.visitas AS
SELECT
  CAST(visita_id AS BIGINT)                                                     AS visita_id,
  CAST(cliente_id AS BIGINT)                                                    AS cliente_id,
  CAST(vendedor_id AS BIGINT)                                                   AS vendedor_id,
  coalesce(try_to_date(data_visita), try_to_date(data_visita, 'dd/MM/yyyy'))    AS data_visita,
  resultado,
  CAST(duracao_min AS INT)                                                      AS duracao_min,
  current_timestamp()                                                          AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.visitas)                  AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.visitas;

COMMENT ON TABLE lakehouse_rotaperfume.silver.visitas IS
  'Silver de visitas: data convertida e duração tipada.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.pagamentos AS
SELECT
  CAST(pagamento_id AS BIGINT)                                                          AS pagamento_id,
  CAST(pedido_id AS BIGINT)                                                             AS pedido_id,
  forma_pagamento,
  CAST(parcelas AS INT)                                                                 AS parcelas,
  CAST(valor AS DECIMAL(18, 2))                                                         AS valor,
  CAST(taxa_pct AS DECIMAL(9, 4))                                                       AS taxa_pct,
  CAST(valor_liquido AS DECIMAL(18, 2))                                                 AS valor_liquido,
  coalesce(try_to_date(data_vencimento), try_to_date(data_vencimento, 'dd/MM/yyyy'))    AS data_vencimento,
  coalesce(try_to_date(data_pagamento), try_to_date(data_pagamento, 'dd/MM/yyyy'))      AS data_pagamento,
  status_pagamento,
  current_timestamp()                                                                   AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.pagamentos)                        AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.pagamentos;

COMMENT ON TABLE lakehouse_rotaperfume.silver.pagamentos IS
  'Silver de pagamentos: valores tipados e datas de vencimento/pagamento convertidas.';

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.silver.estoque AS
SELECT
  coalesce(try_to_date(data_snapshot), try_to_date(data_snapshot, 'dd/MM/yyyy'))   AS data_snapshot,
  sku,
  CAST(saldo AS INT)                                                               AS saldo,
  (CAST(saldo AS INT) = 0)                                                         AS ruptura,
  current_timestamp()                                                              AS _processado_em,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.bronze.estoque)                      AS _linhas_origem
FROM lakehouse_rotaperfume.bronze.estoque;

COMMENT ON TABLE lakehouse_rotaperfume.silver.estoque IS
  'Silver de estoque: saldo tipado e ruptura recalculada de saldo = 0 (não confia no texto de origem).';
COMMENT ON COLUMN lakehouse_rotaperfume.silver.estoque.ruptura IS
  'Recalculada como saldo = 0 — boolean derivado do número, não do texto original.';
