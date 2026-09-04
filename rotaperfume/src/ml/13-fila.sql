-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold — a fila da semana e as 4 ferramentas do agente
-- MAGIC
-- MAGIC Score não é decisão. Este é o último metro: transforma `gold.score_propensao` numa
-- MAGIC **fila de 200 ligações** com nome, motivo em português e o que oferecer, e cria as
-- MAGIC quatro funções SQL que o agente/Genie consulta.
-- MAGIC
-- MAGIC **A ordem das operações importa** (o erro mais fácil aqui):
-- MAGIC 1. junta a carteira e **descarta** quem não é elegível (sem carteira vigente, ou
-- MAGIC    vendedor desligado — `orfao_vendedor_desligado`);
-- MAGIC 2. `ORDER BY score DESC LIMIT 200`;
-- MAGIC 3. `ROW_NUMBER() OVER (PARTITION BY vendedor ORDER BY score DESC)`.
-- MAGIC
-- MAGIC Se o descarte vier **depois** do LIMIT, a fila sai com ~172 linhas (seis vendedores
-- MAGIC desligados levam junto os clientes deles) e o teste 1 quebra o job. A fila é global;
-- MAGIC a capacidade é que é por pessoa — por isso `LIMIT 200`, não cota igual por vendedor.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.fila_semanal AS
WITH carteira_vigente AS (
  -- 1º: uma carteira vigente por cliente; descarta órfão de vendedor desligado.
  SELECT cliente_id, vendedor_id
  FROM (
    SELECT cliente_id, vendedor_id,
           ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY vendedor_id) AS rn
    FROM lakehouse_rotaperfume.silver.carteira
    WHERE vigente = true AND NOT orfao_vendedor_desligado
  ) WHERE rn = 1
),
elegiveis AS (
  SELECT s.cliente_id, s.score, s.faixa,
         v.nome  AS vendedor,
         c.razao_social, c.cidade, c.uf,
         f.ticket_medio, f.valor_total, f.atraso_relativo,
         f.intervalo_medio_dias, f.recencia_dias, f.comprou_lancamento
  FROM lakehouse_rotaperfume.gold.score_propensao s
  JOIN carteira_vigente cv                        ON cv.cliente_id = s.cliente_id
  JOIN lakehouse_rotaperfume.silver.vendedores v  ON v.vendedor_id = cv.vendedor_id
  JOIN lakehouse_rotaperfume.gold.dim_cliente c   ON c.cliente_id  = s.cliente_id
  JOIN lakehouse_rotaperfume.gold.features_cliente f ON f.cliente_id = s.cliente_id
),
top200 AS (
  -- 2º: a fila é global.
  SELECT * FROM elegiveis ORDER BY score DESC LIMIT 200
),
-- Peças da sugestão -------------------------------------------------------------
marca_pref AS (
  -- marca preferida do cliente (mais receita, até o corte)
  SELECT cliente_id, marca FROM (
    SELECT cliente_id, marca,
           ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY SUM(receita) DESC) AS rn
    FROM lakehouse_rotaperfume.gold.fato_vendas
    WHERE data_pedido < DATE'2026-08-31'
    GROUP BY cliente_id, marca
  ) WHERE rn = 1
),
comprou_90d AS (
  -- SKUs que o cliente comprou nos últimos 90 dias antes do corte (para NÃO sugerir)
  SELECT DISTINCT cliente_id, sku
  FROM lakehouse_rotaperfume.gold.fato_vendas
  WHERE data_pedido >= date_sub(DATE'2026-08-31', 90) AND data_pedido < DATE'2026-08-31'
),
sku_candidato AS (
  -- o SKU mais comprado na marca preferida que ele NÃO comprou nos últimos 90 dias
  SELECT cliente_id, sku FROM (
    SELECT fv.cliente_id, fv.sku,
           ROW_NUMBER() OVER (PARTITION BY fv.cliente_id ORDER BY SUM(fv.quantidade) DESC) AS rn
    FROM lakehouse_rotaperfume.gold.fato_vendas fv
    JOIN marca_pref mp ON mp.cliente_id = fv.cliente_id AND mp.marca = fv.marca
    WHERE fv.data_pedido < DATE'2026-08-31'
      AND NOT EXISTS (SELECT 1 FROM comprou_90d c
                      WHERE c.cliente_id = fv.cliente_id AND c.sku = fv.sku)
    GROUP BY fv.cliente_id, fv.sku
  ) WHERE rn = 1
),
estoque_atual AS (
  -- snapshot mais recente por SKU (a tabela é um snapshot semanal)
  SELECT sku, saldo, ruptura FROM (
    SELECT sku, saldo, ruptura,
           ROW_NUMBER() OVER (PARTITION BY sku ORDER BY data_snapshot DESC) AS rn
    FROM lakehouse_rotaperfume.silver.estoque
  ) WHERE rn = 1
),
sugestao_cliente AS (
  SELECT sc.cliente_id,
         concat('Oferecer ', sc.sku, ' (',
                COALESCE(CAST(ea.saldo AS STRING), 'sem info'),
                CASE WHEN ea.ruptura THEN ' — EM RUPTURA' ELSE ' em estoque' END, ').') AS sugestao
  FROM sku_candidato sc
  LEFT JOIN estoque_atual ea ON ea.sku = sc.sku
)
-- 3º: numera dentro de cada vendedor + escreve motivo e sugestão --------------
SELECT
  n.vendedor,
  ROW_NUMBER() OVER (PARTITION BY n.vendedor ORDER BY n.score DESC) AS ordem,
  n.cliente_id,
  n.razao_social,
  n.cidade,
  n.uf,
  n.score,
  n.faixa,
  n.ticket_medio,
  -- motivo: do sinal mais RARO ao mais comum, senão o comum come todos.
  CASE
    WHEN n.atraso_relativo > 3 THEN
      concat('Compra a cada ', FORMAT_NUMBER(n.intervalo_medio_dias, 0),
             ' dias e está há ', FORMAT_NUMBER(n.recencia_dias, 0),
             ' sem pedido. Risco de perder para o concorrente.')
    WHEN n.atraso_relativo > 1.5 THEN
      concat('Está ', FORMAT_NUMBER(n.atraso_relativo, 1), ' vezes mais atrasado que o ritmo dele.')
    WHEN n.comprou_lancamento = 1 THEN
      'Comprou lançamento recente. Alta chance de repetir.'
    WHEN n.valor_total >= (SELECT PERCENTILE(valor_total, 0.9)
                           FROM lakehouse_rotaperfume.gold.features_cliente) THEN
      concat('Cliente grande, R$ ', FORMAT_NUMBER(n.valor_total, 2), ' no ano. Manter próximo.')
    ELSE 'Dentro do ritmo. Contato de manutenção.'
  END AS motivo,
  COALESCE(sg.sugestao, 'Sem recompra óbvia no momento.') AS sugestao
FROM top200 n
LEFT JOIN sugestao_cliente sg ON sg.cliente_id = n.cliente_id;

-- COMMAND ----------

-- COMMENT na tabela (a auditoria de metadado quebra o job sem ele) e em TODAS as colunas
-- (é o comentário de coluna que o Genie lê para responder sem inventar).
COMMENT ON TABLE lakehouse_rotaperfume.gold.fila_semanal IS
  'A fila de 200 ligações da semana: os 200 clientes elegíveis de maior score (score_propensao), com carteira vigente (vendedor ativo), numerados por vendedor. Uma linha por ligação a fazer.';

ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN vendedor      COMMENT 'Vendedor responsável pela ligação (nome, via carteira vigente).';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN ordem         COMMENT 'Ordem de prioridade dentro da fila do vendedor (1 = ligue primeiro).';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN cliente_id    COMMENT 'Cliente a ser contatado.';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN razao_social  COMMENT 'Razão social do cliente.';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN cidade        COMMENT 'Cidade do cliente.';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN uf            COMMENT 'UF do cliente.';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN score         COMMENT 'Score de propensão a comprar em 7 dias (0 a 1), do modelo propensao_compra.';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN faixa         COMMENT 'Faixa do score: Fria, Morna, Quente, Muito quente.';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN ticket_medio  COMMENT 'Ticket médio histórico do cliente (receita por pedido).';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN motivo        COMMENT 'Motivo em português da ligação, montado sobre as features do cliente (por que ele está na fila).';
ALTER TABLE lakehouse_rotaperfume.gold.fila_semanal ALTER COLUMN sugestao      COMMENT 'O que oferecer: SKU mais comprado na marca preferida que ele não compra há 90 dias, com o saldo do estoque mais recente.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## As quatro ferramentas — funções SQL no Unity Catalog
-- MAGIC
-- MAGIC Cada uma com `COMMENT` em português: é o comentário que diz ao agente **quando** usar.
-- MAGIC Parâmetros prefixados com `p_` (parâmetro com nome de coluna fica ambíguo no corpo).
-- MAGIC `LIMIT p_quantos` é proibido (exige constante) — filtra-se por `ordem <= p_quantos`.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.priorizar_carteira(p_vendedor STRING, p_quantos INT)
RETURNS TABLE
COMMENT 'Retorna os próximos p_quantos clientes da fila da semana do vendedor p_vendedor, em ordem de prioridade (com motivo e sugestão). Use quando o vendedor pergunta quem ligar/priorizar.'
RETURN
  SELECT ordem, cliente_id, razao_social, cidade, ROUND(score, 4) AS score, faixa, motivo, sugestao
  FROM lakehouse_rotaperfume.gold.fila_semanal
  WHERE vendedor = p_vendedor AND ordem <= p_quantos
  ORDER BY ordem;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.contexto_cliente(p_cliente_id INT)
RETURNS TABLE
COMMENT 'Retorna o contexto de um cliente: razão social, cidade, total de pedidos, receita acumulada, ticket médio, marca preferida e data da última compra. Use para explicar quem é o cliente antes de ligar.'
RETURN
  SELECT c.razao_social, c.cidade, c.uf,
         c.total_pedidos, c.receita_acumulada,
         f.ticket_medio,
         mp.marca                AS marca_preferida,
         c.data_ultimo_pedido    AS ultima_compra
  FROM lakehouse_rotaperfume.gold.dim_cliente c
  JOIN lakehouse_rotaperfume.gold.features_cliente f ON f.cliente_id = c.cliente_id
  LEFT JOIN (
    SELECT cliente_id, marca FROM (
      SELECT cliente_id, marca,
             ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY SUM(receita) DESC) AS rn
      FROM lakehouse_rotaperfume.gold.fato_vendas
      GROUP BY cliente_id, marca) WHERE rn = 1
  ) mp ON mp.cliente_id = c.cliente_id
  WHERE c.cliente_id = p_cliente_id;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.sugerir_produtos(p_cliente_id INT)
RETURNS TABLE
COMMENT 'Retorna os SKUs que o cliente comprava mas parou nos últimos 90 dias, com quantidade histórica e saldo de estoque atual. Use para sugerir o que oferecer/repor.'
RETURN
  SELECT fv.sku, fv.marca,
         SUM(fv.quantidade) AS qtd_historica,
         e.saldo, e.ruptura
  FROM lakehouse_rotaperfume.gold.fato_vendas fv
  LEFT JOIN (
    SELECT sku, saldo, ruptura FROM (
      SELECT sku, saldo, ruptura,
             ROW_NUMBER() OVER (PARTITION BY sku ORDER BY data_snapshot DESC) AS rn
      FROM lakehouse_rotaperfume.silver.estoque) WHERE rn = 1
  ) e ON e.sku = fv.sku
  LEFT JOIN (
    -- SKUs comprados nos últimos 90 dias (todos os clientes), para anti-join
    SELECT DISTINCT cliente_id, sku
    FROM lakehouse_rotaperfume.gold.fato_vendas
    WHERE data_pedido >= date_sub(DATE'2026-08-31', 90) AND data_pedido < DATE'2026-08-31'
  ) recente ON recente.cliente_id = fv.cliente_id AND recente.sku = fv.sku
  WHERE fv.cliente_id = p_cliente_id
    AND fv.data_pedido < DATE'2026-08-31'
    AND recente.sku IS NULL          -- não comprado nos últimos 90 dias
  GROUP BY fv.sku, fv.marca, e.saldo, e.ruptura
  ORDER BY qtd_historica DESC;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.checar_disponibilidade(p_sku STRING)
RETURNS TABLE
COMMENT 'Retorna saldo e ruptura de um SKU no snapshot de estoque mais recente. Use para checar disponibilidade antes de prometer entrega.'
RETURN
  SELECT sku, data_snapshot, saldo, ruptura
  FROM lakehouse_rotaperfume.silver.estoque
  WHERE sku = p_sku
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY data_snapshot DESC) = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Três testes que quebram o job (padrão raise_error da noite 2)

-- COMMAND ----------

-- Teste 1 — a fila tem exatamente 200 linhas (o descarte veio ANTES do LIMIT).
SELECT '1. fila tem 200 linhas' AS teste,
       linhas AS calculado, 200 AS esperado,
       CASE WHEN linhas = 200 THEN 'PASSOU'
            ELSE raise_error(concat('fila_semanal tem ', CAST(linhas AS STRING),
                 ' linhas, esperado 200 — descarte de vendedor desligado rodou depois do LIMIT?')) END AS resultado
FROM (SELECT COUNT(*) AS linhas FROM lakehouse_rotaperfume.gold.fila_semanal);

-- COMMAND ----------

-- Teste 2 — nenhum motivo nulo ou vazio (o ELSE do CASE cobriu todo mundo).
SELECT '2. nenhum motivo nulo ou vazio' AS teste,
       sem_motivo AS calculado, 0 AS esperado,
       CASE WHEN sem_motivo = 0 THEN 'PASSOU'
            ELSE raise_error(concat(CAST(sem_motivo AS STRING), ' linhas com motivo nulo/vazio')) END AS resultado
FROM (SELECT COUNT(*) FILTER (WHERE motivo IS NULL OR trim(motivo) = '') AS sem_motivo
      FROM lakehouse_rotaperfume.gold.fila_semanal);

-- COMMAND ----------

-- Teste 3 — nenhum score fora de [0, 1].
SELECT '3. score dentro de [0,1]' AS teste,
       fora AS calculado, 0 AS esperado,
       CASE WHEN fora = 0 THEN 'PASSOU'
            ELSE raise_error(concat(CAST(fora AS STRING), ' linhas com score fora de [0,1]')) END AS resultado
FROM (SELECT COUNT(*) FILTER (WHERE score < 0 OR score > 1) AS fora
      FROM lakehouse_rotaperfume.gold.fila_semanal);
