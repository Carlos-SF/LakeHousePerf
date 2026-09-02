-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold — fato_vendas
-- MAGIC
-- MAGIC ## O CONTRATO (escrito antes do SQL)
-- MAGIC
-- MAGIC - **Granularidade:** uma linha por **item de pedido**.
-- MAGIC - **Filtro:** exclui pedidos cancelados (`NOT p.cancelado`). **NÃO** exclui devolução.
-- MAGIC - **Dimensões:** data_pedido, ano, mes, canal, cliente_id, razao_social, segmento,
-- MAGIC   cidade, vendedor_id, sku, categoria, marca, nota_olfativa (+ chaves pedido_id, item_id).
-- MAGIC - **Métricas:** quantidade, preco_praticado, receita, custo, margem, devolucao.
-- MAGIC   - `receita = quantidade * preco_praticado`
-- MAGIC   - `custo   = quantidade * custo_unitario`
-- MAGIC   - `margem  = receita - custo`
-- MAGIC - **Devolução** entra com quantidade e receita **NEGATIVAS**, com a flag `devolucao`.
-- MAGIC - Particionado por `ano, mes`.
-- MAGIC
-- MAGIC **Por que a devolução fica dentro:** se ficar de fora, a gold soma R$ 103,6 mi e a
-- MAGIC silver R$ 102,3 mi — R$ 1,26 mi de diferença entre duas camadas do mesmo pipeline.
-- MAGIC Quem quiser o bruto pede `SUM(receita) FILTER (WHERE NOT devolucao)`.
-- MAGIC
-- MAGIC Lê da silver e das dimensões (dim_produto p/ custo/categoria, dim_cliente p/ razão/
-- MAGIC segmento/cidade). Os dois JOIN são completos, então o grão fica em 191.080 linhas.

-- COMMAND ----------

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.fato_vendas
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
  i.item_id,
  i.pedido_id,
  p.data_pedido,
  p.canal,
  p.cliente_id,
  c.razao_social,
  c.segmento,
  c.cidade,
  p.vendedor_id,
  i.sku,
  pr.categoria,
  pr.marca,
  pr.nota_olfativa,
  i.quantidade,
  i.preco_praticado,
  CAST(i.quantidade * i.preco_praticado AS DECIMAL(18, 2))                          AS receita,
  CAST(i.quantidade * pr.custo_unitario AS DECIMAL(18, 2))                          AS custo,
  CAST(i.quantidade * i.preco_praticado - i.quantidade * pr.custo_unitario AS DECIMAL(18, 2)) AS margem,
  i.devolucao,
  p.ano,
  p.mes
FROM lakehouse_rotaperfume.silver.itens_pedido i
JOIN lakehouse_rotaperfume.silver.pedidos   p  ON p.pedido_id = i.pedido_id
JOIN lakehouse_rotaperfume.gold.dim_produto pr ON pr.sku = i.sku
JOIN lakehouse_rotaperfume.gold.dim_cliente c  ON c.cliente_id = p.cliente_id
WHERE NOT p.cancelado;

-- COMMAND ----------

COMMENT ON TABLE lakehouse_rotaperfume.gold.fato_vendas IS
  'Fato de vendas, grão de item de pedido. Exclui pedidos cancelados; mantém devolução com quantidade e receita negativas. Soma a mesma receita da silver (R$ 102.303.828,05).';

COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.item_id IS 'Chave do item de pedido — o grão da tabela.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.pedido_id IS 'Pedido a que o item pertence. Referencia silver.pedidos.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.data_pedido IS 'Data do pedido (não a da entrega nem a do pagamento).';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.canal IS 'Canal de venda do pedido (Visita, WhatsApp, etc.).';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.cliente_id IS 'Cliente comprador (id já deduplicado da silver).';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.razao_social IS 'Razão social do cliente, desnormalizada da dimensão para consulta direta.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.segmento IS 'Segmento comercial do cliente.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.cidade IS 'Cidade do cliente.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.vendedor_id IS 'Vendedor responsável pelo pedido.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.sku IS 'Produto vendido.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.categoria IS 'Categoria do produto, desnormalizada da dimensão.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.marca IS 'Marca do produto, desnormalizada da dimensão.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.nota_olfativa IS 'Nota olfativa do produto, desnormalizada da dimensão.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.quantidade IS 'Quantidade vendida. Negativa quando é devolução — de propósito, para somar corretamente.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.preco_praticado IS 'Preço unitário efetivamente cobrado (já com desconto comercial aplicado na silver).';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.receita IS 'Quantidade x preço praticado. Negativa na devolução. Some com a silver por construção.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.custo IS 'Quantidade x custo unitário do produto. Negativo na devolução (o custo volta).';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.margem IS 'Receita menos custo do produto. Não considera desconto comercial nem frete.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fato_vendas.devolucao IS 'TRUE quando a linha é uma devolução (quantidade negativa). Permite separar bruto de líquido na análise.';
