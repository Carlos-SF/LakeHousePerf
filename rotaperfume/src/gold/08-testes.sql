-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold — os 9 testes que quebram o pipeline
-- MAGIC
-- MAGIC Teste que não quebra o job não é teste, é relatório. Cada célula levanta
-- MAGIC `raise_error()` quando falha — e como `raise_error` tem tipo NOTHING, ele fica
-- MAGIC **dentro** de `CASE WHEN ... THEN 'PASSOU' ELSE raise_error(...) END`. Falhou →
-- MAGIC `USER_RAISED_EXCEPTION` → a tarefa `testes` fica vermelha e **nada depois roda**.
-- MAGIC O dashboard fica com o dado de ontem — de longe melhor que o dado errado de hoje.
-- MAGIC
-- MAGIC Se um teste falhar: corrija a **transformação**, nunca o teste.

-- COMMAND ----------

-- Teste 1 — o que mais importa: a limpeza NÃO PODE mudar o faturamento.
SELECT '1. receita gold = receita silver' AS teste,
       gold_r AS calculado, silver_r AS esperado,
       CASE WHEN abs(gold_r - silver_r) <= 0.01 THEN 'PASSOU'
            ELSE raise_error(concat('receita da gold ', CAST(gold_r AS STRING),
                                    ' != silver ', CAST(silver_r AS STRING))) END AS resultado
FROM (
  SELECT (SELECT ROUND(SUM(receita), 2)       FROM lakehouse_rotaperfume.gold.fato_vendas) AS gold_r,
         (SELECT ROUND(SUM(valor_liquido), 2) FROM lakehouse_rotaperfume.silver.pedidos)   AS silver_r
);

-- COMMAND ----------

-- Teste 2 — CNPJ único na silver.clientes (a dedup segurou).
SELECT '2. cnpj unico em silver.clientes' AS teste,
       distintos AS calculado, total AS esperado,
       CASE WHEN total = distintos THEN 'PASSOU'
            ELSE raise_error(concat('clientes=', CAST(total AS STRING),
                                    ' cnpj distintos=', CAST(distintos AS STRING))) END AS resultado
FROM (
  SELECT COUNT(*) AS total, COUNT(DISTINCT cnpj) AS distintos
  FROM lakehouse_rotaperfume.silver.clientes
);

-- COMMAND ----------

-- Teste 3 — nenhuma data_pedido nula na silver.pedidos.
SELECT '3. nenhuma data_pedido nula' AS teste,
       nulas AS calculado, 0 AS esperado,
       CASE WHEN nulas = 0 THEN 'PASSOU'
            ELSE raise_error(concat(CAST(nulas AS STRING), ' data_pedido nulas')) END AS resultado
FROM (
  SELECT COUNT(*) FILTER (WHERE data_pedido IS NULL) AS nulas
  FROM lakehouse_rotaperfume.silver.pedidos
);

-- COMMAND ----------

-- Teste 4 — receita negativa só onde devolucao = true.
SELECT '4. receita negativa so em devolucao' AS teste,
       negativas_sem_flag AS calculado, 0 AS esperado,
       CASE WHEN negativas_sem_flag = 0 THEN 'PASSOU'
            ELSE raise_error(concat(CAST(negativas_sem_flag AS STRING),
                                    ' linhas com receita<0 sem devolucao')) END AS resultado
FROM (
  SELECT COUNT(*) FILTER (WHERE receita < 0 AND NOT devolucao) AS negativas_sem_flag
  FROM lakehouse_rotaperfume.gold.fato_vendas
);

-- COMMAND ----------

-- Teste 5 — volume do fato entre 140.000 e 250.000 linhas.
SELECT '5. volume do fato' AS teste,
       linhas AS calculado, 'entre 140.000 e 250.000' AS esperado,
       CASE WHEN linhas BETWEEN 140000 AND 250000 THEN 'PASSOU'
            ELSE raise_error(concat('fato com ', CAST(linhas AS STRING), ' linhas, fora da faixa')) END AS resultado
FROM (
  SELECT COUNT(*) AS linhas FROM lakehouse_rotaperfume.gold.fato_vendas
);

-- COMMAND ----------

-- Teste 6 — nenhum pedido_id na gold que não exista na silver.pedidos.
SELECT '6. pedido_id integro' AS teste,
       orfaos AS calculado, 0 AS esperado,
       CASE WHEN orfaos = 0 THEN 'PASSOU'
            ELSE raise_error(concat(CAST(orfaos AS STRING), ' pedido_id na gold sem par na silver')) END AS resultado
FROM (
  SELECT COUNT(*) AS orfaos
  FROM lakehouse_rotaperfume.gold.fato_vendas f
  LEFT ANTI JOIN lakehouse_rotaperfume.silver.pedidos p ON p.pedido_id = f.pedido_id
);

-- COMMAND ----------

-- Teste 7 — nenhum cliente_id na gold que não exista na silver.clientes.
SELECT '7. cliente_id integro' AS teste,
       orfaos AS calculado, 0 AS esperado,
       CASE WHEN orfaos = 0 THEN 'PASSOU'
            ELSE raise_error(concat(CAST(orfaos AS STRING), ' cliente_id na gold sem par na silver')) END AS resultado
FROM (
  SELECT COUNT(*) AS orfaos
  FROM lakehouse_rotaperfume.gold.fato_vendas f
  LEFT ANTI JOIN lakehouse_rotaperfume.silver.clientes c ON c.cliente_id = f.cliente_id
);

-- COMMAND ----------

-- Teste 8 — conformidade: mart_produto_performance soma o mesmo que fato_vendas.
SELECT '8. mart_produto = fato' AS teste,
       mart_r AS calculado, fato_r AS esperado,
       CASE WHEN abs(mart_r - fato_r) <= 0.01 THEN 'PASSOU'
            ELSE raise_error(concat('mart ', CAST(mart_r AS STRING),
                                    ' != fato ', CAST(fato_r AS STRING))) END AS resultado
FROM (
  SELECT (SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.mart_produto_performance) AS mart_r,
         (SELECT ROUND(SUM(receita), 2) FROM lakehouse_rotaperfume.gold.fato_vendas)              AS fato_r
);

-- COMMAND ----------

-- Teste 9 — todo CNPJ com exatamente 14 dígitos.
SELECT '9. cnpj com 14 digitos' AS teste,
       fora AS calculado, 0 AS esperado,
       CASE WHEN fora = 0 THEN 'PASSOU'
            ELSE raise_error(concat(CAST(fora AS STRING), ' cnpj com tamanho != 14')) END AS resultado
FROM (
  SELECT COUNT(*) FILTER (WHERE length(cnpj) <> 14) AS fora
  FROM lakehouse_rotaperfume.silver.clientes
);
