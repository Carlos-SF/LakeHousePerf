# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — ingestão das 10 tabelas
# MAGIC
# MAGIC Lê os 10 CSVs de `/Volumes/{catalog}/bronze/raw/{sistema}/{tabela}.csv` e grava
# MAGIC `{catalog}.bronze.{tabela}` em Delta (`overwrite`).
# MAGIC
# MAGIC **Regras da bronze — nenhuma limpeza, nenhuma conversão de tipo:**
# MAGIC - tudo entra como **texto** (`inferColumnTypes => false`). Inferir tipo apaga o
# MAGIC   zero à esquerda de 309 CNPJs e transforma `dd/MM/yyyy` em nulo, calado.
# MAGIC - CSVs são CRLF com header. **Sem `multiLine`.**
# MAGIC - acrescenta só dois metadados técnicos: `_ingerido_em` e `_arquivo_origem`.
# MAGIC - a função de ingestão é escrita **uma vez** e itera sobre a lista de 10 tabelas.
# MAGIC - ao final, confere a contagem de cada tabela contra `bronze._raw_arquivos`
# MAGIC   (o controle de chegada do passo `raw_conferencia`). Se divergir, **falha**.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse_rotaperfume", "Catálogo")
catalog = dbutils.widgets.get("catalog")
print(f"catálogo: {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F

# As 10 tabelas da bronze, com o sistema de origem. A ordem é só cosmética;
# o conteúdo é o par (sistema, tabela), que define caminho de origem e comentário.
TABELAS = [
    ("erp", "produtos"),
    ("erp", "pedidos"),
    ("erp", "itens_pedido"),
    ("erp", "pagamentos"),
    ("erp", "estoque"),
    ("crm", "clientes"),
    ("crm", "vendedores"),
    ("crm", "carteira"),
    ("crm", "oportunidades"),
    ("crm", "visitas"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## A ingestão — escrita uma vez
# MAGIC
# MAGIC `read_files` com `inferColumnTypes => false` traz tudo como `string`. O leitor
# MAGIC do Databricks acrescenta sozinho uma coluna `_rescued_data`; ela é descartada com
# MAGIC `EXCEPT` (passar `rescuedDataColumn => ''` **não** desliga — cria uma coluna de
# MAGIC nome vazio e o `CREATE TABLE` quebra).


# COMMAND ----------

def ingerir(sistema: str, tabela: str) -> int:
    """Lê o CSV bruto de `sistema/tabela` e grava a tabela Delta da bronze.

    Tudo como texto, sem limpeza. Devolve a contagem de linhas gravadas.
    """
    caminho = f"/Volumes/{catalog}/bronze/raw/{sistema}/{tabela}.csv"
    destino = f"{catalog}.bronze.{tabela}"

    df = (
        spark.sql(
            f"""
            SELECT * EXCEPT (_rescued_data)
            FROM read_files(
                '{caminho}',
                format => 'csv',
                header => true,
                inferColumnTypes => false,
                multiLine => false
            )
            """
        )
        .withColumn("_ingerido_em", F.current_timestamp())
        .withColumn("_arquivo_origem", F.lit(caminho))
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(destino)
    )

    spark.sql(
        f"COMMENT ON TABLE {destino} IS "
        f"'Bronze bruta do sistema {sistema.upper()} (arquivo {tabela}.csv). "
        f"Ingestão sem limpeza nem conversão: tudo texto, de propósito.'"
    )

    return spark.table(destino).count()


# COMMAND ----------

# Controle de chegada do passo anterior: arquivo -> linhas (já sem o header).
esperado = {
    r["arquivo"]: r["linhas"]
    for r in spark.table(f"{catalog}.bronze._raw_arquivos").collect()
}

relatorio = []
for sistema, tabela in TABELAS:
    gravadas = ingerir(sistema, tabela)
    no_arquivo = esperado.get(f"{tabela}.csv")
    bate = no_arquivo is not None and gravadas == no_arquivo
    relatorio.append((tabela, sistema, gravadas, no_arquivo, bate))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência — a tabela bate com o arquivo de origem?

# COMMAND ----------

resumo = spark.createDataFrame(
    relatorio,
    "tabela string, sistema string, na_tabela long, no_arquivo long, bate boolean",
).orderBy(F.col("na_tabela").desc())
resumo.show(20, truncate=False)

total = sum(linha[2] for linha in relatorio)
print(f"total de linhas ingeridas: {total:,}")

divergentes = [(t, n, e) for (t, _s, n, e, ok) in relatorio if not ok]
if divergentes:
    raise ValueError(
        "Bronze diverge do controle de chegada (_raw_arquivos). "
        f"(tabela, na_tabela, no_arquivo): {divergentes}"
    )

print("OK — as 10 tabelas batem com bronze._raw_arquivos.")
