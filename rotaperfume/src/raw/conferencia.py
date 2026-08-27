# Databricks notebook source
# MAGIC %md
# MAGIC # Raw — conferência de chegada
# MAGIC
# MAGIC O controle de chegada da camada raw. Antes de qualquer transformação, registra
# MAGIC **o que chegou**: para cada um dos 10 arquivos no Volume
# MAGIC `/Volumes/{catalog}/bronze/raw/{sistema}/{tabela}.csv`, quantos **bytes** e quantas
# MAGIC **linhas** (já sem o header). O resultado vai para `{catalog}.bronze._raw_arquivos`.
# MAGIC
# MAGIC É a primeira tarefa do job: se a conferência falhar, a bronze não roda.
# MAGIC
# MAGIC A contagem usa o **mesmo leitor** da bronze (`read_files`, `inferColumnTypes =>
# MAGIC false`, sem `multiLine`), então `linhas` bate, por construção, com o que a bronze
# MAGIC vai gravar. É contra este número que `bronze/ingestao.py` se confere.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse_rotaperfume", "Catálogo")
catalog = dbutils.widgets.get("catalog")
print(f"catálogo: {catalog}")

# COMMAND ----------

from datetime import datetime, timezone

# As 10 tabelas da raw, com o sistema de origem — a mesma lista da bronze.
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

# Infra da raw — idempotente. O Volume é onde os CSVs pousam antes de virar tabela.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Os arquivos chegaram?
# MAGIC
# MAGIC Se algum CSV não estiver no Volume, a conferência falha aqui — de propósito. Suba
# MAGIC os 10 arquivos de `dados/{sistema}/{tabela}.csv` (na raiz do repositório) para
# MAGIC `/Volumes/{catalog}/bronze/raw/{sistema}/` antes de rodar. Ex.:
# MAGIC
# MAGIC ```bash
# MAGIC databricks fs cp dados/erp/produtos.csv \
# MAGIC   dbfs:/Volumes/{catalog}/bronze/raw/erp/produtos.csv --profile <perfil>
# MAGIC ```

# COMMAND ----------

def caminho(sistema: str, tabela: str) -> str:
    return f"/Volumes/{catalog}/bronze/raw/{sistema}/{tabela}.csv"


def existe(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


faltando = [f"{s}/{t}.csv" for s, t in TABELAS if not existe(caminho(s, t))]
if faltando:
    raise FileNotFoundError(
        "Arquivos ausentes no Volume raw — suba-os antes de conferir: " + ", ".join(faltando)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## A conferência — bytes e linhas de cada arquivo

# COMMAND ----------

def conferir(sistema: str, tabela: str) -> tuple:
    """Devolve (sistema, arquivo, bytes, linhas) de um CSV no Volume raw.

    `linhas` é a contagem de registros com o mesmo leitor da bronze (header fora).
    """
    origem = caminho(sistema, tabela)
    tamanho = dbutils.fs.ls(origem)[0].size
    linhas = spark.sql(
        f"""
        SELECT count(*) AS n
        FROM read_files(
            '{origem}',
            format => 'csv',
            header => true,
            inferColumnTypes => false,
            multiLine => false
        )
        """
    ).collect()[0]["n"]
    return (sistema, f"{tabela}.csv", tamanho, linhas)


conferido_em = datetime.now(timezone.utc)
registros = [conferir(s, t) + (conferido_em,) for s, t in TABELAS]

# COMMAND ----------

controle = spark.createDataFrame(
    registros,
    "sistema string, arquivo string, bytes long, linhas long, conferido_em timestamp",
)
controle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.bronze._raw_arquivos"
)

spark.sql(
    f"COMMENT ON TABLE {catalog}.bronze._raw_arquivos IS "
    f"'Controle de chegada da raw: bytes e linhas (sem header) de cada CSV, por arquivo.'"
)

# COMMAND ----------

from pyspark.sql import functions as F

resumo = spark.table(f"{catalog}.bronze._raw_arquivos").orderBy(F.col("linhas").desc())
resumo.show(20, truncate=False)

total = sum(r[3] for r in registros)
print(f"total de linhas conferidas: {total:,}")
