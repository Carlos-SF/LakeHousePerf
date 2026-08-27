# Databricks notebook source
# MAGIC %md
# MAGIC # Conferência de chegada do raw
# MAGIC
# MAGIC A tarefa mais chata do pipeline e a que mais salva emprego: confere se os
# MAGIC **10 arquivos** (5 ERP + 5 CRM) chegaram ao Volume `bronze.raw`, mede cada um
# MAGIC (bytes e linhas de dado) e grava a tabela de controle `bronze._raw_arquivos`.
# MAGIC
# MAGIC Se faltar arquivo ou algum vier vazio, a tarefa **falha e para o pipeline** —
# MAGIC porque arquivo que não chega não dá erro, dá número menor com cara de número certo.

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("catalog", "lakehouse_rouper", "Catálogo")
catalog = dbutils.widgets.get("catalog")

VOLUME_BASE = f"/Volumes/{catalog}/bronze/raw"

# Os 10 arquivos esperados, por sistema de origem.
ESPERADOS = {
    "erp": ["produtos", "pedidos", "itens_pedido", "pagamentos", "estoque"],
    "crm": ["clientes", "vendedores", "carteira", "oportunidades", "visitas"],
}

print(f"Catálogo: {catalog}")
print(f"Volume:   {VOLUME_BASE}")

# COMMAND ----------

# Índice do que realmente está em cada pasta do Volume (arquivo -> tamanho em bytes).
def listar_volume(sistema: str) -> dict[str, int]:
    caminho = f"{VOLUME_BASE}/{sistema}"
    try:
        infos = dbutils.fs.ls(caminho)
    except Exception as e:  # pasta inexistente = nada chegou
        raise FileNotFoundError(f"Pasta do Volume não encontrada: {caminho} ({e})")
    return {fi.name.rstrip("/"): fi.size for fi in infos if fi.name.endswith(".csv")}

presentes = {sistema: listar_volume(sistema) for sistema in ESPERADOS}

# COMMAND ----------

# Confere presença, mede bytes e conta linhas de dado (sem o cabeçalho).
registros = []
faltando = []
vazios = []

for sistema, arquivos in ESPERADOS.items():
    disponiveis = presentes[sistema]
    for base in arquivos:
        nome = f"{base}.csv"
        if nome not in disponiveis:
            faltando.append(f"{sistema}/{nome}")
            continue

        caminho = f"{VOLUME_BASE}/{sistema}/{nome}"
        bytes_ = disponiveis[nome]
        linhas = (
            spark.read.option("header", True).csv(caminho).count()
        )
        if bytes_ == 0 or linhas == 0:
            vazios.append(f"{sistema}/{nome}")

        registros.append((sistema, nome, int(bytes_), int(linhas)))

# COMMAND ----------

# Interrompe o pipeline se algo faltou ou veio vazio.
if faltando:
    raise RuntimeError(
        "Conferência FALHOU — arquivos faltando no Volume: " + ", ".join(faltando)
    )
if vazios:
    raise RuntimeError(
        "Conferência FALHOU — arquivos vazios (0 bytes ou 0 linhas): " + ", ".join(vazios)
    )

print(f"OK: {len(registros)} arquivos conferidos, nenhum faltando, nenhum vazio.")

# COMMAND ----------

# Grava a tabela de controle bronze._raw_arquivos.
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

conferido_em = datetime.now(timezone.utc)

schema = StructType(
    [
        StructField("sistema", StringType(), False),
        StructField("arquivo", StringType(), False),
        StructField("bytes", LongType(), False),
        StructField("linhas", LongType(), False),
        StructField("conferido_em", TimestampType(), False),
    ]
)

df = spark.createDataFrame(
    [Row(sistema=s, arquivo=a, bytes=b, linhas=l, conferido_em=conferido_em) for (s, a, b, l) in registros],
    schema=schema,
)

tabela = f"{catalog}.bronze._raw_arquivos"
(
    df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabela)
)

spark.sql(
    f"COMMENT ON TABLE {tabela} IS "
    "'Controle de chegada do raw: um registro por CSV ingerido no Volume "
    "bronze.raw, com tamanho em bytes e linhas de dado na última conferência.'"
)

print(f"Tabela de controle gravada: {tabela}")

# COMMAND ----------

# Tabela legível ao final.
resumo = spark.table(tabela).orderBy(F.col("linhas").desc())
resumo.show(truncate=False)

totais = resumo.agg(
    F.count("*").alias("arquivos"),
    F.sum("linhas").alias("linhas_de_dado"),
    F.round(F.sum("bytes") / 1024 / 1024, 1).alias("mb"),
)
totais.show(truncate=False)
