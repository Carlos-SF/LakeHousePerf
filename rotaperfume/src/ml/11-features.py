# Databricks notebook source
# MAGIC %md
# MAGIC # ML — features de cliente (uma linha por cliente)
# MAGIC
# MAGIC A gold responde tudo sobre o passado, mas o modelo precisa de **uma linha por
# MAGIC cliente** com tudo que se sabia dele **até uma data de corte**. Este notebook define
# MAGIC **uma** função `montar_features(referencia)` e a chama duas vezes:
# MAGIC
# MAGIC - `gold.features_treino`  — corte `2026-08-01`, com o alvo `comprou_em_7d`.
# MAGIC - `gold.features_cliente` — corte `2026-08-31`, sem alvo (é o que será pontuado).
# MAGIC
# MAGIC Uma função, dois usos: é impossível treino e score divergirem (*training/serving
# MAGIC skew*) — aqui resolvido com um `def`, não com infraestrutura.
# MAGIC
# MAGIC **Regras que não se negociam:**
# MAGIC - Cada fonte é filtrada pela **sua** data, `< referencia`, na primeira linha da leitura.
# MAGIC - **Não** se lê `gold.dim_cliente`: ela agrega a base inteira sem corte = vazamento.
# MAGIC - A data de corte vira **coluna** (`_referencia`), não comentário no código.
# MAGIC - Nada de `current_date()`: o "hoje" deste dataset é `2026-08-31`.
# MAGIC - Toda feature numérica sai como `double` (soma de receita/margem vem `DECIMAL(18,2)`
# MAGIC   e o registro do modelo quebra com "Object of type Decimal is not JSON serializable").
# MAGIC - Cliente sem oportunidade ou sem visita fica com `0`. Só as features de **ritmo**
# MAGIC   podem ser `NULL` (cliente de um pedido só não tem intervalo entre pedidos).

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse_rotaperfume", "Catálogo")
catalog = dbutils.widgets.get("catalog")
print(f"catálogo: {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# As três datas-âncora do dataset. O "hoje" é 2026-08-31; a semana da fila é a primeira
# de agosto. Ficam como constantes para não haver current_date() escondido em lugar nenhum.
CORTE_TREINO = "2026-08-01"
CORTE_CLIENTE = "2026-08-31"
FIM_JANELA_ALVO = "2026-08-07"  # rótulo = comprou entre CORTE_TREINO e este dia (7 dias)

# As 20 features numéricas, na ordem dos quatro grupos. Todas viram double; só as três de
# ritmo podem ficar NULL — as demais são preenchidas com 0.
FEATURES = [
    # RFM
    "recencia_dias", "frequencia_pedidos", "valor_total", "ticket_medio",
    "margem_total", "margem_percentual",
    # Ritmo
    "intervalo_medio_dias", "desvio_intervalo_dias", "atraso_relativo", "pedidos_ultimos_90d",
    # CRM
    "oportunidades_abertas", "oportunidades_ganhas", "taxa_ganho", "visitas_90d", "conversao_visita",
    # Mix
    "skus_distintos", "categorias_distintas", "marcas_distintas", "concentracao_marca_top",
    "comprou_lancamento",
]
RITMO_NULAVEL = {"intervalo_medio_dias", "desvio_intervalo_dias", "atraso_relativo"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## A função — escrita uma vez, dois usos
# MAGIC
# MAGIC Toda fonte é filtrada `< referencia` logo na leitura. Os gaps entre pedidos são
# MAGIC calculados **uma vez** com `lag()` sobre as datas distintas de pedido, e média e
# MAGIC desvio saem daí. O único `join` fora do fato é `dim_produto`, para `comprou_lancamento`.

# COMMAND ----------

def montar_features(referencia: str):
    """Uma linha por cliente com tudo que se sabia dele ATÉ `referencia` (exclusive).

    `referencia` é 'YYYY-MM-DD'. Devolve um DataFrame; não grava nada.
    """
    corte = F.lit(referencia).cast("date")
    limite_90d = F.date_sub(corte, 90)
    limite_120d = F.date_sub(corte, 120)

    # --- fontes, cada uma filtrada pela SUA data, antes do corte ---
    fato = spark.table(f"{catalog}.gold.fato_vendas").where(F.col("data_pedido") < corte)
    oportunidades = (
        spark.table(f"{catalog}.silver.oportunidades").where(F.col("data_abertura") < corte)
    )
    visitas = spark.table(f"{catalog}.silver.visitas").where(F.col("data_visita") < corte)

    # --- RFM + Mix + janela de 90d (tudo do fato, num group by) ---
    rfm = (
        fato.groupBy("cliente_id")
        .agg(
            F.datediff(corte, F.max("data_pedido")).alias("recencia_dias"),
            F.countDistinct("pedido_id").alias("frequencia_pedidos"),
            F.sum("receita").alias("valor_total"),
            F.sum("margem").alias("margem_total"),
            F.countDistinct(
                F.when(F.col("data_pedido") >= limite_90d, F.col("pedido_id"))
            ).alias("pedidos_ultimos_90d"),
            F.countDistinct("sku").alias("skus_distintos"),
            F.countDistinct("categoria").alias("categorias_distintas"),
            F.countDistinct("marca").alias("marcas_distintas"),
        )
        .withColumn("ticket_medio", F.col("valor_total") / F.col("frequencia_pedidos"))
        .withColumn(
            "margem_percentual", F.col("margem_total") / F.nullif(F.col("valor_total"), F.lit(0))
        )
    )

    # --- Ritmo: os gaps entre pedidos consecutivos, calculados UMA vez ---
    datas = fato.select("cliente_id", "data_pedido").distinct()
    janela = Window.partitionBy("cliente_id").orderBy("data_pedido")
    gaps = (
        datas.withColumn(
            "gap", F.datediff(F.col("data_pedido"), F.lag("data_pedido").over(janela))
        )
        .where(F.col("gap").isNotNull())
    )
    ritmo = gaps.groupBy("cliente_id").agg(
        F.avg("gap").alias("intervalo_medio_dias"),
        F.stddev("gap").alias("desvio_intervalo_dias"),
    )

    # --- CRM: oportunidades (ganha e perdida são booleanas na silver) ---
    ganha = F.coalesce(F.col("ganha"), F.lit(False))
    perdida = F.coalesce(F.col("perdida"), F.lit(False))
    crm_op = (
        oportunidades.groupBy("cliente_id")
        .agg(
            F.sum(F.when(~ganha & ~perdida, 1).otherwise(0)).alias("oportunidades_abertas"),
            F.sum(F.when(ganha, 1).otherwise(0)).alias("oportunidades_ganhas"),
            F.count("*").alias("_op_total"),
        )
        .withColumn(
            "taxa_ganho",
            F.col("oportunidades_ganhas") / F.nullif(F.col("_op_total"), F.lit(0)),
        )
        .drop("_op_total")
    )

    # --- CRM: visitas. 'gerou_pedido' neste workspace = resultado == 'Pedido realizado' ---
    crm_vis = (
        visitas.groupBy("cliente_id")
        .agg(
            F.sum(F.when(F.col("data_visita") >= limite_90d, 1).otherwise(0)).alias("visitas_90d"),
            F.sum(F.when(F.col("resultado") == "Pedido realizado", 1).otherwise(0)).alias(
                "_vis_pedido"
            ),
            F.count("*").alias("_vis_total"),
        )
        .withColumn(
            "conversao_visita", F.col("_vis_pedido") / F.nullif(F.col("_vis_total"), F.lit(0))
        )
        .drop("_vis_pedido", "_vis_total")
    )

    # --- Mix: concentração na marca top (receita da maior marca / receita total) ---
    marca_top = (
        fato.groupBy("cliente_id", "marca")
        .agg(F.sum("receita").alias("_receita_marca"))
        .groupBy("cliente_id")
        .agg(F.max("_receita_marca").alias("_receita_marca_top"))
    )

    # --- Mix: comprou algum SKU lançado nos 120 dias antes do corte (único join externo) ---
    produtos = spark.table(f"{catalog}.gold.dim_produto").select("sku", "data_lancamento")
    lancamentos = (
        fato.select("cliente_id", "sku")
        .distinct()
        .join(produtos, "sku")
        .where((F.col("data_lancamento") >= limite_120d) & (F.col("data_lancamento") < corte))
        .groupBy("cliente_id")
        .agg(F.lit(1).alias("comprou_lancamento"))
    )

    # --- monta: base = clientes com pedido antes do corte; o resto entra por LEFT JOIN ---
    features = (
        fato.select("cliente_id")
        .distinct()
        .join(rfm, "cliente_id", "left")
        .join(ritmo, "cliente_id", "left")
        .join(crm_op, "cliente_id", "left")
        .join(crm_vis, "cliente_id", "left")
        .join(marca_top, "cliente_id", "left")
        .join(lancamentos, "cliente_id", "left")
        .withColumn(
            "concentracao_marca_top",
            F.col("_receita_marca_top") / F.nullif(F.col("valor_total"), F.lit(0)),
        )
        .drop("_receita_marca_top")
    )

    # atraso_relativo = recencia / intervalo_medio, com teto 10. ARMADILHA: F.least ignora
    # NULL e devolveria o teto (10) aos clientes de um pedido só, jogando-os para o TOPO da
    # fila. O when() em volta garante NULL onde não há intervalo.
    features = features.withColumn(
        "atraso_relativo",
        F.when(
            F.col("intervalo_medio_dias").isNotNull() & (F.col("intervalo_medio_dias") > 0),
            F.least(F.col("recencia_dias") / F.col("intervalo_medio_dias"), F.lit(10.0)),
        ),
    )

    # cast para double em TODAS as numéricas; 0 no lugar de NULL, exceto nas de ritmo.
    for c in FEATURES:
        col = F.col(c).cast("double")
        if c not in RITMO_NULAVEL:
            col = F.coalesce(col, F.lit(0.0))
        features = features.withColumn(c, col)

    # a data de corte é COLUNA, não comentário no código.
    features = features.withColumn("_referencia", F.lit(referencia).cast("date"))
    return features.select("cliente_id", *FEATURES, "_referencia")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação — as duas tabelas saem da mesma função
# MAGIC
# MAGIC `saveAsTable` **não** grava comentário de tabela; roda-se `COMMENT ON TABLE` em
# MAGIC seguida — a auditoria de metadado quebra o job se faltar.

# COMMAND ----------

def gravar(df, tabela: str, comentario: str) -> int:
    destino = f"{catalog}.gold.{tabela}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(destino)
    )
    spark.sql(f"COMMENT ON TABLE {destino} IS '{comentario}'")
    n = spark.table(destino).count()
    print(f"{destino}: {n:,} clientes")
    return n

# COMMAND ----------

# Treino: mesmo corte 2026-08-01, mais o alvo — comprou entre 01/08 e 07/08 (a semana da fila).
alvo = (
    spark.table(f"{catalog}.gold.fato_vendas")
    .where(
        (F.col("data_pedido") >= F.lit(CORTE_TREINO).cast("date"))
        & (F.col("data_pedido") <= F.lit(FIM_JANELA_ALVO).cast("date"))
    )
    .select("cliente_id")
    .distinct()
    .withColumn("comprou_em_7d", F.lit(1))
)

treino = (
    montar_features(CORTE_TREINO)
    .join(alvo, "cliente_id", "left")
    .withColumn("comprou_em_7d", F.coalesce(F.col("comprou_em_7d"), F.lit(0)).cast("int"))
)

gravar(
    treino,
    "features_treino",
    "Features de cliente para TREINO: uma linha por cliente com 20 features (RFM, ritmo, "
    "CRM, mix) calculadas com dado anterior a _referencia (2026-08-01), mais o alvo "
    "comprou_em_7d (fez pedido entre 2026-08-01 e 2026-08-07). Gerada pela mesma funcao "
    "montar_features que produz features_cliente.",
)

# COMMAND ----------

# Cliente: mesmo motor, corte no "hoje" do dataset (2026-08-31), sem alvo — é o que se pontua.
cliente = montar_features(CORTE_CLIENTE)

gravar(
    cliente,
    "features_cliente",
    "Features de cliente para PONTUACAO: uma linha por cliente com 20 features (RFM, ritmo, "
    "CRM, mix) calculadas com dado anterior a _referencia (2026-08-31), sem alvo. Gerada pela "
    "mesma funcao montar_features que produz features_treino.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência rápida (as réguas do prompt)
# MAGIC
# MAGIC - `features_treino` ~ 2.815 clientes @ 2026-08-01; `features_cliente` ~ 2.816 @ 2026-08-31.
# MAGIC - taxa base de `comprou_em_7d` ~ 10,12%.
# MAGIC - `MIN(recencia_dias)` **não** pode ser negativa (recência negativa = vazamento).

# COMMAND ----------

conf = spark.sql(
    f"""
    SELECT COUNT(*)                            AS clientes,
           MIN(_referencia)                    AS corte,
           SUM(comprou_em_7d)                  AS compraram,
           ROUND(100 * AVG(comprou_em_7d), 2)  AS taxa_base_pct,
           MIN(recencia_dias)                  AS menor_recencia
    FROM {catalog}.gold.features_treino
    """
)
conf.show(truncate=False)

if conf.first()["menor_recencia"] < 0:
    raise ValueError("Recência negativa em features_treino: uma fonte escapou do filtro < referencia (vazamento).")
print("OK — sem recência negativa.")
