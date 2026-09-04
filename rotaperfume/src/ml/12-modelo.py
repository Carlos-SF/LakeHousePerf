# Databricks notebook source
# MAGIC %md
# MAGIC # ML — modelo de propensão + MLflow
# MAGIC
# MAGIC Treina um classificador de propensão a comprar nos próximos 7 dias, registra no Unity
# MAGIC Catalog (mesmo catálogo, GRANT e linhagem das tabelas) e pontua os ~3.000 clientes.
# MAGIC
# MAGIC A ordem é o argumento da aula:
# MAGIC 1. **Baseline antes de treinar** — a régua. `atraso_relativo` sozinho já dá AUC ~0,78;
# MAGIC    "ligue para quem comprou recentemente" dá ~0,35 (pior que a moeda).
# MAGIC 2. Treino com `HistGradientBoostingClassifier` (trata NaN nativo; **não** XGBoost, que
# MAGIC    falha ao recarregar no serverless).
# MAGIC 3. Duas métricas: `auc` (holdout) e `lift_top200` (out-of-fold — a fila real é 200 de 3.000).
# MAGIC 4. Importância por permutação.
# MAGIC 5. MLflow → registro em `gold.propensao_compra`, alias `@prod`.
# MAGIC 6. **Três testes que quebram a tarefa** — inclusive `auc < 0,99` (bom demais é vazamento).
# MAGIC 7. Score → `gold.score_propensao`.
# MAGIC 8. Métricas viram tabela (`gold.modelo_metricas`, `gold.calibragem_holdout`) — o Genie não lê MLflow.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakehouse_rotaperfume", "Catálogo")
catalog = dbutils.widgets.get("catalog")
print(f"catálogo: {catalog}")

# COMMAND ----------

import os

# No serverless com Default Storage, o registro de modelo no UC precisa que o MLflow use o
# repositório de artefatos via Databricks SDK — senão: "PERMISSION_DENIED: Cannot generate
# temporary credentials for model version". Setar ANTES de importar/usar o mlflow de registro.
os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_predict
from sklearn.metrics import roc_auc_score
from sklearn.inspection import permutation_importance
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

# Registry no UC ANTES de qualquer chamada de registro: no serverless (Spark Connect) o MLflow
# tenta LER a spark conf `spark.mlflow.modelRegistryUri` para auto-detectar, e essa leitura é
# bloqueada (CONFIG_NOT_AVAILABLE). Setar explicitamente aqui evita a leitura.
mlflow.set_registry_uri("databricks-uc")

# As 20 features, na mesma ordem do 11-features.py. X = essas colunas; y = comprou_em_7d.
FEATURES = [
    "recencia_dias", "frequencia_pedidos", "valor_total", "ticket_medio",
    "margem_total", "margem_percentual",
    "intervalo_medio_dias", "desvio_intervalo_dias", "atraso_relativo", "pedidos_ultimos_90d",
    "oportunidades_abertas", "oportunidades_ganhas", "taxa_ganho", "visitas_90d", "conversao_visita",
    "skus_distintos", "categorias_distintas", "marcas_distintas", "concentracao_marca_top",
    "comprou_lancamento",
]

MODELO_UC = f"{catalog}.gold.propensao_compra"

# COMMAND ----------

def gravar(df, tabela: str, comentario: str) -> int:
    """Grava a tabela gold em overwrite e roda COMMENT ON TABLE (saveAsTable não grava comment)."""
    destino = f"{catalog}.gold.{tabela}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(destino)
    )
    spark.sql(f"COMMENT ON TABLE {destino} IS '{comentario}'")
    n = spark.table(destino).count()
    print(f"{destino}: {n:,} linhas")
    return n

# COMMAND ----------

# Treino em pandas (2.815 clientes cabem com folga).
pdf = spark.table(f"{catalog}.gold.features_treino").toPandas()
X = pdf[FEATURES]
y = pdf["comprou_em_7d"].astype(int)
taxa_base = float(y.mean())
print(f"clientes: {len(pdf):,} · taxa base: {taxa_base:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Baseline — a régua, antes de treinar qualquer coisa
# MAGIC
# MAGIC 25% de holdout estratificado. Cada regra simples é usada **como se fosse o score**.
# MAGIC `atraso_relativo` tem NaN de propósito (cliente de 1 pedido) → preenchido com 0 só aqui,
# MAGIC porque `roc_auc_score` não aceita NaN no score.

# COMMAND ----------

X_train, X_hold, y_train, y_hold = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=y
)

auc_base_recencia = roc_auc_score(y_hold, -X_hold["recencia_dias"])          # "quem comprou recentemente"
auc_base_valor = roc_auc_score(y_hold, X_hold["valor_total"])                # "quem compra mais"
auc_base_atraso = roc_auc_score(y_hold, X_hold["atraso_relativo"].fillna(0.0))  # "quem está atrasado"
melhor_baseline = max(auc_base_recencia, auc_base_valor, auc_base_atraso)

print(f"{'a resposta':<42}{'AUC':>8}")
for nome, val in [
    ("ligue para quem comprou recentemente", auc_base_recencia),
    ("jogar uma moeda", 0.5),
    ("ligue para quem compra mais", auc_base_valor),
    ("ligue para quem está atrasado", auc_base_atraso),
]:
    print(f"{nome:<42}{val:>8.4f}")
print(f"\nmelhor baseline (régua do teste 1): {melhor_baseline:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Treino — HistGradientBoostingClassifier
# MAGIC
# MAGIC NaN não é imputado: o algoritmo trata nativamente e as features de ritmo são NULL de
# MAGIC propósito. XGBoost treina mas falha ao recarregar no serverless (`__sklearn_tags__`).

# COMMAND ----------

clf = HistGradientBoostingClassifier(random_state=42)
clf.fit(X_train, y_train)
print("modelo treinado nos 75%.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · As duas métricas — auc (holdout) e lift_top200 (out-of-fold)

# COMMAND ----------

auc = roc_auc_score(y_hold, clf.predict_proba(X_hold)[:, 1])

# lift_top200: pontua TODOS os clientes por validação cruzada out-of-fold, ordena, pega 200.
oof = cross_val_predict(
    HistGradientBoostingClassifier(random_state=42),
    X, y,
    cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
    method="predict_proba",
)[:, 1]
top200 = np.argsort(oof)[::-1][:200]
acertos_top200 = int(y.iloc[top200].sum())
lift_top200 = (acertos_top200 / 200) / taxa_base

print(f"auc (holdout):   {auc:.4f}")
print(f"acertos_top200:  {acertos_top200} de 200")
print(f"lift_top200:     {lift_top200:.2f}× a taxa base")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Importância por permutação (holdout, n_repeats=5)

# COMMAND ----------

perm = permutation_importance(
    clf, X_hold, y_hold, n_repeats=5, random_state=42, scoring="roc_auc"
)
importancias = sorted(zip(FEATURES, perm.importances_mean), key=lambda kv: kv[1], reverse=True)
for nome, val in importancias[:10]:
    print(f"{nome:<28}{val:>10.4f}")
feature_1 = importancias[0][0]
print(f"\nfeature nº 1: {feature_1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · MLflow — registro no Unity Catalog, alias @prod
# MAGIC
# MAGIC `mkdirs` da pasta pai ANTES de `set_experiment` (senão `BAD_REQUEST: For input string: "None"`).
# MAGIC Serverless tem MLflow 2.22: `log_model(artifact_path="modelo")`, nunca `name=`.

# COMMAND ----------

usuario = spark.sql("SELECT current_user() AS u").first()["u"]
exp_dir = f"/Users/{usuario}/rotaperfume-ml"
exp_path = f"{exp_dir}/propensao"
WorkspaceClient().workspace.mkdirs(exp_dir)
mlflow.set_experiment(exp_path)
# registry_uri já foi setado no topo (databricks-uc) — antes de qualquer leitura de spark conf.

with mlflow.start_run() as run:
    mlflow.log_params(clf.get_params())
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("lift_top200", lift_top200)
    mlflow.log_metric("acertos_top200", acertos_top200)
    mlflow.log_metric("taxa_base", taxa_base)
    mlflow.log_metric("auc_base_atraso", auc_base_atraso)
    # UC exige assinatura (input + output). infer_signature a partir do treino.
    signature = infer_signature(X_train, clf.predict(X_train))
    mlflow.sklearn.log_model(clf, artifact_path="modelo", signature=signature)
    run_id = run.info.run_id

mv = mlflow.register_model(f"runs:/{run_id}/modelo", MODELO_UC)
versao = int(mv.version)
MlflowClient().set_registered_model_alias(MODELO_UC, "prod", versao)
print(f"registrado {MODELO_UC} versão {versao} — alias @prod apontado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Três testes que interrompem a tarefa
# MAGIC
# MAGIC Inclusive `auc < 0,99`: vazamento não chega com erro, chega com elogio.

# COMMAND ----------

assert auc - melhor_baseline >= 0.05, (
    f"O modelo (auc={auc:.4f}) não ganha do melhor baseline ({melhor_baseline:.4f}) "
    "por pelo menos 0,05. Sem margem sobre o que já era feito de graça, o projeto não se paga."
)
assert auc < 0.99, (
    f"auc={auc:.4f} — bom demais é vazamento, não competência. "
    "Alguma feature enxergou informação depois do corte."
)
assert lift_top200 >= 2.5, (
    f"lift_top200={lift_top200:.2f} < 2,5 — a fila não concentra compra o bastante "
    "para justificar o projeto."
)
print("OK — os três testes passaram.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Score — pontua os ~3.000 clientes de features_cliente

# COMMAND ----------

# Carrega o modelo de volta pelo alias e usa predict_proba (pyfunc.predict devolveria a classe;
# spark_udf não roda no serverless). 3.000 clientes cabem em pandas.
modelo = mlflow.sklearn.load_model(f"models:/{MODELO_UC}@prod")
cols = list(modelo.feature_names_in_)  # ordem exata do treino — não confiar na ordem da tabela

pdf_cli = spark.table(f"{catalog}.gold.features_cliente").toPandas()
scores = modelo.predict_proba(pdf_cli[cols])[:, 1]
ref_cliente = str(pdf_cli["_referencia"].iloc[0])

sdf = spark.createDataFrame(
    pd.DataFrame(
        {"cliente_id": pdf_cli["cliente_id"].astype("int64"), "score": scores.astype("float64")}
    )
)
sdf = (
    sdf.withColumn("_referencia", F.lit(ref_cliente).cast("date"))
    .withColumn("versao", F.lit(versao))
)
sdf.createOrReplaceTempView("_score_tmp")

score_final = spark.sql(
    """
    SELECT CAST(cliente_id AS INT) AS cliente_id,
           score,
           CASE NTILE(4) OVER (ORDER BY score)
             WHEN 1 THEN 'Fria' WHEN 2 THEN 'Morna' WHEN 3 THEN 'Quente'
             ELSE 'Muito quente' END              AS faixa,
           _referencia,
           versao
    FROM _score_tmp
    """
)

gravar(
    score_final,
    "score_propensao",
    "Score de propensao a comprar em 7 dias, um por cliente (corte 2026-08-31). faixa = NTILE(4) "
    "sobre o score (Fria/Morna/Quente/Muito quente). versao = versao do modelo no UC "
    "(lakehouse_rotaperfume.gold.propensao_compra@prod). Consumida pelo prompt 3.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · As métricas viram tabela — o Genie não lê MLflow

# COMMAND ----------

metricas = spark.createDataFrame(
    pd.DataFrame(
        [
            {
                "versao": versao,
                "auc": float(auc),
                "lift_top200": float(lift_top200),
                "acertos_top200": int(acertos_top200),
                "taxa_base": float(taxa_base),
                "auc_base_recencia": float(auc_base_recencia),
                "auc_base_valor": float(auc_base_valor),
                "auc_base_atraso": float(auc_base_atraso),
                "feature_1": feature_1,
            }
        ]
    )
).withColumn("_treinado_em", F.current_timestamp())

gravar(
    metricas,
    "modelo_metricas",
    "Uma linha por treino do modelo de propensao: versao, auc (holdout), lift_top200, "
    "acertos_top200, taxa_base, o auc de cada um dos tres baselines, a feature nº 1 (importancia "
    "por permutacao) e _treinado_em. O Genie le isto, nao o MLflow.",
)

# COMMAND ----------

# Calibragem no holdout: a taxa de compra tem que SUBIR de Fria para Muito quente.
cal_pdf = pd.DataFrame(
    {"score": clf.predict_proba(X_hold)[:, 1].astype("float64"), "comprou": y_hold.astype("int64").values}
)
spark.createDataFrame(cal_pdf).createOrReplaceTempView("_cal_tmp")

calibragem = spark.sql(
    """
    WITH faixado AS (
      SELECT comprou, score,
             CASE NTILE(4) OVER (ORDER BY score)
               WHEN 1 THEN 'Fria' WHEN 2 THEN 'Morna' WHEN 3 THEN 'Quente'
               ELSE 'Muito quente' END AS faixa
      FROM _cal_tmp
    )
    SELECT faixa,
           COUNT(*)         AS clientes,
           SUM(comprou)     AS compraram,
           AVG(comprou)     AS taxa_de_compra,
           AVG(score)       AS score_medio
    FROM faixado
    GROUP BY faixa
    """
)

gravar(
    calibragem,
    "calibragem_holdout",
    "Calibragem do score no holdout (25% de features_treino): por faixa, clientes, compraram, "
    "taxa_de_compra e score_medio. A taxa_de_compra sobe de Fria a Muito quente — a prova do "
    "slide Nao e acuracia, a que o comercial confere sozinho.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

print("=== baselines vs modelo ===")
print(f"recência:  {auc_base_recencia:.4f} | valor: {auc_base_valor:.4f} | "
      f"atraso: {auc_base_atraso:.4f} | modelo: {auc:.4f}")
print(f"\n=== língua do diretor ===")
print(f"dos 200 de maior score, {acertos_top200} compram — lift {lift_top200:.2f}× "
      f"(vs {round(200*taxa_base)} às cegas)")
print(f"\nmodelo {MODELO_UC} v{versao} @prod")
spark.sql(f"SELECT faixa, clientes, compraram, ROUND(100*taxa_de_compra,1) AS pct "
          f"FROM {catalog}.gold.calibragem_holdout ORDER BY score_medio").show(truncate=False)
