# Databricks notebook source
# Código fuente exportado para revisión y control de versiones.

# COMMAND ----------

# MAGIC %md
# MAGIC # Modelo de deteccion y priorizacion de fraude en seguros
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("table_name", "workspace.default.muestra_base_fraude", "Tabla fuente")
dbutils.widgets.text("output_schema", "workspace.default", "Schema salida")

TABLE_NAME = dbutils.widgets.get("table_name")
OUTPUT_SCHEMA = dbutils.widgets.get("output_schema")

print(f"Tabla fuente: {TABLE_NAME}")
print(f"Schema salida: {OUTPUT_SCHEMA}")

# COMMAND ----------

import re
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer, StandardScaler

def clean_name(name: str) -> str:
    name = name.strip()
    name = name.replace("�", "n")
    name = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


raw = spark.table(TABLE_NAME)
rename_map = {c: clean_name(c) for c in raw.columns}
df = raw
for old, new in rename_map.items():
    df = df.withColumnRenamed(old, new)

display(df.limit(5))
print((df.count(), len(df.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Calidad de datos y variable objetivo

# COMMAND ----------

target_col = "Fraude_S_N"
if target_col not in df.columns:
    raise ValueError(f"No encontre la columna objetivo {target_col}. Columnas: {df.columns}")

df = df.withColumn(
    "label",
    F.when(F.lower(F.trim(F.col(target_col))) == F.lit("fraude"), F.lit(1.0)).otherwise(F.lit(0.0)),
)

display(df.groupBy(target_col, "label").count())

quality = []
for c in df.columns:
    quality.append(
        df.select(
            F.lit(c).alias("columna"),
            F.count(F.when(F.col(c).isNull(), c)).alias("nulos"),
            F.approx_count_distinct(F.col(c)).alias("cardinalidad_aprox"),
        )
    )

quality_df = quality[0]
for q in quality[1:]:
    quality_df = quality_df.unionByName(q)

display(quality_df.orderBy(F.desc("nulos"), F.desc("cardinalidad_aprox")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature engineering y control de leakage
# MAGIC
# MAGIC Se excluyen identificadores y variables posteriores al momento de scoring. Las fechas se transforman en diferencias de dias y variables calendario.

# COMMAND ----------

date_cols = [
    "Fecha_Primera_Vigencia_Cert",
    "fecha_primera_vigencia_pol",
    "FEXPEDICION",
    "FSINIESTRO",
    "F_Notificacion",
    "Fecha_Recepcion",
    "Fecha_Apertura",
]

for c in date_cols:
    if c in df.columns:
        df = df.withColumn(c, F.to_date(F.col(c)))

if {"FSINIESTRO", "Fecha_Apertura"}.issubset(set(df.columns)):
    df = df.withColumn("dias_siniestro_a_apertura", F.datediff("Fecha_Apertura", "FSINIESTRO"))
if {"FSINIESTRO", "F_Notificacion"}.issubset(set(df.columns)):
    df = df.withColumn("dias_siniestro_a_notificacion", F.datediff("F_Notificacion", "FSINIESTRO"))
if {"F_Notificacion", "Fecha_Apertura"}.issubset(set(df.columns)):
    df = df.withColumn("dias_notificacion_a_apertura", F.datediff("Fecha_Apertura", "F_Notificacion"))
if {"Fecha_Primera_Vigencia_Cert", "FSINIESTRO"}.issubset(set(df.columns)):
    df = df.withColumn("dias_vigencia_cert_a_siniestro", F.datediff("FSINIESTRO", "Fecha_Primera_Vigencia_Cert"))
if {"fecha_primera_vigencia_pol", "FSINIESTRO"}.issubset(set(df.columns)):
    df = df.withColumn("dias_vigencia_pol_a_siniestro", F.datediff("FSINIESTRO", "fecha_primera_vigencia_pol"))
if {"FEXPEDICION", "FSINIESTRO"}.issubset(set(df.columns)):
    df = df.withColumn("dias_expedicion_a_siniestro", F.datediff("FSINIESTRO", "FEXPEDICION"))
if "FSINIESTRO" in df.columns:
    df = df.withColumn("mes_siniestro", F.month("FSINIESTRO").cast("string"))
    df = df.withColumn("dia_semana_siniestro", F.dayofweek("FSINIESTRO").cast("string"))
if {"edad_actual_asegurado", "edad_ingreso_asegurado"}.issubset(set(df.columns)):
    df = df.withColumn("delta_edad", F.col("edad_actual_asegurado") - F.col("edad_ingreso_asegurado"))
    df = df.withColumn("edad_actual_invalida", F.when(F.col("edad_actual_asegurado") < 0, 1.0).otherwise(0.0))
if "Sum_Valor_Reservas_Inicial" in df.columns:
    df = df.withColumn("log_reserva_inicial", F.log1p(F.greatest(F.col("Sum_Valor_Reservas_Inicial"), F.lit(0))))
    df = df.withColumn("reserva_inicial_es_cero", F.when(F.col("Sum_Valor_Reservas_Inicial") == 0, 1.0).otherwise(0.0))

strict_exclude = {
    target_col,
    "label",
    "IDENTIFICACION_asegurado",
    "Fecha_Primer_Cierre_Siniestro",
    "estado",
    "Periodo_Reporte",
    "Fecha_de_reporte",
    "Ano",
    "An_o",
    "Sum_Valor_Reservas",
    "Sum_Valor_Pagos",
    *date_cols,
}

high_cardinality_exclude = {
    "AGENTE",
    "DIAGNOSTICO",
    "SUCURSAL",
    "Nombre_plan",
    "Amparo_Desc",
}

feature_cols = [
    c for c in df.columns
    if c not in strict_exclude and c not in high_cardinality_exclude
]
display(spark.createDataFrame([(c,) for c in feature_cols], ["variables_modelo"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Split temporal

# COMMAND ----------

order_col = "Fecha_Apertura" if "Fecha_Apertura" in df.columns else "FSINIESTRO"
w = Window.orderBy(F.col(order_col).asc_nulls_last())
df_split = df.withColumn("rn", F.row_number().over(w))
n = df_split.count()
split_at = int(n * 0.75)

train_df = df_split.filter(F.col("rn") <= split_at).drop("rn")
test_df = df_split.filter(F.col("rn") > split_at).drop("rn")

print(f"Train: {train_df.count()} | Test: {test_df.count()}")
display(train_df.groupBy("label").count())
display(test_df.groupBy("label").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preprocesamiento y tres modelos

# COMMAND ----------

numeric_cols = []

for name, dtype in train_df.select(feature_cols).dtypes:
    if dtype in {"int", "bigint", "double", "float", "decimal", "smallint", "tinyint"}:
        numeric_cols.append(name)

# Version liviana para Databricks Free Edition:
# entrenamos solo con variables numericas y temporales derivadas.
categorical_cols = []

print("Variables numericas usadas:")
print(numeric_cols)

imputed_numeric = [f"{c}_imputed" for c in numeric_cols]

imputer = Imputer(
    inputCols=numeric_cols,
    outputCols=imputed_numeric
).setStrategy("median")

assembler = VectorAssembler(
    inputCols=imputed_numeric,
    outputCol="features_raw",
    handleInvalid="keep"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features"
)

models = {
    "logistic_regression": LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=30,
        regParam=0.10,
        elasticNetParam=0.0
    ),
    "random_forest": RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=50,
        maxDepth=6,
        seed=42
    ),
    "gradient_boosting": GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=30,
        maxDepth=3,
        seed=42
    ),
}

pipelines = {
    name: Pipeline(stages=[imputer, assembler, scaler, model])
    for name, model in models.items()
}

# COMMAND ----------

APPROVED_FEATURES = [
    "edad_ingreso_asegurado",
    "edad_actual_asegurado",
    "vigencia_poliza",
    "vigencia_certificado",
    "Sum_Valor_Reservas_Inicial",
    "A_o",
    "dias_siniestro_a_apertura",
    "dias_siniestro_a_notificacion",
    "dias_notificacion_a_apertura",
    "dias_vigencia_cert_a_siniestro",
    "dias_vigencia_pol_a_siniestro",
    "dias_expedicion_a_siniestro",
    "delta_edad",
    "edad_actual_invalida",
    "log_reserva_inicial",
    "reserva_inicial_es_cero",
]

missing_features = [
    column
    for column in APPROVED_FEATURES
    if column not in train_df.columns
]

unexpected_features = [
    column
    for column in numeric_cols
    if column not in APPROVED_FEATURES
]

if missing_features:
    raise ValueError(
        f"Faltan variables obligatorias del modelo: {missing_features}"
    )

if unexpected_features:
    raise ValueError(
        f"El pipeline generó variables no aprobadas: {unexpected_features}"
    )

if numeric_cols != APPROVED_FEATURES:
    raise ValueError(
        "El orden de las variables no coincide con el contrato aprobado"
    )

print("Contrato del modelo aprobado correctamente")
train_df.select(APPROVED_FEATURES).printSchema()

# COMMAND ----------

import inspect
import mlflow
import mlflow.spark

print("Versión de MLflow:", mlflow.__version__)
print("Firma de mlflow.spark.log_model:")
print(inspect.signature(mlflow.spark.log_model))

# COMMAND ----------

evaluator_roc = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

evaluator_pr = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)

results = []
best_model = None
best_model_name = None
best_pr = -1.0

for name, pipeline in pipelines.items():
    print(f"Entrenando {name}...")
    fitted_model = pipeline.fit(train_df)
    pred = fitted_model.transform(test_df)
    pred = pred.withColumn("score_fraude", vector_to_array(F.col("probability"))[1])

    roc = evaluator_roc.evaluate(pred)
    pr = evaluator_pr.evaluate(pred)

    pred_label = pred.withColumn(
        "pred_label",
        F.when(F.col("score_fraude") >= 0.5, 1.0).otherwise(0.0)
    )

    cm = pred_label.groupBy("label", "pred_label").count()
    display(cm.withColumn("model", F.lit(name)))

    results.append((name, roc, pr))

    if pr > best_pr:
        best_pr = pr
        best_model_name = name
        best_model = fitted_model

metrics_df = spark.createDataFrame(
    results,
    ["model", "roc_auc", "area_under_pr"]
).orderBy(F.desc("area_under_pr"))

display(metrics_df)

print(f"Mejor modelo: {best_model_name}")

# COMMAND ----------

print("best_model existe:", "best_model" in globals())
print("Mejor modelo aprobado:", best_model_name if "best_model_name" in globals() else "No disponible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lift por deciles y segmentos de riesgo

# COMMAND ----------

scored = best_model.transform(test_df)
scored = scored.withColumn("score_fraude", vector_to_array(F.col("probability"))[1])

w_score = Window.orderBy(F.desc("score_fraude"))
scored = scored.withColumn("rank_score", F.row_number().over(w_score))
scored = scored.withColumn("decil", F.ntile(10).over(w_score))

base_rate = scored.agg(F.avg("label").alias("base_rate")).first()["base_rate"]

lift = (
    scored.groupBy("decil")
    .agg(
        F.count("*").alias("casos"),
        F.sum("label").alias("fraudes"),
        F.min("score_fraude").alias("score_min"),
        F.max("score_fraude").alias("score_max"),
        F.avg("label").alias("tasa_fraude"),
    )
    .withColumn("lift", F.col("tasa_fraude") / F.lit(base_rate))
    .orderBy("decil")
)

display(lift)

segment_cols = [
    "Ramo_Desc",
    "Nombre_Canal_Comercial",
    "REGIONAL",
    "SUCURSAL",
    "Cobertura",
    "Tipo_apertura",
    "SEXO_asegurado",
    "Ind_Pago_Automatico",
]

for c in segment_cols:
    if c in scored.columns:
        display(
            scored.groupBy(c)
            .agg(F.count("*").alias("casos"), F.avg("label").alias("tasa_fraude"), F.avg("score_fraude").alias("score_promedio"))
            .filter(F.col("casos") >= 30)
            .orderBy(F.desc("tasa_fraude"))
            .limit(10)
        )

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.fraude_prod.mlflow_tmp
COMMENT 'Almacenamiento temporal para registrar modelos Spark con MLflow'
""")

MLFLOW_TMP_PATH = (
    "/Volumes/workspace/fraude_prod/"
    "mlflow_tmp/spark_models"
)

dbutils.fs.mkdirs(MLFLOW_TMP_PATH)

print("Volumen temporal preparado:")
print(MLFLOW_TMP_PATH)

# COMMAND ----------

from pyspark.sql import functions as F
from mlflow.models import infer_signature

MODEL_NAME = (
    "workspace.fraude_prod."
    "fraude_random_forest"
)

# Estandarizamos todas las entradas numéricas como double.
input_example_spark = (
    train_df
    .select([
        F.col(column).cast("double").alias(column)
        for column in APPROVED_FEATURES
    ])
    .limit(10)
)

output_example_spark = (
    best_model
    .transform(input_example_spark)
    .select(
        F.col("prediction")
        .cast("double")
        .alias("prediction")
    )
)

input_example = input_example_spark.toPandas()
output_example = output_example_spark.toPandas()

model_signature = infer_signature(
    input_example,
    output_example
)

print("Modelo a registrar:", MODEL_NAME)
print("Columnas de entrada:", list(input_example.columns))
print("Firma creada correctamente:")
print(model_signature)

# COMMAND ----------

import mlflow
import mlflow.spark

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(
    run_name=(
        "registro_fraude_random_"
        "forest_aprobado_v1"
    )
) as run:

    mlflow.set_tags({
        "estado_validacion": "aprobado",
        "caso_uso": (
            "priorizacion_fraude_seguros"
        ),
        "entorno_objetivo": "produccion",
        "tipo_scoring": "batch_y_online",
        "algoritmo": "random_forest",
    })

    mlflow.log_params({
        "num_trees": 50,
        "max_depth": 6,
        "seed": 42,
        "numero_variables": len(
            APPROVED_FEATURES
        ),
    })

    mlflow.log_metrics({
        "roc_auc_aprobado":
            0.7850787527927352,
        "area_under_pr_aprobado":
            0.6127730517022864,
        "captura_fraude_top_20_pct":
            0.418,
    })

    model_info = mlflow.spark.log_model(
        spark_model=best_model,
        artifact_path="model",
        registered_model_name=MODEL_NAME,
        signature=model_signature,
        input_example=input_example,
        metadata={
            "estado": "aprobado",
            "finalidad": (
                "priorizacion_de_reclamaciones"
            ),
            "advertencia": (
                "El score prioriza investigacion "
                "y no confirma fraude"
            ),
        },
        dfs_tmpdir=MLFLOW_TMP_PATH,
        await_registration_for=300,
    )

    RUN_ID = run.info.run_id

print("Modelo registrado correctamente")
print("Run ID:", RUN_ID)
print("URI:", model_info.model_uri)

# COMMAND ----------

import os
import mlflow

MLFLOW_TMP_PATH = (
    "/Volumes/workspace/fraude_prod/"
    "mlflow_tmp/spark_models"
)

os.environ["MLFLOW_DFS_TMP"] = MLFLOW_TMP_PATH

mlflow.set_registry_uri("databricks-uc")

print(
    "MLFLOW_DFS_TMP:",
    os.environ["MLFLOW_DFS_TMP"]
)

# COMMAND ----------

from mlflow import MlflowClient

MODEL_NAME = (
    "workspace.fraude_prod."
    "fraude_random_forest"
)

client = MlflowClient()

versions = list(
    client.search_model_versions(
        f"name='{MODEL_NAME}'"
    )
)

print("Número de versiones:", len(versions))

for version in versions:
    print(
        "Versión:",
        version.version,
        "| Estado:",
        version.status,
        "| Run ID:",
        version.run_id
    )

# COMMAND ----------

versions = list(
    client.search_model_versions(
        f"name='{MODEL_NAME}'"
    )
)

ready_versions = [
    version
    for version in versions
    if version.status == "READY"
]

if not ready_versions:
    raise ValueError(
        "No existe una versión READY"
    )

latest_version = max(
    ready_versions,
    key=lambda item: int(item.version)
)

MODEL_VERSION = latest_version.version

client.set_registered_model_alias(
    name=MODEL_NAME,
    alias="Champion",
    version=MODEL_VERSION,
)

client.set_model_version_tag(
    name=MODEL_NAME,
    version=MODEL_VERSION,
    key="estado_validacion",
    value="aprobado",
)

client.set_model_version_tag(
    name=MODEL_NAME,
    version=MODEL_VERSION,
    key="uso_autorizado",
    value="scoring_productivo",
)

print("Modelo:", MODEL_NAME)
print("Versión:", MODEL_VERSION)
print("Alias: Champion")
print(
    "URI productiva:",
    f"models:/{MODEL_NAME}@Champion"
)

# COMMAND ----------

import mlflow
import mlflow.spark

from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

MODEL_URI = (
    "models:/workspace.fraude_prod."
    "fraude_random_forest@Champion"
)

production_model = mlflow.spark.load_model(
    MODEL_URI,
    dfs_tmpdir=MLFLOW_TMP_PATH,
)

print("Modelo Champion cargado correctamente")

# COMMAND ----------

validation_sample = (
    test_df
    .select([
        F.col(column)
        .cast("double")
        .alias(column)
        for column in APPROVED_FEATURES
    ])
    .limit(100)
    .withColumn(
        "_validation_id",
        F.monotonically_increasing_id()
    )
)

original_scores = (
    best_model
    .transform(validation_sample)
    .withColumn(
        "score_original",
        vector_to_array(
            F.col("probability")
        )[1]
    )
    .select(
        "_validation_id",
        "score_original"
    )
)

registered_scores = (
    production_model
    .transform(validation_sample)
    .withColumn(
        "score_registrado",
        vector_to_array(
            F.col("probability")
        )[1]
    )
    .select(
        "_validation_id",
        "score_registrado"
    )
)

comparison = (
    original_scores
    .join(
        registered_scores,
        "_validation_id"
    )
    .withColumn(
        "diferencia",
        F.abs(
            F.col("score_original")
            - F.col("score_registrado")
        )
    )
)

max_difference = (
    comparison
    .agg(
        F.max("diferencia")
        .alias("max_difference")
    )
    .first()["max_difference"]
)

display(
    comparison.orderBy(
        F.desc("diferencia")
    )
)

print("Diferencia máxima:", max_difference)

if max_difference is None:
    raise ValueError(
        "No se generaron comparaciones"
    )

if max_difference > 1e-12:
    raise ValueError(
        "El modelo registrado no reproduce "
        "los scores originales"
    )

print(
    "Validación exitosa: Champion reproduce "
    "exactamente el modelo aprobado"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Guardar resultados en Delta

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")

metrics_df.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.fraude_model_metrics")
lift.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.fraude_decile_lift")

scored.select(
    *[c for c in scored.columns if c in ["label", "score_fraude", "decil", "Ramo_Desc", "Nombre_Canal_Comercial", "REGIONAL", "SUCURSAL", "Cobertura", "Tipo_apertura"]],
).write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.fraude_scored_test")

print("Tablas guardadas:")
print(f"- {OUTPUT_SCHEMA}.fraude_model_metrics")
print(f"- {OUTPUT_SCHEMA}.fraude_decile_lift")
print(f"- {OUTPUT_SCHEMA}.fraude_scored_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lectura de negocio
# MAGIC
# MAGIC - El score permite priorizar los casos con mayor probabilidad de fraude.
# MAGIC - El top decil concentra una tasa de fraude superior a la tasa base si el modelo esta ordenando correctamente.
# MAGIC - La salida recomendada para negocio es: identificador operativo, score, decil, prioridad, fecha de scoring y version del modelo.
# MAGIC - El modelo no prueba fraude: focaliza investigacion.
