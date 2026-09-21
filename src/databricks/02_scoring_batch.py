# Databricks notebook source
# Código fuente exportado para revisión y control de versiones.

# COMMAND ----------

# Databricks notebook source

import os
import re
import uuid
import mlflow
import mlflow.spark
from mlflow import MlflowClient
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array


# Parámetros operativos del proceso
dbutils.widgets.text(
    "input_table",
    "workspace.fraude_prod.reclamaciones_entrada_demo",
    "Tabla de entrada",
)

dbutils.widgets.text(
    "output_table",
    "workspace.fraude_prod.scores_reclamaciones",
    "Tabla de scores",
)

dbutils.widgets.text(
    "runs_table",
    "workspace.fraude_prod.scoring_runs",
    "Tabla de ejecuciones",
)

dbutils.widgets.text(
    "model_uri",
    (
        "models:/workspace.fraude_prod."
        "fraude_random_forest@Champion"
    ),
    "Modelo aprobado",
)

dbutils.widgets.text(
    "high_threshold",
    "0.4296664901149328",
    "Umbral riesgo alto",
)

dbutils.widgets.text(
    "medium_threshold",
    "0.38046251630548705",
    "Umbral riesgo medio",
)

dbutils.widgets.text(
    "run_id",
    "",
    "Identificador de ejecución",
)

INPUT_TABLE = dbutils.widgets.get("input_table")
OUTPUT_TABLE = dbutils.widgets.get("output_table")
RUNS_TABLE = dbutils.widgets.get("runs_table")
MODEL_URI = dbutils.widgets.get("model_uri")

HIGH_THRESHOLD = float(
    dbutils.widgets.get("high_threshold")
)

MEDIUM_THRESHOLD = float(
    dbutils.widgets.get("medium_threshold")
)



MLFLOW_TMP_PATH = (
    "/Volumes/workspace/fraude_prod/"
    "mlflow_tmp/spark_models"
)

os.environ["MLFLOW_DFS_TMP"] = MLFLOW_TMP_PATH
mlflow.set_registry_uri("databricks-uc")



MODEL_NAME = (
    "workspace.fraude_prod."
    "fraude_random_forest"
)

MODEL_ALIAS = "Champion"

registry_client = MlflowClient()

resolved_model = (
    registry_client
    .get_model_version_by_alias(
        name=MODEL_NAME,
        alias=MODEL_ALIAS,
    )
)

RESOLVED_VERSION = resolved_model.version

MODEL_VERSION = (
    f"{MODEL_NAME}:"
    f"{RESOLVED_VERSION}"
)

print("Alias solicitado:", MODEL_ALIAS)
print(
    "Versión concreta:",
    RESOLVED_VERSION,
)

CONFIGURED_RUN_ID = (
    dbutils.widgets
    .get("run_id")
    .strip()
)

RUN_ID = (
    CONFIGURED_RUN_ID
    if CONFIGURED_RUN_ID
    else str(uuid.uuid4())
)

START_TIME = datetime.now(timezone.utc)


def clean_name(name: str) -> str:
    name = name.strip()
    name = name.replace("�", "n")
    name = re.sub(
        r"[^0-9a-zA-Z_]+",
        "_",
        name,
    )
    name = re.sub(
        r"_+",
        "_",
        name,
    ).strip("_")
    return name


print("Configuración del scoring")
print("Run ID:", RUN_ID)
print("Entrada:", INPUT_TABLE)
print("Salida:", OUTPUT_TABLE)
print("Modelo:", MODEL_URI)
print("Umbral alto:", HIGH_THRESHOLD)
print("Umbral medio:", MEDIUM_THRESHOLD)

# COMMAND ----------

from delta.tables import DeltaTable

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

RAW_REQUIRED_COLUMNS = [
    "id_reclamacion",
    "edad_ingreso_asegurado",
    "edad_actual_asegurado",
    "vigencia_poliza",
    "vigencia_certificado",
    "Sum_Valor_Reservas_Inicial",
    "A_o",
    "Fecha_Primera_Vigencia_Cert",
    "fecha_primera_vigencia_pol",
    "FEXPEDICION",
    "FSINIESTRO",
    "F_Notificacion",
    "Fecha_Apertura",
]

FORBIDDEN_COLUMNS = {
    "Fraude_S_N",
    "label",
    "Fecha_Primer_Cierre_Siniestro",
    "estado",
    "Periodo_Reporte",
    "Fecha_de_reporte",
    "Sum_Valor_Reservas",
    "Sum_Valor_Pagos",
}


# Leer la fuente completa.
source_df = spark.table(INPUT_TABLE)


# Validar el esquema antes del filtro incremental.
missing_columns = [
    column
    for column in RAW_REQUIRED_COLUMNS
    if column not in source_df.columns
]

forbidden_present = [
    column
    for column in FORBIDDEN_COLUMNS
    if column in source_df.columns
]

if missing_columns:
    raise ValueError(
        "Faltan columnas obligatorias: "
        f"{missing_columns}"
    )

if forbidden_present:
    raise ValueError(
        "La entrada contiene variables "
        "posteriores o prohibidas: "
        f"{forbidden_present}"
    )


# Excluir casos ya puntuados con esta
# versión concreta del modelo.
if spark.catalog.tableExists(
    OUTPUT_TABLE
):
    already_scored_df = (
        spark.table(OUTPUT_TABLE)
        .filter(
            F.col("version_modelo")
            == F.lit(MODEL_VERSION)
        )
        .select("id_reclamacion")
        .distinct()
    )

    incoming_df = (
        source_df
        .join(
            already_scored_df,
            on="id_reclamacion",
            how="left_anti",
        )
    )
else:
    incoming_df = source_df


INPUT_COUNT = incoming_df.count()

print(
    "Reclamaciones nuevas encontradas:",
    INPUT_COUNT,
)


# Un lote vacío es una ejecución válida.
if INPUT_COUNT == 0:
    NO_DATA_END_TIME = (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
    )

    NO_DATA_START_TIME = (
        START_TIME.replace(tzinfo=None)
    )

    NO_DATA_DURATION = (
        NO_DATA_END_TIME
        - NO_DATA_START_TIME
    ).total_seconds()

    no_data_run_df = spark.createDataFrame(
        [(
            RUN_ID,
            NO_DATA_START_TIME,
            NO_DATA_END_TIME,
            MODEL_VERSION,
            "batch",
            0,
            0,
            0,
            float(NO_DATA_DURATION),
            "NO_DATA",
            "No se encontraron reclamaciones nuevas",
        )],
        """
        id_ejecucion STRING,
        fecha_inicio TIMESTAMP,
        fecha_fin TIMESTAMP,
        version_modelo STRING,
        modo_scoring STRING,
        registros_entrada BIGINT,
        registros_exitosos BIGINT,
        registros_error BIGINT,
        duracion_segundos DOUBLE,
        estado STRING,
        mensaje_error STRING
        """,
    )

    target_runs = DeltaTable.forName(
        spark,
        RUNS_TABLE,
    )

    (
        target_runs
        .alias("target")
        .merge(
            no_data_run_df.alias("source"),
            """
            target.id_ejecucion =
                source.id_ejecucion
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Ejecución terminada sin datos nuevos"
    )
    print("Estado: NO_DATA")
    print("Run ID:", RUN_ID)

    dbutils.notebook.exit(
        f"NO_DATA | Run ID: {RUN_ID}"
    )


# Validaciones aplicadas únicamente
# sobre reclamaciones nuevas.
null_identifiers = (
    incoming_df
    .filter(
        F.col("id_reclamacion").isNull()
        | (
            F.trim(
                F.col("id_reclamacion")
            ) == ""
        )
    )
    .limit(1)
    .count()
)

if null_identifiers > 0:
    raise ValueError(
        "Existen reclamaciones sin "
        "id_reclamacion"
    )

duplicate_identifiers = (
    incoming_df
    .groupBy("id_reclamacion")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_identifiers > 0:
    raise ValueError(
        "Existen id_reclamacion duplicados"
    )

print("Contrato de entrada validado")
print("Registros nuevos:", INPUT_COUNT)
print(
    "Columnas obligatorias:",
    len(RAW_REQUIRED_COLUMNS),
)
print("Identificadores nulos: 0")
print("Identificadores duplicados: 0")

# COMMAND ----------

DATE_COLUMNS = [
    "Fecha_Primera_Vigencia_Cert",
    "fecha_primera_vigencia_pol",
    "FEXPEDICION",
    "FSINIESTRO",
    "F_Notificacion",
    "Fecha_Apertura",
]

prepared_df = incoming_df

# Conversión controlada de fechas.
for column in DATE_COLUMNS:
    prepared_df = prepared_df.withColumn(
        column,
        F.to_date(F.col(column)),
    )


# Variables temporales del modelo aprobado.
prepared_df = prepared_df.withColumn(
    "dias_siniestro_a_apertura",
    F.datediff(
        "Fecha_Apertura",
        "FSINIESTRO",
    ),
)

prepared_df = prepared_df.withColumn(
    "dias_siniestro_a_notificacion",
    F.datediff(
        "F_Notificacion",
        "FSINIESTRO",
    ),
)

prepared_df = prepared_df.withColumn(
    "dias_notificacion_a_apertura",
    F.datediff(
        "Fecha_Apertura",
        "F_Notificacion",
    ),
)

prepared_df = prepared_df.withColumn(
    "dias_vigencia_cert_a_siniestro",
    F.datediff(
        "FSINIESTRO",
        "Fecha_Primera_Vigencia_Cert",
    ),
)

prepared_df = prepared_df.withColumn(
    "dias_vigencia_pol_a_siniestro",
    F.datediff(
        "FSINIESTRO",
        "fecha_primera_vigencia_pol",
    ),
)

prepared_df = prepared_df.withColumn(
    "dias_expedicion_a_siniestro",
    F.datediff(
        "FSINIESTRO",
        "FEXPEDICION",
    ),
)


# Variables derivadas de edad.
prepared_df = prepared_df.withColumn(
    "delta_edad",
    F.col("edad_actual_asegurado")
    - F.col("edad_ingreso_asegurado"),
)

prepared_df = prepared_df.withColumn(
    "edad_actual_invalida",
    F.when(
        F.col("edad_actual_asegurado") < 0,
        F.lit(1.0),
    ).otherwise(F.lit(0.0)),
)


# Variables derivadas de reserva inicial.
prepared_df = prepared_df.withColumn(
    "log_reserva_inicial",
    F.log1p(
        F.greatest(
            F.col(
                "Sum_Valor_Reservas_Inicial"
            ),
            F.lit(0),
        )
    ),
)

prepared_df = prepared_df.withColumn(
    "reserva_inicial_es_cero",
    F.when(
        F.col(
            "Sum_Valor_Reservas_Inicial"
        ) == 0,
        F.lit(1.0),
    ).otherwise(F.lit(0.0)),
)


# Contrato numérico del modelo:
# todas las entradas se entregan como double.
for column in APPROVED_FEATURES:
    prepared_df = prepared_df.withColumn(
        column,
        F.expr(
            f"try_cast(`{column}` as double)"
        ),
    )


missing_features = [
    column
    for column in APPROVED_FEATURES
    if column not in prepared_df.columns
]

if missing_features:
    raise ValueError(
        "No fue posible generar las "
        "variables del modelo: "
        f"{missing_features}"
    )

print(
    "Preparación de variables terminada"
)

prepared_df.select(
    APPROVED_FEATURES
).printSchema()

display(
    prepared_df.select(
        "id_reclamacion",
        *APPROVED_FEATURES,
    ).limit(5)
)

# COMMAND ----------

null_expressions = [
    F.sum(
        F.when(
            F.col(column).isNull()
            | F.isnan(F.col(column)),
            1,
        ).otherwise(0)
    ).alias(column)
    for column in APPROVED_FEATURES
]

null_summary = (
    prepared_df
    .agg(*null_expressions)
    .first()
    .asDict()
)

null_rows = [
    (
        column,
        int(null_summary[column]),
        float(
            null_summary[column]
            / INPUT_COUNT
        ),
    )
    for column in APPROVED_FEATURES
]

quality_df = spark.createDataFrame(
    null_rows,
    [
        "variable",
        "valores_faltantes",
        "porcentaje_faltante",
    ],
)

display(
    quality_df.orderBy(
        F.desc("porcentaje_faltante")
    )
)

print(
    "Control de faltantes terminado"
)

# COMMAND ----------

production_model = mlflow.spark.load_model(
    MODEL_URI,
    dfs_tmpdir=MLFLOW_TMP_PATH,
)

print(
    "Modelo Champion cargado:",
    MODEL_URI,
)

STAGING_TABLE = (
    "workspace.fraude_prod."
    "scoring_stage"
)

SCORE_TIME = datetime.now(timezone.utc)

scored_df = (
    production_model
    .transform(prepared_df)
    .withColumn(
        "score_fraude",
        vector_to_array(
            F.col("probability")
        )[1],
    )
    .withColumn(
        "nivel_riesgo",
        F.when(
            F.col("score_fraude")
            >= F.lit(HIGH_THRESHOLD),
            F.lit("ALTO"),
        )
        .when(
            F.col("score_fraude")
            >= F.lit(MEDIUM_THRESHOLD),
            F.lit("MEDIO"),
        )
        .otherwise(F.lit("BAJO")),
    )
    .withColumn(
        "accion_sugerida",
        F.when(
            F.col("nivel_riesgo")
            == "ALTO",
            F.lit(
                "REVISION_PRIORITARIA"
            ),
        )
        .when(
            F.col("nivel_riesgo")
            == "MEDIO",
            F.lit(
                "REVISION_SECUNDARIA"
            ),
        )
        .otherwise(
            F.lit("FLUJO_ESTANDAR")
        ),
    )
)

batch_output_df = (
    scored_df
    .select(
        F.col("id_reclamacion")
        .cast("string"),
        F.col("score_fraude")
        .cast("double"),
        F.col("nivel_riesgo"),
        F.col("accion_sugerida"),
        F.lit(SCORE_TIME)
        .cast("timestamp")
        .alias("fecha_scoring"),
        F.lit(MODEL_VERSION)
        .alias("version_modelo"),
        F.lit(RUN_ID)
        .alias("id_ejecucion"),
        F.lit("batch")
        .alias("modo_scoring"),
        F.lit("SUCCESS")
        .alias("estado_scoring"),
    )
)

# COMMAND ----------

# Si se repite la misma ejecución,
# retiramos su staging anterior.
if spark.catalog.tableExists(
    STAGING_TABLE
):
    spark.sql(
        f"""
        DELETE FROM {STAGING_TABLE}
        WHERE id_ejecucion = '{RUN_ID}'
        """
    )

# Materialización del lote en Delta.
(
    batch_output_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(STAGING_TABLE)
)

# Desde aquí las validaciones leen
# el resultado ya materializado.
scoring_output_df = (
    spark.table(STAGING_TABLE)
    .filter(
        F.col("id_ejecucion")
        == F.lit(RUN_ID)
    )
)

SCORED_COUNT = scoring_output_df.count()

print(
    "Reclamaciones recibidas:",
    INPUT_COUNT,
)
print(
    "Scores materializados:",
    SCORED_COUNT,
)
print(
    "Staging:",
    STAGING_TABLE,
)

# COMMAND ----------

from delta.tables import DeltaTable

target_scores = DeltaTable.forName(
    spark,
    OUTPUT_TABLE,
)

(
    target_scores
    .alias("target")
    .merge(
        scoring_output_df.alias("source"),
        """
        target.id_reclamacion =
            source.id_reclamacion
        AND target.id_ejecucion =
            source.id_ejecucion
        """,
    )
    .whenNotMatchedInsertAll()
    .execute()
)

published_count = (
    spark.table(OUTPUT_TABLE)
    .filter(
        F.col("id_ejecucion")
        == F.lit(RUN_ID)
    )
    .count()
)

if published_count != SCORED_COUNT:
    raise ValueError(
        "La publicación quedó incompleta. "
        f"Staging={SCORED_COUNT}, "
        f"Publicado={published_count}"
    )

print("Publicación completada")
print("Run ID:", RUN_ID)
print("Scores publicados:", published_count)
print("Tabla:", OUTPUT_TABLE)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType,
)

END_TIME = datetime.now(timezone.utc)

DURATION_SECONDS = (
    END_TIME - START_TIME
).total_seconds()

run_schema = StructType([
    StructField(
        "id_ejecucion",
        StringType(),
        False,
    ),
    StructField(
        "fecha_inicio",
        TimestampType(),
        False,
    ),
    StructField(
        "fecha_fin",
        TimestampType(),
        True,
    ),
    StructField(
        "version_modelo",
        StringType(),
        False,
    ),
    StructField(
        "modo_scoring",
        StringType(),
        False,
    ),
    StructField(
        "registros_entrada",
        LongType(),
        True,
    ),
    StructField(
        "registros_exitosos",
        LongType(),
        True,
    ),
    StructField(
        "registros_error",
        LongType(),
        True,
    ),
    StructField(
        "duracion_segundos",
        DoubleType(),
        True,
    ),
    StructField(
        "estado",
        StringType(),
        False,
    ),
    StructField(
        "mensaje_error",
        StringType(),
        True,
    ),
])

run_record_df = spark.createDataFrame(
    [{
        "id_ejecucion": RUN_ID,
        "fecha_inicio": START_TIME,
        "fecha_fin": END_TIME,
        "version_modelo": MODEL_VERSION,
        "modo_scoring": "batch",
        "registros_entrada": int(
            INPUT_COUNT
        ),
        "registros_exitosos": int(
            published_count
        ),
        "registros_error": int(
            INPUT_COUNT
            - published_count
        ),
        "duracion_segundos": float(
            DURATION_SECONDS
        ),
        "estado": "SUCCESS",
        "mensaje_error": None,
    }],
    schema=run_schema,
)

target_runs = DeltaTable.forName(
    spark,
    RUNS_TABLE,
)

(
    target_runs
    .alias("target")
    .merge(
        run_record_df.alias("source"),
        """
        target.id_ejecucion =
            source.id_ejecucion
        """,
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Ejecución registrada")
print("Estado: SUCCESS")
print(
    "Duración:",
    round(DURATION_SECONDS, 2),
    "segundos",
)

# COMMAND ----------

spark.sql(
    f"""
    DELETE FROM {STAGING_TABLE}
    WHERE id_ejecucion = '{RUN_ID}'
    """
)

remaining_stage_rows = (
    spark.table(STAGING_TABLE)
    .filter(
        F.col("id_ejecucion")
        == F.lit(RUN_ID)
    )
    .count()
)

if remaining_stage_rows != 0:
    raise ValueError(
        "No fue posible limpiar staging"
    )

print(
    "Staging limpiado correctamente"
)
print(
    "Proceso batch finalizado:",
    RUN_ID,
)

# COMMAND ----------

display(
    spark.table(OUTPUT_TABLE)
    .filter(
        F.col("id_ejecucion")
        == F.lit(RUN_ID)
    )
    .groupBy(
        "nivel_riesgo",
        "accion_sugerida",
    )
    .agg(
        F.count("*").alias("casos"),
        F.avg(
            "score_fraude"
        ).alias("score_promedio"),
        F.min(
            "score_fraude"
        ).alias("score_minimo"),
        F.max(
            "score_fraude"
        ).alias("score_maximo"),
    )
    .orderBy(
        F.desc("score_promedio")
    )
)

display(
    spark.table(RUNS_TABLE)
    .filter(
        F.col("id_ejecucion")
        == F.lit(RUN_ID)
    )
)
