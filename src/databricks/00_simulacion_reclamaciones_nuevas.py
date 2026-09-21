# Databricks notebook source
# Código fuente exportado para revisión y control de versiones.

# COMMAND ----------

# MAGIC %md
# MAGIC # Simulador de reclamaciones nuevas
# MAGIC
# MAGIC Genera un lote controlado de reclamaciones para probar el pipeline incremental.
# MAGIC
# MAGIC Este notebook representa temporalmente al sistema fuente. No pertenece al proceso productivo de scoring.

# COMMAND ----------

from pyspark.sql import functions as F

INPUT_TABLE = (
    "workspace.fraude_prod."
    "reclamaciones_entrada_demo"
)

BATCH_ID = "DEMO-INC-001"
BATCH_PREFIX = f"{BATCH_ID}-"

BATCH_SIZE = 100

print("Tabla destino:", INPUT_TABLE)
print("Lote:", BATCH_ID)
print("Tamaño esperado:", BATCH_SIZE)

# COMMAND ----------

existing_df = spark.table(INPUT_TABLE)

# Excluir registros creados previamente
# por este mismo simulador.
base_candidates_df = (
    existing_df
    .filter(
        ~F.col("id_reclamacion")
        .startswith("DEMO-INC-")
    )
)

candidate_df = (
    base_candidates_df
    .orderBy("id_reclamacion")
    .limit(BATCH_SIZE)
)

new_batch_df = (
    candidate_df
    .withColumn(
        "id_reclamacion",
        F.concat(
            F.lit(BATCH_PREFIX),
            F.substring(
                F.sha2(
                    F.col("id_reclamacion"),
                    256,
                ),
                1,
                24,
            ),
        ),
    )
    .withColumn(
        "fecha_ingesta",
        F.current_timestamp(),
    )
)

display(
    new_batch_df.select(
        "id_reclamacion",
        "fecha_ingesta",
    ).limit(10)
)

# COMMAND ----------

existing_ids_df = (
    existing_df
    .select("id_reclamacion")
)

new_batch_to_insert_df = (
    new_batch_df
    .join(
        existing_ids_df,
        on="id_reclamacion",
        how="left_anti",
    )
)

NEW_RECORDS = (
    new_batch_to_insert_df.count()
)

if NEW_RECORDS > 0:
    (
        new_batch_to_insert_df
        .select(*existing_df.columns)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(INPUT_TABLE)
    )

print(
    "Reclamaciones nuevas insertadas:",
    NEW_RECORDS,
)

TOTAL_INPUT_RECORDS = (
    spark.table(INPUT_TABLE).count()
)

print(
    "Total en la tabla de entrada:",
    TOTAL_INPUT_RECORDS,
)

# COMMAND ----------

inserted_batch_df = (
    spark.table(INPUT_TABLE)
    .filter(
        F.col("id_reclamacion")
        .startswith(BATCH_PREFIX)
    )
)

INSERTED_BATCH_COUNT = (
    inserted_batch_df.count()
)

if INSERTED_BATCH_COUNT != BATCH_SIZE:
    raise ValueError(
        "El lote no contiene la cantidad "
        "esperada. "
        f"Esperado={BATCH_SIZE}, "
        f"Encontrado={INSERTED_BATCH_COUNT}"
    )

duplicate_ids = (
    inserted_batch_df
    .groupBy("id_reclamacion")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_ids > 0:
    raise ValueError(
        "El lote contiene identificadores "
        "duplicados"
    )

print("Lote verificado correctamente")
print("Registros:", INSERTED_BATCH_COUNT)
print("Duplicados: 0")
