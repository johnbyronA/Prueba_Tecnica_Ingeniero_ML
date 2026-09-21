# Databricks notebook source
# Código fuente exportado para revisión y control de versiones.

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from datetime import datetime, timezone

mlflow.set_registry_uri("databricks-uc")

MODEL_NAME = "workspace.fraude_prod.fraude_random_forest"
MODEL_ALIAS = "Champion"

SCORES_TABLE = "workspace.fraude_prod.scores_reclamaciones"
RUNS_TABLE = "workspace.fraude_prod.scoring_runs"
INPUT_TABLE = "workspace.fraude_prod.reclamaciones_entrada_demo"

HIGH_THRESHOLD = 0.4296664901149328
MEDIUM_THRESHOLD = 0.38046251630548705

client = MlflowClient()

champion = client.get_model_version_by_alias(
    MODEL_NAME,
    MODEL_ALIAS
)

print("Configuración del monitoreo")
print("Fecha de análisis:", datetime.now(timezone.utc).isoformat())
print("Modelo:", MODEL_NAME)
print("Alias:", MODEL_ALIAS)
print("Versión productiva actual:", champion.version)
print("Run ID:", champion.run_id)
print("Umbral alto:", HIGH_THRESHOLD)
print("Umbral medio:", MEDIUM_THRESHOLD)

# COMMAND ----------

reference_row = spark.sql("""
    SELECT
        id_ejecucion,
        fecha_scoring,
        reclamaciones,
        score_promedio,
        score_p50,
        score_p90,
        score_p95,
        porcentaje_alto,
        porcentaje_medio,
        porcentaje_bajo,
        version_modelo
    FROM workspace.fraude_prod.monitoreo_distribucion_scores
    ORDER BY reclamaciones DESC
    LIMIT 1
""").first()

REFERENCE_EXECUTION_ID = reference_row["id_ejecucion"]

print("Línea base seleccionada")
print("Ejecución:", REFERENCE_EXECUTION_ID)
print("Registros:", reference_row["reclamaciones"])
print("Score promedio:", reference_row["score_promedio"])
print("Score P90:", reference_row["score_p90"])
print("Porcentaje alto:", reference_row["porcentaje_alto"])
print("Versión del modelo:", reference_row["version_modelo"])

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            fecha_inicio,
            fecha_fin,
            version_modelo,
            modo_scoring,
            registros_entrada,
            registros_exitosos,
            registros_error,
            duracion_minutos,
            porcentaje_exito,
            estado,
            estado_monitoreo,
            mensaje_error
        FROM workspace.fraude_prod.monitoreo_ejecuciones
        ORDER BY fecha_inicio DESC
    """)
)

# COMMAND ----------

from pyspark.sql import functions as F

baseline_scores = (
    spark.table(SCORES_TABLE)
    .filter(F.col("id_ejecucion") == REFERENCE_EXECUTION_ID)
    .select(
        "id_reclamacion",
        F.col("score_fraude").cast("double")
    )
)

baseline_count = baseline_scores.count()

baseline_high_count = (
    baseline_scores
    .filter(F.col("score_fraude") >= HIGH_THRESHOLD)
    .count()
)

baseline_high_pct = (
    100.0 * baseline_high_count / baseline_count
)

# El escenario indica un aumento relativo del 40%.
target_high_pct = min(
    100.0,
    baseline_high_pct * 1.40
)

# Buscamos cuánto desplazar el score para aproximarnos
# naturalmente al porcentaje objetivo.
target_lower_quantile = 1.0 - (target_high_pct / 100.0)

cutoff_score = baseline_scores.approxQuantile(
    "score_fraude",
    [target_lower_quantile],
    0.0
)[0]

score_shift = max(
    0.0,
    HIGH_THRESHOLD - cutoff_score + 0.000001
)

print("Configuración del incidente simulado")
print("Registros:", baseline_count)
print("Porcentaje alto de referencia:", round(baseline_high_pct, 2))
print("Porcentaje alto objetivo:", round(target_high_pct, 2))
print("Punto de corte encontrado:", cutoff_score)
print("Desplazamiento aplicado al score:", score_shift)

# COMMAND ----------

baseline_period = (
    baseline_scores
    .select(
        "id_reclamacion",
        F.lit("REFERENCIA").alias("periodo"),
        F.col("score_fraude").alias("score_monitoreado"),
        F.when(
            F.col("score_fraude") >= HIGH_THRESHOLD,
            F.lit(1)
        ).otherwise(F.lit(0)).alias("marcado_fraude"),
        F.lit(REFERENCE_EXECUTION_ID).alias("source_execution_id")
    )
)

current_period = (
    baseline_scores
    .withColumn(
        "score_actual",
        F.least(
            F.lit(1.0),
            F.col("score_fraude") + F.lit(score_shift)
        )
    )
    .select(
        "id_reclamacion",
        F.lit("ACTUAL_4_MESES").alias("periodo"),
        F.col("score_actual").alias("score_monitoreado"),
        F.when(
            F.col("score_actual") >= HIGH_THRESHOLD,
            F.lit(1)
        ).otherwise(F.lit(0)).alias("marcado_fraude"),
        F.lit("INCIDENTE_SIMULADO").alias("source_execution_id")
    )
)

incident_scores = baseline_period.unionByName(
    current_period
)

(
    incident_scores.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.fraude_prod.monitoring_incident_scores_demo"
    )
)

print("Tabla aislada creada correctamente")
print(
    "workspace.fraude_prod.monitoring_incident_scores_demo"
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            periodo,
            COUNT(*) AS casos,
            ROUND(AVG(score_monitoreado), 4)
                AS score_promedio,
            ROUND(
                PERCENTILE_APPROX(score_monitoreado, 0.50),
                4
            ) AS score_p50,
            ROUND(
                PERCENTILE_APPROX(score_monitoreado, 0.90),
                4
            ) AS score_p90,
            SUM(marcado_fraude) AS casos_marcados_fraude,
            ROUND(
                100.0 * AVG(marcado_fraude),
                2
            ) AS porcentaje_marcado_fraude
        FROM workspace.fraude_prod.monitoring_incident_scores_demo
        GROUP BY periodo
        ORDER BY periodo DESC
    """)
)

# COMMAND ----------

baseline_ids = baseline_scores.select("id_reclamacion")

baseline_features = (
    spark.table(INPUT_TABLE)
    .join(
        baseline_ids,
        on="id_reclamacion",
        how="inner"
    )
    .select(
        "id_reclamacion",

        F.col("edad_actual_asegurado")
            .cast("double")
            .alias("edad_actual_asegurado"),

        F.col("vigencia_poliza")
            .cast("double")
            .alias("vigencia_poliza"),

        F.col("Sum_Valor_Reservas_Inicial")
            .cast("double")
            .alias("reserva_inicial"),

        F.datediff(
            F.col("F_Notificacion"),
            F.col("FSINIESTRO")
        ).cast("double").alias(
            "dias_siniestro_a_notificacion"
        ),

        F.col("Nombre_Canal_Comercial")
            .alias("canal_comercial"),

        F.col("REGIONAL")
            .alias("regional")
    )
    .withColumn(
        "log_reserva_inicial",
        F.log1p(
            F.greatest(
                F.col("reserva_inicial"),
                F.lit(0.0)
            )
        )
    )
    .withColumn(
        "periodo",
        F.lit("REFERENCIA")
    )
)

# COMMAND ----------

current_features = (
    baseline_features
    .drop("periodo")
    .withColumn(
        "grupo_simulacion",
        F.pmod(
            F.xxhash64("id_reclamacion"),
            F.lit(100)
        )
    )
    .withColumn(
        "reserva_inicial",
        F.when(
            F.col("grupo_simulacion") < 60,
            F.col("reserva_inicial") * F.lit(1.80)
        ).otherwise(
            F.col("reserva_inicial")
        )
    )
    .withColumn(
        "dias_siniestro_a_notificacion",
        F.when(
            F.col("grupo_simulacion") < 50,
            F.col("dias_siniestro_a_notificacion")
            + F.lit(10.0)
        ).otherwise(
            F.col("dias_siniestro_a_notificacion")
        )
    )
    .withColumn(
        "canal_comercial",
        F.when(
            F.col("grupo_simulacion") < 30,
            F.lit("CANAL_DIGITAL_SIMULADO")
        ).otherwise(
            F.col("canal_comercial")
        )
    )
    .withColumn(
        "log_reserva_inicial",
        F.log1p(
            F.greatest(
                F.col("reserva_inicial"),
                F.lit(0.0)
            )
        )
    )
    .drop("grupo_simulacion")
    .withColumn(
        "periodo",
        F.lit("ACTUAL_4_MESES")
    )
)

incident_features = baseline_features.unionByName(
    current_features
)

(
    incident_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.fraude_prod.monitoring_incident_features_demo"
    )
)

print("Tabla de variables creada")
print(
    "workspace.fraude_prod.monitoring_incident_features_demo"
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            periodo,
            COUNT(*) AS registros,
            ROUND(AVG(edad_actual_asegurado), 2)
                AS edad_promedio,
            ROUND(AVG(vigencia_poliza), 2)
                AS vigencia_promedio,
            ROUND(AVG(reserva_inicial), 2)
                AS reserva_promedio,
            ROUND(AVG(log_reserva_inicial), 4)
                AS log_reserva_promedio,
            ROUND(
                AVG(dias_siniestro_a_notificacion),
                2
            ) AS dias_notificacion_promedio,
            COUNT(DISTINCT canal_comercial)
                AS canales_distintos,
            COUNT(DISTINCT regional)
                AS regionales_distintas
        FROM workspace.fraude_prod.monitoring_incident_features_demo
        GROUP BY periodo
        ORDER BY periodo DESC
    """)
)

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

features_pdf = (
    spark.table(
        "workspace.fraude_prod.monitoring_incident_features_demo"
    )
    .toPandas()
)

reference_pdf = features_pdf[
    features_pdf["periodo"] == "REFERENCIA"
].copy()

current_pdf = features_pdf[
    features_pdf["periodo"] == "ACTUAL_4_MESES"
].copy()

EPSILON = 1e-6


def numeric_psi(reference, current, bins=10):
    reference = (
        pd.to_numeric(reference, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .to_numpy()
    )

    current = (
        pd.to_numeric(current, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .to_numpy()
    )

    if len(reference) == 0 or len(current) == 0:
        return None

    quantiles = np.linspace(0.0, 1.0, bins + 1)
    edges = np.unique(
        np.quantile(reference, quantiles)
    )

    # Si la variable es constante, no presenta cambio distribucional.
    if len(edges) < 2:
        return 0.0

    internal_edges = edges[1:-1]

    histogram_edges = np.concatenate((
        [-np.inf],
        internal_edges,
        [np.inf]
    ))

    reference_counts, _ = np.histogram(
        reference,
        bins=histogram_edges
    )

    current_counts, _ = np.histogram(
        current,
        bins=histogram_edges
    )

    reference_pct = reference_counts / reference_counts.sum()
    current_pct = current_counts / current_counts.sum()

    reference_pct = np.clip(
        reference_pct,
        EPSILON,
        None
    )

    current_pct = np.clip(
        current_pct,
        EPSILON,
        None
    )

    return float(
        np.sum(
            (current_pct - reference_pct)
            * np.log(current_pct / reference_pct)
        )
    )


def categorical_psi(reference, current):
    reference = (
        reference
        .fillna("__NULL__")
        .astype(str)
    )

    current = (
        current
        .fillna("__NULL__")
        .astype(str)
    )

    categories = sorted(
        set(reference.unique())
        | set(current.unique())
    )

    reference_pct = (
        reference.value_counts(normalize=True)
        .reindex(categories, fill_value=0.0)
        .to_numpy()
    )

    current_pct = (
        current.value_counts(normalize=True)
        .reindex(categories, fill_value=0.0)
        .to_numpy()
    )

    reference_pct = np.clip(
        reference_pct,
        EPSILON,
        None
    )

    current_pct = np.clip(
        current_pct,
        EPSILON,
        None
    )

    return float(
        np.sum(
            (current_pct - reference_pct)
            * np.log(current_pct / reference_pct)
        )
    )


def classify_drift(psi, ks_statistic=None, ks_pvalue=None):
    if psi is None:
        return "NO_EVALUABLE"

    # PSI >= 0.25 indica cambio importante.
    if psi >= 0.25:
        return "DRIFT_CRITICO"

    # KS combina magnitud y significancia.
    if (
        ks_statistic is not None
        and ks_statistic >= 0.10
        and ks_pvalue is not None
        and ks_pvalue < 0.05
    ):
        return "DRIFT_CRITICO"

    if psi >= 0.10:
        return "ADVERTENCIA"

    return "ESTABLE"

# COMMAND ----------

numeric_features = [
    "edad_actual_asegurado",
    "vigencia_poliza",
    "reserva_inicial",
    "log_reserva_inicial",
    "dias_siniestro_a_notificacion"
]

categorical_features = [
    "canal_comercial",
    "regional"
]

drift_results = []

for feature in numeric_features:
    reference_values = (
        pd.to_numeric(
            reference_pdf[feature],
            errors="coerce"
        )
        .dropna()
    )

    current_values = (
        pd.to_numeric(
            current_pdf[feature],
            errors="coerce"
        )
        .dropna()
    )

    psi_value = numeric_psi(
        reference_values,
        current_values
    )

    ks_result = ks_2samp(
        reference_values,
        current_values,
        alternative="two-sided",
        method="asymp"
    )

    status = classify_drift(
        psi_value,
        float(ks_result.statistic),
        float(ks_result.pvalue)
    )

    drift_results.append({
        "feature_name": feature,
        "feature_type": "NUMERIC",
        "psi": float(psi_value),
        "ks_statistic": float(ks_result.statistic),
        "ks_pvalue": float(ks_result.pvalue),
        "drift_status": status
    })


for feature in categorical_features:
    psi_value = categorical_psi(
        reference_pdf[feature],
        current_pdf[feature]
    )

    status = classify_drift(psi_value)

    drift_results.append({
        "feature_name": feature,
        "feature_type": "CATEGORICAL",
        "psi": float(psi_value),
        "ks_statistic": None,
        "ks_pvalue": None,
        "drift_status": status
    })

drift_results

# COMMAND ----------

scores_pdf = (
    spark.table(
        "workspace.fraude_prod.monitoring_incident_scores_demo"
    )
    .toPandas()
)

reference_scores = scores_pdf[
    scores_pdf["periodo"] == "REFERENCIA"
]["score_monitoreado"]

current_scores = scores_pdf[
    scores_pdf["periodo"] == "ACTUAL_4_MESES"
]["score_monitoreado"]

score_psi = numeric_psi(
    reference_scores,
    current_scores
)

score_ks = ks_2samp(
    reference_scores,
    current_scores,
    alternative="two-sided",
    method="asymp"
)

drift_results.append({
    "feature_name": "score_fraude",
    "feature_type": "PREDICTION",
    "psi": float(score_psi),
    "ks_statistic": float(score_ks.statistic),
    "ks_pvalue": float(score_ks.pvalue),
    "drift_status": classify_drift(
        score_psi,
        float(score_ks.statistic),
        float(score_ks.pvalue)
    )
})

# COMMAND ----------

drift_results_df = spark.createDataFrame(
    drift_results
)

(
    drift_results_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.fraude_prod.monitoring_drift_results_demo"
    )
)

display(
    spark.sql("""
        SELECT
            feature_name,
            feature_type,
            ROUND(psi, 4) AS psi,
            ROUND(ks_statistic, 4) AS ks_statistic,
            ks_pvalue,
            drift_status
        FROM workspace.fraude_prod.monitoring_drift_results_demo
        ORDER BY
            CASE drift_status
                WHEN 'DRIFT_CRITICO' THEN 1
                WHEN 'ADVERTENCIA' THEN 2
                WHEN 'ESTABLE' THEN 3
                ELSE 4
            END,
            psi DESC
    """)
)

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.monitoring_metric_policy (
    metric_order INT,
    monitoring_layer STRING,
    metric_name STRING,
    warning_condition STRING,
    critical_condition STRING,
    expected_response STRING
)
USING DELTA
COMMENT 'Política de métricas y respuestas para monitoreo del modelo de fraude';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.monitoring_metric_policy
VALUES
(
    1,
    'OPERATIONAL',
    'Estado y duración del job',
    'Duración superior al histórico',
    'Job fallido o SLA incumplido',
    'Revisar compute, dependencias y logs'
),
(
    2,
    'OPERATIONAL',
    'Volumen y frescura',
    'Variación superior al 20%',
    'Ausencia de datos o retraso crítico',
    'Validar fuentes, ingestión y particiones'
),
(
    3,
    'DATA_QUALITY',
    'Nulos, duplicados y esquema',
    'Aumento frente a referencia',
    'Clave duplicada, campo obligatorio o esquema incompatible',
    'Aislar registros y corregir el pipeline'
),
(
    4,
    'DATA_QUALITY',
    'Rangos y reglas temporales',
    'Aumento de valores atípicos',
    'Cambio incompatible con la lógica de negocio',
    'Validar fuente y reglas de transformación'
),
(
    5,
    'DATA_DRIFT',
    'PSI numérico o categórico',
    'PSI entre 0.10 y 0.25',
    'PSI igual o superior a 0.25',
    'Investigar variables y segmentos afectados'
),
(
    6,
    'DATA_DRIFT',
    'KS numérico',
    'KS entre 0.05 y 0.10',
    'KS igual o superior a 0.10 con p-value menor a 0.05',
    'Comparar distribuciones y causa del cambio'
),
(
    7,
    'PREDICTION_DRIFT',
    'Score promedio, percentiles y distribución',
    'Cambio persistente frente a referencia',
    'PSI del score igual o superior a 0.25',
    'Validar entradas, versión, umbrales y composición'
),
(
    8,
    'PREDICTION_DRIFT',
    'Tasa de casos marcados',
    'Aumento relativo superior al 20%',
    'Aumento relativo superior al 40%',
    'Revisar capacidad operativa, umbral y drift'
),
(
    9,
    'PERFORMANCE',
    'ROC-AUC y AUPR',
    'Caída relativa superior al 5%',
    'Caída relativa superior al 10%',
    'Analizar concept drift y posible reentrenamiento'
),
(
    10,
    'PERFORMANCE',
    'Precisión, recall y falsos positivos',
    'Desviación frente al objetivo aprobado',
    'Incumplimiento persistente del SLA de negocio',
    'Recalibrar umbral o reentrenar según la causa'
),
(
    11,
    'CALIBRATION',
    'Calibración y Brier score',
    'Probabilidades descalibradas por segmento',
    'Descalibración general persistente',
    'Aplicar recalibración o reentrenamiento'
),
(
    12,
    'BUSINESS',
    'Lift, fraude confirmado y valor recuperado',
    'Caída frente a la línea base',
    'El modelo deja de generar valor neto',
    'Revisar estrategia, capacidad y modelo'
),
(
    13,
    'BUSINESS',
    'Capacidad de investigación',
    'Cola superior a la capacidad diaria',
    'Acumulación que incumple el SLA',
    'Ajustar umbrales o priorización'
),
(
    14,
    'SEGMENT',
    'Desempeño por canal, producto y regional',
    'Deterioro localizado',
    'Deterioro material o inequitativo',
    'Investigar sesgo, cobertura o modelos segmentados'
);

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.monitoring_metric_policy
ORDER BY metric_order;

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.retraining_decision_policy (
    scenario_order INT,
    observed_condition STRING,
    interpretation STRING,
    recommended_action STRING,
    retrain_now BOOLEAN
)
USING DELTA
COMMENT 'Matriz de decisión para reentrenamiento del modelo de fraude';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.retraining_decision_policy
VALUES
(
    1,
    'Error de pipeline, esquema, unidad o transformación',
    'Incidente técnico, no degradación del modelo',
    'Corregir datos o código y reprocesar',
    FALSE
),
(
    2,
    'Cambio accidental de modelo o umbral',
    'Incidente de configuración',
    'Restaurar versión o configuración aprobada',
    FALSE
),
(
    3,
    'Data drift aislado sin etiquetas ni pérdida de desempeño',
    'La población cambió, pero no está probado que el modelo falle',
    'Investigar, aumentar monitoreo y ejecutar shadow scoring',
    FALSE
),
(
    4,
    'Score drift con sobrecarga de investigadores',
    'Puede ser un problema de calibración o capacidad',
    'Revisar umbral, calibración y reglas de priorización',
    FALSE
),
(
    5,
    'Data drift persistente y caída material de AUPR, recall o lift',
    'El modelo perdió capacidad predictiva en la nueva población',
    'Reentrenar con datos recientes y etiquetas maduras',
    TRUE
),
(
    6,
    'Concept drift confirmado',
    'Cambió la relación entre variables y fraude real',
    'Revisar variables, reentrenar y validar contra Champion',
    TRUE
),
(
    7,
    'Nuevo producto, canal o proceso no representado',
    'La población está fuera del dominio de entrenamiento',
    'Recolectar etiquetas y reentrenar o construir modelo segmentado',
    TRUE
),
(
    8,
    'Revisión periódica sin degradación material',
    'El paso del tiempo por sí solo no obliga a cambiar el modelo',
    'Revalidar y mantener Champion si sigue cumpliendo',
    FALSE
);

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.retraining_decision_policy
ORDER BY scenario_order;

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.monitoring_incident_assessment_demo (
    assessment_order INT,
    priority STRING,
    evaluation_area STRING,
    finding STRING,
    evidence STRING,
    conclusion STRING,
    recommended_action STRING,
    responsible_role STRING
)
USING DELTA
COMMENT 'Evaluación ejecutiva del incidente simulado de monitoreo operacional';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.monitoring_incident_assessment_demo
VALUES
(
    1,
    'P0',
    'INTEGRIDAD_TECNICA',
    'Modelo, alias y umbrales sin cambios',
    'Champion continúa en versión 1 y los jobs exitosos no presentan errores',
    'No se observa una causa técnica inmediata',
    'Confirmar cambios de código, esquema y fuentes aguas arriba',
    'MLOPS'
),
(
    2,
    'P0',
    'PREDICTION_DRIFT',
    'Cambio crítico en la distribución del score',
    'Score promedio 0.6198 a 0.7157; PSI 1.2324; KS 0.3093',
    'Prediction drift confirmado',
    'Mantener alerta crítica y analizar las variables que impulsan el cambio',
    'DATA_SCIENCE'
),
(
    3,
    'P0',
    'TASA_MARCADA',
    'Aumento relativo de 40 por ciento',
    'Casos marcados pasan de 63.05 a 88.27 por ciento',
    'La cola puede superar la capacidad de investigación',
    'Revisar umbral, capacidad operativa y priorización',
    'FRAUD_OPERATIONS'
),
(
    4,
    'P1',
    'DATA_DRIFT_CATEGORICO',
    'Cambio extremo en canal comercial',
    'PSI de canal comercial igual a 3.851',
    'Data drift categórico confirmado',
    'Validar si existe un nuevo canal o un cambio de codificación',
    'DATA_OWNER'
),
(
    5,
    'P1',
    'DATA_DRIFT_NUMERICO',
    'Cambio en días entre siniestro y notificación',
    'KS igual a 0.1137 con p-value menor a 0.05',
    'La distribución temporal cambió materialmente',
    'Revisar procesos de notificación y composición del portafolio',
    'BUSINESS_AND_DATA'
),
(
    6,
    'P1',
    'CAMBIO_DE_NEGOCIO',
    'La reserva inicial promedio aumenta aproximadamente 50 por ciento',
    'Promedio aproximado de 23.3 millones a 35.1 millones',
    'Existe un cambio económico relevante aunque PSI no supera el límite',
    'Analizar severidad, productos, coberturas y valores extremos',
    'ACTUARIAL_AND_BUSINESS'
),
(
    7,
    'P1',
    'MODEL_PERFORMANCE',
    'No existen etiquetas maduras posteriores al incidente',
    'No se han calculado AUPR, recall, precisión, lift ni calibración actuales',
    'No puede afirmarse todavía que el modelo se degradó',
    'Esperar fraude confirmado y evaluar desempeño por cohorte',
    'MODEL_VALIDATION'
),
(
    8,
    'P2',
    'RETRAINING_DECISION',
    'No reentrenar de inmediato',
    'Existe drift, pero no evidencia de pérdida predictiva con etiquetas',
    'Primero se debe determinar la causa y persistencia',
    'Investigar, monitorear, evaluar etiquetas y usar shadow scoring',
    'MODEL_GOVERNANCE'
);

# COMMAND ----------

%sql

SELECT
    assessment_order,
    priority,
    evaluation_area,
    finding,
    evidence,
    conclusion,
    recommended_action,
    responsible_role
FROM workspace.fraude_prod.monitoring_incident_assessment_demo
ORDER BY assessment_order;

# COMMAND ----------

%sql

CREATE OR REPLACE VIEW workspace.fraude_prod.monitoring_active_alerts_demo AS

SELECT
    feature_name AS alert_source,
    feature_type AS alert_type,
    drift_status AS severity,
    CONCAT(
        'PSI=',
        CAST(ROUND(psi, 4) AS STRING),
        CASE
            WHEN ks_statistic IS NOT NULL
            THEN CONCAT(
                ', KS=',
                CAST(ROUND(ks_statistic, 4) AS STRING)
            )
            ELSE ''
        END
    ) AS evidence,

    CASE
        WHEN feature_name = 'score_fraude'
            THEN 'Revisar distribución, umbral y capacidad operativa'
        WHEN feature_name = 'canal_comercial'
            THEN 'Validar nuevas categorías o cambios de codificación'
        WHEN feature_name = 'dias_siniestro_a_notificacion'
            THEN 'Validar cambios en el proceso de notificación'
        ELSE 'Investigar el cambio de distribución'
    END AS recommended_action

FROM workspace.fraude_prod.monitoring_drift_results_demo

WHERE drift_status IN (
    'DRIFT_CRITICO',
    'ADVERTENCIA'
);

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.monitoring_active_alerts_demo
ORDER BY
    CASE severity
        WHEN 'DRIFT_CRITICO' THEN 1
        WHEN 'ADVERTENCIA' THEN 2
        ELSE 3
    END,
    alert_source;
