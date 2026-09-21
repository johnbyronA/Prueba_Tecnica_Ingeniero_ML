# Databricks notebook source
# Código fuente exportado para revisión y control de versiones.

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from datetime import datetime, timezone

mlflow.set_registry_uri("databricks-uc")

MODEL_NAME = "workspace.fraude_prod.fraude_random_forest"
ALIAS_PRODUCCION = "Champion"
ALIAS_CANDIDATO = "Candidate"

client = MlflowClient()

print("Configuración MLOps")
print("Modelo:", MODEL_NAME)
print("Alias productivo:", ALIAS_PRODUCCION)
print("Fecha de revisión:", datetime.now(timezone.utc).isoformat())
print("Versión de MLflow:", mlflow.__version__)

# COMMAND ----------

versiones = client.search_model_versions(
    filter_string=f"name = '{MODEL_NAME}'"
)

print("Versiones registradas:", len(versiones))

for version in sorted(versiones, key=lambda x: int(x.version)):
    print("-" * 60)
    print("Versión:", version.version)
    print("Run ID:", version.run_id)
    print("Estado:", version.status)
    print("Origen:", version.source)
    print("Creada:", version.creation_timestamp)

try:
    champion = client.get_model_version_by_alias(
        MODEL_NAME,
        ALIAS_PRODUCCION
    )

    print("\nProducción actual")
    print("Alias:", ALIAS_PRODUCCION)
    print("Versión:", champion.version)
    print("Run ID:", champion.run_id)
    print("URI:", f"models:/{MODEL_NAME}@{ALIAS_PRODUCCION}")

except Exception as error:
    print("No fue posible encontrar el alias Champion")
    print(error)

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.model_governance_events (
    event_id STRING,
    event_timestamp TIMESTAMP,
    model_name STRING,
    action STRING,
    previous_version STRING,
    target_version STRING,
    model_alias STRING,
    validation_status STRING,
    approved_by STRING,
    reason STRING,
    metadata_json STRING
)
USING DELTA
COMMENT 'Historial auditable de promociones, cambios de alias y rollback de modelos';

# COMMAND ----------

%sql

MERGE INTO workspace.fraude_prod.model_governance_events AS destino
USING (
    SELECT
        'baseline-fraude-rf-v1' AS event_id,
        CURRENT_TIMESTAMP() AS event_timestamp,
        'workspace.fraude_prod.fraude_random_forest' AS model_name,
        'BASELINE_SNAPSHOT' AS action,
        CAST(NULL AS STRING) AS previous_version,
        '1' AS target_version,
        'Champion' AS model_alias,
        'APPROVED' AS validation_status,
        CURRENT_USER() AS approved_by,
        'Versión productiva encontrada al iniciar el ejercicio de gobierno' AS reason,
        '{"mlflow_version":"3.8.1","environment":"databricks_serverless"}' AS metadata_json
) AS origen
ON destino.event_id = origen.event_id

WHEN NOT MATCHED THEN INSERT (
    event_id,
    event_timestamp,
    model_name,
    action,
    previous_version,
    target_version,
    model_alias,
    validation_status,
    approved_by,
    reason,
    metadata_json
)
VALUES (
    origen.event_id,
    origen.event_timestamp,
    origen.model_name,
    origen.action,
    origen.previous_version,
    origen.target_version,
    origen.model_alias,
    origen.validation_status,
    origen.approved_by,
    origen.reason,
    origen.metadata_json
);

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.model_governance_events
ORDER BY event_timestamp DESC;

# COMMAND ----------

champion = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_PRODUCCION
)

run = client.get_run(champion.run_id)

print("Información reproducible disponible")
print("Modelo:", MODEL_NAME)
print("Versión:", champion.version)
print("Run ID:", champion.run_id)
print("Artifact URI:", run.info.artifact_uri)
print("Experiment ID:", run.info.experiment_id)
print("Estado del run:", run.info.status)

print("\nParámetros registrados")
if run.data.params:
    for nombre, valor in sorted(run.data.params.items()):
        print(f"{nombre}: {valor}")
else:
    print("No hay parámetros registrados en este run")

print("\nMétricas registradas")
if run.data.metrics:
    for nombre, valor in sorted(run.data.metrics.items()):
        print(f"{nombre}: {valor}")
else:
    print("No hay métricas registradas en este run")

print("\nTags relevantes")
tags_relevantes = [
    "mlflow.runName",
    "mlflow.source.name",
    "mlflow.source.type",
    "mlflow.user",
    "git.commit",
    "mlflow.source.git.commit",
]

for tag in tags_relevantes:
    print(f"{tag}: {run.data.tags.get(tag, 'NO_REGISTRADO')}")

# COMMAND ----------

%sql

DESCRIBE HISTORY workspace.default.muestra_base_fraude;

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.model_reproducibility_manifest (
    manifest_id STRING,
    captured_at TIMESTAMP,
    model_name STRING,
    model_version STRING,
    run_id STRING,
    experiment_id STRING,
    source_table STRING,
    source_table_version BIGINT,
    source_table_timestamp TIMESTAMP,
    source_notebook STRING,
    git_commit STRING,
    parameters_json STRING,
    metrics_json STRING,
    random_seed INT,
    mlflow_version STRING,
    model_signature_status STRING,
    environment_status STRING,
    reproducibility_status STRING,
    limitations STRING
)
USING DELTA
COMMENT 'Manifiesto de información necesaria para reproducir cada versión del modelo';

# COMMAND ----------

%sql

MERGE INTO workspace.fraude_prod.model_reproducibility_manifest AS destino
USING (
    SELECT
        'fraude-rf-v1-manifest' AS manifest_id,
        CURRENT_TIMESTAMP() AS captured_at,
        'workspace.fraude_prod.fraude_random_forest' AS model_name,
        '1' AS model_version,
        '5cac913910464058a36a6c466dce8008' AS run_id,
        '4292052633465994' AS experiment_id,
        'workspace.default.muestra_base_fraude' AS source_table,
        CAST(0 AS BIGINT) AS source_table_version,
        CAST('2026-08-22T16:44:45+00:00' AS TIMESTAMP) AS source_table_timestamp,
        '/Users/usuario@empresa.com/01_modelo_fraude_pyspark'
            AS source_notebook,
        CAST(NULL AS STRING) AS git_commit,

        TO_JSON(
            NAMED_STRUCT(
                'max_depth', 6,
                'num_trees', 50,
                'numero_variables', 16,
                'seed', 42
            )
        ) AS parameters_json,

        TO_JSON(
            NAMED_STRUCT(
                'roc_auc', 0.7850787527927352,
                'area_under_pr', 0.6127730517022864,
                'captura_fraude_top_20_pct', 0.418
            )
        ) AS metrics_json,

        42 AS random_seed,
        '3.8.1' AS mlflow_version,
        'REGISTERED' AS model_signature_status,
        'PARTIAL' AS environment_status,
        'PARTIAL' AS reproducibility_status,

        CONCAT(
            'No se registró el commit de Git. ',
            'Se deben versionar el notebook, las dependencias exactas ',
            'y la configuración del runtime para reproducción completa.'
        ) AS limitations
) AS origen

ON destino.manifest_id = origen.manifest_id

WHEN NOT MATCHED THEN INSERT *
;

# COMMAND ----------

%sql

SELECT
    model_name,
    model_version,
    run_id,
    source_table,
    source_table_version,
    source_notebook,
    git_commit,
    parameters_json,
    metrics_json,
    random_seed,
    reproducibility_status,
    limitations
FROM workspace.fraude_prod.model_reproducibility_manifest;

# COMMAND ----------

DEMO_ID = "ejercicio2-candidate-governance-v1"

# Buscar si el candidato de demostración ya existe
candidate_version = None

for version in client.search_model_versions(
    filter_string=f"name = '{MODEL_NAME}'"
):
    detalle = client.get_model_version(
        name=MODEL_NAME,
        version=version.version
    )

    if detalle.tags.get("governance_demo_id") == DEMO_ID:
        candidate_version = detalle.version
        break

# Crear solamente si aún no existe
if candidate_version is None:
    champion = client.get_model_version_by_alias(
        MODEL_NAME,
        ALIAS_PRODUCCION
    )

    source_uri = f"runs:/{champion.run_id}/model"

    nueva_version = mlflow.register_model(
        model_uri=source_uri,
        name=MODEL_NAME,
        await_registration_for=300
    )

    candidate_version = nueva_version.version

    client.set_model_version_tag(
        MODEL_NAME,
        candidate_version,
        "governance_demo_id",
        DEMO_ID
    )

    client.set_model_version_tag(
        MODEL_NAME,
        candidate_version,
        "validation_status",
        "PENDING"
    )

    client.set_model_version_tag(
        MODEL_NAME,
        candidate_version,
        "based_on_version",
        champion.version
    )

    client.set_model_version_tag(
        MODEL_NAME,
        candidate_version,
        "candidate_type",
        "GOVERNANCE_DEMO"
    )

    client.update_model_version(
        name=MODEL_NAME,
        version=candidate_version,
        description=(
            "Versión candidata creada para demostrar promoción y rollback. "
            "Reutiliza el artefacto de la versión 1 y no representa "
            "una mejora analítica."
        )
    )

# Candidate apunta a la nueva versión
client.set_registered_model_alias(
    name=MODEL_NAME,
    alias=ALIAS_CANDIDATO,
    version=candidate_version
)

print("Candidato preparado")
print("Modelo:", MODEL_NAME)
print("Versión candidata:", candidate_version)
print("Alias:", ALIAS_CANDIDATO)
print("URI:", f"models:/{MODEL_NAME}@{ALIAS_CANDIDATO}")

# COMMAND ----------

champion_actual = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_PRODUCCION
)

candidate_actual = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_CANDIDATO
)

print("Estado de los alias")
print(
    f"{ALIAS_PRODUCCION}: versión {champion_actual.version}"
)
print(
    f"{ALIAS_CANDIDATO}: versión {candidate_actual.version}"
)

assert champion_actual.version == "1", (
    "Champion cambió antes de la aprobación"
)

assert candidate_actual.version != champion_actual.version, (
    "Candidate debe ser una versión distinta"
)

print("Validación correcta: producción permanece intacta")

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.model_validation_results (
    validation_id STRING,
    validation_timestamp TIMESTAMP,
    model_name STRING,
    champion_version STRING,
    candidate_version STRING,
    gate_name STRING,
    gate_status STRING,
    observed_value STRING,
    required_value STRING,
    details STRING
)
USING DELTA
COMMENT 'Resultados auditables de las puertas de validación previas a producción';

# COMMAND ----------

from datetime import datetime, timezone

champion = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_PRODUCCION
)

candidate = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_CANDIDATO
)

champion_run = client.get_run(champion.run_id)
candidate_run = client.get_run(candidate.run_id)

champion_metrics = champion_run.data.metrics
candidate_metrics = candidate_run.data.metrics
candidate_params = candidate_run.data.params
candidate_tags = candidate_run.data.tags

resultados = []

def agregar_gate(
    nombre,
    estado,
    observado,
    requerido,
    detalle
):
    resultados.append({
        "validation_id": (
            f"{MODEL_NAME}-v{candidate.version}-{nombre}"
        ),
        "validation_timestamp": datetime.now(timezone.utc),
        "model_name": MODEL_NAME,
        "champion_version": str(champion.version),
        "candidate_version": str(candidate.version),
        "gate_name": nombre,
        "gate_status": estado,
        "observed_value": str(observado),
        "required_value": str(requerido),
        "details": detalle
    })

# 1. Estado del artefacto
agregar_gate(
    "MODEL_READY",
    "PASS" if candidate.status == "READY" else "FAIL",
    candidate.status,
    "READY",
    "La versión debe estar disponible en Model Registry"
)

# 2. No alterar prematuramente producción
agregar_gate(
    "SEPARATE_FROM_CHAMPION",
    "PASS" if candidate.version != champion.version else "FAIL",
    candidate.version,
    f"Distinta de {champion.version}",
    "Candidate y Champion deben ser versiones diferentes"
)

# 3. Ejecución MLflow terminada
agregar_gate(
    "RUN_FINISHED",
    "PASS" if candidate_run.info.status == "FINISHED" else "FAIL",
    candidate_run.info.status,
    "FINISHED",
    "El entrenamiento debe terminar correctamente"
)

# 4. ROC-AUC mínimo
roc_candidate = candidate_metrics.get("roc_auc_aprobado")
agregar_gate(
    "MIN_ROC_AUC",
    "PASS" if roc_candidate is not None and roc_candidate >= 0.75 else "FAIL",
    roc_candidate,
    ">= 0.75",
    "Piso mínimo de discriminación"
)

# 5. Área bajo Precision-Recall mínima
pr_candidate = candidate_metrics.get("area_under_pr_aprobado")
agregar_gate(
    "MIN_AREA_UNDER_PR",
    "PASS" if pr_candidate is not None and pr_candidate >= 0.55 else "FAIL",
    pr_candidate,
    ">= 0.55",
    "Piso mínimo para identificación de fraude"
)

# 6. Captura de fraude en el 20% superior
capture_candidate = candidate_metrics.get(
    "captura_fraude_top_20_pct"
)

agregar_gate(
    "MIN_TOP20_CAPTURE",
    "PASS"
    if capture_candidate is not None and capture_candidate >= 0.35
    else "FAIL",
    capture_candidate,
    ">= 0.35",
    "Capacidad mínima de priorización operativa"
)

# 7. No degradar ROC-AUC más de 0.02
roc_champion = champion_metrics.get("roc_auc_aprobado")
delta_roc = (
    roc_candidate - roc_champion
    if roc_candidate is not None and roc_champion is not None
    else None
)

agregar_gate(
    "MAX_ROC_DEGRADATION",
    "PASS" if delta_roc is not None and delta_roc >= -0.02 else "FAIL",
    delta_roc,
    ">= -0.02",
    "Evita promover una degradación material"
)

# 8. Semilla registrada
seed = candidate_params.get("seed")

agregar_gate(
    "RANDOM_SEED_REGISTERED",
    "PASS" if seed is not None else "FAIL",
    seed,
    "Valor obligatorio",
    "La semilla permite repetir las particiones y el entrenamiento"
)

# 9. Versión de código
git_commit = (
    candidate_tags.get("git.commit")
    or candidate_tags.get("mlflow.source.git.commit")
)

agregar_gate(
    "CODE_VERSION_REGISTERED",
    "PASS" if git_commit else "WARNING",
    git_commit or "NO_REGISTRADO",
    "Commit Git obligatorio",
    (
        "Advertencia aceptada solamente para esta demostración. "
        "En producción bloquearía la promoción."
    )
)

# Decisión consolidada
estados = [resultado["gate_status"] for resultado in resultados]

if "FAIL" in estados:
    decision = "FAIL"
elif "WARNING" in estados:
    decision = "PASS_WITH_EXCEPTION"
else:
    decision = "PASS"

client.set_model_version_tag(
    MODEL_NAME,
    candidate.version,
    "validation_status",
    decision
)

print("Resultado de validación:", decision)
print("Champion:", champion.version)
print("Candidate:", candidate.version)
print()

for resultado in resultados:
    print(
        resultado["gate_status"],
        "|",
        resultado["gate_name"],
        "| observado:",
        resultado["observed_value"]
    )

# COMMAND ----------

validation_df = spark.createDataFrame(resultados)

validation_df.createOrReplaceTempView(
    "validaciones_candidato_actual"
)

spark.sql("""
MERGE INTO workspace.fraude_prod.model_validation_results AS destino
USING validaciones_candidato_actual AS origen
ON destino.validation_id = origen.validation_id

WHEN MATCHED THEN UPDATE SET
    destino.validation_timestamp = origen.validation_timestamp,
    destino.gate_status = origen.gate_status,
    destino.observed_value = origen.observed_value,
    destino.required_value = origen.required_value,
    destino.details = origen.details

WHEN NOT MATCHED THEN INSERT *
""")

display(
    spark.sql("""
        SELECT
            gate_name,
            gate_status,
            observed_value,
            required_value,
            details
        FROM workspace.fraude_prod.model_validation_results
        WHERE candidate_version = (
            SELECT MAX(candidate_version)
            FROM workspace.fraude_prod.model_validation_results
        )
        ORDER BY
            CASE gate_status
                WHEN 'FAIL' THEN 1
                WHEN 'WARNING' THEN 2
                ELSE 3
            END,
            gate_name
    """)
)

# COMMAND ----------

import json
from datetime import datetime, timezone

ALIAS_ROLLBACK = "PreviousChampion"
APPROVAL_TICKET = "DEMO-EJ2-001"

# Solo se permite porque es una simulación de gobierno.
# En producción una advertencia de Git bloquearía el despliegue.
ALLOW_EXCEPTION = True

approver = spark.sql(
    "SELECT current_user() AS usuario"
).first()["usuario"]

champion_before = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_PRODUCCION
)

candidate_to_promote = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_CANDIDATO
)

validation_status = (
    candidate_to_promote.tags.get("validation_status")
)

print("Preparación de la promoción")
print("Champion actual:", champion_before.version)
print("Candidate:", candidate_to_promote.version)
print("Validación:", validation_status)
print("Aprobador:", approver)
print("Ticket:", APPROVAL_TICKET)

if champion_before.version == candidate_to_promote.version:
    print("La versión candidata ya es Champion. No se realizan cambios.")

else:
    if validation_status == "FAIL":
        raise RuntimeError(
            "Promoción bloqueada: existen validaciones fallidas"
        )

    if (
        validation_status == "PASS_WITH_EXCEPTION"
        and not ALLOW_EXCEPTION
    ):
        raise RuntimeError(
            "Promoción bloqueada: la excepción no fue aprobada"
        )

    if validation_status not in {
        "PASS",
        "PASS_WITH_EXCEPTION"
    }:
        raise RuntimeError(
            f"Estado de validación no autorizado: {validation_status}"
        )

    previous_version = str(champion_before.version)
    target_version = str(candidate_to_promote.version)

    try:
        # Punto explícito de recuperación
        client.set_registered_model_alias(
            name=MODEL_NAME,
            alias=ALIAS_ROLLBACK,
            version=previous_version
        )

        # Promoción productiva
        client.set_registered_model_alias(
            name=MODEL_NAME,
            alias=ALIAS_PRODUCCION,
            version=target_version
        )

        promotion_time = datetime.now(timezone.utc)

        client.set_model_version_tag(
            MODEL_NAME,
            target_version,
            "promotion_status",
            "PROMOTED"
        )

        client.set_model_version_tag(
            MODEL_NAME,
            target_version,
            "approval_ticket",
            APPROVAL_TICKET
        )

        client.set_model_version_tag(
            MODEL_NAME,
            target_version,
            "promoted_by",
            approver
        )

        evento_promocion = [{
            "event_id": (
                f"PROMOTE-v{previous_version}-to-v{target_version}"
                f"-{APPROVAL_TICKET}"
            ),
            "event_timestamp": promotion_time,
            "model_name": MODEL_NAME,
            "action": "PROMOTE",
            "previous_version": previous_version,
            "target_version": target_version,
            "model_alias": ALIAS_PRODUCCION,
            "validation_status": validation_status,
            "approved_by": approver,
            "reason": (
                "Promoción controlada para demostrar el flujo MLOps. "
                "La excepción por Git solo se acepta en esta prueba."
            ),
            "metadata_json": json.dumps({
                "approval_ticket": APPROVAL_TICKET,
                "rollback_alias": ALIAS_ROLLBACK,
                "candidate_alias": ALIAS_CANDIDATO
            })
        }]

        promotion_df = spark.createDataFrame(evento_promocion)
        promotion_df.createOrReplaceTempView(
            "evento_promocion_actual"
        )

        spark.sql("""
        MERGE INTO workspace.fraude_prod.model_governance_events AS destino
        USING evento_promocion_actual AS origen
        ON destino.event_id = origen.event_id

        WHEN MATCHED THEN UPDATE SET
            destino.event_timestamp = origen.event_timestamp,
            destino.validation_status = origen.validation_status,
            destino.approved_by = origen.approved_by,
            destino.reason = origen.reason,
            destino.metadata_json = origen.metadata_json

        WHEN NOT MATCHED THEN INSERT *
        """)

        print("\nPromoción completada")
        print(
            f"{ALIAS_ROLLBACK}: versión {previous_version}"
        )
        print(
            f"{ALIAS_PRODUCCION}: versión {target_version}"
        )

    except Exception:
        # Compensación si falla la promoción o su auditoría
        client.set_registered_model_alias(
            name=MODEL_NAME,
            alias=ALIAS_PRODUCCION,
            version=previous_version
        )

        print(
            "La operación falló. Champion fue restaurado "
            f"a la versión {previous_version}."
        )
        raise

# COMMAND ----------

for alias in [
    ALIAS_PRODUCCION,
    ALIAS_CANDIDATO,
    ALIAS_ROLLBACK
]:
    version = client.get_model_version_by_alias(
        MODEL_NAME,
        alias
    )

    print(f"{alias}: versión {version.version}")

# COMMAND ----------

%sql

SELECT
    event_timestamp,
    action,
    previous_version,
    target_version,
    model_alias,
    validation_status,
    approved_by,
    reason
FROM workspace.fraude_prod.model_governance_events
ORDER BY event_timestamp DESC;

# COMMAND ----------

SCORING_MODEL_URI = (
    f"models:/{MODEL_NAME}@{ALIAS_PRODUCCION}"
)

resolved_champion = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_PRODUCCION
)

print("Configuración del pipeline")
print("URI estable:", SCORING_MODEL_URI)
print("Versión resuelta:", resolved_champion.version)
print("Run ID:", resolved_champion.run_id)

assert resolved_champion.version == "2"

print(
    "El pipeline usaría la versión 2 sin modificar "
    "el notebook de scoring."
)

# COMMAND ----------

import json
from datetime import datetime, timezone

ROLLBACK_TICKET = "DEMO-EJ2-ROLLBACK-001"
ALIAS_ROLLED_BACK = "RolledBack"

approver = spark.sql(
    "SELECT current_user() AS usuario"
).first()["usuario"]

champion_before_rollback = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_PRODUCCION
)

rollback_target = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_ROLLBACK
)

current_version = str(champion_before_rollback.version)
target_version = str(rollback_target.version)

print("Preparación del rollback")
print("Champion actual:", current_version)
print("Versión a restaurar:", target_version)
print("Aprobador:", approver)
print("Ticket:", ROLLBACK_TICKET)

if current_version == target_version:
    print("Champion ya se encuentra en la versión de recuperación.")

else:
    try:
        # Conservar la versión retirada para análisis
        client.set_registered_model_alias(
            name=MODEL_NAME,
            alias=ALIAS_ROLLED_BACK,
            version=current_version
        )

        # Restaurar la versión productiva anterior
        client.set_registered_model_alias(
            name=MODEL_NAME,
            alias=ALIAS_PRODUCCION,
            version=target_version
        )

        rollback_time = datetime.now(timezone.utc)

        client.set_model_version_tag(
            MODEL_NAME,
            current_version,
            "promotion_status",
            "ROLLED_BACK"
        )

        client.set_model_version_tag(
            MODEL_NAME,
            current_version,
            "rollback_ticket",
            ROLLBACK_TICKET
        )

        client.set_model_version_tag(
            MODEL_NAME,
            target_version,
            "promotion_status",
            "RESTORED"
        )

        evento_rollback = [{
            "event_id": (
                f"ROLLBACK-v{current_version}-to-v{target_version}"
                f"-{ROLLBACK_TICKET}"
            ),
            "event_timestamp": rollback_time,
            "model_name": MODEL_NAME,
            "action": "ROLLBACK",
            "previous_version": current_version,
            "target_version": target_version,
            "model_alias": ALIAS_PRODUCCION,
            "validation_status": "RESTORED",
            "approved_by": approver,
            "reason": (
                "Rollback controlado para demostrar recuperación "
                "de la versión productiva anterior."
            ),
            "metadata_json": json.dumps({
                "rollback_ticket": ROLLBACK_TICKET,
                "rolled_back_alias": ALIAS_ROLLED_BACK,
                "recovery_alias": ALIAS_ROLLBACK
            })
        }]

        rollback_df = spark.createDataFrame(evento_rollback)
        rollback_df.createOrReplaceTempView(
            "evento_rollback_actual"
        )

        spark.sql("""
        MERGE INTO workspace.fraude_prod.model_governance_events AS destino
        USING evento_rollback_actual AS origen
        ON destino.event_id = origen.event_id

        WHEN MATCHED THEN UPDATE SET
            destino.event_timestamp = origen.event_timestamp,
            destino.validation_status = origen.validation_status,
            destino.approved_by = origen.approved_by,
            destino.reason = origen.reason,
            destino.metadata_json = origen.metadata_json

        WHEN NOT MATCHED THEN INSERT *
        """)

        print("\nRollback completado")
        print(
            f"{ALIAS_PRODUCCION}: versión {target_version}"
        )
        print(
            f"{ALIAS_ROLLED_BACK}: versión {current_version}"
        )

    except Exception:
        # Recuperar la situación previa si falla la operación
        client.set_registered_model_alias(
            name=MODEL_NAME,
            alias=ALIAS_PRODUCCION,
            version=current_version
        )

        print(
            "Falló el rollback o su auditoría. "
            f"Champion regresó a la versión {current_version}."
        )
        raise

# COMMAND ----------

aliases_a_verificar = [
    ALIAS_PRODUCCION,
    ALIAS_CANDIDATO,
    ALIAS_ROLLBACK,
    ALIAS_ROLLED_BACK
]

for alias in aliases_a_verificar:
    version = client.get_model_version_by_alias(
        MODEL_NAME,
        alias
    )

    print(f"{alias}: versión {version.version}")

# COMMAND ----------

%sql

SELECT
    event_timestamp,
    action,
    previous_version,
    target_version,
    model_alias,
    validation_status,
    approved_by,
    reason
FROM workspace.fraude_prod.model_governance_events
ORDER BY event_timestamp DESC;

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.mlops_artifact_policy (
    artifact_order INT,
    artifact_category STRING,
    artifact_name STRING,
    versioning_mechanism STRING,
    mandatory BOOLEAN,
    purpose STRING
)
USING DELTA
COMMENT 'Política mínima de artefactos versionados para el modelo de fraude';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.mlops_artifact_policy
VALUES
(
    1,
    'CODE',
    'Código de entrenamiento, preparación y scoring',
    'Git commit y pull request aprobado',
    TRUE,
    'Reconstruir la lógica exacta y conocer quién aprobó cada cambio'
),
(
    2,
    'DATA',
    'Datos de entrenamiento y validación',
    'Versión Delta, timestamp o snapshot inmutable',
    TRUE,
    'Reconstruir exactamente la población utilizada'
),
(
    3,
    'MODEL',
    'Artefacto entrenado y firma de entradas y salidas',
    'MLflow Model Registry y Unity Catalog',
    TRUE,
    'Conservar cada versión desplegable y su contrato'
),
(
    4,
    'PARAMETERS',
    'Hiperparámetros, semilla y variables utilizadas',
    'MLflow params y manifiesto de reproducibilidad',
    TRUE,
    'Repetir el entrenamiento bajo la misma configuración'
),
(
    5,
    'ENVIRONMENT',
    'Dependencias, runtime y versiones de librerías',
    'Requirements, lock file o imagen de contenedor',
    TRUE,
    'Evitar incompatibilidades entre entrenamiento y serving'
),
(
    6,
    'METRICS',
    'ROC-AUC, AUPR, recall, precisión y lift',
    'MLflow metrics y tabla Delta de validaciones',
    TRUE,
    'Comparar Candidate contra Champion con criterios objetivos'
),
(
    7,
    'BUSINESS_RULES',
    'Umbrales de riesgo y acciones sugeridas',
    'Archivo de configuración versionado',
    TRUE,
    'Separar las decisiones de negocio del código del modelo'
),
(
    8,
    'TESTS',
    'Pruebas de datos, contrato, regresión y smoke test',
    'Código de pruebas y resultados del pipeline CI/CD',
    TRUE,
    'Bloquear versiones incompatibles o degradadas'
),
(
    9,
    'MONITORING',
    'Distribución de referencia y límites de alertas',
    'Tablas Delta y configuración versionada',
    TRUE,
    'Identificar drift y cambios operativos'
),
(
    10,
    'GOVERNANCE',
    'Aprobaciones, promociones, excepciones y rollback',
    'Bitácora Delta y registros del pipeline CI/CD',
    TRUE,
    'Mantener auditoría completa del ciclo de vida'
),
(
    11,
    'INFRASTRUCTURE',
    'Jobs, permisos, endpoints y recursos Azure',
    'Terraform o Bicep almacenado en Git',
    TRUE,
    'Reconstruir la infraestructura de forma controlada'
),
(
    12,
    'DOCUMENTATION',
    'Model card, limitaciones y responsables',
    'Documento versionado junto al código',
    TRUE,
    'Comunicar uso permitido, riesgos y responsables'
);

# COMMAND ----------

%sql

SELECT
    artifact_order,
    artifact_category,
    artifact_name,
    versioning_mechanism,
    purpose
FROM workspace.fraude_prod.mlops_artifact_policy
ORDER BY artifact_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flujo propuesto de promoción del modelo
# MAGIC
# MAGIC 1. **Desarrollo**
# MAGIC    - El científico de datos y/o ingeniero de ML trabaja en una rama de Git.
# MAGIC    - El código, configuración, pruebas e infraestructura quedan versionados.
# MAGIC    - Los cambios ingresan mediante pull request y revisión.
# MAGIC
# MAGIC 2. **Integración continua**
# MAGIC    - Se ejecutan pruebas unitarias y de calidad de datos.
# MAGIC    - Se valida la firma del modelo y la compatibilidad del esquema.
# MAGIC    - Se reconstruye el entrenamiento con una versión Delta explícita.
# MAGIC    - Se comparan métricas contra el Champion vigente.
# MAGIC
# MAGIC 3. **Registro del candidato**
# MAGIC    - MLflow registra una nueva versión.
# MAGIC    - El alias Candidate identifica la versión pendiente.
# MAGIC    - La versión todavía no recibe tráfico productivo.
# MAGIC
# MAGIC 4. **Puertas de validación**
# MAGIC    - Calidad y completitud de datos.
# MAGIC    - Métricas mínimas de desempeño.
# MAGIC    - Degradación máxima permitida.
# MAGIC    - Pruebas de regresión y smoke test.
# MAGIC    - Commit, dependencias, firma y datos reproducibles.
# MAGIC    - Revisión de sesgos, seguridad y cumplimiento.
# MAGIC
# MAGIC 5. **Aprobación**
# MAGIC    - Un aprobador distinto del desarrollador autoriza el cambio.
# MAGIC    - La aprobación queda asociada a un ticket.
# MAGIC    - Un service principal ejecuta la promoción.
# MAGIC
# MAGIC 6. **Promoción**
# MAGIC    - Se conserva la versión vigente como PreviousChampion.
# MAGIC    - El alias Champion se mueve a la versión aprobada.
# MAGIC    - Los consumidores mantienen una URI estable.
# MAGIC
# MAGIC 7. **Monitoreo**
# MAGIC    - Se vigilan errores, latencia, volumen y calidad.
# MAGIC    - Se monitorean distribución del score, drift y desempeño.
# MAGIC    - La nueva versión puede iniciar en shadow o canary.
# MAGIC
# MAGIC 8. **Rollback**
# MAGIC    - Ante una degradación, Champion vuelve a PreviousChampion.
# MAGIC    - La versión retirada se conserva para análisis.
# MAGIC    - El evento y su responsable quedan auditados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separación de responsabilidades
# MAGIC
# MAGIC - **Científico de datos:** desarrolla y registra Candidate.
# MAGIC - **Ingeniero de Machine Learning:** mantiene pipelines, pruebas y empaquetado.
# MAGIC - **Validador de modelos:** revisa métricas, reproducibilidad y riesgos.
# MAGIC - **Negocio o fraude:** aprueba umbrales y utilidad operativa.
# MAGIC - **Plataforma y seguridad:** administra identidades, permisos y secretos.
# MAGIC - **Service principal CI/CD:** mueve los aliases autorizados.
# MAGIC - **Operaciones:** monitorea producción y activa el rollback.
# MAGIC
# MAGIC El desarrollador del modelo no debe aprobar y desplegar por sí solo su propia versión.

# COMMAND ----------

versiones_finales = client.search_model_versions(
    filter_string=f"name = '{MODEL_NAME}'"
)

champion_final = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_PRODUCCION
)

candidate_final = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_CANDIDATO
)

rolled_back_final = client.get_model_version_by_alias(
    MODEL_NAME,
    ALIAS_ROLLED_BACK
)

print("Resumen final del Ejercicio 2")
print("Modelo:", MODEL_NAME)
print("Versiones registradas:", len(versiones_finales))
print("Champion:", champion_final.version)
print("Candidate:", candidate_final.version)
print("RolledBack:", rolled_back_final.version)

assert champion_final.version == "1"
assert candidate_final.version == "2"
assert rolled_back_final.version == "2"

print("\nEstado final correcto")
print("- La versión 1 fue restaurada en producción")
print("- La versión 2 quedó conservada para análisis")
print("- El rollback no eliminó artefactos ni historial")

# COMMAND ----------

%sql

SELECT
    event_timestamp,
    action,
    previous_version,
    target_version,
    validation_status,
    approved_by,
    reason
FROM workspace.fraude_prod.model_governance_events
ORDER BY event_timestamp;
