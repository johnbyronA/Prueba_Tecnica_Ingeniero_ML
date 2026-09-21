# Databricks notebook source
# Código fuente exportado para revisión y control de versiones.

# COMMAND ----------

MONTHLY_CLAIMS = 3_000_000
DAYS_PER_MONTH = 30

HOURS_PER_DAY = 24
SECONDS_PER_DAY = 86_400

daily_average = MONTHLY_CLAIMS / DAYS_PER_MONTH
hourly_average = daily_average / HOURS_PER_DAY
requests_per_second_average = daily_average / SECONDS_PER_DAY

PEAK_FACTOR = 10
estimated_peak_rps = (
    requests_per_second_average * PEAK_FACTOR
)

print("Dimensionamiento inicial")
print("Reclamaciones mensuales:", MONTHLY_CLAIMS)
print("Promedio diario:", round(daily_average))
print("Promedio por hora:", round(hourly_average, 2))
print(
    "Solicitudes por segundo promedio:",
    round(requests_per_second_average, 2)
)
print(
    "Pico ilustrativo a 10 veces el promedio:",
    round(estimated_peak_rps, 2)
)

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.score_consumption_options (
    comparison_order INT,
    comparison_dimension STRING,
    batch_daily STRING,
    online_api STRING,
    decision_criterion STRING
)
USING DELTA
COMMENT 'Comparación de alternativas para consumo corporativo del score de fraude';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.score_consumption_options
VALUES
(
    1,
    'CASOS_DE_USO',
    'Cola de investigadores, auditoría, BI, priorización masiva, backfill y rescoring',
    'Registro de reclamación, enrutamiento inmediato y decisiones transaccionales',
    'Determinar si la decisión puede esperar o debe ocurrir dentro de la transacción'
),
(
    2,
    'PATRON_DE_CONSUMO',
    'Procesamiento masivo, incremental y programado',
    'Solicitud individual o pequeños grupos bajo demanda',
    'Analizar volumen, concurrencia y comportamiento de picos'
),
(
    3,
    'LATENCIA',
    'Minutos u horas; disponibilidad diaria según SLA',
    'Milisegundos o pocos segundos',
    'Usar API solamente cuando el proceso realmente requiere respuesta inmediata'
),
(
    4,
    'VENTAJA_PRINCIPAL',
    'Alta eficiencia para volumen, trazabilidad y costos predecibles',
    'Respuesta inmediata e integración directa con sistemas',
    'Priorizar simplicidad y valor operativo'
),
(
    5,
    'COSTO_COMPUTE',
    'Bajo a medio; compute activo durante la ejecución',
    'Medio a alto; capacidad disponible, endpoint, APIM y observabilidad',
    'Comparar costo por lote contra costo por solicitud y capacidad reservada'
),
(
    6,
    'ESCALABILIDAD',
    'Escala horizontal para grandes volúmenes y reprocesos',
    'Autoscaling condicionado por concurrencia, cold start y límites',
    'Dimensionar con pruebas de carga y picos, no solo con el promedio'
),
(
    7,
    'DISPONIBILIDAD',
    'Depende del calendario y del cumplimiento del job',
    'Debe estar disponible mientras opere el sistema transaccional',
    'La API requiere SLA, redundancia y estrategia ante indisponibilidad'
),
(
    8,
    'COMPLEJIDAD_OPERATIVA',
    'Media; job, reintentos, idempotencia, tablas Delta y alertas',
    'Alta; endpoint, autenticación, red, rate limits, timeouts y autoscaling',
    'Aceptar complejidad adicional solamente cuando genera valor'
),
(
    9,
    'MANEJO_DE_ERRORES',
    'Reprocesamiento del lote, cuarentena y MERGE idempotente',
    'Timeout, retry controlado, circuit breaker y respuesta de contingencia',
    'Evitar duplicados y tormentas de reintentos'
),
(
    10,
    'TRAZABILIDAD',
    'Natural mediante tablas Delta, ejecución, versión y fecha',
    'Debe persistirse request, respuesta, latencia y versión del modelo',
    'Toda predicción debe poder reconstruirse y auditarse'
),
(
    11,
    'SEGURIDAD',
    'Unity Catalog, service principal y permisos de tablas',
    'Entra ID, API Management, Key Vault, TLS y mínimo privilegio',
    'No exponer directamente el endpoint ni utilizar tokens personales'
),
(
    12,
    'RECOMENDACION',
    'Canal principal para 3 millones de reclamaciones mensuales',
    'Canal selectivo para decisiones verdaderamente síncronas',
    'Adoptar arquitectura híbrida con un contrato común'
);

# COMMAND ----------

%sql

SELECT
    comparison_order,
    comparison_dimension,
    batch_daily,
    online_api,
    decision_criterion
FROM workspace.fraude_prod.score_consumption_options
ORDER BY comparison_order;

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.score_interface_contract (
    field_order INT,
    interface_section STRING,
    field_name STRING,
    data_type STRING,
    required BOOLEAN,
    description STRING,
    example_value STRING
)
USING DELTA
COMMENT 'Contrato común para entrada y consumo del score de fraude';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.score_interface_contract
VALUES
(
    1,
    'REQUEST',
    'id_reclamacion',
    'STRING',
    TRUE,
    'Identificador único e idempotente de la reclamación',
    'REC-2026-000123'
),
(
    2,
    'REQUEST',
    'fecha_evento',
    'TIMESTAMP',
    TRUE,
    'Fecha del evento utilizada para preservar la lógica temporal',
    '2026-09-19T10:30:00Z'
),
(
    3,
    'REQUEST',
    'payload_reclamacion',
    'OBJECT',
    TRUE,
    'Variables crudas del contrato canónico; la solución de fraude genera las características',
    'Objeto JSON versionado'
),
(
    4,
    'REQUEST',
    'contract_version',
    'STRING',
    TRUE,
    'Versión del contrato de entrada',
    '1.0'
),
(
    5,
    'REQUEST',
    'trace_id',
    'STRING',
    TRUE,
    'Identificador para trazabilidad entre sistemas',
    '7f7f7c77-41ce-4caf'
),
(
    6,
    'RESPONSE',
    'id_reclamacion',
    'STRING',
    TRUE,
    'Identificador recibido en la solicitud',
    'REC-2026-000123'
),
(
    7,
    'RESPONSE',
    'score_fraude',
    'DOUBLE',
    TRUE,
    'Probabilidad o score utilizado para priorización',
    '0.7851'
),
(
    8,
    'RESPONSE',
    'nivel_riesgo',
    'STRING',
    TRUE,
    'Clasificación operativa según umbrales vigentes',
    'ALTO'
),
(
    9,
    'RESPONSE',
    'accion_sugerida',
    'STRING',
    TRUE,
    'Acción recomendada para el consumidor',
    'REVISION_PRIORITARIA'
),
(
    10,
    'RESPONSE',
    'fecha_scoring',
    'TIMESTAMP',
    TRUE,
    'Momento en que se calculó el resultado',
    '2026-09-19T10:30:01Z'
),
(
    11,
    'RESPONSE',
    'model_name',
    'STRING',
    TRUE,
    'Nombre gobernado del modelo',
    'workspace.fraude_prod.fraude_random_forest'
),
(
    12,
    'RESPONSE',
    'model_version',
    'STRING',
    TRUE,
    'Versión concreta resuelta desde Champion',
    '1'
),
(
    13,
    'RESPONSE',
    'contract_version',
    'STRING',
    TRUE,
    'Versión del contrato de respuesta',
    '1.0'
),
(
    14,
    'RESPONSE',
    'trace_id',
    'STRING',
    TRUE,
    'Identificador de trazabilidad de extremo a extremo',
    '7f7f7c77-41ce-4caf'
),
(
    15,
    'RESPONSE',
    'scoring_status',
    'STRING',
    TRUE,
    'Resultado técnico del procesamiento',
    'SUCCESS'
),
(
    16,
    'RESPONSE',
    'reason_code',
    'STRING',
    FALSE,
    'Código de error o motivo de respuesta degradada',
    'INVALID_SCHEMA'
);

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.score_interface_contract
ORDER BY field_order;

# COMMAND ----------

import json

score_response_example = {
    "id_reclamacion": "REC-2026-000123",
    "score_fraude": 0.7851,
    "nivel_riesgo": "ALTO",
    "accion_sugerida": "REVISION_PRIORITARIA",
    "fecha_scoring": "2026-09-19T10:30:01Z",
    "model_name": (
        "workspace.fraude_prod.fraude_random_forest"
    ),
    "model_version": "1",
    "contract_version": "1.0",
    "trace_id": "7f7f7c77-41ce-4caf",
    "scoring_status": "SUCCESS",
    "reason_code": None
}

print(
    json.dumps(
        score_response_example,
        indent=2,
        ensure_ascii=False
    )
)

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.score_service_objectives (
    objective_order INT,
    objective STRING,
    batch_daily STRING,
    online_api STRING,
    observation STRING
)
USING DELTA
COMMENT 'Objetivos propuestos de servicio para los canales de scoring';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.score_service_objectives
VALUES
(
    1,
    'FRECUENCIA',
    'Ejecución diaria a las 02:00',
    'Bajo demanda',
    'La frecuencia debe ajustarse al proceso de negocio'
),
(
    2,
    'LATENCIA_OBJETIVO',
    'Resultados disponibles antes de las 06:00',
    'P95 menor o igual a 1 segundo',
    'La API requiere pruebas de carga antes de comprometer el SLA'
),
(
    3,
    'DISPONIBILIDAD',
    'Cumplimiento mensual de ejecuciones',
    'Objetivo inicial 99.9 por ciento',
    'El nivel definitivo depende de criticidad y presupuesto'
),
(
    4,
    'CAPACIDAD',
    'Al menos 100000 reclamaciones diarias más backfill',
    'Promedio 1.16 RPS y prueba inicial hasta 12 RPS',
    'Los picos reales deben obtenerse del sistema consumidor'
),
(
    5,
    'TIMEOUT',
    'No aplica por solicitud individual',
    'Timeout inicial de 2 segundos',
    'Definir respuesta de contingencia y circuit breaker'
),
(
    6,
    'REINTENTOS',
    'Dos reintentos con espera y ejecución idempotente',
    'Máximo uno o dos reintentos con backoff',
    'Evitar tormentas de reintentos y solicitudes duplicadas'
),
(
    7,
    'RECUPERACION',
    'Reprocesamiento desde Delta y MERGE',
    'Fallback a cola asíncrona o revisión manual',
    'No bloquear indefinidamente la operación transaccional'
),
(
    8,
    'ESCALADO',
    'Compute elástico durante la ventana batch',
    'Autoscaling sin scale to zero para SLA estricto',
    'Scale to zero puede introducir cold start'
);

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.score_service_objectives
ORDER BY objective_order;

# COMMAND ----------

%sql

CREATE OR REPLACE VIEW workspace.fraude_prod.consumer_scores_batch_v1 AS

WITH resultados_ordenados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_reclamacion
            ORDER BY fecha_scoring DESC
        ) AS posicion
    FROM workspace.fraude_prod.scores_reclamaciones
)

SELECT
    id_reclamacion,
    score_fraude,
    nivel_riesgo,
    accion_sugerida,
    fecha_scoring,

    'workspace.fraude_prod.fraude_random_forest'
        AS model_name,

    COALESCE(
        NULLIF(
            REGEXP_EXTRACT(
                version_modelo,
                ':([0-9]+)$',
                1
            ),
            ''
        ),
        version_modelo
    ) AS model_version,

    '1.0' AS contract_version,
    id_ejecucion AS trace_id,
    estado_scoring AS scoring_status,
    CAST(NULL AS STRING) AS reason_code

FROM resultados_ordenados
WHERE posicion = 1;

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.consumer_scores_batch_v1
ORDER BY score_fraude DESC
LIMIT 20;

# COMMAND ----------

%sql

SELECT
    COUNT(*) AS reclamaciones,
    COUNT(DISTINCT id_reclamacion)
        AS reclamaciones_unicas,
    MAX(fecha_scoring)
        AS ultima_actualizacion
FROM workspace.fraude_prod.consumer_scores_batch_v1;

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.score_error_contract (
    error_order INT,
    http_status INT,
    reason_code STRING,
    meaning STRING,
    consumer_action STRING,
    retry_allowed BOOLEAN
)
USING DELTA
COMMENT 'Contrato de errores y contingencias para la API de scoring';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.score_error_contract
VALUES
(
    1,
    200,
    'SUCCESS',
    'Scoring realizado correctamente',
    'Consumir el resultado',
    FALSE
),
(
    2,
    202,
    'QUEUED_FOR_BATCH',
    'La solicitud fue aceptada para procesamiento asíncrono',
    'Consultar posteriormente mediante trace_id',
    FALSE
),
(
    3,
    400,
    'INVALID_SCHEMA',
    'El payload no cumple el contrato',
    'Corregir la solicitud; no reintentar sin cambios',
    FALSE
),
(
    4,
    401,
    'UNAUTHENTICATED',
    'La identidad no pudo autenticarse',
    'Renovar credenciales o token',
    FALSE
),
(
    5,
    403,
    'FORBIDDEN',
    'La identidad no tiene autorización',
    'Solicitar permisos; no reintentar automáticamente',
    FALSE
),
(
    6,
    409,
    'IDEMPOTENCY_CONFLICT',
    'El identificador existe con un payload diferente',
    'Revisar el identificador y el contenido enviado',
    FALSE
),
(
    7,
    422,
    'INVALID_VALUES',
    'Los valores no cumplen reglas de calidad',
    'Corregir los datos de negocio',
    FALSE
),
(
    8,
    429,
    'RATE_LIMITED',
    'El consumidor superó el límite permitido',
    'Reintentar con backoff y respetar Retry-After',
    TRUE
),
(
    9,
    500,
    'SCORING_ERROR',
    'Error interno durante la preparación o inferencia',
    'Reintentar una vez y usar contingencia si persiste',
    TRUE
),
(
    10,
    503,
    'SERVICE_UNAVAILABLE',
    'El servicio no se encuentra disponible',
    'Activar circuit breaker y enviar a cola asíncrona',
    TRUE
);

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.score_cost_drivers (
    cost_order INT,
    cost_component STRING,
    batch_daily STRING,
    online_api STRING,
    optimization_action STRING
)
USING DELTA
COMMENT 'Principales impulsores de costo para batch y API de scoring';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.score_cost_drivers
VALUES
(
    1,
    'COMPUTE',
    'Consumo durante la ventana diaria y reprocesos',
    'Capacidad activa continuamente o bajo autoscaling',
    'Medir costo por mil scores y utilizar tamaños adecuados'
),
(
    2,
    'STORAGE',
    'Delta de entradas, scores, auditoría y checkpoints',
    'Logs de requests, respuestas y telemetría',
    'Definir retención, compactación y archivado'
),
(
    3,
    'ORCHESTRATION',
    'Workflows, reintentos y monitoreo de jobs',
    'Despliegue, autoscaling y gestión del endpoint',
    'Automatizar mediante CI/CD e infraestructura como código'
),
(
    4,
    'API_MANAGEMENT',
    'No aplica para lectura directa autorizada',
    'API Management, políticas, rate limits y autenticación',
    'Agrupar consumidores y aplicar cuotas'
),
(
    5,
    'NETWORKING',
    'Transferencia entre ADLS, Databricks y consumidores',
    'Private Link, balanceo y tráfico por solicitud',
    'Mantener servicios en regiones compatibles'
),
(
    6,
    'OBSERVABILITY',
    'Logs por ejecución y métricas agregadas',
    'Logs por request, latencia, errores y trazas',
    'Muestrear payloads y controlar retención'
),
(
    7,
    'AVAILABILITY',
    'Costo concentrado durante la ejecución',
    'Redundancia y capacidad mínima para cumplir el SLA',
    'No ofrecer 99.9 por ciento si el caso no lo necesita'
),
(
    8,
    'ENGINEERING',
    'Complejidad media y operación programada',
    'Mayor esfuerzo en seguridad, contratos y resiliencia',
    'Reutilizar preparación, pruebas y contrato común'
);

# COMMAND ----------

%sql

CREATE TABLE IF NOT EXISTS workspace.fraude_prod.score_consumption_recommendation (
    use_case_order INT,
    consumer_use_case STRING,
    recommended_channel STRING,
    rationale STRING,
    contingency STRING
)
USING DELTA
COMMENT 'Canal recomendado según el caso de uso consumidor';

# COMMAND ----------

%sql

INSERT OVERWRITE TABLE workspace.fraude_prod.score_consumption_recommendation
VALUES
(
    1,
    'Cola diaria de investigadores',
    'BATCH',
    'Tolera horas y requiere priorización masiva y auditable',
    'Utilizar el último score disponible si el job se retrasa'
),
(
    2,
    'Auditoría y reportería',
    'BATCH',
    'Consulta histórica, reprocesable y orientada a volumen',
    'Leer snapshot anterior certificado'
),
(
    3,
    'Rescoring completo por nueva versión',
    'BATCH',
    'Proceso intensivo que debe ser idempotente y trazable',
    'Procesar por particiones y reanudar desde checkpoints'
),
(
    4,
    'Enrutamiento durante el registro de una reclamación',
    'API',
    'La decisión afecta inmediatamente el flujo transaccional',
    'Enviar a cola asíncrona o revisión manual ante indisponibilidad'
),
(
    5,
    'Consulta puntual por un investigador',
    'API_O_BATCH',
    'Puede consultar el score persistido o solicitar actualización',
    'Mostrar fecha y versión del último resultado disponible'
),
(
    6,
    'Integración masiva de otro sistema',
    'BATCH',
    'Evita millones de llamadas y reduce costos operativos',
    'Intercambiar mediante tabla, archivo o evento de finalización'
),
(
    7,
    'Decisión crítica de bloqueo automático',
    'API_CON_CONTROLES',
    'Requiere baja latencia, alta disponibilidad y reglas de contingencia',
    'No bloquear únicamente por el score; aplicar reglas y revisión'
);

# COMMAND ----------

%sql

SELECT *
FROM workspace.fraude_prod.score_consumption_recommendation
ORDER BY use_case_order;
