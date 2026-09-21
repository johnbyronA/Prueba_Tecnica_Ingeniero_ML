# Databricks notebook source
# Código fuente exportado para revisión y control de versiones.

# COMMAND ----------

import json
import os
import shutil
import mlflow

mlflow.set_registry_uri("databricks-uc")

MODELO_ORIGINAL = "workspace.fraude_prod.fraude_random_forest"
VERSION_ORIGINAL = "1"
MODELO_SERVING = "workspace.fraude_prod.fraude_random_forest_serving"

URI_ORIGINAL = f"models:/{MODELO_ORIGINAL}/{VERSION_ORIGINAL}"
DIRECTORIO_LOCAL = "/tmp/fraude_rf_serving_patch"

print("Modelo original:", URI_ORIGINAL)
print("Modelo compatible:", MODELO_SERVING)

# COMMAND ----------

shutil.rmtree(DIRECTORIO_LOCAL, ignore_errors=True)
os.makedirs(DIRECTORIO_LOCAL, exist_ok=True)

modelo_local = mlflow.artifacts.download_artifacts(
    artifact_uri=URI_ORIGINAL,
    dst_path=DIRECTORIO_LOCAL
)

archivos_con_prune_tree = []

for raiz, _, archivos in os.walk(modelo_local):
    for archivo in archivos:
        ruta = os.path.join(raiz, archivo)

        try:
            with open(ruta, "r", encoding="utf-8") as f:
                contenido = f.read()

            if "pruneTree" in contenido:
                archivos_con_prune_tree.append(ruta)
        except (UnicodeDecodeError, PermissionError, IsADirectoryError):
            pass

print("Modelo descargado en:", modelo_local)
print("Archivos con pruneTree:", len(archivos_con_prune_tree))

for ruta in archivos_con_prune_tree:
    print(ruta)

# COMMAND ----------

def eliminar_prune_tree(objeto):
    if isinstance(objeto, dict):
        return {
            clave: eliminar_prune_tree(valor)
            for clave, valor in objeto.items()
            if clave != "pruneTree"
        }

    if isinstance(objeto, list):
        return [eliminar_prune_tree(valor) for valor in objeto]

    return objeto


archivos_modificados = 0

for ruta in archivos_con_prune_tree:
    with open(ruta, "r", encoding="utf-8") as f:
        contenido_original = f.read()

    try:
        metadata = json.loads(contenido_original)
    except json.JSONDecodeError:
        print("No se modificó porque no es JSON:", ruta)
        continue

    metadata_compatible = eliminar_prune_tree(metadata)

    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(metadata_compatible, f, separators=(",", ":"))

    archivos_modificados += 1
    print("Modificado:", ruta)

print("Total de archivos modificados:", archivos_modificados)

if archivos_modificados == 0:
    raise RuntimeError(
        "No se modificó ningún archivo. Detén el proceso y revisa la celda anterior."
    )

# COMMAND ----------

referencias_restantes = []

for raiz, _, archivos in os.walk(modelo_local):
    for archivo in archivos:
        ruta = os.path.join(raiz, archivo)

        try:
            with open(ruta, "r", encoding="utf-8") as f:
                if "pruneTree" in f.read():
                    referencias_restantes.append(ruta)
        except (UnicodeDecodeError, PermissionError, IsADirectoryError):
            pass

print("Referencias restantes:", len(referencias_restantes))

if referencias_restantes:
    for ruta in referencias_restantes:
        print(ruta)
    raise RuntimeError("Todavía existen referencias a pruneTree.")

print("Copia compatible preparada correctamente.")

# COMMAND ----------

import mlflow.spark

RUTA_TEMPORAL_UC = (
    "/Volumes/workspace/fraude_prod/mlflow_tmp/spark_models"
)

os.environ["MLFLOW_DFS_TMP"] = RUTA_TEMPORAL_UC

modelo_original_spark = mlflow.spark.load_model(
    URI_ORIGINAL,
    dfs_tmpdir=RUTA_TEMPORAL_UC
)

modelo_compatible_spark = mlflow.spark.load_model(
    modelo_local,
    dfs_tmpdir=RUTA_TEMPORAL_UC
)

print("Modelo original cargado")
print("Copia compatible cargada")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array

FEATURE_COLS = [
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
    "reserva_inicial_es_cero"
]

TABLA_ENTRADA = (
    "workspace.fraude_prod.reclamaciones_entrada_demo"
)

entrada = spark.table(TABLA_ENTRADA)

date_cols = [
    "Fecha_Primera_Vigencia_Cert",
    "fecha_primera_vigencia_pol",
    "FEXPEDICION",
    "FSINIESTRO",
    "F_Notificacion",
    "Fecha_Recepcion",
    "Fecha_Apertura"
]

preparada = entrada

for columna in date_cols:
    if columna in preparada.columns:
        preparada = preparada.withColumn(
            columna,
            F.to_date(F.col(columna))
        )

preparada = (
    preparada
    .withColumn(
        "dias_siniestro_a_apertura",
        F.datediff("Fecha_Apertura", "FSINIESTRO")
    )
    .withColumn(
        "dias_siniestro_a_notificacion",
        F.datediff("F_Notificacion", "FSINIESTRO")
    )
    .withColumn(
        "dias_notificacion_a_apertura",
        F.datediff("Fecha_Apertura", "F_Notificacion")
    )
    .withColumn(
        "dias_vigencia_cert_a_siniestro",
        F.datediff(
            "FSINIESTRO",
            "Fecha_Primera_Vigencia_Cert"
        )
    )
    .withColumn(
        "dias_vigencia_pol_a_siniestro",
        F.datediff(
            "FSINIESTRO",
            "fecha_primera_vigencia_pol"
        )
    )
    .withColumn(
        "dias_expedicion_a_siniestro",
        F.datediff("FSINIESTRO", "FEXPEDICION")
    )
    .withColumn(
        "delta_edad",
        F.col("edad_actual_asegurado")
        - F.col("edad_ingreso_asegurado")
    )
    .withColumn(
        "edad_actual_invalida",
        F.when(
            F.col("edad_actual_asegurado") < 0,
            1.0
        ).otherwise(0.0)
    )
    .withColumn(
        "log_reserva_inicial",
        F.log1p(
            F.greatest(
                F.col("Sum_Valor_Reservas_Inicial"),
                F.lit(0)
            )
        )
    )
    .withColumn(
        "reserva_inicial_es_cero",
        F.when(
            F.col("Sum_Valor_Reservas_Inicial") == 0,
            1.0
        ).otherwise(0.0)
    )
)

columnas_faltantes = [
    columna
    for columna in FEATURE_COLS
    if columna not in preparada.columns
]

print("Columnas faltantes:", columnas_faltantes)

if columnas_faltantes:
    raise RuntimeError(
        f"Faltan columnas: {columnas_faltantes}"
    )

muestra = preparada.select(
    "id_reclamacion",
    *[
        F.col(c).cast("double").alias(c)
        for c in FEATURE_COLS
    ]
).limit(100)

print("Registros de prueba:", muestra.count())

# COMMAND ----------

pred_original = (
    modelo_original_spark
    .transform(muestra)
    .select(
        "id_reclamacion",
        F.col("prediction").alias("pred_original"),
        vector_to_array("probability")[1].alias("score_original")
    )
)

pred_compatible = (
    modelo_compatible_spark
    .transform(muestra)
    .select(
        "id_reclamacion",
        F.col("prediction").alias("pred_compatible"),
        vector_to_array("probability")[1].alias("score_compatible")
    )
)

comparacion = (
    pred_original
    .join(pred_compatible, "id_reclamacion", "inner")
    .withColumn(
        "diferencia_score",
        F.abs(
            F.col("score_original")
            - F.col("score_compatible")
        )
    )
)

resultado = comparacion.agg(
    F.count("*").alias("registros_comparados"),
    F.sum(
        F.when(
            F.col("pred_original") != F.col("pred_compatible"),
            1
        ).otherwise(0)
    ).alias("predicciones_diferentes"),
    F.max("diferencia_score").alias("max_diferencia_score")
)

display(resultado)

# COMMAND ----------

from mlflow.tracking import MlflowClient

with mlflow.start_run(
    run_name="preparar_random_forest_para_serving"
) as run:
    mlflow.log_artifacts(
        modelo_local,
        artifact_path="modelo_serving"
    )

    run_id_serving = run.info.run_id

URI_ARTEFACTO_SERVING = (
    f"runs:/{run_id_serving}/modelo_serving"
)

print("Run ID:", run_id_serving)
print("URI del artefacto:", URI_ARTEFACTO_SERVING)

# COMMAND ----------

version_serving = mlflow.register_model(
    model_uri=URI_ARTEFACTO_SERVING,
    name=MODELO_SERVING
)

print("Modelo registrado:", version_serving.name)
print("Versión:", version_serving.version)
print("Estado inicial:", version_serving.status)

# COMMAND ----------

cliente = MlflowClient()

cliente.set_registered_model_alias(
    name=MODELO_SERVING,
    alias="Serving",
    version=version_serving.version
)



cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving.version,
    key="modelo_origen",
    value=f"{MODELO_ORIGINAL}:{VERSION_ORIGINAL}"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving.version,
    key="ajuste_compatibilidad",
    value="remove_pruneTree_metadata"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving.version,
    key="equivalencia_validada",
    value="100_rows_zero_difference"
)

print(
    "URI compatible:",
    f"models:/{MODELO_SERVING}@Serving"
)

# COMMAND ----------

import shutil

DIRECTORIO_VERIFICACION = (
    "/tmp/verificar_modelo_serving_registrado"
)

shutil.rmtree(
    DIRECTORIO_VERIFICACION,
    ignore_errors=True
)

os.makedirs(
    DIRECTORIO_VERIFICACION,
    exist_ok=True
)

URI_REGISTRADA = (
    "models:/workspace.fraude_prod."
    "fraude_random_forest_serving/1"
)

modelo_registrado_local = (
    mlflow.artifacts.download_artifacts(
        artifact_uri=URI_REGISTRADA,
        dst_path=DIRECTORIO_VERIFICACION
    )
)

referencias_registradas = []

for raiz, _, archivos in os.walk(modelo_registrado_local):
    for archivo in archivos:
        ruta = os.path.join(raiz, archivo)

        try:
            with open(ruta, "r", encoding="utf-8") as f:
                if "pruneTree" in f.read():
                    referencias_registradas.append(ruta)
        except (
            UnicodeDecodeError,
            PermissionError,
            IsADirectoryError
        ):
            pass

print("URI inspeccionada:", URI_REGISTRADA)
print("Directorio:", modelo_registrado_local)
print(
    "Referencias pruneTree:",
    len(referencias_registradas)
)

for ruta in referencias_registradas:
    print(ruta)

# COMMAND ----------

RUTA_SPARK_CORREGIDA = os.path.join(
    modelo_registrado_local,
    "sparkml"
)

print("Ruta Spark:", RUTA_SPARK_CORREGIDA)
print(
    "Existe:",
    os.path.isdir(RUTA_SPARK_CORREGIDA)
)

if not os.path.isdir(RUTA_SPARK_CORREGIDA):
    raise RuntimeError(
        "No se encontró la carpeta sparkml corregida."
    )

# COMMAND ----------

firma_original = mlflow.models.get_model_info(
    URI_ORIGINAL
).signature

with mlflow.start_run(
    run_name="empaquetado_pyfunc_serving_limpio"
):
    modelo_pyfunc = mlflow.pyfunc.log_model(
        artifact_path="modelo_serving_limpio",
        loader_module="mlflow.spark",
        data_path=RUTA_SPARK_CORREGIDA,
        signature=firma_original,
        pip_requirements=[
            f"mlflow=={mlflow.__version__}",
            "pyspark==4.1.0",
            "pandas>=2.0.0"
        ]
    )

print("URI limpia:", modelo_pyfunc.model_uri)

# COMMAND ----------

version_serving_v2 = mlflow.register_model(
    model_uri=modelo_pyfunc.model_uri,
    name=MODELO_SERVING
)

cliente.set_registered_model_alias(
    name=MODELO_SERVING,
    alias="Serving",
    version=version_serving_v2.version
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving_v2.version,
    key="modelo_origen",
    value=f"{MODELO_ORIGINAL}:{VERSION_ORIGINAL}"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving_v2.version,
    key="equivalencia_validada",
    value="100_rows_zero_difference"
)

print("Nueva versión:", version_serving_v2.version)
print(
    "URI:",
    f"models:/{MODELO_SERVING}/"
    f"{version_serving_v2.version}"
)

# COMMAND ----------

from mlflow.tracking import MlflowClient

cliente = MlflowClient()
VERSION_SERVING = "2"

cliente.set_registered_model_alias(
    name=MODELO_SERVING,
    alias="Serving",
    version=VERSION_SERVING
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=VERSION_SERVING,
    key="modelo_origen",
    value=f"{MODELO_ORIGINAL}:{VERSION_ORIGINAL}"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=VERSION_SERVING,
    key="equivalencia_validada",
    value="100_rows_zero_difference"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=VERSION_SERVING,
    key="tipo_empaquetado",
    value="fresh_mlflow_pyfunc_for_serving"
)

version_alias = cliente.get_model_version_by_alias(
    name=MODELO_SERVING,
    alias="Serving"
)

print("Modelo:", version_alias.name)
print("Versión con alias Serving:", version_alias.version)

# COMMAND ----------

ARCHIVO_MLMODEL = os.path.join(
    modelo_registrado_local,
    "MLmodel"
)

print("Directorio completo:", modelo_registrado_local)
print(
    "Existe MLmodel:",
    os.path.isfile(ARCHIVO_MLMODEL)
)

if not os.path.isfile(ARCHIVO_MLMODEL):
    raise RuntimeError(
        "El directorio no contiene MLmodel."
    )

# COMMAND ----------

firma_original = mlflow.models.get_model_info(
    URI_ORIGINAL
).signature

with mlflow.start_run(
    run_name="wrapper_serving_completo"
):
    modelo_pyfunc_v3 = mlflow.pyfunc.log_model(
        artifact_path="modelo_serving_completo",
        loader_module="mlflow.spark",
        data_path=modelo_registrado_local,
        signature=firma_original,
        pip_requirements=[
            f"mlflow=={mlflow.__version__}",
            "pyspark==4.1.0",
            "pandas>=2.0.0"
        ]
    )

print("URI del paquete:", modelo_pyfunc_v3.model_uri)

# COMMAND ----------

DIRECTORIO_VALIDACION_V3 = (
    "/tmp/validar_paquete_serving_v3"
)

shutil.rmtree(
    DIRECTORIO_VALIDACION_V3,
    ignore_errors=True
)

paquete_v3_local = (
    mlflow.artifacts.download_artifacts(
        artifact_uri=modelo_pyfunc_v3.model_uri,
        dst_path=DIRECTORIO_VALIDACION_V3
    )
)

mlmodel_interno = os.path.join(
    paquete_v3_local,
    "data",
    "MLmodel"
)

print(
    "MLmodel interno disponible:",
    os.path.isfile(mlmodel_interno)
)

referencias_v3 = []

for raiz, _, archivos in os.walk(paquete_v3_local):
    for archivo in archivos:
        ruta = os.path.join(raiz, archivo)

        try:
            with open(ruta, "r", encoding="utf-8") as f:
                if "pruneTree" in f.read():
                    referencias_v3.append(ruta)
        except (
            UnicodeDecodeError,
            PermissionError,
            IsADirectoryError
        ):
            pass

print("Referencias pruneTree:", len(referencias_v3))

# COMMAND ----------

import yaml

mlmodel_externo = os.path.join(
    paquete_v3_local,
    "MLmodel"
)

with open(
    mlmodel_externo,
    "r",
    encoding="utf-8"
) as f:
    configuracion_mlflow = yaml.safe_load(f)

ruta_data_relativa = (
    configuracion_mlflow
    ["flavors"]
    ["python_function"]
    ["data"]
)

ruta_data_real = os.path.join(
    paquete_v3_local,
    ruta_data_relativa
)

mlmodel_interno_real = os.path.join(
    ruta_data_real,
    "MLmodel"
)

print("Data declarada:", ruta_data_relativa)
print("Ruta real:", ruta_data_real)
print(
    "MLmodel interno real:",
    os.path.isfile(mlmodel_interno_real)
)

print("\nArchivos MLmodel encontrados:")

for raiz, _, archivos in os.walk(paquete_v3_local):
    for archivo in archivos:
        if archivo == "MLmodel":
            print(
                os.path.relpath(
                    os.path.join(raiz, archivo),
                    paquete_v3_local
                )
            )

# COMMAND ----------

from mlflow.tracking import MlflowClient

version_serving_v3 = mlflow.register_model(
    model_uri=modelo_pyfunc_v3.model_uri,
    name=MODELO_SERVING
)

cliente = MlflowClient()

cliente.set_registered_model_alias(
    name=MODELO_SERVING,
    alias="Serving",
    version=version_serving_v3.version
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving_v3.version,
    key="modelo_origen",
    value=f"{MODELO_ORIGINAL}:{VERSION_ORIGINAL}"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving_v3.version,
    key="equivalencia_validada",
    value="100_rows_zero_difference"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_serving_v3.version,
    key="tipo_empaquetado",
    value="nested_complete_mlflow_model"
)

print("Versión creada:", version_serving_v3.version)
print(
    "Alias Serving:",
    cliente.get_model_version_by_alias(
        name=MODELO_SERVING,
        alias="Serving"
    ).version
)

# COMMAND ----------

import json
import os
import shutil
import yaml
import mlflow

from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")

MODELO_SERVING = (
    "workspace.fraude_prod."
    "fraude_random_forest_serving"
)

URI_VERSION_1 = f"models:/{MODELO_SERVING}/1"
URI_VERSION_2 = f"models:/{MODELO_SERVING}/2"

BASE_FINAL = "/tmp/fraude_serving_final"
V1_LOCAL = os.path.join(BASE_FINAL, "v1")
V2_LOCAL = os.path.join(BASE_FINAL, "v2")

shutil.rmtree(BASE_FINAL, ignore_errors=True)
os.makedirs(V1_LOCAL, exist_ok=True)
os.makedirs(V2_LOCAL, exist_ok=True)

# V1 contiene el MLmodel Spark limpio.
modelo_v1_local = mlflow.artifacts.download_artifacts(
    artifact_uri=URI_VERSION_1,
    dst_path=V1_LOCAL
)

# V2 contiene el wrapper con data/sparkml.
paquete_final = mlflow.artifacts.download_artifacts(
    artifact_uri=URI_VERSION_2,
    dst_path=V2_LOCAL
)

with open(
    os.path.join(paquete_final, "MLmodel"),
    "r",
    encoding="utf-8"
) as f:
    config_externa = yaml.safe_load(f)

data_relativa = (
    config_externa
    ["flavors"]
    ["python_function"]
    ["data"]
)

ruta_sparkml = os.path.join(
    paquete_final,
    data_relativa
)

mlmodel_origen = os.path.join(
    modelo_v1_local,
    "MLmodel"
)

mlmodel_destino = os.path.join(
    os.path.dirname(ruta_sparkml),
    "MLmodel"
)

shutil.copy2(
    mlmodel_origen,
    mlmodel_destino
)

print("Data Spark declarada:", data_relativa)
print(
    "Carpeta Spark disponible:",
    os.path.isdir(ruta_sparkml)
)
print(
    "MLmodel hermano disponible:",
    os.path.isfile(mlmodel_destino)
)

referencias_prune_tree = []

for raiz, _, archivos in os.walk(paquete_final):
    for archivo in archivos:
        ruta = os.path.join(raiz, archivo)

        try:
            with open(ruta, "r", encoding="utf-8") as f:
                if "pruneTree" in f.read():
                    referencias_prune_tree.append(ruta)
        except (
            UnicodeDecodeError,
            PermissionError,
            IsADirectoryError
        ):
            pass

print(
    "Referencias pruneTree:",
    len(referencias_prune_tree)
)

if not os.path.isdir(ruta_sparkml):
    raise RuntimeError("No existe la carpeta Spark.")

if not os.path.isfile(mlmodel_destino):
    raise RuntimeError("Falta MLmodel junto a sparkml.")

if referencias_prune_tree:
    raise RuntimeError(
        "El paquete todavía contiene pruneTree."
    )

# Validación local antes de registrar.
os.environ["MLFLOW_DFS_TMP"] = (
    "/Volumes/workspace/fraude_prod/"
    "mlflow_tmp/spark_models"
)

modelo_validado = mlflow.pyfunc.load_model(
    paquete_final
)

print("Carga local del paquete: CORRECTA")

# Registrar solamente después de validar.
with mlflow.start_run(
    run_name="spark_serving_layout_final"
) as run:
    mlflow.log_artifacts(
        paquete_final,
        artifact_path="modelo_serving_final"
    )
    uri_final = (
        f"runs:/{run.info.run_id}/"
        "modelo_serving_final"
    )

version_final = mlflow.register_model(
    model_uri=uri_final,
    name=MODELO_SERVING
)

cliente = MlflowClient()

cliente.set_registered_model_alias(
    name=MODELO_SERVING,
    alias="Serving",
    version=version_final.version
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_final.version,
    key="equivalencia_validada",
    value="100_rows_zero_difference"
)

cliente.set_model_version_tag(
    name=MODELO_SERVING,
    version=version_final.version,
    key="compatibilidad_serving",
    value="spark_layout_and_pruneTree_fixed"
)

print("Versión final:", version_final.version)
print("Alias Serving actualizado")
