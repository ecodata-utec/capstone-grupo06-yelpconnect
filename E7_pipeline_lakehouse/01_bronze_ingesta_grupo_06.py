# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # E7.1 · Ingesta a Bronze
# MAGIC **Cierra con:** M6 Big Data (batch) + M7 Integración (incremental/streaming)
# MAGIC
# MAGIC Principio de la capa bronze: **fidelidad al origen** — sin limpiar, sin filtrar,
# MAGIC pero SÍ con metadata de auditoría (archivo origen, timestamp de ingesta).
# MAGIC
# MAGIC Dos modos:
# MAGIC - **Batch full** (mínimo obligatorio): `read_files` sobre el Volume.
# MAGIC - **Incremental con Auto Loader** (obligatorio para al menos UNA entidad, se ve en M7):
# MAGIC   demuestra ingesta idempotente — si suben un archivo nuevo al Volume, solo procesa lo nuevo.
# MAGIC
# MAGIC > **Trazabilidad —** implementa los bloques *Ingesta de archivos de origen* y *Almacenamiento analítico versionado*
# MAGIC > de la sección 3.2, la retención de ADR-04 y el derecho de supresión de la sección 5.4 del Documento Formal.

# COMMAND ----------

RAW = "/Volumes/workspace/yelp_bronze/raw"
import time
T0 = time.time()   # bitácora del pipeline (E7/04)
ENTIDADES = ["business", "user", "tip", "checkin"]  # review va con Auto Loader abajo

def retencion_30d(tabla):
    """ADR-04: el historial de Delta se conserva 30 días; así una supresión desaparece también de las versiones anteriores."""
    spark.sql(f"""ALTER TABLE {tabla} SET TBLPROPERTIES (
      'delta.logRetentionDuration' = 'interval 30 days',
      'delta.deletedFileRetentionDuration' = 'interval 30 days')""")

# Falla controlada (M10 · sección 8.1): demuestra el reintento del job. Ver E7/04.
dbutils.widgets.dropdown("simular_falla", "no", ["no", "si"], "Simular falla controlada (M10)")
FLAG_FALLA = "/Volumes/workspace/yelp_gov/artefactos/_simular_falla"
def existe(ruta):
    try:
        dbutils.fs.ls(ruta); return True
    except Exception:
        return False
if dbutils.widgets.get("simular_falla") == "si" and existe(FLAG_FALLA):
    dbutils.fs.rm(FLAG_FALLA)    # el reintento ya no encontrará el archivo y la tarea terminará bien
    raise RuntimeError("Falla controlada (M10): el reintento automático del job debe superarla")

# COMMAND ----------

# DBTITLE 1: Modo batch — 4 entidades
from pyspark.sql import functions as F

for e in ENTIDADES:
    (spark.read.json(f"{RAW}/{e}.json.gz")
        .withColumn("_archivo_origen", F.col("_metadata.file_path"))
        .withColumn("_fecha_ingesta", F.current_timestamp())
        .write.mode("overwrite")
        .saveAsTable(f"workspace.yelp_bronze.brz_{e}"))
    retencion_30d(f"workspace.yelp_bronze.brz_{e}")
    print(f"✔ brz_{e}: {spark.table(f'workspace.yelp_bronze.brz_{e}').count():,} filas")

# COMMAND ----------

# DBTITLE 1: Modo incremental — Auto Loader para review (la entidad de mayor volumen)

CHECKPOINT = "/Volumes/workspace/yelp_bronze/raw/_checkpoints/review"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .load(f"{RAW}/review*.json.gz")
    .withColumn("_archivo_origen", F.col("_metadata.file_path"))
    .withColumn("_fecha_ingesta", F.current_timestamp())
 .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)          # micro-batch y termina: amigable con la cuota serverless
    .toTable("workspace.yelp_bronze.brz_review")
).awaitTermination()
retencion_30d("workspace.yelp_bronze.brz_review")

print(f"✔ brz_review: {spark.table('workspace.yelp_bronze.brz_review').count():,} filas")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _archivo_origen, count(*) AS filas
# MAGIC FROM workspace.yelp_bronze.brz_review
# MAGIC GROUP BY _archivo_origen;

# COMMAND ----------

# DBTITLE 1,Experimento de idempotencia — reversión (solo uso manual, después de la prueba)
# Elimina de bronze las filas cargadas desde review_2.json.gz durante el experimento de idempotencia (M7),
# para conservar la muestra documentada (400.000 reseñas). Pasos:
#   1. Cambiar REVERTIR a True y ejecutar SOLO esta celda (con una cuenta de Data Custodian).
#   2. Borrar review_2.json.gz del Volume: Catalog → workspace → yelp_bronze → raw.
#   3. Volver a poner REVERTIR = False, para que el job diario no la ejecute.
REVERTIR = False

if REVERTIR:
    borradas = spark.sql("""
        DELETE FROM workspace.yelp_bronze.brz_review
        WHERE _archivo_origen LIKE '%review_2%'""").first()["num_affected_rows"]
    print(f"✔ Reversión aplicada: {borradas:,} filas de review_2 eliminadas de brz_review")
else:
    print("ℹ️ Reversión desactivada (REVERTIR = False): no se modifica brz_review")

display(spark.sql("""SELECT _archivo_origen, count(*) AS filas
                     FROM workspace.yelp_bronze.brz_review GROUP BY _archivo_origen"""))

# COMMAND ----------

# DBTITLE 1,Supresión de datos personales (RGPD art. 17 · Ley 29733 · sección 5.4)
# La lista de supresión se aplica en cada corrida: bronze se recarga desde el Volume, y silver, la cuarentena y gold
# se reconstruyen desde bronze, así que el borrado se propaga solo a todas las capas. Se borran la cuenta, sus reseñas
# y sus tips (Acta 07). VACUUM semanal (E7/03) elimina las versiones anteriores dentro de los 30 días de ADR-04.
SUP = "workspace.yelp_gov.solicitudes_supresion"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {SUP} (
  user_id          STRING NOT NULL COMMENT 'Titular que pidió la supresión de sus datos',
  solicitud_id     STRING          COMMENT 'Identificador de la solicitud RGPD/ARCO',
  fecha_solicitud  DATE            COMMENT 'Fecha en que se recibió la solicitud')
COMMENT 'Lista de supresión (RGPD art. 17, CCPA, Ley 29733). Confidencial: solo steward y custodians (E6)'""")

n_sup = spark.table(SUP).count()
for t in ["brz_user", "brz_review", "brz_tip"]:
    borradas = spark.sql(f"""DELETE FROM workspace.yelp_bronze.{t}
                             WHERE user_id IN (SELECT user_id FROM {SUP})""").first()["num_affected_rows"]
    print(f"✔ Supresión en {t}: {borradas:,} filas eliminadas ({n_sup} titulares en la lista)")

# COMMAND ----------

# DBTITLE 1,Bitácora del pipeline (E7/04 · sección 8.2)
from pyspark.sql import functions as F
spark.sql("""CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_bitacora (
  ejecucion_ts TIMESTAMP, capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE
) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida'""")
duracion = round(time.time() - T0, 1)
filas = [("bronze", f"brz_{e}", spark.table(f"workspace.yelp_bronze.brz_{e}").count(), duracion)
         for e in ["business", "user", "review", "tip", "checkin"]]
(spark.createDataFrame(filas, "capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE")
      .select(F.current_timestamp().alias("ejecucion_ts"), "capa", "tabla", "filas", "duracion_seg")
      .write.insertInto("workspace.yelp_gov.pipeline_bitacora"))
print(f"✔ Bitácora: {len(filas)} registros · ingesta a bronze en {duracion} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experimento obligatorio (evidencia para el informe)
# MAGIC 1. Ejecutar este notebook completo → anotar conteos.
# MAGIC 2. Volver a ejecutar SOLO la celda de Auto Loader **sin subir archivos nuevos** → debe procesar 0 registros.
# MAGIC 3. Subir un archivo `review_2.json.gz` pequeño al Volume y re-ejecutar → solo procesa el nuevo.
# MAGIC
# MAGIC > Experimento de idempotencia (10/11/2026). (1) Al re-ejecutar Auto Loader sin archivos nuevos, brz_review se mantuvo en 400.000 filas: el checkpoint registra los archivos ya procesados. (2) Se subió review_2.json.gz con 1.000 reseñas: Auto Loader procesó solo ese archivo (401.000 filas; review.json.gz sin cambios). (3) Terminada la prueba, se eliminaron esas filas de bronze y el archivo del Volume, para conservar la muestra documentada. Evidencia: img/img/M7_Idempotencia_autoloader.jpg
# MAGIC
# MAGIC ### Por qué Auto Loader es idempotente
# MAGIC El checkpoint guarda qué archivos ya se procesaron y el esquema inferido (`schemaLocation`); cada micro-batch se confirma
# MAGIC una sola vez en Delta (exactly-once). Por eso, al volver a ejecutar sin archivos nuevos se procesan 0 filas, y al subir uno
# MAGIC nuevo solo se procesa ese. `trigger(availableNow=True)` procesa lo pendiente y se apaga: no queda un stream consumiendo cuota.
# MAGIC
# MAGIC ### Batch o incremental por entidad
# MAGIC | Entidad | Modo | Por qué |
# MAGIC |---|---|---|
# MAGIC | review | Auto Loader (incremental) | Entidad de mayor volumen (400.000 filas) y la única que recibe archivos nuevos (`review_2.json.gz`); el checkpoint evita reprocesar |
# MAGIC | user | Batch, overwrite | Tope fijo de 150.000 usuarios; se recarga completa y en cada corrida se le aplica la lista de supresión |
# MAGIC | business | Batch, overwrite | 14.577 filas: recargarla cuesta menos que mantener un checkpoint |
# MAGIC | tip, checkin | Batch, overwrite | Volumen bajo y archivo único por entidad en la muestra |
# MAGIC
# MAGIC ### Definition of Done (E7.1)
# MAGIC - [x ] 5 tablas bronze pobladas con columnas de auditoría.
# MAGIC - [ x] Experimento de idempotencia documentado.
# MAGIC - [x ] Decisión batch vs streaming justificada por entidad (tabla en markdown).
# MAGIC - [x] Supresión (5.4) aplicada en bronze en cada corrida y retención de 30 días (ADR-04).