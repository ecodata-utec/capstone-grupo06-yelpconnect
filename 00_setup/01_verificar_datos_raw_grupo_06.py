# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Verificación de datos crudos
# MAGIC Valida que los 5 archivos del Yelp Open Dataset (muestreados) estén en el Volume
# MAGIC y hace una primera exploración de esquemas. **No transforma nada** — eso es trabajo
# MAGIC del pipeline (E7).
# MAGIC
# MAGIC > **Trazabilidad —** las observaciones alimentan las secciones 4.2 (modelo lógico) y 6.2 (reglas de calidad).

# COMMAND ----------

RAW = "/Volumes/workspace/yelp_bronze/raw"
ESPERADOS = ["business", "review", "user", "tip", "checkin"]

archivos = [f.name for f in dbutils.fs.ls(RAW)]
print("Archivos en el Volume:", archivos)

faltantes = [e for e in ESPERADOS if not any(e in a for a in archivos)]
assert not faltantes, f"❌ Faltan archivos: {faltantes}. Suban la muestra al Volume."
print("✔ Los 5 datasets están presentes")

tam_mb = sum(f.size for f in dbutils.fs.ls(RAW) if f.name.endswith(".json.gz")) / 1024**2
print(f"Tamaño de la muestra en crudo (.json.gz): {tam_mb:,.1f} MB · meta de la guía: < 300 MB")

# COMMAND ----------

# DBTITLE 1: Exploración rápida de esquemas y volúmenes
for e in ESPERADOS:
    df = spark.read.json(f"{RAW}/{e}.json.gz")
    print(f"\n=== {e} · {df.count():,} filas ===")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observaciones sobre los datos crudos
# MAGIC
# MAGIC 1. Se observan campos anidados o en lista dentro de un texto:`business.attributes` y `business.hours` llegan como objetos anidados; `categories` es una lista en un solo texto separado por comas; y en `checkin`, todas las visitas de un negocio vienen en un solo texto de fechas (`"2016-04-26 19:49:16, 2016-08-30 18:36:57, ..."`).
# MAGIC
# MAGIC 2. Se observan algunos tipos sospechosos, por ejemplo las fechas llegan como texto con hora, `stars` de la reseña aparece con decimal (`4.0`) aunque es un entero de 1 a 5, e `is_open` es un 0/1 en lugar de verdadero/falso.
# MAGIC
# MAGIC 3. Se observan posibles problemas de calidad, ya que `tip` no tiene un identificador propio, algunos negocios traen `attributes` u `hours` vacíos, y el texto de reseñas y tips es libre, así que puede contener nombres u otros datos personales.

# COMMAND ----------

