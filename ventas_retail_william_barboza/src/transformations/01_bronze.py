from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

# Leemos las variables inyectadas desde el bloque configuration: del YAML
bronze_schema = spark.conf.get("bronze_schema", "bronze")
catalog = spark.conf.get("catalog", "proyecto_final")

# Ruta base del volumen usando el catálogo dinámico
VOLUME_PATH = f"/Volumes/{catalog}/landing/raw_data/ventas_retail_william_barboza"

# -------------------------------------------------------------------------
# 1. BRONZE: CLIENTES (CSV)
# -------------------------------------------------------------------------
@dp.table(
    name=f"{bronze_schema}.clientes_raw",
    comment="Ingesta cruda incremental de la entidad clientes (CSV)"
)
def clientes_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/clientes/")
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
    )

# -------------------------------------------------------------------------
# 2. BRONZE: PRODUCTOS (CSV)
# -------------------------------------------------------------------------
@dp.table(
    name=f"{bronze_schema}.productos_raw",
    comment="Ingesta cruda incremental de la entidad productos (CSV)"
)
def productos_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/productos/")
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
    )

# -------------------------------------------------------------------------
# 3. BRONZE: PEDIDOS (JSON)
# -------------------------------------------------------------------------
@dp.table(
    name=f"{bronze_schema}.pedidos_raw",
    comment="Ingesta cruda incremental de la entidad pedidos (JSON)"
)
def pedidos_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .load(f"{VOLUME_PATH}/pedidos/")
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
    )

# -------------------------------------------------------------------------
# 4. BRONZE: DETALLE_PEDIDOS (JSON)
# -------------------------------------------------------------------------
@dp.table(
    name=f"{bronze_schema}.detalle_pedidos_raw",
    comment="Ingesta cruda incremental de la entidad detalle_pedidos (JSON)"
)
def detalle_pedidos_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .load(f"{VOLUME_PATH}/detalle_pedidos/")
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_name"))
    )