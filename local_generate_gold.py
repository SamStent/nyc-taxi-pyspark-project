from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofmonth, month, year

# -----------------------------------------
# SPARK SESSION (sin Delta, estable)
# -----------------------------------------
spark = (
    SparkSession.builder
    .appName("NYC Taxi Local Pipeline - Parquet Stable")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

# -----------------------------------------
# RUTAS
# -----------------------------------------
RAW_PATH = "data/raw"
BRONZE_PATH = "data/delta/bronze"
SILVER_PATH = "data/delta/silver"
GOLD_PATH = "data/delta/gold"

# -----------------------------------------
# BRONZE: lectura de CSV → Parquet
# -----------------------------------------
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(RAW_PATH)
)

df.write.mode("overwrite").parquet(BRONZE_PATH)

# -----------------------------------------
# SILVER: limpieza básica → Parquet
# -----------------------------------------
df = spark.read.parquet(BRONZE_PATH)

df = (
    df.withColumn("pickup_year", year(col("tpep_pickup_datetime")))
      .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
      .withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime")))
      .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
      .withColumn("trip_duration_minutes",
                  (col("tpep_dropoff_datetime").cast("long") -
                   col("tpep_pickup_datetime").cast("long")) / 60)
)

df.write.mode("overwrite").parquet(SILVER_PATH)

# -----------------------------------------
# GOLD: reducir tamaño + KPIs → Parquet
# -----------------------------------------
df = spark.read.parquet(SILVER_PATH)

# Reducir tamaño para evitar OOM
df = df.limit(200000)

df.write.mode("overwrite").parquet(GOLD_PATH)

print("Pipeline completado correctamente.")
spark.stop()



'''from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofmonth, month, year,
    unix_timestamp, count, avg, sum as _sum
)
import os

# Crear/obtener sesión de Spark (en Databricks ya existe)
builder = (
    SparkSession.builder
    .appName("NYC Taxi Local Pipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

RAW_PATH = "data/raw"
BRONZE_PATH = "data/delta/bronze"
SILVER_PATH = "data/delta/silver"
GOLD_PATH = "data/delta/gold"

# -----------------------------
# BRONZE: leer CSV crudos
# -----------------------------
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{RAW_PATH}/*.csv")
)

# Añadir año/mes desde pickup
df = (
    df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
      .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
      .withColumn("year", year("tpep_pickup_datetime"))
      .withColumn("month", month("tpep_pickup_datetime"))
)

df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(BRONZE_PATH)


# -----------------------------
# SILVER: limpieza
# -----------------------------
df = spark.read.format("delta").load(BRONZE_PATH)

df = (
    df.withColumn("trip_distance", col("trip_distance").cast("float"))
      .withColumn("fare_amount", col("fare_amount").cast("float"))
      .withColumn("passenger_count", col("passenger_count").cast("int"))
)

df = df.filter(col("tpep_pickup_datetime").isNotNull())
df = df.filter(col("tpep_dropoff_datetime").isNotNull())
df = df.filter(col("trip_distance") > 0)
df = df.filter(col("fare_amount") > 0)
df = df.filter(col("passenger_count") > 0)
df = df.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
)

df = df.filter(col("trip_duration_minutes") < 180)
df = df.filter(col("trip_distance") < 100)

df = (
    df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
      .withColumn("pickup_day", dayofmonth("tpep_pickup_datetime"))
      .withColumn("pickup_month", month("tpep_pickup_datetime"))
      .withColumn("pickup_year", year("tpep_pickup_datetime"))
)

df.write.format("delta").mode("overwrite").partitionBy("pickup_year", "pickup_month").save(SILVER_PATH)

# -----------------------------
# GOLD: KPIs
# -----------------------------
df = spark.read.format("delta").load(SILVER_PATH)

# Limitar
df = df.limit(200000)

df.write.format("delta").mode("overwrite").save(GOLD_PATH)

# Registrar vista temporal para Streamlit
df.createOrReplaceTempView("nyc_taxi_gold")

print("Tabla Gold generada y vista temporal creada: nyc_taxi_gold")
print("Ahora puedes ejecutar Streamlit.")'''
