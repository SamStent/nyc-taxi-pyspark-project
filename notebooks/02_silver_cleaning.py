# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofmonth, month, year,
    unix_timestamp, (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
)
from pyspark.sql.functions import expr
from src.config.settings import (
    BRONZE_TABLE, SILVER_PATH, SILVER_TABLE,
    DATETIME_COL
)

spark = SparkSession.builder.getOrCreate()

# 1. Leer tabla Bronze
df = spark.table(BRONZE_TABLE)

# 2. Conversión de tipos
df = (
    df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
      .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
      .withColumn("trip_distance", col("trip_distance").cast("float"))
      .withColumn("fare_amount", col("fare_amount").cast("float"))
      .withColumn("passenger_count", col("passenger_count").cast("int"))
)

# 3. Filtrar registros inválidos
df = df.filter(col("tpep_pickup_datetime").isNotNull())
df = df.filter(col("tpep_dropoff_datetime").isNotNull())
df = df.filter(col("trip_distance") > 0)
df = df.filter(col("fare_amount") > 0)
df = df.filter(col("passenger_count") > 0)
df = df.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

# Outliers: duración > 3 horas o distancia > 100 km
df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
)

df = df.filter(col("trip_duration_minutes") < 180)
df = df.filter(col("trip_distance") < 100)

# 4. Columnas derivadas
df = (
    df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
      .withColumn("pickup_day", dayofmonth("tpep_pickup_datetime"))
      .withColumn("pickup_month", month("tpep_pickup_datetime"))
      .withColumn("pickup_year", year("tpep_pickup_datetime"))
)

# 5. Escribir Silver en Delta
(
    df.write
      .format("delta")
      .mode("overwrite")
      .partitionBy("pickup_year", "pickup_month")
      .save(SILVER_PATH)
)

# 6. Registrar tabla Silver
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print("Silver layer created successfully.")
