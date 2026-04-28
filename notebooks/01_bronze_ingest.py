# Databricks notebook source
from pyspark.sql.functions import year, month
from pyspark.sql import SparkSession
from src.config.settings import BRONZE_PATH, BRONZE_TABLE, DATETIME_COL

spark = SparkSession.builder.getOrCreate()

# Ruta local o DBFS donde subiste los CSV
input_path = "dbfs:/FileStore/tables/yellow_tripdata_2015_01.csv"

# 1. Leer CSV crudo
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

# 2. Añadir columnas de año y mes
df = df.withColumn("year", year(df[DATETIME_COL]))
df = df.withColumn("month", month(df[DATETIME_COL]))

# 3. Escribir en Delta (Bronze)
(
    df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "month")
    .save(BRONZE_PATH)
)

# 4. Registrar tabla en el metastore
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print("Bronze layer created successfully.")
