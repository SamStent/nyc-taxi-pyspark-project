# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as _sum,
    hour, dayofmonth, month, year
)
from src.config.settings import SILVER_TABLE, GOLD_PATH, GOLD_TABLE

spark = SparkSession.builder.getOrCreate()

# 1. Leer Silver
df = spark.table(SILVER_TABLE)

# 2. KPIs globales
kpi_global = df.agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_distance"),
    avg("trip_duration_minutes").alias("avg_duration"),
    _sum("fare_amount").alias("total_revenue"),
    avg("fare_amount").alias("avg_revenue")
)

# 3. Viajes por hora
trips_by_hour = (
    df.groupBy("pickup_hour")
      .agg(count("*").alias("trips"))
      .orderBy("pickup_hour")
)

# 4. Viajes por día
trips_by_day = (
    df.groupBy("pickup_year", "pickup_month", "pickup_day")
      .agg(count("*").alias("trips"))
      .orderBy("pickup_year", "pickup_month", "pickup_day")
)

# 5. Ingresos por día
revenue_by_day = (
    df.groupBy("pickup_year", "pickup_month", "pickup_day")
      .agg(_sum("fare_amount").alias("revenue"))
      .orderBy("pickup_year", "pickup_month", "pickup_day")
)

# 6. Guardar Gold en Delta
(
    df.write
      .format("delta")
      .mode("overwrite")
      .save(GOLD_PATH)
)

# 7. Registrar tabla Gold
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE}
    USING DELTA
    LOCATION '{GOLD_PATH}'
""")

print("Gold layer created successfully.")
