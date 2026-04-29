import streamlit as st
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pandas as pd

# Crear/obtener sesión de Spark (en Databricks ya existe)
builder = (
    SparkSession.builder
    .appName("NYC Taxi Local Pipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Nombre de la tabla Gold
GOLD_TABLE = "nyc_taxi_gold"

@st.cache_data
def load_data():
    # df = spark.read.format("delta").load("data/delta/gold")
    df = spark.read.parquet("data/delta/gold") # -> En casa (Linux) mejor esta sino peta el ordenador.
    pdf = df.toPandas()
    return pdf

st.title("NYC Taxi – Gold KPIs Dashboard 🚕")

st.write("Este dashboard utiliza la capa **Gold** del pipeline (datos limpios y agregados).")

# Cargar datos
pdf = load_data()

# Asegurarnos de que las columnas clave existen
required_cols = [
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
    "pickup_year",
    "pickup_month",
    "pickup_day",
    "pickup_hour"
]

missing = [c for c in required_cols if c not in pdf.columns]
if missing:
    st.error(f"Faltan columnas en la tabla Gold: {missing}")
    st.stop()

# KPIs básicos
total_trips = len(pdf)
total_revenue = pdf["fare_amount"].sum()
avg_distance = pdf["trip_distance"].mean()
avg_duration = pdf["trip_duration_minutes"].mean()

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Trips", f"{total_trips:,}")
col2.metric("Total Revenue", f"${total_revenue:,.2f}")
col3.metric("Avg Distance (km)", f"{avg_distance:.2f}")
col4.metric("Avg Duration (min)", f"{avg_duration:.1f}")

