import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

st.title("NYC Taxi Dashboard 🚕")

# Leer tabla Gold
df = spark.table("nyc_taxi_gold")

# Convertir a Pandas para Streamlit
pdf = df.toPandas()

# KPI: viajes totales
total_trips = len(pdf)
st.metric("Total Trips", f"{total_trips:,}")

# Gráfico simple: viajes por hora
pdf["hour"] = pd.to_datetime(pdf["tpep_pickup_datetime"]).dt.hour
hourly = pdf.groupby("hour").size().reset_index(name="count")

st.bar_chart(hourly, x="hour", y="count")
