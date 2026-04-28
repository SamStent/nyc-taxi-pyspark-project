# settings.py

# Nombre del proyecto
PROJECT_NAME = "nyc_taxi_pyspark_project"

# Rutas en Databricks (DBFS)
BRONZE_PATH = "dbfs:/mnt/nyc_taxi/bronze/"
SILVER_PATH = "dbfs:/mnt/nyc_taxi/silver/"
GOLD_PATH = "dbfs:/mnt/nyc_taxi/gold/"

# Nombres de tablas Delta
BRONZE_TABLE = "nyc_taxi_bronze"
SILVER_TABLE = "nyc_taxi_silver"
GOLD_TABLE = "nyc_taxi_gold"

# Columnas clave
DATETIME_COL = "tpep_pickup_datetime"
YEAR_COL = "year"
MONTH_COL = "month"
