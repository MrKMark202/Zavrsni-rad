import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime

# Kreiranje Spark sesije
spark = SparkSession.builder\
        .appName("US_DISASTER_SHEMA")\
        .config("spark.sql.shuffle.partitions", "4")\
        .getOrCreate()

# Konfiguracija za PostgreSQL
POSTGRES_CONFIG = {
    "url": f"jdbc:postgresql://localhost:5432/us_disasters2",
    "properties": {
        "user": "postgres",
        "password": "1234",
        "driver": "org.postgresql.Driver",
    },
}

# 1. Dohvat podataka iz PostgreSQL-a (tablica State)
state_data = spark.read.jdbc(
    url=POSTGRES_CONFIG["url"],
    table='"State"',  # Tablica iz koje učitavamo
    properties=POSTGRES_CONFIG["properties"]
)

print("Podaci iz PostgreSQL-a uspješno učitani!")



# 4. Dohvat podataka iz CSV-a i preimenovanje "state" u "name"
csv_data = spark.read.csv("data/US_DISASTERS_PROCESSED_20.csv", header=True, inferSchema=True)
csv_data = csv_data.withColumnRenamed("state", "name")

# Spajanje podataka iz CSV i PostgreSQL
combined_data = (
    csv_data.select(F.col("name"), F.col("fips"))  # Preimenujemo 'state' u 'name'
    .union(state_data.select(F.col("name"), F.col("fips")))  # Spajamo s podacima iz PostgreSQL
)

# Dodavanje tehničkog ključa (redoslijedno)
window_spec = Window.orderBy("name")  # Redoslijed po imenu
stateDIM = (
    combined_data
    .withColumn("state_tk", F.row_number().over(window_spec))  # Osigurava redoslijedne ID-eve
    .withColumn("version", F.lit(1))  # Početna verzija
    .withColumn("date_from", F.lit("1900-01-01"))  # Početni datum
    .withColumn("date_to", F.lit("2200-01-01"))  # Krajnji datum
)

# Spremanje u PostgreSQL
stateDIM.write.jdbc(
    url=POSTGRES_CONFIG["url"],
    table="State_DIM",
    mode="overwrite",
    properties=POSTGRES_CONFIG["properties"]
)

# 9. Zatvaranje Spark sessiona
spark.stop()
