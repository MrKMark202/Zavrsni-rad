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

# 1. Dohvat podataka iz PostgreSQL-a (tablica Declaration)
declaration_data = spark.read.jdbc(
    url=POSTGRES_CONFIG["url"],
    table='"Declaration"',
    properties=POSTGRES_CONFIG["properties"]
).select("last_refresh", "place_code")

# 2. Dohvat podataka iz CSV-a (isti stupci kao iz Declaration)
# 4. Dohvat podataka iz CSV-a i preimenovanje "state" u "name"
csv_data = spark.read.csv("data/US_DISASTERS_PROCESSED_20.csv", header=True, inferSchema=True)
csv_fema_data = csv_data.select("last_refresh", "place_code")

# 3. Spajanje podataka iz PostgreSQL-a i CSV-a, uklanjanje duplikata
combined_fema_data = (
    csv_fema_data
    .union(declaration_data)  # Spajamo podatke iz baze i CSV-a
)

# 4. Dodavanje tehničkog ključa (redoslijedno)
window_spec = Window.orderBy("place_code")  # Redoslijed po deklaraciji
femaDIM = (
    combined_fema_data
    .withColumn("fema_tk", F.row_number().over(window_spec))  # Osigurava redoslijedne ID-eve
    .withColumn("version", F.lit(1))  # Početna verzija
    .withColumn("date_from", F.lit("1900-01-01"))  # Početni datum
    .withColumn("date_to", F.lit("2200-01-01"))  # Krajnji datum
)

# 5. Spremanje u PostgreSQL tablicu "FEMA_DIM"
femaDIM.write.jdbc(
    url=POSTGRES_CONFIG["url"],
    table="FEMA_DIM",
    mode="overwrite",
    properties=POSTGRES_CONFIG["properties"]
)