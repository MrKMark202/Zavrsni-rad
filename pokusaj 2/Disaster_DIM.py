import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Kreiranje Spark sesije
spark = SparkSession.builder\
        .appName("US_DISASTER_SCHEMA")\
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

# 1. Dohvat podataka iz PostgreSQL-a (tablica Disaster)
disaster_data = spark.read.jdbc(
    url=POSTGRES_CONFIG["url"],
    table='"Disaster"',
    properties=POSTGRES_CONFIG["properties"]
).select(
    "disaster_number", 
    "incident_type", 
    F.col("ih_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
    F.col("ia_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
    F.col("pa_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
    F.col("hm_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
)

# 2. Dohvat podataka iz CSV-a
csv_data = spark.read.csv("data/US_DISASTERS_PROCESSED_20.csv", header=True, inferSchema=True)
csv_disaster_data = csv_data.select(
    "disaster_number", 
    "incident_type", 
    F.col("ih_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
    F.col("ia_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
    F.col("pa_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
    F.col("hm_program_declared").cast("int"),  # Konverzija BOOLEAN → INT
)

# 3. Spajanje podataka iz PostgreSQL-a i CSV-a, uklanjanje duplikata
combined_disaster_data = (
    csv_disaster_data
    .union(disaster_data)  # Spajamo podatke iz baze i CSV-a
)

# 4. Dodavanje tehničkog ključa (redoslijedno)
window_spec = Window.orderBy("disaster_number")  
disasterDIM = (
    combined_disaster_data
    .withColumn("disaster_tk", F.row_number().over(window_spec))  # Tehnički ključ
    .withColumn("version", F.lit(1))  
    .withColumn("date_from", F.lit("1900-01-01"))  
    .withColumn("date_to", F.lit("2200-01-01"))  
)

# 5. Spremanje u PostgreSQL tablicu "Disaster_DIM"
disasterDIM.write.jdbc(
    url=POSTGRES_CONFIG["url"],
    table="Disaster_DIM",
    mode="overwrite",
    properties=POSTGRES_CONFIG["properties"]
)