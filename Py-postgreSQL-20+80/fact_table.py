import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Kreiranje Spark sesije
spark = SparkSession.builder\
        .appName("US_DISASTER_FACT_TABLE")\
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


# 1. Učitavanje svih dimenzijskih tablica
disaster_dim = spark.read.jdbc(POSTGRES_CONFIG["url"], "disaster_dim", properties=POSTGRES_CONFIG["properties"])
state_dim = spark.read.jdbc(POSTGRES_CONFIG["url"], "state_dim", properties=POSTGRES_CONFIG["properties"])
fema_dim = spark.read.jdbc(POSTGRES_CONFIG["url"], "fema_dim", properties=POSTGRES_CONFIG["properties"])
incident_dates_dim = spark.read.jdbc(POSTGRES_CONFIG["url"], "incident_datesdim", properties=POSTGRES_CONFIG["properties"])
declaration_dim = spark.read.jdbc(POSTGRES_CONFIG["url"], "declaration_dim", properties=POSTGRES_CONFIG["properties"])

disaster_dim.printSchema()

# 2. Spajanje svih tehničkih ključeva na disaster_dim
fact_table = (
    disaster_dim
    .join(state_dim, disaster_dim["disaster_tk"] == state_dim["state_tk"], "left")  
    .join(fema_dim, disaster_dim["disaster_tk"] == fema_dim["fema_tk"], "left")  
    .join(incident_dates_dim, disaster_dim["disaster_tk"] == incident_dates_dim["incident_dates_tk"], "left")  
    .join(declaration_dim, disaster_dim["disaster_tk"] == declaration_dim["declaration_tk"], "left")  
    .select("state_tk", "fema_tk", "incident_dates_tk", "declaration_tk", "disaster_tk", "deaths") 
)

# 3. Uklanjanje redaka s NULL vrijednostima u ključevima
fact_table = fact_table.dropna(subset=["state_tk", "incident_dates_tk"])

# 3. Spremanje u PostgreSQL kao "Fact_Disaster"
fact_table.write.jdbc(
    url=POSTGRES_CONFIG["url"],
    table="fact_table",
    mode="overwrite",
    properties=POSTGRES_CONFIG["properties"]
)

print("Tablica činjenica 'Fact_Disaster' uspješno kreirana!")
