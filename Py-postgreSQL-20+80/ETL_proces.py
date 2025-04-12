import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import date

# Povezivanje na bazu
DATABASE_URL = "postgresql://postgres:1234@localhost:5432/us_disasters2"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Učitavanje dodatnih CSV podataka (20%)
csv_file = "data/US_DISASTERS_PROCESSED_20.csv"
df_csv = pd.read_csv(csv_file)

# Ekstrakcija podataka iz baze
disasters = pd.read_sql('SELECT * FROM "Disaster"', engine)
states = pd.read_sql('SELECT * FROM "State"', engine)
declarations = pd.read_sql('SELECT * FROM "Declaration"', engine)

# 🟢 **1. Incident_datesDIM**
incident_dates = disasters[['incident_begin_date', 'incident_end_date']].drop_duplicates()
incident_dates['version'] = 1
incident_dates['date_from'] = date(1900, 1, 1)
incident_dates['date_to'] = date(2200, 1, 1)
incident_dates.to_sql("Incident_datesDIM", engine, if_exists="append", index=False)

# 🟢 **2. Disaster_DIM**
disaster_dim = disasters.drop(columns=['id','incident_begin_date', 'incident_end_date'])
disaster_dim['version'] = 1
disaster_dim['date_from'] = date(1900, 1, 1)
disaster_dim['date_to'] = date(2200, 1, 1)
disaster_dim.to_sql("Disaster_DIM", engine, if_exists="append", index=False)

# 🟢 **3. State_DIM**
state_dim = states[['name', 'fips']]
state_dim['version'] = 1
state_dim['date_from'] = date(1900, 1, 1)
state_dim['date_to'] = date(2200, 1, 1)
state_dim.to_sql("State_DIM", engine, if_exists="append", index=False)

# 🟢 **4. FEMA_DIM**
fema_dim = declarations[['fema_declaration_string', 'place_code', 'last_refresh']].drop_duplicates()
fema_dim['version'] = 1
fema_dim['date_from'] = date(1900, 1, 1)
fema_dim['date_to'] = date(2200, 1, 1)
fema_dim.to_sql("FEMA_DIM", engine, if_exists="append", index=False)

# 🟢 **5. Declaration_DIM**
declaration_dim = declarations.drop(columns=['fema_declaration_string', 'place_code', 'last_refresh'])
declaration_dim['version'] = 1
declaration_dim['date_from'] = date(1900, 1, 1)
declaration_dim['date_to'] = date(2200, 1, 1)
declaration_dim.to_sql("Declaration_DIM", engine, if_exists="append", index=False)

print("ETL proces završen i podaci su spremljeni u dimenzijske tablice!")
