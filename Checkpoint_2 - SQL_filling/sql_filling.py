# Imports
import pandas as pd
import numpy as np
import requests
import random
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Boolean
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

CSV_FILE_PATH = "Stari.py/US_DISASTERS_PROCESSED_80.csv" # Putanja do predprocesiranog skupa podataka
df = pd.read_csv(CSV_FILE_PATH, delimiter=',') # Učitavanje predprocesiranog skupa podataka
print("CSV size: ", df.shape) # Ispis broja redaka i stupaca
print(df.head()) # Ispis prvih redaka dataframe-a


Base = declarative_base() # Stvaranje baze

# Definiranje sheme baze
#-----------------------------------------------------------------------------------------------------
# Tablica State
class State(Base):
    __tablename__ = 'state'
    id = Column(Integer, primary_key=True)
    name = Column(String(45), nullable=False)
    fips = Column(String(20), unique=True)
        
# Tablica Disaster
class Disaster(Base):
    __tablename__ = 'disaster'
    id = Column(Integer, primary_key=True)
    disaster_number = Column(Integer, nullable=False)
    incident_type = Column(String(45), nullable=False)
    incident_begin_date = Column(Date, nullable=False)
    incident_end_date = Column(Date, nullable=False)
    ih_program_declared = Column(Boolean, nullable=False)
    ia_program_declared = Column(Boolean, nullable=False)
    pa_program_declared = Column(Boolean, nullable=False)
    hm_program_declared = Column(Boolean, nullable=False)
    deaths = Column(Integer, nullable=False)


# Tablica Declaration
class Declaration(Base):
    __tablename__ = 'declaration'
    id = Column(Integer, primary_key=True)
    declaration_title = Column(String(100), nullable=False)
    declaration_type = Column(String(45), nullable=False)
    declaration_date = Column(Date, nullable=False)
    designated_area = Column(String(100), nullable=False)
    declaration_request_number = Column(String(45), nullable=False)
    fema_declaration_string = Column(String(45), nullable=False)
    place_code = Column(String(45), nullable=False)
    last_refresh = Column(Date, nullable=False)
    disaster_fk = Column(Integer, ForeignKey('disaster.id')) # Strani ključ za disaster
    
class DisasteredStates(Base):
    __tablename__ = 'disastered_states'
    id = Column(Integer, primary_key=True)
    disaster_fk = Column(Integer, ForeignKey('disaster.id')) # Strani ključ za disaster
    state_fk = Column(Integer, ForeignKey('state.id')) # Strani ključ za state




engine = create_engine('mysql+pymysql://root:root@localhost:3306/US_DISASTERS_FROM_1953' , echo=False) # Stvaranje konekcije na bazu
Base.metadata.drop_all(engine) # Brisanje tablica ako već postoje, kako bi se izbjegli problemi s dupliciranjem podataka
Base.metadata.create_all(engine) # Stvaranje tablica

Session = sessionmaker(bind=engine) # Stvaranje sesije
session = Session() # Stvaranje instance sesije


# Popunjavanje tablica
# popunjavanje je specifično za svaki skup podataka, u pravilu nećete morati generirati svoje podatke već ćete koristiti podatke iz skupa
# -------------------------------------------------------------------------------------------------------
'''
state_names = df['state'].unique().tolist()
for i, name in enumerate(state_names):
    state = State(name=name)
    session.add(state)
session.commit()
'''

for index, row in df.iterrows():
    existing_state = session.query(State).filter_by(fips=row['fips']).first()
    if existing_state is None:
        state = State(name=row['state'], fips=row['fips'])
        session.add(state)
session.commit()

for index, row in df.iterrows():
    deaths = int(random.normalvariate(5000, 2000))
    deaths = max(0, min(deaths, 10000))
    disaster = Disaster(
        disaster_number=row['disaster_number'], 
        incident_type=row['incident_type'], 
        incident_begin_date=row['incident_begin_date'],
        incident_end_date=row['incident_end_date'], 
        ih_program_declared=row['ih_program_declared'], 
        ia_program_declared=row['ia_program_declared'],
        pa_program_declared=row['pa_program_declared'], 
        hm_program_declared=row['hm_program_declared'],
        deaths=deaths
    )
    session.add(disaster)
session.commit()

for index, row in df.iterrows():
    disaster = session.query(Disaster).filter_by(disaster_number=row['disaster_number']).first()
    declaration = Declaration(
        declaration_title=row['declaration_title'],
        declaration_type=row['declaration_type'],
        declaration_date=row['declaration_date'],
        designated_area=row['designated_area'],
        declaration_request_number=row['declaration_request_number'],
        fema_declaration_string=row['fema_declaration_string'],
        place_code=row['place_code'],
        last_refresh=row['last_refresh'],
        disaster_fk = disaster.id,
    )
    session.add(declaration)
session.commit()

for index, row in df.iterrows():
    disaster = session.query(Disaster).filter_by(disaster_number=row['disaster_number']).first()
    state = session.query(State).filter_by(name=row['state']).first()
    disastered_states = DisasteredStates(
        disaster_fk = disaster.id,
        state_fk = state.id
    )
    session.add(disastered_states)
session.commit()


session.close()

# print("COMPLETE!")