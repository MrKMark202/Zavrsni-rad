from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker

# ✅ NOVI način definiranja baze
Base = declarative_base()

class IncidentDatesDIM(Base):
    __tablename__ = "Incident_datesDIM"
    incident_tk = Column(Integer, primary_key=True, autoincrement=True)
    incident_begin_date = Column(Date)
    incident_end_date = Column(Date)
    version = Column(Integer, default=1)
    date_from = Column(Date)
    date_to = Column(Date)

class DisasterDIM(Base):
    __tablename__ = "Disaster_DIM"
    disaster_tk = Column(Integer, primary_key=True, autoincrement=True)
    disaster_number = Column(Integer)  
    incident_type = Column(String)
    ih_program_declared = Column(String)
    ia_program_declared = Column(String)
    pa_program_declared = Column(String)
    hm_program_declared = Column(String)
    deaths = Column(Integer)
    version = Column(Integer, default=1)
    date_from = Column(Date)
    date_to = Column(Date)

class StateDIM(Base):
    __tablename__ = "State_DIM"
    state_tk = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    fips = Column(String, unique=False)  
    version = Column(Integer, default=1)
    date_from = Column(Date)
    date_to = Column(Date)

class FEMADIM(Base):
    __tablename__ = "FEMA_DIM"
    fema_tk = Column(Integer, primary_key=True, autoincrement=True)
    fema_declaration_string = Column(String)
    place_code = Column(Integer)
    last_refresh = Column(Date)
    version = Column(Integer, default=1)
    date_from = Column(Date)
    date_to = Column(Date)

class DeclarationDIM(Base):
    __tablename__ = "Declaration_DIM"
    declaration_tk = Column(Integer, primary_key=True, autoincrement=True)
    disaster_number = Column(Integer)
    declaration_title = Column(String)
    declaration_type = Column(String)
    declaration_date = Column(Date)
    designated_area = Column(String)
    declaration_request_number = Column(Integer)
    version = Column(Integer, default=1)
    date_from = Column(Date)
    date_to = Column(Date)

# ✅ 1. Povezivanje na bazu (promijeni podatke prema tvojoj konfiguraciji)
DATABASE_URL = "postgresql://postgres:1234@localhost:5432/us_disasters2"
engine = create_engine(DATABASE_URL)

# ✅ 2. Kreiranje tablica ako ne postoje
Base.metadata.create_all(engine)

# ✅ 3. Kreiranje SQLAlchemy sesije
Session = sessionmaker(bind=engine)
session = Session()

print("Baza povezana i tablice su kreirane!")
