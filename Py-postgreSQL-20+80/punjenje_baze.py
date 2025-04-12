from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

# Kreiranje SparkSession-a
spark = SparkSession.builder.appName("US_Disasters").getOrCreate()

# Kreiranje konekcije na PostgreSQL bazu
DATABASE_URL = "postgresql://postgres:1234@localhost:5432/us_disasters2"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Definicija tablica
class State(Base):
    __tablename__ = 'State'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    fips = Column(String, nullable=False)

class Disaster(Base):
    __tablename__ = 'Disaster'
    id = Column(Integer, primary_key=True, autoincrement=True)
    disaster_number = Column(Integer, unique=True, nullable=False)
    incident_type = Column(String, nullable=False)
    incident_begin_date = Column(Date, nullable=False)
    incident_end_date = Column(Date, nullable=False)
    ih_program_declared = Column(Boolean, nullable=False)
    ia_program_declared = Column(Boolean, nullable=False)
    pa_program_declared = Column(Boolean, nullable=False)
    hm_program_declared = Column(Boolean, nullable=False)
    deaths = Column(Integer, nullable=False)

class Declaration(Base):
    __tablename__ = 'Declaration'
    id = Column(Integer, primary_key=True, autoincrement=True)
    declaration_title = Column(String, nullable=False)
    declaration_type = Column(String, nullable=False)
    declaration_date = Column(Date, nullable=False)
    designated_area = Column(String, nullable=False)
    declaration_request_number = Column(Integer, nullable=False)
    fema_declaration_string = Column(String, nullable=False)
    place_code = Column(Integer, nullable=False)
    last_refresh = Column(Date, nullable=False)
    disaster_fk = Column(Integer, ForeignKey('Disaster.id'), nullable=False)

class DisasteredStates(Base):
    __tablename__ = 'Disastered_states'
    id = Column(Integer, primary_key=True, autoincrement=True)
    disaster_fk = Column(Integer, ForeignKey('Disaster.id'), nullable=False)
    state_fk = Column(Integer, ForeignKey('State.id'), nullable=False)

# Kreiranje tablica
Base.metadata.create_all(engine)

# Funkcija za učitavanje CSV-a i spremanje u bazu pomoću Spark-a
def load_csv_to_db(csv_path):
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

        # Punjenje tablice State (provjera duplikata)
    states = df.select("state", "fips").distinct().collect()
    for row in states:
        existing_state = session.query(State).filter_by(fips=str(row.fips)).first()
        if not existing_state:
            session.add(State(name=row.state, fips=row.fips))
    session.commit()

    # Punjenje tablice Disaster
    disasters = df.select("disaster_number", "incident_type", "incident_begin_date", "incident_end_date", "ih_program_declared", "ia_program_declared", "pa_program_declared", "hm_program_declared", "deaths").distinct().collect()
    for row in disasters:
        existing_disaster = session.query(Disaster).filter_by(disaster_number=row.disaster_number).first()
        if not existing_disaster:
            disaster = Disaster(
                disaster_number=row.disaster_number,
                incident_type=row.incident_type,
                incident_begin_date=row.incident_begin_date,
                incident_end_date=row.incident_end_date,
                ih_program_declared=row.ih_program_declared,
                ia_program_declared=row.ia_program_declared,
                pa_program_declared=row.pa_program_declared,
                hm_program_declared=row.hm_program_declared,
                deaths=row.deaths
            )
            session.add(disaster)
    session.commit()

    # Punjenje tablice Declaration
    declarations = df.select("declaration_title", "declaration_type", "declaration_date", "designated_area", "declaration_request_number", "fema_declaration_string", "place_code", "last_refresh", "disaster_number").distinct().collect()
    for row in declarations:
        disaster = session.query(Disaster).filter_by(disaster_number=row.disaster_number).first()
        if disaster:
            declaration = Declaration(
                declaration_title=row.declaration_title,
                declaration_type=row.declaration_type,
                declaration_date=row.declaration_date,
                designated_area=row.designated_area,
                declaration_request_number=row.declaration_request_number,
                fema_declaration_string=row.fema_declaration_string,
                place_code=row.place_code,
                last_refresh=row.last_refresh,
                disaster_fk=disaster.id
            )
            session.add(declaration)
    session.commit()

    # Punjenje tablice DisasteredStates
    disastered_states = df.select("disaster_number", "fips").distinct().collect()
    for row in disastered_states:
        disaster = session.query(Disaster).filter_by(disaster_number=row.disaster_number).first()
        state = session.query(State).filter_by(fips=str(row.fips)).first()
        if disaster and state:
            ds_entry = DisasteredStates(disaster_fk=disaster.id, state_fk=state.id)
            session.add(ds_entry)
    session.commit()
    print("Podaci uspješno spremljeni u bazu!")

# Poziv funkcije
load_csv_to_db("data/US_DISASTERS_PROCESSED_80.csv")