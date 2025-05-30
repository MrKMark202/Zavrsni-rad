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
    country_name = Column(String, nullable=False)

class Disaster(Base):
    __tablename__ = 'Disaster'
    id = Column(Integer, primary_key=True, autoincrement=True)
    disaster_number = Column(Integer, nullable=False)
    incident_type = Column(String, nullable=False)
    incident_begin_date = Column(Date, nullable=False)
    incident_end_date = Column(Date, nullable=False)
    incident_duration = Column(Integer, nullable=False)
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
    declaration_request_number = Column(Integer, nullable=False)
    disaster_fk = Column(Integer, ForeignKey('Disaster.id'), nullable=False)
    state_fk = Column(Integer, ForeignKey('State.id'), nullable=False)

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
    states = df.select("state", "country_name").distinct().collect()
    for row in states:
        existing_state = session.query(State).filter_by(country_name=str(row.country_name)).first()
        if not existing_state:
            session.add(State(name=row.state, country_name=row.country_name))
    session.commit()

    # Punjenje tablice Disaster
    disasters = df.select("disaster_number", "incident_type", "incident_begin_date", "incident_end_date", "incident_duration","ih_program_declared", "ia_program_declared", "pa_program_declared", "hm_program_declared", "deaths").distinct().collect()
    for row in disasters:
        existing_disaster = session.query(Disaster).filter_by(disaster_number=row.disaster_number).first()
        if not existing_disaster:
            disaster = Disaster(
                disaster_number=row.disaster_number,
                incident_type=row.incident_type,
                incident_begin_date=row.incident_begin_date,
                incident_end_date=row.incident_end_date,
                incident_duration=row.incident_duration,
                ih_program_declared=row.ih_program_declared,
                ia_program_declared=row.ia_program_declared,
                pa_program_declared=row.pa_program_declared,
                hm_program_declared=row.hm_program_declared,
                deaths=row.deaths
            )
            session.add(disaster)
    session.commit()

    # Punjenje tablice Declaration
    declarations = df.select("declaration_title", "declaration_type", "declaration_date", "declaration_request_number", "country_name", "incident_type").distinct().collect()
    for row in declarations:
        disaster = session.query(Disaster).filter_by(incident_type=row.incident_type).first()
        state = session.query(State).filter_by(country_name=row.country_name).first()
        if disaster:
            declaration = Declaration(
                declaration_title=row.declaration_title,
                declaration_type=row.declaration_type,
                declaration_date=row.declaration_date,
                declaration_request_number=row.declaration_request_number,
                disaster_fk=disaster.id,
                state_fk=state.id
            )
            session.add(declaration)
    session.commit()

    # Punjenje tablice DisasteredStates
    disastered_states = df.select("disaster_number", "country_name").distinct().collect()
    for row in disastered_states:
        disaster = session.query(Disaster).filter_by(disaster_number=row.disaster_number).first()
        state = session.query(State).filter_by(country_name=str(row.country_name)).first()
        if disaster and state:
            ds_entry = DisasteredStates(disaster_fk=disaster.id, state_fk=state.id)
            session.add(ds_entry)
    session.commit()
    print("Podaci uspješno spremljeni u bazu!")

# Poziv funkcije
load_csv_to_db("data/US_DISASTERS_PROCESSED_80.csv")