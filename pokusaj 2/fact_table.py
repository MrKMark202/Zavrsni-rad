import sys
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, MetaData, Column, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from model import StateDim, DisasterDim, FEMA, IncidentDatesDim, DeclarationDim  # Tvoje ORM klase

# Osiguraj da konzola podržava UTF-8 kodiranje
sys.stdout.reconfigure(encoding='utf-8')

# Kreiranje Spark sesije
spark = SparkSession.builder.appName("US_Disasters").getOrCreate()

# Definiraj engine za vezu s PostgreSQL bazom
engine = create_engine('postgresql://postgres:1234@localhost:5432/us_disasters2')
metadata = MetaData(schema="public")
metadata.reflect(bind=engine)

# Konfiguracija za SQLAlchemy
DATABASE_URL = "postgresql://postgres:1234@localhost:5432/us_disasters2"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Definiraj base klasu
Base = declarative_base(metadata=metadata)


# Definicija tablice DisasterFact
class DisasterFact(Base):
    __tablename__ = 'disaster_fact'
    __table_args__ = {'schema': 'public'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_tk = Column(Integer, ForeignKey('public.state_dim.state_tk'), nullable=False)
    disaster_tk = Column(Integer, ForeignKey('public.disaster_dim.disaster_tk'), nullable=False)
    declaration_tk = Column(Integer, ForeignKey('public.declaration_dim.declaration_tk'), nullable=False)
    fema_tk = Column(Integer, ForeignKey('public.fema_dim.fema_tk'), nullable=False)
    incident_dates_tk = Column(Integer, ForeignKey('public.incident_datesdim.incident_dates_tk'), nullable=False)

# Kreiranje tablica
Base.metadata.create_all(engine)

# Kreiraj sesiju
Session = sessionmaker(bind=engine)
session = Session()

# Funkcija za učitavanje podataka u tablicu činjenica
def load_fact_table_from_existing_dimensions(csv_path_1, csv_path_2):
    # Učitaj oba CSV-a s automatskim prepoznavanjem sheme
    df1 = spark.read.csv(csv_path_1, header=True, inferSchema=True)
    df2 = spark.read.csv(csv_path_2, header=True, inferSchema=True)

    # Kombiniraj oba DataFrame-a
    df = df1.union(df2)

    # Provjera za ispravne datume i ključne stupce
    df = df.filter(
        (df["incident_begin_date"].isNotNull()) & 
        (df["incident_end_date"].isNotNull()) & 
        (df["fips"].isNotNull()) & 
        (df["disaster_number"].isNotNull()) & 
        (df["declaration_title"].isNotNull())
    )

    # Prikaz rezultata nakon filtriranja
    print("Prvi rezultati filtriranih podataka:")
    df.show(10)

    # Prebacivanje Spark DataFrame u Pandas za lakše iteriranje
    pandas_df = df.toPandas()

    # Učitavanje podataka i pretrage
    for index, row in pandas_df.iterrows():
        state = session.query(StateDim).filter_by(fips=str(row['fips'])).first()
        disaster = session.query(DisasterDim).filter_by(disaster_number=row['disaster_number']).first()
        declaration = session.query(DeclarationDim).filter_by(
            declaration_title=row['declaration_title'],
            declaration_type=row['declaration_type'],
            declaration_date=row['declaration_date'],
            designated_area=row['designated_area'],
            declaration_request_number=row['declaration_request_number']
        ).first()
        incident = session.query(IncidentDatesDim).filter_by(
            incident_begin_date=row['incident_begin_date'],
            incident_end_date=row['incident_end_date']
        ).first()
        fema = session.query(FEMA).filter_by(
            fema_declaration_string=row['fema_declaration_string'],
            place_code=row['place_code'],
            last_refresh=row['last_refresh']
        ).first()

        # Ako su svi strani ključevi pronađeni, dodaj podatke u tablicu činjenica
        if all([state, disaster, declaration, incident, fema]):
            fact_row = DisasterFact(
                state_tk=state.state_tk,
                disaster_tk=disaster.disaster_tk,
                declaration_tk=declaration.declaration_tk,
                incident_dates_tk=incident.incident_dates_tk,
                fema_tk=fema.fema_tk
            )
            session.add(fact_row)

    # Pohrani promjene u bazu
    session.commit()
    print("Tablica činjenica uspješno napunjena!")

# Pokreni funkciju
load_fact_table_from_existing_dimensions("data/US_DISASTERS_PROCESSED_80.csv", "data/US_DISASTERS_PROCESSED_20.csv")