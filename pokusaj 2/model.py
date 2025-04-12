from sqlalchemy import Column, Integer, String, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StateDim(Base):
    __tablename__ = 'state_dim'
    state_tk = Column(Integer, primary_key=True)
    fips = Column(String, unique=True)
    name = Column(String)

class DisasterDim(Base):
    __tablename__ = 'disaster_dim'
    __table_args__ = {'schema': 'public'}
    disaster_tk = Column(Integer, primary_key=True)
    disaster_number = Column(Integer, unique=True)
    incident_type = Column(String)
    ih_program_declared = Column(Boolean)
    ia_program_declared = Column(Boolean)
    pa_program_declared = Column(Boolean)
    hm_program_declared = Column(Boolean)
    deaths = Column(Integer)
    

class FEMA(Base):
    __tablename__ = 'fema_dim'
    fema_tk = Column(Integer, primary_key=True)
    fema_declaration_string = Column(String)
    place_code = Column(String)
    last_refresh = Column(Date)

class IncidentDatesDim(Base):
    __tablename__ = 'incident_datesdim'
    incident_dates_tk = Column(Integer, primary_key=True)
    incident_begin_date = Column(Date)
    incident_end_date = Column(Date)

class DeclarationDim(Base):
    __tablename__ = 'declaration_dim'
    declaration_tk = Column(Integer, primary_key=True)
    declaration_title = Column(String)
    declaration_type = Column(String)
    declaration_date = Column(Date)
    designated_area = Column(String)
    declaration_request_number = Column(String)