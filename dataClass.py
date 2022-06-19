
from datetime import datetime
from sqlalchemy import  Column, Integer, String, Date, Float
from SQLConnection import Base
class datadump(Base):
    __tablename__ = "datadump"
    _id = Column(Integer,primary_key=True,unique=True)
    Date = Column(Date)
    PHU_ID = Column(Integer)
    PHU_name = Column(String(225))
    Agegroup = Column(String(255))
    atLeastOneGrp = Column(String(255))
    secondDoseCumulative = Column(Integer)
    totalPopulation = Column(Integer)
    percentAtLeastOneDose = Column(Float)
    percentrFullyVaccinated = Column(Float)
class datadumpcaserate(Base):
    __tablename__ = "datadumpcaserate"
    _id = Column(Integer,primary_key=True,unique=True)
    date = Column(Date)
    agegroup =  Column(String(255))
    casesunvacrateper100K = Column(Float)
    casespartialvacrateper100K = Column(Float)
    casesfullvacrateper100K  = Column(Float)

class datadumphos(Base):
    __tablename__ = "datadumphos"
    _id = Column(Integer,primary_key=True,unique=True)
    date = Column(Date)
    icuUnvac =  Column(Integer)
    icuPartialVac = Column(Integer)
    icuFullVac = Column(Integer)
    hospitalnonicuUnvac  = Column(Integer)
    hospitalnonicuPartialVac = Column(Integer)
    hospitalnonicuFullVac = Column(Integer)


    
