from sqlalchemy import  create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()
username = "root"
password = "Kartik1998"
host = "localhost"
port = "3306"
database = "TermProject"

def CreateEngine():
    engineUrl = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")
    if not database_exists(engineUrl.url):
        create_database(engineUrl.url)
    return engineUrl
def CreateSession(engine):
    session = sessionmaker(bind=engine)
    return  session

