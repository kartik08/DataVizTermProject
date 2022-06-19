import datetime
import urllib
from urllib.request import urlopen
import pandas as pd
import json
import requests
from SQLConnection import CreateEngine,Base, CreateSession
from dataClass import datadump
from dataImport import  latestData
import pygsheets

if __name__ == "__main__":
    engine = CreateEngine()
    Base.metadata.create_all(engine)
    Session = CreateSession(engine)
    dataImport = latestData(Session,engine)
    df = pd.read_sql_table('datadump', engine)
    df = df[~df['Agegroup'].isin(['Adults_18plus', 'Ontario_12plus', 'Undisclosed_or_missing'])]
    client = pygsheets.authorize(service_file='D:/Data Analytics/Semester 2/Data visulization/termproject-334004-3dff780c03bb.json')
    sheet = client.open('TermProject')
    wks = sheet.worksheet_by_title("DataDump")
    wks.clear(fields = '*')
    wks.set_dataframe(df, (1, 1))

    df = pd.read_sql_table('datadumpcaserate', engine)
    wks = sheet.worksheet_by_title("CaseRate")
    wks.clear(fields='*')
    wks.set_dataframe(df, (1, 1))

    df = pd.read_sql_table('datadumphos', engine)
    wks = sheet.worksheet_by_title("Hospital")
    wks.clear(fields='*')
    wks.set_dataframe(df, (1, 1))


