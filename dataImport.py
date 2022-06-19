import datetime
import urllib
from urllib.request import urlopen
import pandas as pd
import json
import requests
from SQLConnection import CreateEngine,Base
from dataClass import datadump, datadumpcaserate , datadumphos
from sqlalchemy import desc
def latestData(Session,engine):
    session = Session()
    maxDate_dataDump = session.query(datadump).order_by(desc('Date')).first()
    delta = datetime.timedelta(days=1)
    if maxDate_dataDump is None:
        start_date_datadump = datetime.date(2021,7,26)
    else:
        start_date_datadump = maxDate_dataDump.Date + delta
    end_date = datetime.date.today()
    dataFrame = pd.DataFrame()
    while start_date_datadump < end_date:
        urlTempDataDump = f'https://data.ontario.ca/api/3/action/datastore_search?resource_id=2a362139-b782-43b1-b3cb-078a2ef19524&q={{"Date":"{start_date_datadump}"}}&limit=500';
        fileobj = requests.get(urlTempDataDump)
        dict1 = fileobj.json()
        tempDataDump = pd.DataFrame(dict1["result"]["records"])
        if dataFrame.shape[0] == 0:
            dataFrame = tempDataDump
        else:
            dataFrame = dataFrame.append(tempDataDump,ignore_index=False)
        start_date_datadump+=delta
    if dataFrame.shape[0] > 0:
        dataFrame["Date"] = pd.to_datetime(dataFrame["Date"], format="%Y-%m-%dT%H:%M:%S")
        dataFrame = dataFrame.drop_duplicates(subset=["_id"])
        dataFrame = dataFrame.rename(
                columns={'_id': '_id', 'Date': 'Date', 'PHU ID': "PHU_ID", 'PHU name': 'PHU_name', 'Agegroup': 'Agegroup',
                         'At least one dose_cumulative': 'atLeastOneGrp', 'Second_dose_cumulative': 'secondDoseCumulative',
                         'Total population': 'totalPopulation', 'Percent_at_least_one_dose': 'percentAtLeastOneDose',
                         'Percent_fully_vaccinated': 'percentrFullyVaccinated', 'rank Date': 'rankDate'})
        column_name = ["_id","Date","PHU_ID","PHU_name","Agegroup","atLeastOneGrp","secondDoseCumulative","totalPopulation","percentAtLeastOneDose","percentrFullyVaccinated"]
        dataFrame =  dataFrame[column_name]
        dataFrame.to_sql('datadump', engine, if_exists="append", index=False)

    maxDate_dataDump_datadumpcaserate = session.query(datadumpcaserate).order_by(desc('date')).first()
    if maxDate_dataDump_datadumpcaserate is None:
        start_date_datadump_datadumpcaserate = datetime.date(2021, 7, 26)
    else:
        start_date_datadump_datadumpcaserate = maxDate_dataDump_datadumpcaserate.date + delta
    end_date__datadump_datadumpcaserate = datetime.date.today()
    dataFrame = pd.DataFrame()
    while start_date_datadump_datadumpcaserate < end_date__datadump_datadumpcaserate:
        urlTempDataDump_datadumpcaserate = f'https://data.ontario.ca/en/api/3/action/datastore_search?resource_id=c08620e0-a055-4d35-8cec-875a459642c3&limit=500&q={{"date":"{start_date_datadump_datadumpcaserate}"}}';
        fileobj = requests.get(urlTempDataDump_datadumpcaserate)
        dict1 = fileobj.json()
        tempDataDump = pd.DataFrame(dict1["result"]["records"])
        if dataFrame.shape[0] == 0:
            dataFrame = tempDataDump
        else:
            dataFrame = dataFrame.append(tempDataDump, ignore_index=False)
        start_date_datadump_datadumpcaserate += delta
    if dataFrame.shape[0] > 0:
        dataFrame["date"] = pd.to_datetime(dataFrame["date"], format="%Y-%m-%dT%H:%M:%S")
        dataFrame = dataFrame.drop_duplicates(subset=["_id"])
        dataFrame = dataFrame.rename(
            columns={'_id': '_id', 'date': 'Date', 'agegroup':'agegroup', 'cases_unvac_rate_per100K':'casesunvacrateper100K',
   'cases_partial_vac_rate_per100K':'casespartialvacrateper100K', 'cases_full_vac_rate_per100K':'casesfullvacrateper100K', 'rank Date': 'rankDate'})
        column_name = ["_id", "Date", "agegroup", "casesunvacrateper100K", "casespartialvacrateper100K", "casesfullvacrateper100K"]
        dataFrame = dataFrame[column_name]
        dataFrame.to_sql('datadumpcaserate', engine, if_exists="append", index=False)




    maxDate_dataDump_datadumphos =  session.query(datadumphos).order_by(desc('date')).first()
    if maxDate_dataDump_datadumphos is None:
        start_date_datadump_datadumphos = datetime.date(2021, 7, 26)
    else:
        start_date_datadump_datadumphos = maxDate_dataDump_datadumphos.date + delta
    end_date__datadump_datadumphos = datetime.date.today()
    dataFrame = pd.DataFrame()
    while start_date_datadump_datadumphos < end_date__datadump_datadumphos:
        urlTempDataDump_datadumphos = f'https://data.ontario.ca/en/api/3/action/datastore_search?resource_id=274b819c-5d69-4539-a4db-f2950794138c&limit=500&q={{"date":"{start_date_datadump_datadumphos}"}}';
        fileobj = requests.get(urlTempDataDump_datadumphos)
        dict1 = fileobj.json()
        tempDataDump = pd.DataFrame(dict1["result"]["records"])
        if dataFrame.shape[0] == 0:
            dataFrame = tempDataDump
        else:
            dataFrame = dataFrame.append(tempDataDump, ignore_index=False)
        start_date_datadump_datadumphos += delta
    if dataFrame.shape[0] > 0:
        dataFrame["date"] = pd.to_datetime(dataFrame["date"], format="%Y-%m-%dT%H:%M:%S")
        dataFrame = dataFrame.drop_duplicates(subset=["_id"])
        dataFrame = dataFrame.rename(
            columns={'_id': '_id', 'date': 'Date', 'icu_unvac':'icuUnvac', 'icu_partial_vac':'icuPartialVac', 'icu_full_vac':'icuFullVac',
       'hospitalnonicu_unvac':'hospitalnonicuUnvac', 'hospitalnonicu_partial_vac':'hospitalnonicuPartialVac',
       'hospitalnonicu_full_vac':'hospitalnonicuFullVac'})
        column_name = ['_id', 'Date', 'icuUnvac', 'icuPartialVac', 'icuFullVac', 'hospitalnonicuUnvac', 'hospitalnonicuPartialVac','hospitalnonicuFullVac']
        dataFrame = dataFrame[column_name]
        dataFrame.to_sql('datadumphos', engine, if_exists="append", index=False)






