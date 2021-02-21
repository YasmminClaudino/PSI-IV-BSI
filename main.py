#code only to test api response
#
#


import json
import boto3
from datetime import datetime
import json
import requests


data = []
dateHour = str(datetime.now())

def getDate(date):
    return date[:10]


def getHour(hour):
    hh = hour[11:13]
    mm = "00"
    return hh + mm


def getResponse(date, hour):
    response = requests.get(
        f'https://apitempo.inmet.gov.br/estacao/dados/{date}/{hour}')
    return response


def isResponseValid(response):
    if response.status_code == 200:
        return True
    else:
        return print("Response is not valid " + str(response.status_code))


def parseJson(response):
    parseJsonData = response.json()
    return filter(lambda x: x["UF"] == "PE", parseJsonData)


def createDic(filterData):
    for element in filterData:
        dir = {
            'DC_NOME': element['DC_NOME'],
            'TEMP': element['UMID_MED'],
            'UMD': element['UMID_MED']
        }
        data.append(dir)

date = getDate(dateHour)
hour = str(getHour(dateHour))
response = getResponse(date, hour)

if (isResponseValid(response)):
    filterData = parseJson(response)
    createDic(filterData)
    print(data)


