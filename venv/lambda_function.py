import json
import requests

def getResponse(dados):
    response = requests.get(
        f'https://apitempo.inmet.gov.br/estacao/dados/{dados}')
    return (response if isResponseValid(response) else "Response is not valid")

def isResponseValid(response):
    return (True if response.status_code == 200 else False)

def jsonParser(response):
    parseData = response.json()
    parseDataFilter = filter(lambda x: x["UF"] == "PE", parseData)
    print(parseDataFilter)
response = getResponse("2020-05-02")
jsonParser(response)

#print(getResponse("2022-05-02"))
dataJson = response.json()
