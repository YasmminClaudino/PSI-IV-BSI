import json
import base64
import requests

dir = {}
def getEvents(event):
    records = event.get('Records')
    print(records)
    for item in records:
        data = item['kinesis']['data']
        message = json.loads(base64.b64decode(data))
        location = message['DC_NOME']
        heatIndex = message['HEAT_INDEX']
        if (heatIndex != None):
            index = getIndexValue(heatIndex)
            dir[location] = str(heatIndex), index
        else:
           print("HEAT INDEX WAS NOT ABLE FOR " + location)

    print(dir)
    sendEvents(dir)

def getIndexValue(heatIndex):
    if heatIndex <= 26:
        return "Morno"
    elif heatIndex > 26 or heatIndex <= 32:
        return "Quente"
    elif heatIndex > 32 or heatIndex <= 40:
        return "Bem Quente"
    elif heatIndex > 40:
        return  "Extremamente Quente"

def sendEvents(dir):
    DEVICE_ID=""
    #Value was removed for security question
    DEVICE_TOKEN = ""
    try:
        url = f'http://52.202.101.95:8080/api/v1/{DEVICE_TOKEN}/telemetry'
        requests.post(url, data=json.dumps(dir))
        print("Data was sent to thingsboard")
    except:
        print("Could not send data to thingsboard")

def lambda_handler(event, context):
    getEvents(event)
    return {
        'statusCode': 200,
        'body': json.dumps(dir)
    }


