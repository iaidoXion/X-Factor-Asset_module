import requests
import json
with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())

APIURL = SETTING['PLUGIN']['Tanium']['INPUT']['API']['URL']
CSP = SETTING['PLUGIN']['Tanium']['INPUT']['API']['PATH']['Sensor']
CSID = SETTING['PLUGIN']['Tanium']['INPUT']['API']['SensorID']['COMMON']

def plug_in(sessionKey) :
    CSH = {'session': sessionKey}
    CSU = APIURL + CSP + CSID
    CSR = requests.post(CSU, headers=CSH, verify=False)
    CSRC = CSR.status_code
    CSRT = CSR.content.decode('utf-8')
    CSRJ = json.loads(CSRT)
    CSRJD = CSRJ['data']
    dataList = []
    for d in CSRJD['result_sets'][0]['rows']:
        DL = []
        for i in d['data'] :
            DL.append(i)
        dataList.append(DL)
    RD = {'resCode': CSRC, 'dataList': dataList}
    return RD