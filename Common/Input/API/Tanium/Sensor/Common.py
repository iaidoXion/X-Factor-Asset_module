import requests
import json
import logging
with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())

APIURL = SETTING['CORE']['Tanium']['INPUT']['API']['URL']
CSP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['Sensor']
CSID = SETTING['CORE']['Tanium']['INPUT']['API']['SensorID']['COMMON']

def plug_in(sessionKey) :
    try:
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
        logging.info('Tanium API Sensor 호출 성공')
        logging.info('Sensor ID : ' + str(CSID))
        return RD
    except :
        logging.warning('Tanium API Sensor 호출 Error 발생')
        logging.warning('Sensor ID : '+str(CSID))