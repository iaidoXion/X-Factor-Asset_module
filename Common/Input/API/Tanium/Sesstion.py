import requests
import json

with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
APIURL = SETTING['CORE']['Tanium']['INPUT']['API']['URL']
SKP = SETTING['CORE']['Tanium']['INPUT']['API']['PATH']['SesstionKey']
APIUNM = SETTING['CORE']['Tanium']['INPUT']['API']['username']
APIPWD = SETTING['CORE']['Tanium']['INPUT']['API']['password']

def plug_in() :
    SKH = '{"username": "'+APIUNM+'", "domain": "", "password": "'+APIPWD+'"}'
    SKURL = APIURL + SKP
    SKR = requests.post(SKURL, data=SKH, verify=False)
    SKRC = SKR.status_code
    SKRT = SKR.content.decode('utf-8')
    SKRJ = json.loads(SKRT)
    SK = SKRJ['data']['session']
    dataList = [SK]
    RD = {'resCode': SKRC, 'dataList': dataList}
    return RD