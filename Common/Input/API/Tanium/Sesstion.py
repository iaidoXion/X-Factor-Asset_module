import requests
import json

def plug_in() :
    apiUrl = "https://1.223.168.93:49105"

    sesstionPath = "/api/v2/session/login"
    sesstionHeaders = '{"username": "administrator", "domain": "", "password": "xion123!"}'
    sesstionUrls = apiUrl + sesstionPath
    sesstionResponse = requests.post(sesstionUrls, data=sesstionHeaders, verify=False)
    sesstionResCode = sesstionResponse.status_code
    sesstionResponseText = sesstionResponse.content.decode('utf-8')
    sesstionResponseJson = json.loads(sesstionResponseText)
    sessionKey = sesstionResponseJson['data']['session']
    dataList = [sessionKey]
    RD = {'resCode': sesstionResCode, 'dataList': dataList}

    return RD