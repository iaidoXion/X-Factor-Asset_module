import requests
import json

def plug_in(sessionKey) :
    apiUrl = "https://1.223.168.93:49105"

    commonSensorPath = "/api/v2/result_data/saved_question/"
    commonSensorID = "2307"
    commonSensorHeaders = {'session': sessionKey}
    commonSensorUrls = apiUrl + commonSensorPath + commonSensorID
    commonSensorResponse = requests.post(commonSensorUrls, headers=commonSensorHeaders, verify=False)
    commonSensorResCode = commonSensorResponse.status_code
    commonSensorResponseText = commonSensorResponse.content.decode('utf-8')
    commonSensorResponseJson = json.loads(commonSensorResponseText)
    commonSensorResponseJsonData = commonSensorResponseJson['data']
    dataList = []
    for d in commonSensorResponseJsonData['result_sets'][0]['rows'] :
        DL = []
        for i in d['data'] :
            DL.append(i)
        dataList.append(DL)
    RD = {'resCode': commonSensorResCode, 'dataList': dataList}
    return RD