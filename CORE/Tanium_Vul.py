
import urllib3
import json
from Common.Input.API.Tanium.Sesstion import plug_in as CIATSPI
from Common.Input.API.Tanium.Sensor.Common import plug_in as CIATSCPI
from Common.Transform.Dataframe.Asset.All import plug_in as CTDAAPI
from Common.Transform.Preprocessing import plug_in as CTPPI
from Common.Transform.Preprocessing import plug_in as VUL_TDFPI
from Common.Output.DB.Postgresql.Tanium.VulOrg import plug_in as CODBPTAOPI

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def minutely_plug_in(use):                                                                     # 변수 명 Full Name : Full Name에서 대문자로 명시한 것들을 뽑아서 사용 (괄호 안의 내용은 설명)
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    VUMIPAPIU = SETTING['CORE']['Tanium']['SOURCE']['MINUTELY']['INPUT']['API'].lower()            # (Source Data MINUTELY Input plug in API 사용 여부 설정)
    VUMTPIU = SETTING['CORE']['Tanium']['SOURCE']['MINUTELY']['Transform'].lower()            # (Source Data MINUTELY Transform(preprocessing) plug in 사용 여부 설정)
    SOMOPIDBPU = SETTING['CORE']['Tanium']['SOURCE']['MINUTELY']['OUTPUT']['DB']['PS'].lower()      # (Source Data MINUTELY Output plug in postgresql DB 사용 여부 설정)
    STCU = SETTING['CORE']['Tanium']['STATISTICS']['COLLECTIONUSE'].lower()                      # (통계 Data 수집 여부 설정)
    STMIPIDBPU = SETTING['CORE']['Tanium']['STATISTICS']['MINUTELY']['INPUT']['DB']['PS'].lower()   # (통계 Data MINUTELY Input plug in postgresql DB 사용 여부 설정)
    STMTPIU = SETTING['CORE']['Tanium']['STATISTICS']['MINUTELY']['Transform'].lower()              # (통계 Data MINUTELY Transform(preprocessing) plug in 사용 여부 설정)
    STMOPODBPU = SETTING['CORE']['Tanium']['STATISTICS']['MINUTELY']['OUTPUT']['DB']['PS'].lower()  # (통계 Data MINUTELY Output plug in postgresql DB 사용 여부 설정)
    
    VQPT = SETTING['CORE']['Tanium']['PROJECT']['VUL']['PATH']
    VQFN = SETTING['CORE']['Tanium']['PROJECT']['VUL']['VUL_FILE_NAME']
    
    if use == 'first' :
        PATH = VQPT + VQFN
        VDF = VUL_TDFPI(PATH, 'question')
        VQIDB = CODBPTAOPI(VDF, 'question')
        if VQIDB == 200 :
            print('취약점 모듈 시작')
        else :
            print(VQIDB)
            print("에러로 인한 모듈종료")
            return ''
            
    if VUMIPAPIU == 'true':     
        SK = CIATSPI()['dataList'][0]  
        VDIPDL = CIATSCPI(SK, 'VUL')['dataList']
        
    VUDDFT = CTDAAPI(VDIPDL, 'API', 'VUL') 
    # if VUMTPIU == 'true' :
    VOPPT = CTPPI(VUDDFT, 'VUL')
    CODBPTAOPI(VOPPT, 'VUL')






