import urllib3
import json
from Common.Input.API.Tanium.Sesstion import plug_in as CIATSPI
from Common.Input.API.Tanium.Sensor.Common import plug_in as CIATSCPI
from Common.Transform.Dataframe.Asset.All import plug_in as CTDAAPI
from Common.Transform.Preprocessing import plug_in as CTPPI
from Common.Transform.Preprocessing import plug_in as VUL_TDFPI
from Common.Output.DB.Postgresql.Tanium.VulOrg import plug_in as CODBPTAOPI
from Common.Input.DB.Postgresql.Vul_List_material.material import vul_list_pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def minutely_plug_in(use):                                                                     # 변수 명 Full Name : Full Name에서 대문자로 명시한 것들을 뽑아서 사용 (괄호 안의 내용은 설명)
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    VUMIPAPIU = SETTING['CORE']['Tanium']['SOURCE']['MINUTELY']['INPUT']['API'].lower()            # (Source Data MINUTELY Input plug in API 사용 여부 설정)
    
    # CREATE TABLE
    CTVACU = SETTING['CORE']['Tanium']['PROJECT']['VUL']['AUTO_CREATE']['USE']
    CTVACCC = SETTING['CORE']['Tanium']['PROJECT']['VUL']['AUTO_CREATE']['INGREDIENTS']['COLUM']
    
    if use :
        VQIDB = CODBPTAOPI('', 'question', 'create')
        
        # VDF = VUL_TDFPI(vul_list_pd(), 'question')
        
        # VQIDB = CODBPTAOPI(VDF, 'question', 'insert')
        
    if VUMIPAPIU == 'true':     
        SK = CIATSPI()['dataList'][0]  
        VDIPDL = CIATSCPI(SK, 'VUL')['dataList']
        
    VUDDFT = CTDAAPI(VDIPDL, 'API', 'VUL') 
    # if VUMTPIU == 'true' :
    VOPPT = CTPPI(VUDDFT, 'VUL')
    CODBPTAOPI(VOPPT, 'VUL')






