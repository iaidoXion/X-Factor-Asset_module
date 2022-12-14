import urllib3
import json
from Common.Input.API.Tanium.Sesstion import plug_in as CIATSPI
from Common.Input.API.Tanium.Sensor.Common import plug_in as CIATSCPI
from Common.Input.DB.Postgresql.Tanium.AssetOrg import plug_in as CIDBPTAOPI
from Common.Input.DB.Postgresql.Tanium.StatisticsList import plug_in as CIDBPTSLPI
from Common.Input.DB.Postgresql.Tanium.Statistics import plug_in as CIDBPTSPI
from Common.Transform.Dataframe.Asset.All import plug_in as CTDAAPI
from Common.Transform.Dataframe.Asset.Part import plug_in as CTDAPPI
from Common.Transform.Dataframe.Statistics.All import plug_in as CTDSAPI
from Common.Transform.Dataframe.Statistics.Part import plug_in as CTDSPPI
from Common.Transform.Preprocessing import plug_in as CTPPI
from Common.Transform.Merge import plug_in as CTMPI
from Common.Analysis.Statistics.Usage import plug_in as CASUPI
from Common.Analysis.Statistics.GroupByCount import plug_in as CASGBCPI
from Common.Analysis.Statistics.Compare import plug_in as CASCPI
from Common.Output.DB.Postgresql.Tanium.AssetOrg import plug_in as CODBPTAOPI
from Common.Output.DB.Postgresql.Tanium.StatisticsList import plug_in as CODBPTSLPI
from Common.Output.DB.Postgresql.Tanium.Statistics import plug_in as CODBPTAPI

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def plug_in():                                                   # 변수 명 Full Name : Full Name에서 대문자로 명시한 것들을 뽑아서 사용 (괄호 안의 내용은 설명)
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    TSODCU = SETTING['CORE']['Tanium']['PLUGIN']['SOURCE']['USE']

    TDCCMU = SETTING['CORE']['Tanium']['PLUGIN']['CYCLE']['MINUTELY']
    TDCCDU = SETTING['CORE']['Tanium']['PLUGIN']['CYCLE']['DAILY']

    TSODIPAU = SETTING['CORE']['Tanium']['PLUGIN']['SOURCE']['INPUT']['API']
    TSODOPDBU = SETTING['CORE']['Tanium']['PLUGIN']['SOURCE']['OUTPUT']['DB']['PS']


    TSTDCU = SETTING['CORE']['Tanium']['PLUGIN']['STATISTICS']['USE']
    TSTDIPDBPU = SETTING['CORE']['Tanium']['PLUGIN']['STATISTICS']['INPUT']['DB']['PS']
    # SOURCE
    if TSODCU == 'true' :
        if TSODIPAU == 'true' :
            print('source 수집 하고 source Input이 API이고')
            SK = CIATSPI()['dataList'][0]                                       # Sesstion Key (Tanium Sesstion Key 호출)
            SDIPDL = CIATSCPI(SK)['dataList']                                     # Source Data InPut Data List (Tanium API Sensor Data 호출)
            INPUTDATATYPE = 'API'
        else :
            print('source 수집 하고 source Input이 API가 아니고')
        SDDFT = CTDAAPI(SDIPDL, INPUTDATATYPE)                                        # Source Data Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)

        if TDCCMU == 'true':
            print('source 수집 하고 Minutely 수집 하고')
            if TSODOPDBU == 'true':
                print('source 수집 하고 Minutely 수집 하고 source Output이 DB PS이고')
                CODBPTAOPI(SDDFT, 'minutely')                                       # (minutely_asset Table에 수집)
            else :
                print('source 수집 하고 Minutely 수집 하고 source Output이 DB PS아니고')

            if TDCCDU == 'true' :
                print('source 수집 하고 Minutely 수집 하고 daily 수집 하고')
                CSMDL = CIDBPTAOPI('minutely_asset_all')  # Common Sensor Minutely Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset Table )
                MSDDFT = CTDAAPI(CSMDL, 'DB')  # minutely Source Data Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
                CODBPTAOPI(MSDDFT, 'daily')  # (daily_asset Table에 수집)
            else :
                print('source 수집 하고 Minutely 수집 하고 daily 수집 안하고')
        elif TDCCMU == 'false':
            print('source 수집 하고 Minutely 수집 안하고')
            if TDCCDU == 'true':
                print('source 수집 하고 Minutely 수집 안하고 daily 수집 하고')
                if TSODOPDBU == 'true':
                    print('source 수집 하고 Minutely 수집 안하고 daily 수집 하고 Output이 DB PS이고')
                    CODBPTAOPI(SDDFT, 'daily')
                else :
                    print('source 수집 하고 Minutely 수집 안하고 daily 수집 하고 Output이 DB PS아니고')
            else :
                print('source 수집 하고 Minutely 수집 안하고 daily 수집 안하고')
        else :
            print('Minutely 수집 true, false 이외의 값')
    else :
        print('source 수집 안하고')

    # Asset List
    if TSTDCU == 'true' :
        if TSTDIPDBPU == 'true' :
            MDSDDIPDL = CIDBPTAOPI('minutely_daily_asset')  # Minutely Daily Source Data InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset, daily_asset Table)

            MDSDDFTF = CTDAPPI(MDSDDIPDL, 'DB', 'minutely_daily_asset')  # Minutely Daily Source Data Data Frame Transform First (호출한 데이터를 Data Frame 형태로 변형)

            MDSDDPPT = CTPPI(MDSDDFTF, 'minutely_daily_asset')  # Minutely Daily Source Data PreProcession Transform (데이터 전처리)
            MDSDDFTS = CTDAPPI(MDSDDPPT, 'DB', 'minutely_daily_asset')  # Minutely Daily Source Data Data Frame Transform Second (전처리한 데이터를 Data Frame 형태로 변형)

            US = CASUPI(MDSDDFTS)  # Usage Statistics (사용량 통계)
            USDFT = CTDSPPI(US, 'DB', 'minutely_statistics_list', 'usage')  # Usage Statistics DataFrame Transform (사용량 통계를 Data Frame 형태로 변형)
            CS = CASCPI(MDSDDFTS, '')  # Compare Statistics (비교 통계)
            CSDFT = CTDSPPI(CS, 'DB', 'minutely_statistics_list', 'compare')  # Compare Statistics DataFrame Transform (비교 통계를 Data Frame 형태로 변형)
            UCSM = CTMPI(USDFT, CSDFT)  # Usage and Compare Statistics Merge (DataFrame 형태의 사용량 통계 & 비교 통계 병합)
        CODBPTSLPI(UCSM, 'minutely')  # (minutely_statistics_list Table에 수집)

        MSLDIPDL = CIDBPTSLPI('minutely')
        MSLDDFT = CTDSAPI(MSLDIPDL, 'DB', 'minutely_statistics_list')
        CODBPTSLPI(MSLDDFT, 'daily')


        # statistics
        IPMALSDL = CIDBPTSLPI('minutely')  # InPut Minutely Asset List Statistics Data List (Module로 DB에 수집한 데이터 호출 : minutely_statistics_list Table)
        IPMALSDDFT = CTDSAPI(IPMALSDL, 'DB', 'minutely_statistics_list')  # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
        OSGBS = CASGBCPI(IPMALSDDFT, 'os', 'OP')  # OS Group By Statistics (OS 통계)
        IVGBS = CASGBCPI(IPMALSDDFT, 'virtual', 'IV')  # Is Virtual Group By Statistics (가상, 물리 자산 통계)
        CTGBS = CASGBCPI(IPMALSDDFT, 'asset', 'CT')  # Chassis Type Group By Statistics (자산 형태 통계)
        LPCGBS = CASGBCPI(IPMALSDDFT, 'listen_port_count_change', 'LPC')  # Listen Port Count Group By Statistics (listen port count 변경 여부 통계)
        EPCGBS = CASGBCPI(IPMALSDDFT, 'established_port_count_change', 'EPC')  # Listen Port Count Group By Statistics (established port count 변경 여부 통계)
        AC = CASCPI(IPMALSDDFT, 'alarm')
        ADT = CTDSPPI(AC, 'DB', 'minutely_statistics_list', 'alarm')
        GRUGBS = CASGBCPI(ADT, 'group_ram_usage_exceeded', 'ip_group')
        GCUGBS = CASGBCPI(ADT, 'group_cpu_usage_exceeded', 'ip_group')
        GLPCGBS = CASGBCPI(ADT, 'group_listen_port_count_change', 'ip_group')
        GEPCGBS = CASGBCPI(ADT, 'group_established_port_count_change', 'ip_group')
        GRPCGBS = CASGBCPI(ADT, 'group_running_processes_count_exceeded', 'ip_group')
        GRPLRGBS = CASGBCPI(ADT, 'group_last_reboot', 'ip_group')

        MAIPDL = CIDBPTAOPI('minutely_asset_part')  # Minutely Asset InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset Table)
        MADFTF = CTDAPPI(MAIPDL, 'DB', 'minutely_asset')  # Minutely Asset Data Frame Transform First (호출한 데이터를 Data Frame 형태로 변형)
        MAPPT = CTPPI(MADFTF, 'minutely_asset')  # Minutely Asset PreProcession Transform (데이터 전처리)
        MADFTS = CTDAPPI(MAPPT, 'DB', 'minutely_asset')  # Minutely Asset Data Frame Transform Second (전처리한 데이터를 Data Frame 형태로 변형)
        IAGBS = CASGBCPI(MADFTS, 'installed_applications', 'IANM')  # Installed Applications Group By Statistics (Installed Application 통계)
        RPGBS = CASGBCPI(MADFTS, 'running_processes', 'RPNM')  # Running Processes Group By Statistics (Running Processes 통계)

        MSTD = OSGBS + IVGBS + CTGBS + LPCGBS + EPCGBS + IAGBS + RPGBS + GRUGBS + GCUGBS + GLPCGBS + GEPCGBS + GRPCGBS + GRPLRGBS  # Minutely Statistics Total Data (minutely_statistics Table에 넣을 모든 통계데이터)
        SDDFT = CTDSAPI(MSTD, 'DB', 'minutely_statistics')  # Statistics Data Data Frame Transform (Statistics 데이터를 Data Frame 형태로 변형)
        CODBPTAPI(SDDFT, 'minutely')


        MSIPDL = CIDBPTSPI('minutely')                                        # InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_statistics Table)
        MSDFT = CTDSPPI(MSIPDL, 'DB', 'minutely_statistics', '')                # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
        CODBPTAPI(MSDFT, 'daily')                                             # (daily_statistics Table에 수집)
        # 만약에 minutely 수집이 false

"""
def preprocessing_minutely() :
    CSDL = CIDBPTASPI('minutely', 'minutely_asset')
    CSTDFF = CTDAPARTPI(CSDL, 'DB', 'minutely_asset')
    CSTPP = CTPPI(CSTDFF, 'minutely_asset')
    CSTDFS = CTDAPARTPI(CSTPP, 'DB', 'minutely_asset')
    COTAPPI(CSTDFS, 'minutely')

def preprocessing_daily() :
    CIDBPTAPPI('minutely', 'minutely_preprocessing_all')
"""




