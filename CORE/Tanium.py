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

    SOIPAPIU = SETTING['CORE']['Tanium']['SOURCE']['INPUT']['API']
    SOTPU = SETTING['CORE']['Tanium']['SOURCE']['Transform']
    SOOPDBPU = SETTING['CORE']['Tanium']['SOURCE']['OUTPUT']['DB']['PS']
    STCU = SETTING['CORE']['Tanium']['STATISTICS']['COLLECTIONUSE']
    STIPDBPU = SETTING['CORE']['Tanium']['STATISTICS']['INPUT']['DB']['PS']
    STOPDBPU = SETTING['CORE']['Tanium']['STATISTICS']['OUTPUT']['DB']['PS']

    # SOURCE
    if SOIPAPIU == 'true' :
        SK = CIATSPI()['dataList'][0]                                       # Sesstion Key (Tanium Sesstion Key 호출)
        SDIPDL = CIATSCPI(SK)['dataList']                                     # Source Data InPut Data List (Tanium API Sensor Data 호출)
    SODDFT = CTDAAPI(SDIPDL, 'API')                                          # Source Data Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
    if SOTPU == 'true' :
        SOPPT = CTPPI(SODDFT, 'minutely_asset_all')
        SOODL = CTDAAPI(SOPPT, 'DB')
    else :
        SOODL = SODDFT

    if SOOPDBPU == 'true' :
        CODBPTAOPI(SOODL, 'minutely')                                       # (minutely_asset Table에 수집)
        CSMDL = CIDBPTAOPI('minutely_asset_all')  # Common Sensor Minutely Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset Table )
        MSDDFT = CTDAAPI(CSMDL, 'DB')  # minutely Source Data Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
        #CODBPTAOPI(MSDDFT, 'daily')  # (daily_asset Table에 수집)



    if STCU == 'true' :
        # Asset List
        if STIPDBPU == 'true' :
            MDSDDIPDL = CIDBPTAOPI('minutely_daily_asset')  # Minutely Daily Source Data InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset, daily_asset Table)

        MDSDDFTF = CTDAPPI(MDSDDIPDL, 'DB', 'minutely_daily_asset')  # Minutely Daily Source Data Data Frame Transform First (호출한 데이터를 Data Frame 형태로 변형)

        MDSDDPPT = CTPPI(MDSDDFTF, 'minutely_daily_asset')  # Minutely Daily Source Data PreProcession Transform (데이터 전처리)
        MDSDDFTS = CTDAPPI(MDSDDPPT, 'DB', 'minutely_daily_asset')  # Minutely Daily Source Data Data Frame Transform Second (전처리한 데이터를 Data Frame 형태로 변형)

        US = CASUPI(MDSDDFTS)  # Usage Statistics (사용량 통계)
        USDFT = CTDSPPI(US, 'DB', 'minutely_statistics_list', 'usage')  # Usage Statistics DataFrame Transform (사용량 통계를 Data Frame 형태로 변형)
        CS = CASCPI(MDSDDFTS, '')  # Compare Statistics (비교 통계)
        CSDFT = CTDSPPI(CS, 'DB', 'minutely_statistics_list', 'compare')  # Compare Statistics DataFrame Transform (비교 통계를 Data Frame 형태로 변형)
        UCSM = CTMPI(USDFT, CSDFT)  # Usage and Compare Statistics Merge (DataFrame 형태의 사용량 통계 & 비교 통계 병합)
        if STOPDBPU == 'true' :
            CODBPTSLPI(UCSM, 'minutely')  # (minutely_statistics_list Table에 수집)

            MSLDIPDL = CIDBPTSLPI('minutely')                           # Minutely Statistics List Data InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_statistics_list Table)
            MSLDDFT = CTDSAPI(MSLDIPDL, 'DB', 'minutely_statistics_list')   # Minutely Statistics List Data Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
            #CODBPTSLPI(MSLDDFT, 'daily')                                       # (daily_statistics_list Table에 수집)

            # statistics
            IPMALSDL = CIDBPTSLPI('minutely')  # InPut Minutely Asset List Statistics Data List (Module로 DB에 수집한 데이터 호출 : minutely_statistics_list Table)
            IPMALSDDFT = CTDSAPI(IPMALSDL, 'DB', 'minutely_statistics_list')  # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
            OSGBS = CASGBCPI(IPMALSDDFT, 'os', 'OP')  # OS Group By Statistics (OS 통계)
            OSVGBS = CASGBCPI(IPMALSDDFT, 'operating_system', 'OS') # OS Version Group By Statistics (OS 버전 포함 통계)
            IVGBS = CASGBCPI(IPMALSDDFT, 'virtual', 'IV')  # Is Virtual Group By Statistics (가상, 물리 자산 통계)
            CTGBS = CASGBCPI(IPMALSDDFT, 'asset', 'CT')  # Chassis Type Group By Statistics (자산 형태 통계)
            LPCGBS = CASGBCPI(IPMALSDDFT, 'listen_port_count_change', 'LPC')  # Listen Port Count Group By Statistics (listen port count 변경 여부 통계)
            EPCGBS = CASGBCPI(IPMALSDDFT, 'established_port_count_change', 'EPC')  # Listen Port Count Group By Statistics (established port count 변경 여부 통계)
            AC = CASCPI(IPMALSDDFT, 'alarm')
            ADT = CTDSPPI(AC, 'DB', 'minutely_statistics_list', 'alarm')
            DUSGBS = CASGBCPI(ADT, 'drive_usage_size_exceeded', 'DUS')
            RUSGBS = CASGBCPI(ADT, 'ram_usage_size_exceeded', 'RUS')
            CPUGBS = CASGBCPI(ADT, 'cpu_usage_size_exceeded', 'CPU')
            LRBGBS = CASGBCPI(ADT, 'last_reboot_exceeded', 'LRB')
            GRUGBS = CASGBCPI(ADT, 'group_ram_usage_exceeded', 'ip_group')
            GCUGBS = CASGBCPI(ADT, 'group_cpu_usage_exceeded', 'ip_group')
            GLPCGBS = CASGBCPI(ADT, 'group_listen_port_count_change', 'ip_group')
            GEPCGBS = CASGBCPI(ADT, 'group_established_port_count_change', 'ip_group')
            GRSCGBS = CASGBCPI(ADT, 'group_running_service_count_exceeded', 'ip_group')
            GRPLRGBS = CASGBCPI(ADT, 'group_last_reboot', 'ip_group')
            GDUSGBS = CASGBCPI(ADT, 'group_drive_usage_size_exceeded', 'ip_group')

            #대역별 서버수량 상위5개
            GSCGBS = CASGBCPI(ADT, 'group_server_count', 'ip_group')

            #서버 총 수량 추이 그래프
            #print(IVGBS)

            MAIPDL = CIDBPTAOPI('minutely_asset_part')  # Minutely Asset InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset Table)
            MADFTF = CTDAPPI(MAIPDL, 'DB', 'minutely_asset')  # Minutely Asset Data Frame Transform First (호출한 데이터를 Data Frame 형태로 변형)
            MAPPT = CTPPI(MADFTF, 'minutely_asset')  # Minutely Asset PreProcession Transform (데이터 전처리)
            MADFTS = CTDAPPI(MAPPT, 'DB', 'minutely_asset')  # Minutely Asset Data Frame Transform Second (전처리한 데이터를 Data Frame 형태로 변형)
            IAGBS = CASGBCPI(MADFTS, 'installed_applications', 'IANM')  # Installed Applications Group By Statistics (Installed Application 통계)
            RSGBS = CASGBCPI(MADFTS, 'running_service', 'RSNM')  # Running Service Group By Statistics (Running Service 통계)


            MSTD = OSGBS + OSVGBS + IVGBS + CTGBS + LPCGBS + EPCGBS + IAGBS + RSGBS + LRBGBS + DUSGBS + RUSGBS + CPUGBS + GRUGBS + GCUGBS + GLPCGBS + GEPCGBS + GRSCGBS + GRPLRGBS + GDUSGBS + GSCGBS # Minutely Statistics Total Data (minutely_statistics Table에 넣을 모든 통계데이터)


            SDDFT = CTDSAPI(MSTD, 'DB', 'minutely_statistics')  # Statistics Data Data Frame Transform (Statistics 데이터를 Data Frame 형태로 변형)
            CODBPTAPI(SDDFT, 'minutely')


            MSIPDL = CIDBPTSPI('minutely')                                        # InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_statistics Table)
            MSDFT = CTDSPPI(MSIPDL, 'DB', 'minutely_statistics', '')                # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
            #CODBPTAPI(MSDFT, 'daily')                                             # (daily_statistics Table에 수집)
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




