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


def minutely_asset():                                                   # 변수 명 Full Name : Full Name에서 대문자로 명시한 것들을 뽑아서 사용 (괄호 안의 내용은 설명)

    SK = CIATSPI()['dataList'][0]                                       # Sesstion Key (Tanium Sesstion Key 호출)
    IPDL = CIATSCPI(SK)['dataList']                                     # InPut Data List (Tanium API Sensor Data 호출)
    DFT = CTDAAPI(IPDL, 'API')                                          # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
    CODBPTAOPI(DFT, 'minutely')                                         # (minutely_asset Table에 수집)


def daily_asset():
    # 만약에 minutely 수집이 true면
    CSMDL = CIDBPTAOPI('minutely_asset_all')                            # Common Sensor Minutely Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset Table )
    DFT = CTDAAPI(CSMDL, 'DB')                                          # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
    CODBPTAOPI(DFT, 'daily')                                            # (daily_asset Table에 수집)
    # 만약에 minutely 수집이 false

def minutely_statistics_list():
    # 만약에 minutely 수집이 true면
    IPDL = CIDBPTAOPI('minutely_daily_asset')                           # InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset, daily_asset Table)

    DFTF = CTDAPPI(IPDL, 'DB', 'minutely_daily_asset')                  # Data Frame Transform First (호출한 데이터를 Data Frame 형태로 변형)

    PPT = CTPPI(DFTF, 'minutely_daily_asset')                           # PreProcession Transform (데이터 전처리)
    DFTS = CTDAPPI(PPT, 'DB', 'minutely_daily_asset')                   # Data Frame Transform Second (전처리한 데이터를 Data Frame 형태로 변형)

    US = CASUPI(DFTS)                                                   # Usage Statistics (사용량 통계)
    USDFT = CTDSPPI(US, 'DB', 'minutely_statistics_list', 'usage')      # Usage Statistics DataFrame Transform (사용량 통계를 Data Frame 형태로 변형)
    CS = CASCPI(DFTS)                                                   # Compare Statistics (비교 통계)
    CSDFT = CTDSPPI(CS, 'DB', 'minutely_statistics_list', 'compare')    # Compare Statistics DataFrame Transform (비교 통계를 Data Frame 형태로 변형)
    UCSM = CTMPI(USDFT, CSDFT)                                          # Usage and Compare Statistics Merge (DataFrame 형태의 사용량 통계 & 비교 통계 병합)
    CODBPTSLPI(UCSM, 'minutely')                                        # (minutely_statistics_list Table에 수집)


def daily_statistics_list():
    # 만약에 minutely 수집이 true면
    IPDL = CIDBPTSLPI('minutely')
    DFT = CTDSAPI(IPDL, 'DB', 'minutely_statistics_list')
    CODBPTSLPI(DFT, 'daily')
    # 만약에 minutely 수집이 false

def minutely_statistics():
    # 만약에 minutely 수집이 true면
    IPMALSDL = CIDBPTSLPI('minutely')                                   # InPut Minutely Asset List Statistics Data List (Module로 DB에 수집한 데이터 호출 : minutely_statistics_list Table)
    DFT = CTDSAPI(IPMALSDL, 'DB', 'minutely_statistics_list')           # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
    OSGBS = CASGBCPI(DFT, 'os', 'OP')                                   # OS Group By Statistics (OS 통계)
    IVGBS = CASGBCPI(DFT, 'virtual', 'IV')                              # Is Virtual Group By Statistics (가상, 물리 자산 통계)
    CTGBS = CASGBCPI(DFT, 'asset', 'CT')                                # Chassis Type Group By Statistics (자산 형태 통계)
    LPCGBS = CASGBCPI(DFT, 'listen_port_count_change', 'LPC')           # Listen Port Count Group By Statistics (listen port count 변경 여부 통계)
    EPCGBS = CASGBCPI(DFT, 'established_port_count_change', 'EPC')      # Listen Port Count Group By Statistics (established port count 변경 여부 통계)

    IPMADL = CIDBPTAOPI('minutely_asset_part')                          # InPut Minutely Asset Data List (Module로 DB에 수집한 데이터 호출 : minutely_asset Table)
    DFTF = CTDAPPI(IPMADL, 'DB', 'minutely_asset')                      # Data Frame Transform First (호출한 데이터를 Data Frame 형태로 변형)
    PPT = CTPPI(DFTF, 'minutely_asset')                                 # PreProcession Transform (데이터 전처리)
    DFTS = CTDAPPI(PPT, 'DB', 'minutely_asset')                         # Data Frame Transform Second (전처리한 데이터를 Data Frame 형태로 변형)
    IAGBS = CASGBCPI(DFTS, 'installed_applications', 'IANM')            # Installed Applications Group By Statistics (Installed Application 통계)
    RPGBS = CASGBCPI(DFTS, 'running_processes', 'RPNM')                 # Running Processes Group By Statistics (Running Processes 통계)

    MSTD = OSGBS + IVGBS + CTGBS + LPCGBS + EPCGBS + IAGBS + RPGBS      # Minutely Statistics Total Data (minutely_statistics Table에 넣을 모든 통계데이터)
    SDDFT = CTDSAPI(MSTD, 'DB', 'minutely_statistics')                  # Statistics Data Data Frame Transform (Statistics 데이터를 Data Frame 형태로 변형)
    CODBPTAPI(SDDFT, 'minutely')                                        # (minutely_statistics Table에 수집)


def daily_statistics():
    # 만약에 minutely 수집이 true면
    IPDL = CIDBPTSPI('minutely')                                        # InPut Data List (Module로 DB에 수집한 데이터 호출 : minutely_statistics Table)
    DFT = CTDSPPI(IPDL, 'DB', 'minutely_statistics', '')                # Data Frame Transform (호출한 데이터를 Data Frame 형태로 변형)
    CODBPTAPI(DFT, 'daily')                                             # (daily_statistics Table에 수집)
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


def plug_in():
    # 만약에 minutely 수집이 true면
    minutely_asset()
    daily_asset()
    minutely_statistics_list()
    daily_statistics_list()
    minutely_statistics()
    daily_statistics()