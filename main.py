import urllib3

from Common.Input.API.Tanium.Sesstion import plug_in as CIATSPI
from Common.Input.API.Tanium.Sensor.Common import plug_in as CIATSCPI
from Common.Input.DB.Postgresql.Tanium.Asset import plug_in as CIDBPTAPI
from Common.Transform.Dataframe import plug_in as CTDFPI
from Common.Transform.Preprocessing import plug_in as CTPPI
from Common.Output.Tanium.Asset import plug_in as COTAPI

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def asset_minutely() :
    sessionKey = CIATSPI()['dataList'][0]
    commonSensorMDL = CIATSCPI(sessionKey)['dataList']
    commonSensorMT = CTDFPI(commonSensorMDL, 'API')
    COTAPI(commonSensorMT, 'minutely')

def asset_daily() :
    commonSensorDDL = CIDBPTAPI('today', 'all')
    commonSensorDT = CTDFPI(commonSensorDDL, 'asset', 'DB')
    COTAPI(commonSensorDT, 'daily')


def statistics_minutely() :
    #chassis_type = CIDBPTAPI('today', 'chassis_type')
    #os_platform = CIDBPTAPI('today', 'os_platform')
    commonSensorDDL = CIDBPTAPI('today', 'statistics')
    commonSensorDT = CTDFPI(commonSensorDDL, 'statistics', 'DB')
    dd = CTPPI(commonSensorDT)
    print(CTDFPI(dd, 'statistics', 'DB'))


if __name__ == "__main__":
    statistics_minutely()