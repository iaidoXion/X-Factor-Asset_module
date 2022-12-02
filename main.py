import urllib3

from Common.Input.API.Tanium.Sesstion import plug_in as CIATSPI
from Common.Input.API.Tanium.Sensor.Common import plug_in as CIATSCPI
from Common.Input.DB.Postgresql.Tanium.Asset import plug_in as CIDBPTAPI
from Common.Transform.Dataframe import plug_in as CTDFPI
from Common.Transform.Preprocessing import plug_in as CTPPI
from Common.Output.Tanium.Asset import plug_in as COTAPI
from Common.Analysis.Statistics.GroupByCount import plug_in as CASGBCPI
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def asset_minutely() :
    sessionKey = CIATSPI()['dataList'][0]
    CSDL = CIATSCPI(sessionKey)['dataList']
    CST = CTDFPI(CSDL, 'asset', 'API')
    COTAPI(CST, 'minutely')

def asset_daily() :
    CSMDL = CIDBPTAPI('minutely', 'all')
    CST = CTDFPI(CSMDL, 'asset', 'DB')
    COTAPI(CST, 'daily')


def statistics_minutely() :
    CSDL = CIDBPTAPI('minutely', 'minutely_daily_asset')
    CSTDFF = CTDFPI(CSDL, 'statistics', 'DB')
    CSTPP = CTPPI(CSTDFF)
    CSTDFS = CTDFPI(CSTPP, 'statistics', 'DB')
    CASGBCPI(CSTDFS, 'os', 'OP')
    CASGBCPI(CSTDFS, 'virtual', 'IV')
    CASGBCPI(CSTDFS, 'asset', 'CT')
    CASGBCPI(CSTDFS, 'installed_applications', 'IANM')


if __name__ == "__main__":
    asset_minutely()
    statistics_minutely()