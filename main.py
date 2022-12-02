import urllib3

from Common.Input.API.Tanium.Sesstion import plug_in as CIATSPI
from Common.Input.API.Tanium.Sensor.Common import plug_in as CIATSCPI
from Common.Input.DB.Postgresql.Tanium.Asset import plug_in as CIDBPTAPI
from Common.Transform.Dataframe.Asset.All import plug_in as CTDAALLPI
from Common.Transform.Dataframe.Asset.Part import plug_in as CTDAPARTPI
from Common.Transform.Preprocessing import plug_in as CTPPI
from Common.Output.Tanium.Asset import plug_in as COTAPI
from Common.Analysis.Statistics.GroupByCount import plug_in as CASGBCPI
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def asset_minutely() :
    sessionKey = CIATSPI()['dataList'][0]
    CSDL = CIATSCPI(sessionKey)['dataList']
    CST = CTDAALLPI(CSDL, 'API')
    COTAPI(CST, 'minutely')

def asset_daily() :
    CSMDL = CIDBPTAPI('minutely', 'all')
    CST = CTDAALLPI(CSMDL, 'DB')
    COTAPI(CST, 'daily')


def statistics_minutely() :
    CSDL = CIDBPTAPI('minutely', 'minutely_daily_asset')
    CSTDFF = CTDAPARTPI(CSDL, 'DB')
    CSTPP = CTPPI(CSTDFF)
    CSTDFS = CTDAPARTPI(CSTPP, 'DB')
    CASGBCPI(CSTDFS, 'os', 'OP')
    CASGBCPI(CSTDFS, 'virtual', 'IV')
    CASGBCPI(CSTDFS, 'asset', 'CT')
    CASGBCPI(CSTDFS, 'installed_applications', 'IANM')


if __name__ == "__main__":
    asset_minutely()
    statistics_minutely()