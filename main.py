from datetime import datetime
from CORE.Tanium import minutely_plug_in as CTMPI
from CORE.Tanium import daily_plug_in as CTDPI
import urllib3
import logging
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main() :
    if TU == 'true' :
        if CMU == 'true' :
            CTMPI()
        else:
            logging.info('Tanium Minutely cycle 사용여부  : ' + CMU)

        if CDU == 'true' :
            CTDPI()
        else:
            logging.info('Tanium Daily cycle 사용여부  : ' + CDU)
    else:
        logging.info('Tanium 사용여부 : '+TU)

if __name__ == "__main__":

    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    LOGFD = SETTING['PROJECT']['LOG']['directory']
    LOGFNM = SETTING['PROJECT']['LOG']['fileName']
    LOGFF = SETTING['PROJECT']['LOG']['fileFormat']
    TU = SETTING['CORE']['Tanium']['COREUSE']
    CMU = SETTING['CORE']['Tanium']['CYCLEUSE']['MINUTELY']
    CDU = SETTING['CORE']['Tanium']['CYCLEUSE']['DAILY']

    today = datetime.today().strftime("%Y%m%d")
    logFile = LOGFD + LOGFNM + today + LOGFF
    logFormat = '%(levelname)s, %(asctime)s, %(message)s'
    logDateFormat = '%Y%m%d%H%M%S'
    logging.basicConfig(filename=logFile, format=logFormat, datefmt=logDateFormat, level=logging.DEBUG)
    logging.info('Module Started')
    main()
    logging.info('Module Finished')