from datetime import datetime, timedelta
from CORE.Tanium import minutely_plug_in as CTMPI
from CORE.Tanium import daily_plug_in as CTDPI
import urllib3
import logging
import json
import threading
import time
from apscheduler.schedulers.background import BlockingScheduler
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

run_main = True
MinuitTime = 0
def minutely() :
    if CMU == 'true':
        now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        print('\rminutely', end ="")
        print(now)
        CTMPI()
    else:
        logging.info('Tanium Minutely cycle 사용여부  : ' + CMU)

def daily():
    if CDU == 'true' :
        now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        print('daily')
        print(now)
        CTDPI()
    else:
        logging.info('Tanium Daily cycle 사용여부  : ' + CDU)
        
def count() :
    count = 0
    running = '\\'
    while run_main :
        if count == 0 :
            running = '\\'
        elif count == 1 :
            running = '|'
        elif count == 2 :
            running = '/'
        elif count == 3 :
            running = 'ㅡ'
        elif count == 4 :
            running = '|'
        print('Module is running....{}'.format(running), end='\r')
        time.sleep(0.5)
        count = count +1
        if count == 4 :
            count = 0
    
def main():
    if TU == 'true':
        sched = BlockingScheduler(timezone='Asia/Seoul')
        sched.add_job(minutely, 'interval', seconds=CMT)  # seconds='3'
        sched.add_job(daily, 'cron', hour=CDTH, minute=CDTM)
        sched.start()
    else:
        logging.info('Tanium 사용여부 : '+TU)

if __name__ == "__main__":
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    LOGFD = SETTING['PROJECT']['LOG']['directory']
    LOGFNM = SETTING['PROJECT']['LOG']['fileName']
    LOGFF = SETTING['PROJECT']['LOG']['fileFormat']
    TU = SETTING['CORE']['Tanium']['COREUSE'].lower()
    CMU = SETTING['CORE']['Tanium']['CYCLE']['MINUTELY']['USE'].lower()
    CMT = SETTING['CORE']['Tanium']['CYCLE']['MINUTELY']['TIME']
    CDU = SETTING['CORE']['Tanium']['CYCLE']['DAILY']['USE'].lower()
    CDTH = SETTING['CORE']['Tanium']['CYCLE']['DAILY']['TIME']['hour']
    CDTM = SETTING['CORE']['Tanium']['CYCLE']['DAILY']['TIME']['minute']

    today = datetime.today().strftime("%Y%m%d")
    logFile = LOGFD + LOGFNM + today + LOGFF
    logFormat = '%(levelname)s, %(asctime)s, %(message)s'
    logDateFormat = '%Y%m%d%H%M%S'
    logging.basicConfig(filename=logFile, format=logFormat, datefmt=logDateFormat, level=logging.DEBUG)
    logging.info('Module Started')
    thread = threading.Thread(target=count)
    thread.daemon = True
    thread.start()
    
    main()
    run_main = False
    logging.info('Module Finished')


