from datetime import datetime, timedelta
from CORE.Tanium.Dashboard import minutely_plug_in as CTMPI
from CORE.Tanium.Dashboard import daily_plug_in as CTDPI
from CORE.Tanium.Vul import minutely_plug_in as CTVMPI
import urllib3
import logging
import json
import threading
import time
from apscheduler.schedulers.background import BlockingScheduler
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

run_main = True
MinuitTime = 0

CMU_status = False
CDU_status = False
TVU_status = False
def minutely() :
    if CMU == 'true':
        now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        print('\rminutely', end ="")
        print(now)
        CTMPI()
        if TVU == 'true' :
            CTVMPI('used')
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
    process = []
    if TU == 'true':
        install = True
        
        while install :
            answer = input('모듈을 처음 사용하십니까? (Y/N)')
            if answer.lower() == 'y' :
                used = True
                install = False
            elif answer.lower() == 'n' :
                used = False
                install = False
            else :
                print('Y(y) or N(n) 만 눌러주세요')
        
        if used :
            if CMU == 'true' :
                CTMPI()
                print('Tanium Minutely Module 성공')
                logging.info('Tanium Minutely Module 성공')
            else:
                logging.info('Tanium Minutely cycle 사용여부  : ' + CMU)

            if CDU == 'true' :
                CTDPI()
                print('Tanium Daily Module 성공')
                logging.info('Tanium Daily Module 성공')
            else:
                logging.info('Tanium Daily cycle 사용여부  : ' + CDU)
                
            if TVU == 'true' :
                CTVMPI(used)
                print('Tanium VUL Module 성공')
                logging.info('Tanium VUL Module 성공')
            else:
                logging.info('Tanium VUL cycle 사용여부  : ' + CDU)
        print("스케쥴링을 시작하겠습니다.")
        
        for i in reversed(range(3)) :
            print("...........{}".format(i + 1), end="\r")
            time.sleep(1) 
        thread.start()
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
    TVU = SETTING['CORE']['Tanium']['PROJECT']['VUL']['USE'].lower()

    today = datetime.today().strftime("%Y%m%d")
    logFile = LOGFD + LOGFNM + today + LOGFF
    logFormat = '%(levelname)s, %(asctime)s, %(message)s'
    logDateFormat = '%Y%m%d%H%M%S'
    logging.basicConfig(filename=logFile, format=logFormat, datefmt=logDateFormat, level=logging.DEBUG)
    logging.info('Module Started')
    thread = threading.Thread(target=count)
    thread.daemon = True
    
    main()
    run_main = False
    logging.info('Module Finished')


