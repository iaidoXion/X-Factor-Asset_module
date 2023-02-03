from datetime import datetime, timedelta
from CORE.Tanium.Dashboard import minutely_plug_in as CTMPI
from CORE.Tanium.Dashboard import daily_plug_in as CTDPI
from CORE.Tanium.Vul import minutely_plug_in as CTVMPI
from Common.ETC.thread import count as count
from Common.Output.DB.Postgresql.AutoCreateTable.Query import QueryPlugIn as QPI
from Common.Output.DB.Postgresql.AutoCreateTable.AutoCreateOrg import plug_in as CODPTV
import urllib3
import logging
import json
import threading
import time
from apscheduler.schedulers.background import BlockingScheduler
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

run_main = True
MinuitTime = 0
result_list = []
def minutely() :
    if CMU == 'true':
        now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        print('\rminutely', end ="")
        print(now)
        CTMPI()
        if TVU == 'true' :
            CTVMPI('used')
        logging.info('Minutely CMU Module Succesed!!')
    else:
        logging.info('Tanium Minutely cycle 사용여부  : ' + CMU)

def daily():
    if CDU == 'true' :
        now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        print('\rdaily', end ="")
        print(now)
        CTDPI()
        logging.info('Minutely CDU Module Succesed!!')
    else:
        logging.info('Tanium Daily cycle 사용여부  : ' + CDU)
        
def vul() :
    if TVU == 'true' :
        now = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        print('\rVUL', end ="")
        print(now)
        CTVMPI('')
        logging.info('Minutely VUL Module Succesed!!')
    else:
        logging.info('Tanium VUL cycle 사용여부  : ' + TVU)
    
def main():
    if CUSTOMER == "X-FACTOR" :
        if TU == 'true':
            if AUTOCREATEUSE == 'true' : #테이블 자동생성
                for i in AUTOCREATE.values() :
                    if i['USE'] == 'TRUE' :
                        query = QPI(i['NAME'])
                        CODPTV(query, i['NAME'], 'create')
                logging.info('AutoCreate Success!!!')
                
            if CMU == 'true' :
                CTMPI()
                print('Tanium Minutely Module 성공')
                logging.info('Tanium Minutely Module 성공')
            else:
                logging.info('Tanium Minutely cycle 사용여부  : ' + CMU)
            
            if TVU == 'true' :
                CTVMPI('first')
                print('Tanium VUL Module 성공')
                logging.info('Tanium VUL Module 성공')
            else:
                logging.info('Tanium VUL cycle 사용여부  : ' + CDU)
            
            if PTD == "true" :
                if CDU == 'true' :
                    CTDPI()
                    print('Tanium Daily Module 성공')
                    logging.info('Tanium Daily Module 성공')
                else:
                    logging.info('Tanium Daily cycle 사용여부  : ' + CDU)
            
            print("스케쥴링을 시작하겠습니다.")
            
            for i in reversed(range(3)) :
                print("...........{}".format(i + 1), end="\r")
                time.sleep(1) 
                
            thread.start()
            sched = BlockingScheduler(timezone='Asia/Seoul')
            sched.add_job(minutely, 'interval', seconds=CMT)  # seconds='3'
            sched.add_job(daily, 'cron', hour=CDTH, minute=CDTM)
            sched.add_job(vul, 'interval', seconds=CMT)  # seconds='3'
            logging.info('Start the Scheduling~')
            sched.start()
        else:
            logging.info('Tanium 사용여부 : '+TU)

if __name__ == "__main__":
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    CUSTOMER = SETTING['PROJECT']['CUSTOMER']
    
    AUTOCREATE = SETTING['PROJECT']['AUTOCREATE']['TABLE']
    AUTOCREATEUSE = SETTING['PROJECT']['AUTOCREATE']['USE'].lower()
    LOGFD = SETTING['PROJECT']['LOG']['directory']
    LOGFNM = SETTING['PROJECT']['LOG']['fileName']
    LOGFF = SETTING['PROJECT']['LOG']['fileFormat']
    PTD = SETTING['PROJECT']['TEST']['DAILY'].lower()
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
    logging.info('Module Finished')


