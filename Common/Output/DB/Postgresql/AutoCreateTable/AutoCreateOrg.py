from datetime import datetime, timedelta
import psycopg2
import json
from tqdm import tqdm
import logging
def plug_in(data, cycle, type) :
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['PWD']
        VQ = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['VQ']
        VJ = SETTING['CORE']['Tanium']['OUTPUT']['DB']['PS']['TNM']['VJ']
        VUL_STS = SETTING['CORE']['Tanium']['ONOFFTYPE']
        PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()
    if type == 'create' :
        success = True
        process = 0
        createConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        createCur = createConn.cursor()
        processList = ["DROP TABLE {}".format(cycle), "DROP SEQ {}".format(cycle), "CREATE SEQ {}".format(cycle), "CREATE TABLE {}".format(cycle)]
        try :
            logging.info('Auto Create Table Start')
            while success :
                if process == 0 :
                    CTQ = data[0]
                if process == 1 :
                    CTQ = data[1]
                if process == 2 :
                    CTQ = data[2]
                if process == 3:
                    CTQ = data[3]
                try :
                    createCur.execute(CTQ)
                    print("{} 이 성공했습니다.".format(processList[process]))
                    logging.info('{} is Successed!!  ({}//{})'.format(processList[process], process + 1, len(processList)))
                    createConn.commit()
                    process = process + 1
                    if process == 4 :
                        success = False
                        logging.info('{} Table Create Successed!!'.format(cycle))
                except Exception as e :
                    if '테이블 없음' in str(e) or 'table "{}" does not exist'.format(cycle) in str(e):
                        logging.warning('Error : {}'.format(str(e).strip()))
                        print("==========================")
                        print(str(e).strip())
                        print("==========================")
                        createConn.rollback()
                        process = 2
                        logging.info('RESTART {} '.format(processList[process]))
                    elif '시퀀스 없음' in str(e) :
                        createConn.rollback()
                        process = 2
                    else :
                        print(str(e))
                        logging.warning('RESTART is Failed : {}'.format(str(e).strip()))
                        break
                    print("{} 테이블 다시 생성".format(cycle))
                    print("==========================")
            createConn.close()
            print("==================={} 테이블이 만들어졌습니다 ===================".format(cycle))
            logging.info('{} Table Create Successed!!'.format(cycle))
        except Exception as e :
            logging.info('ENTRY ERROR : {} '.format(str(e)))
            print(e)
        
    