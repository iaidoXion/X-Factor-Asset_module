from datetime import datetime, timedelta
import psycopg2
import json
from tqdm import tqdm
import logging
import sys
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
        DROP = SETTING['PROJECT']['AUTOCREATE']['DROP'].lower()
    if type == 'create' :
        success = True
        process = 0
        status = 0
        createConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        createCur = createConn.cursor()
        processList = ["DROP TABLE {}".format(cycle), "DROP SEQ {}".format(cycle), "CREATE SEQ {}".format(cycle), "CREATE TABLE {}".format(cycle)]
        if DROP == 'false' :
            process = 2
        try :
            logging.info('Auto Create Table Start')
            while success :
                if process == 0 :
                    CTQ = data[process]
                if process == 1 :
                    CTQ = data[process]
                if process == 2 :
                    CTQ = data[process]
                if process == 3:
                    CTQ = data[process]
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
                        print("{} 테이블 다시 생성".format(cycle))
                        print("==========================")
                    elif '"seq_{}_num" 이름의 릴레이션(relation)이 이미 있습니다'.format(cycle) in str(e) or 'relation "seq_{}_num" already exists'.format(cycle) in str(e):
                        if DROP == 'false' :
                            if '기타 다른 개체들이 이 개체에 의존하고 있어, seq_{}_num 시퀀스 삭제할 수 없음 '.format(cycle) :
                                print(str(e).split(':')[1])
                                print("AutoCreate를 끄시거나 Drop기능을 켜주시고 다시 실행해주세요")
                                status = 400
                                break
                        
                        logging.warning('Error : {}'.format(str(e).strip()))
                        print("==========================")
                        print(str(e).strip())
                        print("==========================")
                        createConn.rollback()
                        process = 1
                        logging.info('RESTART {} '.format(processList[process]))
                        print("{} 시퀀스 다시 생성".format(cycle))
                        print("==========================")
                    else :
                        print(str(e))
                        logging.warning('RESTART is Failed : {}'.format(str(e).strip()))
                        status = 400
                        break
                    
            createConn.close()
            if status == 400 :
                print("모듈 종료")
                quit()
            else :
                print("==================={} 테이블이 만들어졌습니다 ===================".format(cycle))
                logging.info('{} Table Create Successed!!'.format(cycle))
        except Exception as e :
            logging.info('ENTRY ERROR : {} '.format(str(e)))
            print(e)
        
    