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
        
        TVAC = SETTING['CORE']['Tanium']['PROJECT']['VUL']['AUTO_CREATE']
    if type == 'create' :
        
        success = True
        process = 0
        
        createConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        createCur = createConn.cursor()
        
        while success :
            if process == 0 :
                CTQ = """ DROP TABLE vulnerability_list;"""
            if process == 1 :
                CTQ =    """DROP SEQUENCE seq_vulnerability_list_num;"""
            if process == 2 :
                CTQ = """ CREATE SEQUENCE seq_vulnerability_list_num
                            INCREMENT BY 1
                            MINVALUE 1
                            MAXVALUE 9223372036854775807
                            START 1
                            CACHE 1
                            NO CYCLE;"""
            if process == 3 :
                CTQ = """CREATE TABLE vulnerability_list (
                        vulnerability_num int4 NOT NULL DEFAULT nextval('seq_vulnerability_list_num'::regclass),
                        vulnerability_classification varchar(50) NOT NULL,
                        vulnerability_code varchar(50) NOT NULL,
                        vulnerability_item varchar(300) NOT NULL,
                        vulnerability_explanation text NOT NULL,
                        vulnerability_standard_good text NOT NULL,
                        vulnerability_standard_weak text NOT NULL,
                        vulnerability_create_date timestamp NOT NULL,
                        CONSTRAINT vulnerability_list_pk PRIMARY KEY (vulnerability_num),
                        CONSTRAINT vulnerability_list_un UNIQUE (vulnerability_code)
                        );"""
            try :
                createCur.execute(CTQ)
                print("{} 이 성공적으로 들어갔습니다.".format(process))
                createConn.commit()
                process = process + 1
                if process == 4 :
                    print(process)
                    success = False
                    logging.info('Table Create Successed')
            except Exception as e :
                print("==========================")
                print(e)
                print("==========================")
                break
        createConn.close()
        
    elif type == 'insert' :
        try :
            if cycle == 'question' :
                TNM = VQ
                insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            elif cycle == 'VUL' :
                TNM = VJ
                insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            insertConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
            insertCur = insertConn.cursor()
            
            if cycle == 'question':
                IQ = """ INSERT INTO 
                    """ + TNM + """ (
                        vulnerability_classification,
                        vulnerability_code,
                        vulnerability_item,
                        vulnerability_explanation,
                        vulnerability_standard_good,
                        vulnerability_standard_weak,
                        vulnerability_create_date
                        ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                        );"""
            elif cycle == 'VUL' :
                if VUL_STS == 'None' :
                    IQ = """ INSERT INTO 
                    """ + TNM + """ (computer_id,
                                        vulnerability_code,
                                        vulnerability_judge_result,
                                        vulnerability_judge_update_time,
                                        vulnerability_judge_reason,
                                        computer_name,
                                        chassis_type,
                                        tanium_client_nat_ip_address,
                                        last_reboot,
                                        operating_system,
                                        classification_cid,
                                        online) 
                        VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                ) 
                        ON CONFLICT (classification_cid)
                        DO UPDATE SET 
                                computer_id = excluded.computer_id ,
                                vulnerability_code = excluded.vulnerability_code,
                                vulnerability_judge_result = excluded.vulnerability_judge_result,
                                vulnerability_judge_update_time = excluded.vulnerability_judge_update_time,
                                vulnerability_judge_reason = excluded.vulnerability_judge_reason,
                                computer_name = excluded.computer_name,
                                chassis_type = excluded.chassis_type,
                                tanium_client_nat_ip_address = excluded.tanium_client_nat_ip_address,
                                last_reboot = excluded.last_reboot,
                                operating_system = excluded.operating_system,
                                classification_cid = excluded.classification_cid,
                                online = excluded.online;"""
                    datalen = len(data.computer_id)
                elif VUL_STS == "online" :
                    insertCur.execute('TRUNCATE TABLE ' + TNM + ';')
                    insertCur.execute('ALTER SEQUENCE seq_vulnerability_judge_num RESTART WITH 1;')
                    
                    IQ = """ INSERT INTO 
                    """ + TNM + """ (computer_id,
                                        vulnerability_code,
                                        vulnerability_judge_result,
                                        vulnerability_judge_update_time,
                                        vulnerability_judge_reason,
                                        computer_name,
                                        chassis_type,
                                        tanium_client_nat_ip_address,
                                        last_reboot,
                                        operating_system,
                                        classification_cid,
                                        online) 
                        VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                );"""
                elif VUL_STS == "Offline" :
                    return None

            if cycle == 'question' :
                datalen = len(data.vulnerability_code)
            else :
                datalen = len(data.computer_id)

            if PROGRESS == 'true' :
                DATA_list = tqdm(range(datalen),
                                total=datalen,
                                desc='OP_DB_VOG_{}'.format(cycle))
            else :
                DATA_list = range(datalen)
            try :
                for i in DATA_list:
                    if cycle == 'question' :
                        VCL = data.vulnerability_classification[i]
                        VC = data.vulnerability_code[i]
                        VI = data.vulnerability_item[i]
                        VE = data.vulnerability_explanation[i]
                        VSG = data.vulnerability_standard_good[i]
                        VSW = data.vulnerability_standard_weak[i]
                        VCD = data.vulnerability_create_date[i]
                        dataList = VCL, VC, VI, VE, VSG, VSW, VCD
                    if cycle == 'VUL' :
                        CI = data.computer_id[i]
                        VC = data.vulnerability_code[i]
                        VJR = data.vulnerability_judge_result[i]
                        VJUT = data.vulnerability_judge_update_time[i]
                        VJRS = data.vulnerability_judge_reason[i]
                        VJCN = data.computer_name[i]
                        VJCT = data.chassis_type[i]
                        VJIP = data.tanium_client_nat_ip_address[i]
                        VJLR = data.last_reboot[i]
                        VJOS = data.operating_system[i]
                        CCD = data.classification_cid[i]
                        ONLINE = data.online[i]
                        dataList = CI, VC, VJR, VJUT, VJRS, VJCN, VJCT, VJIP, VJLR, VJOS, CCD, ONLINE
                    else :
                        insertCur.execute(IQ, (dataList))
                    
            except Exception as e:
                if '고유 제약 조건을 위반함' in str(e) :
                    print('이미 Question이 들어가져있습니다.')
                    return 200
                else :
                    print(e)
                    print("existance")
                    return str(e)
            insertConn.commit()
            insertConn.close()
            return 200
        except ConnectionError as e:
            print(e)
