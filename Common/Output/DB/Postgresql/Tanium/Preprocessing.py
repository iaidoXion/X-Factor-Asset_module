from datetime import datetime, timedelta
import psycopg2
import json
with open("setting.json", encoding="UTF-8") as f:
    SETTING = json.loads(f.read())
def plug_in(data, cycle) :
    try :
        DBHost = 'localhost'
        DBPort = '5432'
        DBName = 'postgres'
        DBUser = 'aidoi'
        DBPwd = 'iaido8906#'
        if cycle == 'minutely' :
            TNM = 'minutely_preprocessing_asset'
            insertDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        insertConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHost, DBPort, DBName, DBUser, DBPwd))
        insertCur = insertConn.cursor()
        if cycle == 'minutely':
            IQ = """
                INSERT INTO """ + TNM + """ (
                    computer_id, last_reboot, disk_total_space, disk_used_space, os_platform, operating_system, is_virtual, chassis_type, ipv_address, 
                    listen_port_count, established_port_count, ram_use_size, ram_total_size, 
                    installed_applications_name, running_service, cup_consumption, collection_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '""" + insertDate + """'
                )
                ON CONFLICT 
                    (computer_id)
                DO UPDATE SET
                    last_reboot = excluded.last_reboot, 
                    disk_total_space = excluded.disk_total_space, 
                    disk_used_space = excluded.disk_used_space, 
                    os_platform = excluded.os_platform,
                    operating_system = excluded.operating_system,
                    is_virtual = excluded.is_virtual, 
                    chassis_type = excluded.chassis_type, 
                    ipv_address = excluded.ipv_address, 
                    listen_port_count = excluded.listen_port_count, 
                    established_port_count = excluded.established_port_count, 
                    ram_use_size = excluded.ram_use_size, 
                    ram_total_size = excluded.ram_total_size, 
                    installed_applications_name = excluded.installed_applications_name, 
                    running_service = excluded.running_service, 
                    cup_consumption = excluded.cup_consumption, 
                    collection_date = '""" + insertDate + """'
            """
        datalen = len(data.computer_id)
        for i in range(datalen):
            CI = data.computer_id[i]
            LR = data.last_reboot[i]
            DTS = str(data.disk_total_space[i][0])
            DUS = str(data.disk_used_space[i][0])
            OP = data.os_platform[i]
            OS = data.operating_system[i]
            IV = data.is_virtual[i]
            CT = data.chassis_type[i]
            IP = data.ipv_address[i]
            LPC = data.listen_port_count[i]
            EPC = data.established_port_count[i]
            RUS = str(data.ram_use_size[i])
            RTS = str(data.ram_total_size[i])
            IA = data.installed_applications_name[i]
            RS = data.running_service[i]
            CPUC = data.cup_consumption[i]
            dataList = CI, LR, DTS, DUS, OP, OS, IV, CT, IP, LPC, EPC, RUS, RTS, IA, RS, CPUC
            insertCur.execute(IQ, (dataList))
        insertConn.commit()
        insertConn.close()
    except ConnectionError as e:
        print(e)
