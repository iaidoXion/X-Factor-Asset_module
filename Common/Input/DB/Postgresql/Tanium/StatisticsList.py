import psycopg2
import json
from datetime import datetime, timedelta
def plug_in(dataType) :
#def plug_in(period, dataType) :
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['PORT']
        DBNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['USER']
        DBPWD = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['PWD']
        MSLT = SETTING['CORE']['Tanium']['INPUT']['DB']['PS']['TNM']['MSL']
        nowTime = datetime.today().strftime("%Y-%m-%d %H:%M")
        DL = []
        selectConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        if dataType == 'minutely':
            SQ = """
                select 
                    computer_id,
                    computer_name,
                    ipv_address,
                    chassis_type,
                    os_platform,
                    is_virtual,
                    last_reboot,
                    driveUsage,
                    ramUsage,
                    cpuUsage,
                    listenPortCountChange,
                    establishedPortCountChange,
                    running_service_count,
                    online,
                    asset_list_statistics_collection_date
                from  
                    """ + MSLT + """
                where to_char(asset_list_statistics_collection_date , 'YYYY-MM-DD HH24:MI') = '"""+nowTime+"""'"""
        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        for RS in selectRS:
            DL.append(RS)
        return DL
    except ConnectionError as e:
        print(e)