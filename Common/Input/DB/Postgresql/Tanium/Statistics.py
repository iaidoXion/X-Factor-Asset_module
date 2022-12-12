import psycopg2
import json
from datetime import datetime, timedelta
def plug_in(dataType) :
#def plug_in(period, dataType) :
    try:
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['PORT']
        DBNM = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['USER']
        DBPWD = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['PWD']
        MST = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['TNM']['MS']
        nowTime = datetime.today().strftime("%Y-%m-%d %H:%M")
        DL = []
        selectConn = psycopg2.connect(
            'host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        if dataType == 'minutely':
            SQ = """
                select 
                    classification,
                    item,
                    item_count
                from  
            """ + MST +"""
            where to_char(statistics_collection_date , 'YYYY-MM-DD HH24:MI') = '"""+nowTime+"""'"""
        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        for RS in selectRS:
            DL.append(RS)
        return DL
    except ConnectionError as e:
        print(e)