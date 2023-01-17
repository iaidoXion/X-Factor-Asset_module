from datetime import datetime
from dateutil.relativedelta import relativedelta

def plug_in(data, dataType) :
    DL = []
    for c in range(len(data.computer_id)):
        if dataType == 'alarm' :
            IPS = data.ipv_address[c].split('.')
            if len(IPS) > 1 :
                IPG = IPS[0]+'.'+IPS[1]+'.'+IPS[2]
            else:
                IPG = IPS[0]

            if data.ramUsage[c] == 'unconfirmed':
                RAM = data.ramUsage[c]
            else:
                if float(data.ramUsage[c]) >= 95.0 :
                    RAM = '95Risk'
                if float(data.ramUsage[c]) >= 75.0 :
                    RAM = '75Risk'
                if float(data.ramUsage[c]) >= 60.0 :
                    RAM = '60Risk'
                if float(data.ramUsage[c]) < 60.0 :
                    RAM = 'safety'

            if data.cpuUsage[c] == 'unconfirmed':
                CPU = data.cpuUsage[c]
            else:
                if float(data.cpuUsage[c]) >= 95.0:
                    CPU = '95Risk'
                if float(data.cpuUsage[c]) >= 75.0:
                    CPU = '75Risk'
                if float(data.cpuUsage[c]) >= 60.0 :
                    CPU = '60Risk'
                if float(data.cpuUsage[c]) < 60.0 :
                    CPU = 'safety'

            if data.running_service_count[c] == 'unconfirmed' :
                RPC = data.running_service_count[c]
            else:
                if int(data.running_service_count[c]) > 100 :
                    RPC = 'Yes'
                else :
                    RPC = 'No'

            if data.driveUsage[c] == 'unconfirmed' :
                DUS = data.driveUsage[c]
            else :
                if float(data.driveUsage[c]) >= 99.0:
                    DUS = '99Risk'
                if float(data.driveUsage[c]) >= 95.0:
                    DUS = '95Risk'
                if float(data.driveUsage[c]) >= 75.0:
                    DUS = '75Risk'
                if float(data.driveUsage[c]) >= 60.0 :
                    DUS = '60Risk'
                if float(data.driveUsage[c]) < 60.0 :
                    DUS = 'Safety'

            if data.last_reboot[c] == 'unconfirmed' :
                LRB = data.last_reboot[c]
            else :
                now = datetime.now()
                six_month_str = (now - relativedelta(months=1)).strftime("%Y-%m-%d %H:%M:%S")
                six_month = datetime.strptime(six_month_str, '%Y-%m-%d %H:%M:%S')
                LRBDT = datetime.strptime(data.last_reboot[c], '%Y-%m-%d %H:%M:%S')
                if LRBDT < six_month :
                    LRB = 'Yes'
                else :
                    LRB = 'No'



            DL.append([data.computer_id[c], IPG, RAM, CPU, data.listenPortCountChange[c], data.establishedPortCountChange[c], RPC, LRB, DUS])
        elif dataType == 'online' :
            IPS = data.ipv_address[c].split('.')
            if len(IPS) > 1:
                IPG = IPS[0] + '.' + IPS[1] + '.' + IPS[2]
            else:
                IPG = IPS[0]
            if data.asset_list_statistics_collection_date[c] == 'unconfirmed' :
                ALSCD = data.asset_list_statistics_collection_date[c]
            else :
                now = datetime.now()
                thirty_minutes_str = (now - relativedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
                thirty_minutes = datetime.strptime(thirty_minutes_str, '%Y-%m-%d %H:%M:%S')
                ALSCDDT = datetime.strptime(str(data.asset_list_statistics_collection_date[c]), '%Y-%m-%d %H:%M:%S')
                if ALSCDDT < thirty_minutes :
                    ALSCD = 'Yes'
                else :
                    ALSCD = 'No'
            DL.append([data.computer_id[c], IPG, ALSCD])
        else :
            if data.today_listen_port_count[c].isdigit() and data.yesterday_listen_port_count[c].isdigit():
                if data.today_listen_port_count[c] == data.yesterday_listen_port_count[c]:
                    listenPortCountChange = 'No'
                else:
                    listenPortCountChange = 'Yes'
            else:
                listenPortCountChange = 'unconfirmed'

            if data.today_established_port_count[c].isdigit() and data.yesterday_established_port_count[c].isdigit():
                if data.today_established_port_count[c] == data.yesterday_established_port_count[c]:
                    establishedPortCountChange = 'No'
                else:
                    establishedPortCountChange = 'Yes'
            else:
                establishedPortCountChange = 'unconfirmed'

            if data['online'][c] == 'True':
                online = 'Yes'
            else:
                online = 'unconfirmed'
            DL.append([data.computer_id[c], listenPortCountChange, establishedPortCountChange,  str(online)])
    return DL