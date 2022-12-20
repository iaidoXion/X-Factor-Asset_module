from datetime import datetime
from dateutil.relativedelta import relativedelta

def plug_in(data, dataType) :
    #print(data)
    DL = []
    for c in range(len(data.computer_id)):
        if dataType == 'alarm' :
            IPS = data.ipv_address[c].split('.')
            if len(IPS) > 1 :
                IPG = IPS[0]+'.'+IPS[0]+'.'+IPS[2]
            else:
                IPG = IPS[0]

            if data.ramUsage[c] == 'unconfirmed':
                RAM = data.ramUsage[c]
            else:
                if float(data.ramUsage[c]) > 60.0 :
                    RAM = 'Yes'
                else :
                    RAM = 'No'

            if data.cpuUsage[c] == 'unconfirmed':
                CPU = data.cpuUsage[c]
            else:
                if float(data.cpuUsage[c]) > 60.0 :
                    CPU = 'Yes'
                else :
                    CPU = 'No'

            if data.runningProcessesCount[c] == 'unconfirmed' :
                RPC = data.runningProcessesCount[c]
            else:
                if int(data.runningProcessesCount[c]) > 100 :
                    RPC = 'Yes'
                else :
                    RPC = 'No'

            if data.driveUsage[c] == 'unconfirmed' :
                DUS = data.driveUsage[c]
            else :
                if float(data.driveUsage[c]) > 60.0 :
                    DUS = 'Yes'
                else:
                    DUS = 'No'

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

            if len(data['running_processes'][c]) > 1:
                runningProcessesCount = len(data['running_processes'][c])
            else:
                if not data['running_processes'][c][0].startswith('[current') and not data['running_processes'][c][0].startswith('TSE-Error') and not data['running_processes'][c][0].startswith('Unknown'):
                    runningProcessesCount = 'unconfirmed'
                else:
                    runningProcessesCount = 1

            if data['online'][c] == 'True':
                online = 'Yes'
            else:
                online = 'unconfirmed'
            DL.append([data.computer_id[c], listenPortCountChange, establishedPortCountChange, str(runningProcessesCount), str(online)])
    return DL