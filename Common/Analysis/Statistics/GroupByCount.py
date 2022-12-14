import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
def plug_in(data, classification, itemType) :
    DL = []
    RD = []
    CNM = itemType
    now = datetime.now()
    six_month_str = (now - relativedelta(months=6)).strftime("%Y-%m-%d %H:%M:%S")
    six_month = datetime.strptime(six_month_str, '%Y-%m-%d %H:%M:%S')
    for c in range(len(data.computer_id)):
        if classification == 'os' :
            DL.append(data.os_platform[c])
        elif classification == 'virtual' :
            DL.append(data.is_virtual[c])
        elif classification == 'asset' :
            DL.append(data.chassis_type[c])
        elif classification == 'installed_applications' :
            for d in data.installed_applications_name[c].replace('"', '').replace('{', '').replace('}', '').split(',') :
                DL.append(d)
        elif classification == 'listen_port_count_change' :
            DL.append(data.listenPortCountChange[c])
        elif classification == 'established_port_count_change' :
            DL.append(data.establishedPortCountChange[c])
        elif classification == 'running_processes' :
            for d in data.running_processes[c] :
                DL.append(d)
        elif classification == 'group_ram_usage_exceeded' :
            if data.ram[c] == 'Yes' :
                DL.append(data.ip_group[c])
        elif classification == 'group_cpu_usage_exceeded' :
            if data.cpu[c] == 'Yes':
                DL.append(data.ip_group[c])
        elif classification == 'group_listen_port_count_change' :
            if data.listenport_count[c] == 'Yes':
                DL.append(data.ip_group[c])
        elif classification == 'group_established_port_count_change' :
            if data.establishedport_count[c] == 'Yes':
                DL.append(data.ip_group[c])
        elif classification == 'group_running_processes_count_exceeded' :
            if data.running_processes_count[c] == 'Yes':
                DL.append(data.ip_group[c])
        elif classification == 'group_last_reboot' :
            if not data.last_reboot[c] == 'unconfirmed' :
                if datetime.strptime(data.last_reboot[c], '%Y-%m-%d %H:%M:%S') < six_month :
                    DL.append(data.ip_group[c])

        #elif classification == 'alarm_group_ram' :
            #print(data.ramUsage[c])

    DF = pd.DataFrame(DL, columns=[CNM])
    DFG = DF.groupby([CNM]).size().reset_index(name='counts')
    DFGS = DFG.sort_values(by="counts", ascending=False)

    if classification == 'os' :
        statistics_unique = classification + '_' + DFGS.OP
        item = DFGS.OP
    elif classification == 'virtual':
        statistics_unique = classification + '_' + DFG.IV
        item = DFGS.IV
    elif classification == 'asset':
        statistics_unique = classification+'_'+DFGS.CT
        item = DFGS.CT
    elif classification == 'installed_applications':
        statistics_unique = classification+'_'+DFGS.IANM
        item = DFGS.IANM
    elif classification == 'listen_port_count_change':
        statistics_unique = classification+'_'+ DFGS.LPC
        item = DFGS.LPC
    elif classification == 'established_port_count_change':
        statistics_unique = classification + '_' + DFGS.EPC
        item = DFGS.EPC
    elif classification == 'running_processes':
        statistics_unique = classification + '_' + DFGS.RPNM
        item = DFGS.RPNM
    elif classification == 'group_ram_usage_exceeded' or classification == 'group_cpu_usage_exceeded' or classification == 'group_listen_port_count_change' or classification == 'group_established_port_count_change' or classification  == 'group_running_processes_count_exceeded' or classification == 'group_last_reboot' :
        statistics_unique = classification + '_' + DFGS.ip_group
        item = DFGS.ip_group
    item_count = DFG.counts

    for DFC in range(len(DFGS)) :
        RD.append([statistics_unique[DFC], classification, item[DFC], item_count[DFC]])
    return RD

