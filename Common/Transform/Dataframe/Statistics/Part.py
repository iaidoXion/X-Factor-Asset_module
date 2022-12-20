import pandas as pd

def plug_in(data, inputPlugin, dataType, columnsType) :
    if inputPlugin == 'DB':
        if dataType == 'minutely_statistics_list' :
            if columnsType == 'usage':
                DFC = ['computer_id', 'computer_name', 'ipv_address', 'chassis_type', 'os_platform', 'is_virtual',
                       'last_reboot', 'driveUsage', 'ramUsage', 'cpuUsage']
            elif columnsType == 'compare' :
                DFC = ['computer_id', 'listenPortCountChange','establishedPortCountChange', 'runningProcessesCount', 'online']
            elif columnsType == 'alarm' :
                DFC = ['computer_id','ip_group', 'ram', 'cpu', 'listenport_count', 'establishedport_count', 'running_processes_count', 'last_reboot', 'drive']
        elif dataType == 'minutely_statistics' :
            DFC = ['classification', 'item', 'item_count']
        DFL = []
        for d in data:
            if dataType == 'minutely_statistics_list' :
                CID = d[0]
                if columnsType == 'usage' :
                    CNM = d[1]
                    IP = d[2]
                    CT = d[3]
                    OSP = d[4]
                    IV = d[5]
                    LR = d[6]
                    DUS = d[7]
                    RUS = d[8]
                    CPUUS = d[9]
                    DFL.append([CID, CNM, IP, CT, OSP, IV, LR, DUS, RUS, CPUUS])
                elif columnsType == 'compare' :
                    LPC = d[1]
                    EPC = d[2]
                    RPC = d[3]
                    OL = d[4]
                    DFL.append([CID, LPC, EPC, RPC, OL])
                elif columnsType == 'alarm':
                    IPG = d[1]
                    RAM = d[2]
                    CPU = d[3]
                    LPC = d[4]
                    EPC = d[5]
                    RPC = d[6]
                    LRB = d[7]
                    DUS = d[8]
                    DFL.append([CID, IPG, RAM, CPU, LPC, EPC, RPC, LRB, DUS])
            elif dataType == 'minutely_statistics' :
                classification = d[0]
                item = d[1]
                IC = d[2]
                DFL.append([classification, item, IC])
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF

