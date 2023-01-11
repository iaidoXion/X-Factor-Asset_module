import pandas as pd

def plug_in(data, inputPlugin, dataType, columnsType) :
    if inputPlugin == 'DB':
        if dataType == 'minutely_statistics_list' :
            if columnsType == 'usage':
                DFC = ['computer_id',  'driveUsage', 'ramUsage', 'cpuUsage']
            elif columnsType == 'compare' :
                DFC = ['computer_id', 'listenPortCountChange','establishedPortCountChange', 'online']
            elif columnsType == 'normal':
                DFC = ['computer_id', 'computer_name', 'ipv_address', 'chassis_type', 'os_platform', 'operating_system', 'is_virtual', 'last_reboot', 'tanium_client_subnet', 'manufacturer', 'nvidia_smi']
            elif columnsType == 'count':
                DFC = ['computer_id', 'running_service_count', 'session_ip_count']


            elif columnsType == 'alarm' :
                DFC = ['computer_id','ip_group', 'ram', 'cpu', 'listenport_count', 'establishedport_count', 'running_service_count', 'last_reboot', 'drive']
        elif dataType == 'minutely_statistics' :
            DFC = ['classification', 'item', 'item_count']
        elif dataType == 'minutely_statistics_list_online' :
            DFC = ['computer_id', 'asset_list_statistics_collection_date']
        DFL = []
        for d in data:
            if dataType == 'minutely_statistics_list' :
                CID = d[0]
                if columnsType == 'usage' :
                    DUS = d[1]
                    RUS = d[2]
                    CPUUS = d[3]
                    DFL.append([CID, DUS, RUS, CPUUS])
                elif columnsType == 'compare' :
                    LPC = d[1]
                    EPC = d[2]
                    OL = d[3]
                    DFL.append([CID, LPC, EPC, OL])

                elif columnsType == 'normal':
                    CNM = d[1]
                    IP = d[2]
                    CT = d[3]
                    OSP = d[4]
                    OS = d[5]
                    IV = d[6]
                    LR = d[7]
                    TCS = d[8]
                    MF = d[9]
                    NS = d[10]
                    DFL.append([CID, CNM, IP, CT, OSP, OS, IV, LR, TCS, MF, NS])

                elif columnsType == 'count':
                    RSC = d[1]
                    SIP = d[2]
                    DFL.append([CID, RSC, SIP])

                elif columnsType == 'alarm':
                    IPG = d[1]
                    RAM = d[2]
                    CPU = d[3]
                    LPC = d[4]
                    EPC = d[5]
                    RSC = d[6]
                    LRB = d[7]
                    DUS = d[8]
                    DFL.append([CID, IPG, RAM, CPU, LPC, EPC, RSC, LRB, DUS])
            elif dataType == 'minutely_statistics' :
                classification = d[0]
                item = d[1]
                IC = d[2]
                DFL.append([classification, item, IC])
            elif dataType == 'minutely_statistics_list_online' :
                CID = d[0]
                ALSCD = d[1]
                DFL.append([CID, ALSCD])
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF

