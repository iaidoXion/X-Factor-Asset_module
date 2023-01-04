import pandas as pd

def plug_in(data, inputPlugin, dataType) :
    if inputPlugin == 'DB':
        if dataType == 'minutely_statistics_list' :
            DFC = ['computer_id', 'computer_name', 'ipv_address', 'chassis_type', 'os_platform', 'is_virtual',
                   'last_reboot', 'driveUsage', 'ramUsage', 'cpuUsage', 'listenPortCountChange',
                   'establishedPortCountChange', 'running_service_count', 'online']
        elif dataType == 'minutely_statistics' :
            DFC = ['minutely_statistics_unique', 'classification', 'item', 'item_count']
        DFL = []
        for d in data:
            if dataType == 'minutely_statistics_list' :
                CID = d[0]
                CNM = d[1]
                IP = d[2]
                CT = d[3]
                OSP = d[4]
                IV = d[5]
                LR = d[6]
                DUS = d[7]
                RUS = d[8]
                CPUUS = d[9]
                LPC = d[10]
                EPC = d[11]
                RSC = d[12]
                OL = d[13]
                DFL.append([CID, CNM, IP, CT, OSP, IV, LR, DUS, RUS, CPUUS, LPC, EPC, RSC, OL])
            elif dataType == 'minutely_statistics':
                MSU = d[0]
                classification = d[1]
                item = d[2]
                IC = d[3]
                DFL.append([MSU, classification, item, IC])
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF

