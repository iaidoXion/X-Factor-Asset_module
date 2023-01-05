import pandas as pd

def plug_in(data, inputPlugin, dataType) :
    if inputPlugin == 'DB':
        if dataType == 'minutely_statistics_list' :
            DFC = ['computer_id', 'computer_name', 'ipv_address', 'chassis_type', 'os_platform', 'operating_system', 'is_virtual',
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
                OP = d[5]
                IV = d[6]
                LR = d[7]
                DUS = d[8]
                RUS = d[9]
                CPUUS = d[10]
                LPC = d[11]
                EPC = d[12]
                RSC = d[13]
                OL = d[14]
                DFL.append([CID, CNM, IP, CT, OSP, OP, IV, LR, DUS, RUS, CPUUS, LPC, EPC, RSC, OL])
            elif dataType == 'minutely_statistics':
                MSU = d[0]
                classification = d[1]
                item = d[2]
                IC = d[3]
                DFL.append([MSU, classification, item, IC])
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF

