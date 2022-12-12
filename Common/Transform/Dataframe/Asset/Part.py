import pandas as pd

def plug_in(data, inputPlugin, dataType) :
    if inputPlugin == 'DB':
        if dataType == 'minutely_asset' :
            DFC = ['computer_id', 'installed_applications_name', 'manufacturer', 'running_processes']
        elif dataType == 'minutely_daily_asset' :
            DFC = ['computer_id', 'computer_name', 'last_reboot', 'disk_total_space', 'disk_used_space',
                   'os_platform', 'is_virtual', 'chassis_type',
                   'ipv_address', 'today_listen_port_count', 'yesterday_listen_port_count',
                   'today_established_port_count', 'yesterday_established_port_count',
                   'ram_use_size', 'ram_total_size', 'installed_applications_name',
                   'running_processes', 'cup_consumption', 'online']
        DFL = []
        for d in data:
            if dataType == 'minutely_asset':
                CID = d[0]
                IAN= d[1]
                manufacturer = d[2]
                RP= d[3]
                DFL.append([CID, IAN, manufacturer, RP])
            elif dataType == 'minutely_daily_asset' :
                CID = d[0]
                CNM = d[1]
                LR = d[2]
                DTS = d[3]
                DUS = d[4]
                OSP = d[5]
                IV = d[6]
                CT = d[7]
                IP = d[8]
                TLPC = d[9]
                YLPC = d[10]
                TEPC = d[11]
                YEPC = d[12]
                RUS = d[13]
                RTS = d[14]
                IAN = d[15]
                RP = d[16]
                CC = d[17]
                OL = d[18]
                DFL.append([CID, CNM, LR, DTS, DUS, OSP, IV, CT, IP, TLPC, YLPC, TEPC, YEPC, RUS, RTS, IAN, RP, CC, OL])

    DF = pd.DataFrame(DFL, columns=DFC)
    return DF