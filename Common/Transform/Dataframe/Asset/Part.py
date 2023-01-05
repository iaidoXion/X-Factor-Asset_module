import pandas as pd

def plug_in(data, inputPlugin, dataType) :
    if inputPlugin == 'DB':
        if dataType == 'minutely_asset' :
            DFC = ['computer_id', 'installed_applications_name', 'manufacturer', 'running_service']
        elif dataType == 'minutely_daily_asset' :
            DFC = ['computer_id', 'computer_name', 'last_reboot', 'disk_total_space', 'disk_used_space',
                   'os_platform', 'operating_system', 'is_virtual', 'chassis_type',
                   'ipv_address', 'today_listen_port_count', 'yesterday_listen_port_count',
                   'today_established_port_count', 'yesterday_established_port_count',
                   'ram_use_size', 'ram_total_size', 'installed_applications_name',
                   'running_service', 'cup_consumption', 'online']
        DFL = []
        for d in data:
            if dataType == 'minutely_asset':
                CID = d[0]
                IAN= d[1]
                manufacturer = d[2]
                RS= d[3]
                DFL.append([CID, IAN, manufacturer, RS])
            elif dataType == 'minutely_daily_asset' :
                CID = d[0]
                CNM = d[1]
                LR = d[2]
                DTS = d[3]
                DUS = d[4]
                OSP = d[5]
                OS = d[6]
                IV = d[7]
                CT = d[8]
                IP = d[9]
                TLPC = d[10]
                YLPC = d[11]
                TEPC = d[12]
                YEPC = d[13]
                RUS = d[14]
                RTS = d[15]
                IAN = d[16]
                RS = d[17]
                CC = d[18]
                OL = d[19]
                DFL.append([CID, CNM, LR, DTS, DUS, OSP, OS, IV, CT, IP, TLPC, YLPC, TEPC, YEPC, RUS, RTS, IAN, RS, CC, OL])

    DF = pd.DataFrame(DFL, columns=DFC)
    return DF