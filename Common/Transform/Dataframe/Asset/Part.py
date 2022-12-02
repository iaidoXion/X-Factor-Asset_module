import pandas as pd

def plug_in(data, inputPlugin) :
    if inputPlugin == 'DB':
        DFC = ['computer_id', 'last_reboot', 'today_disk_used_space', 'yesterday_disk_used_space',
               'os_platform', 'is_virtual', 'chassis_type',
               'ipv_address', 'today_listen_port_count', 'yesterday_listen_port_count',
               'today_established_port_count', 'yesterday_established_port_count',
               'ram_use_size', 'ram_total_size', 'installed_applications_name',
               'running_processes', 'cup_consumption']

    DFL = []
    for d in data :
        if inputPlugin == 'DB' :
            CID = d[0]
            LR = d[1]
            TDUS = d[2]
            YDUS = d[3]
            OSP = d[4]
            IV = d[5]
            CT = d[6]
            IP = d[7]
            TLPC = d[8]
            YLPC = d[9]
            TEPC = d[10]
            YEPC = d[11]
            RUS = d[12]
            RTS = d[13]
            IAN = d[14]
            RP = d[15]
            CC = d[16]
        DFL.append([CID, LR, TDUS, YDUS, OSP, IV, CT, IP, TLPC, YLPC, TEPC, YEPC, RUS, RTS, IAN, RP, CC])
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF