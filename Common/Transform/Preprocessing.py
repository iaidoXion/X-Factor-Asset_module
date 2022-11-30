from datetime import datetime, timedelta
import pandas as pd



def plug_in(data):
    DL = []
    for c in range(len(data['today_computer_id'])) :
        CID = data['today_computer_id'][c]
        if not data['today_last_reboot'][c].startswith('[current') and not data['today_last_reboot'][c].startswith('TSE-Error'):
            LR = datetime.strptime(data['today_last_reboot'][c].replace('-', '+').split(' +')[0], "%a, %d %b %Y %H:%M:%S")
        else :
            LR = data['today_last_reboot'][c]

        TDUS = []
        TDUS_item = []
        TDUS_sum = 0
        TDUS_result = 0
        YDUS = []
        YDUS_item = []
        YDUS_sum = 0
        YDUS_result = 0

        TDUS_list = data['today_disk_used_space'][c].split(',')
        if "current result unavailable" in TDUS_list[0]:
            TDUS_item.append(TDUS_list[0])
        else:
            if len(TDUS_list) == 1:
                item = TDUS_list[0].split(' ')
                TDUS_item.append(item)
            elif len(TDUS_list) > 1:
                for d in TDUS_list:
                    item = d.split(' ')
                    TDUS_item.append(item)
            for i in TDUS_item :
                if len(i) == 3 :
                    if ('KB' in i[2]):
                        TDUS_result = int(i[1])
                    elif ('MB' in i[2]):
                        TDUS_result = int(i[1]) * 1024
                    elif ('GB' in i[2]):  # 기준
                        TDUS_result = int(i[1]) * 1024 * 1024
                    elif ('TB' in i[2]):
                        TDUS_result = int(i[1]) * 1024 * 1024 * 1024
                    elif ('PB' in i[2]):
                        TDUS_result = int(i[1]) * 1024 * 1024 * 1024 * 1024
                elif len(i) == 2:
                    if ("K" in i[1].upper()):
                        item = i[1].upper().find("K")
                        TDUS_result = float(i[1][:item])
                    elif ("M" in i[1].upper()):
                        item = i[1].upper().find("M")
                        TDUS_result = float(i[1][:item]) * 1024
                    elif ("G" in i[1].upper()):
                        item = i[1].upper().find("G")
                        TDUS_result = float(i[1][:item]) * 1024 * 1024
                TDUS_sum += TDUS_result
        items = round(TDUS_sum / 1024 / 1024)
        TDUS.append(str(items) + "KB")

        YDUS_list = data['yesterday_disk_used_space'][c].split(',')
        if "current result unavailable" in YDUS_list[0]:
            YDUS_item.append(YDUS_list[0])
        else:
            if len(YDUS_list) == 1:
                item = YDUS_list[0].split(' ')
                YDUS_item.append(item)
            elif len(YDUS_list) > 1:
                for d in YDUS_list:
                    item = d.split(' ')
                    YDUS_item.append(item)
            for i in YDUS_item :
                if len(i) == 3 :
                    if ('KB' in i[2]):
                        YDUS_result = int(i[1])
                    elif ('MB' in i[2]):
                        YDUS_result = int(i[1]) * 1024
                    elif ('GB' in i[2]):  # 기준
                        YDUS_result = int(i[1]) * 1024 * 1024
                    elif ('TB' in i[2]):
                        YDUS_result = int(i[1]) * 1024 * 1024 * 1024
                    elif ('PB' in i[2]):
                        YDUS_result = int(i[1]) * 1024 * 1024 * 1024 * 1024
                elif len(i) == 2:
                    if ("K" in i[1].upper()):
                        item = i[1].upper().find("K")
                        YDUS_result = float(i[1][:item])
                    elif ("M" in i[1].upper()):
                        item = i[1].upper().find("M")
                        YDUS_result = float(i[1][:item]) * 1024
                    elif ("G" in i[1].upper()):
                        item = i[1].upper().find("G")
                        YDUS_result = float(i[1][:item]) * 1024 * 1024
                YDUS_sum += YDUS_result
        items = round(YDUS_sum / 1024 / 1024)
        YDUS.append(str(items) + "KB")

        TIPA = data['today_ipv_address'][c]
        TLPC = data['today_listen_port_count'][c]
        YLPC = data['yesterday_listen_port_count'][c]
        TEPC = data['today_established_port_count'][c]
        YEPC = data['yesterday_established_port_count'][c]
        if not data['today_ram_use_size'][c].startswith('[current') and not data['today_last_reboot'][c].startswith('TSE-Error'):
            TRUS = data['today_ram_use_size'][c].split(' ')[0]
        else :
            TRUS = data['today_ram_use_size'][c]
        if not data['today_ram_total_size'][c].startswith('[current') and not data['today_last_reboot'][c].startswith('TSE-Error'):
            TRTS = data['today_ram_total_size'][c].split(' ')[0]
        else :
            TRTS = data['today_ram_total_size'][c]
        TIANM = data['today_installed_applications_name'][c].replace('{', '').replace('}', '').replace('"', '').split(',')
        TRP = data['today_running_processes'][c].replace('{', '').replace('}', '').replace('"', '').split(',')
        if not data['today_cup_consumption'][c].startswith('[current') and not data['today_last_reboot'][c].startswith('TSE-Error'):
            TCPUC = data['today_cup_consumption'][c].split(' ')[0]
        else :
            TCPUC = data['today_cup_consumption'][c]
        DL.append([CID, LR, TDUS, YDUS, TIPA, TLPC, YLPC, TEPC, YEPC, TRUS, TRTS, TIANM, TRP, TCPUC])

    return DL




