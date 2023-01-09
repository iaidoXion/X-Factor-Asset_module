from datetime import datetime, timedelta
import pandas as pd



def plug_in(data, dataType):
    DL = []
    #print(len(data))
    for c in range(len(data['computer_id'])) :
        CID = data['computer_id'][c]
        IANM = data['installed_applications_name'][c]
        ## IANM 전처리 해야함. 그리고 나서 통계낼때 통계에서 전처리 한게 있는지 확인!! Common>Analysis>Statistics>Compare.py
        RS = data['running_service'][c].replace('{', '').replace('}', '').replace('"', '').split(',')
        ## RS 전처리 해야함. 그리고 나서 통계낼때 통계에서 전처리 한게 있는지 확인!! Common>Analysis>Statistics>Compare.py
        manufacturer = data['manufacturer'][c]
        ## manufacturer 전처리 해야함.
        if dataType == 'minutely_daily_asset':
            CNM = data['computer_name'][c]
            if not data['last_reboot'][c].startswith('[current') and not data['last_reboot'][c].startswith('TSE-Error') and not data['last_reboot'][c].startswith('Unknown'):
                LR = datetime.strptime(data['last_reboot'][c].replace('-', '+').split(' +')[0], "%a, %d %b %Y %H:%M:%S")
            else:
                LR = 'unconfirmed'

            DTS = []
            DTS_item = []
            DTS_sum = 0
            DTS_result = 0
            if not data['disk_total_space'][c].startswith('{"[current') and not data['disk_total_space'][c].startswith('{"TSE-Error') and not data['disk_total_space'][c].startswith('{"Unknown'):
                if data['disk_total_space'][c] == None:
                    data['disk_total_space'][c] = 0
                else:
                    DTS_list = data['disk_total_space'][c].split(',')
                    if len(DTS_list) == 1:
                        item = DTS_list[0].split(' ')
                        DTS_item.append(item)
                    elif len(DTS_list) > 1:
                        for d in DTS_list:
                            item = d.split(' ')
                            DTS_item.append(item)
                    for i in DTS_item:
                        if len(i) == 3:
                            if ('KB' in i[2]):
                                DTS_result = int(i[1])
                            elif ('MB' in i[2]):
                                DTS_result = int(i[1]) * 1024
                            elif ('GB' in i[2]):  # 기준
                                DTS_result = int(i[1]) * 1024 * 1024
                            elif ('TB' in i[2]):
                                DTS_result = int(i[1]) * 1024 * 1024 * 1024
                            elif ('PB' in i[2]):
                                DTS_result = int(i[1]) * 1024 * 1024 * 1024 * 1024
                        elif len(i) == 2:
                            if ("K" in i[1].upper()):
                                item = i[1].upper().find("K")
                                DTS_result = float(i[1][:item])
                            elif ("M" in i[1].upper()):
                                item = i[1].upper().find("M")
                                DTS_result = float(i[1][:item]) * 1024
                            elif ("G" in i[1].upper()):
                                item = i[1].upper().find("G")
                                DTS_result = float(i[1][:item]) * 1024 * 1024
                        DTS_sum += DTS_result
                    items = round(DTS_sum / 1024 / 1024)
                DTS.append(str(items))
            else:
                DTS.append(data['disk_total_space'][c].replace('{"', '').replace('"}', ''))

            DUS = []
            DUS_item = []
            DUS_sum = 0
            DUS_result = 0
            if not data['disk_used_space'][c].startswith('{"[current') and not data['disk_used_space'][c].startswith('{"TSE-Error') and not data['disk_used_space'][c].startswith('{"Unknown'):
                if data['disk_used_space'][c] == None:
                    data['disk_used_space'][c] = 0
                else:
                    DUS_list = data['disk_used_space'][c].split(',')
                    if len(DUS_list) == 1:
                        item = DUS_list[0].split(' ')
                        DUS_item.append(item)
                    elif len(DUS_list) > 1:
                        for d in DUS_list:
                            item = d.split(' ')
                            DUS_item.append(item)
                    for i in DUS_item:
                        if len(i) == 3:
                            if ('KB' in i[2]):
                                DUS_result = int(i[1])
                            elif ('MB' in i[2]):
                                DUS_result = int(i[1]) * 1024
                            elif ('GB' in i[2]):  # 기준
                                DUS_result = int(i[1]) * 1024 * 1024
                            elif ('TB' in i[2]):
                                DUS_result = int(i[1]) * 1024 * 1024 * 1024
                            elif ('PB' in i[2]):
                                DUS_result = int(i[1]) * 1024 * 1024 * 1024 * 1024
                        elif len(i) == 2:
                            if ("K" in i[1].upper()):
                                item = i[1].upper().find("K")
                                DUS_result = float(i[1][:item])
                            elif ("M" in i[1].upper()):
                                item = i[1].upper().find("M")
                                DUS_result = float(i[1][:item]) * 1024
                            elif ("G" in i[1].upper()):
                                item = i[1].upper().find("G")
                                DUS_result = float(i[1][:item]) * 1024 * 1024
                        DUS_sum += DUS_result
                    items = round(DUS_sum / 1024 / 1024)
                DUS.append(str(items))
            else:
                DUS.append(data['disk_used_space'][c].replace('{"', '').replace('"}', ''))

            if not data['os_platform'][c].startswith('[current') and not data['os_platform'][c].startswith('TSE-Error') and not data['os_platform'][c].startswith('Unknown'):
                OP = data['os_platform'][c]
            else:
                OP = 'unconfirmed'
            if not data['operating_system'][c].startswith('[current') and not data['operating_system'][c].startswith('TSE-Error') and not data['operating_system'][c].startswith('Unknown'):
                OS = data['operating_system'][c]
            else:
                OS = 'unconfirmed'
            if not data['is_virtual'][c].startswith('[current') and not data['is_virtual'][c].startswith('TSE-Error') and not data['is_virtual'][c].startswith('Unknown'):
                IV = data['is_virtual'][c]
            else:
                IV = 'unconfirmed'
            if not data['chassis_type'][c].startswith('[current') and not data['chassis_type'][c].startswith('TSE-Error') and not data['chassis_type'][c].startswith('Unknown'):
                CT = data['chassis_type'][c]
            else:
                CT = 'unconfirmed'
            if not data['ipv_address'][c].startswith('[current') and not data['ipv_address'][c].startswith('TSE-Error') and not data['ipv_address'][c].startswith('Unknown'):
                IPV = data['ipv_address'][c]
            else:
                IPV = 'unconfirmed'

            if not data['today_listen_port_count'][c] == None and not data['today_listen_port_count'][c].startswith('[current') and not data['today_listen_port_count'][c].startswith('TSE-Error') and not data['today_listen_port_count'][c].startswith('Unknown'):
                LPC = data['today_listen_port_count'][c]
            else:
                LPC = 'unconfirmed'

            if not data['today_established_port_count'][c] == None and not data['today_established_port_count'][c].startswith('[current') and not data['today_established_port_count'][c].startswith('TSE-Error') and not data['today_established_port_count'][c].startswith('Unknown'):
                EPC = data['today_established_port_count'][c]
            else:
                EPC = 'unconfirmed'

            if not data['yesterday_listen_port_count'][c] == None and not data['yesterday_listen_port_count'][c].startswith('[current') and not data['yesterday_listen_port_count'][c].startswith('TSE-Error') and not data['yesterday_listen_port_count'][c].startswith('Unknown'):
                YLPC = data['yesterday_listen_port_count'][c]
            else:
                YLPC = 'unconfirmed'

            if not data['yesterday_established_port_count'][c] == None and not data['yesterday_established_port_count'][c].startswith('[current') and not data['yesterday_established_port_count'][c].startswith('TSE-Error') and not data['yesterday_established_port_count'][c].startswith('Unknown'):
                YEPC = data['yesterday_established_port_count'][c]
            else:
                YEPC = 'unconfirmed'

            if not data['ram_use_size'][c].startswith('[current') and not data['ram_use_size'][c].startswith('TSE-Error') and not data['ram_use_size'][c].startswith('Unknown'):
                RUS = data['ram_use_size'][c].split(' ')[0]
            else:
                RUS = data['ram_use_size'][c]
            if not data['ram_total_size'][c].startswith('[current') and not data['ram_total_size'][c].startswith('TSE-Error') and not data['ram_total_size'][c].startswith('Unknown'):
                RTS = data['ram_total_size'][c].split(' ')[0]
            else:
                RTS = data['ram_total_size'][c]

            if not data['cup_consumption'][c].startswith('[current') and not data['cup_consumption'][c].startswith('TSE-Error'):
                CPUC = float(data['cup_consumption'][c].split(' ')[0])
            else:
                CPUC = data['cup_consumption'][c]

            OL = data['online'][c]
            ## OL 전처리 해야함. 그리고 나서 통계낼때 통계에서 전처리 한게 있는지 확인!! Common>Analysis>Statistics>Compare.py

            TCS = data['tanium_client_subnet'][c]
            ## TCS 전처리 해야함.

            SIP = []
            for d in data['session_ip'][c] :
                if len(d.replace('{"', '').replace('"}', '').split('","')) > 1 :
                    SIP.append(d.replace('{"', '').replace('"}', '').split('","'))
                else:
                    if not d.replace('{"', '').replace('"}', '').startswith('[current') and not d.replace('{"', '').replace('"}', '').startswith('[no') and not d.replace('{"', '').replace('"}', '').startswith('TSE-Error') :
                        SIP.append(d.replace('{"', '').replace('"}', ''))
                    else :
                        SIP.append(['unconfirmed'])

            NS = data['nvidia_smi'][c]
            ## NS 전처리 해야함.

            DL.append([CID, CNM, LR, DTS, DUS, OP, OS, IV, CT, IPV, LPC, YLPC, EPC, YEPC, RUS, RTS, IANM, RS, CPUC, OL, TCS, manufacturer, SIP[0], NS])
        elif dataType == 'minutely_asset':
            DL.append([CID, IANM, manufacturer, RS])

    return DL




