from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import json
import logging
from collections import Counter

def plug_in(data, dataType):
    if dataType == 'question' :
        a = []
        good_list = []
        weak_list = []
        date_list = []
        fullpath = data
        QDF = pd.read_excel(fullpath)
        for i in QDF['vulnerability_standard'] :
            a = i.split('취약')
            a[1] = "취약" + a[1]
            good_list.append(a[0])
            weak_list.append(a[1])
            date_list.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        QDF['vulnerability_standard_good'] = good_list
        QDF['vulnerability_standard_weak'] = weak_list
        # pprint(QDF['vulnerability_standard_good'])
        QDF['vulnerability_create_date'] = date_list
        DF = QDF.drop(['vulnerability_standard'], axis=1)
        return DF
    if dataType == 'VUL' :
        try:
            weak_dict = {}
            status_list = []
            value_list = []
            cid_list = []
            cpn_list = []
            ct_list = []
            ip_list = []
            lr_list = []
            os_list = []
            online_list = []
            swv_list = []
            date_list = []
            logging.info('Tanium ' + dataType + ' Data Transform(Dataframe) Plug In Start')
            for i in data :
                for j in i['list'] :
                    if 'cid' in i :
                        cid_list.append(i['cid'])
                    if 'cpn' in i :
                        cpn_list.append(i['cpn'])
                    if 'ct' in i :
                        ct_list.append(i['ct'])
                    if 'ip' in i :
                        ip_list.append(i['ip'])
                    if 'lr' in i :
                        lr_list.append(i['lr'])
                    if 'os' in i :
                        os_list.append(i['os'])
                    if 'online' in i :
                        online_list.append(i['online'])
                    if 'status' in j :
                        status_list.append(j['status'])
                    else :
                        status_list.append('TSE-Error')
                    if 'value' in j :
                        value_list.append(j['value'])
                    else :
                        value_list.append('TSE-Error')
                    if 'SWV' in j :
                        swv_list.append(j['SWV'])
                    else :
                        swv_list.append('TSE-Error')
                    date_list.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    # class_date_list.append(datetime.now().strftime('%Y-%m-%d'))
            logging.info('Completing list operations for putting into a data frame')
            for i in range(len(value_list)) :
                if type(value_list[i]) == list :
                    for j in range(len(value_list[i])) :
                        if type(value_list[i][j]) == dict :
                            value_list[i][j] = str(value_list[i][j])
                elif type(value_list[i]) == dict :
                    value_list[i] = str(value_list[i])
            dup_cid = dict(Counter(cid_list))
            cid = []
            for x in dup_cid :
                for y in range(int(dup_cid[x])) :
                    cid.append(x + '-' + str(y))
            weak_dict['computer_id'] = cid_list
            weak_dict['vulnerability_code'] = swv_list
            weak_dict['vulnerability_judge_result'] = status_list
            weak_dict['vulnerability_judge_update_time'] = date_list
            weak_dict['vulnerability_judge_reason'] = value_list
            weak_dict['computer_name'] = cpn_list
            weak_dict['chassis_type'] = ct_list
            weak_dict['tanium_client_nat_ip_address'] = ip_list
            # for i in range(len(lr_list)) :
            #     if 'current result unavailable' in lr_list[i] :
            #         lr_list[i] = '0000-00-00 00:00:00.000'
            # pprint(lr_list)
            weak_dict['last_reboot'] = lr_list
            weak_dict['online'] = online_list
            weak_dict['operating_system'] = os_list
            weak_dict['classification_cid'] = cid
            # weak_dict['classification_date'] = class_date_list
            DF = pd.DataFrame(weak_dict)
            DF = DF.astype({'computer_id': 'object'})
            DF = DF.astype({'vulnerability_judge_update_time': 'datetime64'})
            logging.info('Tanium ' + dataType + ' Data Transform(Dataframe) Plug In Finish')

            return DF
        except:
            logging.warning('Error running Tanium ' + dataType + ' Data Transform (Data Frame) plugin')
            return 'error'
    
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()
    if PROGRESS == 'true' :
        DATA_list = tqdm(range(len(data['computer_id'])), 
                            total=len(data['computer_id']),
                            desc='Common_TransForm_PrePro_{}'.format(dataType),
                            )
    else :
        DATA_list = range(len(data['computer_id']))
    
    DL = []
    for c in DATA_list :
        
        CID = data['computer_id'][c]
        IANM = []
        if not data['installed_applications_name'][c].startswith('{"[current') and not data['installed_applications_name'][c].startswith('{"TSE-Error') and not data['installed_applications_name'][c].startswith('{"Unknown') and not data['installed_applications_name'][c]== ' ':
            for d in data.installed_applications_name[c].replace('"', '').replace('{', '').replace('}', '').split('!'):
                if d.startswith(','):
                    d = d.lstrip(',').replace('"','')
                else :
                    d = d.replace('"','')
                IANM.append(d)
            IANM.pop(-1)
        else:
            IANM.append('unconfirmed')


        if not data['running_service'][c].startswith('{"[current') and not data['running_service'][c].startswith('{"TSE-Error') and not data['running_service'][c].startswith('Unknown') and not data['running_service'][c] == ' ':
            if data['running_service'][c][1] == '"':
                RS = data['running_service'][c].replace('"','').replace('{','').replace('}','').split(',')
            else:
                RS = data['running_service'][c].replace('"','').replace('{','').replace('}','').split(',')
        else:
            RS= 'unconfirmed'


        if not data['manufacturer'][c].startswith('[current') and not data['manufacturer'][c].startswith('TSE-Error') and not data['manufacturer'][c].startswith('Unknown') and not data['manufacturer'][c]== ' ':
            MF = data['manufacturer'][c]
        else:
            MF = 'unconfirmed'

        #세션ip 전처리 추가 예정(server별 session상위5개)
        if data['session_ip'][c].startswith('{"[current') or data['session_ip'][c].startswith('{"TSE-Error') or data['session_ip'][c].startswith('{"[Unknown') or data['session_ip'][c] == ' ':
            SIP = ['unconfirmed']
        elif data['session_ip'][c].startswith('{"[no result'):
            SIP = ['no results']
        else:
            if data['session_ip'][c][2] == '(':
                SIP = data['session_ip'][c].replace('{', '[').replace('}', ']')
                SIP = eval(SIP)
                SIPL = []
                for a in range(len(SIP)):
                    if SIP[a].startswith('[hash'):
                        SIPL.append('hash collision')
                    else:
                        b = eval(SIP[a])
                        SIPL.append(b[1])
                SIP = []
                SIP.append(SIPL)

            else:
                SIP = data['session_ip'][c].replace('{', '[').replace('}', ']')
                SIP = eval(SIP)
                SIPL = []
                for a in range(len(SIP)) :
                    abc = SIP[a].split(' ')
                    SIPL.append(abc[1])
                SIP = []
                SIP.append(SIPL)

        if dataType == 'minutely_daily_asset':
            CNM = data['computer_name'][c]
            CDS = data['cup_details_cup_speed'][c]
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
                                DTS_result = float(i[1])
                            elif ('MB' in i[2]):
                                DTS_result = float(i[1]) * 1024
                            elif ('GB' in i[2]):  # 기준
                                DTS_result = float(i[1]) * 1024 * 1024
                            elif ('TB' in i[2]):
                                DTS_result = float(i[1]) * 1024 * 1024 * 1024
                            elif ('PB' in i[2]):
                                DTS_result = float(i[1]) * 1024 * 1024 * 1024 * 1024
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
                                DUS_result = float(i[1])
                            elif ('MB' in i[2]):
                                DUS_result = float(i[1]) * 1024
                            elif ('GB' in i[2]):  # 기준
                                DUS_result = float(i[1]) * 1024 * 1024
                            elif ('TB' in i[2]):
                                DUS_result = float(i[1]) * 1024 * 1024 * 1024
                            elif ('PB' in i[2]):
                                DUS_result = float(i[1]) * 1024 * 1024 * 1024 * 1024
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
                DUS.append(str(items).replace('{', '').replace('}', '').replace('[', '').replace(']', ''))
            else:
                DUS.append(data['disk_used_space'][c].replace('{"', '').replace('"}', '').replace('{', '').replace('}', ''))

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


            if not data['online'][c].startswith('[current') and not data['online'][c].startswith(
                    'TSE-Error') and not data['online'][c].startswith('Unknown'):
                OL = data['online'][c]
            else:
                OL = 'unconfirmed'


            if not data['tanium_client_subnet'][c].startswith('[current') and not data['tanium_client_subnet'][c].startswith(
                    'TSE-Error') and not data['tanium_client_subnet'][c].startswith('Unknown'):
                TCS = data['tanium_client_subnet'][c]
            else:
                TCS = 'unconfirmed'



            NS= []
            if data['nvidia_smi'][c].startswith('{"[current') or data['nvidia_smi'][c].startswith(
                    '{"TSE-Error') or data['nvidia_smi'][c].startswith('{"[Unknown') or data['nvidia_smi'][c] == ' ':
                NS = 'unconfirmed'
            elif data['nvidia_smi'][c].startswith('{"[no result'):
                NS = 'no results'
            else:
                NS_item = []
                NS_count = 0
                NS_list = data['nvidia_smi'][c].split(',')
                NS_count = NS_list[0].split(': ')[1].replace('"', '')
                NS_item = NS_list[1].split(': ')[1].replace('"}', '')

                NS = [NS_count, NS_item]


            DL.append([CID, CNM, LR, DTS, DUS, OP, OS, IV, CT, IPV, LPC, YLPC, EPC, YEPC, RUS, RTS, IANM, RS, CPUC, OL, TCS, MF, SIP, NS, CDS])
        elif dataType == 'minutely_asset':
            DL.append([CID, IANM, MF, RS, SIP])
    return DL




