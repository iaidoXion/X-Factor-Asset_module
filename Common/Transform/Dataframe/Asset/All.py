import pandas as pd
from tqdm import tqdm
import json
import logging
from ast import literal_eval
def plug_in(data, inputPlugin, project) :
    with open("setting.json", encoding="UTF-8") as f:
        SETTING = json.loads(f.read())
    PROGRESS = SETTING['PROJECT']['PROGRESSBAR'].lower()
    if project == 'DSB' :
        DFC = [
            'computer_id', 'computer_name', 'last_reboot', 'disk_total_space', 'disk_used_space', 'os_platform',
            'operating_system', 'is_virtual', 'chassis_type', 'ipv_address', 'listen_port_count',
            'established_port_count', 'ram_use_size', 'ram_total_size', 'installed_applications_name',
            'installed_applications_version', 'installed_applications_silent_uninstall_string',
            'installed_applications_uninstallable', 'running_processes', 'running_service', 'cup_consumption',
            'cup_details_system_type', 'cup_details_cup', 'cup_details_cup_speed',
            'cup_details_total_physical_processors', 'cup_details_total_cores',
            'cup_details_total_logical_processors',
            'disk_free_space', 'high_cup_processes', 'high_memory_processes', 'high_uptime', 'ip_address',
            'tanium_client_nat_ip_address', 'last_logged_in_user', 'listen_ports_process', 'listen_ports_name',
            'listen_ports_local_port', 'last_system_crash', 'mac_address', 'memory_consumption', 'open_port',
            'open_share_details_name', 'open_share_details_path', 'open_share_details_status',
            'open_share_details_type', 'open_share_details_permissions', 'primary_owner_name', 'uptime',
            'usb_write_protected', 'user_accounts', 'ad_query_last_logged_in_user_date',
            'ad_query_last_logged_in_user_name', 'ad_query_last_logged_in_user_time',
            'tanium_client_subnet','manufacturer', 'sessionIp', 'nvidiaSmi', 'online'
        ]
        DFL = []

        if PROGRESS == 'true' :
            DATA_list = tqdm(enumerate(data),
                            total=len(data),
                            desc='CM_TRF_DF_AS_ALL_{}'.format(inputPlugin))
        else :
            DATA_list = enumerate(data)

        for index, d in DATA_list :
            if inputPlugin == 'API':
                CI = d[0][0]['text']
                CN = d[1][0]['text']
                LR = d[2][0]['text']
                DTS = []
                for DTSD in d[3]:
                    DTS.append(DTSD['text'])
                DUS = []
                for DUSD in d[4]:
                    DUS.append(DUSD['text'])
                OP = d[5][0]['text']
                OS = d[6][0]['text']
                IV = d[7][0]['text']
                CT = d[8][0]['text']
                IP = d[9][0]['text']
                LPC = d[10][0]['text']
                EPC = d[11][0]['text']
                RUS = d[12][0]['text']
                RTS = d[13][0]['text']
                IA = []
                for IAD in d[14]:
                    IA.append(IAD['text'])
                IAV = []
                for IAVD in d[15]:
                    IAV.append(IAVD['text'])
                IASUS = []
                for IASUSD in d[16]:
                    IASUS.append(IASUSD['text'])
                IAU = []
                for IAUD in d[17]:
                    IAU.append(IAUD['text'])
                RP = []
                for RPD in d[18]:
                    RP.append(RPD['text'])
                RS = []
                for RSD in d[19]:
                    RS.append(RSD['text'])
                CPUC = d[20][0]['text']
                CPUDST = d[21][0]['text']
                CPUDCPU = d[22][0]['text']
                CPUDCPUS = d[23][0]['text']
                CPUDTPP = d[24][0]['text']
                CPUDTC = d[25][0]['text']
                CPUDTLP = d[26][0]['text']
                DFS = []
                for DFSD in d[27]:
                    DFS.append(DFSD['text'])
                HCPUP = []
                for HCPUPD in d[28]:
                    HCPUP.append(HCPUPD['text'])
                HMP = []
                for HMPD in d[29]:
                    HMP.append(HMPD['text'])
                HU = []
                for HUD in d[30]:
                    HU.append(HUD['text'])
                IPA = []
                for IPAD in d[31]:
                    IPA.append(IPAD['text'])
                TCNATIPA = d[32][0]['text']
                LLIU = d[33][0]['text']
                LPP = []
                for LPPD in d[34]:
                    LPP.append(LPPD['text'])
                LPN = []
                for LPND in d[35]:
                    LPN.append(LPND['text'])
                LPLP = []
                for LPLPD in d[36]:
                    LPLP.append(LPLPD['text'])
                LSC = d[37][0]['text']
                MACA = []
                for MACAD in d[38]:
                    MACA.append(MACAD['text'])
                MC = d[39][0]['text']
                openPort = []
                for op in d[40]:
                    openPort.append(op['text'])
                OSDN = []
                for OSDND in d[41]:
                    OSDN.append(OSDND['text'])
                OSDPath = []
                for OSDPathD in d[42]:
                    OSDPath.append(OSDPathD['text'])
                OSDS = []
                for OSDSD in d[43]:
                    OSDS.append(OSDSD['text'])
                OSDT = []
                for OSDTD in d[44]:
                    OSDT.append(OSDTD['text'])
                OSDP = []
                for OSDPD in d[45]:
                    OSDP.append(OSDPD['text'])
                PON = d[46][0]['text']
                Uptime = d[47][0]['text']
                USBWP = d[48][0]['text']
                UA = []
                for UAD in d[49]:
                    UA.append(UAD['text'])
                ADQLLIUD = d[50][0]['text']
                ADQLLIUN = d[51][0]['text']
                ADQLLIUT = d[52][0]['text']
                TCS = d[53][0]['text']
                manufacturer = d[54][0]['text']
                SI =[]
                for SID in d[55] :
                    SI.append(SID['text'])
                nvidiaSmi = []
                for NS in d[56] :
                    nvidiaSmi.append(NS['text'])
                OL = d[57][0]['text']


            if inputPlugin == 'DB':
                CI = d[0]
                CN = d[1]
                LR = d[2]
                DTS = d[3]
                DUS = d[4]
                OP = d[5]
                OS = d[6]
                IV = d[7]
                CT = d[8]
                IP = d[9]
                LPC = d[10]
                EPC = d[11]
                RUS = d[12]
                RTS = d[13]
                IA = d[14]
                IAV = d[15]
                IASUS = d[16]
                IAU = d[17]
                RP = d[18]
                RS = d[19]
                CPUC = d[20]
                CPUDST = d[21]
                CPUDCPU = d[22]
                CPUDCPUS = d[23]
                CPUDTPP = d[24]
                CPUDTC = d[25]
                CPUDTLP = d[26]
                DFS = d[27]
                HCPUP = d[28]
                HMP = d[29]
                HU = d[30]
                IPA = d[31] # 확인
                TCNATIPA = d[32]
                LLIU = d[33]
                LPP = d[34]
                LPN = d[35]
                LPLP = d[36]
                LSC = d[37]
                MACA = d[38]
                MC = d[39]
                openPort = d[40]
                OSDN = d[41]
                OSDPath = d[42]
                OSDS = d[43]
                OSDT = d[44]
                OSDP = d[45]
                PON = d[46]
                Uptime = d[47]
                USBWP = d[48]
                UA = d[49]
                ADQLLIUD = d[50]
                ADQLLIUN = d[51]
                ADQLLIUT = d[52]
                TCS = d[53]
                manufacturer = d[54]
                SI = d[55]
                nvidiaSmi = d[56]
                OL = d[57]

            DFL.append(
                [CI, CN, LR, DTS, DUS, OP, OS, IV, CT, IP, LPC, EPC, RUS, RTS, IA, IAV, IASUS, IAU, RP, RS, CPUC,
                    CPUDST, CPUDCPU, CPUDCPUS, CPUDTPP, CPUDTC, CPUDTLP, DFS, HCPUP, HMP, HU, IPA, TCNATIPA, LLIU,
                    LPP, LPN, LPLP, LSC, MACA, MC, openPort, OSDN, OSDPath, OSDS, OSDT, OSDP, PON, Uptime, USBWP,
                    UA, ADQLLIUD, ADQLLIUN, ADQLLIUT, TCS, manufacturer, SI, nvidiaSmi, OL])
    
    # elif project == 'VUL' :
    #     DFL = []
    #     DFC = ['SW1', 'SW2', 'SW2_2', 'SW2-3', 'SW2_4', 'SW3', 'SW4', 'SW4_2']
    #     chekc_swv = [20, 42, 9, 24]
        
    #     data = data['data']['result_sets'][0]['rows']
    #     for i in range(len(data)) :
    #         dict = {}
    #         dict_list = []
    #         list_dict = {}
    #         count = 0
    #         SWV_list = []
    #         for index, cdata in enumerate(chekc_swv) :
    #             for j in range(cdata) :
    #                 SWV_list.append('SW' + str(index+1) + '-' + str(j + 1).zfill(2))
    #         for k in range(len(DFC)) :
    #             count = count + 1
    #             for j in data[i]['data'][k] :
    #                 if j['text'] == 'TSE-Error: No Sensor Definition for this Platform':
    #                     logging.info('ComputerID :  {} has {}'.format(data[i]['cid'], j['text']))
    #                     continue
    #                 if j['text'] == 'TSE-Error: Python is disabled':
    #                     logging.info('ComputerID :  {} has {}'.format(data[i]['cid'], j['text']))
    #                     continue
    #                 elif j['text'] == '[current result unavailable]' :
    #                     logging.info('ComputerID :  {} has {}'.format(data[i]['cid'], j['text']))
    #                     continue
    #                 elif j['text'] == 'TSE-Error: Python is not available on this system.':
    #                     logging.info('ComputerID :  {} has {}'.format(data[i]['cid'], j['text']))
    #                     continue
    #                 else :
    #                     if j['text'] == 'TSE-Error: Failed to send to sensor child process: broken pipe' :
    #                         dict['SWV'] = DFC[k]
    #                         dict['value'] = j['text']
    #                         j['text'] = dict
    #                     else :
    #                         if not j['text'].startswith('{') and not j['text'].endswith('}'):
    #                             j['text'] = '{' + j['text'] + '}'
    #                         dict = literal_eval(j['text'])
    #                         SWV_list.remove(dict['SWV'])
    #                     dict_list.append(dict)
    #             if len(DFC) == count : 
    #                 if not len(SWV_list) == 0 :
    #                     for index in SWV_list :
    #                         sub_dict = {}
    #                         sub_dict['SWV'] = index
    #                         sub_dict['status'] = 'None'
    #                         sub_dict['value'] = 'None'
    #                         dict_list.append(sub_dict)
    #             if len(dict_list) != 0 :
    #                 list_dict['list'] = dict_list
    #                 list_dict['cid'] = data[i]['data'][8][0]['text'] #computer_id
    #                 list_dict['cpn'] = data[i]['data'][9][0]['text'] #computer_name
    #                 list_dict['os'] = data[i]['data'][10][0]['text'] #Operating System
    #                 list_dict['ip'] = data[i]['data'][11][0]['text'] #Tanium Client NAT IP Address
    #                 list_dict['ct'] = data[i]['data'][12][0]['text'] #Chassis Type
    #                 list_dict['lr'] = data[i]['data'][13][0]['text'] #Last Reboot
    #                 list_dict['online'] = data[i]['data'][14][0]['text']
    #         if len(list_dict) != 0 :
    #             dataList.append(list_dict)
    #     for i in range(len(dataList)) :
    #         dataList[i]['list'] = sorted(dataList[i]['list'], key= lambda x: x['SWV'])
    #     returnList = {'resCode': resCode, 'dataList': dataList}
    #     logging.info('Tanium ' + APITYPE + ' Data API Call Success')
    #     logging.info('Tanium ' + APITYPE + ' Data API Response Code : ' + str(resCode))
    DF = pd.DataFrame(DFL, columns=DFC)
    return DF