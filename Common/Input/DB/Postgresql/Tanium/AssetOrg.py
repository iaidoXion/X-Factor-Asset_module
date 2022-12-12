import psycopg2
from datetime import datetime, timedelta
import json

def plug_in(dataType) :
#def plug_in(period, dataType) :
    try :
        with open("setting.json", encoding="UTF-8") as f:
            SETTING = json.loads(f.read())
        DBHOST = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['HOST']
        DBPORT = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['PORT']
        DBNM = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['NAME']
        DBUNM = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['USER']
        DBPWD = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['PWD']
        MAT = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['TNM']['MA']
        DAT = SETTING['PLUGIN']['Tanium']['INPUT']['DB']['PS']['TNM']['DA']
        COLLECTIONTYPE = SETTING['PLUGIN']['Tanium']['COLLECTIONTYPE']
        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        nowTime = datetime.today().strftime("%Y-%m-%d %H:%M")
        DL = []
        selectConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHOST, DBPORT, DBNM, DBUNM, DBPWD))
        selectCur = selectConn.cursor()
        #if period == 'minutely' :
        if dataType == 'minutely_asset_all':
            if COLLECTIONTYPE == 'online' :
                SQ = """
                    select 
                        computer_id, computer_name, last_reboot, disk_total_space, disk_used_space, os_platform, 
                        operating_system, is_virtual, chassis_type, ipv_address, listen_port_count, 
                        established_port_count, ram_use_size, ram_total_size, installed_applications_name, 
                        installed_applications_version, installed_applications_silent_uninstall_string, 
                        installed_applications_uninstallable, running_processes, running_service, cup_consumption, 
                        cup_details_system_type, cup_details_cup, cup_details_cup_speed, 
                        cup_details_total_physical_processors, cup_details_total_cores, 
                        cup_details_total_logical_processors, 
                        disk_free_space, high_cup_processes, 
                        high_memory_processes, high_uptime, ip_address, tanium_client_nat_ip_address, 
                        last_logged_in_user, listen_ports_process, listen_ports_name, listen_ports_local_port, 
                        last_system_crash, mac_address, memory_consumption, open_port, open_share_details_name, 
                        open_share_details_path, open_share_details_status, open_share_details_type, 
                        open_share_details_permissions, primary_owner_name, uptime, usb_write_protected, user_accounts, 
                        ad_query_last_logged_in_user_date, ad_query_last_logged_in_user_name, 
                        ad_query_last_logged_in_user_time, tanium_client_subnet, manufacturer, session_ip,
                        nvidia_smi, online, asset_collection_date
                    from  
                        """ + MAT + """ 
                    where 
                        to_char(asset_collection_date , 'YYYY-MM-DD HH24:MI') = '"""+nowTime+"""'"""
            else :
                SQ = """
                    select 
                        computer_id, computer_name, last_reboot, disk_total_space, disk_used_space, os_platform, 
                        operating_system, is_virtual, chassis_type, ipv_address, listen_port_count, 
                        established_port_count, ram_use_size, ram_total_size, installed_applications_name, 
                        installed_applications_version, installed_applications_silent_uninstall_string, 
                        installed_applications_uninstallable, running_processes, running_service, cup_consumption, 
                        cup_details_system_type, cup_details_cup, cup_details_cup_speed, 
                        cup_details_total_physical_processors, cup_details_total_cores, 
                        cup_details_total_logical_processors, 
                        disk_free_space, high_cup_processes, 
                        high_memory_processes, high_uptime, ip_address, tanium_client_nat_ip_address, 
                        last_logged_in_user, listen_ports_process, listen_ports_name, listen_ports_local_port, 
                        last_system_crash, mac_address, memory_consumption, open_port, open_share_details_name, 
                        open_share_details_path, open_share_details_status, open_share_details_type, 
                        open_share_details_permissions, primary_owner_name, uptime, usb_write_protected, user_accounts, 
                        ad_query_last_logged_in_user_date, ad_query_last_logged_in_user_name, 
                        ad_query_last_logged_in_user_time, tanium_client_subnet, manufacturer, session_ip,
                        nvidia_smi, online, asset_collection_date
                    from  
                        """ + MAT

        elif dataType == 'minutely_asset_part':
            if COLLECTIONTYPE == 'online':
                SQ = """
                    select 
                        computer_id, 
                        installed_applications_name, 
                        manufacturer,
                        running_processes
                    from 
                        """ + MAT + """
                    where 
                        to_char(asset_collection_date , 'YYYY-MM-DD HH24:MI') = '"""+nowTime+"""'"""
            else :
                SQ = """
                    select 
                        computer_id, 
                        installed_applications_name, 
                        manufacturer,
                        running_processes
                    from 
                        """ + MAT
        elif dataType == 'minutely_daily_asset':
            if COLLECTIONTYPE == 'online':
                SQ = """
                    select
                        ma.computer_id as computer_id,
                        ma.computer_name as computer_name,
                        ma.last_reboot as last_reboot,
                        ma.disk_total_space as disk_total_space,
                        ma.disk_used_space as disk_used_space,
                        ma.os_platform as os_platform, 
                        ma.is_virtual as is_virtual, 
                        ma.chassis_type as chassis_type,
                        ma.ipv_address as ipv_address,
                        ma.listen_port_count as today_listen_port_count,
                        da.listen_port_count as yesterday_listen_port_count,
                        ma.established_port_count as today_established_port_count,
                        da.established_port_count as yesterday_established_port_count,
                        ma.ram_use_size as ram_use_size,
                        ma.ram_total_size as ram_total_size, 
                        ma.installed_applications_name as installed_applications_name, 
                        ma.running_processes as running_processes, 
                        ma.cup_consumption as cup_consumption,
                        ma.online as online
                    from
                        (select 
                            computer_id, 
                            computer_name,
                            last_reboot, 
                            disk_total_space,
                            disk_used_space, 
                            os_platform, 
                            is_virtual, 
                            chassis_type,
                            ipv_address, 
                            listen_port_count, 
                            established_port_count,
                            ram_use_size, 
                            ram_total_size, 
                            installed_applications_name, 
                            running_processes, 
                            cup_consumption,
                            online  
                        from 
                            """+MAT+"""
                        where
                            to_char(asset_collection_date, 'YYYY-MM-DD HH24:MI') = '"""+nowTime+"""'
                        ) as ma
                    LEFT JOIN 
                        (select 
                            computer_id,
                            listen_port_count, 
                            established_port_count
                        from 
                            """+DAT+""" 
                        where 
                            to_char(asset_collection_date , 'YYYY-MM-DD') = '""" + yesterday + """') as da
                    ON ma.computer_id = da.computer_id
                """
            else :
                SQ = """
                    select
                        ma.computer_id as computer_id,
                        ma.computer_name as computer_name,
                        ma.last_reboot as last_reboot,
                        ma.disk_total_space as disk_total_space,
                        ma.disk_used_space as disk_used_space,
                        ma.os_platform as os_platform, 
                        ma.is_virtual as is_virtual, 
                        ma.chassis_type as chassis_type,
                        ma.ipv_address as ipv_address,
                        ma.listen_port_count as today_listen_port_count,
                        da.listen_port_count as yesterday_listen_port_count,
                        ma.established_port_count as today_established_port_count,
                        da.established_port_count as yesterday_established_port_count,
                        ma.ram_use_size as ram_use_size,
                        ma.ram_total_size as ram_total_size, 
                        ma.installed_applications_name as installed_applications_name, 
                        ma.running_processes as running_processes, 
                        ma.cup_consumption as cup_consumption,
                        ma.online as online
                    from
                        (select 
                            computer_id, 
                            computer_name,
                            last_reboot, 
                            disk_total_space,
                            disk_used_space, 
                            os_platform, 
                            is_virtual, 
                            chassis_type,
                            ipv_address, 
                            listen_port_count, 
                            established_port_count,
                            ram_use_size, 
                            ram_total_size, 
                            installed_applications_name, 
                            running_processes, 
                            cup_consumption,
                            online  
                        from 
                            """ + MAT + """
                        ) as ma
                    LEFT JOIN 
                        (select 
                            computer_id,
                            listen_port_count, 
                            established_port_count
                        from 
                            """ + DAT + """ 
                        where 
                            to_char(asset_collection_date , 'YYYY-MM-DD') = '""" + yesterday + """') as da
                    ON ma.computer_id = da.computer_id
                """

        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        for RS in selectRS:
            DL.append(RS)
        return DL
    except ConnectionError as e:
        print(e)
        #

