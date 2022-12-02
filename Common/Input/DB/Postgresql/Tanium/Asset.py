import psycopg2
from datetime import datetime, timedelta
def plug_in(period, type) :
    try :
        yesterday = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
        DBHost = 'localhost'
        DBPort = '5432'
        DBName = 'postgres'
        DBUser = 'aidoi'
        DBPwd = 'iaido8906#'
        TNM = 'minutely_asset'
        DL = []
        selectConn = psycopg2.connect('host={0} port={1} dbname={2} user={3} password={4}'.format(DBHost, DBPort, DBName, DBUser, DBPwd))
        selectCur = selectConn.cursor()
        if period == 'minutely' :
            if type == 'minutely_asset_all' :
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
                        ad_query_last_logged_in_user_time, asset_collection_date
                    from  
                        """ + TNM + """
                """
            elif type == 'chassis_type' :
                SQ = """
                    select 
                        chassis_type, count(computer_id)
                    from 
                        minutely_asset  
                    group by chassis_type
                """
            elif type == 'os_platform' :
                SQ = """
                    select 
                        os_platform, count(computer_id)
                    from 
                        minutely_asset 
                    group by os_platform
                """
            elif type == 'installed_applications' :
                SQ = """
                    select 
                        installed_applications_name
                    from 
                        minutely_asset;
                """

            elif type == 'minutely_daily_asset' :
                SQ = """
                    select
                        ma.computer_id as today_computer_id,
                        ma.last_reboot as today_last_reboot,
                        ma.disk_used_space as today_disk_used_space,
                        da.disk_used_space as yesterday_disk_used_space,
                        ma.os_platform, 
                        ma.is_virtual, 
                        ma.chassis_type,
                        ma.ipv_address as today_ipv_address,
                        ma.listen_port_count as today_listen_port_count,
                        da.listen_port_count as yesterday_listen_port_count,
                        ma.established_port_count as today_established_port_count,
                        da.established_port_count as yesterday_established_port_count,
                        ma.ram_use_size as today_ram_use_size,
                        ma.ram_total_size as today_ram_total_size, 
                        ma.installed_applications_name as today_installed_applications_name, 
                        ma.running_processes as today_running_processes, 
                        ma.cup_consumption as today_cup_consumption
                    from
                        (select 
                            computer_id, 
                            last_reboot, 
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
                            cup_consumption  
                        from 
                            minutely_asset) as ma
                    LEFT JOIN 
                        (select 
                            computer_id, 
                            disk_used_space, 
                            listen_port_count, 
                            established_port_count
                        from 
                            daily_asset 
                        where 
                            to_char(asset_collection_date , 'YYYY-MM-DD') = '"""+yesterday+"""') as da
                    ON ma.computer_id = da.computer_id
                """


        selectCur.execute(SQ)
        selectRS = selectCur.fetchall()
        for RS in selectRS:
            DL.append(RS)
        return DL
    except ConnectionError as e:
        print(e)
