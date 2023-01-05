
def plug_in(data) :
    DL = []
    for c in range(len(data.computer_id)):
        if data.disk_used_space[c][0].isdigit() and data.disk_total_space[c][0].isdigit() :
            driveUsage = int(data.disk_used_space[c][0]) / int(data.disk_total_space[c][0]) * 100
        else :
            driveUsage = 'unconfirmed'

        if data.ram_use_size[c].isdigit() and data.ram_total_size[c].isdigit() :
            ramUsage = int(data.ram_use_size[c]) / int(data.ram_total_size[c]) * 100
        else :
            ramUsage = 'unconfirmed'

        if type(data.cup_consumption[c]) == float :
            cpuUsage = data.cup_consumption[c]
        else :
            cpuUsage = 'unconfirmed'





        DL.append([data.computer_id[c], data.computer_name[c], data.ipv_address[c], data.chassis_type[c],
                   data.os_platform[c], data.operating_system[c], data.is_virtual[c], str(data.last_reboot[c]), str(driveUsage),
                   str(ramUsage), str(cpuUsage)])
    return DL



    #'computer_id', 'last_reboot', 'disk_total_space', 'disk_used_space',
    #'os_platform', 'is_virtual', 'chassis_type',
    #'ipv_address', 'today_listen_port_count', 'yesterday_listen_port_count',
    #'today_established_port_count', 'yesterday_established_port_count',
    #'ram_use_size', 'ram_total_size', 'installed_applications_name',
    #'running_processes', 'cup_consumption'