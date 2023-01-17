

def plug_in(data):
    DL = []
    for c in range(len(data.computer_id)):
        DL.append([data.computer_id[c], data.computer_name[c], data.ipv_address[c], data.chassis_type[c], data.os_platform[c], data.operating_system[c], data.is_virtual[c], str(data.last_reboot[c]), data.tanium_client_subnet[c], data.manufacturer[c], data.nvidia_smi[c],
                    data.ram_use_size[c], data.ram_total_size[c], data.cup_details_cup_speed[c], data.disk_used_space[c], data.disk_total_space[c]])
    return DL

