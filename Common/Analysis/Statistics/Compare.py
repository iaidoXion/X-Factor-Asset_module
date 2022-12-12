

def plug_in(data) :
    #print(data)
    DL = []
    for c in range(len(data.computer_id)):
        if data.today_listen_port_count[c].isdigit() and data.yesterday_listen_port_count[c].isdigit():
            if data.today_listen_port_count[c] == data.yesterday_listen_port_count[c]:
                listenPortCountChange = 'No'
            else:
                listenPortCountChange = 'Yes'
        else:
            listenPortCountChange = 'unconfirmed'

        if data.today_established_port_count[c].isdigit() and data.yesterday_established_port_count[c].isdigit():
            if data.today_established_port_count[c] == data.yesterday_established_port_count[c]:
                establishedPortCountChange = 'no'
            else:
                establishedPortCountChange = 'yes'
        else:
            establishedPortCountChange = 'unconfirmed'

        if len(data['running_processes'][c]) > 1:
            runningProcessesCount = len(data['running_processes'][c])
        else:
            if not data['running_processes'][c][0].startswith('[current') and not data['running_processes'][c][0].startswith('TSE-Error') and not data['running_processes'][c][0].startswith('Unknown'):
                runningProcessesCount = 'unconfirmed'
            else:
                runningProcessesCount = 1

        if data['online'][c] == 'True':
            online = 'yes'
        else:
            online = 'unconfirmed'
        DL.append([data.computer_id[c], listenPortCountChange, establishedPortCountChange, str(runningProcessesCount), str(online)])


    return DL