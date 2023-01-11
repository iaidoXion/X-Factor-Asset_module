

def plug_in(data):
    DL = []
    for c in range(len(data.computer_id)):
        if len(data['running_service'][c]) > 1:
            running_service_count = len(data['running_service'][c])
        else:
            if data['running_service']== 'unconfirmed' :
                running_service_count = 'unconfirmed'
            else:
                running_service_count = 1

        if len(data['session_ip'][c]) > 1:
            session_ip_count = len(data['session_ip'][c])
        else:
            session_ip_count = 1

        DL.append(
            [data.computer_id[c], str(running_service_count), str(session_ip_count)])
    return DL

