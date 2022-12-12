import pandas as pd
def plug_in(data, classification, itemType) :
    DL = []
    RD = []
    CNM = itemType
    for c in range(len(data.computer_id)):
        if classification == 'os' :
            DL.append(data.os_platform[c])
        elif classification== 'virtual' :
            DL.append(data.is_virtual[c])
        elif classification == 'asset' :
            DL.append(data.chassis_type[c])
        elif classification == 'installed_applications' :
            for d in data.installed_applications_name[c].replace('"', '').replace('{', '').replace('}', '').split(',') :
                DL.append(d)
        elif classification == 'listen_port_count_change' :
            DL.append(data.listenPortCountChange[c])
        elif classification == 'established_port_count_change' :
            DL.append(data.establishedPortCountChange[c])
        elif classification == 'running_processes' :
            for d in data.running_processes[c] :
                DL.append(d)

    DF = pd.DataFrame(DL, columns=[CNM])
    DFG = DF.groupby([CNM]).size().reset_index(name='counts')
    DFGS = DFG.sort_values(by="counts", ascending=False)

    if classification == 'os' :
        statistics_unique = classification + '_' + DFGS.OP
        item = DFGS.OP
    elif classification == 'virtual':
        statistics_unique = classification + '_' + DFG.IV
        item = DFGS.IV
    elif classification == 'asset':
        statistics_unique = classification+'_'+DFGS.CT
        item = DFGS.CT
    elif classification == 'installed_applications':
        statistics_unique = classification+'_'+DFGS.IANM
        item = DFGS.IANM
    elif classification == 'listen_port_count_change':
        statistics_unique = classification+'_'+ DFGS.LPC
        item = DFGS.LPC
    elif classification == 'established_port_count_change':
        statistics_unique = classification + '_' + DFGS.EPC
        item = DFGS.EPC
    elif classification == 'running_processes':
        statistics_unique = classification + '_' + DFGS.RPNM
        item = DFGS.RPNM
    item_count = DFG.counts

    for DFC in range(len(DFGS)) :
        RD.append([statistics_unique[DFC], classification, item[DFC], item_count[DFC]])
    return RD

