import pandas as pd
def plug_in(data, classification, type) :
    DL = []
    for c in range(len(data.computer_id)):
        CNM = type
        if classification == 'os' :
            DL.append((data.os_platform[c]))
        elif classification== 'virtual' :
            DL.append((data.is_virtual[c]))
        elif classification == 'asset' :
                DL.append((data.chassis_type[c]))
        elif classification == 'installed_applications' :
            for d in data.installed_applications_name[c]:
                DL.append(d)

    DF = pd.DataFrame(DL, columns=[CNM])
    DFG = DF.groupby([CNM]).size().reset_index(name='counts')
    #DFGS = DFG.sort_values(by="counts", ascending=False)

    if classification == 'os' :
        statistics_unique = classification + '_' + DFG.OP
        item = DFG.OP
    elif classification == 'virtual':
        statistics_unique = classification + '_' + DFG.IV
        item = DFG.IV
    elif classification == 'asset':
        statistics_unique = classification+'_'+DFG.CT
        item = DFG.CT
    elif classification == 'installed_applications':
        statistics_unique = classification+'_'+DFG.IANM
        item = DFG.IANM
    item_count = DFG.counts
    #for DFC in range(len(DFG)) :
        #print({"minutely_statistics_unique" : statistics_unique[DFC], "classification" : classification, "item" : item[DFC], "count" : item_count[DFC]})


    #print(statistics_unique)
    #print(classification)
    #print(item)
    #print(item_count)
    #os_Windows,

    RD = []


    #for

    #if item == 'asset' :
        #statistics_unique = item + '_' + DFGS.CT

    #if item == 'asset':
        #print(statistics_unique)


        #print(IANDL['name'][0])