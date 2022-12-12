import pandas as pd

def plug_in(leftDF, rightDF) :
    RD = pd.merge(left=leftDF, right=rightDF, how="outer", on=['computer_id'])

    return RD