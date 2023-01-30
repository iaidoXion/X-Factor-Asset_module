import pandas as pd
import logging

def plug_in(leftDF, rightDF) :
    try:
        RD = pd.merge(left=leftDF, right=rightDF, how="outer", on=['computer_id'])
        logging.info('Merge.py - 성공')
        return RD
    except Exception as e:
        logging.warning('Merge.py - Error 발생')
        logging.warning('Error : ' + e)