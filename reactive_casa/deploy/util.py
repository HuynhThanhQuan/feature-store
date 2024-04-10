import pandas as pd
import os

from database_connector import oraDB

import logging
logger = logging.getLogger(__name__) 


engine, connection = oraDB.connect_CINS_SMY()


def download_or_reload(saved_fn, query, reload_local_file=True):
    if (not os.path.exists(saved_fn)) or (not reload_local_file):
        return download_to_parquet(saved_fn, query)
    else:
        return reload(saved_fn)

    
def download_to_parquet(saved_fn, query):
    df = load_sql_to_dataframe(query)
    if len(df)>0:
        df.to_parquet(saved_fn)
    return df


def load_sql_to_dataframe(query):
    df = pd.read_sql(query, engine)
    df.columns = [i.upper() for i in df.columns]
    logger.debug(f'loaded sql - len {len(df)}')
    return df


def reload(saved_fn):
    df = pd.read_parquet(saved_fn)
    logger.debug(f'{saved_fn}: reloaded - len {len(df)}')
    return df