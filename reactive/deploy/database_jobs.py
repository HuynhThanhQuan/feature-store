import os
import numpy as np
import pandas as pd

from sqlalchemy import create_engine

import sql_template
from database_connector import oraDB

import logging
logger = logging.getLogger(__name__) 


engine, connection = oraDB.connect_CINS_SMY()


def push_score_to_DW(config, report_date):
    score_fn = f'./out/SCORE_{report_date}'
    if os.path.exists(score_fn):
        score_df = pd.read_parquet(score_fn)
        total_rows = len(score_df)
        report_date = report_date.replace('-','')

        # Try to create table first
        try:
            connection.execute(sql_template.QUERY_CREATE_CUSTOMER_SCORE_TABLE % report_date)
            logger.info(f'Create score table of {report_date}')
        except:
            logger.warn('Table already exists - drop and re-insert')
            connection.execute(sql_template.QUERY_DROP_CUSTOMER_SCORE_TABLE % report_date)
            logger.warn('Table is dropped')
            connection.execute(sql_template.QUERY_CREATE_CUSTOMER_SCORE_TABLE % report_date)
            logger.warn('Table is re-created')
            
        # Inserting data 
        insert_query = sql_template.QUERY_INSERT_CUSTOMER_SCORE % report_date
        iterate_push_batch_data(score_df, insert_query, config)
        logger.info(f'Finished push score data to DW {report_date}')
    else:
        logger.error(f'Error when {score_fn} not exist. Exit')
        
        
def iterate_push_batch_data(data, query, config):
    total_rows = len(data)
    logging.info(f'Total rows: {total_rows}')
    
    num_batches = int(np.ceil(total_rows/config['batch_size']))
    logging.info(f'Batch size/Total batches: {config['batch_size']} / {num_batches}')
    
    # Insert values into table batch-by-batch
    for i in range(num_batches):
        start = i * config['batch_size']
        end = (i + 1) * config['batch_size']
        if end >= total_rows:
            end = total_rows - 1
        logger.debug(f'Insert batch data {i+1}/{num_batches} - data points from {start} to {end}')
        batch_data = data.iloc[start:end].to_numpy()
        batch_data = np.where(pd.isnull(batch_data), None, batch_data)
        batch_data = [tuple(row) for row in batch_data]
        connection.executemany(query, batch_data)
        engine.commit()
    
        
def push_raw_matrix_data_to_DW(data, config, dt):
    assert isinstance(data, pd.DataFrame), "data is not a Data Frame"
    assert data.index.name == 'CUSTOMER_CDE', "data index name is not CUSTOMER_CDE"
    
    data = data.reset_index() # get CUSTOMER_CDE from index
    rtp_dt_tbl = dt.replace('-','')
    
    # Try to create table first
    try:
        connection.execute(sql_template.QUERY_CREATE_RAW_MATRIX_DATA_TABLE % rtp_dt_tbl)
        logger.info(f'Create raw-matrix data table of {rtp_dt_tbl}')
    except:
        logger.warn('Table already exists - drop and re-insert')
        connection.execute(sql_template.QUERY_DROP_RAW_MATRIX_DATA_TABLE % rtp_dt_tbl)
        logger.warn('Table is dropped')
        connection.execute(sql_template.QUERY_CREATE_RAW_MATRIX_DATA_TABLE % rtp_dt_tbl)
        logger.warn('Table is re-created')
        
    # Inserting data
    insert_query = sql_template.QUERY_INSERT_RAW_MATRIX_DATA_TABLE % rtp_dt_tbl
    iterate_push_batch_data(data, insert_query, config)
    logger.info(f'Finished push raw matrix data to DW {rtp_dt_tbl}')
