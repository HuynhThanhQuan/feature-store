import os
from oraDB import oraDB
import cx_Oracle
import logging
import datetime
import util

logger = logging.getLogger(__name__)
SQL_TABLE_FOLDER = './sql/table'


def read_sql_and_execute(table_name, folder_path):
    conn, cur= oraDB.connect()
    try:
        k_filepath = os.path.join(folder_path, table_name + '.sql')
        with open(k_filepath, 'r') as f:
            k_query = f.read()
        # logging.debug(k_query)
        cur.execute(k_query)
        cur.execute('COMMIT')
        logger.info(f'Succeed to execute {table_name}')
    except cx_Oracle.DatabaseError as e:
        logger.error(e)
        logger.error(f'Failed to execute {table_name}')
    except Exception as er:
        logger.error(er)
        logger.error(f'Failed to execute {table_name}')
    return

@util.timeit
def create_empty_tmp_tables(response):
    config = response['config']
    RPT_DT = response['RPT_DT']
    RPT_DT_TBL = response['RPT_DT_TBL']
    table_check = response['TABLE_CHECK']
    folder_path = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT, 'create')
    
    logger.info('Create empty TMP tables')
    
    # Generate prerequisite tables include CINS_TMP_CUSTOMER, CINS_TMP_CARD_DIM
    tmp_customer = f'CINS_TMP_CUSTOMER_{RPT_DT_TBL}'
    if not table_check[tmp_customer]:
        read_sql_and_execute(tmp_customer, folder_path)
    if tmp_customer in table_check.keys():
        del table_check[tmp_customer]
    tmp_card_dim = f'CINS_TMP_CARD_DIM_{RPT_DT_TBL}'
    if not table_check[tmp_card_dim]:
        read_sql_and_execute(tmp_card_dim, folder_path)
    if tmp_card_dim in table_check.keys():
        del table_check[tmp_card_dim]
    # Run the rest
    for k, v in table_check.items():
        if not v:
            read_sql_and_execute(k, folder_path)
    return {}

@util.timeit
def insert_into_tmp_tables(response):
    config = response['config']
    RPT_DT = response['RPT_DT']
    RPT_DT_TBL = response['RPT_DT_TBL']
    table_check = response['TABLE_CHECK']
    folder_path = os.path.join(SQL_TABLE_FOLDER, 'report_date', RPT_DT, 'insert')
    
    logger.info('Insert into TMP tables')
    
    # Generate prerequisite tables include CINS_TMP_CUSTOMER, CINS_TMP_CARD_DIM
    tmp_customer = f'CINS_TMP_CUSTOMER_{RPT_DT_TBL}'
    read_sql_and_execute(tmp_customer, folder_path)
    if tmp_customer in table_check.keys():
        del table_check[tmp_customer]
    tmp_card_dim = f'CINS_TMP_CARD_DIM_{RPT_DT_TBL}'
    read_sql_and_execute(tmp_card_dim, folder_path)
    if tmp_card_dim in table_check.keys():
        del table_check[tmp_card_dim]
    # Run the rest
    for k, v in table_check.items():
        read_sql_and_execute(k, folder_path)
    return {}