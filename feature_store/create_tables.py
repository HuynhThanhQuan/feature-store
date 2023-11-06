import pandas as pd
import numpy as np
from oraDB import oraDB
import os
import sys

TABLE_FOLDER = './tables'
RPT_DT = '11-06-2023'
RPT_DT_TBL = RPT_DT.replace('-', '')


def read_sql_placeholder(sql_filepath):
    with open(sql_filepath,'r') as f:
        content = f.read()
    contents = content.split(';')
    contents = [c.strip() for c in contents]
    return contents

def execute_query(sql_filepath):
    contents = read_sql_placeholder(sql_filepath)
    for content in contents:
        try:
            # print(content)
            conn,cur= oraDB.connect()
            cur.execute(content)
        except Exception as e:
            print(e)
            cur.close()


# Job create CINS_CUSTOMER table
def create_CINS_CUSTOMER():
    tablename_ver = f'CINS_CUSTOMER_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver}-----')
    
# Job create CINS_CARD_DIM table
def create_CINS_CARD_DIM():
    tablename_ver = f'CINS_CARD_DIM_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')

# Job create CINS_CUSTOMER_STATUS table
def create_CINS_CUSTOMER_STATUS():
    tablename_ver = f'CINS_CUSTOMER_STATUS_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')

# Job create CINS_EB_MB_CROSSELL
def create_CINS_EB_MB_CROSSELL():
    tablename_ver = f'CINS_EB_MB_CROSSELL_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')

# Job create CINS_CREDIT_CARD_TRANSACTION
def create_CREDIT_CARD_TRANSACTION():
    tablename_ver = f'CINS_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')
    
# Job create CINS_DATA_RPT_CARD
def create_CINS_DATA_RPT_CARD():
    tablename_ver = f'CINS_DATA_RPT_CARD_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')
    
# Job create CINS_DATA_RPT_LOAN
def create_CINS_DATA_RPT_LOAN():
    tablename_ver = f'CINS_DATA_RPT_LOAN_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')

# Job create CINS_POS_MERCHANT_AMT_6M
def create_CINS_POS_MERCHANT_AMT_6M():
    tablename_ver = f'CINS_POS_MERCHANT_AMT_6M_CREATE_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')
    tablename_ver = f'CINS_POS_MERCHANT_AMT_6M_INSERT_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')
    
# Job create CINS_POS_MERCHANT_6M
def create_CINS_POS_MERCHANT_6M():
    tablename_ver = f'CINS_POS_MERCHANT_6M_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')
    
# Job create CINS_POS_TERMINAL_AMT_6M
def create_CINS_POS_TERMINAL_AMT_6M():
    tablename_ver = f'CINS_POS_TERMINAL_AMT_6M_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')
    
# Job create CINS_POS_TERMINAL_6M
def create_CINS_POS_TERMINAL_6M():
    tablename_ver = f'CINS_POS_TERMINAL_6M_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')
    
# Job create CINS_CARD_CREDIT_LOAN_6M
def create_CINS_CARD_CREDIT_LOAN_6M():
    tablename_ver = f'CINS_CARD_CREDIT_LOAN_6M_{RPT_DT_TBL}'
    sql_filepath = os.path.join(TABLE_FOLDER, tablename_ver + '.sql')
    execute_query(sql_filepath)
    print(f'-----Finished creating table {tablename_ver} -----')

    
if __name__ == '__main__':
    # create_CINS_CUSTOMER()
    # create_CINS_CARD_DIM()
    # create_CINS_CUSTOMER_STATUS()
    # create_CINS_EB_MB_CROSSELL()
    # create_CREDIT_CARD_TRANSACTION()
    # create_CINS_DATA_RPT_CARD()
    # create_CINS_DATA_RPT_LOAN()
    create_CINS_POS_MERCHANT_AMT_6M()
    # create_CINS_POS_MERCHANT_6M()
    # create_CINS_POS_TERMINAL_AMT_6M()
    # create_CINS_POS_TERMINAL_6M()
    # create_CINS_CARD_CREDIT_LOAN_6M()