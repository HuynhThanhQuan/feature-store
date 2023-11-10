import time
from functools import wraps
import logging
import os
from oraDB import oraDB
import gen_feature
import re
import yaml

logger = logging.getLogger(__name__)

def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.debug(f'[timeit] Function "{func.__name__}" took "{elapsed_time}" seconds')
        return result
    return wrapper


@timeit
def my_func(n=100):
    for _ in range(n):
        pass
    
def get_numrow_from_insert():
    conn, cur= oraDB.connect()
    
    # Check exisiting table
    query = """SELECT table_name FROM user_tables WHERE table_name = 'CINS_TMP_UTIL_TEST'"""
    cur.execute(query)
    result = cur.fetchone()
    conn.commit()
    if result is not None:
        print('Exist')
        print('Drop table')
        query = """DROP TABLE CINS_TMP_UTIL_TEST"""
        cur.execute(query)
        conn.commit()
    else:
        print('Not exists')
        
    query = """CREATE TABLE CINS_TMP_UTIL_TEST (CUSTOMER_CDE VARCHAR2(25 BYTE))"""
    cur.execute(query)
    conn.commit()
    print('Created')
    
    query = """INSERT INTO CINS_TMP_UTIL_TEST (CUSTOMER_CDE) VALUES (123)"""
    cur.execute(query)
    result = cur.rowcount
    conn.commit()
    if result is not None:
        print(result)


def split_each_feature_into_a_file():
    feature_fp = './sql/feature/placeholder/'
    unstructured_fp = os.path.join(feature_fp, 'unstructured')
    structured_fp = os.path.join(feature_fp, 'structured')
    
    for fp in [unstructured_fp, structured_fp]:
        for file in os.listdir(fp):
            if file.startswith('.'):
                continue
            with open(os.path.join(fp, file), 'r') as f:
                content = f.read()
                features = content.split(';')
                print(f'File {file} has {len(features)} features')
                for idx, feature in enumerate(features):
                    description, feat_nms, derived_tables = gen_feature.read_ft_and_tbl_in_subquery(feature)
                    if feat_nms and len(feat_nms) > 0:
                        feat_nm = feat_nms[0]
                        with open(os.path.join(feature_fp, f'{feat_nm}.sql'), 'w') as f:
                            f.write(feature.strip())
                    print(f'Feature {idx} is {feat_nm}')


def generate_test_scripts():
    tables, features = [], []

    tables = ['CINS_2M_PART', 'CINS_TMP_LST']
    # features = ['CASA_AVG_BAL_1M', 'CASA_CT_ACCT_ACTIVE', 'CASA_CT_TXN_1M', 'CASA_DAY_SINCE_LTST_TXN','CASA_MAX_BAL_1M', 'CASA_MIN_BAL_1M', 'CASA_SUM_TXN_AMT_1M']
    
    # tables = [
    # 'CINS_TMP_CUSTOMER', 'CINS_TMP_CARD_DIM', 'CINS_TMP_CUSTOMER_STATUS', 'CINS_TMP_CREDIT_CARD_LOAN_6M', 'CINS_TMP_CREDIT_CARD_TRANSACTION', 'CINS_TMP_DATA_RPT_CARD', 'CINS_TMP_DATA_RPT_LOAN', 'CINS_TMP_EB_MB_CROSSELL'
    # ]

    # features = [
    # 'REACTIVATED', 'INACTIVE', 'CASA_INACTIVE', 'EB_MBIB_INACTIVE', 'CARD_CREDIT_INACTIVE','EB_SACOMPAY_INACTIVE', 'AGE', 'GEN_GRP', 'PROFESSION', 'LIFE_STG','CASA_AVG_BAL_1M', 'CASA_CT_ACCT_ACTIVE', 'CASA_CT_TXN_1M', 'CASA_DAY_SINCE_LTST_TXN','CASA_MAX_BAL_1M', 'CASA_MIN_BAL_1M', 'CASA_SUM_TXN_AMT_1M'
    # ]

    sel_date = '11-06-2023'
    sel_date_tbl = sel_date.replace('-','')
    output_dev = f'./sql/script/FS_dev_{sel_date_tbl}.sql'
    output_prod = f'./sql/script/FS_prod_{sel_date_tbl}.sql'
    create_tbl_fd = './sql/table/placeholder/create'
    insert_tbl_fd = './sql/table/placeholder/insert'
    feat_official_fd = './sql/feature/placeholder/official'
    tbl_nm = 'CINS_FEATURE_STORE_V2'


    commit_ck = ';\n\n\nCOMMIT;\n\n\n'
    
    scripts = []
    # Read CREATE & INSERT INTO table scripts first
    for t in tables:
        with open(os.path.join(create_tbl_fd, t + '.sql'),'r') as f:
            create_script = f.read().strip()
            if create_script.endswith(';'):
                id = create_script.rfind(';')
                create_script = create_script[:id]
        scripts.append(create_script)
        
    for t in tables:
        with open(os.path.join(insert_tbl_fd, t + '.sql'),'r') as f:
            insert_script = f.read().strip()
            if insert_script.endswith(';'):
                id = insert_script.rfind(';')
                insert_script = insert_script[:id]
        scripts.append(insert_script)

    # Read Feature
    for f in features:
        with open(os.path.join(feat_official_fd, f + '.sql'),'r') as f:
            feat_script = f.read().strip()
            if feat_script.endswith(';'):
                id = feat_script.rfind(';')
                feat_script = feat_script[:id]
        scripts.append(feat_script)
    
    # Aggregated
    final_script = commit_ck.join(scripts)

    final_script += commit_ck

    final_script = final_script.strip()
    

    # Replace TBL_NM, RPT_DT and RPT_DT_TBL
    final_script = final_script.replace('{TBL_NM}', f'{tbl_nm}')
    final_script = final_script.replace('{RPT_DT}', f'{sel_date}')
    final_script = final_script.replace('{RPT_DT_TBL}', f'{sel_date_tbl}')

    # Post-processing
    if not final_script.strip().endswith(';'):
        final_script += ';'
    final_script_prod = final_script.replace('DW_ANALYTICS', 'DWPROD')

    #Output_dev
    with open(output_dev, 'w') as f:
        f.writelines(final_script)
    #Output_prod
    with open(output_prod, 'w') as f:
        f.writelines(final_script_prod)

    print('Done')


def generate_backfill_report(data):
    grouped = {}
    for item in data:
        derived_from = item.get('Derived From', {})
        for table, columns in derived_from.items():
            if table not in grouped:
                grouped[table] = []
            if isinstance(columns, list):
                grouped[table].extend(columns)
            else:
                grouped[table].append(columns)
    for table, columns in grouped.items():
        grouped[table] = list(set(columns))
    return grouped

def get_backfill_info():
    def extract_key_values(query):
        # print(query)
        description = re.findall(r'/\*(.*?)\*/',query, re.DOTALL)
        if description and len(description) > 0:
            description = description[0].strip()
            try:
                data = yaml.safe_load(description)
                return data
            except yaml.YAMLError as exc:
                print(exc)

    path = './sql/script/FS_prod.sql'
    with open(path,'r') as f:
        content = f.read()

    subs = content.split('COMMIT;')
    subs = [s.strip() for s in subs]
    yaml_infos = []
    for sub in subs:
        data = extract_key_values(sub)
        if data:
            yaml_infos.append(data)
    # print(yaml_infos)
    groups = generate_backfill_report(yaml_infos)
    for table, columns in groups.items():
        print(f'Table: {table.split(".")[-1]}')
        for column in columns:
            print(f'{column}')
        print('----------------------')


    for table, columns in groups.items():
        if 'CINS' in table:
            continue
        column_str = ', '.join(columns)
        query = f"SELECT {column_str} FROM {table}"
        print(query)

    
if __name__ == '__main__':    
    # get_numrow_from_insert()
    # split_each_feature_into_a_file()
    generate_test_scripts()
    # get_backfill_info()
