import time
from functools import wraps
import logging
import os
from oraDB import oraDB
import gen_feature
import re
import yaml
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


def read_sql_file(fp):
    content = None
    if os.path.exists(fp):
        with open(fp,'r') as f:
            content = f.read().strip()
            if content.endswith(';'):
                id = content.rfind(';')
                content = content[:id]
    return content


def generate_test_scripts():
    # Default
    tables, features = [], []

    # Test
    # tables = ['CINS_2M_PART', 'CINS_TMP_LST']
    features = [
        # 'CASA_ACCT_ACTIVE_CT_12M','CASA_ACCT_CT_36M',
        # 'CASA_BAL_AVG_12M','CASA_BAL_AVG_1M','CASA_BAL_AVG_3M','CASA_BAL_AVG_6M',
        # 'CASA_BAL_MAX_12M','CASA_BAL_MAX_1M','CASA_BAL_MAX_3M','CASA_BAL_MAX_6M',
        # 'CASA_BAL_MIN_12M','CASA_BAL_MIN_1M','CASA_BAL_MIN_3M','CASA_BAL_MIN_6M',
        # 'CASA_BAL_SUM_12M','CASA_BAL_SUM_1M','CASA_BAL_SUM_3M','CASA_BAL_SUM_6M',
        # 'CASA_BAL_SUM_NOW','CASA_DAY_SINCE_LAST_TXN_CT_36M',
        # 'CASA_TXN_AMT_AVG_12M','CASA_TXN_AMT_AVG_1M','CASA_TXN_AMT_AVG_3M','CASA_TXN_AMT_AVG_6M',
        # 'CASA_TXN_AMT_MAX_12M','CASA_TXN_AMT_MAX_1M','CASA_TXN_AMT_MAX_3M','CASA_TXN_AMT_MAX_6M',
        # 'CASA_TXN_AMT_MIN_12M','CASA_TXN_AMT_MIN_1M','CASA_TXN_AMT_MIN_3M','CASA_TXN_AMT_MIN_6M',
        # 'CASA_TXN_AMT_SUM_12M','CASA_TXN_AMT_SUM_1M','CASA_TXN_AMT_SUM_3M','CASA_TXN_AMT_SUM_6M',
        # 'CASA_TXN_CT_12M','CASA_TXN_CT_1M','CASA_TXN_CT_3M','CASA_TXN_CT_6M',
        'CASA_ACCT_COMBO_CT_36M', 'CASA_ACCT_PAYROLL_CT_36M'
    ]
    
    # Full
    # tables = [
    # 'CINS_TMP_CUSTOMER', 'CINS_TMP_CARD_DIM', 'CINS_TMP_CUSTOMER_STATUS', 'CINS_TMP_CREDIT_CARD_LOAN_6M', 'CINS_TMP_CREDIT_CARD_TRANSACTION', 'CINS_TMP_DATA_RPT_CARD', 'CINS_TMP_DATA_RPT_LOAN', 'CINS_TMP_EB_MB_CROSSELL', 'CINS_2M_PART', 'CINS_TMP_LST', 'CINS_FEATURE_STORE_V2'
    # ]
    # features = [
    # 'REACTIVATED', 'INACTIVE', 'CASA_INACTIVE', 'EB_MBIB_INACTIVE', 'CARD_CREDIT_INACTIVE','EB_SACOMPAY_INACTIVE', 'AGE', 'GEN_GRP', 'PROFESSION', 'LIFE_STG','CASA_AVG_BAL_1M', 'CASA_CT_ACCT_ACTIVE', 'CASA_CT_TXN_1M', 'CASA_DAY_SINCE_LTST_TXN','CASA_MAX_BAL_1M', 'CASA_MIN_BAL_1M', 'CASA_SUM_TXN_AMT_1M'
    # ]

    # Config
    sel_date = '11-06-2023'
    sel_date_tbl = sel_date.replace('-','')
    output_dev = f'./sql/script/test/FS_dev_{sel_date_tbl}.sql'
    output_prod = f'./sql/script/test/FS_prod_{sel_date_tbl}.sql'
    create_tbl_fd = './sql/table/placeholder/create'
    insert_tbl_fd = './sql/table/placeholder/insert'
    feat_official_fd = './sql/feature/placeholder/official'
    tbl_nm = 'CINS_FEATURE_STORE_V2'

    commit_ck = ';\n\n\nCOMMIT;\n\n\n'
    
    # Generate
    scripts = []
    # Drop tables first
    for t in tables:
        if t not in ['CINS_2M_PART', 'CINS_FEATURE_STORE_V2']:
            drop_sql = f"DROP TABLE {t}_{sel_date_tbl}"
        else:
            drop_sql = f"DROP TABLE {t}"
        scripts.append(drop_sql)

    # Read CREATE & INSERT INTO table scripts first
    for t in tables:
        create_sql_fp = os.path.join(create_tbl_fd, t + '.sql')
        create_script = read_sql_file(create_sql_fp)
        if create_script:
            scripts.append(create_script)
        
    for t in tables:
        insert_sql_fp = os.path.join(insert_tbl_fd, t + '.sql')
        insert_script = read_sql_file(insert_sql_fp)
        if insert_script:
            scripts.append(insert_script)

    # Read Feature
    print(f'Num features {len(features)}')
    for f in features:
        print(f, end=' ')
        feat_sql_fp = os.path.join(feat_official_fd, f + '.sql')
        feat_script = read_sql_file(feat_sql_fp)
        if feat_script:
            print('added')
            scripts.append(feat_script)
        else:
            print('missed')
    
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

    path = './sql/script/test/FS_prod_01102023.sql'
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

def extract_yaml_from_string(s):
    pattern = r'(/\*(.*?)\*/)'
    match = re.search(pattern, s, re.DOTALL)
    if match:
        comment_sec = match.group(1).strip()
        desc_sec = match.group(2).strip()
        return yaml.safe_load(desc_sec), desc_sec, s.replace(comment_sec, '').strip()
    else:
        return None, None, None
    

def convert_yaml_to_string(yaml_data):
    return yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)


def gen_derived_feature_script():
    base_fp = './sql/feature/placeholder/test_basefeat/base'
    derived_fp = './sql/feature/placeholder/test_basefeat/derived'

    features = ['CASA_TXN_AMT', 'CASA_BAL']
    
    for feature in features:
        feat_fp = os.path.join(base_fp, feature + '.sql')
        with open(feat_fp, 'r') as f:
            base_script = f.read()
        desc_yaml, desc_sec, sql_sec = extract_yaml_from_string(base_script)
        derived_desc_yaml = desc_yaml.copy()
        del derived_desc_yaml['Derived By']

        base_feat_name = desc_yaml.get('Feature Name')
        derived_format = desc_yaml.get('Derived By', {})

        aggs = derived_format.get('Aggregations')
        tws = derived_format.get('Time-Windows')

        for agg in aggs:
            for tw in tws:
                month_window = tw.replace('M', '')
                feat_name = f'{base_feat_name}_{agg}_{tw}'
                feat_fp = f'{feat_name}.sql'
                
                derived_desc_yaml['Feature Name'] = feat_name
                derived_desc_yaml['Derived From'] = desc_yaml['Derived From']
                derived_desc_yaml['TW'] = tw
                derived_desc = convert_yaml_to_string(derived_desc_yaml)
                derived_sql = sql_sec
                derived_sql = derived_sql.replace("{{FEATURE_NAME}}", feat_name)
                derived_sql = derived_sql.replace("{{AGG}}", agg)
                derived_sql = derived_sql.replace("{{MONTH_WINDOW}}", month_window)
                content = f"/*\n{derived_desc}*/\n{derived_sql}"
                with open(os.path.join(derived_fp, feat_fp), 'w') as f:
                    f.write(content)
        
    
if __name__ == '__main__':    
    # get_numrow_from_insert()
    # split_each_feature_into_a_file()
    generate_test_scripts()
    # get_backfill_info()
    # gen_derived_feature_script()
