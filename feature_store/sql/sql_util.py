import sys
sys.path.append('..')


import os
import gen_feature
import re
import yaml
import re
import util_func
import logging

logger = logging.getLogger(__name__)



COMMIT_CHECKPOINT = util_func.COMMIT_CHECKPOINT

def split_each_feature_into_a_file():
    feature_fp = './feature/placeholder/'
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


def gen_run_oneoff_script(sel_date):
    
    sel_date_tbl = sel_date.replace('-','')

    # config
    output_dev = f'./out/FS_dev_casa_{sel_date_tbl}.sql'
    output_prod = f'./out/FS_prod_casa_{sel_date_tbl}.sql'
    table_template = './template/'
    feat_template = './template/feature'

    # Default
    tables, features = [], []

    # Test
    ## Table to be inserted data (feature-store table)
    tbl_nm = f'CINS_FEATURE_STORE_REACTIVATED_{sel_date_tbl}'

    ## Truncate or drop
    drop_tables = []
    # truncate_tables = ['CINS_TMP_CUSTOMER', 'CINS_TMP_CUSTOMER_STATUS', 'CINS_TMP_CARD_DIM']
    truncate_tables = []
    ## Create table
    # create_tables = ['CINS_FEATURE_STORE_REACTIVATED']
    create_tables = []
    ## Insert table
    insert_tables = []

    ## Feature
    features = [
        # 'CASA_HOLD', 'CARD_CREDIT_HOLD', 'EB_SACOMPAY_HOLD', 'EB_MBIB_HOLD',
        # 'LIFE_STG', 'AREA',
        # 'LOR', 'CREDIT_SCORE',
        # 'CASA_BAL_SUM_NOW', 'CASA_DAY_SINCE_LAST_TXN_CT_36M', 
        # 'CARD_CREDIT_MAX_LIMIT', 'CARD_CREDIT_SUM_BAL_NOW', 
        # 'EB_SACOMPAY_DAY_SINCE_LTST_LOGIN', 'EB_SACOMPAY_DAY_SINCE_LTST_TXN', 'EB_MBIB_DAY_SINCE_ACTIVE',
        'CASA_ACCT_CT_36M', 'CASA_BAL_SUM_12M', 'CASA_BAL_MAX_12M', 'CASA_ACCT_ACTIVE_CT_12M', 'CASA_TXN_AMT_SUM_12M', 'CASA_TXN_CT_12M',
        'CASA_BAL_SUM_24M', 'CASA_BAL_SUM_36M', 'CASA_TXN_AMT_SUM_24M', 'CASA_TXN_AMT_SUM_36M', 'CASA_TXN_CT_24M', 'CASA_TXN_CT_36M',
        'GEN_GRP', 'AGE', 'LOR', 'PROFESSION', 
    ]
    
    # Generate
    scripts = []

    # DDL 
    for t in drop_tables:
        drop_sql = f"DROP TABLE {t}_{sel_date_tbl}"
        scripts.append(drop_sql)

    for t in truncate_tables:
        truncate_sql = f"TRUNCATE TABLE {t}_{sel_date_tbl}"
        scripts.append(truncate_sql)

    for t in create_tables:
        create_sql_fp = os.path.join(table_template, 'ddl', t + '.sql')
        create_script = util_func.read_sql_file(create_sql_fp)
        if create_script:
            scripts.append(create_script)

    for t in insert_tables:
        insert_sql_fp = os.path.join(table_template, 'dml', t + '.sql')
        insert_script = util_func.read_sql_file(insert_sql_fp)
        if insert_script:
            scripts.append(insert_script)

    # Create table with RPT_DT
    create_sql_fp = os.path.join(table_template, 'ddl', 'CINS_FEATURE_STORE_REACTIVATED.sql')
    create_script = util_func.read_sql_file(create_sql_fp)
    create_script = create_script.replace('CINS_FEATURE_STORE_REACTIVATED',tbl_nm)
    if create_script:
        scripts.append(create_script)
        
    # DML
    print(f'Num features {len(features)}')
    for f in features:
        print(f, end=' ')
        feat_sql_fp = os.path.join(feat_template, f + '.sql')
        feat_script = util_func.read_sql_file(feat_sql_fp)
        if feat_script:
            print('added')
            scripts.append(feat_script)
        else:
            print('missed')
    
    # Aggregated
    # Check SQL script ended properly
    proper_scripts = []
    for s in scripts:
        if not s.strip().endswith(';'):
            proper_scripts.append(s + ';')
        else:
            proper_scripts.append(s)

    final_script = COMMIT_CHECKPOINT.join(proper_scripts)

    final_script += COMMIT_CHECKPOINT

    final_script = final_script.strip()

    # Replace TBL_NM, RPT_DT and RPT_DT_TBL
    final_script = final_script.replace('{TBL_NM}', f'{tbl_nm}')
    final_script = final_script.replace('{RPT_DT}', f'{sel_date}')
    final_script = final_script.replace('{RPT_DT_TBL}', f'{sel_date_tbl}')

    # Post-processing: ensure ";" in placed
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

def gen_run_oneoff_script_many_dates():
    sel_dates = [
        '01-01-2023',
        '01-02-2023',
        '01-03-2023',
        '01-04-2023',
        '01-05-2023',
        '01-06-2023',
        '01-07-2023',
        '01-08-2023',
        '01-09-2023',
        '01-10-2023',
        '01-11-2023',
        '01-12-2023',
    ]
    for sel_date in sel_dates:
        gen_run_oneoff_script(sel_date)


@DeprecationWarning
def generate_backfill_report(data):
    """Generate data for backfill report include: tables, columns and script"""
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
    """Get data for backfill report include: tables, columns and script"""
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

    path = './script/FS_prod_01102023.sql'
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


def gen_derived_feature_scripts_from_base_feature():
    base_fp = './template/feature/base_feature/base'
    derived_fp = './template/feature/base_feature/derived'

    # features = []
    # features = ['CARD_CREDIT_TXN', 'CARD_CREDIT_TXN_DOM', 'CARD_CREDIT_TXN_INTER', 'CARD_CREDIT_TXN_OFFLINE', 'CARD_CREDIT_TXN_ONLINE']
    features = ['CASA_BAL', 'CASA_TXN_AMT']


    for feature in features:
        feat_fp = os.path.join(base_fp, feature + '.sql')
        with open(feat_fp, 'r') as f:
            base_script = f.read()
        desc_yaml, _, sql_sec = util_func.extract_desc_yaml_section_from_string(base_script)
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
                derived_desc = util_func.convert_yaml_to_string(derived_desc_yaml)
                derived_sql = sql_sec
                derived_sql = derived_sql.replace("{{FEATURE_NAME}}", feat_name)
                derived_sql = derived_sql.replace("{{AGG}}", agg)
                derived_sql = derived_sql.replace("{{MONTH_WINDOW}}", month_window)
                content = f"/*\n{derived_desc}*/\n{derived_sql}"
                with open(os.path.join(derived_fp, feat_fp), 'w') as f:
                    f.write(content)
        
    
if __name__ == '__main__':    
    # split_each_feature_into_a_file()
    gen_run_oneoff_script_many_dates()
    # get_backfill_info()
    # gen_derived_feature_scripts_from_base_feature()
