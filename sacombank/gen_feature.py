import os
from oraDB import oraDB
import cx_Oracle
import re
import multiprocessing
import util
# Set up logging
import logging
import datetime

logger = logging.getLogger(__name__)

def read_ft_and_tbl_in_subquery(query):
    description = re.findall(r'/\*(.*?)\*/',query, re.DOTALL)
    features, derived_tables = None, None
    if description and len(description) > 0:
        description = description[0].strip()
        comps = description.split('\n')
        if len(comps) == 2:
            features = comps[0].replace('Feature Name:', '').strip().split(',')
            features = [i.strip() for i in features]
            derived_tables = comps[1].replace('Derived From:','').strip().split(',')
            derived_tables = [i.strip() for i in derived_tables]
    return description, features, derived_tables
            
def run_feature_record_query(f_record):
    conn,cur= oraDB.connect()
    #ALTER SESSION
    # cur.execute("ALTER SESSION FORCE PARALLEL DML PARALLEL 64")
    # cur.execute("ALTER SESSION FORCE PARALLEL QUERY PARALLEL 64")
    
    query = f_record['query'] 
    desc = f_record['desc']
    features = f_record['features']
    d_tables = f_record['derived_tables']
    ftype = f_record['ftype']
    f_record['query_status'] = 'uninitiated'
    try:
        if features and d_tables:
            logger.info(f'INSERTING feature {features}')
            logger.debug(f'Start INSERT feature {features} FROM {d_tables} - ftype: {ftype}')
        else:
            logger.debug(f'[WARN] Start INSERT script {desc}')
        cur.execute(query)
        if features and d_tables:
            logger.debug(f'Succeed INSERT feature {features} FROM {d_tables} - ftype: {ftype}')
        else:
            logger.debug(f'[WARN] Succeed INSERT script {desc}')
        f_record['query_status'] = 'passed'
    except cx_Oracle.DatabaseError as e:
        logger.error(e)
        logger.error(f'Failed to execute query record {f_record}')
        f_record['query_status'] = 'failed'
    except Exception as er:
        logger.error(er)
        logger.error(f'Failed to execute query record {f_record}')
        f_record['query_status'] = 'failed'
    finally:
        cur.close()
        conn.close()
    return f_record

def run_feature_record_query_with_conn(f_record, conn):
    query = f_record['query'] 
    desc = f_record['desc']
    features = f_record['features']
    d_tables = f_record['derived_tables']
    ftype = f_record['ftype']
    f_record['query_status'] = 'uninitiated'
    try:
        if features and d_tables:
            logger.info(f'INSERTING feature {features}')
            logger.debug(f'Start INSERT feature {features} FROM {d_tables} - ftype: {ftype}')
        else:
            logger.debug(f'[WARN] Start INSERT script {desc}')
        conn.execute(query)
        if features and d_tables:
            logger.debug(f'Succeed INSERT feature {features} FROM {d_tables} - ftype: {ftype}')
        else:
            logger.debug(f'[WARN] Succeed INSERT script {desc}')
        f_record['query_status'] = 'passed'
    except cx_Oracle.DatabaseError as e:
        logger.error(e)
        logger.error(f'Failed to execute query record {f_record}')
        f_record['query_status'] = 'failed'
    except Exception as er:
        logger.error(er)
        logger.error(f'Failed to execute query record {f_record}')
        f_record['query_status'] = 'failed'
    return f_record


def read_sql_feature_file(filepath, ftype):
    filename = os.path.splitext(os.path.basename(filepath))[0]
    records = []
    if os.path.exists(filepath):
        with open(filepath, 'r') as f: 
            f_query = f.read()
        subqueries = f_query.split(';')
        for subq in subqueries:
            if len(subq) > 0:
                desc, features, derived_tables = read_ft_and_tbl_in_subquery(subq)
                record = {
                    'query': subq,
                    'desc': desc,
                    'features': features,
                    'derived_tables': derived_tables,
                    'ftype': ftype,
                    'batch': filename
                }
                records.append(record)
    return records

def run_multiquery(feature_sqls):
    feature_sql_jobs = None
    # Execute multi-query process
    s0 = datetime.datetime.now()
    logger.info('Start multi-quering features')
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        feature_sql_jobs = pool.map(run_feature_record_query, feature_sqls)
    logger.info('End multi-quering features')
    el = datetime.datetime.now() - s0
    logger.info(f'[timeit] Elapsed time of multi-query feature {el}')
    return feature_sql_jobs


@util.timeit
def run_feature_query(response):
    config = response['config']
    RPT_DT = response['RPT_DT']
    RPT_DT_TBL = response['RPT_DT_TBL']
    NUM_PROCESSES = config['NUM_PROCESSES']
    unstructured_fp = os.path.join(config['SQL_FEATURE_FOLDER'], 'report_date', RPT_DT, 'unstructured')
    structured_fp = os.path.join(config['SQL_FEATURE_FOLDER'], 'report_date', RPT_DT, 'structured')
    
    # Prepare feature sql code (flatten)
    feature_sqls = []
    logger.info(f'Aggregate INSERT unstructured features code')
    for file in os.listdir(unstructured_fp):
        if file.endswith(".sql"): 
            filepath = os.path.join(unstructured_fp, file)
            feature_sqls.extend(read_sql_feature_file(filepath, ftype='unstructured'))
            
    logger.info(f'Aggregate INSERT structured features code')
    for file in os.listdir(structured_fp):
        if file.endswith(".sql"): 
            filepath = os.path.join(structured_fp, file)
            feature_sqls.extend(read_sql_feature_file(filepath, ftype='structured'))
            
    # run_multiquery(feature_sqls)
    
    logger.info(f"Prepare inserting feature into table {config['FEATURE_STORE_TBL_NM']}")
    s0 = datetime.datetime.now()
    logger.info('Start quering features')
    dbEngine = oraDB.create_engine()
    feature_sql_jobs = []
    with dbEngine.connect() as conn:
        # conn.execute("ALTER SESSION FORCE PARALLEL DML PARALLEL 64")
        conn.execute("ALTER SESSION FORCE PARALLEL QUERY PARALLEL 64")
        for feat_sql in feature_sqls:
            f_record = run_feature_record_query_with_conn(feat_sql, conn)
            feature_sql_jobs.append(f_record)
    logger.info('End quering features')
    el = datetime.datetime.now() - s0
    logger.info(f'[timeit] Elapsed time: {el}')
    
    result = {
        'FEATURE_SQL_JOBS': feature_sql_jobs
    }
    return result
    
