# /opt/bitnami/miniconda/bin/python main.py

import yaml
import gen_script, check_DB, gen_table, gen_feature, util, ft_dependency
import argparse
import sys
import logging
from datetime import datetime, timedelta
import json
import traceback



def read_config():
    # Auto define env-varibles
    EXC_TMSP = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Read config
    with open('./config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    REPORT_DATE_cfg = config['REPORT_DATE']
    request_cfg = REPORT_DATE_cfg['REQUEST']
    req_type = request_cfg['TYPE']
    req_dates = request_cfg['DATES']
    # Auto translate request
    if req_type == 'R':
        req_dates = [datetime.strptime(i, '%d-%m-%Y') for i in req_dates]
        min_date = min(req_dates)
        max_date = max(req_dates)
        req_dates = [(min_date + timedelta(days=x)).strftime('%d-%m-%Y') for x in range((max_date - min_date).days+1)]
    return {
        'config': config,
        'REQUEST_REPORT_DATES': req_dates,
        'EXECUTION_TIMESTAMP': EXC_TMSP
    }
        


def configure_logging(response, log_level='INFO'):
    # Configure Logging
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
        
    log_format = logging.Formatter('%(asctime)s - %(processName)s - %(name)-15s - %(levelname)-6s - %(message)s')
    
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(log_format)
    stdout_handler.setLevel(log_level)
    
    cur_time = response['EXECUTION_TIMESTAMP']
    filename = f'./log/{cur_time}.log'
    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(log_format)
    file_handler.setLevel(logging.DEBUG)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)
    return {}
    
def save_metadata(response):
    exc_time = response['EXECUTION_TIMESTAMP']
    meta_fp =f'./metadata/{exc_time}.meta'
    with open(meta_fp, 'w') as file:
        json.dump(response, file)

@util.timeit
def generate_features_report_date(response):
    """
    1. Generate scripts: TMP tables, features
    2. CREATE TABLE and INSERT INTO into TMP tables
    3. Generate and INSERT INTO feature data
    """
    # Gen script of RPT_DT
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(gen_script.gen_feature_script(response))
    # Create & Insert data into TMP table
    response.update(check_DB.check_exists(response))
    response.update(check_DB.drop_tables(response))
    response.update(check_DB.check_exists(response))
    response.update(gen_table.create_empty_tmp_tables(response))
    response.update(check_DB.check_exists(response))
    response.update(gen_table.insert_into_tmp_tables(response))
    # Gen and insert features
    response.update(gen_feature.run_feature_query(response))

@util.timeit
def drop_tables_report_date(response):
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(check_DB.check_exists(response))
    response.update(check_DB.drop_tables(response))

@util.timeit
def check_existing_tmp_tables(response):
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(check_DB.check_exists(response))

@util.timeit
def generate_scripts(response):
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(gen_script.gen_feature_script(response))

@util.timeit
def create_empty_tmp_tables(response):
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(check_DB.check_exists(response))
    response.update(gen_table.create_empty_tmp_tables(response))

@util.timeit
def insert_into_tmp_tables(response):
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(check_DB.check_exists(response))
    response.update(gen_table.insert_into_tmp_tables(response))

@util.timeit
def gen_feature_only(response):
    response.update(gen_script.gen_feature_script(response))
    response.update(gen_feature.run_feature_query(response))

@util.timeit
def gen_sql_script(response):
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(gen_script.gen_feature_script(response))
    response.update(gen_script.aggregate_sql_scripts(response))
    
@util.timeit    
def test_new_func():
    logging.debug('Test new Function')
    util.my_func()
    
@util.timeit    
def export_feature_dependency(response):
    response.update(ft_dependency.analyze(response))
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('job', 
                        type=int,
                        help="""Job to execute
                        1. Generate Features
                        2. Drop TMP tables
                        3. Check existing tables on DB
                        4. Generate scripts
                        5. Run CREATE TMP tables (empty)
                        6. Run INSERT INTO TMP tables
                        7. Run INSERT INTO Features Store
                        8. Test new function
                        9. Analyze Feature and Table Dependency
                        10. Aggregate into 1 single script
    """)
    parser.add_argument('--log', choices=['DEBUG','INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help='Set the logging level')
    args = parser.parse_args()
    
    # Read Config and Logging
    response = read_config()
    response.update(configure_logging(response, args.log))
    
    logging.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    logging.info('======================================================================================================')
    logging.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    e_start = datetime.now()
    logging.info(f'Start Execution Time: {e_start}')
    # Report a list of requested dates
    REQUEST_REPORT_DATES = response['REQUEST_REPORT_DATES']
    for RPT_DT in REQUEST_REPORT_DATES:
        RPT_DT_TBL = RPT_DT.replace("-", "")
        response['RPT_DT'] = RPT_DT
        response['RPT_DT_TBL'] = RPT_DT_TBL
        logging.info(f'>>>---PREPARING REPORT DATE {RPT_DT}--->>>')
    
        t_start = datetime.now()
        logging.info(f'>> Start   : {t_start}')

        # Job
        try:
            if args.job == 0:
                logging.info('Hello')
            elif args.job == 1:
                generate_features_report_date(response)
            elif args.job == 2:
                drop_tables_report_date(response)
            elif args.job == 3:
                check_existing_tmp_tables(response)
            elif args.job == 4:
                generate_scripts(response)
            elif args.job == 5:
                create_empty_tmp_tables(response)
            elif args.job == 6:
                insert_into_tmp_tables(response)
            elif args.job == 7:
                gen_feature_only(response)
            elif args.job == 8:
                test_new_func()
            elif args.job == 9:
                export_feature_dependency(response)
            elif args.job == 10:
                gen_sql_script(response)
            else:
                logging.info('Welcome and Goodbye')
        except Exception as er:
            logging.error(er)
            logging.error(traceback.format_exc())
        finally:
            save_metadata(response)
        t_end = datetime.now()
        logging.info(f'>> Finished: {t_end} \t\t  took {t_end - t_start}')
        logging.info(f'<<<---FINISHED REPORT DATE {RPT_DT}----<<<\n')
        
    e_end = datetime.now()
    logging.info(f'Main Execution end at: {e_end} took {e_end - e_start}')
    logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    logging.info('======================================================================================================')
    logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n\n')
