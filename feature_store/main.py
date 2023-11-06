# /opt/bitnami/miniconda/bin/python main.py

import yaml
import gen_script, check_DB, gen_table, gen_feature, util, ft_dependency
import argparse
import sys
import logging
from datetime import datetime
import json
import traceback


def read_RPT_DT():
    with open('./config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    if 'RPT_DT' in config:
        RPT_DT = config['RPT_DT']
    else:
        current_date = datetime.date.today()
        RPT_DT = current_date.strftime('%dd%mm%Y')
    RPT_DT_TBL = RPT_DT.replace("-", "")
    return {'RPT_DT': RPT_DT,
            'RPT_DT_TBL': RPT_DT_TBL,
            'config': config}

def configure_logging(response, log_level='INFO'):
    # Configure Logging
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
        
    log_format = logging.Formatter('%(asctime)s - %(processName)s - %(name)-15s - %(levelname)-6s - %(message)s')
    
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(log_format)
    stdout_handler.setLevel(log_level)
    
    cur_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'./log/{cur_time}.log'
    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(log_format)
    file_handler.setLevel(logging.DEBUG)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)
    return {
        'EXECUTION_TIMESTAMP': cur_time
    }
    
def save_metadata(response):
    exc_time = response['EXECUTION_TIMESTAMP']
    meta_fp =f'./metadata/{exc_time}.meta'
    with open(meta_fp, 'w') as file:
        json.dump(response, file)

@util.timeit
def generate_features_report_date(response):
    # Gen script o RPT_DT
    response.update(gen_script.gen_tmp_table_script(response))
    response.update(gen_script.gen_feature_script(response))
    
    # Create & Insert data into TMP table
    response.update(check_DB.check_exists(response))
    response.update(gen_table.create_empty_tmp_tables(response))
    response.update(gen_table.insert_into_tmp_tables(response))
    
    # Gen features
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
def test_new_func():
    logging.debug('Test new Function')
    util.my_func()
    
@util.timeit    
def visualize_feature_dependency(response):
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
                        6. Run INSERT data INTO TMP tables
                        7. Run GENERATE Features and insert into Feature Store
                        8. Test new function
                        9. Analyze Feature and Table Dependency
    """)
    parser.add_argument('--log', choices=['DEBUG','INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help='Set the logging level')
    args = parser.parse_args()
    
    # Read Config and Logging
    response = read_RPT_DT()
    response.update(configure_logging(response, args.log))
    
    logging.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    logging.info('==================================')
    logging.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    
    exc_start = datetime.now()
    logging.info(f'Execution time: {exc_start}')
    
    
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
            visualize_feature_dependency(response)
        else:
            logging.info('Welcome and Goodbye')
    except Exception as er:
        logging.error(er)
        logging.error(traceback.format_exc())
    finally:
        save_metadata(response)
    exc_end = datetime.now()
    logging.info(f'Job Main End {exc_end} took {exc_end - exc_start}')
    
    logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    logging.info('==================================')
    logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')