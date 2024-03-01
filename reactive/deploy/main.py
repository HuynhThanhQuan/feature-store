import os
import sys
import logging
from datetime import datetime
import argparse
import yaml
import jobs


def configure_logging(log_level='INFO'):
    """
    Configure Logging
    1. Stdout
    2. File
    """
    os.makedirs('./log', exist_ok=True)
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
        
    log_format = logging.Formatter('%(asctime)s - %(processName)s - %(name)-15s - %(levelname)-6s - %(message)s')
    
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(log_format)
    stdout_handler.setLevel(log_level)
    
    cur_time = datetime.now().strftime('%y-%m-%d %H:%M:%S')
    filename = f'./log/{cur_time}.log'
    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(log_format)
    file_handler.setLevel(logging.DEBUG)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)


def run(args):
    # Config Logging
    configure_logging(args.log)
    
    # Config ML jobs
    ml_config = {}
    config_fp = f'./config/{args.job}.yaml'
    with open(config_fp, 'r') as f:
        ml_config.update(yaml.safe_load(f))
    logging.info(ml_config)
    logging.debug(f'{args}')
    
    # Run selected ML job
    if args.job == 'train':
        jobs.train(args, ml_config)
    elif args.job == 'test':
        jobs.test(args, ml_config)
    elif args.job == 'serve':
        jobs.serve(args, ml_config)
    elif args.job == 'adhoc':
        jobs.adhoc(args, ml_config)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='Reactive ML System',
        usage='%(prog)s [options]',
        description='Reactive ML system to train/test/serve and some util tasks',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('job', type=str, choices=['train', 'test', 'serve', 'adhoc'], help="""Select job train/test/serve/adhoc base config file""")
    parser.add_argument('--log', choices=['DEBUG','INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help='Set the logging level')
    parser.add_argument('--batch_size', type=int, default=100000, help='Batch size for push prediction data to DW')
    parser.add_argument('--reload_local_file', action='store_true', help='Option to reload local file')
    parser.add_argument('--update_data', dest='reload_local_file', action='store_false', help='Update data')
    parser.add_argument('--overwrite_tmp_file', action='store_true', help='Overwrite temp data')
    parser.set_defaults(reload_local_file=True)
    parser.set_defaults(overwrite_tmp_file=False)
    args = parser.parse_args()
    
    run(args)