# /opt/bitnami/miniconda/bin/python main.py

import os
import sys
import pathlib
import pandas as pd
import numpy as np
import pickle
import logging
import preprocessing
from datetime import datetime
import argparse
import yaml
import handle_db
import constant
from prepare_data import DataHandler
from model_ops import Trainer, Predictor


def configure_logging(log_level='INFO'):
    # Configure Logging
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
    

def train_model(args, ml_config):
    def prepare_dataset(dataset_dates):
        df = None
        for d in dataset_dates:
            data_handler = DataHandler(d, reload_local_file=args.reload_local_file)
            data_label = data_handler.get_data_label()
            if df is None:
                df = data_label
            else:
                df = pd.concat([df, data_label], ignore_index=True)
        assert len(df) > 0
        y = df.pop(constant.LABEL)
        X = df
        return X, y
    
    data_dict = {
        'train': prepare_dataset(ml_config['train']),
        'valid': prepare_dataset(ml_config['valid']),
        'backtest': prepare_dataset(ml_config['backtest'])
    }
    trainer_ops = Trainer(data_dict, ml_config)
    trainer_ops.train()
    
    
def test(args, ml_config):
    predictor = Predictor(ml_config['model_path'])
    for dt in ml_config['test_date']:
        logging.info(f'Testing {dt} data')
        data_handler = DataHandler(dt, 
                                   reload_local_file=args.reload_local_file, 
                                   overwrite_tmp_file=args.overwrite_tmp_file)
        data_label = data_handler.get_data_label()
        y = data_label.pop(constant.LABEL)
        X = data_label
        predictor.evaluate(X, y)
        
    
def serve(args, ml_config):
    predictor = Predictor(ml_config['model_path'])
    for dt in ml_config['report_date']:
        logging.info(f'Serving {dt} data')
        data_handler = DataHandler(dt, 
                                   reload_local_file=args.reload_local_file, 
                                   overwrite_tmp_file=args.overwrite_tmp_file)
        data = data_handler.get_formatted_raw_feature_data()
        score = predictor.score(data)
        score = score[:, 1]
        score_df = pd.DataFrame({
            'CUSTOMER_CDE': data.index,
            'SCORE': score, 
        })
        logging.info('Sorting best customer')
        score_df = score_df.sort_values(by='SCORE', ascending=False).reset_index(drop=True)
        score_fn = f'./out/SCORE_{dt}'
        score_df.to_parquet(score_fn)
        logging.info(f'Stored data at {score_fn}')
        if ml_config['sync_db']:
            handle_db.push_score_to_DW(args, dt)
            logging.info(f'Finished insert REACTIVATED SCORE {dt}')
            

def adhoc(args, ml_config):
    if ml_config['task'] == 'push_raw_data':
        logging.info('Prepare pushing raw matrix data to DW')
        for dt in ml_config['report_date']:
            data_handler = DataHandler(dt, 
                                       reload_local_file=args.reload_local_file,
                                       overwrite_tmp_file=args.overwrite_tmp_file)
            data = data_handler.get_formatted_raw_feature_data()
            logging.info(f'Data shape {data.shape}')
            handle_db.push_raw_matrix_data_to_DW(data, args, dt)
    elif ml_config['task'] == 'get_label':
        logging.info('Prepare getting label')
        for dt in ml_config['report_date']:
            data_handler = DataHandler(dt, 
                                       reload_local_file=args.reload_local_file,
                                       overwrite_tmp_file=args.overwrite_tmp_file)
            data_handler.get_label()
    else:
        logging.warn(f'{ml_config["task"]} task is unimplemented')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('mode', type=str, choices=['train', 'test', 'serve', 'adhoc'], help="""Select mode train/test/serve/adhoc base config file""")
    parser.add_argument('--log', choices=['DEBUG','INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help='Set the logging level')
    parser.add_argument('--batch_size', type=int, default=100000, help='Batch size for push prediction data to DW')
    parser.add_argument('--reload_local_file', action='store_true', help='Option to reload local file')
    parser.add_argument('--update_data', dest='reload_local_file', action='store_false', help='Update data')
    parser.add_argument('--overwrite_tmp_file', action='store_true', help='Overwrite temp data')
    parser.set_defaults(reload_local_file=True)
    parser.set_defaults(overwrite_tmp_file=False)
    args = parser.parse_args()
    
    configure_logging(args.log)
    
    ml_config = {}
    config_fp = f'./config/{args.mode}.yaml'
    with open(config_fp, 'r') as f:
        ml_config.update(yaml.safe_load(f))
    logging.info(ml_config)
    
    logging.debug(f'{args}')
    
    if args.mode == 'train':
        train_model(args, ml_config)
    elif args.mode == 'test':
        test(args, ml_config)
    elif args.mode == 'serve':
        serve(args, ml_config)
    elif args.mode == 'adhoc':
        adhoc(args, ml_config)