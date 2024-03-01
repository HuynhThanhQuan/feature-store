import handle_db
import constant
from prepare_data import DataHandler
from model_ops import Trainer, Predictor
import pathlib
import pandas as pd
import numpy as np
import pickle
import preprocessing


def train(args, ml_config):
    """
    Training model with options 
    """
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
    """
    Test model with selected date
    """
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
    """
    Predefined adhoc tasks
    """
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