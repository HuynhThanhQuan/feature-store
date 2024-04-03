import pandas as pd
import numpy as np
import pickle
import pathlib

import constant
from prepare_data import DataHandler
from model_ops import Trainer, Predictor
import preprocessor
import database_jobs
from validator import LocalDataValidator, DataWareHouseValidator

import logging
logger = logging.getLogger(__name__) 


class ReactiveJobHandler:
    def __init__(self, config):
        self.config = config

    def run(self):
        # Run selected ML job
        if self.config['job'] == 'train':
            self.train()
        elif self.config['job'] == 'test':
            self.test()
        elif self.config['job'] == 'serve':
            self.serve()
        elif self.config['job'] == 'adhoc':
            self.adhoc()

    def train(self):
        """
        Training model with options 
        """
        def prepare_dataset(dataset_dates):
            df = None
            for d in dataset_dates:
                data_handler = DataHandler(d, reload_local_file=self.config['reload_local_file'], overwrite_tmp_file=self.config['overwrite_tmp_file'])
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
            'train': prepare_dataset(self.config['train']),
            'valid': prepare_dataset(self.config['valid']),
            'backtest': prepare_dataset(self.config['backtest'])
        }
        trainer_ops = Trainer(data_dict, self.config)
        trainer_ops.train()
        
    def test(self):
        """
        Test model with selected date
        """
        predictor = Predictor(self.config['model_path'])
        for dt in self.config['test_date']:
            logger.info(f'Testing {dt} data')
            data_handler = DataHandler(dt, reload_local_file=self.config['reload_local_file'], overwrite_tmp_file=self.config['overwrite_tmp_file'])
            data_label = data_handler.get_data_label()
            y = data_label.pop(constant.LABEL)
            X = data_label
            predictor.evaluate(X, y)
        
    def serve(self):
        predictor = Predictor(self.config['model_path'])
        for dt in self.config['report_date']:
            logger.info(f'Serving {dt} data')
            # Get data handler of report date
            data_handler = DataHandler(dt, reload_local_file=self.config['reload_local_file'], overwrite_tmp_file=self.config['overwrite_tmp_file'])
            data = data_handler.get_formatted_raw_feature_data()
            # Scoring
            score = predictor.score(data)
            score = score[:, 1]
            score_df = pd.DataFrame({
                'CUSTOMER_CDE': data.index,
                'SCORE': score, 
            })
            # Sorting in probability descending
            logger.info('Sorting best customer')
            score_df = score_df.sort_values(by='SCORE', ascending=False).reset_index(drop=True)
            score_fn = data_handler.score_fp
            score_df.to_parquet(score_fn)
            logger.info(f'Stored data at {score_fn}')
            if self.config['sync_db']:
                database_jobs.push_score_to_DW(score_fn, dt, self.config)
                logger.info(f'Finished insert REACTIVATED SCORE {dt}')

    def adhoc(self):
        """
        Predefined adhoc tasks
        """
        if self.config['task'] == 'push_raw_data':
            logger.info('Adhoc: Push raw data into DW')
            for dt in self.config['report_date']:
                logger.info(f'Pushing raw matrix data {dt}')
                data_handler = DataHandler(dt, reload_local_file=self.config['reload_local_file'], overwrite_tmp_file=self.config['overwrite_tmp_file'])
                data = data_handler.get_formatted_raw_feature_data()
                logger.info(f'Data shape {data.shape}')
                database_jobs.push_raw_matrix_data_to_DW(data,self.config, dt)
        elif self.config['task'] == 'get_label':
            logger.info('Adhoc: Getting label')
            for dt in self.config['report_date']:
                logger.info(f'Prepare getting label {dt}')
                data_handler = DataHandler(dt, reload_local_file=self.config['reload_local_file'], overwrite_tmp_file=self.config['overwrite_tmp_file'])
                data_handler.get_label()
        elif self.config['task'] == 'validate_local_data':
            logger.info('Adhoc: Validate local data')
            for dt in self.config['report_date']:
                logger.info(f'Validating local data {dt}')
                data_handler = DataHandler(dt, reload_local_file=self.config['reload_local_file'], overwrite_tmp_file=self.config['overwrite_tmp_file'])
                val_obj = LocalDataValidator(data_handler)
                val_obj.validate()
        elif self.config['task'] == 'download_data_label':
            logger.info('Adhoc: Download data label')
            for dt in self.config['report_date']:
                try:
                    logger.info(f'Download data {dt}')
                    data_handler = DataHandler(dt, reload_local_file=False, overwrite_tmp_file=True)
                    data_label = data_handler.get_data_label()
                except Exception as e:
                    logger.error(f'Failed to download data {dt}')
        elif self.config['task'] == 'validate_datawarehouse':
            logger.info('Adhoc: Validate DataWarehouse')
            val_obj = DataWareHouseValidator(self.config['report_date'])
            val_obj.run()
        else:
            logger.warn(f'{self.config["task"]} task is unimplemented')