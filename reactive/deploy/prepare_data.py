import os
import pandas as pd
import pathlib

import sql_template
import constant
import preprocessor
import pickle
import util

import logging
logger = logging.getLogger(__name__) 


DATA_LOC = pathlib.Path('./data')


class DataHandler:
    def __init__(self, 
                 report_date, 
                 target_columns=constant.RAW_FEATURE_NAMES, 
                 reload_local_file=True,
                 overwrite_tmp_file=False):
        self.report_date = report_date
        self.report_date_loc = DATA_LOC / report_date
        self.report_date_tbl = report_date.replace('-','')
        self.raw_fp = self.report_date_loc / 'FEATURE_DATA_RAW'
        self.fmt_fp = self.report_date_loc / 'FEATURE_DATA_FMT'
        self.data_label_fp = self.report_date_loc / 'DATA_LABEL'
        self.label_fp = self.report_date_loc / constant.LABEL
        self.score_fp = self.report_date_loc / 'SCORE_REACTIVE'
        self.target_columns = target_columns
        self.reload_local_file = reload_local_file
        self.overwrite_tmp_file = overwrite_tmp_file
        self.init()
        
    def init(self):
        # Mkdir or reused RPT-DT folder
        if not os.path.exists(self.report_date_loc):
            logger.info(f'Create RPT_DT {self.report_date}')
            os.makedirs(self.report_date_loc)
        
    def download_raw_feature_data(self):
        """
        Download raw feature data from CINS_FEATURE_STORE_REACTIVATED_{RPT_DT} with selected report-date and store for merging
        """
        
        logger.info('Prepare loading raw feature data')
        if (not self.reload_local_file) or (not os.path.exists(self.report_date_loc / 'CUSTOMER_CDE')):
            # Download customer-cde of rpt-dt
            logger.debug(f'Downloading CUSTOMER_CDE of {self.report_date}')
            util.download_to_parquet(self.report_date_loc / 'CUSTOMER_CDE', 
                                    sql_template.QUERY_CUSTOMER_CDE_RPT_DT % self.report_date_tbl)
            
        # Download all raw-feature data
        for raw_nm in self.target_columns:
            if (not self.reload_local_file) or (not os.path.exists(self.report_date_loc / raw_nm)):
                logger.debug(f'Downloading raw-data {raw_nm}')
                util.download_to_parquet(self.report_date_loc / raw_nm, 
                                        sql_template.QUERY_RAW_DATA_FROM_FEATURE_STORE % (self.report_date_tbl, self.report_date, raw_nm))
            
    def merge_raw_feature_data(self):
        """
        Merge them together to build raw-feat-data matrix
        """
        logger.info('Merging raw feature data ')
        if (not self.reload_local_file) or (not os.path.exists(self.raw_fp) or (self.overwrite_tmp_file)):
            cust_id = util.reload(self.report_date_loc / 'CUSTOMER_CDE')
            logger.info(f'Total customer-cde: {len(cust_id)}')
            for col in self.target_columns:
                logger.debug(f'Merging {col}')
                col_fp = self.report_date_loc / col
                if col_fp.exists():
                    df = util.reload(col_fp)
                    df = df[['CUSTOMER_CDE', 'FTR_VAL']]
                    df.columns = ['CUSTOMER_CDE', col]
                    cust_id = cust_id.merge(df, how='left', left_on='CUSTOMER_CDE', right_on='CUSTOMER_CDE')
                else:
                    raise FileNotFoundError(f'{str(col_fp)} not exists, please check data in Feature Store')
            cust_id = cust_id.set_index('CUSTOMER_CDE')
            cust_id.to_parquet(self.raw_fp)
        
    def get_ordered_raw_feature_data(self):
        """
        Get ordered-raw feature data
        """
        logger.info('Prepare loading ordered-raw feature data ')
        self.download_raw_feature_data()
        self.merge_raw_feature_data()
        df = util.reload(self.raw_fp)
        
        # check column names must be fulfilled
        assert 'CUSTOMER_CDE' not in df.columns.tolist()
        missing_cols = set(constant.RAW_FEATURE_NAMES) - set(df.columns.tolist())
        if len(missing_cols) > 0:
            raise Exception(f'Missing columns {missing_cols}')
        # order columns
        df = df[constant.RAW_FEATURE_NAMES]
        return df
    
    def get_formatted_raw_feature_data(self):
        """
        Get formatted-raw feature data
        """
        logger.info('Prepare loading formatted-raw feature data ')
        if (not self.reload_local_file) or (not os.path.exists(self.fmt_fp) or (self.overwrite_tmp_file)):
            df = self.get_ordered_raw_feature_data()
            df = preprocessor.format_datatype(df)
            df.to_parquet(self.fmt_fp)
            return df
        else:
            return pd.read_parquet(self.fmt_fp)
                                   
    def get_data_label(self, invalid_data='raise'):
        """
        Get data and label
        """
        logger.info('Prepare loading data-label')
        if (not self.reload_local_file) or (not os.path.exists(self.data_label_fp) or (self.overwrite_tmp_file)):
            data = self.get_formatted_raw_feature_data()
            # Download reactivated label
            logger.info(f'Prepare loading REACTIVATE LABEL of {self.report_date}')
            label = util.download_or_reload(self.report_date_loc / constant.LABEL, 
                                         sql_template.QUERY_RAW_DATA_FROM_FEATURE_STORE % (self.report_date_tbl, self.report_date, constant.LABEL),
                                         reload_local_file = self.reload_local_file)
            if label is not None and (len(label) == 0):
                if invalid_data=='raise':
                    raise Exception('REACTIVATED label not found or not qualified')
                else:
                    return 
            label = label[['CUSTOMER_CDE', 'FTR_VAL']]
            label.columns = ['CUSTOMER_CDE', constant.LABEL]
            label = label.set_index('CUSTOMER_CDE')
            label[constant.LABEL] = label[constant.LABEL].astype(int)
            data_label = data.merge(label, how='inner', left_index=True, right_index=True)
            data_label.to_parquet(self.data_label_fp)
            return data_label
        else:
            return pd.read_parquet(self.data_label_fp)
    
    def get_label(self):
        """
        Get label
        """
        logger.info('Prepare loading label')
        if (not self.reload_local_file) or (not os.path.exists(self.label_fp)):
            label = util.download_or_reload(self.report_date_loc / constant.LABEL, 
                                            sql_template.QUERY_RAW_DATA_FROM_FEATURE_STORE % (self.report_date_tbl, self.report_date, constant.LABEL),
                                            reload_local_file = self.reload_local_file)
            return label
        else:
            return pd.read_parquet(self.label_fp)