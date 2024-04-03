import constant
import os
import pandas as pd

import logging
logger = logging.getLogger(__name__) 


class LocalDataValidator:
    def __init__(self, data_handler):
        self.data_handler = data_handler
        self.raw_feature_data_report = []
        logger.debug(f'Prepare validating local data {self.data_handler.report_date}')

    def validate(self):
        logger.debug('Validating...')
        self.validate_raw_feature_data()
        self.remove_invalid_raw_feature_data()
        logger.debug('Validate done')

    def validate_raw_feature_data(self):
        logger.debug('Validate features')
        for raw_nm in constant.RAW_FEATURE_NAMES:
            raw_feat_report = [raw_nm]
            logger.debug(f'Validate features {raw_nm}')
            # Check filepath existed
            raw_feat_fp = self.data_handler.report_date_loc / raw_nm
            if os.path.exists(raw_feat_fp):
                raw_feat_report.append(True)
                logger.debug(f'\tPASSED check existed')

                 # Check data length
                raw_feat_file = pd.read_parquet(raw_feat_fp)
                len_df = len(raw_feat_file)
                if len_df > 0:
                    raw_feat_report.append(True)
                    logger.debug(f'\tPASSED check valid length')

                    # Check unique CUSTOMER_CDE, no duplicated
                    num_unique_cust = raw_feat_file['CUSTOMER_CDE'].nunique()
                    if len_df == num_unique_cust:
                        raw_feat_report.append(True)
                        logger.debug(f'\tPASSED check duplicated CUSTOMER CDE')
                    else:
                        raw_feat_report.append(False)
                        logger.error(f'\tFAILED check duplicated CUSTOMER CDE - org {len_df} vs dst {num_unique_cust}')
                else:
                    raw_feat_report.extend([False, False])
                    logger.error(f'\tFAILED check valid length - dst {len_df}')
            else:
                raw_feat_report.extend([False, False, False])
                logger.error(f'\tFAILED check existed - not existed {raw_feat_fp}')
            self.raw_feature_data_report.append(raw_feat_report)
        self.raw_feature_data_report = pd.DataFrame(self.raw_feature_data_report, columns=['RAW_FEAT_NM', 'CHECK_EXIST', 'CHECK_LENGTH', 'CHECK_DUPLICATE'])
            
    def remove_invalid_raw_feature_data(self):
        logger.warning('Adhoc: Remove Invalid data')
        for i, row in self.raw_feature_data_report.iterrows():
            raw_nm = row['RAW_FEAT_NM']
            if (row['CHECK_EXIST']) and (not row['CHECK_LENGTH']):
                raw_feat_fp = self.data_handler.report_date_loc / raw_nm
                os.remove(raw_feat_fp)
                logger.warning(f'Removed Invalid data {raw_feat_fp}')


class DataWareHouseValidator:
    def __init__(self, report_dates):
        self.report_dates=report_dates
        
    def run(self):
        # Unimplemented
        pass