
import constant
import os
import pandas as pd

import logging
logger = logging.getLogger(__name__) 


class LocalDataValidator:
    def __init__(self, data_handler):
        self.data_handler = data_handler
        logger.debug(f'Prepare validating local data {self.data_handler.report_date}')

    def validate(self):
        logger.debug('Validating...')
        self.validate_raw_feature_data()
        logger.debug('Validate done')

    def validate_raw_feature_data(self):
        logger.debug('Validate features')
        for raw_nm in constant.RAW_FEATURE_NAMES:
            logger.debug(f'Validate features {raw_nm}')
            # Check filepath existed
            raw_feat_fp = self.data_handler.report_date_loc / raw_nm
            if os.path.exists(raw_feat_fp):
                logger.debug(f'\tPASSED check existed')
            else:
                logger.error(f'\tFAILED check existed - not existed {raw_feat_fp}')
            # Check data length
            raw_feat_file = pd.read_parquet(raw_feat_fp)
            len_df = len(raw_feat_file)
            if len_df > 0:
                logger.debug(f'\tPASSED check valid length')
            else:
                logger.error(f'\tFAILED check valid length - dst {len_df}')
            
            # Check unique CUSTOMER_CDE, no duplicated
            num_unique_cust = raw_feat_file['CUSTOMER_CDE'].nunique()
            if len_df == num_unique_cust:
                logger.debug(f'\tPASSED check duplicated CUSTOMER CDE')
            else:
                logger.error(f'\tFAILED check duplicated CUSTOMER CDE - org {len_df} vs dst {num_unique_cust}')