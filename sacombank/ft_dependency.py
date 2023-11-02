import os
from pathlib import Path
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def collect(feat_meta):
    all_feat_tbls = []
    for re in feat_meta:
        if re:
            feats = re.get('features')
            tbls = re.get('derived_tables')
            if feats and tbls:
                for f in feats:
                    for t in tbls:
                        all_feat_tbls.append((f,t))
    return all_feat_tbls


def analyze(response):
    metadata_folder = 'metadata'
    # From newest to oldest file
    dec_metadata_files = sorted(os.listdir(metadata_folder))[::-1]
    if len(dec_metadata_files) > 0:
        logger.info('Finding latest valid metadata file')
        last_valid_meta_fn, data = None, None
        for i in dec_metadata_files:
            with open (os.path.join(metadata_folder, i), 'r') as f:
                data = json.load(f)
                if 'FEATURE_SQL_JOBS' in data.keys():
                    last_valid_meta_fn = i
                    break
        if last_valid_meta_fn and data:
            # Collect 
            logger.info(f'Analyze latest metadata file {last_valid_meta_fn}')
            feat_meta = data['FEATURE_SQL_JOBS']
            logger.debug(f'Number of features {len(feat_meta)} to be analyzed')
            all_feat_tbls = collect(feat_meta)
            logger.debug(f'Found {len(all_feat_tbls)} feature-table links')
            
            # Save Output
            exc_time = response['EXECUTION_TIMESTAMP']
            output_fn = f'./output/feature_dependency_{last_valid_meta_fn}_{exc_time}.csv'
            output_df = pd.DataFrame(data=all_feat_tbls, columns=['feature_name', 'derived_table'])
            output_df.to_csv(output_fn)
            logger.debug(f'Save output at {output_fn}')
            
            return {
                'FEATURE_DEPENDENCY': {
                    'feature_dependency': all_feat_tbls
                }
            }
            return {
                'FEATURE_DEPENDENCY': {
                    'last_metadata': last_valid_meta_fn
                }
            }
        else:
            logger.info('Not Found valid metadata')
            return {
                "FEATURE_DEPENDENCY": "Not Found valid metadata"
            }
    else:
        logger.info('No metadata found')
        return {
            "FEATURE_DEPENDENCY": "No metadata found"
        }