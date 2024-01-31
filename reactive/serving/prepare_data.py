import pathlib
import sql_template
import util
import constant
import logging
logger = logging.getLogger(__name__) 


DATA_LOC = pathlib.Path('./data')


def get_data(report_date, column_names=constant.RAW_FEATURE_NAMES):
    download_raw_feature_data(report_date, column_names)
    raw_feat_fp = merge_raw_feature_data(report_date, column_names)
    df = util.reload(raw_feat_fp)
    df = df.set_index('CUSTOMER_CDE')
    return df


def download_raw_feature_data(rpt_dt, raw_nms):
    """
    Download raw data from CINS_FEATURE_STORE_REACTIVATED_{RPT_DT} with selected report-date
    """
    rpt_dt_fp = DATA_LOC / rpt_dt
    rpt_dt_tbl = rpt_dt.replace('-','')

    # Init or reused RPT-DT folder
    if not os.path.exists(rpt_dt_fp):
        logger.info(f'Create RPT_DT {rpt_dt}')
        os.makedirs(rpt_dt_fp)
    # Download customer-cde of rpt-dt
    logger.info(f'Downloading CUSTOMER_CDE of {rpt_dt}')
    query = sql_template.QUERY_CUSTOMER_CDE_RPT_DT % rpt_dt_tbl
    filename = rpt_dt_fp / 'CUSTOMER_CDE'
    util.download_to_parquet(filename, query)

    # Download all raw-feature data
    for raw_nm in raw_nms:
        logger.info(f'Downloading raw-data {raw_nm}')
        query = sql_template.QUERY_RAW_DATA_FROM_FEATURE_STORE % (rpt_dt, raw_nm)
        filename = rpt_dt_fp / raw_nm
        util.download_to_parquet(filename, query)


def merge_raw_feature_data(rpt_dt, raw_nms):
    """
    After downloading raw data, merge them together to build raw-feat matrix
    """
    rpt_dt_fp = DATA_LOC / rpt_dt
    cust_id = util.reload(rpt_dt_fp / 'CUSTOMER_CDE')

    for raw_nm in raw_nms:
        logger.debug('Merging ', raw_nm)
        raw_fn = rpt_dt_fp / raw_nm
        if raw_fn.exists():
            df = util.reload(raw_fn)
            df = df[['CUSTOMER_CDE', 'FTR_VAL']]
            df.columns = ['CUSTOMER_CDE', raw_nm]
            cust_id = cust_id.merge(df, how='left', left_on='CUSTOMER_CDE', right_on='CUSTOMER_CDE')
            raw_feat_fp = rpt_dt_fp / 'RAW_FEATURE_DATA'
            cust_id.to_parquet(raw_feat_fp)
            return raw_feat_fp
        else:
            raise FileNotFoundError(f'{str(raw_fn)} not exists, please check data in Feature Store')