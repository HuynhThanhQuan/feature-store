import pandas as pd
import logging
import constant
logger = logging.getLogger(__name__) 


# def format_datatype(X):
#     """
#     Format data types due to un-recognize data types after download from Oralce DW
#     """
#     X['LOR'] = pd.to_numeric(X['LOR'],errors='coerce')
#     X['CREDIT_SCORE'] = pd.to_numeric(X['CREDIT_SCORE'],errors='coerce')
#     X['CASA_HOLD'] = pd.to_numeric(X['CASA_HOLD'],errors='coerce')
#     X['CASA_BAL_SUM_NOW'] = pd.to_numeric(X['CASA_BAL_SUM_NOW'],errors='coerce')
#     X['CASA_BAL_SUM_36M'] = pd.to_numeric(X['CASA_BAL_SUM_36M'],errors='coerce')
#     X['CASA_BAL_SUM_24M'] = pd.to_numeric(X['CASA_BAL_SUM_24M'],errors='coerce')
#     X['CASA_BAL_SUM_12M'] = pd.to_numeric(X['CASA_BAL_SUM_12M'],errors='coerce')
#     X['CASA_BAL_MAX_12M'] = pd.to_numeric(X['CASA_BAL_MAX_12M'],errors='coerce')
#     X['CASA_TXN_AMT_SUM_12M'] = pd.to_numeric(X['CASA_TXN_AMT_SUM_12M'],errors='coerce')
#     X['CASA_TXN_AMT_SUM_24M'] = pd.to_numeric(X['CASA_TXN_AMT_SUM_24M'],errors='coerce')
#     X['CASA_TXN_AMT_SUM_36M'] = pd.to_numeric(X['CASA_TXN_AMT_SUM_36M'],errors='coerce')
#     X['CASA_TXN_CT_12M'] = pd.to_numeric(X['CASA_TXN_CT_12M'],errors='coerce')
#     X['CASA_TXN_CT_24M'] = pd.to_numeric(X['CASA_TXN_CT_24M'],errors='coerce')
#     X['CASA_TXN_CT_36M'] = pd.to_numeric(X['CASA_TXN_CT_36M'],errors='coerce')
#     X['CASA_ACCT_CT_36M'] = pd.to_numeric(X['CASA_ACCT_CT_36M'],errors='coerce')
#     X['CASA_ACCT_ACTIVE_CT_12M'] = pd.to_numeric(X['CASA_ACCT_ACTIVE_CT_12M'],errors='coerce')
#     X['CASA_DAY_SINCE_LAST_TXN_CT_36M'] = pd.to_numeric(X['CASA_DAY_SINCE_LAST_TXN_CT_36M'],errors='coerce')
#     return X


def format_datatype(df):
    """
    Format data types due to un-recognize data types after download from Oralce DW
    """
    for feat_nm in constant.NUMERICAL_FEATURES:
        df[feat_nm] = pd.to_numeric(df[feat_nm],errors='coerce')
    return df