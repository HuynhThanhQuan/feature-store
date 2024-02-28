import constant



def transform(X):
    # check feature names must be matched
    missing_cols = set(constant.RAW_FEATURE_NAMES) - set(X.columns.tolist())
    if len(missing_cols) > 0:
        raise Exception(f'Missing columns {missing_cols}')
    # order columns before going next
    X = X[constant.RAW_FEATURE_NAMES]

    # format data types
    X['CASA_HOLD'] = pd.to_numeric(X['CASA_HOLD'],errors='coerce')
    X['CARD_CREDIT_HOLD'] = pd.to_numeric(X['CARD_CREDIT_HOLD'],errors='coerce')
    X['EB_SACOMPAY_HOLD'] = pd.to_numeric(X['EB_SACOMPAY_HOLD'],errors='coerce')
    X['EB_MBIB_HOLD'] = pd.to_numeric(X['EB_MBIB_HOLD'],errors='coerce')
    X['LOR'] = pd.to_numeric(X['LOR'],errors='coerce')
    X['CREDIT_SCORE'] = pd.to_numeric(X['CREDIT_SCORE'],errors='coerce')
    X['CASA_BAL_SUM_NOW'] = pd.to_numeric(X['CASA_BAL_SUM_NOW'],errors='coerce')
    X['CASA_DAY_SINCE_LAST_TXN_CT_36M'] = pd.to_numeric(X['CASA_DAY_SINCE_LAST_TXN_CT_36M'],errors='coerce')
    X['CARD_CREDIT_MAX_LIMIT'] = pd.to_numeric(X['CARD_CREDIT_MAX_LIMIT'],errors='coerce')
    X['CARD_CREDIT_SUM_BAL_NOW'] = pd.to_numeric(X['CARD_CREDIT_SUM_BAL_NOW'],errors='coerce')
    X['EB_SACOMPAY_DAY_SINCE_LTST_LOGIN'] = pd.to_numeric(X['EB_SACOMPAY_DAY_SINCE_LTST_LOGIN'],errors='coerce')
    X['EB_SACOMPAY_DAY_SINCE_LTST_TXN'] = pd.to_numeric(X['EB_SACOMPAY_DAY_SINCE_LTST_TXN'],errors='coerce')
    X['EB_MBIB_DAY_SINCE_ACTIVE'] = pd.to_numeric(X['EB_MBIB_DAY_SINCE_ACTIVE'],errors='coerce')

    # fill value some special columns
    for col in ['CASA_HOLD', 'CARD_CREDIT_HOLD', 'EB_SACOMPAY_HOLD', 'EB_MBIB_HOLD']:
        X[col].fillna(0, inplace=True)
        X[col] = X[col].astype(int)

    preprocessor = pickle.load(open('./artifact/preprocessor','rb'))
    X_tf = preprocessor.transform(X)
    return X_tf
