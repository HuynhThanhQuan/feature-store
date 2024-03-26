QUERY_CUSTOMER_CDE_RPT_DT = """
    SELECT DISTINCT CUSTOMER_CDE FROM CINS_REACTIVATED_CASA_CUSTOMER_%s
"""

QUERY_RAW_DATA_FROM_FEATURE_STORE = """
    SELECT T1.CUSTOMER_CDE, T1.FTR_NM, T1.FTR_VAL, T1.RPT_DT 
    FROM CINS_FEATURE_STORE_REACTIVATED_%s T1
    RIGHT JOIN CINS_REACTIVATED_CASA_CUSTOMER_%s T2 ON T1.CUSTOMER_CDE = T2.CUSTOMER_CDE
    WHERE T1.RPT_DT = TO_DATE('%s', 'DD-MM-YY')
    AND T1.FTR_NM = '%s'
"""

# CUSTOMER CASA SCORE

QUERY_CREATE_CUSTOMER_SCORE_TABLE = """
    CREATE TABLE CINS_REACTIVATED_CASA_SCORE_%s (
        CUSTOMER_CDE VARCHAR2(30 BYTE),
        SCORE NUMBER
    )
"""

QUERY_DROP_CUSTOMER_SCORE_TABLE = """
    DROP TABLE CINS_REACTIVATED_CASA_SCORE_%s
"""


QUERY_INSERT_CUSTOMER_SCORE = """
    INSERT INTO CINS_REACTIVATED_CASA_SCORE_%s (CUSTOMER_CDE, SCORE)
    VALUES (:1, :2)
"""

# CASA RAW MATRIX DATA

QUERY_CREATE_RAW_MATRIX_DATA_TABLE = """
    CREATE TABLE CINS_REACTIVATED_CASA_RAW_MATRIX_DATA_%s (
        CUSTOMER_CDE VARCHAR2(30 BYTE),
        # CASA_HOLD NUMBER,
        # CARD_CREDIT_HOLD NUMBER,
        # EB_SACOMPAY_HOLD NUMBER,
        # EB_MBIB_HOLD NUMBER,
        # LOR NUMBER,
        # CREDIT_SCORE NUMBER,
        # CASA_BAL_SUM_NOW NUMBER,
        # CASA_DAY_SINCE_LAST_TXN_CT_36M NUMBER,
        # CARD_CREDIT_MAX_LIMIT NUMBER,
        # CARD_CREDIT_SUM_BAL_NOW NUMBER,
        # EB_SACOMPAY_DAY_SINCE_LTST_LOGIN NUMBER,
        # EB_SACOMPAY_DAY_SINCE_LTST_TXN NUMBER,
        # EB_MBIB_DAY_SINCE_ACTIVE NUMBER,
        # LIFE_STG VARCHAR2(30 BYTE),
        # AREA VARCHAR2(30 BYTE)
    )
"""

QUERY_DROP_RAW_MATRIX_DATA_TABLE = """
    DROP TABLE CINS_REACTIVATED_CASA_RAW_MATRIX_DATA_%s
"""


QUERY_INSERT_RAW_MATRIX_DATA_TABLE = """
    INSERT INTO CINS_REACTIVATED_CASA_RAW_MATRIX_DATA_%s (CUSTOMER_CDE, CASA_HOLD, CARD_CREDIT_HOLD, EB_SACOMPAY_HOLD, EB_MBIB_HOLD, LOR, CREDIT_SCORE, CASA_BAL_SUM_NOW, CASA_DAY_SINCE_LAST_TXN_CT_36M, CARD_CREDIT_MAX_LIMIT, CARD_CREDIT_SUM_BAL_NOW, EB_SACOMPAY_DAY_SINCE_LTST_LOGIN, EB_SACOMPAY_DAY_SINCE_LTST_TXN, EB_MBIB_DAY_SINCE_ACTIVE, LIFE_STG, AREA)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)
"""