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
        SCORE NUMBER,
        RANK NUMBER
    )
"""

QUERY_DROP_CUSTOMER_SCORE_TABLE = """
    DROP TABLE CINS_REACTIVATED_CASA_SCORE_%s
"""


QUERY_INSERT_CUSTOMER_SCORE = """
    INSERT INTO CINS_REACTIVATED_CASA_SCORE_%s (CUSTOMER_CDE, SCORE, RANK)
    VALUES (:1, :2, :3)
"""