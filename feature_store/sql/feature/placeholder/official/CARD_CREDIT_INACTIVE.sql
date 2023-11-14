/*
Feature Name: CARD_CREDIT_INACTIVE
Derived From: 
    DW_ANALYTICS.DW_CUSTOMER_DIM: 
        - CUSTOMER_CDE
        -  ACTIVE
        -  COMPANY_KEY
        -  SUB_SECTOR_CDE
    DW_ANALYTICS.DW_CARD_MASTER_DIM: 
        - CUSTOMER_CDE
        - CARD_CDE
        - STATUS_CDE
        - ACTIVATION_DT
    DW_ANALYTICS.DW_CARD_TRANSACTION_FCT: 
        - CARD_CDE
        - TRAN_STATUS
        - POST_DT
        - MCC_CDE
        - PROCESS_DT
        - CUSTOMER_CDE
Tags: 
    - LABEL
    - CARD
TW: 3M
*/
INSERT INTO {TBL_NM}
WITH 
T1 AS (
    SELECT CUSTOMER_CDE
    FROM DW_ANALYTICS.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1
    AND COMPANY_KEY = 1
    AND SUB_SECTOR_CDE IN ('1700','1602')
),
T2 AS (
    SELECT DISTINCT CUSTOMER_CDE
    FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
    WHERE CARD_CDE LIKE '3%'
        AND STATUS_CDE = ' '
        AND ACTIVATION_DT < ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3)
        AND TO_CHAR(ACTIVATION_DT, 'yyyy') > 1900
        AND CUSTOMER_CDE IS NOT NULL
),
T3 AS (
    SELECT DISTINCT CUSTOMER_CDE
    FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
    WHERE CARD_CDE LIKE '3%'
        AND TRAN_STATUS = 'S'
        AND POST_DT IS NOT NULL
        AND MCC_CDE NOT IN (0,4829)
        AND MCC_CDE IS NOT NULL
        AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
        AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3)
        AND CUSTOMER_CDE IS NOT NULL
),
T4 AS (
    SELECT DISTINCT T1.CUSTOMER_CDE
    FROM T1
    LEFT JOIN T2 ON T1.CUSTOMER_CDE = T2.CUSTOMER_CDE
    LEFT JOIN T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_CDE
    WHERE T2.CUSTOMER_CDE IS NOT NULL 
        AND T3.CUSTOMER_CDE IS NULL
)

SELECT 
    CUSTOMER_CDE,
    'CARD_CREDIT_INACTIVE' FTR_NM,
    1 FTR_VAL,
    TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM T4