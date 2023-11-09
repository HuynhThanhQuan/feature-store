/*
Feature Name: CASA_CT_ACCT_ACTIVE
Derived From: 
  DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM:  
    - ACCT_ID
    - ACTIVE
    - CUSTOMER_CDE
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - TRANSACTION_CODE
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - ACCOUNT_NUMBER
    - PROCESS_DT
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_{RPT_DT_TBL}:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 12M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CASA_CT_ACCT_ACTIVE' FTR_NM,
        COUNT(DISTINCT ACCT_ID) FTR_VAL,
        TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM DIM
WHERE ACTIVE = 1
  AND EXISTS
    (SELECT CUSTOMER_ID
     FROM DW_ANALYTICS.DWA_STMT_EBANK FCT
     JOIN
       (SELECT TRANSACTION_CODE
        FROM DW_ANALYTICS.TRANSACTION_CODE
        WHERE INITIATION = 'CUSTOMER') TC ON FCT.TRANSACTION_CODE = TC.TRANSACTION_CODE
     WHERE DIM.CUSTOMER_CDE = FCT.CUSTOMER_ID
       AND PRODUCT_CATEGORY LIKE '10__'
       AND CUSTOMER_ID IN
         (SELECT CUSTOMER_CDE
          FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
       AND DIM.ACCT_ID = FCT.ACCOUNT_NUMBER
       AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -12) )
GROUP BY CUSTOMER_CDE