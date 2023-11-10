/*
Feature Name: CASA_CT_TXN_1M
Derived From: 
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - STMT_ENTRY_ID
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - PROCESS_DT
    - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_{RPT_DT_TBL}:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 1M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_CT_TXN_1M' AS FTR_NM,
    COUNT(STMT_ENTRY_ID) AS FTR_VAL,
    TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DW_ANALYTICS.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
GROUP BY CUSTOMER_ID