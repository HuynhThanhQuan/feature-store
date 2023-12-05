/*
Feature Name: CASA_ACCT_CT_36M
Derived From: 
  DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM:  
    - ACCT_ID
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
  - BEHAVIORAL
TW: 36M
*/
INSERT INTO {TBL_NM}
SELECT
  DIM.CUSTOMER_CDE,
  'CASA_ACCT_CT_36M' FTR_NM,
  COUNT(DISTINCT DIM.ACCT_ID) FTR_VAL,
  TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM DIM
JOIN DW_ANALYTICS.DWA_STMT_EBANK FCT ON DIM.CUSTOMER_CDE = FCT.CUSTOMER_ID
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON FCT.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL} TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.ACCT_ID = FCT.ACCOUNT_NUMBER
  AND TC.INITIATION = 'CUSTOMER'
  AND FCT.PRODUCT_CATEGORY LIKE '10__'
  AND FCT.PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND FCT.PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
GROUP BY DIM.CUSTOMER_CDE;