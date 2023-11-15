/*
Feature Name: CASA_TXN_AMT_AVG_6M
Derived From:
  DW_ANALYTICS.DWA_STMT_EBANK:
  - AMT_LCY
  - PRODUCT_CATEGORY
  - PROCESS_DT
  - CUSTOMER_ID
  - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE:
  - TRANSACTION_CODE
  - INITIATION
  CINS_TMP_CUSTOMER_{RPT_DT_TBL}:
  - CUSTOMER_CDE
Tags:
- CASA
TW: 6M
*/
INSERT INTO {TBL_NM}
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_TXN_AMT_AVG_6M' AS FTR_NM,
    NVL(AVG(ABS(TXN.AMT_LCY)), 0) AS FTR_VAL,
    TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL} TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER'  
    AND TXN.PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -6)
GROUP BY TXN.CUSTOMER_ID;