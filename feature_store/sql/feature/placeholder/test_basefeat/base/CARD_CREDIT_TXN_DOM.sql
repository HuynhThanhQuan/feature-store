/*
Feature Name: CARD_CREDIT_TXN_DOM
Derived From: 
  CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}:
    - PROCESS_DT
    - ACQ_CNTRY_CDE
Derived By:
  Aggregations:
    - CT
  Time-Windows: 
    - 1M
    - 3M
    - 6M
    - 12M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       '{{FEATURE_NAME}}' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
WHERE ACQ_CNTRY_CDE = '704'
  AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -{{MONTH_WINDOW}}) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE