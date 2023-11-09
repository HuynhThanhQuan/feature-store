/*
Feature Name: CASA_AVG_BAL_1M
Derived From: 
  DW_ANALYTICS.DW_DEPOSIT_FCT: CATEGORY_CDE,CUSTOMER_CDE,ACTUAL_BAL_LCL,PROCESS_DT
  CINS_TMP_CUSTOMER_{RPT_DT_TBL}
Tags: CASA
TW: 1M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
  'CASA_AVG_BAL_1M' AS FTR_NM,
  AVG(ACTUAL_BAL_LCL) AS FTR_VAL,
  TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT
WHERE CATEGORY_CDE LIKE '10__'
  AND CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
  AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE