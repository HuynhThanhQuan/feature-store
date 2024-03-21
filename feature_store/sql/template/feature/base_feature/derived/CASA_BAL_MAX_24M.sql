/*
Feature Name: CASA_BAL_MAX_24M
Derived From:
  DW_ANALYTICS.DW_DEPOSIT_FCT:
  - CATEGORY_CDE
  - CUSTOMER_CDE
  - ACTUAL_BAL_LCL
  - PROCESS_DT
  CINS_TMP_CUSTOMER_{RPT_DT_TBL}:
  - CUSTOMER_CDE
Tags:
- CASA
- MONETARY
TW: 24M
*/
INSERT INTO {TBL_NM}
SELECT
  DF.CUSTOMER_CDE,
  'CASA_BAL_MAX_24M' AS FTR_NM,
  MAX(DF.ACTUAL_BAL_LCL) AS FTR_VAL,
  TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT DF
JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL} TMP ON DF.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DF.CATEGORY_CDE LIKE '10__'
  AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -24) <= DF.PROCESS_DT
  AND DF.PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
GROUP BY DF.CUSTOMER_CDE;