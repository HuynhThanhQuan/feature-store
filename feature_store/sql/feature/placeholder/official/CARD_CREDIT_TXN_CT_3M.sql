/*
Feature Name: CARD_CREDIT_TXN_CT_3M
Derived From:
  CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}:
  - PROCESS_DT
TW: 3M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
  'CARD_CREDIT_TXN_CT_3M' AS FTR_NM,
  COUNT(*) AS FTR_VAL,
  TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
WHERE PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3)
GROUP BY CUSTOMER_CDE