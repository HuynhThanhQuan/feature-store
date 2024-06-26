/*
Feature Name: CARD_CREDIT_TXN_INTER_CT_3M
Derived From:
  CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}:
  - ACQ_CNTRY_CDE
  - PROCESS_DT
TW: 3M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
  'CARD_CREDIT_TXN_INTER_CT_3M' AS FTR_NM,
  COUNT(*) AS FTR_VAL,
  TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
WHERE ACQ_CNTRY_CDE <> '704'
  AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE