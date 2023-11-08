/*
Feature Name: CARD_CREDIT_SUM_TXN_INTER_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_TXN_INTER_1M' AS FTR_NM,
       SUM(AMT_BILL) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
WHERE ACQ_CNTRY_CDE <> '704'
  AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE