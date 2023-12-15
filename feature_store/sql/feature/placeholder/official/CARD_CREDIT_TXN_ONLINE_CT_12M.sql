/*
Feature Name: CARD_CREDIT_TXN_ONLINE_CT_12M
Derived From:
  CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}:
  - TXN_OL_CDE
  - PROCESS_DT
TW: 12M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
    'CARD_CREDIT_TXN_ONLINE_CT_12M' AS FTR_NM,
    COUNT(*) AS FTR_VAL,
    TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
WHERE TXN_OL_CDE = 'E'
  AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -12) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE