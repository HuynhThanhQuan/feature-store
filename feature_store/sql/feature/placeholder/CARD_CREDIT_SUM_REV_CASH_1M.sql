/*
Feature Name: CARD_CREDIT_SUM_REV_CASH_1M, CARD_CREDIT_SUM_REV_CASH_3M, CARD_CREDIT_SUM_REV_CASH_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_REV_CASH_1M' FTR_NM,
                                     NVL(SUM(ABS(AMT_BILL)), 0) FTR_VAL,
                                     TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                                     CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
WHERE PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1)
  AND MCC_CDE IN ('6010',
                  '6011',
                  '6211',
                  '6012',
                  '6051')
GROUP BY CUSTOMER_CDE