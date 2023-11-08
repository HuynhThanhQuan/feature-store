/*
Feature Name: CARD_CREDIT_LIMIT_TXN_M1
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}, CINS_TMP_DATA_RPT_CARD_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT A.CUSTOMER_CDE,
       'CARD_CREDIT_LIMIT_TXN_M1' AS FTR_NM,
       ROUND(A.AMT_BILL/B.TT_CARD_LIMIT, 4) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          SUM(AMT_BILL) AS AMT_BILL
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
   WHERE CUSTOMER_CDE IS NOT NULL
     AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
   GROUP BY CUSTOMER_CDE) A
LEFT JOIN
  (SELECT CUSTOMER_CDE,
          SUM(TT_CARD_LIMIT) AS TT_CARD_LIMIT
   FROM CINS_TMP_DATA_RPT_CARD_{RPT_DT_TBL}
   GROUP BY CUSTOMER_CDE) B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE