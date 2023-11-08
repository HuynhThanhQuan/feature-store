/*
Feature Name: CARD_CREDIT_UP_SELL_LABEL3_6M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM} 
WITH A AS
  (SELECT CUSTOMER_CDE ,
          PRODUCT_CDE ,
          PROCESS_DT
   FROM
     (SELECT CUSTOMER_CDE ,
             PRODUCT_CDE ,
             PROCESS_DT ,
             ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE,
                                             CARD_CDE,
                                             PROCESS_DT,
                                             APPROVAL_CDE,
                                             RETRVL_REFNO
                                ORDER BY NULL) RN
      FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
      WHERE SUBSTR(CARD_CDE, 1, 1) = '3'
        AND CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL}) )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_UP_SELL_LABEL3_6M' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A
WHERE PRODUCT_CDE IN ('3024',
                      '9413',
                      '9415')
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
GROUP BY CUSTOMER_CDE