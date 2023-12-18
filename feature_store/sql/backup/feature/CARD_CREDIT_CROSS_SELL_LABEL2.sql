/*
Feature Name: CARD_CREDIT_CROSS_SELL_LABEL2
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM} WITH A AS
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT
   FROM
     (SELECT CUSTOMER_CDE,
             CARD_CDE,
             ACTIVATION_DT,
             ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CARD_CDE
                               ORDER BY UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
      WHERE SUBSTR(CARD_CDE, 1, 1) = '3'
        AND STATUS_CDE = ' '
        AND PLASTIC_CDE = ' '
        AND BASIC_SUPP_IND = 'B'
        AND TO_CHAR(LAST_RENEWAL_DT) = '01-JAN-00'
        AND CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL}) )
   WHERE RN =1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CROSS_SELL_LABEL2' AS FTR_NM,
       TO_CHAR(ACTIVATION_DT) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY ACTIVATION_DT DESC) RN
   FROM A
   WHERE ACTIVATION_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY') )
WHERE RN = 2