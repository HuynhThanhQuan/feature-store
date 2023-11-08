/*
Feature Name: CARD_CREDIT_MAX_CLASS
Derived From: DW_CARD_MASTER_DIM
*/
INSERT INTO {TBL_NM}
WITH A AS
  (SELECT DISTINCT CUSTOMER_CDE,
                   CARD_CDE,
                   CASE
                       WHEN (SUBSTR(CARDHOLDER_NO, 1, 6) = '512341'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '356480'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '472074'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '970403'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '211241'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '486265') THEN '1'
                       WHEN (SUBSTR(CARDHOLDER_NO, 1, 6) = '472075'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '356481'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '526830'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '625002') THEN '2'
                       WHEN (SUBSTR(CARDHOLDER_NO, 1, 6) = '436438'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '423238') THEN '3'
                       WHEN SUBSTR(CARDHOLDER_NO, 1, 6) = '455376' THEN '4'
                       WHEN (SUBSTR(CARDHOLDER_NO, 1, 6) = '555715'
                             OR SUBSTR(CARDHOLDER_NO, 1, 6) = '552332') THEN '5'
                       WHEN SUBSTR(CARDHOLDER_NO, 1, 6) = '356062' THEN '6'
                       WHEN SUBSTR(CARDHOLDER_NO, 1, 6) = '466243' THEN '7'
                       ELSE '0'
                   END AS CREDIT_CLASS
   FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
   WHERE ACTIVATION_DT <= TO_DATE('{RPT_DT}', 'DD-MM-YY')
     AND ACTIVATION_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
     AND SUBSTR(CARD_CDE, 1, 1) = '3'
     AND PLASTIC_CDE = ' '
     AND STATUS_CDE = ' '
     AND CUSTOMER_CDE <> '-1'
     AND CUSTOMER_CDE IS NOT NULL
     AND CUSTOMER_CDE NOT LIKE '%#%' )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_MAX_CLASS' AS FTR_NM,
       MAX(CREDIT_CLASS) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          CAST(CREDIT_CLASS AS INT) AS CREDIT_CLASS
   FROM A)
GROUP BY CUSTOMER_CDE