/*
Feature Name: FAV_POS_6M_CT
Derived From: CINS_TMP_POS_MERCHANT_6M_10082023, CINS_TMP_POS_TERMINAL_6M_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'FAV_POS_6M_CT' FTR_NM,
                       MERCHANT_CDE ||'-'||terminal_id FTR_VAL,
                       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
                       CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          MERCHANT_CDE,
          TERMINAL_ID,
          RPT_DT,
          CT_TXN_TERMINAL,
          ADD_TSTP,
          ROW_NUMBER()OVER(PARTITION BY CUSTOMER_CDE
                           ORDER BY CT_TXN_TERMINAL DESC) RN1
   FROM (
           (SELECT *
            FROM CINS_TMP_POS_MERCHANT_6M_10082023
            WHERE to_date(RPT_DT, 'DD-MM-YY') = to_date('10-08-2023', 'DD-MM-YY') )
         UNION  ALL
           (SELECT CUSTOMER_CDE,
                   MERCHANT_CDE,
                   TERMINAL_ID,
                   TO_CHAR(RPT_DT),
                   CT_TXN_TERMINAL,
                   ADD_TSTP
            FROM CINS_TMP_POS_TERMINAL_6M_10082023
            WHERE to_date(RPT_DT, 'DD-MM-YY') = to_date('10-08-2023', 'DD-MM-YY') )))
WHERE RN1 = 1;

/*
Feature Name: FAV_POS_6M_SM
Derived From: CINS_TMP_POS_TERMINAL_AMT_6M_10082023, CINS_TMP_POS_MERCHANT_AMT_6M_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'FAV_POS_6M_SM' FTR_NM,
                       MERCHANT_CDE ||'-'||terminal_id FTR_VAL,
                       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
                       CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          MERCHANT_CDE,
          TERMINAL_ID,
          RPT_DT,
          AMT_BILL,
          ADD_TSTP,
          ROW_NUMBER()OVER(PARTITION BY CUSTOMER_CDE
                           ORDER BY AMT_BILL DESC) RN1
   FROM (
           (SELECT *
            FROM CINS_TMP_POS_TERMINAL_AMT_6M_10082023
            WHERE to_date(RPT_DT, 'DD-MM-YY') = to_date('10-08-2023', 'DD-MM-YY'))
         UNION  ALL
           (SELECT *
            FROM CINS_TMP_POS_MERCHANT_AMT_6M_10082023
            WHERE to_date(RPT_DT, 'DD-MM-YY') = to_date('10-08-2023', 'DD-MM-YY'))))
WHERE RN1 = 1;
/*
Feature Name: LOR
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'LOR' FTR_NM,
             TO_DATE('10-08-2023', 'DD-MM-YY') - TO_DATE(CUS_OPEN_DT) FTR_VAL,
             TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
             CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.dw_customer_dim
WHERE SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND ACTIVE = '1'
  AND COMPANY_KEY = '1'
  AND TO_DATE(CUS_OPEN_DT) <= TO_DATE('10-08-2023', 'DD-MM-YY') ;

/*
Feature Name: CREDIT_SCORE
Derived From: STG_CRS_CUSTOMER_SCORE, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT B.CUSTOMER_CDE,
       'CREDIT_SCORE' FTR_NM,
                      CASE
                          WHEN A.CREDIT_SCORE IS NULL THEN 0
                          ELSE A.CREDIT_SCORE
                      END FTR_VAL,
                      TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
                      CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CREDIT_SCORE
   FROM
     (SELECT CUSTOMER_CDE,
             CREDIT_SCORE,
             DATE_1,
             PROCESS_DATE,
             ROW_NUMBER()OVER(PARTITION BY CUSTOMER_CDE
                              ORDER BY CREDIT_SCORE DESC) RN
      FROM
        (SELECT trim(' '
                     FROM(CUSTOMER_CDE))CUSTOMER_CDE,
                NVL(FINANCIALSCORE, NONFINANCIALSCORE) CREDIT_SCORE,
                RANK()OVER(PARTITION BY CUSTOMER_CDE
                           ORDER BY DATE_1 DESC)RANK_SCORE,
                      date_1,
                      PROCESS_DATE
         FROM DW_ANALYTICS.STG_CRS_CUSTOMER_SCORE)
      WHERE RANK_SCORE = 1
        AND TO_DATE(DATE_1) < TO_DATE('10-08-2023', 'DD-MM-YY') )
   WHERE RN = 1 ) A
RIGHT JOIN
  (SELECT CUSTOMER_CDE
   FROM DW_ANALYTICS.DW_CUSTOMER_DIM
   WHERE SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND ACTIVE = '1'
     AND COMPANY_KEY = '1') B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_MAX_LIMIT
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_10082023, CINS_TMP_CARD_DIM_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_MAX_LIMIT' AS FTR_NM,
       MAX(TT_CARD_LIMIT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DATA_RPT_CARD_493
WHERE CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_10082023)
  AND SUBSTR(CARD_CDE, 1, 1) = '3'
  AND CARD_CDE IN
    (SELECT CARD_CDE
     FROM CINS_TMP_CARD_DIM_10082023)
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('10-08-2023', 'DD-MM-YY'), -36)
  AND PROCESS_DT < TO_DATE('10-08-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_SACOMPAY_DAY_SINCE_LTST_LOGIN
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          LAST_SIGNED_ON,
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE
                             ORDER BY LAST_SIGNED_ON DESC) RN
   FROM DW_ANALYTICS.DW_EWALL_USER_DIM
   WHERE USER_STATUS = 'A'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_10082023) )
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_DAY_SINCE_LTST_LOGIN' AS FTR_NM,
       AVG(TO_DATE('10-08-2023', 'DD-MM-YY') - TO_DATE(LAST_SIGNED_ON)) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A
WHERE TO_DATE(LAST_SIGNED_ON) < TO_DATE('10-08-2023', 'DD-MM-YY')
  AND TO_DATE(LAST_SIGNED_ON) >= ADD_MONTHS(TO_DATE('10-08-2023', 'DD-MM-YY'), -36)
  AND RN = 1
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_MAX_BRAND_LIMIT
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_DATA_RPT_CARD_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH T AS
  (SELECT A.CUSTOMER_CDE,
          A.CARD_CDE,
          A.CREDIT_BRAND,
          B.TT_CARD_LIMIT
   FROM
     (SELECT CUSTOMER_CDE,
             CARD_CDE,
             CASE
                 WHEN SUBSTR(CARDHOLDER_NO, 1, 1) = '4' THEN 'VISA'
                 WHEN SUBSTR(CARDHOLDER_NO, 1, 1) = '3' THEN 'JCB'
                 WHEN SUBSTR(CARDHOLDER_NO, 1, 1) = '5' THEN 'MASTERCARD'
                 WHEN SUBSTR(CARDHOLDER_NO, 1, 1) = '6' THEN 'UNION'
                 WHEN (SUBSTR(CARDHOLDER_NO, 1) = '9'
                       OR SUBSTR(CARDHOLDER_NO, 1) = '2') THEN 'NAPAS'
                 ELSE 'OTHER'
             END AS CREDIT_BRAND,
             ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE,
                                             CARD_CDE
                                ORDER BY UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
      WHERE SUBSTR(CARD_CDE, 1, 1) = '3'
        AND PLASTIC_CDE = ' '
        AND STATUS_CDE = ' ' ) A
   JOIN
     (SELECT CUSTOMER_CDE,
             CARD_CDE,
             MAX(TT_CARD_LIMIT) AS TT_CARD_LIMIT
      FROM CINS_TMP_DATA_RPT_CARD_10082023
      GROUP BY CUSTOMER_CDE,
               CARD_CDE) B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE
   AND A.CARD_CDE = B.CARD_CDE
   WHERE A.RN = 1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_MAX_BRAND_LIMIT' AS FTR_NM,
       CREDIT_BRAND AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CREDIT_BRAND,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY TT_CARD_LIMIT DESC) RN
   FROM T
   WHERE CUSTOMER_CDE <> '1'
     AND CUSTOMER_CDE <> '-1' )
WHERE RN = 1 ;

/*
Feature Name: CARD_CREDIT_MAX_CLASS
Derived From: DW_CARD_MASTER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
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
   WHERE ACTIVATION_DT <= TO_DATE('10-08-2023', 'DD-MM-YY')
     AND ACTIVATION_DT >= ADD_MONTHS(TO_DATE('10-08-2023', 'DD-MM-YY'), -36)
     AND SUBSTR(CARD_CDE, 1, 1) = '3'
     AND PLASTIC_CDE = ' '
     AND STATUS_CDE = ' '
     AND CUSTOMER_CDE <> '-1'
     AND CUSTOMER_CDE IS NOT NULL
     AND CUSTOMER_CDE NOT LIKE '%#%' )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_MAX_CLASS' AS FTR_NM,
       MAX(CREDIT_CLASS) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          CAST(CREDIT_CLASS AS INT) AS CREDIT_CLASS
   FROM A)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_CT_CONSUMP_LOAN
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
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
           FROM CINS_TMP_CUSTOMER_10082023) )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_CONSUMP_LOAN' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A
WHERE PRODUCT_CDE IN ('3024',
                      '9413',
                      '9415')
  AND ADD_MONTHS(TO_DATE('10-08-2023', 'DD-MM-YY'), -6) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('10-08-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_HOLD
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_HOLD' AS FTR_NM,
       '1' AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT DISTINCT CUSTOMER_CDE
   FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
   WHERE SUBSTR(CARD_CDE, 1, 1) = '3'
     AND STATUS_CDE = ' '
     AND BASIC_SUPP_IND = 'B'
     AND TO_CHAR(LAST_RENEWAL_DT) = '01-JAN-00'
     AND ACTIVATION_DT <= TO_DATE('10-08-2023', 'DD-MM-YY')
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_10082023) );

/*
Feature Name: CASA_HOLD
Derived From: DW_ACCOUNT_MASTER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_HOLD' AS FTR_NM,
       '1' AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT DISTINCT CUSTOMER_CDE
   FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM
   WHERE ACTIVE = 1
     AND COMPANY_KEY = 1
     AND SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND CATEGORY_CDE LIKE '10__'
     AND TO_CHAR(CLOSE_DT) = '01-JAN-00'
     AND OPEN_DT <= TO_DATE('10-08-2023', 'DD-MM-YY') );

/*
Feature Name: CARD_CREDIT_CROSS_SELL_LABEL1
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
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
           FROM CINS_TMP_CUSTOMER_10082023) )
   WHERE RN =1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CROSS_SELL_LABEL1' AS FTR_NM,
       TO_CHAR(ACTIVATION_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY ACTIVATION_DT DESC) RN
   FROM A
   WHERE ACTIVATION_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 1;

/*
Feature Name: CARD_CREDIT_CROSS_SELL_LABEL2
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 WITH A AS
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
           FROM CINS_TMP_CUSTOMER_10082023) )
   WHERE RN =1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CROSS_SELL_LABEL2' AS FTR_NM,
       TO_CHAR(ACTIVATION_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY ACTIVATION_DT DESC) RN
   FROM A
   WHERE ACTIVATION_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 2;

/*
Feature Name: CARD_CREDIT_CROSS_SELL_LABEL3
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
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
           FROM CINS_TMP_CUSTOMER_10082023) )
   WHERE RN =1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CROSS_SELL_LABEL3' AS FTR_NM,
       TO_CHAR(ACTIVATION_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY ACTIVATION_DT DESC) RN
   FROM A
   WHERE ACTIVATION_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 3;

/*
Feature Name: CARD_CREDIT_CROSS_SELL_LABEL4
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
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
           FROM CINS_TMP_CUSTOMER_10082023) )
   WHERE RN =1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CROSS_SELL_LABEL4' AS FTR_NM,
       TO_CHAR(ACTIVATION_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY ACTIVATION_DT DESC) RN
   FROM A
   WHERE ACTIVATION_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 4;

/*
Feature Name: CARD_CREDIT_CROSS_SELL_LABEL5
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
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
           FROM CINS_TMP_CUSTOMER_10082023) )
   WHERE RN =1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CROSS_SELL_LABEL5' AS FTR_NM,
       TO_CHAR(ACTIVATION_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY ACTIVATION_DT DESC) RN
   FROM A
   WHERE ACTIVATION_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 5;

/*
Feature Name: CARD_CREDIT_CROSS_SELL_LABEL6
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
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
           FROM CINS_TMP_CUSTOMER_10082023) )
   WHERE RN =1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CROSS_SELL_LABEL6' AS FTR_NM,
       TO_CHAR(ACTIVATION_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CARD_CDE,
          ACTIVATION_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY ACTIVATION_DT DESC) RN
   FROM A
   WHERE ACTIVATION_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 6;

/*
Feature Name: CASA_CROSS_SELL_LABEL1
Derived From: DW_ACCOUNT_MASTER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT
   FROM
     (SELECT CUSTOMER_CDE,
             ACCT_ID,
             OPEN_DT,
             ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, ACCT_ID
                               ORDER BY UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM
      WHERE ACTIVE = 1
        AND COMPANY_KEY = 1
        AND SUB_SECTOR_CDE IN ('1700',
                               '1602')
        AND CATEGORY_CDE LIKE '10__'
        AND TO_CHAR(CLOSE_DT) = '01-JAN-00' )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'CASA_CROSS_SELL_LABEL1' AS FTR_NM,
       TO_CHAR(OPEN_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY OPEN_DT DESC) RN
   FROM A
   WHERE OPEN_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 1;

/*
Feature Name: CASA_CROSS_SELL_LABEL2
Derived From: DW_ACCOUNT_MASTER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT
   FROM
     (SELECT CUSTOMER_CDE,
             ACCT_ID,
             OPEN_DT,
             ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, ACCT_ID
                               ORDER BY UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM
      WHERE ACTIVE = 1
        AND COMPANY_KEY = 1
        AND SUB_SECTOR_CDE IN ('1700',
                               '1602')
        AND CATEGORY_CDE LIKE '10__'
        AND TO_CHAR(CLOSE_DT) = '01-JAN-00' )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'CASA_CROSS_SELL_LABEL2' AS FTR_NM,
       TO_CHAR(OPEN_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY OPEN_DT DESC) RN
   FROM A
   WHERE OPEN_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 2;

/*
Feature Name: CASA_CROSS_SELL_LABEL3
Derived From: DW_ACCOUNT_MASTER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT
   FROM
     (SELECT CUSTOMER_CDE,
             ACCT_ID,
             OPEN_DT,
             ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, ACCT_ID
                               ORDER BY UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM
      WHERE ACTIVE = 1
        AND COMPANY_KEY = 1
        AND SUB_SECTOR_CDE IN ('1700',
                               '1602')
        AND CATEGORY_CDE LIKE '10__'
        AND TO_CHAR(CLOSE_DT) = '01-JAN-00' )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'CASA_CROSS_SELL_LABEL3' AS FTR_NM,
       TO_CHAR(OPEN_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY OPEN_DT DESC) RN
   FROM A
   WHERE OPEN_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 3;

/*
Feature Name: EB_MB_CROSS_SELL_LABEL1
Derived From: CINS_TMP_EB_MB_CROSSELL_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MB_CROSS_SELL_LABEL1' AS FTR_NM,
       TO_CHAR(INPUT_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CORP_ID,
          INPUT_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY INPUT_DT DESC) RN
   FROM CINS_TMP_EB_MB_CROSSELL_10082023
   WHERE INPUT_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 1 ;

/*
Feature Name: EB_MB_CROSS_SELL_LABEL2
Derived From: CINS_TMP_EB_MB_CROSSELL_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MB_CROSS_SELL_LABEL2' AS FTR_NM,
       TO_CHAR(INPUT_DT) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CORP_ID,
          INPUT_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY INPUT_DT DESC) RN
   FROM CINS_TMP_EB_MB_CROSSELL_10082023
   WHERE INPUT_DT < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 2 ;

/*
Feature Name: SACOMBANK_PAY_CROSS_SELL_LABEL1
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          EWALL_ID,
          FIRST_SIGNED_ON
   FROM
     (SELECT CUSTOMER_CDE,
             EWALL_ID,
             FIRST_SIGNED_ON,
             ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, EWALL_ID
                               ORDER BY REC_UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_EWALL_USER_DIM
      WHERE CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_10082023)
        AND USER_STATUS = 'A' )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'SACOMBANK_PAY_CROSS_SELL_LABEL1' AS FTR_NM,
       TO_CHAR(FIRST_SIGNED_ON) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          EWALL_ID,
          FIRST_SIGNED_ON,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY FIRST_SIGNED_ON DESC) RN
   FROM A
   WHERE FIRST_SIGNED_ON < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 1 ;

/*
Feature Name: SACOMBANK_PAY_CROSS_SELL_LABEL2
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_10082023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          EWALL_ID,
          FIRST_SIGNED_ON
   FROM
     (SELECT CUSTOMER_CDE,
             EWALL_ID,
             FIRST_SIGNED_ON,
             ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, EWALL_ID
                               ORDER BY REC_UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_EWALL_USER_DIM
      WHERE CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_10082023)
        AND USER_STATUS = 'A' )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'SACOMBANK_PAY_CROSS_SELL_LABEL2' AS FTR_NM,
       TO_CHAR(FIRST_SIGNED_ON) AS FTR_VAL,
       TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          EWALL_ID,
          FIRST_SIGNED_ON,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY FIRST_SIGNED_ON DESC) RN
   FROM A
   WHERE FIRST_SIGNED_ON < TO_DATE('10-08-2023', 'DD-MM-YY') )
WHERE RN = 2 ;

/*
Feature Name: AREA_SPLIT
Derived From: DW_CUSTOMER_DIM, DW_ORG_LOCATION_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'AREA_SPLIT' FTR_NM,
                    B.AREA_NAME FTR_VAL,
                    TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
                    CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          SUB_BRANCH_KEY
   FROM DW_ANALYTICS.DW_CUSTOMER_DIM
   WHERE SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND ACTIVE = '1'
     AND COMPANY_KEY = '1'
     AND UPDATE_DT <= TO_DATE('10-08-2023', 'DD-MM-YY') )A
JOIN
  (SELECT *
   FROM DW_ANALYTICS.DW_ORG_LOCATION_DIM
   WHERE ACTIVE = '1'
     AND COMPANY_KEY = '1') B ON A.SUB_BRANCH_KEY = B.SUB_BRANCH_KEY;

/*
Feature Name: ADDR_TOWN
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'ADDR_TOWN' FTR_NM,
                   A.TOWN_COUNTRY FTR_VAL,
                   TO_DATE('10-08-2023', 'DD-MM-YY') AS RPT_DT,
                   CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          TOWN_COUNTRY
   FROM DW_ANALYTICS.DW_CUSTOMER_DIM
   WHERE SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND ACTIVE = '1'
     AND COMPANY_KEY = '1' ) A