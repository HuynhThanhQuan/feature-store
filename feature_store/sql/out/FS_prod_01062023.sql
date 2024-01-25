DROP TABLE CINS_TMP_CUSTOMER_01062023;


COMMIT;


DROP TABLE CINS_TMP_CUSTOMER_STATUS_01062023;


COMMIT;


CREATE TABLE CINS_TMP_CUSTOMER_01062023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE)
);


COMMIT;


/*
Table Name: CINS_TMP_CUSTOMER_01062023
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
*/
INSERT INTO CINS_TMP_CUSTOMER_01062023
SELECT CUSTOMER_CDE 
FROM (
    SELECT DISTINCT CUSTOMER_CDE 
    FROM DWPROD.DW_CUSTOMER_DIM
    WHERE SUB_SECTOR_CDE IN ('1700', '1602') 
    AND ACTIVE = '1' 
    AND COMPANY_KEY = '1'
    AND CUSTOMER_CDE IS NOT NULL
);


COMMIT;


CREATE TABLE CINS_TMP_CUSTOMER_STATUS_01062023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    CUST_STT NUMBER,
    CUST_STT_CHG NUMBER
);


COMMIT;


/*
Table Name: CINS_TMP_CUSTOMER_STATUS_01062023
Derived From: 
  DWPROD.DW_CUST_PRODUCT_LOC_FCT: 
    - CUST_STATUS
    - PROCESS_DT
    - CUSTOMER_CDE
  CINS_TMP_CUSTOMER_01062023: 
    - CUSTOMER_CDE
*/
INSERT INTO CINS_TMP_CUSTOMER_STATUS_01062023
SELECT A.CUSTOMER_CDE, A.RPT_DT, A.CUST_STT, A.CUST_STT - LAG(A.CUST_STT) OVER (PARTITION BY A.CUSTOMER_CDE ORDER BY A.RPT_DT) CUST_STT_CHG
FROM (
    SELECT T.CUSTOMER_CDE, T.PROCESS_DT RPT_DT,
        MAX(CASE
            WHEN T.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T.CUST_STATUS = 'DONG BANG' THEN 0
        END) CUST_STT
    FROM DWPROD.DW_CUST_PRODUCT_LOC_FCT T
        JOIN CINS_TMP_CUSTOMER_01062023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    WHERE 
        T.PROCESS_DT = ADD_MONTHS(TO_DATE('01-06-2023', 'DD-MM-YY'), -1)
        OR T.PROCESS_DT = TO_DATE('01-06-2023', 'DD-MM-YY')
    GROUP BY 
        T.CUSTOMER_CDE, 
        T.PROCESS_DT
) A;


COMMIT;


/*
Feature Name: REACTIVATED
Derived From: 
  CINS_TMP_CUSTOMER_STATUS_01062023: 
    - CUSTOMER_CDE 
    - CUST_STT 
    - CUST_STT_CHG 
    - RPT_DT
Tags: 
  - LABEL 
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'REACTIVATED' FTR_NM,
       1 FTR_VAL,
       TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CUSTOMER_STATUS_01062023
WHERE CUST_STT = 2
  AND CUST_STT_CHG = 1
  AND RPT_DT = TO_DATE('01-06-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: CASA_HOLD
Derived From: 
    DW_ACCOUNT_MASTER_DIM:
        - CUSTOMER_CDE
        - ACTIVE
        - COMPANY_KEY
        - SUB_SECTOR_CDE
        - CATEGORY_CDE
        - OPEN_DT
        - CLOSE_DT
Tags:
    - CASA
TW: 1M
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT
    DISTINCT AMD.CUSTOMER_CDE,
    'CASA_HOLD' AS FTR_NM,
    '1' AS FTR_VAL,
    TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_ACCOUNT_MASTER_DIM AMD
WHERE AMD.ACTIVE = 1
    AND AMD.COMPANY_KEY = 1
    AND AMD.SUB_SECTOR_CDE IN ('1700', '1602')
    AND AMD.CATEGORY_CDE LIKE '10__'
    AND TO_CHAR(AMD.CLOSE_DT) = '01-JAN-00'
    AND AMD.OPEN_DT <= TO_DATE('01-06-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: CARD_CREDIT_HOLD
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_HOLD' AS FTR_NM,
       '1' AS FTR_VAL,
       TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT DISTINCT CUSTOMER_CDE
   FROM DWPROD.DW_CARD_MASTER_DIM
   WHERE SUBSTR(CARD_CDE, 1, 1) = '3'
     AND STATUS_CDE = ' '
     AND BASIC_SUPP_IND = 'B'
     AND TO_CHAR(LAST_RENEWAL_DT) = '01-JAN-00'
     AND ACTIVATION_DT <= TO_DATE('01-06-2023', 'DD-MM-YY')
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_01062023) );


COMMIT;


/*
Feature Name: EB_SACOMPAY_HOLD
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT DISTINCT CUSTOMER_CDE,
                'EB_SACOMPAY_HOLD' FTR_NM,
                1 FTR_VAL,
                TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
                CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_EWALL_USER_DIM
WHERE FIRST_SIGNED_ON < TO_DATE('01-06-2023', 'DD-MM-YY')
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01062023);


COMMIT;


/*
Feature Name: EB_MBIB_HOLD
Derived From: DW_EB_USER, CINS_TMP_CUSTOMER_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT DISTINCT CUSTOMER_CDE,
                'EB_MBIB_HOLD' FTR_NM,
                1 FTR_VAL,
                TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
                CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_EB_USER
WHERE PROCESS_DT IN
    (SELECT MAX(PROCESS_DT)
     FROM DWPROD.DW_EB_USER
     WHERE PROCESS_DT < TO_DATE('01-06-2023', 'DD-MM-YY') )
  AND DEL_FLG = 'N'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01062023);


COMMIT;


/*
Feature Name: LIFE_STG
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
    - CUS_OPEN_DT
    - BIRTHDAY
Tags: 
  - DEMOGRAPHIC
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
  'LIFE_STG' FTR_NM,
  CASE
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-06-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 18 AND 26 THEN 'Bắt đầu sự nghiệp'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-06-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 27 AND 35 THEN 'Lập gia đình'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-06-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 36 AND 45 THEN 'Thiết lập tài sản'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-06-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 46 AND 54 THEN 'Bảo vệ tài sản'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-06-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 55 AND 64 THEN 'Cuối sự nghiệp'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-06-2023', 'DD-MM-YY'), BIRTHDAY)/12) >= 65 THEN 'Nghỉ hưu'
      ELSE NULL
  END FTR_VAL,
  TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700','1602')
  AND CUS_OPEN_DT < TO_DATE('01-06-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: AGE
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - SUB_SECTOR_CDE
  DWPROD.DW_ORG_LOCATION_DIM:
    - SUB_BRANCH_CDE
    - AREA_CDE
Tags: 
  - DEMOGRAPHIC
TW: ALL
*/

INSERT INTO CINS_FEATURE_STORE_V2
WITH 
T1 AS (
SELECT
DISTINCT T1.CUSTOMER_CDE, T2.AREA_CDE
FROM DWPROD.DW_CUSTOMER_FULL_DIM T1
INNER JOIN DWPROD.DW_ORG_LOCATION_DIM T2 ON T1.SUB_BRANCH_CDE = T2.SUB_BRANCH_CDE
RIGHT JOIN CINS_TMP_CUSTOMER_01062023 T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_CDE
)
SELECT 
CUSTOMER_CDE,
AREA_CDE AS AREA,
TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
CURRENT_TIMESTAMP ADD_TSTP
FROM T1;


COMMIT;


/*
Feature Name: LOR
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'LOR' FTR_NM,
             TO_DATE('01-06-2023', 'DD-MM-YY') - TO_DATE(CUS_OPEN_DT) FTR_VAL,
             TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
             CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.dw_customer_dim
WHERE SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND ACTIVE = '1'
  AND COMPANY_KEY = '1'
  AND TO_DATE(CUS_OPEN_DT) <= TO_DATE('01-06-2023', 'DD-MM-YY');


COMMIT;


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
                      TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         FROM DWPROD.STG_CRS_CUSTOMER_SCORE)
      WHERE RANK_SCORE = 1
        AND TO_DATE(DATE_1) < TO_DATE('01-06-2023', 'DD-MM-YY') )
   WHERE RN = 1 ) A
RIGHT JOIN
  (SELECT CUSTOMER_CDE
   FROM DWPROD.DW_CUSTOMER_DIM
   WHERE SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND ACTIVE = '1'
     AND COMPANY_KEY = '1') B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_BAL_SUM_NOW
Derived From: 
    DW_DEPOSIT_FCT:
        - ACTUAL_BAL_LCL
        - CATEGORY_CDE
        - CUSTOMER_CDE
        - PROCESS_DT 
    CINS_TMP_CUSTOMER_01062023:
        - CUSTOMER_CDE
Tags:
    - CASA
    - MONETARY
TW: NOW
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT
    CS.CUSTOMER_CDE,
    'CASA_BAL_SUM_NOW' AS FTR_NM,
    SUM(CS.ACTUAL_BAL_LCL) AS FTR_VAL,
    TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_DEPOSIT_FCT CS
JOIN CINS_TMP_CUSTOMER_01062023 TMP ON CS.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE CS.CATEGORY_CDE LIKE '10__'
    AND CS.PROCESS_DT = (
        SELECT MAX(PROCESS_DT)
        FROM DWPROD.DW_DEPOSIT_FCT
        WHERE PROCESS_DT < TO_DATE('01-06-2023', 'DD-MM-YY')
          AND CATEGORY_CDE LIKE '10__'
    )
GROUP BY CS.CUSTOMER_CDE;


COMMIT;


/* 
Feature Name: CASA_DAY_SINCE_LAST_TXN_CT_36M
Derived From: 
  DWPROD.DWA_STMT_EBANK: 
    - CUSTOMER_ID
    - PROCESS_DT
    - PRODUCT_CATEGORY
    - TRANSACTION_CODE
  DWPROD.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01062023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - RECENCY
TW: 36M
*/ 
INSERT INTO CINS_FEATURE_STORE_V2
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_DAY_SINCE_LAST_TXN_CT_36M' AS FTR_NM,
    TO_DATE('01-06-2023', 'DD-MM-YY') - NVL(MAX(TXN.PROCESS_DT), ADD_MONTHS(TO_DATE('01-06-2023', 'DD-MM-YY'), -36)) AS FTR_VAL,
    TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DWA_STMT_EBANK TXN
JOIN DWPROD.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01062023 TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER'
    AND TXN.PROCESS_DT < TO_DATE('01-06-2023', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-06-2023', 'DD-MM-YY'), -36)
GROUP BY TXN.CUSTOMER_ID;


COMMIT;


/*
Feature Name: CARD_CREDIT_MAX_LIMIT
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_01062023, CINS_TMP_CARD_DIM_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_MAX_LIMIT' AS FTR_NM,
       MAX(TT_CARD_LIMIT) AS FTR_VAL,
       TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DATA_RPT_CARD_493
WHERE CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01062023)
  AND SUBSTR(CARD_CDE, 1, 1) = '3'
  AND CARD_CDE IN
    (SELECT CARD_CDE
     FROM CINS_TMP_CARD_DIM_01062023)
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-06-2023', 'DD-MM-YY'), -36)
  AND PROCESS_DT < TO_DATE('01-06-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CARD_CREDIT_SUM_BAL_NOW
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_BAL_NOW' FTR_NM,
                                 SUM(TT_ORIGINAL_BALANCE) FTR_VAL,
                                 TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
                                 CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT BAL.*,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CARD_CDE
                            ORDER BY PROCESS_DT DESC) RN
   FROM
     (SELECT CUSTOMER_CDE,
             CARD_CDE,
             TT_ORIGINAL_BALANCE,
             PROCESS_DT
      FROM DWPROD.DATA_RPT_CARD_493 CC
      WHERE PROCESS_DT < TO_DATE('01-06-2023', 'DD-MM-YY')
        AND CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_01062023)
        AND CARD_CDE LIKE '3%' ) BAL) BAL
WHERE RN = 1
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: EB_SACOMPAY_DAY_SINCE_LTST_LOGIN
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          LAST_SIGNED_ON,
          ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE
                             ORDER BY LAST_SIGNED_ON DESC) RN
   FROM DWPROD.DW_EWALL_USER_DIM
   WHERE USER_STATUS = 'A'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_01062023) )
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_DAY_SINCE_LTST_LOGIN' AS FTR_NM,
       AVG(TO_DATE('01-06-2023', 'DD-MM-YY') - TO_DATE(LAST_SIGNED_ON)) AS FTR_VAL,
       TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A
WHERE TO_DATE(LAST_SIGNED_ON) < TO_DATE('01-06-2023', 'DD-MM-YY')
  AND TO_DATE(LAST_SIGNED_ON) >= ADD_MONTHS(TO_DATE('01-06-2023', 'DD-MM-YY'), -36)
  AND RN = 1
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: EB_SACOMPAY_DAY_SINCE_LTST_TXN
Derived From: DW_EWALL_TRANSACTION_FCT, CINS_TMP_CUSTOMER_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_DAY_SINCE_LTST_TXN' FTR_NM,
                                        TO_DATE('01-06-2023', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('01-06-2023', 'DD-MM-YY'), -36)) FTR_VAL,
                                        TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
                                        CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_EWALL_TRANSACTION_FCT
WHERE PROCESS_DT < TO_DATE('01-06-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-06-2023', 'DD-MM-YY'), -36)
  AND TXN_STATUS = 'S'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01062023)
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: EB_MBIB_DAY_SINCE_ACTIVE
Derived From: DW_EB_USER, CINS_TMP_CUSTOMER_01062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MBIB_DAY_SINCE_ACTIVE' FTR_NM,
       TO_DATE('01-06-2023', 'DD-MM-YY') - ACTIVATE_DATE FTR_VAL,
       TO_DATE('01-06-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          min(ACTIVATE_DT) ACTIVATE_DATE
   FROM DWPROD.DW_EB_USER t1
   WHERE EXISTS
       (SELECT 1
        FROM CINS_TMP_CUSTOMER_01062023 t2
        WHERE t1.CUSTOMER_CDE=t2.CUSTOMER_CDE)
     AND TO_DATE(ACTIVATE_DT) <= TO_DATE('01-06-2023', 'DD-MM-YY')
     AND ACTIVATE_DT != TO_DATE('01/01/2400', 'DD/MM/YYYY')
   GROUP BY customer_cde) A;


COMMIT;