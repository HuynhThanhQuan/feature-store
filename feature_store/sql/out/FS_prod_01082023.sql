DROP TABLE CINS_TMP_CUSTOMER_01082023;


COMMIT;


DROP TABLE CINS_TMP_CARD_DIM_01082023;


COMMIT;


DROP TABLE CINS_FEATURE_STORE_REACTIVATED_01082023;


COMMIT;


CREATE TABLE CINS_TMP_CUSTOMER_01082023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE)
);


COMMIT;


CREATE TABLE CINS_TMP_CARD_DIM_01082023 (
    CARD_CDE VARCHAR2(25 BYTE)
);


COMMIT;


/*
Table Name: CINS_TMP_CUSTOMER_01082023
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
*/
INSERT INTO CINS_TMP_CUSTOMER_01082023
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


/*
Table Name: CINS_TMP_CARD_DIM_01082023
Derived From: 
  DWPROD.DW_CARD_MASTER_DIM: 
    - CARD_CDE
    - STATUS_CDE
    - PLASTIC_CDE 
*/
INSERT INTO CINS_TMP_CARD_DIM_01082023 
SELECT DISTINCT CARD_CDE 
FROM DWPROD.DW_CARD_MASTER_DIM DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 C ON DIM.CUSTOMER_CDE=C.CUSTOMER_CDE
WHERE DIM.STATUS_CDE = ' ' 
    AND DIM.PLASTIC_CDE = ' ';


COMMIT;


CREATE TABLE CINS_FEATURE_STORE_REACTIVATED_01082023 (	
    CUSTOMER_CDE VARCHAR2(30 BYTE),
    FTR_NM VARCHAR2(200 BYTE),
    FTR_VAL VARCHAR2(2000 BYTE),
    RPT_DT VARCHAR2(50 BYTE),
    ADD_TSTP TIMESTAMP (6)
);


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
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT
    DISTINCT AMD.CUSTOMER_CDE,
    'CASA_HOLD' AS FTR_NM,
    '1' AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_ACCOUNT_MASTER_DIM AMD
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON AMD.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE AMD.ACTIVE = 1
    AND AMD.COMPANY_KEY = 1
    AND AMD.SUB_SECTOR_CDE IN ('1700', '1602')
    AND AMD.CATEGORY_CDE LIKE '10__'
    AND TO_CHAR(AMD.CLOSE_DT) = '01-JAN-00'
    AND AMD.OPEN_DT <= TO_DATE('01-08-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: CARD_CREDIT_HOLD
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_HOLD' AS FTR_NM,
       '1' AS FTR_VAL,
       TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT DISTINCT DIM.CUSTOMER_CDE
   FROM DWPROD.DW_CARD_MASTER_DIM DIM
   RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
   WHERE SUBSTR(DIM.CARD_CDE, 1, 1) = '3'
     AND DIM.STATUS_CDE = ' '
     AND DIM.BASIC_SUPP_IND = 'B'
     AND TO_CHAR(DIM.LAST_RENEWAL_DT) = '01-JAN-00'
     AND DIM.ACTIVATION_DT <= TO_DATE('01-08-2023', 'DD-MM-YY'));


COMMIT;


/*
Feature Name: EB_SACOMPAY_HOLD
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT DISTINCT DIM.CUSTOMER_CDE,
    'EB_SACOMPAY_HOLD' AS FTR_NM,
    1 AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_EWALL_USER_DIM DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.FIRST_SIGNED_ON < TO_DATE('01-08-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: EB_MBIB_HOLD
Derived From: DW_EB_USER, CINS_TMP_CUSTOMER_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT DISTINCT DIM.CUSTOMER_CDE,
    'EB_MBIB_HOLD' AS FTR_NM,
    1 AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_EB_USER DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.PROCESS_DT IN
    (SELECT MAX(PROCESS_DT)
     FROM DWPROD.DW_EB_USER
     WHERE PROCESS_DT < TO_DATE('01-08-2023', 'DD-MM-YY') )
  AND DIM.DEL_FLG = 'N';


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
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT DIM.CUSTOMER_CDE,
  'LIFE_STG' AS FTR_NM,
  CASE
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-08-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 18 AND 26 THEN 'Bắt đầu sự nghiệp'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-08-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 27 AND 35 THEN 'Lập gia đình'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-08-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 36 AND 45 THEN 'Thiết lập tài sản'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-08-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 46 AND 54 THEN 'Bảo vệ tài sản'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-08-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 55 AND 64 THEN 'Cuối sự nghiệp'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-08-2023', 'DD-MM-YY'), BIRTHDAY)/12) >= 65 THEN 'Nghỉ hưu'
      ELSE NULL
  END AS FTR_VAL,
  TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_CUSTOMER_DIM DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.ACTIVE = 1
  AND DIM.COMPANY_KEY = 1
  AND DIM.SUB_SECTOR_CDE IN ('1700','1602')
  AND DIM.CUS_OPEN_DT < TO_DATE('01-08-2023', 'DD-MM-YY');


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

INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
WITH 
T1 AS (
  SELECT
  DISTINCT T1.CUSTOMER_CDE, T2.AREA_CDE
  FROM DWPROD.DW_CUSTOMER_FULL_DIM T1
  INNER JOIN DWPROD.DW_ORG_LOCATION_DIM T2 ON T1.SUB_BRANCH_CDE = T2.SUB_BRANCH_CDE
  RIGHT JOIN CINS_TMP_CUSTOMER_01082023 T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_CDE
)
SELECT 
  CUSTOMER_CDE,
  'AREA' AS FTR_NM, 
  AREA_CDE AS FTR_VAL,
  TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM T1;


COMMIT;


/*
Feature Name: LOR
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT DIM.CUSTOMER_CDE,
    'LOR' AS FTR_NM,
    TO_DATE('01-08-2023', 'DD-MM-YY') - TO_DATE(CUS_OPEN_DT) AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_CUSTOMER_DIM DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.SUB_SECTOR_CDE IN ('1700', '1602')
  AND DIM.ACTIVE = '1'
  AND DIM.COMPANY_KEY = '1'
  AND TO_DATE(DIM.CUS_OPEN_DT) <= TO_DATE('01-08-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: CREDIT_SCORE
Derived From: STG_CRS_CUSTOMER_SCORE, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
WITH 
T0 AS (
    SELECT 
        TRIM(' ' FROM(CUSTOMER_CDE))CUSTOMER_CDE,
        NVL(FINANCIALSCORE, NONFINANCIALSCORE) CREDIT_SCORE,
        RANK()OVER(PARTITION BY CUSTOMER_CDE ORDER BY DATE_1 DESC)RANK_SCORE, 
        DATE_1,
        PROCESS_DATE
     FROM DWPROD.STG_CRS_CUSTOMER_SCORE
),
T1 AS (
    SELECT 
        CUSTOMER_CDE,
        CREDIT_SCORE,
        DATE_1,
        PROCESS_DATE,
        ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE ORDER BY CREDIT_SCORE DESC) RN
    FROM T0
    WHERE RANK_SCORE = 1
    AND TO_DATE(DATE_1) < TO_DATE('31-12-23', 'DD-MM-YY')
),
T2 AS (
    SELECT CUSTOMER_CDE
    FROM DWPROD.DW_CUSTOMER_DIM
    WHERE SUB_SECTOR_CDE IN ('1700', '1602')
    AND ACTIVE = '1'
    AND COMPANY_KEY = '1'
),
T3 AS (
    SELECT T1.CUSTOMER_CDE, T1.CREDIT_SCORE
    FROM T1 RIGHT JOIN T2 ON T1.CUSTOMER_CDE = T2.CUSTOMER_CDE
    WHERE T1.RN = 1
),
T5 AS (
    SELECT 
        T3.CUSTOMER_CDE,
        CASE
          WHEN T3.CREDIT_SCORE IS NULL THEN 0
          ELSE T3.CREDIT_SCORE
        END CREDIT_SCORE
    FROM T3 
    RIGHT JOIN CINS_TMP_CUSTOMER_01082023 T4 ON T3.CUSTOMER_CDE = T4.CUSTOMER_CDE
    WHERE T3.CUSTOMER_CDE IS NOT NULL
    AND T4.CUSTOMER_CDE IS NOT NULL
)
SELECT 
    CUSTOMER_CDE,
    'CREDIT_SCORE' AS FTR_NM,
    CREDIT_SCORE AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM T5;


COMMIT;


/*
Feature Name: CASA_BAL_SUM_NOW
Derived From: 
    DW_DEPOSIT_FCT:
        - ACTUAL_BAL_LCL
        - CATEGORY_CDE
        - CUSTOMER_CDE
        - PROCESS_DT 
    CINS_TMP_CUSTOMER_01082023:
        - CUSTOMER_CDE
Tags:
    - CASA
    - MONETARY
TW: NOW
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT
    CS.CUSTOMER_CDE,
    'CASA_BAL_SUM_NOW' AS FTR_NM,
    SUM(CS.ACTUAL_BAL_LCL) AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_DEPOSIT_FCT CS
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON CS.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE CS.CATEGORY_CDE LIKE '10__'
    AND CS.PROCESS_DT = (
        SELECT MAX(PROCESS_DT)
        FROM DWPROD.DW_DEPOSIT_FCT
        WHERE PROCESS_DT < TO_DATE('01-08-2023', 'DD-MM-YY')
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
  CINS_TMP_CUSTOMER_01082023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - RECENCY
TW: 36M
*/ 
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
WITH 
T0 AS (
    SELECT * FROM CINS_TMP_CUSTOMER_01082023
),
T1 AS (
    SELECT * FROM DWPROD.DWA_STMT_EBANK
    WHERE PROCESS_DT < TO_DATE('01-08-2023', 'DD-MM-YY')
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-08-2023', 'DD-MM-YY'), -36)
    AND PRODUCT_CATEGORY LIKE '10__' 
),
T2 AS (
    SELECT * FROM DWPROD.TRANSACTION_CODE
    WHERE INITIATION = 'CUSTOMER'
),
T3 AS (
    SELECT *
    FROM T0
    LEFT JOIN T1 ON T0.CUSTOMER_CDE = T1.CUSTOMER_ID
    LEFT JOIN T2 ON T1.TRANSACTION_CODE = T2.TRANSACTION_CODE)
SELECT 
    CUSTOMER_CDE AS CUSTOMER_CDE,
    'CASA_DAY_SINCE_LAST_TXN_CT_36M' AS FTR_NM,
    TO_DATE('01-08-2023', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('01-08-2023', 'DD-MM-YY'), -36)) AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM T3
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CARD_CREDIT_MAX_LIMIT
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_01082023, CINS_TMP_CARD_DIM_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT DIM.CUSTOMER_CDE,
       'CARD_CREDIT_MAX_LIMIT' AS FTR_NM,
       MAX(TT_CARD_LIMIT) AS FTR_VAL,
       TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DATA_RPT_CARD_493 DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE SUBSTR(DIM.CARD_CDE, 1, 1) = '3'
  AND DIM.CARD_CDE IN
    (SELECT CARD_CDE FROM CINS_TMP_CARD_DIM_01082023)
  AND DIM.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-08-2023', 'DD-MM-YY'), -36)
  AND DIM.PROCESS_DT < TO_DATE('01-08-2023', 'DD-MM-YY')
GROUP BY DIM.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CARD_CREDIT_SUM_BAL_NOW
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT CUSTOMER_CDE,
    'CARD_CREDIT_SUM_BAL_NOW' AS FTR_NM,
    SUM(TT_ORIGINAL_BALANCE) AS FTR_VAL,
    TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT BAL.*, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CARD_CDE ORDER BY PROCESS_DT DESC) RN
   FROM
     (SELECT CC.CUSTOMER_CDE,
             CARD_CDE,
             TT_ORIGINAL_BALANCE,
             PROCESS_DT
      FROM DWPROD.DATA_RPT_CARD_493 CC
      RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON CC.CUSTOMER_CDE = TMP.CUSTOMER_CDE
      WHERE CC.PROCESS_DT < TO_DATE('01-08-2023', 'DD-MM-YY')
      AND CARD_CDE LIKE '3%' ) BAL) BAL
WHERE RN = 1
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: EB_SACOMPAY_DAY_SINCE_LTST_LOGIN
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023 
WITH A AS
  (SELECT DIM.CUSTOMER_CDE,
          LAST_SIGNED_ON,
          ROW_NUMBER() OVER (PARTITION BY DIM.CUSTOMER_CDE ORDER BY LAST_SIGNED_ON DESC) RN
   FROM DWPROD.DW_EWALL_USER_DIM DIM
   RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
   WHERE DIM.USER_STATUS = 'A')
SELECT CUSTOMER_CDE,
  'EB_SACOMPAY_DAY_SINCE_LTST_LOGIN' AS FTR_NM,
  AVG(TO_DATE('01-08-2023', 'DD-MM-YY') - TO_DATE(LAST_SIGNED_ON)) AS FTR_VAL,
  TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM A
WHERE TO_DATE(LAST_SIGNED_ON) < TO_DATE('01-08-2023', 'DD-MM-YY')
  AND TO_DATE(LAST_SIGNED_ON) >= ADD_MONTHS(TO_DATE('01-08-2023', 'DD-MM-YY'), -36)
  AND RN = 1
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: EB_SACOMPAY_DAY_SINCE_LTST_TXN
Derived From: DW_EWALL_TRANSACTION_FCT, CINS_TMP_CUSTOMER_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT FCT.CUSTOMER_CDE,
  'EB_SACOMPAY_DAY_SINCE_LTST_TXN' AS FTR_NM,
  TO_DATE('01-08-2023', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('01-08-2023', 'DD-MM-YY'), -36)) AS FTR_VAL,
  TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DW_EWALL_TRANSACTION_FCT FCT
RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON FCT.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE PROCESS_DT < TO_DATE('01-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-08-2023', 'DD-MM-YY'), -36)
  AND TXN_STATUS = 'S'
GROUP BY FCT.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: EB_MBIB_DAY_SINCE_ACTIVE
Derived From: DW_EB_USER, CINS_TMP_CUSTOMER_01082023
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01082023
SELECT CUSTOMER_CDE,
  'EB_MBIB_DAY_SINCE_ACTIVE' AS FTR_NM,
  TO_DATE('01-08-2023', 'DD-MM-YY') - ACTIVATE_DATE AS FTR_VAL,
  TO_DATE('01-08-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT T1.CUSTOMER_CDE,
          MIN(ACTIVATE_DT) AS ACTIVATE_DATE
   FROM DWPROD.DW_EB_USER T1
   RIGHT JOIN CINS_TMP_CUSTOMER_01082023 TMP ON T1.CUSTOMER_CDE = TMP.CUSTOMER_CDE
   WHERE TO_DATE(ACTIVATE_DT) <= TO_DATE('01-08-2023', 'DD-MM-YY')
     AND ACTIVATE_DT != TO_DATE('01/01/2400', 'DD/MM/YYYY')
   GROUP BY T1.CUSTOMER_CDE) A;


COMMIT;