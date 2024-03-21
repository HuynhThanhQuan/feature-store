CREATE TABLE CINS_FEATURE_STORE_REACTIVATED_01022023 (	
    CUSTOMER_CDE VARCHAR2(30 BYTE),
    FTR_NM VARCHAR2(200 BYTE),
    FTR_VAL VARCHAR2(2000 BYTE),
    RPT_DT VARCHAR2(50 BYTE),
    ADD_TSTP TIMESTAMP (6)
);


COMMIT;


/*
Feature Name: CASA_ACCT_CT_36M
Derived From: 
  DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM:  
    - ACCT_ID
    - CUSTOMER_CDE
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - TRANSACTION_CODE
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - ACCOUNT_NUMBER
    - PROCESS_DT
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01022023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - BEHAVIORAL
TW: 36M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
  DIM.CUSTOMER_CDE,
  'CASA_ACCT_CT_36M' FTR_NM,
  COUNT(DISTINCT DIM.ACCT_ID) FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM DIM
JOIN DW_ANALYTICS.DWA_STMT_EBANK FCT ON DIM.CUSTOMER_CDE = FCT.CUSTOMER_ID
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON FCT.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.ACCT_ID = FCT.ACCOUNT_NUMBER
  AND TC.INITIATION = 'CUSTOMER'
  AND FCT.PRODUCT_CATEGORY LIKE '10__'
  AND FCT.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
  AND FCT.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -36)
GROUP BY DIM.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_BAL_SUM_12M
Derived From:
  DW_ANALYTICS.DW_DEPOSIT_FCT:
  - CATEGORY_CDE
  - CUSTOMER_CDE
  - ACTUAL_BAL_LCL
  - PROCESS_DT
  CINS_TMP_CUSTOMER_01022023:
  - CUSTOMER_CDE
Tags:
- CASA
- MONETARY
TW: 12M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
  DF.CUSTOMER_CDE,
  'CASA_BAL_SUM_12M' AS FTR_NM,
  SUM(DF.ACTUAL_BAL_LCL) AS FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT DF
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DF.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DF.CATEGORY_CDE LIKE '10__'
  AND ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -12) <= DF.PROCESS_DT
  AND DF.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
GROUP BY DF.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_BAL_MAX_12M
Derived From:
  DW_ANALYTICS.DW_DEPOSIT_FCT:
  - CATEGORY_CDE
  - CUSTOMER_CDE
  - ACTUAL_BAL_LCL
  - PROCESS_DT
  CINS_TMP_CUSTOMER_01022023:
  - CUSTOMER_CDE
Tags:
- CASA
- MONETARY
TW: 12M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
  DF.CUSTOMER_CDE,
  'CASA_BAL_MAX_12M' AS FTR_NM,
  MAX(DF.ACTUAL_BAL_LCL) AS FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT DF
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DF.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DF.CATEGORY_CDE LIKE '10__'
  AND ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -12) <= DF.PROCESS_DT
  AND DF.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
GROUP BY DF.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_ACCT_ACTIVE_CT_12M
Derived From: 
  DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM:  
    - ACCT_ID
    - ACTIVE
    - CUSTOMER_CDE
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - TRANSACTION_CODE
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - ACCOUNT_NUMBER
    - PROCESS_DT
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01022023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - BEHAVIORAL
TW: 12M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
  DIM.CUSTOMER_CDE,
  'CASA_ACCT_ACTIVE_CT_12M' FTR_NM,
  COUNT(DISTINCT DIM.ACCT_ID) FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM DIM
JOIN DW_ANALYTICS.DWA_STMT_EBANK FCT ON DIM.CUSTOMER_CDE = FCT.CUSTOMER_ID
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON FCT.TRANSACTION_CODE = TC.TRANSACTION_CODE
RIGHT JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.ACTIVE = 1
  AND DIM.ACCT_ID = FCT.ACCOUNT_NUMBER
  AND TC.INITIATION = 'CUSTOMER'
  AND FCT.PRODUCT_CATEGORY LIKE '10__'
  AND FCT.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
  AND FCT.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -12)
GROUP BY DIM.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_TXN_AMT_SUM_12M
Derived From: 
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - AMT_LCY
    - PRODUCT_CATEGORY
    - PROCESS_DT
    - CUSTOMER_ID
    - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01022023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - MONETARY
TW: 12M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_TXN_AMT_SUM_12M' AS FTR_NM,
    NVL(SUM(ABS(TXN.AMT_LCY)), 0) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER' 
    AND TXN.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -12)
GROUP BY TXN.CUSTOMER_ID;


COMMIT;


/*
Feature Name: CASA_TXN_CT_12M
Derived From: 
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - STMT_ENTRY_ID
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - PROCESS_DT
    - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01022023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - FREQUENCY
TW: 12M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_TXN_CT_12M' AS FTR_NM,
    COUNT(DISTINCT TXN.STMT_ENTRY_ID) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER'
    AND TXN.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -12)
GROUP BY TXN.CUSTOMER_ID;


COMMIT;


/*
Feature Name: CASA_BAL_SUM_24M
Derived From:
  DW_ANALYTICS.DW_DEPOSIT_FCT:
  - CATEGORY_CDE
  - CUSTOMER_CDE
  - ACTUAL_BAL_LCL
  - PROCESS_DT
  CINS_TMP_CUSTOMER_01022023:
  - CUSTOMER_CDE
Tags:
- CASA
- MONETARY
TW: 24M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
  DF.CUSTOMER_CDE,
  'CASA_BAL_SUM_24M' AS FTR_NM,
  SUM(DF.ACTUAL_BAL_LCL) AS FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT DF
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DF.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DF.CATEGORY_CDE LIKE '10__'
  AND ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -24) <= DF.PROCESS_DT
  AND DF.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
GROUP BY DF.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_BAL_SUM_36M
Derived From:
  DW_ANALYTICS.DW_DEPOSIT_FCT:
  - CATEGORY_CDE
  - CUSTOMER_CDE
  - ACTUAL_BAL_LCL
  - PROCESS_DT
  CINS_TMP_CUSTOMER_01022023:
  - CUSTOMER_CDE
Tags:
- CASA
- MONETARY
TW: 36M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
  DF.CUSTOMER_CDE,
  'CASA_BAL_SUM_36M' AS FTR_NM,
  SUM(DF.ACTUAL_BAL_LCL) AS FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT DF
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DF.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DF.CATEGORY_CDE LIKE '10__'
  AND ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -36) <= DF.PROCESS_DT
  AND DF.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
GROUP BY DF.CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_TXN_AMT_SUM_24M
Derived From:
  DW_ANALYTICS.DWA_STMT_EBANK:
  - AMT_LCY
  - PRODUCT_CATEGORY
  - PROCESS_DT
  - CUSTOMER_ID
  - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE:
  - TRANSACTION_CODE
  - INITIATION
  CINS_TMP_CUSTOMER_01022023:
  - CUSTOMER_CDE
Tags:
- CASA
- MONETARY
TW: 24M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_TXN_AMT_SUM_24M' AS FTR_NM,
    NVL(SUM(ABS(TXN.AMT_LCY)), 0) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER'  
    AND TXN.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -24)
GROUP BY TXN.CUSTOMER_ID;


COMMIT;


/*
Feature Name: CASA_TXN_AMT_SUM_36M
Derived From:
  DW_ANALYTICS.DWA_STMT_EBANK:
  - AMT_LCY
  - PRODUCT_CATEGORY
  - PROCESS_DT
  - CUSTOMER_ID
  - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE:
  - TRANSACTION_CODE
  - INITIATION
  CINS_TMP_CUSTOMER_01022023:
  - CUSTOMER_CDE
Tags:
- CASA
- MONETARY
TW: 36M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_TXN_AMT_SUM_36M' AS FTR_NM,
    NVL(SUM(ABS(TXN.AMT_LCY)), 0) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER'  
    AND TXN.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -36)
GROUP BY TXN.CUSTOMER_ID;


COMMIT;


/*
Feature Name: CASA_TXN_CT_24M
Derived From: 
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - STMT_ENTRY_ID
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - PROCESS_DT
    - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01022023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - FREQUENCY
TW: 24M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_TXN_CT_24M' AS FTR_NM,
    COUNT(DISTINCT TXN.STMT_ENTRY_ID) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER'
    AND TXN.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -24)
GROUP BY TXN.CUSTOMER_ID;


COMMIT;


/*
Feature Name: CASA_TXN_CT_36M
Derived From: 
  DW_ANALYTICS.DWA_STMT_EBANK: 
    - STMT_ENTRY_ID
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - PROCESS_DT
    - TRANSACTION_CODE
  DW_ANALYTICS.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01022023:
    - CUSTOMER_CDE
Tags: 
  - CASA
  - FREQUENCY
TW: 36M
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT
    TXN.CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_TXN_CT_36M' AS FTR_NM,
    COUNT(DISTINCT TXN.STMT_ENTRY_ID) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN DW_ANALYTICS.TRANSACTION_CODE TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
JOIN CINS_TMP_CUSTOMER_01022023 TMP ON TXN.CUSTOMER_ID = TMP.CUSTOMER_CDE
WHERE TXN.PRODUCT_CATEGORY LIKE '10__'
    AND TC.INITIATION = 'CUSTOMER'
    AND TXN.PROCESS_DT < TO_DATE('01-02-2023', 'DD-MM-YY')
    AND TXN.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-02-2023', 'DD-MM-YY'), -36)
GROUP BY TXN.CUSTOMER_ID;


COMMIT;


/*
Feature Name: GEN_GRP
Derived From:
  DW_ANALYTICS.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
    - CUS_OPEN_DT
    - BIRTHDAY
Tags: 
  - DEMOGRAPHIC
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT CUSTOMER_CDE,
  'GEN_GRP' FTR_NM,
  CASE
      WHEN EXTRACT(YEAR FROM BIRTHDAY) < 1965 THEN 'Trước Gen X'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 1965 AND 1980 THEN 'Gen X'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 1981 AND 1996 THEN 'Gen Y'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 1997 AND 2012 THEN 'Gen Z'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 2013 AND 2025 THEN 'Gen A'
  END FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700','1602')
  AND CUS_OPEN_DT < TO_DATE('01-02-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: AGE
Derived From: 
  DW_ANALYTICS.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
    - CUS_OPEN_DT
    - BIRTHDAY
Tags: 
  - DEMOGRAPHIC
TW: ALL
*/

INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT CUSTOMER_CDE,
    'AGE' AS FTR_NM,
    FLOOR(MONTHS_BETWEEN(TO_DATE('01-02-2023', 'DD-MM-YY'), BIRTHDAY)/12) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE ACTIVE = 1
    AND COMPANY_KEY = 1
    AND SUB_SECTOR_CDE IN ('1700','1602')
    AND CUS_OPEN_DT < TO_DATE('01-02-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: LOR
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT DIM.CUSTOMER_CDE,
    'LOR' AS FTR_NM,
    TO_DATE('01-02-2023', 'DD-MM-YY') - TO_DATE(CUS_OPEN_DT) AS FTR_VAL,
    TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM DIM
RIGHT JOIN CINS_TMP_CUSTOMER_01022023 TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.SUB_SECTOR_CDE IN ('1700', '1602')
  AND DIM.ACTIVE = '1'
  AND DIM.COMPANY_KEY = '1'
  AND TO_DATE(DIM.CUS_OPEN_DT) <= TO_DATE('01-02-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: PROFESSION
Derived From: 
  DW_ANALYTICS.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
    - CUS_OPEN_DT
    - SUB_INDUSTRY_CDE
  DW_ANALYTICS.DW_INDUSTRY_DIM: 
    - SUB_INDUSTRY_CDE
    - INDUSTRY_GROUP_NAME_VN
Tags: 
  - DEMOGRAPHIC
*/
INSERT INTO CINS_FEATURE_STORE_REACTIVATED_01022023
SELECT CUSTOMER_CDE,
  'PROFESSION' FTR_NM,
  INDUSTRY_GROUP_NAME_VN FTR_VAL,
  TO_DATE('01-02-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM A
JOIN DW_ANALYTICS.DW_INDUSTRY_DIM B ON A.SUB_INDUSTRY_CDE = B.SUB_INDUSTRY_CDE
WHERE A.ACTIVE = 1
  AND A.COMPANY_KEY = 1
  AND A.SUB_SECTOR_CDE IN ('1700','1602')
  AND CUS_OPEN_DT < TO_DATE('01-02-2023', 'DD-MM-YY');


COMMIT;