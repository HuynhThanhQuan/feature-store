DROP TABLE CINS_TMP_CUSTOMER_01102023;


COMMIT;


DROP TABLE CINS_TMP_CARD_DIM_01102023;


COMMIT;


DROP TABLE CINS_TMP_CUSTOMER_STATUS_01102023;


COMMIT;


DROP TABLE CINS_TMP_CREDIT_CARD_LOAN_6M_01102023;


COMMIT;


DROP TABLE CINS_TMP_CREDIT_CARD_TRANSACTION_01102023;


COMMIT;


DROP TABLE CINS_TMP_DATA_RPT_CARD_01102023;


COMMIT;


DROP TABLE CINS_TMP_DATA_RPT_LOAN_01102023;


COMMIT;


DROP TABLE CINS_TMP_EB_MB_CROSSELL_01102023;


COMMIT;


DROP TABLE CINS_2M_PART;


COMMIT;


DROP TABLE CINS_TMP_LST_01102023;


COMMIT;


DROP TABLE CINS_FEATURE_STORE_V2;


COMMIT;


CREATE TABLE CINS_TMP_CUSTOMER_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE)
);


COMMIT;


CREATE TABLE CINS_TMP_CARD_DIM_01102023 (
    CARD_CDE VARCHAR2(25 BYTE)
);


COMMIT;


CREATE TABLE CINS_TMP_CUSTOMER_STATUS_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    CUST_STT NUMBER,
    CUST_STT_CHG NUMBER
);


COMMIT;


CREATE TABLE CINS_TMP_CREDIT_CARD_LOAN_6M_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    RN NUMBER
);


COMMIT;


CREATE TABLE CINS_TMP_CREDIT_CARD_TRANSACTION_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    PROCESS_DT DATE,
    APPROVAL_CDE VARCHAR2(25 BYTE),
    RETRVL_REFNO VARCHAR2(25 BYTE),
    AMT_BILL NUMBER,
    ACQ_CNTRY_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TXN_CURR_CDE VARCHAR2(25 BYTE),
    BILL_CURR_CDE VARCHAR2(25 BYTE),
    PRODUCT_CDE VARCHAR2(25 BYTE),
    TXN_OL_CDE VARCHAR2(25 BYTE),
    MCC_CDE NUMBER,
    TXN_OM_CDE VARCHAR2(25 BYTE),
    AMT_FEE NUMBER
);


COMMIT;


CREATE TABLE CINS_TMP_DATA_RPT_CARD_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    CARD_CDE VARCHAR2(25 BYTE),
    PROCESS_DT DATE,
    TT_CARD_LIMIT NUMBER
);


COMMIT;


CREATE TABLE CINS_TMP_DATA_RPT_LOAN_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    TT_LOAN_GROUP NUMBER
);


COMMIT;


CREATE TABLE CINS_TMP_EB_MB_CROSSELL_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    CORP_ID VARCHAR2(100 BYTE),
    INPUT_DT DATE
);


COMMIT;


CREATE TABLE CINS_2M_PART (
    PRODUCT VARCHAR2(25 BYTE),
    CUSTOMER_CDE VARCHAR2(25 BYTE)
);


COMMIT;


CREATE TABLE CINS_TMP_LST_01102023 (
    PRODUCT VARCHAR(20), 
    CUSTOMER_CDE VARCHAR(20)
);


COMMIT;


CREATE TABLE CINS_FEATURE_STORE_V2 (	
    CUSTOMER_CDE VARCHAR2(30 BYTE),
    FTR_NM VARCHAR2(200 BYTE),
    FTR_VAL VARCHAR2(2000 BYTE),
    RPT_DT VARCHAR2(50 BYTE),
    ADD_TSTP TIMESTAMP (6)
);


COMMIT;


/*
Table Name: CINS_TMP_CUSTOMER_01102023
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
*/
INSERT INTO CINS_TMP_CUSTOMER_01102023 
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
Table Name: CINS_TMP_CARD_DIM_01102023
Derived From: 
  DWPROD.DW_CARD_MASTER_DIM: 
    - CARD_CDE
    - STATUS_CDE
    - PLASTIC_CDE 
*/
INSERT INTO CINS_TMP_CARD_DIM_01102023 
SELECT DISTINCT CARD_CDE 
FROM DWPROD.DW_CARD_MASTER_DIM 
WHERE STATUS_CDE = ' ' 
    AND PLASTIC_CDE = ' ';


COMMIT;


/*
Table Name: CINS_TMP_CUSTOMER_STATUS_01102023
Derived From: 
  DWPROD.DW_CUST_PRODUCT_LOC_FCT: 
    - CUST_STATUS
    - PROCESS_DT
    - CUSTOMER_CDE
  CINS_TMP_CUSTOMER_01102023: 
    - CUSTOMER_CDE
*/
INSERT INTO CINS_TMP_CUSTOMER_STATUS_01102023
SELECT A.CUSTOMER_CDE, A.RPT_DT, A.CUST_STT, A.CUST_STT - LAG(A.CUST_STT) OVER (PARTITION BY A.CUSTOMER_CDE ORDER BY A.RPT_DT) CUST_STT_CHG
FROM (
    SELECT T.CUSTOMER_CDE, T.PROCESS_DT RPT_DT,
        MAX(CASE
            WHEN T.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T.CUST_STATUS = 'DONG BANG' THEN 0
        END) CUST_STT
    FROM DWPROD.DW_CUST_PRODUCT_LOC_FCT T
        JOIN CINS_TMP_CUSTOMER_01102023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    WHERE 
        T.PROCESS_DT = ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -1)
        OR T.PROCESS_DT = TO_DATE('01-10-2023', 'DD-MM-YY')
    GROUP BY 
        T.CUSTOMER_CDE, 
        T.PROCESS_DT
) A;


COMMIT;


/*
Table Name: CINS_TMP_CREDIT_CARD_LOAN_6M_01102023
Derived From: 
    DWPROD.DW_CARD_MASTER_DIM:
        - CUSTOMER_CDE
        - CARD_CDE
        - ACTIVATION_DT
        - PLASTIC_CDE
        - STATUS_CDE
*/
INSERT INTO CINS_TMP_CREDIT_CARD_LOAN_6M_01102023
SELECT CUSTOMER_CDE, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE ORDER BY ACTIVATION_DT DESC) RN
FROM DWPROD.DW_CARD_MASTER_DIM
WHERE SUBSTR(CARD_CDE,1,1) = '3' 
    AND PLASTIC_CDE = ' ' 
    AND STATUS_CDE = ' '
    AND TO_DATE('01-10-2023','DD-MM-YY') - TO_DATE(ACTIVATION_DT) >= 180;


COMMIT;


/*
Table Name: CINS_TMP_CREDIT_CARD_TRANSACTION_01102023
Derived From: 
    DWPROD.DW_CARD_TRANSACTION_FCT:
        - CUSTOMER_CDE
        - CARD_CDE
        - PROCESS_DT
        - APPROVAL_CDE
        - RETRVL_REFNO
        - AMT_BILL
        - ACQ_CNTRY_CDE
        - MERCHANT_CDE
        - TXN_CURR_CDE
        - BILL_CURR_CDE
        - PRODUCT_CDE
        - MCC_CDE
        - TXN_OL_CDE
        - TXN_OM_CDE
        - AMT_FEE
        - TRAN_STATUS
        - COMPANY_KEY
        - SUB_SECTOR_CDE
    CINS_TMP_CUSTOMER_01102023:
        - CUSTOMER_CDE
*/
INSERT INTO CINS_TMP_CREDIT_CARD_TRANSACTION_01102023 
SELECT 
    CUSTOMER_CDE, PROCESS_DT, APPROVAL_CDE, RETRVL_REFNO, AMT_BILL, ACQ_CNTRY_CDE, MERCHANT_CDE, TXN_CURR_CDE, BILL_CURR_CDE, PRODUCT_CDE, TXN_OL_CDE, MCC_CDE, TXN_OM_CDE, AMT_FEE
FROM (
    SELECT 
        A.*,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE, CARD_CDE, PROCESS_DT, APPROVAL_CDE, RETRVL_REFNO ORDER BY NULL) RN
    FROM (
        SELECT 
            T.CUSTOMER_CDE, T.CARD_CDE, T.PROCESS_DT, T.APPROVAL_CDE, T.RETRVL_REFNO, T.AMT_BILL, T.ACQ_CNTRY_CDE, T.MERCHANT_CDE, T.TXN_CURR_CDE, T.BILL_CURR_CDE, T.PRODUCT_CDE, T.MCC_CDE, T.TXN_OL_CDE, T.TXN_OM_CDE, T.AMT_FEE
        FROM 
            DWPROD.DW_CARD_TRANSACTION_FCT T
            JOIN CINS_TMP_CUSTOMER_01102023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
        WHERE 
            T.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -36)
            AND T.PROCESS_DT <= TO_DATE('01-10-2023', 'DD-MM-YY')
            AND T.CARD_CDE LIKE '3%'
            AND T.TRAN_STATUS = 'S'
            AND REGEXP_LIKE(T.TXN_OL_CDE, '^[A-Z]$')
            AND T.COMPANY_KEY = 1
            AND T.SUB_SECTOR_CDE IN ('1700', '1602')
    ) A
)
WHERE RN = 1;


COMMIT;


/*
Table Name: CINS_TMP_DATA_RPT_CARD_01102023
Derived From: 
    DWPROD.DATA_RPT_CARD_493:
        - CUSTOMER_CDE
        - CARD_CDE
        - PROCESS_DT
        - TT_CARD_LIMIT
    CINS_TMP_CUSTOMER_01102023:
        - CUSTOMER_CDE
    CINS_TMP_CARD_DIM_01102023:
        - CARD_CDE
*/
INSERT INTO CINS_TMP_DATA_RPT_CARD_01102023
SELECT CUSTOMER_CDE, CARD_CDE, PROCESS_DT, TT_CARD_LIMIT
FROM (
    SELECT T.CUSTOMER_CDE, T.CARD_CDE, T.PROCESS_DT, T.TT_CARD_LIMIT,
    ROW_NUMBER() OVER (PARTITION BY T.CUSTOMER_CDE, T.CARD_CDE ORDER BY T.PROCESS_DT DESC) RN
    FROM DWPROD.DATA_RPT_CARD_493 T
    JOIN CINS_TMP_CUSTOMER_01102023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    JOIN CINS_TMP_CARD_DIM_01102023 D ON T.CARD_CDE=D.CARD_CDE
    AND SUBSTR(T.CARD_CDE,1,1) = '3'
    AND T.PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -36)
    AND T.PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
    )
WHERE RN = 1;


COMMIT;


/*
Table Name: CINS_TMP_DATA_RPT_LOAN_01102023
Derived From: 
    DWPROD.DATA_RPT_CARD_493:
        - CUSTOMER_CDE
        - CARD_CDE
        - PROCESS_DT
        - TT_CARD_LIMIT
        - TT_LOAN_GROUP
        - COMPANY_KEY
    CINS_TMP_CUSTOMER_01102023:
        - CUSTOMER_CDE
    CINS_TMP_CARD_DIM_01102023:
        - CARD_CDE
*/
INSERT INTO CINS_TMP_DATA_RPT_LOAN_01102023
SELECT 
    CUSTOMER_CDE, 
    MAX(TT_LOAN_GROUP) AS TT_LOAN_GROUP
FROM (
    SELECT 
        T.CUSTOMER_CDE,
        CAST(SUBSTR(T.TT_LOAN_GROUP,2,1) AS INT) TT_LOAN_GROUP
    FROM 
        DWPROD.DATA_RPT_CARD_493 T
        JOIN CINS_TMP_CUSTOMER_01102023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
        JOIN CINS_TMP_CARD_DIM_01102023 D ON T.CARD_CDE=D.CARD_CDE
    WHERE 
        T.COMPANY_KEY = 1 
        AND SUBSTR(T.CARD_CDE,1,1) = '3'
        AND ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -6) <= T.PROCESS_DT 
        AND T.PROCESS_DT < TO_DATE('01-10-2023','DD-MM-YY')
)
GROUP BY 
    CUSTOMER_CDE;


COMMIT;


/*
Table Name: CINS_TMP_EB_MB_CROSSELL_01102023
Derived From: 
    DWPROD.DW_EB_USER:
        - CUSTOMER_CDE
        - LOGIN_ALLOWED
        - DEL_FLG
        - PROCESS_DT
        - CORP_ID
        - REC_UPDATE_DT
        - INPUT_DT
    CINS_TMP_CUSTOMER_01102023:
        - CUSTOMER_CDE
*/
INSERT INTO CINS_TMP_EB_MB_CROSSELL_01102023 
SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT 
FROM (
    SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CORP_ID ORDER BY REC_UPDATE_DT DESC) RN 
    FROM DWPROD.DW_EB_USER  
    WHERE CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_CUSTOMER_01102023) 
        AND LOGIN_ALLOWED NOT IN ('N') 
        AND DEL_FLG NOT IN ('N')
        AND PROCESS_DT = TO_DATE('01-10-2023', 'DD-MM-YY')
) 
WHERE RN = 1;


COMMIT;


/*
Table Name: CINS_2M_PART
Derived From: 
    DWPROD.DWD_TOI_FCT: 
        - CUSTOMER_CDE
        - NII_DEPOSIT_MTH 
        - NII_LOAN_MTH 
        - NII_FEE_MTH 
        - NII_FX_MTH
        - PROCESS_DT
    DW_CUSTOMER_DIM:
        - ACTIVE
        - CUSTOMER_CDE
        - COMPANY_KEY
        - SUB_SECTOR_CDE
*/
INSERT INTO CINS_2M_PART
SELECT 'CARDS' PRODUCT, A.CUSTOMER_CDE 
FROM DWPROD.DWA_CARD_TOI_FCT A
JOIN (
    SELECT CUSTOMER_CDE 
    FROM DWPROD.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1 
    AND COMPANY_KEY = 1 
    AND SUB_SECTOR_CDE IN ('1700', '1602')
) B 
ON A.CUSTOMER_CDE=B.CUSTOMER_CDE
WHERE TRUNC(A.PROCESS_DT) IN (
    '31-JAN-2022',
    '28-FEB-2022',
    '31-MAR-2022',
    '30-APR-2022',
    '31-MAY-2022',
    '30-JUN-2022',
    '31-JUL-2022',
    '31-AUG-2022',
    '30-SEP-2022',
    '31-OCT-2022',
    '30-NOV-2022',
    '31-DEC-2022'
)
GROUP BY A.CUSTOMER_CDE
HAVING SUM(A.NII_CARD_MTD)>=300000;


COMMIT;


INSERT INTO CINS_2M_PART
SELECT 'OTHER' PRODUCT, A.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE, PROCESS_DT, (NII_DEPOSIT_MTH + NII_LOAN_MTH + NII_FEE_MTH + NII_FX_MTH) NII_CARD_MTD 
    FROM DWPROD.DWD_TOI_FCT
) A
JOIN (
    SELECT CUSTOMER_CDE 
    FROM DWPROD.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1 
    AND COMPANY_KEY = 1 
    AND SUB_SECTOR_CDE IN ('1700', '1602')
) B 
ON A.CUSTOMER_CDE=B.CUSTOMER_CDE
WHERE TRUNC(A.PROCESS_DT) IN (
    '31-JAN-2022',
    '28-FEB-2022',
    '31-MAR-2022',
    '30-APR-2022',
    '31-MAY-2022',
    '30-JUN-2022',
    '31-JUL-2022',
    '31-AUG-2022',
    '30-SEP-2022',
    '31-OCT-2022',
    '30-NOV-2022',
    '31-DEC-2022'
)
GROUP BY A.CUSTOMER_CDE
HAVING SUM(A.NII_CARD_MTD)>=300000;


COMMIT;


/*
Table Name: CINS_TMP_LST_01102023
Derived From: 
    CINS_TMP_CREDIT_CARD_TRANSACTION_01102023: 
        - CUSTOMER_CDE
        - PROCESS_DT
    CINS_2M_PART:
        - PRODUCT
        - CUSTOMER_CDE
    DWPROD.DW_EB_TRANSACTION_FCT:
        - CUSTOMER_CDE
        - TXN_DT
        - TXN_STATUS
    DWPROD.DW_EWALL_TRANSACTION_FCT:
        - CUSTOMER_CDE
        - PROCESS_DT
        - TXN_STATUS
    DWPROD.DWA_STMT_EBANK:
        - CUSTOMER_ID
        - PRODUCT_CATEGORY
        - PROCESS_DT
        - TRANSACTION_CODE
    DWPROD.TRANSACTION_CODE:
        - INITIATION
        - TRANSACTION_CODE
*/
---CREDIT
INSERT INTO CINS_TMP_LST_01102023
SELECT 'CARD' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE,
        'CARD_CREDIT_CT_TXN_6M' FTR_NM,
        COUNT(*) FTR_VAL,
        TO_DATE('01-10-2023','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM CINS_TMP_CREDIT_CARD_TRANSACTION_01102023
    WHERE PROCESS_DT < TO_DATE('01-10-2023','DD-MM-YY')
        AND PROCESS_DT >= TO_DATE('01-10-2023','DD-MM-YY') - INTERVAL '6' MONTH
    GROUP BY CUSTOMER_CDE
) E
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'CARD'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE E.FTR_VAL > 0;


COMMIT;


---IB/MB
INSERT INTO CINS_TMP_LST_01102023
SELECT 'IBMB' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE,
        'EB_MBIB_CT_TXN_6M' FTR_NM,
        COUNT(TXN_ID) FTR_VAL,
        TO_DATE('01-10-2023','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM DWPROD.DW_EB_TRANSACTION_FCT
    WHERE TXN_DT < TO_DATE('01-10-2023','DD-MM-YY')
        AND TXN_DT >= TO_DATE('01-10-2023','DD-MM-YY') - INTERVAL '6' MONTH
        AND TXN_STATUS = 'SUC'
        AND CUSTOMER_CDE IN (
            SELECT CUSTOMER_CDE 
            FROM CINS_TMP_CUSTOMER_01102023
        )
    GROUP BY CUSTOMER_CDE
) E 
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'OTHER'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE FTR_VAL > 0;


COMMIT;


---SACOMPAY
INSERT INTO CINS_TMP_LST_01102023
SELECT 'SACOMPAY' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE,
        'EB_SACOMPAY_CT_TXN_6M' FTR_NM,
        COUNT(DISTINCT TXN_ID) FTR_VAL,
        TO_DATE('01-10-2023','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM DWPROD.DW_EWALL_TRANSACTION_FCT
    WHERE PROCESS_DT < TO_DATE('01-10-2023','DD-MM-YY') 
        AND PROCESS_DT >= TO_DATE('01-10-2023','DD-MM-YY') - INTERVAL '6' MONTH
        AND TXN_STATUS = 'S'
        AND CUSTOMER_CDE IN (
            SELECT CUSTOMER_CDE 
            FROM CINS_TMP_CUSTOMER_01102023
        )
    GROUP BY CUSTOMER_CDE
) E 
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'OTHER'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE FTR_VAL > 0;


COMMIT;


---CASA
INSERT INTO CINS_TMP_LST_01102023
SELECT 'CASA' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_ID CUSTOMER_CDE,
        'CASA_CT_TXN_6M' FTR_NM,
        COUNT(STMT_ENTRY_ID) FTR_VAL,
        TO_DATE('01-10-2023','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM DWPROD.DWA_STMT_EBANK TXN
    JOIN (
        SELECT TRANSACTION_CODE 
        FROM DWPROD.TRANSACTION_CODE 
        WHERE INITIATION = 'CUSTOMER'
    ) TC
    ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
    WHERE PRODUCT_CATEGORY LIKE '10__'
        AND PROCESS_DT < TO_DATE('01-10-2023','DD-MM-YY')
        AND PROCESS_DT >= TO_DATE('01-10-2023','DD-MM-YY') - INTERVAL '6' MONTH
        AND CUSTOMER_ID IN (
            SELECT CUSTOMER_CDE 
            FROM CINS_TMP_CUSTOMER_01102023
        )
    GROUP BY CUSTOMER_ID
) E 
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'OTHER'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE FTR_VAL > 0;


COMMIT;


/*
Feature Name: REACTIVATED
Derived From: 
  CINS_TMP_CUSTOMER_STATUS_01102023: 
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
       TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CUSTOMER_STATUS_01102023
WHERE CUST_STT = 2
  AND CUST_STT_CHG = 1
  AND RPT_DT = TO_DATE('01-10-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: INACTIVE
Derived From: 
  CINS_TMP_CUSTOMER_STATUS_01102023: 
    - CUSTOMER_CDE
    - CUST_STT
    - RPT_DT
Tags: 
  - LABEL
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'INACTIVE' FTR_NM,
       CASE
           WHEN CUST_STT = 1 THEN 1
           WHEN CUST_STT = 2 THEN 0
       END FTR_VAL,
       TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CUSTOMER_STATUS_01102023
WHERE CUST_STT >= 1
  AND RPT_DT = TO_DATE('01-10-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: CASA_INACTIVE
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
    - CUS_OPEN_DT
    - CUSTOMER_CDE
  DWPROD.DWA_STMT_EBANK: 
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - TRANSACTION_CODE
    - PROCESS_DT
  DWPROD.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION 
TW: 36M
Tags: 
  - LABEL 
*/
INSERT INTO CINS_FEATURE_STORE_V2
WITH 
T1 AS (
    SELECT CUSTOMER_CDE
    FROM DWPROD.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1
      AND COMPANY_KEY = 1
      AND SUB_SECTOR_CDE IN ('1700','1602')
      AND CUS_OPEN_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
      AND CUSTOMER_CDE IS NOT NULL
),
T2 AS (
    SELECT DISTINCT CUSTOMER_ID
    FROM DWPROD.DWA_STMT_EBANK TXN
    JOIN
       (SELECT TRANSACTION_CODE
        FROM DWPROD.TRANSACTION_CODE
        WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
    WHERE PRODUCT_CATEGORY LIKE '10__'
      AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
      AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -12)
      AND CUSTOMER_ID IS NOT NULL
),
T3 AS (
    SELECT DISTINCT CUSTOMER_ID
    FROM DWPROD.DWA_STMT_EBANK TXN
    JOIN
      (SELECT TRANSACTION_CODE
      FROM DWPROD.TRANSACTION_CODE
      WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
    WHERE PRODUCT_CATEGORY LIKE '10__'
      AND PROCESS_DT < ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -12)
      AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -36)
      AND CUSTOMER_ID IS NOT NULL
),
T4 AS (
    SELECT DISTINCT T1.CUSTOMER_CDE
    FROM T1
    LEFT JOIN T2 ON T1.CUSTOMER_CDE = T2.CUSTOMER_ID
    LEFT JOIN T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_ID
    WHERE T2.CUSTOMER_ID IS NULL 
      AND T3.CUSTOMER_ID IS NOT NULL
)

SELECT CUSTOMER_CDE,
    'CASA_INACTIVE' FTR_NM,
    1 FTR_VAL,
    TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP 
FROM T4;


COMMIT;


/*
Feature Name: EB_MBIB_INACTIVE
Derived From: 
  DWPROD.DW_EB_TRANSACTION_FCT: 
    - CUSTOMER_CDE
    - TXN_DT
    - TXN_STATUS
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
TW: 36M
Tags: 
  - LABEL
*/
INSERT INTO CINS_FEATURE_STORE_V2
WITH 
T1 AS (
  SELECT CUSTOMER_CDE
  FROM DWPROD.DW_CUSTOMER_DIM
  WHERE ACTIVE = 1
    AND COMPANY_KEY = 1
    AND SUB_SECTOR_CDE IN ('1700','1602')
),
T2 AS (
  SELECT DISTINCT CUSTOMER_CDE
  FROM DWPROD.DW_EB_TRANSACTION_FCT
  WHERE TXN_STATUS = 'SUC'
    AND TXN_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
    AND TXN_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -12)
    AND CUSTOMER_CDE IS NOT NULL
),
T3 AS (
  SELECT DISTINCT CUSTOMER_CDE
  FROM DWPROD.DW_EB_TRANSACTION_FCT
  WHERE TXN_STATUS = 'SUC'
    AND TXN_DT < ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -12)
    AND TXN_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -36)
    AND CUSTOMER_CDE IS NOT NULL
),
T4 AS (
  SELECT T1.CUSTOMER_CDE
  FROM T1
  LEFT JOIN T2 ON T1.CUSTOMER_CDE = T2.CUSTOMER_CDE
  LEFT JOIN T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_CDE
  WHERE T2.CUSTOMER_CDE IS NULL
      AND T3.CUSTOMER_CDE IS NOT NULL
)
SELECT CUSTOMER_CDE,
  'EB_MBIB_INACTIVE' FTR_NM,
  1 FTR_VAL,
  TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM T4;


COMMIT;


/*
Feature Name: CARD_CREDIT_INACTIVE
Derived From: 
    DWPROD.DW_CUSTOMER_DIM: 
        - CUSTOMER_CDE
        -  ACTIVE
        -  COMPANY_KEY
        -  SUB_SECTOR_CDE
    DWPROD.DW_CARD_MASTER_DIM: 
        - CUSTOMER_CDE
        - CARD_CDE
        - STATUS_CDE
        - ACTIVATION_DT
    DWPROD.DW_CARD_TRANSACTION_FCT: 
        - CARD_CDE
        - TRAN_STATUS
        - POST_DT
        - MCC_CDE
        - PROCESS_DT
        - CUSTOMER_CDE
Tags: 
- LABEL
TW: 3M
*/
INSERT INTO CINS_FEATURE_STORE_V2
WITH 
T1 AS (
    SELECT CUSTOMER_CDE
    FROM DWPROD.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1
    AND COMPANY_KEY = 1
    AND SUB_SECTOR_CDE IN ('1700','1602')
),
T2 AS (
    SELECT DISTINCT CUSTOMER_CDE
    FROM DWPROD.DW_CARD_MASTER_DIM
    WHERE CARD_CDE LIKE '3%'
        AND STATUS_CDE = ' '
        AND ACTIVATION_DT < ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -3)
        AND TO_CHAR(ACTIVATION_DT, 'yyyy') > 1900
        AND CUSTOMER_CDE IS NOT NULL
),
T3 AS (
    SELECT DISTINCT CUSTOMER_CDE
    FROM DWPROD.DW_CARD_TRANSACTION_FCT
    WHERE CARD_CDE LIKE '3%'
        AND TRAN_STATUS = 'S'
        AND POST_DT IS NOT NULL
        AND MCC_CDE NOT IN (0,4829)
        AND MCC_CDE IS NOT NULL
        AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
        AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -3)
        AND CUSTOMER_CDE IS NOT NULL
),
T4 AS (
    SELECT DISTINCT T1.CUSTOMER_CDE
    FROM T1
    LEFT JOIN T2 ON T1.CUSTOMER_CDE = T2.CUSTOMER_CDE
    LEFT JOIN T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_CDE
    WHERE T2.CUSTOMER_CDE IS NOT NULL 
        AND T3.CUSTOMER_CDE IS NULL
)

SELECT 
    CUSTOMER_CDE,
    'CARD_CREDIT_INACTIVE' FTR_NM,
    1 FTR_VAL,
    TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM T4;


COMMIT;


/*
Feature Name: EB_SACOMPAY_INACTIVE
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
  DWPROD.DW_EWALL_TRANSACTION_FCT: 
    - CUSTOMER_CDE
    - TXN_DT
    - TXN_STATUS
Tags: 
    - LABEL
*/
INSERT INTO CINS_FEATURE_STORE_V2
WITH
T1 AS (
    SELECT CUSTOMER_CDE
    FROM DWPROD.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1
        AND COMPANY_KEY = 1
        AND SUB_SECTOR_CDE IN ('1700','1602')
),
T2 AS (
    SELECT DISTINCT CUSTOMER_CDE
    FROM DWPROD.DW_EWALL_TRANSACTION_FCT
    WHERE TXN_STATUS = 'S'
        AND TXN_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
        AND TXN_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -12)
        AND CUSTOMER_CDE IS NOT NULL
),
T3 AS (
    SELECT DISTINCT CUSTOMER_CDE
    FROM DWPROD.DW_EWALL_TRANSACTION_FCT
    WHERE TXN_STATUS = 'S'
        AND TXN_DT < ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -12)
        AND TXN_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -36)
        AND CUSTOMER_CDE IS NOT NULL
),
T4 AS (
    SELECT DISTINCT T1.CUSTOMER_CDE
    FROM T1
    LEFT JOIN T2 ON T1.CUSTOMER_CDE = T2.CUSTOMER_CDE
    LEFT JOIN T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_CDE
    WHERE T2.CUSTOMER_CDE IS NULL 
      AND T3.CUSTOMER_CDE IS NOT NULL
)
SELECT CUSTOMER_CDE,
    'EB_SACOMPAY_INACTIVE' FTR_NM,
    1 FTR_VAL,
    TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM T4;


COMMIT;


/*
Feature Name: AGE
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
TW: ALL
*/

INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
    'AGE' FTR_NM,
    FLOOR(MONTHS_BETWEEN(TO_DATE('01-10-2023', 'DD-MM-YY'), BIRTHDAY)/12) FTR_VAL,
    TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
    AND COMPANY_KEY = 1
    AND SUB_SECTOR_CDE IN ('1700','1602')
    AND CUS_OPEN_DT < TO_DATE('01-10-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: GEN_GRP
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
  'GEN_GRP' FTR_NM,
  CASE
      WHEN EXTRACT(YEAR FROM BIRTHDAY) < 1965 THEN 'Trước Gen X'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 1965 AND 1980 THEN 'Gen X'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 1981 AND 1996 THEN 'Gen Y'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 1997 AND 2012 THEN 'Gen Z'
      WHEN EXTRACT(YEAR FROM BIRTHDAY) BETWEEN 2013 AND 2025 THEN 'Gen A'
  END FTR_VAL,
  TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700','1602')
  AND CUS_OPEN_DT < TO_DATE('01-10-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: PROFESSION
Derived From: 
  DWPROD.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - ACTIVE
    - COMPANY_KEY
    - SUB_SECTOR_CDE
    - CUS_OPEN_DT
    - SUB_INDUSTRY_CDE
  DWPROD.DW_INDUSTRY_DIM: 
    - SUB_INDUSTRY_CDE
Tags: 
  - DEMOGRAPHIC
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
  'PROFESSION' FTR_NM,
  INDUSTRY_GROUP_NAME_VN FTR_VAL,
  TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_CUSTOMER_DIM A
JOIN DWPROD.DW_INDUSTRY_DIM B ON A.SUB_INDUSTRY_CDE = B.SUB_INDUSTRY_CDE
WHERE A.ACTIVE = 1
  AND A.COMPANY_KEY = 1
  AND A.SUB_SECTOR_CDE IN ('1700','1602')
  AND CUS_OPEN_DT < TO_DATE('01-10-2023', 'DD-MM-YY');


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
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-10-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 18 AND 26 THEN 'Bắt đầu sự nghiệp'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-10-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 27 AND 35 THEN 'Lập gia đình'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-10-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 36 AND 45 THEN 'Thiết lập tài sản'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-10-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 46 AND 54 THEN 'Bảo vệ tài sản'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-10-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 55 AND 64 THEN 'Cuối sự nghiệp'
      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('01-10-2023', 'DD-MM-YY'), BIRTHDAY)/12) >= 65 THEN 'Nghỉ hưu'
      ELSE NULL
  END FTR_VAL,
  TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700','1602')
  AND CUS_OPEN_DT < TO_DATE('01-10-2023', 'DD-MM-YY');


COMMIT;


/*
Feature Name: CASA_AVG_BAL_1M
Derived From: 
  DWPROD.DW_DEPOSIT_FCT: 
    - CATEGORY_CDE
    - CUSTOMER_CDE
    - ACTUAL_BAL_LCL
    - PROCESS_DT
  CINS_TMP_CUSTOMER_01102023:
    - CUSTOMER_CDE 
Tags: 
  - CASA
TW: 1M
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
  'CASA_AVG_BAL_1M' AS FTR_NM,
  AVG(ACTUAL_BAL_LCL) AS FTR_VAL,
  TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
  CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_DEPOSIT_FCT
WHERE CATEGORY_CDE LIKE '10__'
  AND CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_CUSTOMER_01102023)
  AND ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_CT_ACCT_ACTIVE
Derived From: 
  DWPROD.DW_ACCOUNT_MASTER_DIM:  
    - ACCT_ID
    - ACTIVE
    - CUSTOMER_CDE
  DWPROD.DWA_STMT_EBANK: 
    - TRANSACTION_CODE
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - ACCOUNT_NUMBER
    - PROCESS_DT
  DWPROD.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01102023:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 12M
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_CT_ACCT_ACTIVE' FTR_NM,
        COUNT(DISTINCT ACCT_ID) FTR_VAL,
        TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_ACCOUNT_MASTER_DIM DIM
WHERE ACTIVE = 1
  AND EXISTS
    (SELECT CUSTOMER_ID
     FROM DWPROD.DWA_STMT_EBANK FCT
     JOIN
       (SELECT TRANSACTION_CODE
        FROM DWPROD.TRANSACTION_CODE
        WHERE INITIATION = 'CUSTOMER') TC ON FCT.TRANSACTION_CODE = TC.TRANSACTION_CODE
     WHERE DIM.CUSTOMER_CDE = FCT.CUSTOMER_ID
       AND PRODUCT_CATEGORY LIKE '10__'
       AND CUSTOMER_ID IN
         (SELECT CUSTOMER_CDE
          FROM CINS_TMP_CUSTOMER_01102023)
       AND DIM.ACCT_ID = FCT.ACCOUNT_NUMBER
       AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -12) )
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_CT_TXN_1M
Derived From: 
  DWPROD.DWA_STMT_EBANK: 
    - STMT_ENTRY_ID
    - CUSTOMER_ID
    - PRODUCT_CATEGORY
    - PROCESS_DT
    - TRANSACTION_CODE
  DWPROD.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01102023:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 1M
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_CT_TXN_1M' AS FTR_NM,
    COUNT(STMT_ENTRY_ID) AS FTR_VAL,
    TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DWPROD.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DWPROD.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -1)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01102023)
GROUP BY CUSTOMER_ID;


COMMIT;


/* 
Feature Name: CASA_DAY_SINCE_LTST_TXN
Derived From: 
  DWPROD.DWA_STMT_EBANK: 
    - CUSTOMER_ID
    - PROCESS_DT
    - PRODUCT_CATEGORY
    - TRANSACTION_CODE
  DWPROD.TRANSACTION_CODE: 
    - TRANSACTION_CODE
  CINS_TMP_CUSTOMER_01102023:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 36M
*/ 
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_ID AS CUSTOMER_CDE,
    'CASA_DAY_SINCE_LTST_TXN' FTR_NM,
    TO_DATE('01-10-2023', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -36)) FTR_VAL,
    TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DWPROD.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -36)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01102023)
GROUP BY CUSTOMER_ID;


COMMIT;


/*
Feature Name: CASA_MAX_BAL_1M
Derived From: 
  DWPROD.DW_DEPOSIT_FCT:  
    - ACTUAL_BAL_LCL
    - CATEGORY_CDE
    - CUSTOMER_CDE
    - PROCESS_DT
  CINS_TMP_CUSTOMER_01102023:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 1M
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
    'CASA_MAX_BAL_1M' AS FTR_NM,
    MAX(ACTUAL_BAL_LCL) AS FTR_VAL,
    TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_DEPOSIT_FCT
WHERE CATEGORY_CDE LIKE '10__'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01102023)
  AND ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_MIN_BAL_1M
Derived From: 
  DWPROD.DW_DEPOSIT_FCT: 
    - ACTUAL_BAL_LCL
    - CATEGORY_CDE
    - CUSTOMER_CDE
    - PROCESS_DT
  CINS_TMP_CUSTOMER_01102023:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 1M
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_MIN_BAL_1M' AS FTR_NM,
       MIN(ACTUAL_BAL_LCL) AS FTR_VAL,
       TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DW_DEPOSIT_FCT
WHERE CATEGORY_CDE LIKE '10__'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01102023)
  AND ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;


COMMIT;


/*
Feature Name: CASA_SUM_TXN_AMT_1M
Derived From: 
  DWPROD.DWA_STMT_EBANK: 
    - AMT_LCY
    - PRODUCT_CATEGORY
    - PROCESS_DT
    - CUSTOMER_CDE
    - TRANSACTION_CODE
  DWPROD.TRANSACTION_CODE: 
    - TRANSACTION_CODE
    - INITIATION
  CINS_TMP_CUSTOMER_01102023:
    - CUSTOMER_CDE
Tags: 
  - CASA
TW: 1M
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_ID AS CUSTOMER_CDE,
       'CASA_SUM_TXN_AMT_1M' FTR_NM,
        NVL(SUM(ABS(AMT_LCY)), 0) FTR_VAL,
        TO_DATE('01-10-2023', 'DD-MM-YY') AS RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
FROM DWPROD.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DWPROD.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('01-10-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('01-10-2023', 'DD-MM-YY'), -1)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_01102023)
GROUP BY CUSTOMER_ID;


COMMIT;