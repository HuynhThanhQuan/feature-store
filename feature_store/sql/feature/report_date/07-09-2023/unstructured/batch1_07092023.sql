/*
Feature Name: EB_MBIB_DAY_SINCE_LTST_TXN
Derived From: DW_EB_TRANSACTION_FCT, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MBIB_DAY_SINCE_LTST_TXN' FTR_NM,
                                    TO_DATE('07-09-2023', 'DD-MM-YY') - NVL(MAX(TXN_DT), ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)) FTR_VAL,
                                    TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                                    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT
WHERE TXN_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)
  AND TXN_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
  AND TXN_STATUS = 'SUC'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_07092023)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_MBIB_INACTIVE
Derived From: DW_EB_TRANSACTION_FCT, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MBIB_INACTIVE' FTR_NM,
                          1 FTR_VAL,
                          TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                          CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT
     WHERE TXN_STATUS = 'SUC'
       AND TXN_DT < ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12)
       AND TXN_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)
       AND CUSTOMER_CDE IS NOT NULL)
  AND CUSTOMER_CDE NOT IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT
     WHERE TXN_STATUS = 'SUC'
       AND TXN_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
       AND TXN_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12)
       AND CUSTOMER_CDE IS NOT NULL);

/*
Feature Name: EB_SACOMPAY_DAY_SINCE_LTST_TXN
Derived From: DW_EWALL_TRANSACTION_FCT, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_DAY_SINCE_LTST_TXN' FTR_NM,
                                        TO_DATE('07-09-2023', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)) FTR_VAL,
                                        TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                                        CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
WHERE PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)
  AND TXN_STATUS = 'S'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_07092023)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_SACOMPAY_CT_INACTIVE
Derived From: DW_EWALL_TRANSACTION_FCT, CINS_TMP_CUSTOMER_07092023, DW_EWALL_USER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_CT_INACTIVE' FTR_NM,
                                 COUNT(*) FTR_VAL,
                                 TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                                 CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          NVL(MONTHS_BETWEEN(LEAD(TXN_DT) OVER (PARTITION BY CUSTOMER_CDE
                                                ORDER BY TXN_DT), TXN_DT), MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), TXN_DT)) TXN_GAP
   FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
   WHERE TXN_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
     AND TXN_STATUS = 'S'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_07092023)
   UNION ALL SELECT B.CUSTOMER_CDE,
                    MONTHS_BETWEEN(MIN(TXN_DT), MAX(IDENTIFY_DT)) TXN_GAP
   FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT A
   JOIN DW_ANALYTICS.DW_EWALL_USER_DIM B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE
   WHERE TXN_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
     AND TXN_STATUS = 'S'
     AND TXN_DT > IDENTIFY_DT
     AND B.CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_07092023)
   GROUP BY B.CUSTOMER_CDE)
WHERE TXN_GAP > 12
  AND TXN_GAP <= 36
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_SACOMPAY_INACTIVE
Derived From: DW_EWALL_TRANSACTION_FCT, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_INACTIVE' FTR_NM,
                              1 FTR_VAL,
                              TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                              CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
     WHERE TXN_STATUS = 'S'
       AND TXN_DT < ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12)
       AND TXN_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)
       AND CUSTOMER_CDE IS NOT NULL)
  AND CUSTOMER_CDE NOT IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
     WHERE TXN_STATUS = 'S'
       AND TXN_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
       AND TXN_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12)
       AND CUSTOMER_CDE IS NOT NULL);

/*
Feature Name: CARD_CREDIT_DAY_SINCE_LTST_TXN
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_DAY_SINCE_LTST_TXN' FTR_NM,
                                        TO_DATE('07-09-2023', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)) FTR_VAL,
                                        TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                                        CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
WHERE PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)
  AND CARD_CDE LIKE '3%'
  AND TRAN_STATUS = 'S'
  AND POST_DT IS NOT NULL
  AND (MCC_CDE NOT IN (0,
                       6010,
                       6011,
                       6012,
                       4829,
                       6051)
       AND MCC_CDE IS NOT NULL
       OR MCC_CDE IN (6010,
                      6011,
                      6211,
                      6012,
                      6051))
  AND ACQUIRER_REFNO NOT LIKE '% %'
  AND PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_CUSTOMER_DIM
     WHERE ACTIVE = 1
       AND COMPANY_KEY = 1
       AND SUB_SECTOR_CDE IN ('1700',
                              '1602'))
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_SUM_BAL_NOW
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_BAL_NOW' FTR_NM,
                                 SUM(TT_ORIGINAL_BALANCE) FTR_VAL,
                                 TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
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
      FROM DW_ANALYTICS.DATA_RPT_CARD_493 CC
      WHERE PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
        AND CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_07092023)
        AND CARD_CDE LIKE '3%' ) BAL) BAL
WHERE RN = 1
GROUP BY CUSTOMER_CDE;


/*
Feature Name: CARD_CREDIT_CT_CARD_ACTIVE
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_CARD_ACTIVE' FTR_NM,
                                    COUNT(DISTINCT CARD_CDE) FTR_VAL,
                                    TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                                    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM DIM
WHERE ACTIVE = 1
  AND CARD_CDE LIKE '3%'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_07092023)
  AND EXISTS
    (SELECT CUSTOMER_CDE,
            CARD_CDE
     FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT FCT
     WHERE DIM.CUSTOMER_CDE = FCT.CUSTOMER_CDE
       AND DIM.CARD_CDE = FCT.CARD_CDE
       AND CARD_CDE LIKE '3%'
       AND TRAN_STATUS = 'S'
       AND POST_DT IS NOT NULL
       AND (MCC_CDE NOT IN (0,
                            6010,
                            6011,
                            6012,
                            4829,
                            6051)
            AND MCC_CDE IS NOT NULL
            OR MCC_CDE IN (6010,
                           6011,
                           6211,
                           6012,
                           6051))
       AND PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12))
GROUP BY CUSTOMER_CDE;


/*
Feature Name: CARD_CREDIT_CT_INACTIVE_ALL
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_INACTIVE_ALL' FTR_NM,
                                     COUNT(*) FTR_VAL,
                                     TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                                     CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          NVL(MONTHS_BETWEEN(LEAD(PROCESS_DT) OVER (PARTITION BY CUSTOMER_CDE
                                                    ORDER BY PROCESS_DT), PROCESS_DT), MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), PROCESS_DT)) TXN_GAP
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_07092023
   WHERE PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY'))
WHERE TXN_GAP > 3
GROUP BY CUSTOMER_CDE;


/*
Feature Name: CARD_CREDIT_INACTIVE
Derived From: DW_CUSTOMER_DIM, DW_CARD_TRANSACTION_FCT, DW_CARD_MASTER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_INACTIVE' FTR_NM,
                              1 FTR_VAL,
                              TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                              CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
     WHERE CARD_CDE LIKE '3%'
       AND STATUS_CDE = ' '
       AND ACTIVATION_DT < ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -3)
       AND TO_CHAR(ACTIVATION_DT, 'yyyy') > 1900)
  AND CUSTOMER_CDE NOT IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
     WHERE CARD_CDE LIKE '3%'
       AND TRAN_STATUS = 'S'
       AND POST_DT IS NOT NULL
       AND MCC_CDE NOT IN (0,
                           4829)
       AND MCC_CDE IS NOT NULL
       AND PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -3));


/*
Feature Name: CASA_CT_ACCT_ACTIVE
Derived From: DW_ACCOUNT_MASTER_DIM, DWA_STMT_EBANK, TRANSACTION_CODE, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_CT_ACCT_ACTIVE' FTR_NM,
                             COUNT(DISTINCT ACCT_ID) FTR_VAL,
                             TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                             CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM DIM
WHERE ACTIVE = 1
  AND EXISTS
    (SELECT CUSTOMER_ID
     FROM DW_ANALYTICS.DWA_STMT_EBANK FCT
     JOIN
       (SELECT TRANSACTION_CODE
        FROM DW_ANALYTICS.TRANSACTION_CODE
        WHERE INITIATION = 'CUSTOMER') TC ON FCT.TRANSACTION_CODE = TC.TRANSACTION_CODE
     WHERE DIM.CUSTOMER_CDE = FCT.CUSTOMER_ID
       AND PRODUCT_CATEGORY LIKE '10__'
       AND CUSTOMER_ID IN
         (SELECT CUSTOMER_CDE
          FROM CINS_TMP_CUSTOMER_07092023)
       AND DIM.ACCT_ID = FCT.ACCOUNT_NUMBER
       AND PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12) )
GROUP BY CUSTOMER_CDE;

/* 
Feature Name: CASA_DAY_SINCE_LTST_TXN
Derived From: DWA_STMT_EBANK, TRANSACTION_CODE, CINS_TMP_CUSTOMER_07092023
*/ 
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_ID CUSTOMER_CDE,
       'CASA_DAY_SINCE_LTST_TXN' FTR_NM,
                                 TO_DATE('07-09-2023', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)) FTR_VAL,
                                 TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                                 CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DW_ANALYTICS.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_07092023)
GROUP BY CUSTOMER_ID;

/*
Feature Name: CASA_SUM_BAL_NOW
Derived From: DW_DEPOSIT_FCT, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_SUM_BAL_NOW' FTR_NM,
                          SUM(ACTUAL_BAL_LCL) FTR_VAL,
                          TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                          CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          ACTUAL_BAL_LCL
   FROM DW_ANALYTICS.DW_DEPOSIT_FCT CS
   WHERE CATEGORY_CDE LIKE '10__'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_07092023)
     AND PROCESS_DT IN
       (SELECT MAX(PROCESS_DT)
        FROM DW_ANALYTICS.DW_DEPOSIT_FCT
        WHERE PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
          AND CATEGORY_CDE LIKE '10__' ) )
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CASA_INACTIVE
Derived From: DWA_STMT_EBANK, TRANSACTION_CODE, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_INACTIVE' FTR_NM,
                       1 FTR_VAL,
                       TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                       CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUS_OPEN_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
  AND CUSTOMER_CDE NOT IN
    (SELECT CUSTOMER_ID
     FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
     JOIN
       (SELECT TRANSACTION_CODE
        FROM DW_ANALYTICS.TRANSACTION_CODE
        WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
     WHERE PRODUCT_CATEGORY LIKE '10__'
       AND PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY')
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12))
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_ID
     FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
     JOIN
       (SELECT TRANSACTION_CODE
        FROM DW_ANALYTICS.TRANSACTION_CODE
        WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
     WHERE PRODUCT_CATEGORY LIKE '10__'
       AND PROCESS_DT < ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -12)
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -36));

/*
Feature Name: AGE
Derived From: DW_CUSTOMER_DIM
*/

INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'AGE' FTR_NM,
             FLOOR(MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), BIRTHDAY)/12) FTR_VAL,
             TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
             CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUS_OPEN_DT < TO_DATE('07-09-2023', 'DD-MM-YY');

/*
Feature Name: LIFE_STG
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'LIFE_STG' FTR_NM,
                  CASE
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 18 AND 26 THEN 'Bắt đầu sự nghiệp'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 27 AND 35 THEN 'Lập gia đình'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 36 AND 45 THEN 'Thiết lập tài sản'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 46 AND 54 THEN 'Bảo vệ tài sản'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 55 AND 64 THEN 'Cuối sự nghiệp'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('07-09-2023', 'DD-MM-YY'), BIRTHDAY)/12) >= 65 THEN 'Nghỉ hưu'
                      ELSE NULL
                  END FTR_VAL,
                  TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                  CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUS_OPEN_DT < TO_DATE('07-09-2023', 'DD-MM-YY');

/*
Feature Name: GEN_GRP
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'GEN_GRP' FTR_NM,
                 CASE
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) < 1965 THEN 'Trước Gen X'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 1965 AND 1980 THEN 'Gen X'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 1981 AND 1996 THEN 'Gen Y'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 1997 AND 2012 THEN 'Gen Z'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 2013 AND 2025 THEN 'Gen A'
                 END FTR_VAL,
                 TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                 CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUS_OPEN_DT < TO_DATE('07-09-2023', 'DD-MM-YY');


/*
Feature Name: PROFESSION
Derived From: DW_CUSTOMER_DIM, DW_INDUSTRY_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'PROFESSION' FTR_NM,
                    INDUSTRY_GROUP_NAME_VN FTR_VAL,
                    TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM A
JOIN DW_ANALYTICS.DW_INDUSTRY_DIM B ON A.SUB_INDUSTRY_CDE = B.SUB_INDUSTRY_CDE
WHERE A.ACTIVE = 1
  AND A.COMPANY_KEY = 1
  AND A.SUB_SECTOR_CDE IN ('1700',
                           '1602')
  AND CUS_OPEN_DT < TO_DATE('07-09-2023', 'DD-MM-YY');

/*
Feature Name: DEBT_GRP
Derived From: DW_CUSTOMER_FULL_DIM, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'DEBT_GRP' FTR_NM,
                  DB_GRP_CIC FTR_VAL,
                  TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                  CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_FULL_DIM
WHERE DB_GRP_CIC IS NOT NULL
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_CUSTOMER_DIM
     WHERE ACTIVE = 1
       AND COMPANY_KEY = 1
       AND SUB_SECTOR_CDE IN ('1700',
                              '1602')
       AND CUS_OPEN_DT < TO_DATE('07-09-2023', 'DD-MM-YY'));

/*
Feature Name: AREA, BRANCH
Derived From: DW_ORG_LOCATION_DIM, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2 WITH CUST AS
  (SELECT CUSTOMER_CDE,
          COMPANY_BOOK
   FROM DW_ANALYTICS.DW_CUSTOMER_DIM
   WHERE ACTIVE = 1
     AND COMPANY_KEY = 1
     AND SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND CUS_OPEN_DT < TO_DATE('07-09-2023', 'DD-MM-YY') )
SELECT C.CUSTOMER_CDE,
       'AREA' FTR_NM,
              O.AREA_CDE FTR_VAL,
              TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
              CURRENT_TIMESTAMP ADD_TSTP
FROM CUST C
JOIN DW_ANALYTICS.DW_ORG_LOCATION_DIM O ON C.COMPANY_BOOK = O.SUB_BRANCH_CDE
WHERE O.ACTIVE = 1
UNION ALL
SELECT C.CUSTOMER_CDE,
       'BRANCH' FTR_NM,
                O.BRANCH_CDE FTR_VAL,
                TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                CURRENT_TIMESTAMP ADD_TSTP
FROM CUST C
JOIN DW_ANALYTICS.DW_ORG_LOCATION_DIM O ON C.COMPANY_BOOK = O.SUB_BRANCH_CDE
WHERE O.ACTIVE = 1;

/*
Feature Name: INACTIVE
Derived From: CINS_TMP_CUSTOMER_STATUS_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'INACTIVE' FTR_NM,
       CASE
           WHEN CUST_STT = 1 THEN 1
           WHEN CUST_STT = 2 THEN 0
       END FTR_VAL,
       TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CUSTOMER_STATUS_07092023
WHERE CUST_STT >= 1
  AND RPT_DT = TO_DATE('07-09-2023', 'DD-MM-YY');

/*
Feature Name: REACTIVATED
Derived From: CINS_TMP_CUSTOMER_STATUS_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'REACTIVATED' FTR_NM,
       1 FTR_VAL,
       TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CUSTOMER_STATUS_07092023
WHERE CUST_STT = 2
  AND CUST_STT_CHG = 1
  AND RPT_DT = TO_DATE('07-09-2023', 'DD-MM-YY');

/*
Feature Name: EB_MBIB_HOLD
Derived From: DW_EB_USER, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT DISTINCT CUSTOMER_CDE,
                'EB_MBIB_HOLD' FTR_NM,
                1 FTR_VAL,
                TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EB_USER
WHERE PROCESS_DT IN
    (SELECT MAX(PROCESS_DT)
     FROM DW_ANALYTICS.DW_EB_USER
     WHERE PROCESS_DT < TO_DATE('07-09-2023', 'DD-MM-YY') )
  AND DEL_FLG = 'N'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_07092023);

/*
Feature Name: EB_SACOMPAY_HOLD
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT DISTINCT CUSTOMER_CDE,
                'EB_SACOMPAY_HOLD' FTR_NM,
                1 FTR_VAL,
                TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
                CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_USER_DIM
WHERE FIRST_SIGNED_ON < TO_DATE('07-09-2023', 'DD-MM-YY')
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_07092023);

/*
Feature Name: EB_MBIB_DAY_SINCE_ACTIVE
Derived From: DW_EB_USER, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MBIB_DAY_SINCE_ACTIVE' FTR_NM,
       0 FTR_VAL,
       TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          min(ACTIVATE_DT) ACTIVATE_DATE
   FROM DW_ANALYTICS.DW_EB_USER t1
   WHERE EXISTS
       (SELECT 1
        FROM CINS_TMP_CUSTOMER_07092023 t2
        WHERE t1.CUSTOMER_CDE=t2.CUSTOMER_CDE)
     AND ACTIVATE_DT = TO_DATE('01/01/2400', 'DD/MM/YYYY')
   GROUP BY customer_cde) A;

/*
Feature Name: EB_MBIB_DAY_SINCE_ACTIVE
Derived From: DW_EB_USER, CINS_TMP_CUSTOMER_07092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MBIB_DAY_SINCE_ACTIVE' FTR_NM,
       TO_DATE('07-09-2023', 'DD-MM-YY') - ACTIVATE_DATE FTR_VAL,
       TO_DATE('07-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          min(ACTIVATE_DT) ACTIVATE_DATE
   FROM DW_ANALYTICS.DW_EB_USER t1
   WHERE EXISTS
       (SELECT 1
        FROM CINS_TMP_CUSTOMER_07092023 t2
        WHERE t1.CUSTOMER_CDE=t2.CUSTOMER_CDE)
     AND TO_DATE(ACTIVATE_DT) <= TO_DATE('07-09-2023', 'DD-MM-YY')
     AND ACTIVATE_DT != TO_DATE('01/01/2400', 'DD/MM/YYYY')
   GROUP BY customer_cde) A;