/*
Feature Name: APPLIANCES_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'APPLIANCES_SM_AMT_1M' FEATURE_NM,
                              sum(A.AMT_BILL) FEATURE_VAL,
                              TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                              CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'APPLIANCES') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;


/*
Feature Name: BEAUTY_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'BEAUTY_SM_AMT_1M' FEATURE_NM,
                          sum(A.AMT_BILL) FEATURE_VAL,
                          TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                          CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'BEAUTY') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: PUBLIC_SERVICE_HEALTHCARE_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'PUBLIC_SERVICE_HEALTHCARE_SM_AMT_1M' FEATURE_NM,
                                             sum(A.AMT_BILL) FEATURE_VAL,
                                             TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                                             CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'PUBLIC_SERVICE_HEALTHCARE') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;


/*
Feature Name: SERVICE_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'SERVICE_SM_AMT_1M' FEATURE_NM,
                           sum(A.AMT_BILL) FEATURE_VAL,
                           TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                           CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'SERVICE') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;


/*
Feature Name: SHOPPING_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'SHOPPING_SM_AMT_1M' FEATURE_NM,
                            sum(A.AMT_BILL) FEATURE_VAL,
                            TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                            CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'SHOPPING') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;


/*
Feature Name: UTILITIES_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'UTILITIES_SM_AMT_1M' FEATURE_NM,
                             sum(A.AMT_BILL) FEATURE_VAL,
                             TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                             CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'UTILITIES') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: VEHICLES_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'VEHICLES_SM_AMT_1M' FEATURE_NM,
                            sum(A.AMT_BILL) FEATURE_VAL,
                            TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                            CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'VEHICLES') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: CASH_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASH_SM_AMT_1M' FEATURE_NM,
                        sum(A.AMT_BILL) FEATURE_VAL,
                        TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                        CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'CASH') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: CHILD_PET_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CHILD_PET_SM_AMT_1M' FEATURE_NM,
                             sum(A.AMT_BILL) FEATURE_VAL,
                             TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                             CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'CHILD_PET') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;


/*
Feature Name: EDUCATION_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/

INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EDUCATION_SM_AMT_1M' FEATURE_NM,
                             sum(A.AMT_BILL) FEATURE_VAL,
                             TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                             CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'EDUCATION') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: FOOD_GROCERY_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'FOOD_GROCERY_SM_AMT_1M' FEATURE_NM,
                                sum(A.AMT_BILL) FEATURE_VAL,
                                TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                                CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'FOOD_GROCERY') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: HOBBIES_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'HOBBIES_SM_AMT_1M' FEATURE_NM,
                           sum(A.AMT_BILL) FEATURE_VAL,
                           TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                           CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'HOBBIES') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: HOBBIES_ENTERTAINMENT_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'HOBBIES_ENTERTAINMENT_SM_AMT_1M' FEATURE_NM,
                                         sum(A.AMT_BILL) FEATURE_VAL,
                                         TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                                         CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'HOBBIES_ENTERTAINMENT') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: HOBBIES_SPORT_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'HOBBIES_SPORT_SM_AMT_1M' FEATURE_NM,
                                 sum(A.AMT_BILL) FEATURE_VAL,
                                 TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                                 CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'HOBBIES_SPORT') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: INSURANCE_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'INSURANCE_SM_AMT_1M' FEATURE_NM,
                             sum(A.AMT_BILL) FEATURE_VAL,
                             TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                             CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             AMT_BILL,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'INSURANCE') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY a.CUSTOMER_CDE;

/*
Feature Name: TRAVEL_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'TRAVEL_CT_TXN_1M' FEATURE_NM,
                          count(*) FEATURE_VAL,
                          TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                          CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          cardhdr_no,
          approval_cde,
          retrvl_refno,
          PROCESS_DT
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'TRAVEL') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: APPLIANCES_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'APPLIANCES_CT_TXN_1M' FEATURE_NM,
                              count(*) FEATURE_VAL,
                              TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                              CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          cardhdr_no,
          approval_cde,
          retrvl_refno,
          PROCESS_DT
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'APPLIANCES') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: BEAUTY_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'BEAUTY_CT_TXN_1M' FEATURE_NM,
                          count(*) FEATURE_VAL,
                          TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                          CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          cardhdr_no,
          approval_cde,
          retrvl_refno,
          PROCESS_DT
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'BEAUTY') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: PUBLIC_SERVICE_HEALTHCARE_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'PUBLIC_SERVICE_HEALTHCARE_CT_TXN_1M' FEATURE_NM,
                                             count(*) FEATURE_VAL,
                                             TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                                             CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          cardhdr_no,
          approval_cde,
          retrvl_refno,
          PROCESS_DT
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'PUBLIC_SERVICE_HEALTHCARE') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: SERVICE_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'SERVICE_CT_TXN_1M' FEATURE_NM,
                           count(*) FEATURE_VAL,
                           TO_DATE('17-06-2023', 'DD-MM-YY') AS RPT_DT,
                           CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          mcc_cde,
          cardhdr_no,
          approval_cde,
          retrvl_refno,
          PROCESS_DT
   FROM
     (SELECT CUSTOMER_CDE,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('17-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('17-06-2023', 'DD-MM-YY'), -1)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM DW_ANALYTICS.DW_CUSTOMER_DIM
              WHERE SUB_SECTOR_CDE IN ('1700',
                                       '1602')
                AND ACTIVE = '1'
                AND COMPANY_KEY = '1') ))
   WHERE rn = 1 ) A
JOIN
  (SELECT *
   FROM CINS_MCC_CATEGORY
   WHERE CATEGORY = 'SERVICE') B ON A.MCC_CDE = B.MCC_CDE
GROUP BY A.CUSTOMER_CDE;

