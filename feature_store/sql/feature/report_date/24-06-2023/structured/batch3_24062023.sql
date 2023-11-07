/*
Feature Name:
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'SHOPPING_CT_TXN_1M' FEATURE_NM,
                            count(*) FEATURE_VAL,
                            TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: UTILITIES_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'UTILITIES_CT_TXN_1M' FEATURE_NM,
                             count(*) FEATURE_VAL,
                             TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: VEHICLES_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'VEHICLES_CT_TXN_1M' FEATURE_NM,
                            count(*) FEATURE_VAL,
                            TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: CASH_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'CASH_CT_TXN_1M' FEATURE_NM,
                        count(*) FEATURE_VAL,
                        TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: CHILD_PET_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'CHILD_PET_CT_TXN_1M' FEATURE_NM,
                             count(*) FEATURE_VAL,
                             TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: EDUCATION_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'EDUCATION_CT_TXN_1M' FEATURE_NM,
                             count(*) FEATURE_VAL,
                             TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: FOOD_GROCERY_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'FOOD_GROCERY_CT_TXN_1M' FEATURE_NM,
                                count(*) FEATURE_VAL,
                                TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: HOBBIES_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'HOBBIES_CT_TXN_1M' FEATURE_NM,
                           count(*) FEATURE_VAL,
                           TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: HOBBIES_ENTERTAINMENT_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'HOBBIES_ENTERTAINMENT_CT_TXN_1M' FEATURE_NM,
                                         count(*) FEATURE_VAL,
                                         TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: HOBBIES_SPORT_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'HOBBIES_SPORT_CT_TXN_1M' FEATURE_NM,
                                 count(*) FEATURE_VAL,
                                 TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;

/*
Feature Name: INSURANCE_CT_TXN_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'INSURANCE_CT_TXN_1M' FEATURE_NM,
                             count(*) FEATURE_VAL,
                             TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -1)
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
GROUP BY A.CUSTOMER_CDE;



/*
Feature Name: CARD_TOP1_MERCHANT_6M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_24062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_TOP1_MERCHANT_6M' FTR_NM,
                               merchant_cde FTR_VAL,
                               TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
                               CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          merchant_cde,
          count(*) ct_txn_merchant,
          row_number()over(PARTITION BY CUSTOMER_CDE
                           ORDER BY count(*) DESC) rn1
   FROM
     (SELECT CUSTOMER_CDE,
             merchant_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                trim(' '
                     FROM(merchant_cde)) merchant_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -6)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM CINS_TMP_CUSTOMER_24062023) ))
   WHERE rn = 1
   GROUP BY CUSTOMER_CDE,
            merchant_cde)
WHERE rn1 = 1;

/*
Feature Name: CARD_CT_VAR_BRANCH_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_24062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CT_VAR_BRANCH_3M' FTR_NM,
                               count(DISTINCT sub_branch_cde) FTR_VAL,
                               TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
                               CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          sub_branch_cde,
          PROCESS_DT
   FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
   WHERE tran_status = 'S'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_24062023) )
WHERE PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -3)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_BRANCH_LOC_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_24062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_BRANCH_LOC_3M' FTR_NM,
                            sub_Branch_cde FTR_VAL,
                            TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
                            CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          sub_branch_cde,
          max(PROCESS_DT),
          ROW_NUMBER()OVER(PARTITION BY CUSTOMER_CDE
                           ORDER BY max(PROCESS_DT) DESC) RN
   FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
   WHERE tran_status = 'S'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_24062023)
     AND PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
     AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -3)
   GROUP BY CUSTOMER_CDE,
            sub_branch_cde)
WHERE RN = 1;

/*
Feature Name: CASA_CT_VAR_BRANCH_REG_3M
Derived From: DW_ACCOUNT_MASTER_DIM, CINS_TMP_CUSTOMER_24062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_CT_VAR_BRANCH_REG_3M' FTR_NM,
                                   count(DISTINCT sub_branch_cde) FTR_VAL,
                                   TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
                                   CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          acct_id,
          sub_branch_cde,
          row_number()over(PARTITION BY CUSTOMER_CDE, acct_id
                           ORDER BY update_dt DESC) rn
   FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM
   WHERE CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_24062023)
     AND open_dt < TO_DATE('24-06-2023', 'DD-MM-YY')
     AND active = 1
     AND open_dt >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -3)
     AND company_key = 1
     AND active = 1
     AND category_cde like '10__' )
WHERE rn = 1
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CT_VAR_BRANCH_REG_3M
Derived From: DW_CARD_MASTER_DIM, CINS_TMP_CUSTOMER_24062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CT_VAR_BRANCH_REG_3M' FTR_NM,
                                   count(DISTINCT sub_branch_cde)FTR_VAL,
                                   TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
                                   CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
WHERE anniv_dt < TO_DATE('24-06-2023', 'DD-MM-YY')
  AND anniv_dt >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -3)
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_24062023)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_CT_TXN_3M
Derived From: DW_EB_TRANSACTION_FCT, CINS_TMP_CUSTOMER_24062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_CT_TXN_3M' FTR_NM,
                      count(DISTINCT txn_id) FTR_VAL,
                      TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
                      CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          txn_id,
          txn_dt
   FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT
   WHERE CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_24062023)
     AND TXN_ENTRY_STATUS = 'SUC' )
WHERE TXN_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
  AND TXN_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -3)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_SUM_TXN_AMT_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_24062023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_SUM_TXN_AMT_3M' FTR_NM,
                             SUM(ABS(AMT_BILL)) FTR_VAL,
                             TO_DATE('24-06-2023', 'DD-MM-YY') AS RPT_DT,
                             CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          cardhdr_no,
          approval_cde,
          retrvl_refno,
          PROCESS_DT,
          amt_bill,
          row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno, amt_bill
                           ORDER BY PROCESS_DT DESC) rn
   FROM
     (SELECT CUSTOMER_CDE,
             cardhdr_no,
             TRIM(' '
                  FROM (approval_cde)) approval_cde,
             retrvl_refno,
             amt_bill,
             PROCESS_DT
      FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
      WHERE CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_24062023)
        AND tran_status = 'S'
        AND PROCESS_DT < TO_DATE('24-06-2023', 'DD-MM-YY')
        AND PROCESS_DT >= ADD_MONTHS(TO_DATE('24-06-2023', 'DD-MM-YY'), -3)
      GROUP BY CUSTOMER_CDE,
               cardhdr_no,
               TRIM(' '
                    FROM (approval_cde)),
               retrvl_refno,
               amt_bill,
               PROCESS_DT))
WHERE rn = 1
GROUP BY CUSTOMER_CDE;



