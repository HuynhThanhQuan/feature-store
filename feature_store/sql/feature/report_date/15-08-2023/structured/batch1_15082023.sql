/*
Feature Name: EB_MBIB_SUM_TXN_AMT_1M, EB_MBIB_SUM_TXN_AMT_3M, EB_MBIB_SUM_TXN_AMT_6M
Derived From: DW_EB_TRANSACTION_FCT, CINS_TMP_CUSTOMER_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MBIB_SUM_TXN_AMT_1M' FTR_NM,
                                NVL(SUM(ABS(AMT_ENTRY_LCL)), 0) FTR_VAL,
                                TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                                CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT DISTINCT TXN_ID,
                   CUSTOMER_CDE,
                   TXN_DT,
                   AMT_ENTRY_LCL
   FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT
   WHERE TXN_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
     AND TXN_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
     AND TXN_STATUS = 'SUC'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_15082023) ) TXN
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_MBIB_CT_TXN_1M, EB_MBIB_CT_TXN_3M, EB_MBIB_CT_TXN_6M
Derived From: DW_EB_TRANSACTION_FCT, CINS_TMP_CUSTOMER_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_MBIB_CT_TXN_1M' FTR_NM,
                           COUNT(TXN_ID) FTR_VAL,
                           TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                           CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT
WHERE TXN_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND TXN_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
  AND TXN_STATUS = 'SUC'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_15082023)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_SACOMPAY_SUM_TXN_AMT_1M, EB_SACOMPAY_SUM_TXN_AMT_3M, EB_SACOMPAY_SUM_TXN_AMT_6M
Derived From: DW_EWALL_TRANSACTION_FCT, CINS_TMP_CUSTOMER_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_SUM_TXN_AMT_1M' FTR_NM,
                                    NVL(SUM((TO_AMT)), 0) FTR_VAL,
                                    TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                                    CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT DISTINCT TXN_ID,
                   CUSTOMER_CDE,
                   PROCESS_DT,
                   ABS(TO_AMT) TO_AMT
   FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
   WHERE PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
     AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
     AND TXN_STATUS = 'S'
     AND CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_15082023) ) TXN
GROUP BY CUSTOMER_CDE;

/*
Feature Name: EB_SACOMPAY_CT_TXN_1M, EB_SACOMPAY_CT_TXN_3M, EB_SACOMPAY_CT_TXN_6M
Derived From: DW_EWALL_TRANSACTION_FCT, CINS_TMP_CUSTOMER_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_CT_TXN_1M' FTR_NM,
                               COUNT(DISTINCT TXN_ID) FTR_VAL,
                               TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                               CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
WHERE PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
  AND TXN_STATUS = 'S'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_15082023)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_SUM_TXN_AMT_1M, CARD_CREDIT_SUM_TXN_AMT_3M, CARD_CREDIT_SUM_TXN_AMT_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_TXN_AMT_1M' FTR_NM,
                                    NVL(SUM(ABS(AMT_BILL)), 0) FTR_VAL,
                                    TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                                    CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
WHERE PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_CT_TXN_1M, CARD_CREDIT_CT_TXN_3M, CARD_CREDIT_CT_TXN_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_TXN_1M' FTR_NM,
                               COUNT(*) FTR_VAL,
                               TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                               CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
WHERE PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_SUM_REV_CASH_1M, CARD_CREDIT_SUM_REV_CASH_3M, CARD_CREDIT_SUM_REV_CASH_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_REV_CASH_1M' FTR_NM,
                                     NVL(SUM(ABS(AMT_BILL)), 0) FTR_VAL,
                                     TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                                     CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
WHERE PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
  AND MCC_CDE IN ('6010',
                  '6011',
                  '6211',
                  '6012',
                  '6051')
GROUP BY CUSTOMER_CDE;


/*
Feature Name: CARD_CREDIT_SUM_REV_SALE_1M, CARD_CREDIT_SUM_REV_SALE_3M, CARD_CREDIT_SUM_REV_SALE_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_REV_SALE_1M' FTR_NM,
                                     NVL(SUM(ABS(AMT_BILL)), 0) FTR_VAL,
                                     TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                                     CURRENT_TIMESTAMP ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_15082023
WHERE PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
  AND MCC_CDE NOT IN (0,
                      6010,
                      6011,
                      6012,
                      4829,
                      6051)
  AND MCC_CDE IS NOT NULL
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CASA_CT_TXN_1M, CASA_CT_TXN_3M, CASA_CT_TXN_6M
Derived From: DWA_STMT_EBANK, TRANSACTION_CODE, CINS_TMP_CUSTOMER_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_ID CUSTOMER_CDE,
       'CASA_CT_TXN_1M' FTR_NM,
                        COUNT(STMT_ENTRY_ID) FTR_VAL,
                        TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                        CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DW_ANALYTICS.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_15082023)
GROUP BY CUSTOMER_ID;

/*
Feature Name: CASA_SUM_TXN_AMT_1M, CASA_SUM_TXN_AMT_3M, CASA_SUM_TXN_AMT_6M
Derived From: DWA_STMT_EBANK, TRANSACTION_CODE, CINS_TMP_CUSTOMER_15082023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_ID CUSTOMER_CDE,
       'CASA_SUM_TXN_AMT_1M' FTR_NM,
                             NVL(SUM(ABS(AMT_LCY)), 0) FTR_VAL,
                             TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
                             CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DW_ANALYTICS.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('15-08-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_15082023)
GROUP BY CUSTOMER_ID;

/*
Feature Name: TRAVEL_SM_AMT_1M, TRAVEL_SM_AMT_3M, TRAVEL_SM_AMT_6M
Derived From: CINS_MCC_CATEGORY, DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT customer_cde,
       'TRAVEL_SM_AMT_1M' FEATURE_NM,
       sum(A.AMT_BILL) FEATURE_VAL,
       TO_DATE('15-08-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT customer_cde,
          mcc_cde,
          AMT_BILL
   FROM
     (SELECT customer_cde,
             mcc_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             process_dt,
             AMT_BILL,
             row_number()over(PARTITION BY customer_cde, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY process_dt DESC) rn
      FROM
        (SELECT customer_cde,
                mcc_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                ABS(AMT_BILL) AMT_BILL,
                process_dt
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE process_dt < TO_DATE('15-08-2023', 'DD-MM-YY')
           AND process_dt >= ADD_MONTHS(TO_DATE('15-08-2023', 'DD-MM-YY'), -1)
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
GROUP BY a.CUSTOMER_CDE;