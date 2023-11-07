/*
Feature Name: CARD_AVG_BAL_3M
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_AVG_BAL_3M' FTR_NM,
                         AVG(ABS(TT_ORIGINAL_BALANCE)) FTR_VAL,
                         TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
                         CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DATA_RPT_CARD_493
WHERE CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_23092023)
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -3)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_DEBT_GRP_6M
Derived From: DATA_RPT_CARD_493, CINS_TMP_CUSTOMER_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_DEBT_GRP_6M' FTR_NM,
                                 max(tt_loan_group) FTR_VAL,
                                 TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
                                 CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          tt_loan_group,
          PROCESS_DT
   FROM DW_ANALYTICS.DATA_RPT_CARD_493
   WHERE CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_23092023)
     AND CARD_CDE LIKE '3%' )
WHERE PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -6)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_FAV_BRANCH_LOC_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_FAV_BRANCH_LOC_3M' FTR_NM,
                                sub_branch_cde FTR_VAL,
                                TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
                                CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          sub_branch_cde,
          count(*) ct_txn_sub_branch,
          row_number()over(PARTITION BY CUSTOMER_CDE
                           ORDER BY count(*) DESC) rn1
   FROM
     (SELECT CUSTOMER_CDE,
             sub_branch_cde,
             cardhdr_no,
             approval_cde,
             retrvl_refno,
             PROCESS_DT,
             row_number()over(PARTITION BY CUSTOMER_CDE, cardhdr_no, approval_cde, retrvl_refno
                              ORDER BY PROCESS_DT DESC) rn
      FROM
        (SELECT CUSTOMER_CDE,
                trim(' '
                     FROM(sub_branch_cde)) sub_branch_cde,
                cardhdr_no,
                trim(' '
                     FROM (approval_cde)) approval_cde,
                retrvl_refno,
                PROCESS_DT
         FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
         WHERE PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -3)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM CINS_TMP_CUSTOMER_23092023) ))
   WHERE rn = 1
   GROUP BY CUSTOMER_CDE,
            sub_branch_cde)
WHERE rn1 = 1;

/*
Feature Name: CASA_AVG_BAL_1M
Derived From: DW_DEPOSIT_FCT, CINS_TMP_CUSTOMER_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_AVG_BAL_1M' AS FTR_NM,
       AVG(ACTUAL_BAL_LCL) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT
WHERE CATEGORY_CDE LIKE '10__'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_23092023)
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CASA_MAX_BAL_1M
Derived From: DW_DEPOSIT_FCT, CINS_TMP_CUSTOMER_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_MAX_BAL_1M' AS FTR_NM,
       MAX(ACTUAL_BAL_LCL) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT
WHERE CATEGORY_CDE LIKE '10__'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_23092023)
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CASA_MIN_BAL_1M
Derived From: DW_DEPOSIT_FCT, CINS_TMP_CUSTOMER_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CASA_MIN_BAL_1M' AS FTR_NM,
       MIN(ACTUAL_BAL_LCL) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_DEPOSIT_FCT
WHERE CATEGORY_CDE LIKE '10__'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_23092023)
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;


/*
Feature Name: CARD_CREDIT_CT_TXN_DOM_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_TXN_DOM_1M' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE ACQ_CNTRY_CDE = '704'
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_CT_TXN_INTER_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_TXN_INTER_1M' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE ACQ_CNTRY_CDE <> '704'
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_CT_TXN_ONLINE_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/ 
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_TXN_ONLINE_1M' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE TXN_OL_CDE = 'E'
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_CT_TXN_OFFLINE_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_TXN_OFFLINE_1M' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE TXN_OL_CDE NOT IN ('E')
  AND TXN_OL_CDE IS NOT NULL
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_SUM_TXN_DOM_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_TXN_DOM_1M' AS FTR_NM,
       SUM(AMT_BILL) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE ACQ_CNTRY_CDE = '704'
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_SUM_TXN_INTER_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_TXN_INTER_1M' AS FTR_NM,
       SUM(AMT_BILL) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE ACQ_CNTRY_CDE <> '704'
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_SUM_TXN_ONLINE_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_TXN_ONLINE_1M' AS FTR_NM,
       SUM(AMT_BILL) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE TXN_OL_CDE = 'E'
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_SUM_TXN_OFFLINE_1M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_SUM_TXN_OFFLINE_1M' AS FTR_NM,
       SUM(AMT_BILL) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
WHERE TXN_OL_CDE NOT IN ('E')
  AND TXN_OL_CDE IS NOT NULL
  AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_UP_SELL_LABEL3_6M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_23092023
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
           FROM CINS_TMP_CUSTOMER_23092023) )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_UP_SELL_LABEL3_6M' AS FTR_NM,
       COUNT(*) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A
WHERE PRODUCT_CDE IN ('3024',
                      '9413',
                      '9415')
  AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -36)
GROUP BY CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_LIMIT_TXN_M1
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023, CINS_TMP_DATA_RPT_CARD_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE,
       'CARD_CREDIT_LIMIT_TXN_M1' AS FTR_NM,
       ROUND(A.AMT_BILL/B.TT_CARD_LIMIT, 4) AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          SUM(AMT_BILL) AS AMT_BILL
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
   WHERE CUSTOMER_CDE IS NOT NULL
     AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
   GROUP BY CUSTOMER_CDE) A
LEFT JOIN
  (SELECT CUSTOMER_CDE,
          SUM(TT_CARD_LIMIT) AS TT_CARD_LIMIT
   FROM CINS_TMP_DATA_RPT_CARD_23092023
   GROUP BY CUSTOMER_CDE) B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;



/*
Feature Name: CARD_CREDIT_OVER_LIMIT_20_70_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023, CINS_TMP_DATA_RPT_CARD_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT A.CUSTOMER_CDE ,
       'CARD_CREDIT_OVER_LIMIT_20_70_6M' AS FTR_NM ,
       CASE
           WHEN (ROUND(A.AMT_BILL/B.TT_CARD_LIMIT, 4) >= 1.2000
                 AND ROUND(A.AMT_BILL/B.TT_CARD_LIMIT, 4) <= 1.7000) THEN 1
           ELSE 0
       END AS FTR_VAL ,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT ,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          SUM(AMT_BILL) AS AMT_BILL
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
   WHERE CUSTOMER_CDE IS NOT NULL
     AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -6) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
   GROUP BY CUSTOMER_CDE) A
LEFT JOIN
  (SELECT CUSTOMER_CDE,
          SUM(TT_CARD_LIMIT)*6 AS TT_CARD_LIMIT
   FROM CINS_TMP_DATA_RPT_CARD_23092023
   GROUP BY CUSTOMER_CDE) B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_AMOUNT_CASH_LESS_30_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          SUM(CASE
                  WHEN (TXN_OL_CDE = 'C'
                        OR TXN_OL_CDE = 'Z'
                        OR TXN_OL_CDE = 'S') THEN AMT_BILL
                  ELSE 0
              END) AS AMOUNT_CASH,
          SUM(CASE
                  WHEN TXN_OL_CDE NOT IN ('C', 'Z', 'S') THEN AMT_BILL
                  ELSE 0
              END) AS AMOUNT_SALE
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
   WHERE ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -6) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
   GROUP BY CUSTOMER_CDE)
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_AMOUNT_CASH_LESS_30_6M' AS FTR_NM,
       CASE
           WHEN (AMOUNT_CASH + AMOUNT_SALE) > 0
                AND AMOUNT_CASH/(AMOUNT_CASH + AMOUNT_SALE) < 0.3 THEN 1
           ELSE 0
       END AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A;

/*
Feature Name: CARD_CREDIT_AMOUNT_SALE_MCC_VANG_50_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          SUM(CASE
                  WHEN (TXN_OL_CDE = 'C'
                        OR TXN_OL_CDE = 'Z'
                        OR TXN_OL_CDE = 'S') THEN AMT_BILL
                  ELSE 0
              END) AS AMOUNT_CASH,
          SUM(CASE
                  WHEN TXN_OL_CDE NOT IN ('C', 'Z', 'S')
                       AND MCC_CDE IN ('7631', '5944') THEN AMT_BILL
                  ELSE 0
              END) AS AMOUNT_SALE_MCC_VANG
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
   WHERE ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -6) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
   GROUP BY CUSTOMER_CDE)
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_AMOUNT_SALE_MCC_VANG_50_6M' AS FTR_NM,
       CASE
           WHEN (AMOUNT_CASH + AMOUNT_SALE_MCC_VANG) > 0
                AND AMOUNT_SALE_MCC_VANG/(AMOUNT_CASH + AMOUNT_SALE_MCC_VANG) < 0.5 THEN 1
           ELSE 0
       END AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A;

/*
Feature Name: CARD_CREDIT_CASH_RATIO_30_6M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_23092023, DATA_RPT_CARD_493
*/
INSERT INTO CINS_FEATURE_STORE_V2 
WITH A AS
  (SELECT CUSTOMER_CDE,
          SUM(AMT_BILL) AS AMT_BILL
   FROM
     (SELECT CUSTOMER_CDE ,
             ABS(AMT_BILL) AS AMT_BILL
      FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
      WHERE UPPER(TXN_OM_CDE) IN ('PATM',
                                  'PAUTO',
                                  'PBRCHCH',
                                  'PMTQRV',
                                  'PMTCUP',
                                  'PMTIPM',
                                  'PMTBII')
        AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -6) <= PROCESS_DT
        AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
        AND SUBSTR(CARD_CDE, 1, 1) = '3'
        AND CUSTOMER_CDE IN
          (SELECT CUSTOMER_CDE
           FROM CINS_TMP_CUSTOMER_23092023)
        AND CUSTOMER_CDE <> '1'
        AND CUSTOMER_CDE <> '-1'
        AND CUSTOMER_CDE NOT LIKE '%#%' )
   GROUP BY CUSTOMER_CDE),
                          B AS
  (SELECT CUSTOMER_CDE ,
          SUM(TT_ORIGINAL_BALANCE) AS TT_ORIGINAL_BALANCE
   FROM DW_ANALYTICS.DATA_RPT_CARD_493
   WHERE CUSTOMER_CDE IN
       (SELECT CUSTOMER_CDE
        FROM CINS_TMP_CUSTOMER_23092023)
     AND SUBSTR(CARD_CDE, 1, 1) = '3'
     AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -6) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
     AND CUSTOMER_CDE <> '1'
     AND CUSTOMER_CDE <> '-1'
     AND CUSTOMER_CDE NOT LIKE '%#%'
   GROUP BY CUSTOMER_CDE)
SELECT A.CUSTOMER_CDE,
       'CARD_CREDIT_CASH_RATIO_30_6M' AS FTR_NM,
       CASE
           WHEN A.AMT_BILL/B.TT_ORIGINAL_BALANCE > 0.3
                AND B.TT_ORIGINAL_BALANCE <> 0 THEN 1
           ELSE 0
       END AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM A A
LEFT JOIN B B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;

/*
Feature Name: CARD_CREDIT_UP_SELL_LABEL1_6M
Derived From: DW_CARD_MASTER_DIM
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_UP_SELL_LABEL1_6M' AS FTR_NM,
       FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CASE
              WHEN REASON = 'UD' THEN 1
              ELSE 0
          END AS FTR_VAL,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY UPDATE_DT DESC) RN
   FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
   WHERE STATUS_CDE = ' '
     AND PLASTIC_CDE = ' ' )
WHERE RN = 1
  AND FTR_VAL = 1;

/*
Feature Name: CARD_CREDIT_UP_SELL_LABEL2_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
WITH A AS
  (SELECT CUSTOMER_CDE,
          PROCESS_DT,
          ROUND(SUM(AMT_BILL)/COUNT(*), 2) AS TICKET_SIZE
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
   WHERE ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -3) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')
   GROUP BY CUSTOMER_CDE,
            PROCESS_DT)
SELECT B.CUSTOMER_CDE,
       'CARD_CREDIT_UP_SELL_LABEL2_6M' AS FTR_NM,
       CASE
           WHEN (B.TICKET_SIZE_N0 > B.TICKET_SIZE_N1
                 AND B.TICKET_SIZE_N1 > B.TICKET_SIZE_N2)
                AND (B.TICKET_SIZE_N0 <> 0
                     AND B.TICKET_SIZE_N1 <> 0
                     AND B.TICKET_SIZE_N2 <> 0) THEN 1
           ELSE 0
       END AS FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          SUM(CASE
                  WHEN (ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1) <= PROCESS_DT
                        AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY')) THEN TICKET_SIZE
                  ELSE 0
              END) AS TICKET_SIZE_N0,
          SUM(CASE
                  WHEN (ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -2) <= PROCESS_DT
                        AND PROCESS_DT < ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -1)) THEN TICKET_SIZE
                  ELSE 0
              END) AS TICKET_SIZE_N1,
          SUM(CASE
                  WHEN (ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -3) <= PROCESS_DT
                        AND PROCESS_DT < ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -2)) THEN TICKET_SIZE
                  ELSE 0
              END) AS TICKET_SIZE_N2
   FROM A
   GROUP BY CUSTOMER_CDE) B;


/*
Feature Name: CARD_CREDIT_UP_SELL_LABEL4_6M
Derived From: CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
*/
INSERT INTO CINS_FEATURE_STORE_V2
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_UP_SELL_LABEL4_6M' AS FTR_NM,
       FTR_VAL,
       TO_DATE('23-09-2023', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CASE
              WHEN PRODUCT_CDE IN ('4200',
                                   '4201',
                                   '4203',
                                   '4204',
                                   '4205',
                                   '4207',
                                   '4208',
                                   '4210',
                                   '4213',
                                   '4214',
                                   '4215',
                                   '4216',
                                   '4217',
                                   '4218',
                                   '4219',
                                   '4220',
                                   '4221',
                                   '4224',
                                   '4230',
                                   '4231',
                                   '4232',
                                   '4233',
                                   '4234',
                                   '4235',
                                   '4238',
                                   '4240',
                                   '4243',
                                   '4244',
                                   '4245',
                                   '4246',
                                   '4247',
                                   '4248',
                                   '4249',
                                   '4250',
                                   '4251',
                                   '4252',
                                   '4253',
                                   '4254',
                                   '4255',
                                   '4256',
                                   '4269',
                                   '4271',
                                   '4272',
                                   '4277',
                                   '4278',
                                   '7203',
                                   '7204',
                                   '7205',
                                   '7206',
                                   '4600',
                                   '4601',
                                   '4603',
                                   '4604',
                                   '4605',
                                   '4607',
                                   '4608',
                                   '4609',
                                   '4610',
                                   '4611',
                                   '4612',
                                   '4613',
                                   '4614',
                                   '4615',
                                   '4616',
                                   '4617',
                                   '4618',
                                   '5400',
                                   '5401',
                                   '5403',
                                   '5404',
                                   '5405',
                                   '5407',
                                   '5408',
                                   '5409',
                                   '5412',
                                   '5413',
                                   '5414',
                                   '5415',
                                   '5416',
                                   '5417',
                                   '5418',
                                   '5419',
                                   '5420',
                                   '5421',
                                   '5422',
                                   '5423') THEN 1
              ELSE 0
          END AS FTR_VAL,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY PROCESS_DT DESC) RN
   FROM CINS_TMP_CREDIT_CARD_TRANSACTION_23092023
   WHERE PRODUCT_CDE IS NOT NULL
     AND ADD_MONTHS(TO_DATE('23-09-2023', 'DD-MM-YY'), -6) <= PROCESS_DT
     AND PROCESS_DT < TO_DATE('23-09-2023', 'DD-MM-YY') )
WHERE RN = 1
  AND FTR_VAL = 1 ;