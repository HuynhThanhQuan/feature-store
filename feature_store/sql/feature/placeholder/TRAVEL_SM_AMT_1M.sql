/*
Feature Name: TRAVEL_SM_AMT_1M, TRAVEL_SM_AMT_3M, TRAVEL_SM_AMT_6M
Derived From: CINS_MCC_CATEGORY, DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM}
SELECT customer_cde,
       'TRAVEL_SM_AMT_1M' FEATURE_NM,
       sum(A.AMT_BILL) FEATURE_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
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
         WHERE process_dt < TO_DATE('{RPT_DT}', 'DD-MM-YY')
           AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1)
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
GROUP BY a.CUSTOMER_CDE