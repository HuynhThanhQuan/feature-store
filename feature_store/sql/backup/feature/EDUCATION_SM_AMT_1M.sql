/*
Feature Name: EDUCATION_SM_AMT_1M
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM, CINS_MCC_CATEGORY
*/

INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'EDUCATION_SM_AMT_1M' FEATURE_NM,
                             sum(A.AMT_BILL) FEATURE_VAL,
                             TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1)
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
GROUP BY a.CUSTOMER_CDE