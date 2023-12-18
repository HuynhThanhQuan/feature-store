/*
Feature Name: CARD_SUM_TXN_AMT_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_SUM_TXN_AMT_3M' FTR_NM,
                             SUM(ABS(AMT_BILL)) FTR_VAL,
                             TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
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
           FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
        AND tran_status = 'S'
        AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
        AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3)
      GROUP BY CUSTOMER_CDE,
               cardhdr_no,
               TRIM(' '
                    FROM (approval_cde)),
               retrvl_refno,
               amt_bill,
               PROCESS_DT))
WHERE rn = 1
GROUP BY CUSTOMER_CDE