/*
Feature Name: CARD_TOP1_MERCHANT_6M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_TOP1_MERCHANT_6M' FTR_NM,
                               merchant_cde FTR_VAL,
                               TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -6)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL}) ))
   WHERE rn = 1
   GROUP BY CUSTOMER_CDE,
            merchant_cde)
WHERE rn1 = 1