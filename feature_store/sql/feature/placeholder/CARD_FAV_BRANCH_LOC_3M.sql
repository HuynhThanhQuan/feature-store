/*
Feature Name: CARD_FAV_BRANCH_LOC_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_FAV_BRANCH_LOC_3M' FTR_NM,
                                sub_branch_cde FTR_VAL,
                                TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
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
         WHERE PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
           AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3)
           AND tran_status = 'S'
           AND CUSTOMER_CDE IN
             (SELECT CUSTOMER_CDE
              FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL}) ))
   WHERE rn = 1
   GROUP BY CUSTOMER_CDE,
            sub_branch_cde)
WHERE rn1 = 1