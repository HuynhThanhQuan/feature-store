/*
Feature Name: CARD_BRANCH_LOC_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_BRANCH_LOC_3M' FTR_NM,
                            sub_Branch_cde FTR_VAL,
                            TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
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
        FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
     AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
     AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3)
   GROUP BY CUSTOMER_CDE,
            sub_branch_cde)
WHERE RN = 1