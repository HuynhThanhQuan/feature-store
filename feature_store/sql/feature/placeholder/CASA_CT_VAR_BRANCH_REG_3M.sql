/*
Feature Name: CASA_CT_VAR_BRANCH_REG_3M
Derived From: DW_ACCOUNT_MASTER_DIM, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CASA_CT_VAR_BRANCH_REG_3M' FTR_NM,
                                   count(DISTINCT sub_branch_cde) FTR_VAL,
                                   TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
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
        FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
     AND open_dt < TO_DATE('{RPT_DT}', 'DD-MM-YY')
     AND active = 1
     AND open_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -3)
     AND company_key = 1
     AND active = 1
     AND category_cde like '10__' )
WHERE rn = 1
GROUP BY CUSTOMER_CDE