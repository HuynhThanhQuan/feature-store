/*
Feature Name: CASA_CT_VAR_BRANCH_REG_3M
Derived From: 
  DW_ACCOUNT_MASTER_DIM:
    - CUSTOMER_CDE
    - ACCT_ID
    - UPDATE_DT
    - OPEN_DT
    - ACTIVE
    - COMPANY_KEY
    - CATEGORY_CDE
  CINS_TMP_CUSTOMER_{RPT_DT_TBL}:
    - CUSTOMER_CDE
    - RPT_DT
    - ADD_TSTP
Tags:
  - CASA
TW: 3M
*/
INSERT INTO {TBL_NM}
with temp_01 as
(
select a.customer_cde, acct_id, sub_branch_cde ,
        row_number()over(partition by a.customer_cde,acct_id order by update_dt desc) rn
from  DW_ANALYTICS.dw_account_master_dim a
inner join CINS_TMP_CUSTOMER_{RPT_DT_TBL}  b on a.customer_cde = b.customer_cde
where 1 =1
    and open_dt < TO_DATE('{RPT_DT}','DD-MM-YY') AND open_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -3)
    and company_key = 1 
    and active = 1 
    and category_cde like '10__'
)
select customer_cde, 
    'CASA_CT_VAR_BRANCH_REG_3M' FTR_NM, 
    count(distinct sub_branch_cde) FTR_VAL, 
    TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, 
    CURRENT_TIMESTAMP ADD_TSTP
from temp_01
where rn = 1  
group by customer_cde;