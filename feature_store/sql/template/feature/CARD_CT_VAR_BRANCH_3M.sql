/*
Feature Name: CARD_CT_VAR_BRANCH_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
with temp_01 as 
(
select a.customer_cde, sub_branch_cde, process_dt 
from DW_ANALYTICS.DW_CARD_TRANSACTION_FCT a
inner join  CINS_TMP_CUSTOMER_{RPT_DT_TBL}  b  on a.CUSTOMER_CDE = b.CUSTOMER_CDE
where tran_status = 'S' 
)

select customer_cde,
    'CARD_CT_VAR_BRANCH_3M' FTR_NM,
    count(distinct sub_branch_cde) FTR_VAL, 
    TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP 
from temp_01
where 1 =1
    and process_dt < TO_DATE('{RPT_DT}','DD-MM-YY')
    AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -3)
group by customer_cde
;