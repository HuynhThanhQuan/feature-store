/*
Feature Name: CARD_FAV_BRANCH_LOC_6M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}

with temp_02 as 
(
 select a.customer_cde, trim(' ' from(sub_branch_cde)) sub_branch_cde,cardhdr_no, 
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,
        process_dt
from DW_ANALYTICS.dw_card_transaction_fct a
inner join CINS_TMP_CUSTOMER_{RPT_DT_TBL}  b on a.CUSTOMER_CDE = b.CUSTOMER_CDE
where 1 =1 
    and process_dt < TO_DATE('{RPT_DT}','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
    and tran_status = 'S' 
)
, temp_03 as
(
select customer_cde, sub_branch_cde,cardhdr_no, 
        approval_cde, retrvl_refno,
        process_dt, 
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn 
from  temp_02 
)
,temp_04 as
(
select customer_cde, sub_branch_cde, 
       count(*) ct_txn_sub_branch,
       row_number()over(partition by customer_cde order by count(*) desc) rn1
from temp_03
where rn =1
group by customer_cde, sub_branch_cde
)
select customer_cde,
    'CARD_FAV_BRANCH_LOC_6M' FTR_NM ,
    sub_branch_cde FTR_VAL, TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP 
from temp_04
where rn1 = 1;