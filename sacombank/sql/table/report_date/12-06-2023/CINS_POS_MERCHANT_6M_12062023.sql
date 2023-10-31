-- This table contains data on the number of transactions for each merchant for each customer in the last 6 months.
CREATE TABLE CINS_POS_MERCHANT_6M_12062023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    CT_TXN_TERMINAL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

-- This query inserts data into the POS_MERCHANT_6M table
INSERT INTO CINS_POS_MERCHANT_6M_12062023
select E.customer_cde, F.MERCHANT_ID  , NULL TERMINAL_ID,TO_CHAR(TO_DATE('12-06-2023','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, ct_txn_pos,CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
select customer_cde, MERCHANT_CDE,  ct_txn_pos FROM
(
select customer_cde, merchant_cde,
       count(*) ct_txn_pos, row_number()over(partition by customer_cde order by count(*) desc) rn1
from
(
select customer_cde, merchant_cde,cardhdr_no,
        approval_cde, retrvl_refno,
        process_dt, 
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn

from 
(
 select customer_cde, trim(' ' from(merchant_cde)) merchant_cde, cardhdr_no,
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,
        process_dt
        from DW_ANALYTICS.dw_card_transaction_fct T1
        where process_dt < TO_DATE('12-06-2023','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('12-06-2023','DD-MM-YY'), -6)
        and tran_status = 'S' 
        and  exists (select 1 from CINS_TMP_CUST t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
 )    
         )
where rn = 1
group by customer_cde, merchant_cde
             )
where rn1 = 1
) E
JOIN
(SELECT MERCHANT_ID
FROM DW_ANALYTICS.DW_CARD_MERCHANT_DIM) F
ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL;