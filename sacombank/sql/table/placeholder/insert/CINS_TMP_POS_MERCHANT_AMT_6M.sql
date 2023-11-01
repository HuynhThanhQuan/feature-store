INSERT INTO CINS_TMP_POS_MERCHANT_AMT_6M_{RPT_DT_TBL} 
select E.customer_cde, F.MERCHANT_ID  , NULL TERMINAL_ID,TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL,CURRENT_TIMESTAMP ADD_TSTP  
FROM (
    select customer_cde, MERCHANT_CDE,  AMT_BILL 
    FROM (
        select customer_cde, merchant_cde,
        SUM(AMT_BILL) AMT_BILL, row_number()over(partition by customer_cde order by SUM(AMT_BILL) desc) rn1
        from (
            select customer_cde, merchant_cde,cardhdr_no, approval_cde, retrvl_refno, process_dt, AMT_BILL,
            row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn
            from (
                select customer_cde, trim(' ' from(merchant_cde)) merchant_cde, cardhdr_no,
                trim(' ' from (approval_cde)) approval_cde, retrvl_refno,ABS(AMT_BILL) AMT_BILL, process_dt
                from DW_ANALYTICS.dw_card_transaction_fct T1
                where process_dt < TO_DATE('{RPT_DT}','DD-MM-YY') 
                    AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
                    and tran_status = 'S' 
                    and exists (select 1 from CINS_TMP_CUSTOMER_{RPT_DT_TBL} t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
            )
        )
    where rn = 1
    group by customer_cde, merchant_cde)
where rn1 = 1) E
JOIN
(SELECT MERCHANT_ID
FROM DW_ANALYTICS.DW_CARD_MERCHANT_DIM) F
ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL