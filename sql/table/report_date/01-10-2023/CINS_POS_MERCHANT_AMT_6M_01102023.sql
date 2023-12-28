-- This table contains data on the total amount billed for each merchant for each customer in the last 6 months.
CREATE TABLE CINS_POS_MERCHANT_AMT_6M_01102023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    AMT_BILL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);


/* This script inserts data into the CINS_POS_MERCHANT_AMT_6M_01102023 table by selecting the customer code, merchant ID, report date, amount billed, and timestamp from the DW_CARD_TRANSACTION_FCT and DW_CARD_MERCHANT_DIM tables. 
The data is filtered based on certain conditions and grouped by customer code and merchant code. 
The result is then joined with the DW_CARD_MERCHANT_DIM table to get the merchant ID. 
The final result is inserted into the CINS_POS_MERCHANT_AMT_6M_01102023 table.
*/

INSERT INTO CINS_POS_MERCHANT_AMT_6M_01102023 
select E.customer_cde, F.MERCHANT_ID  , NULL TERMINAL_ID,TO_CHAR(TO_DATE('01-10-2023','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL,CURRENT_TIMESTAMP ADD_TSTP  
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
                where process_dt < TO_DATE('01-10-2023','DD-MM-YY') 
                    AND process_dt >= ADD_MONTHS(TO_DATE('01-10-2023','DD-MM-YY'), -6)
                    and tran_status = 'S' 
                    and exists (select 1 from CINS_TMP_CUST t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
            )
        )
    where rn = 1
    group by customer_cde, merchant_cde)
where rn1 = 1) E
JOIN
(SELECT MERCHANT_ID
FROM DW_ANALYTICS.DW_CARD_MERCHANT_DIM) F
ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL;