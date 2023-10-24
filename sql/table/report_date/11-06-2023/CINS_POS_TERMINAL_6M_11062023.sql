-- This table contains data on the number of transactions for each terminal for each customer in the last 6 months.
CREATE TABLE CINS_POS_TERMINAL_6M_11062023 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT DATE,
    CT_TXN_TERMINAL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);


/*
This script inserts data into the CINS_POS_TERMINAL_6M_11062023 table by selecting customer, merchant, and terminal information from the DW_CARD_TRANSACTION_FCT and DW_CARD_TERMINAL_DIM tables. 
The data is filtered based on transaction status, customer code, and a specified date range. 
The script also calculates the count of transactions for each customer and selects the terminal with the highest transaction count. 
The resulting data is then inserted into the CINS_POS_TERMINAL_6M_11062023 table along with the report date and current timestamp.
*/
INSERT INTO CINS_POS_TERMINAL_6M_11062023
select E.customer_cde,
    F.MERCHANT_CDE, F.TERMINAL_ID
    ,TO_CHAR(TO_DATE('11-06-2023','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL ,CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
select customer_cde, MERCHANT_CDE, TERMINAL_ID ,  AMT_BILL FROM
(
select customer_cde, merchant_cde, TERMINAL_ID,
       SUM(AMT_BILL) AMT_BILL, row_number()over(partition by customer_cde order by SUM(AMT_BILL) desc) rn1
from
(
select customer_cde, merchant_cde,cardhdr_no, TERMINAL_ID,
        approval_cde, retrvl_refno,
        process_dt, AMT_BILL,
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn

from 
(
 select customer_cde, trim(' ' from(merchant_cde)) merchant_cde, cardhdr_no, TERMINAL_ID,
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,ABS(AMT_BILL) AMT_BILL,
        process_dt
        from DW_ANALYTICS.dw_card_transaction_fct T1
        where process_dt < TO_DATE('11-06-2023','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('11-06-2023','DD-MM-YY'), -6)
        and tran_status = 'S' 
        and  exists (select 1 from CINS_TMP_CUST t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
 )    
         )
where rn = 1
group by customer_cde, merchant_cde, terminal_id
             )
where rn1 = 1
) E
LEFT JOIN
(
SELECT MERCHANT_CDE, TERMINAL_ID, TERMINAL_TYPE FROM DW_ANALYTICS.DW_CARD_TERMINAL_DIM 
WHERE TERMINAL_TYPE = 'POS'
) F
ON E.MERCHANT_CDE = F.MERCHANT_CDE AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE F.MERCHANT_CDE IS NOT NULL;