INSERT INTO CINS_TMP_POS_TERMINAL_6M_20072023 
SELECT 
        E.customer_cde,
        F.MERCHANT_CDE, 
        F.TERMINAL_ID,
        TO_CHAR(TO_DATE('20-07-2023','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, 
        AMT_BILL,
        CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
        SELECT 
                customer_cde, 
                MERCHANT_CDE, 
                TERMINAL_ID,  
                AMT_BILL 
        FROM
        (
                SELECT 
                        customer_cde, 
                        merchant_cde, 
                        TERMINAL_ID,
                        SUM(AMT_BILL) AMT_BILL, 
                        row_number()over(partition by customer_cde order by SUM(AMT_BILL) desc) rn1
                FROM
                (
                        SELECT 
                                customer_cde, 
                                merchant_cde,
                                cardhdr_no, 
                                TERMINAL_ID,
                                approval_cde, 
                                retrvl_refno,
                                process_dt, 
                                AMT_BILL,
                                row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn
                        FROM 
                        (
                                SELECT 
                                        customer_cde, 
                                        trim(' ' from(merchant_cde)) merchant_cde, 
                                        cardhdr_no, 
                                        TERMINAL_ID,
                                        trim(' ' from (approval_cde)) approval_cde, 
                                        retrvl_refno,
                                        ABS(AMT_BILL) AMT_BILL,
                                        process_dt
                                FROM 
                                        DW_ANALYTICS.dw_card_transaction_fct T1
                                WHERE 
                                        process_dt < TO_DATE('20-07-2023','DD-MM-YY') 
                                        AND process_dt >= ADD_MONTHS(TO_DATE('20-07-2023','DD-MM-YY'), -6)
                                        AND tran_status = 'S' 
                                        AND EXISTS (
                                                SELECT 1 
                                                FROM CINS_TMP_CUSTOMER_20072023 t2 
                                                WHERE t1.CUSTOMER_CDE=t2.CUSTOMER_CDE
                                        ) 
                        )    
                )
                WHERE rn = 1
                GROUP BY customer_cde, merchant_cde, terminal_id
        )
        WHERE rn1 = 1
) E
LEFT JOIN
(
        SELECT 
                MERCHANT_CDE, 
                TERMINAL_ID, 
                TERMINAL_TYPE 
        FROM 
                DW_ANALYTICS.DW_CARD_TERMINAL_DIM 
        WHERE 
                TERMINAL_TYPE = 'POS'
) F
ON 
        E.MERCHANT_CDE = F.MERCHANT_CDE 
        AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE 
        F.MERCHANT_CDE IS NOT NULL
