INSERT INTO CINS_TMP_POS_MERCHANT_6M_22082023 
SELECT 
        E.customer_cde, 
        F.MERCHANT_ID,  
        NULL TERMINAL_ID,
        TO_CHAR(TO_DATE('22-08-2023','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, 
        ct_txn_pos,
        CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
        SELECT 
                customer_cde, 
                MERCHANT_CDE,  
                ct_txn_pos 
        FROM
        (
                SELECT 
                        customer_cde, 
                        merchant_cde,
                        COUNT(*) ct_txn_pos, 
                        ROW_NUMBER() OVER(PARTITION BY customer_cde ORDER BY COUNT(*) DESC) rn1
                FROM
                (
                        SELECT 
                                customer_cde, 
                                merchant_cde,
                                cardhdr_no,
                                approval_cde, 
                                retrvl_refno,
                                process_dt, 
                                ROW_NUMBER() OVER(PARTITION BY customer_cde,cardhdr_no, approval_cde, retrvl_refno ORDER BY process_dt DESC) rn
                        FROM 
                        (
                                SELECT 
                                        customer_cde, 
                                        TRIM(' ' FROM(merchant_cde)) merchant_cde, 
                                        cardhdr_no,
                                        TRIM(' ' FROM (approval_cde)) approval_cde, 
                                        retrvl_refno,
                                        process_dt
                                FROM 
                                        DW_ANALYTICS.dw_card_transaction_fct T1
                                WHERE 
                                        process_dt < TO_DATE('22-08-2023','DD-MM-YY') 
                                        AND process_dt >= ADD_MONTHS(TO_DATE('22-08-2023','DD-MM-YY'), -6)
                                        AND tran_status = 'S' 
                                        AND EXISTS (
                                                SELECT 1 
                                                FROM CINS_TMP_CUSTOMER_22082023 t2 
                                                WHERE t1.CUSTOMER_CDE=t2.CUSTOMER_CDE
                                        ) 
                        )    
                )
                WHERE rn = 1
                GROUP BY customer_cde, merchant_cde
        )
        WHERE rn1 = 1
) E
JOIN
(
        SELECT MERCHANT_ID
        FROM DW_ANALYTICS.DW_CARD_MERCHANT_DIM
) F
ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL