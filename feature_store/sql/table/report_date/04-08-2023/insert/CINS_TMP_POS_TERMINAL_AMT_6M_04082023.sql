INSERT INTO CINS_TMP_POS_TERMINAL_AMT_6M_04082023
SELECT E.customer_cde,
           F.MERCHANT_CDE,
           F.TERMINAL_ID,
           TO_DATE('04-08-2023','DD-MM-YY') AS RPT_DT,
           CT_TXN_TERMINAL,
           CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
        SELECT customer_cde,
                   MERCHANT_CDE,
                   TERMINAL_ID,
                   ct_txn_terminal 
        FROM
        (
                SELECT customer_cde,
                           merchant_cde,
                           TERMINAL_ID,
                           COUNT(*) ct_txn_terminal,
                           ROW_NUMBER() OVER(PARTITION BY customer_cde ORDER BY COUNT(*) DESC) rn1
                FROM
                (
                        SELECT customer_cde,
                                   merchant_cde,
                                   cardhdr_no,
                                   TERMINAL_ID,
                                   approval_cde,
                                   retrvl_refno,
                                   process_dt, 
                                   ROW_NUMBER() OVER(PARTITION BY customer_cde,cardhdr_no, approval_cde, retrvl_refno ORDER BY process_dt DESC) rn
                        FROM 
                        (
                                SELECT customer_cde,
                                           TRIM(' ' FROM(merchant_cde)) merchant_cde,
                                           cardhdr_no,
                                           TERMINAL_ID,
                                           TRIM(' ' FROM (approval_cde)) approval_cde,
                                           retrvl_refno,
                                           process_dt
                                FROM DW_ANALYTICS.dw_card_transaction_fct T1
                                WHERE process_dt < TO_DATE('04-08-2023','DD-MM-YY') 
                                AND process_dt >= ADD_MONTHS(TO_DATE('04-08-2023','DD-MM-YY'), -6)
                                AND tran_status = 'S' 
                                AND EXISTS (SELECT 1 FROM CINS_TMP_CUSTOMER_04082023 t2 WHERE t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
                        )    
                )
                WHERE rn = 1
                GROUP BY customer_cde, merchant_cde, terminal_id
        )
        WHERE rn1 = 1
) E
LEFT JOIN
(
        SELECT MERCHANT_CDE, TERMINAL_ID, TERMINAL_TYPE 
        FROM DW_ANALYTICS.DW_CARD_TERMINAL_DIM 
        WHERE TERMINAL_TYPE = 'POS'
) F
ON E.MERCHANT_CDE = F.MERCHANT_CDE AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE F.MERCHANT_CDE IS NOT NULL