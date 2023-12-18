INSERT INTO CINS_TMP_POS_MERCHANT_6M_{RPT_DT_TBL} 
SELECT 
        E.CUSTOMER_CDE, 
        F.MERCHANT_ID,  
        NULL TERMINAL_ID,
        TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, 
        CT_TXN_POS,
        CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
        SELECT 
                CUSTOMER_CDE, 
                MERCHANT_CDE,  
                CT_TXN_POS 
        FROM
        (
                SELECT 
                        CUSTOMER_CDE, 
                        MERCHANT_CDE,
                        COUNT(*) CT_TXN_POS, 
                        ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE ORDER BY COUNT(*) DESC) RN1
                FROM
                (
                        SELECT 
                                CUSTOMER_CDE, 
                                MERCHANT_CDE,
                                CARDHDR_NO,
                                APPROVAL_CDE, 
                                RETRVL_REFNO,
                                PROCESS_DT, 
                                ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE,CARDHDR_NO, APPROVAL_CDE, RETRVL_REFNO ORDER BY PROCESS_DT DESC) RN
                        FROM 
                        (
                                SELECT 
                                        CUSTOMER_CDE, 
                                        TRIM(' ' FROM(MERCHANT_CDE)) MERCHANT_CDE, 
                                        CARDHDR_NO,
                                        TRIM(' ' FROM (APPROVAL_CDE)) APPROVAL_CDE, 
                                        RETRVL_REFNO,
                                        PROCESS_DT
                                FROM 
                                        DW_ANALYTICS.DW_CARD_TRANSACTION_FCT T1
                                WHERE 
                                        PROCESS_DT < TO_DATE('{RPT_DT}','DD-MM-YY') 
                                        AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
                                        AND TRAN_STATUS = 'S' 
                                        AND EXISTS (
                                                SELECT 1 
                                                FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL} T2 
                                                WHERE T1.CUSTOMER_CDE=T2.CUSTOMER_CDE
                                        ) 
                        )    
                )
                WHERE RN = 1
                GROUP BY CUSTOMER_CDE, MERCHANT_CDE
        )
        WHERE RN1 = 1
) E
JOIN
(
        SELECT MERCHANT_ID
        FROM DW_ANALYTICS.DW_CARD_MERCHANT_DIM
) F
ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL