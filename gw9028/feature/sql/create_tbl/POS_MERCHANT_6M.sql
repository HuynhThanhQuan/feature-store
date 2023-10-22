-- This table contains data on the number of transactions for each merchant for each customer in the last 6 months.
CREATE TABLE POS_MERCHANT_6M (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    CT_TXN_TERMINAL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

-- This query inserts data into the POS_MERCHANT_6M table
INSERT INTO POS_MERCHANT_6M
SELECT E.customer_cde, F.MERCHANT_ID, NULL AS TERMINAL_ID, TO_CHAR(TO_DATE('{RPT_DT}', 'DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, ct_txn_pos, CURRENT_TIMESTAMP AS ADD_TSTP  
FROM (
    SELECT customer_cde, MERCHANT_CDE, ct_txn_pos 
    FROM (
        SELECT customer_cde, merchant_cde, COUNT(*) AS ct_txn_pos, ROW_NUMBER() OVER (PARTITION BY customer_cde ORDER BY COUNT(*) DESC) AS rn1
        FROM (
            SELECT customer_cde, merchant_cde, cardhdr_no, approval_cde, retrvl_refno, process_dt, ROW_NUMBER() OVER (PARTITION BY customer_cde, cardhdr_no, approval_cde, retrvl_refno ORDER BY process_dt DESC) AS rn
            FROM DW_ANALYTICS.dw_card_transaction_fct T1
            WHERE process_dt < TO_DATE('{RPT_DT}', 'DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -6) AND tran_status = 'S' AND EXISTS (SELECT 1 FROM CINS_TMP_CUST t2 WHERE t1.CUSTOMER_CDE = t2.CUSTOMER_CDE) 
        )
        WHERE rn = 1
        GROUP BY customer_cde, merchant_cde
    )
    WHERE rn1 = 1
) E
JOIN DW_ANALYTICS.DW_CARD_MERCHANT_DIM F ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL;