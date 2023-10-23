-- This table contains data on the total amount billed for each terminal for each customer in the last 6 months.
CREATE TABLE CINS_POS_TERMINAL_AMT_6M_20231001 (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    AMT_BILL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

-- This query inserts data into the CINS_POS_TERMINAL_AMT_6M_20231001 table
INSERT INTO CINS_POS_TERMINAL_AMT_6M_20231001
SELECT E.customer_cde, F.MERCHANT_CDE, F.TERMINAL_ID, TO_CHAR(TO_DATE('2023-10-01', 'DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL, CURRENT_TIMESTAMP AS ADD_TSTP  
FROM (
    SELECT customer_cde, MERCHANT_CDE, TERMINAL_ID, AMT_BILL 
    FROM (
        SELECT customer_cde, merchant_cde, TERMINAL_ID, SUM(AMT_BILL) AS AMT_BILL, ROW_NUMBER() OVER (PARTITION BY customer_cde ORDER BY SUM(AMT_BILL) DESC) AS rn1
        FROM (
            SELECT customer_cde, merchant_cde, cardhdr_no, approval_cde, retrvl_refno, process_dt, AMT_BILL, ROW_NUMBER() OVER (PARTITION BY customer_cde, cardhdr_no, approval_cde, retrvl_refno ORDER BY process_dt DESC) AS rn
            FROM DW_ANALYTICS.dw_card_transaction_fct T1
            WHERE process_dt < TO_DATE('2023-10-01', 'DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('2023-10-01', 'DD-MM-YY'), -6) AND tran_status = 'S' AND EXISTS (SELECT 1 FROM CINS_TMP_CUST t2 WHERE t1.CUSTOMER_CDE = t2.CUSTOMER_CDE) 
        )
        WHERE rn = 1
        GROUP BY customer_cde, merchant_cde, terminal_id
    )
    WHERE rn1 = 1
) E
LEFT JOIN DW_ANALYTICS.DW_CARD_TERMINAL_DIM F ON E.MERCHANT_CDE = F.MERCHANT_CDE AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE F.MERCHANT_CDE IS NOT NULL;