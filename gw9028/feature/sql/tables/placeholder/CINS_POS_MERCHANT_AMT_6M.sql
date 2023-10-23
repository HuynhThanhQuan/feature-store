-- This table contains data on the total amount billed for each merchant for each customer in the last 6 months.
CREATE TABLE CINS_POS_MERCHANT_AMT_6M_{RPT_DT_TBL} (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    AMT_BILL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);


/* This script inserts data into the CINS_POS_MERCHANT_AMT_6M_{RPT_DT_TBL} table by selecting the customer code, merchant ID, report date, amount billed, and timestamp from the DW_CARD_TRANSACTION_FCT and DW_CARD_MERCHANT_DIM tables. 
The data is filtered based on certain conditions and grouped by customer code and merchant code. 
The result is then joined with the DW_CARD_MERCHANT_DIM table to get the merchant ID. 
The final result is inserted into the CINS_POS_MERCHANT_AMT_6M_{RPT_DT_TBL} table.
*/

INSERT INTO CINS_POS_MERCHANT_AMT_6M_{RPT_DT_TBL} 
SELECT E.customer_cde, F.MERCHANT_ID, NULL AS TERMINAL_ID, TO_CHAR(TO_DATE('{RPT_DT}', 'DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL, CURRENT_TIMESTAMP AS ADD_TSTP  
FROM (
    SELECT customer_cde, MERCHANT_CDE, AMT_BILL 
    FROM (
        SELECT customer_cde, merchant_cde, SUM(AMT_BILL) AS AMT_BILL, ROW_NUMBER() OVER (PARTITION BY customer_cde ORDER BY SUM(AMT_BILL) DESC) AS rn1
        FROM (
            SELECT customer_cde, merchant_cde, cardhdr_no, approval_cde, retrvl_refno, process_dt, AMT_BILL, ROW_NUMBER() OVER (PARTITION BY customer_cde, cardhdr_no, approval_cde, retrvl_refno ORDER BY process_dt DESC) AS rn
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