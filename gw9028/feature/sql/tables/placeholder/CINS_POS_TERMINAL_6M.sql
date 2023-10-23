-- This table contains data on the number of transactions for each terminal for each customer in the last 6 months.
CREATE TABLE CINS_POS_TERMINAL_6M_{RPT_DT_TBL} (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT DATE,
    CT_TXN_TERMINAL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);


/*
This script inserts data into the CINS_POS_TERMINAL_6M_{RPT_DT_TBL} table by selecting customer, merchant, and terminal information from the DW_CARD_TRANSACTION_FCT and DW_CARD_TERMINAL_DIM tables. 
The data is filtered based on transaction status, customer code, and a specified date range. 
The script also calculates the count of transactions for each customer and selects the terminal with the highest transaction count. 
The resulting data is then inserted into the CINS_POS_TERMINAL_6M_{RPT_DT_TBL} table along with the report date and current timestamp.
*/
INSERT INTO CINS_POS_TERMINAL_6M_{RPT_DT_TBL}
SELECT E.customer_cde,
       F.MERCHANT_CDE,
       F.TERMINAL_ID,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CT_TXN_TERMINAL,
       CURRENT_TIMESTAMP ADD_TSTP  
FROM (
    SELECT customer_cde,
           MERCHANT_CDE,
           TERMINAL_ID,
           ct_txn_terminal 
    FROM (
        SELECT customer_cde,
               merchant_cde,
               TERMINAL_ID,
               COUNT(*) ct_txn_terminal,
               ROW_NUMBER() OVER (PARTITION BY customer_cde ORDER BY COUNT(*) DESC) rn1
        FROM (
            SELECT customer_cde,
                   merchant_cde,
                   cardhdr_no,
                   TERMINAL_ID,
                   approval_cde,
                   retrvl_refno,
                   process_dt,
                   ROW_NUMBER() OVER (PARTITION BY customer_cde, cardhdr_no, approval_cde, retrvl_refno ORDER BY process_dt DESC) rn
            FROM DW_ANALYTICS.dw_card_transaction_fct T1
            WHERE process_dt < TO_DATE('{RPT_DT}', 'DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -6)
            AND tran_status = 'S' 
            AND EXISTS (SELECT 1 FROM CINS_TMP_CUST t2 WHERE t1.CUSTOMER_CDE = t2.CUSTOMER_CDE) 
        )
        WHERE rn = 1
        GROUP BY customer_cde, merchant_cde, terminal_id
    )
    WHERE rn1 = 1
) E
LEFT JOIN (
    SELECT MERCHANT_CDE, TERMINAL_ID, TERMINAL_TYPE 
    FROM DW_ANALYTICS.DW_CARD_TERMINAL_DIM 
    WHERE TERMINAL_TYPE = 'POS'
) F ON E.MERCHANT_CDE = F.MERCHANT_CDE AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE F.MERCHANT_CDE IS NOT NULL;