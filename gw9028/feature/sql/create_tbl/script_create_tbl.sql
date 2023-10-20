
-- This query creates a temporary table called CINS_TMP_CUST that contains all customer codes that meet the following criteria:
-- 1. They belong to the sub-sectors with codes '1700' or '1602'
-- 2. They are active
-- 3. They belong to company key '1'
-- 4. They have a customer status of 'HOAT DONG' on the report date specified by the user
CREATE TABLE CINS_TMP_CUST AS
SELECT A.CUSTOMER_CDE 
FROM 
        (SELECT customer_cde 
         FROM dw_analytics.dw_customer_dim
         WHERE SUB_SECTOR_CDE IN ('1700','1602') 
             AND ACTIVE = '1' 
             AND COMPANY_KEY = '1') A 
JOIN 
        (SELECT DISTINCT customer_cde 
         FROM dw_analytics.dw_cust_product_loc_fct
         WHERE CUST_STATUS = 'HOAT DONG' 
             AND PROCESS_DT = TO_DATE('{RPT_DT}','DD-MM-YYYY')) B 
ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;

 

-- This query creates a temporary table called CINS_TMP_CARD_DIM that contains all distinct card codes from the DW_CARD_MASTER_DIM table that meet the following criteria:
-- 1. They have a status code of ' '
-- 2. They have a plastic code of ' '
CREATE TABLE CINS_TMP_CARD_DIM AS 
SELECT DISTINCT CARD_CDE 
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM 
WHERE STATUS_CDE = ' ' 
    AND PLASTIC_CDE = ' ';

 

-- This query creates a temporary table called CINS_TMP_EB_MB_CROSSELL that contains the following information:
-- 1. Customer code
-- 2. Corporate ID
-- 3. Input date
-- The data is selected from the DW_ANALYTICS.DW_EB_USER table based on the following criteria:
-- 1. The customer code is in the CINS_TMP_CUST table
-- 2. The login is allowed (LOGIN_ALLOWED is not 'N')
-- 3. The record is not deleted (DEL_FLG is not 'N')
-- If there are multiple records for the same customer code and corporate ID, only the most recent record is selected.
CREATE TABLE CINS_TMP_EB_MB_CROSSELL AS 
SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT 
FROM 
    (SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CORP_ID ORDER BY REC_UPDATE_DT DESC) RN 
     FROM DW_ANALYTICS.DW_EB_USER  
     WHERE CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_CUST) 
         AND LOGIN_ALLOWED NOT IN ('N') 
         AND DEL_FLG NOT IN ('N')
    ) 
WHERE RN = 1;


-- This query creates a temporary table called CINS_TMP_CREDIT_CARD_TRANSACTION that contains credit card transaction data.
-- The data is selected from the DW_ANALYTICS.DW_CARD_TRANSACTION_FCT table based on the following criteria:
-- 1. The transaction date is within the last 36 months from the report date specified by the user
-- 2. The card code starts with '3'
-- 3. The transaction status is 'S'
-- 4. The transaction online code is a single uppercase letter
-- 5. The company key is 1
-- 6. The sub-sector code is either '1700' or '1602'
-- The table contains the following columns:
-- 1. Customer code
-- 2. Transaction date
-- 3. Approval code
-- 4. Retrieval reference number
-- 5. Billed amount
-- 6. Acquiring country code
-- 7. Merchant code
-- 8. Transaction currency code
-- 9. Billing currency code
-- 10. Product code
-- 11. Transaction online code
-- 12. MCC code
-- 13. Transaction offline code
-- 14. Fee amount

CREATE TABLE CINS_TMP_CREDIT_CARD_TRANSACTION AS 
SELECT CUSTOMER_CDE
    , PROCESS_DT
    , APPROVAL_CDE
    , RETRVL_REFNO
    , AMT_BILL
    , ACQ_CNTRY_CDE
    , MERCHANT_CDE
    , TXN_CURR_CDE
    , BILL_CURR_CDE
    , PRODUCT_CDE
    , TXN_OL_CDE
    , MCC_CDE
    , TXN_OM_CDE
    , AMT_FEE
FROM (
    SELECT A.*
        , ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE, CARD_CDE, PROCESS_DT, APPROVAL_CDE, RETRVL_REFNO ORDER BY NULL) RN
    FROM (
        SELECT T.CUSTOMER_CDE
            , T.CARD_CDE
            , T.PROCESS_DT
            , T.APPROVAL_CDE
            , T.RETRVL_REFNO
            , T.AMT_BILL
            , T.ACQ_CNTRY_CDE
            , T.MERCHANT_CDE
            , T.TXN_CURR_CDE
            , T.BILL_CURR_CDE
            , T.PRODUCT_CDE
            , T.MCC_CDE
            , T.TXN_OL_CDE
            , T.TXN_OM_CDE
            , T.AMT_FEE
        FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT T
        JOIN CINS_TMP_CUST C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
        WHERE T.PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
            AND T.PROCESS_DT <= TO_DATE('{RPT_DT}', 'DD-MM-YY')
            AND T.CARD_CDE LIKE '3%'
            AND T.TRAN_STATUS = 'S'
            AND REGEXP_LIKE(T.TXN_OL_CDE, '^[A-Z]$')
            AND T.COMPANY_KEY = 1
            AND T.SUB_SECTOR_CDE IN ('1700', '1602')
    ) A
)
WHERE RN = 1;


-- This query creates a temporary table called CINS_TMP_CUSTOMER_STATUS that contains customer status data.
-- The data is selected from the DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT table based on the following criteria:
-- 1. The process date is either the report date specified by the user or one month prior to the report date.
-- The table contains the following columns:
-- 1. Customer code
-- 2. Report date
-- 3. Customer status
-- 4. Customer status change (compared to the previous report date)
CREATE TABLE CINS_TMP_CUSTOMER_STATUS AS 
SELECT A.CUSTOMER_CDE, A.RPT_DT, A.CUST_STT,
    A.CUST_STT - LAG(A.CUST_STT) OVER (PARTITION BY A.CUSTOMER_CDE ORDER BY A.RPT_DT) CUST_STT_CHG
FROM
    (SELECT T.CUSTOMER_CDE,
        T.PROCESS_DT RPT_DT,
        MAX(CASE
            WHEN T.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T.CUST_STATUS = 'DONG BANG' THEN 0
        END) CUST_STT
    FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT T
    JOIN CINS_TMP_CUST C
        ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    WHERE T.PROCESS_DT = ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1)
        OR T.PROCESS_DT = TO_DATE('{RPT_DT}', 'DD-MM-YY')
    GROUP BY T.CUSTOMER_CDE, T.PROCESS_DT
    ) A;

-- This query creates a temporary table called CINS_TMP_DATA_RPT_CARD_{RPT_DT} that contains data on card limits for each customer's most recent card.
-- The data is selected from the DW_ANALYTICS.DATA_RPT_CARD_493 table based on the following criteria:
-- 1. The card number starts with '3'
-- 2. The process date is within the last 36 months before the report date specified by the user.
-- The table contains the following columns:
-- 1. Customer code
-- 2. Card code
-- 3. Process date
-- 4. Card limit
-- The query uses a subquery to select the most recent card limit for each customer and card combination.
-- The subquery uses the ROW_NUMBER() function to assign a row number to each record within each customer and card group, ordered by process date in descending order.
-- The outer query then selects only the records with row number 1, which correspond to the most recent card limit for each customer and card combination.

CREATE TABLE CINS_TMP_DATA_RPT_CARD_{RPT_DT} AS
SELECT CUSTOMER_CDE, CARD_CDE, PROCESS_DT, TT_CARD_LIMIT
FROM
(
    SELECT T.CUSTOMER_CDE,
        T.CARD_CDE,
        T.PROCESS_DT,
        T.TT_CARD_LIMIT,
        ROW_NUMBER() OVER (PARTITION BY T.CUSTOMER_CDE, T.CARD_CDE ORDER BY T.PROCESS_DT DESC) RN
    FROM DW_ANALYTICS.DATA_RPT_CARD_493 T
    JOIN CINS_TMP_CUST C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    JOIN CINS_TMP_CARD_DIM D ON T.CARD_CDE=D.CARD_CDE
    WHERE SUBSTR(T.CARD_CDE,1,1) = '3'
        AND T.PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
        AND T.PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
)
WHERE RN = 1;



-- This query creates a temporary table called CINS_TMP_DATA_RPT_LOAN_{RPT_DT} that contains data on the most recent loan group for each customer's most recent card.
-- The data is selected from the DW_ANALYTICS.DATA_RPT_CARD_493 table based on the following criteria:
-- 1. The card number starts with '3'
-- 2. The process date is within the last 6 months before the report date specified by the user.
-- The table contains the following columns:
-- 1. Customer code
-- 2. Most recent loan group
-- The query uses a subquery to select the most recent loan group for each customer and card combination.
-- The subquery extracts the second character of the TT_LOAN_GROUP column and converts it to an integer.
-- The outer query then selects the maximum loan group for each customer.
-- The result is stored in a temporary table with the name CINS_TMP_DATA_RPT_LOAN_{RPT_DT}.

CREATE TABLE CINS_TMP_DATA_RPT_LOAN_{RPT_DT} AS
SELECT CUSTOMER_CDE, MAX(TT_LOAN_GROUP) AS TT_LOAN_GROUP
FROM
(
    SELECT T.CUSTOMER_CDE,
    CAST(SUBSTR(T.TT_LOAN_GROUP,2,1) AS INT) TT_LOAN_GROUP
    FROM DW_ANALYTICS.DATA_RPT_CARD_493 T
    JOIN CINS_TMP_CUST C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    JOIN CINS_TMP_CARD_DIM D ON T.CARD_CDE=D.CARD_CDE
    WHERE T.COMPANY_KEY = 1
        AND SUBSTR(T.CARD_CDE,1,1) = '3'
        AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -6) <= T.PROCESS_DT 
        AND T.PROCESS_DT < TO_DATE('{RPT_DT}','DD-MM-YY')
)
GROUP BY CUSTOMER_CDE;


-- This table contains data on the total amount billed for each merchant for each customer in the last 6 months.
CREATE TABLE POS_MERCHANT_AMT_6M (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    AMT_BILL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

-- This table contains data on the number of transactions for each merchant for each customer in the last 6 months.
CREATE TABLE POS_MERCHANT_6M (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    CT_TXN_TERMINAL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

-- This table contains data on the total amount billed for each terminal for each customer in the last 6 months.
CREATE TABLE POS_TERMINAL_AMT_6M (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT VARCHAR2(25 BYTE),
    AMT_BILL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

-- This table contains data on the number of transactions for each terminal for each customer in the last 6 months.
CREATE TABLE POS_TERMINAL_6M (
    CUSTOMER_CDE VARCHAR2(25 BYTE),
    MERCHANT_CDE VARCHAR2(25 BYTE),
    TERMINAL_ID VARCHAR2(20 BYTE),
    RPT_DT DATE,
    CT_TXN_TERMINAL NUMBER,
    ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

/* This script inserts data into the POS_MERCHANT_AMT_6M table by selecting the customer code, merchant ID, report date, amount billed, and timestamp from the DW_CARD_TRANSACTION_FCT and DW_CARD_MERCHANT_DIM tables. 
The data is filtered based on certain conditions and grouped by customer code and merchant code. 
The result is then joined with the DW_CARD_MERCHANT_DIM table to get the merchant ID. 
The final result is inserted into the POS_MERCHANT_AMT_6M table.
*/

INSERT INTO POS_MERCHANT_AMT_6M 
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

-- This query inserts data into the POS_TERMINAL_AMT_6M table
INSERT INTO POS_TERMINAL_AMT_6M
SELECT E.customer_cde, F.MERCHANT_CDE, F.TERMINAL_ID, TO_CHAR(TO_DATE('{RPT_DT}', 'DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL, CURRENT_TIMESTAMP AS ADD_TSTP  
FROM (
    SELECT customer_cde, MERCHANT_CDE, TERMINAL_ID, AMT_BILL 
    FROM (
        SELECT customer_cde, merchant_cde, TERMINAL_ID, SUM(AMT_BILL) AS AMT_BILL, ROW_NUMBER() OVER (PARTITION BY customer_cde ORDER BY SUM(AMT_BILL) DESC) AS rn1
        FROM (
            SELECT customer_cde, merchant_cde, cardhdr_no, approval_cde, retrvl_refno, process_dt, AMT_BILL, ROW_NUMBER() OVER (PARTITION BY customer_cde, cardhdr_no, approval_cde, retrvl_refno ORDER BY process_dt DESC) AS rn
            FROM DW_ANALYTICS.dw_card_transaction_fct T1
            WHERE process_dt < TO_DATE('{RPT_DT}', 'DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -6) AND tran_status = 'S' AND EXISTS (SELECT 1 FROM CINS_TMP_CUST t2 WHERE t1.CUSTOMER_CDE = t2.CUSTOMER_CDE) 
        )
        WHERE rn = 1
        GROUP BY customer_cde, merchant_cde, terminal_id
    )
    WHERE rn1 = 1
) E
LEFT JOIN DW_ANALYTICS.DW_CARD_TERMINAL_DIM F ON E.MERCHANT_CDE = F.MERCHANT_CDE AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE F.MERCHANT_CDE IS NOT NULL;

/*
This script inserts data into the POS_TERMINAL_6M table by selecting customer, merchant, and terminal information from the DW_CARD_TRANSACTION_FCT and DW_CARD_TERMINAL_DIM tables. 
The data is filtered based on transaction status, customer code, and a specified date range. 
The script also calculates the count of transactions for each customer and selects the terminal with the highest transaction count. 
The resulting data is then inserted into the POS_TERMINAL_6M table along with the report date and current timestamp.
*/
INSERT INTO POS_TERMINAL_6M
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


-- This query creates a temporary table named CINS_TMP_CARD_CREDIT_LOAN_6M_{RPT_DT} that contains the customer code and a row number for each customer's credit card that was activated more than 6 months ago.
-- The temporary table is created using data from the DW_CARD_MASTER_DIM table.
-- The row number is assigned based on the activation date of the credit card, with the most recent activation date receiving the lowest row number.
-- The query filters for credit cards that start with '3', have no plastic code, no status code, and were activated at least 180 days before the specified report date.
-- The report date is specified using the placeholder {RPT_DT}.
CREATE TABLE CINS_TMP_CARD_CREDIT_LOAN_6M_{RPT_DT} AS 
SELECT 
    CUSTOMER_CDE,
    ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE ORDER BY ACTIVATION_DT DESC) RN
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
WHERE 
    SUBSTR(CARD_CDE,1,1) = '3' 
    AND PLASTIC_CDE = ' ' 
    AND STATUS_CDE = ' '
    AND TO_DATE('{RPT_DT}','DD-MM-YY') - TO_DATE(ACTIVATION_DT) >= 180
