-- This query creates a temporary table called CINS_TMP_DATA_RPT_CARD_2023-10-01 that contains data on card limits for each customer's most recent card.
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

CREATE TABLE CINS_DATA_RPT_CARD_20231001 AS
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
        AND T.PROCESS_DT >= ADD_MONTHS(TO_DATE('2023-10-01', 'DD-MM-YY'), -36)
        AND T.PROCESS_DT < TO_DATE('2023-10-01', 'DD-MM-YY')
)
WHERE RN = 1;
