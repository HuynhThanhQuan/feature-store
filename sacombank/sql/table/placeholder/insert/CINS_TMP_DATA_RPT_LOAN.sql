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

INSERT INTO CINS_TMP_DATA_RPT_LOAN_{RPT_DT_TBL}
SELECT CUSTOMER_CDE, MAX(TT_LOAN_GROUP) AS TT_LOAN_GROUP
FROM (
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
GROUP BY CUSTOMER_CDE