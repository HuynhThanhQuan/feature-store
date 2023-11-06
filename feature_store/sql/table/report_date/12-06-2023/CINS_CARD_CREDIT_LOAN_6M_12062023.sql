-- This query creates a temporary table named CINS_TMP_CARD_CREDIT_LOAN_6M_12-06-2023 that contains the customer code and a row number for each customer's credit card that was activated more than 6 months ago.
-- The temporary table is created using data from the DW_CARD_MASTER_DIM table.
-- The row number is assigned based on the activation date of the credit card, with the most recent activation date receiving the lowest row number.
-- The query filters for credit cards that start with '3', have no plastic code, no status code, and were activated at least 180 days before the specified report date.
-- The report date is specified using the placeholder 12-06-2023.
CREATE TABLE CINS_CARD_CREDIT_LOAN_6M_12062023 AS 
SELECT CUSTOMER_CDE, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE ORDER BY ACTIVATION_DT DESC) RN
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
WHERE SUBSTR(CARD_CDE,1,1) = '3' 
    AND PLASTIC_CDE = ' ' 
    AND STATUS_CDE = ' '
    AND TO_DATE('12-06-2023','DD-MM-YY') - TO_DATE(ACTIVATION_DT) >= 180