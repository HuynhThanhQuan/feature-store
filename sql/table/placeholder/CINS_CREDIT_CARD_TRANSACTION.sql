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

CREATE TABLE CINS_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL} AS 
SELECT CUSTOMER_CDE, PROCESS_DT, APPROVAL_CDE, RETRVL_REFNO, AMT_BILL, ACQ_CNTRY_CDE, 
MERCHANT_CDE, TXN_CURR_CDE, BILL_CURR_CDE, PRODUCT_CDE, TXN_OL_CDE, MCC_CDE, TXN_OM_CDE, AMT_FEE
FROM (
    SELECT A.*,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE, CARD_CDE, PROCESS_DT, APPROVAL_CDE, RETRVL_REFNO ORDER BY NULL) RN
    FROM (
        SELECT T.CUSTOMER_CDE, T.CARD_CDE, T.PROCESS_DT, T.APPROVAL_CDE, T.RETRVL_REFNO, T.AMT_BILL, T.ACQ_CNTRY_CDE, T.MERCHANT_CDE, T.TXN_CURR_CDE, T.BILL_CURR_CDE, T.PRODUCT_CDE, T.MCC_CDE, T.TXN_OL_CDE, T.TXN_OM_CDE, T.AMT_FEE
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