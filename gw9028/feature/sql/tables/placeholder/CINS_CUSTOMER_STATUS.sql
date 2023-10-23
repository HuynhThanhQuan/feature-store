-- This query creates a temporary table called CINS_TMP_CUSTOMER_STATUS that contains customer status data.
-- The data is selected from the DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT table based on the following criteria:
-- 1. The process date is either the report date specified by the user or one month prior to the report date.
-- The table contains the following columns:
-- 1. Customer code
-- 2. Report date
-- 3. Customer status
-- 4. Customer status change (compared to the previous report date)
CREATE TABLE CINS_CUSTOMER_STATUS_{RPT_DT_TBL} AS 
SELECT A.CUSTOMER_CDE, A.RPT_DT, A.CUST_STT,
    A.CUST_STT - LAG(A.CUST_STT) OVER (PARTITION BY A.CUSTOMER_CDE ORDER BY A.RPT_DT) CUST_STT_CHG
FROM (
    SELECT T.CUSTOMER_CDE, T.PROCESS_DT RPT_DT,
        MAX(CASE
            WHEN T.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T.CUST_STATUS = 'DONG BANG' THEN 0
        END) CUST_STT
    FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT T
    JOIN CINS_TMP_CUST C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    WHERE T.PROCESS_DT = ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1)
        OR T.PROCESS_DT = TO_DATE('{RPT_DT}', 'DD-MM-YY')
    GROUP BY T.CUSTOMER_CDE, T.PROCESS_DT
    ) A;