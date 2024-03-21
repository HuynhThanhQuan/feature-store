WITH
T1 AS (
    SELECT * FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT
    WHERE PROCESS_DT = TO_DATE('31-12-2022', 'DD-MM-YY')
    AND WHERE SD_TKTT = 1
),
T2 AS (
    SELECT T1.CUSTOMER_CDE,
        MAX(CASE
            WHEN T1.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T1.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T1.CUST_STATUS = 'DONG BANG' THEN 0
        END) AS CUST_STT
    FROM T1 
    GROUP BY T1.CUSTOMER_CDE
),
T3 AS (
    SELECT 
    CUST_STT, 
    COUNT(DISTINCT CUSTOMER_CDE) AS NUM_CUST
    FROM T2
    GROUP BY CUST_STT
)
SELECT * FROM T3
ORDER BY 1;