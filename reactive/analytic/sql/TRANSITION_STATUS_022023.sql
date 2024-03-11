WITH
T0 AS (
    SELECT * FROM DW_ANALYTICS.DW_CUSTOMER_DIM
),
T1 AS (
    SELECT * FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT
    WHERE PROCESS_DT = TO_DATE('31-01-2023', 'DD-MM-YY')
    OR PROCESS_DT = TO_DATE('31-12-2022', 'DD-MM-YY')
),
T2 AS (
    SELECT T0.CUSTOMER_CDE, T1.PROCESS_DT AS RPT_DT,
        MAX(CASE
            WHEN T1.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T1.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T1.CUST_STATUS = 'DONG BANG' THEN 0
        END) AS CUST_STT
    FROM T0 
    INNER JOIN T1 ON T0.CUSTOMER_CDE = T1.CUSTOMER_CDE
    GROUP BY T0.CUSTOMER_CDE, PROCESS_DT
),
T3 AS (
    SELECT CUSTOMER_CDE, 
    RPT_DT, LAG(RPT_DT) OVER (PARTITION BY CUSTOMER_CDE ORDER BY RPT_DT) AS RPT_DT_LAG, 
    CUST_STT, LAG(CUST_STT) OVER (PARTITION BY CUSTOMER_CDE ORDER BY RPT_DT) AS CUST_STT_LAG
    FROM T2
),
T4 AS (
    SELECT CUSTOMER_CDE, 
    RPT_DT, RPT_DT_LAG,
    CUST_STT, CUST_STT_LAG, 
    CUST_STT - CUST_STT_LAG AS CUST_STT_CHG
    FROM T3 
    WHERE RPT_DT = TO_DATE('31-01-2023', 'DD-MM-YY')
),
T5 AS (
    SELECT CUSTOMER_CDE, RPT_DT, RPT_DT_LAG, CUST_STT, CUST_STT_LAG, CUST_STT_CHG,
    CASE 
        WHEN CUST_STT = 2 AND CUST_STT_LAG = 2 THEN '1. STIL ACTIVE'
        WHEN CUST_STT = 2 AND CUST_STT_LAG = 1 THEN '2. UP-REACTIVE'    
        WHEN CUST_STT = 2 AND CUST_STT_LAG = 0 THEN '2. UP-REACTIVE'
        WHEN CUST_STT = 1 AND CUST_STT_LAG = 2 THEN '3. DOWN-HIBER'
        WHEN CUST_STT = 1 AND CUST_STT_LAG = 1 THEN '1. STILL HIBER'
        WHEN CUST_STT = 1 AND CUST_STT_LAG = 0 THEN 'SHOULD NOT HAPPEN'
        WHEN CUST_STT = 0 AND CUST_STT_LAG = 2 THEN 'SHOULD NOT HAPPEN'
        WHEN CUST_STT = 0 AND CUST_STT_LAG = 1 THEN '4. DOWN-HIBER-FREEZE'
        WHEN CUST_STT = 0 AND CUST_STT_LAG = 0 THEN '1. STILL FREEZE'
        ELSE 'UNCLASSIFIED'
    END AS CUST_STATUS_CHG
    FROM T4
    WHERE CUST_STT IS NOT NULL AND CUST_STT_LAG IS NOT NULL
),
T6 AS (
    SELECT CUST_STATUS_CHG, COUNT(DISTINCT CUSTOMER_CDE) AS NUM_CUST
    FROM T5
    GROUP BY CUST_STATUS_CHG
    ORDER BY CUST_STATUS_CHG
)
SELECT * 
FROM T6;