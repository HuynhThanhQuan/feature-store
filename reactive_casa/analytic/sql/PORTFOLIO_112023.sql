WITH 
T0 AS (
    SELECT * FROM CINS_REACTIVATED_CASA_CUST_STT_CHG_01112023),
T1 AS (
    SELECT * FROM CINS_REACTIVATED_CASA_SCORE_01112023),
T2 AS (
    SELECT *
        FROM (SELECT T0.*, T1.SCORE
        FROM T0 INNER JOIN T1 ON T0.CUSTOMER_CDE = T1.CUSTOMER_CDE
        WHERE REACTIVATED IS NOT NULL
        AND CUST_STT_LAG <= 1
        ORDER BY SCORE DESC)
    WHERE ROWNUM <= 10000),
T21 AS (
    SELECT * FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT
    WHERE SD_TKTT = 1 AND PROCESS_DT = TO_DATE('31-10-2023', 'DD-MM-YY')),
T3 AS (
    SELECT T21.*, T2.SCORE, T2.CUST_STT, T2.CUST_STT_LAG, T2.CUST_STT_CHG
    FROM T21 
    INNER JOIN T2 ON T21.CUSTOMER_CDE = T2.CUSTOMER_CDE),
T4 AS (
    SELECT * FROM DW_ANALYTICS.DW_ORG_LOCATION_DIM),
T5 AS (
    SELECT * FROM DW_ANALYTICS.DW_CUSTOMER_FULL_DIM),
T6 AS (
    SELECT * FROM DW_ANALYTICS.DW_DEPOSIT_FCT),
T7 AS (
    SELECT * FROM DW_ANALYTICS.DW_LOAN_FCT),
T8 AS (
    SELECT * FROM DW_ANALYTICS.DW_SBVCODE_DIM),
D1 AS (
    SELECT 
    T4.AREA_NAME, T5.SUB_BRANCH_CDE, T4.SUB_BRANCH_NAME, 
    T5.CUSTOMER_CDE, T5.FULL_NAME, T1.SCORE,
    T3.CUST_TYPE, T3.CUST_STATUS, T3.CUST_OPEN_DT,
    T3.TKTT, T3.TK, T3.VAY, T3.BH, T3.PAY, T3.IB, T3.MB, T3.SP_KHAC, T3.LASTEST_TRANS_DT
    FROM T5
    INNER JOIN T3 ON T5.CUSTOMER_CDE = T3.CUSTOMER_CDE
    INNER JOIN T4 ON T5.SUB_BRANCH_CDE = T4.SUB_BRANCH_CDE
    INNER JOIN T1 ON T5.CUSTOMER_CDE = T1.CUSTOMER_CDE),
D2 AS (
    SELECT 
    T1.CUSTOMER_CDE, SUM(ACTUAL_BAL_LCL) AS TOTAL_BAL
    FROM T1 
    LEFT JOIN T6 ON T1.CUSTOMER_CDE = T6.CUSTOMER_CDE
    INNER JOIN T8 ON T8.SBVCODE_LVL_3 = T6.SBVCODE_LVL_3
    WHERE T8.APPLICATION = 'DP' AND T6.PROCESS_DT = TO_DATE('31-10-2023', 'DD-MM-YY')
    GROUP BY T1.CUSTOMER_CDE),
D3 AS (
    SELECT 
    T1.CUSTOMER_CDE, SUM(OS_AMT_LCL) AS TOTAL_LOAN
    FROM T1 
    LEFT JOIN T7 ON T1.CUSTOMER_CDE = T7.CUSTOMER_CDE
    INNER JOIN T8 ON T8.SBVCODE_LVL_3 = T7.SBVCODE_LVL_3
    WHERE T8.APPLICATION = 'LN' AND T7.PROCESS_DT = TO_DATE('31-10-2023', 'DD-MM-YY')
    GROUP BY T1.CUSTOMER_CDE),
D4 AS (
    SELECT 
    D1.AREA_NAME, D1.SUB_BRANCH_CDE, D1.SUB_BRANCH_NAME, 
    D1.CUSTOMER_CDE, D1.FULL_NAME, D1.SCORE,
    D1.CUST_TYPE, D1.CUST_STATUS, D1.CUST_OPEN_DT,
    D1.TKTT, D1.TK, D1.VAY, D1.BH, D1.PAY, D1.IB, D1.MB, D1.SP_KHAC, D2.TOTAL_BAL, D3.TOTAL_LOAN ,D1.LASTEST_TRANS_DT
    FROM D1 
    LEFT JOIN D2 ON D1.CUSTOMER_CDE = D2.CUSTOMER_CDE
    LEFT JOIN D3 ON D1.CUSTOMER_CDE = D3.CUSTOMER_CDE)  
SELECT * 
FROM D4
;