INSERT INTO CINS_TMP_CUSTOMER_STATUS_08072023
SELECT A.CUSTOMER_CDE, A.RPT_DT, A.CUST_STT, A.CUST_STT - LAG(A.CUST_STT) OVER (PARTITION BY A.CUSTOMER_CDE ORDER BY A.RPT_DT) CUST_STT_CHG
FROM (
    SELECT T.CUSTOMER_CDE, T.PROCESS_DT RPT_DT,
        MAX(CASE
            WHEN T.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T.CUST_STATUS = 'DONG BANG' THEN 0
        END) CUST_STT
    FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT T
    JOIN CINS_TMP_CUSTOMER_08072023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    WHERE T.PROCESS_DT = ADD_MONTHS(TO_DATE('08-07-2023', 'DD-MM-YY'), -1)
        OR T.PROCESS_DT = TO_DATE('08-07-2023', 'DD-MM-YY')
    GROUP BY T.CUSTOMER_CDE, T.PROCESS_DT) A