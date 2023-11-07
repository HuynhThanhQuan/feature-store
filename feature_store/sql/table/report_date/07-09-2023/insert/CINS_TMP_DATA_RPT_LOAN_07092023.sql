INSERT INTO CINS_TMP_DATA_RPT_LOAN_07092023
SELECT 
    CUSTOMER_CDE, 
    MAX(TT_LOAN_GROUP) AS TT_LOAN_GROUP
FROM (
    SELECT 
        T.CUSTOMER_CDE,
        CAST(SUBSTR(T.TT_LOAN_GROUP,2,1) AS INT) TT_LOAN_GROUP
    FROM 
        DW_ANALYTICS.DATA_RPT_CARD_493 T
        JOIN CINS_TMP_CUSTOMER_07092023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
        JOIN CINS_TMP_CARD_DIM_07092023 D ON T.CARD_CDE=D.CARD_CDE
    WHERE 
        T.COMPANY_KEY = 1 
        AND SUBSTR(T.CARD_CDE,1,1) = '3'
        AND ADD_MONTHS(TO_DATE('07-09-2023', 'DD-MM-YY'), -6) <= T.PROCESS_DT 
        AND T.PROCESS_DT < TO_DATE('07-09-2023','DD-MM-YY')
)
GROUP BY 
    CUSTOMER_CDE
