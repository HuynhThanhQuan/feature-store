INSERT INTO CINS_TMP_DATA_RPT_CARD_08092023
SELECT CUSTOMER_CDE, CARD_CDE, PROCESS_DT, TT_CARD_LIMIT
FROM (
    SELECT T.CUSTOMER_CDE, T.CARD_CDE, T.PROCESS_DT, T.TT_CARD_LIMIT,
    ROW_NUMBER() OVER (PARTITION BY T.CUSTOMER_CDE, T.CARD_CDE ORDER BY T.PROCESS_DT DESC) RN
    FROM DW_ANALYTICS.DATA_RPT_CARD_493 T
    JOIN CINS_TMP_CUSTOMER_08092023 C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    JOIN CINS_TMP_CARD_DIM_08092023 D ON T.CARD_CDE=D.CARD_CDE
    AND SUBSTR(T.CARD_CDE,1,1) = '3'
    AND T.PROCESS_DT >= ADD_MONTHS(TO_DATE('08-09-2023', 'DD-MM-YY'), -36)
    AND T.PROCESS_DT < TO_DATE('08-09-2023', 'DD-MM-YY')
    )
WHERE RN = 1