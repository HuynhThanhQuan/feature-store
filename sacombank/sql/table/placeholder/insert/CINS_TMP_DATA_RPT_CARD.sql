INSERT INTO CINS_TMP_DATA_RPT_CARD_{RPT_DT_TBL}
SELECT CUSTOMER_CDE, CARD_CDE, PROCESS_DT, TT_CARD_LIMIT
FROM (
    SELECT T.CUSTOMER_CDE, T.CARD_CDE, T.PROCESS_DT, T.TT_CARD_LIMIT,
    ROW_NUMBER() OVER (PARTITION BY T.CUSTOMER_CDE, T.CARD_CDE ORDER BY T.PROCESS_DT DESC) RN
    FROM DW_ANALYTICS.DATA_RPT_CARD_493 T
    JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL} C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    JOIN CINS_TMP_CARD_DIM_{RPT_DT_TBL} D ON T.CARD_CDE=D.CARD_CDE
    AND SUBSTR(T.CARD_CDE,1,1) = '3'
    AND T.PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
    AND T.PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
    )
WHERE RN = 1