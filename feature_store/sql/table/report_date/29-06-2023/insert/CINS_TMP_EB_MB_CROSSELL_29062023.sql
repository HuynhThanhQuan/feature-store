INSERT INTO CINS_TMP_EB_MB_CROSSELL_29062023 
SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT 
FROM (
    SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CORP_ID ORDER BY REC_UPDATE_DT DESC) RN 
    FROM DW_ANALYTICS.DW_EB_USER  
    WHERE CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_CUSTOMER_29062023) 
        AND LOGIN_ALLOWED NOT IN ('N') 
        AND DEL_FLG NOT IN ('N')
        AND PROCESS_DT = TO_DATE('29-06-2023', 'DD-MM-YY')
) 
WHERE RN = 1
