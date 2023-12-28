-- This query creates a temporary table called CINS_TMP_EB_MB_CROSSELL that contains the following information:
-- 1. Customer code
-- 2. Corporate ID
-- 3. Input date
-- The data is selected from the DW_ANALYTICS.DW_EB_USER table based on the following criteria:
-- 1. The customer code is in the CINS_TMP_CUST table
-- 2. The login is allowed (LOGIN_ALLOWED is not 'N')
-- 3. The record is not deleted (DEL_FLG is not 'N')
-- If there are multiple records for the same customer code and corporate ID, only the most recent record is selected.
CREATE TABLE CINS_EB_MB_CROSSELL_12062023 AS 
SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT 
FROM (
    SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CORP_ID ORDER BY REC_UPDATE_DT DESC) RN 
    FROM DW_ANALYTICS.DW_EB_USER  
    WHERE CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_CUST) 
        AND LOGIN_ALLOWED NOT IN ('N') 
        AND DEL_FLG NOT IN ('N') ) 
WHERE RN = 1;