-- This query creates a temporary table called CINS_TMP_CARD_DIM that contains all distinct card codes from the DW_CARD_MASTER_DIM table that meet the following criteria:
-- 1. They have a status code of ' '
-- 2. They have a plastic code of ' '
INSERT INTO CINS_TMP_CARD_DIM_{RPT_DT_TBL} 
SELECT DISTINCT CARD_CDE 
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM 
WHERE STATUS_CDE = ' ' AND PLASTIC_CDE = ' '

