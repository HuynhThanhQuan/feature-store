INSERT INTO CINS_TMP_CARD_DIM_16072023 
SELECT DISTINCT CARD_CDE 
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM 
WHERE STATUS_CDE = ' ' 
    AND PLASTIC_CDE = ' '
