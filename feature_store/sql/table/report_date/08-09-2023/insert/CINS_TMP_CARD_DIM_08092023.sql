INSERT INTO CINS_TMP_CARD_DIM_08092023 
SELECT DISTINCT CARD_CDE 
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM 
WHERE STATUS_CDE = ' ' 
    AND PLASTIC_CDE = ' '
