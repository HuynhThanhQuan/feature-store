/*
Table Name: CINS_TMP_CARD_DIM_{RPT_DT_TBL}
Derived From: 
  DW_ANALYTICS.DW_CARD_MASTER_DIM: 
    - CARD_CDE
    - STATUS_CDE
    - PLASTIC_CDE 
*/
INSERT INTO CINS_TMP_CARD_DIM_{RPT_DT_TBL} 
SELECT DISTINCT CARD_CDE 
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM 
WHERE STATUS_CDE = ' ' 
    AND PLASTIC_CDE = ' ';