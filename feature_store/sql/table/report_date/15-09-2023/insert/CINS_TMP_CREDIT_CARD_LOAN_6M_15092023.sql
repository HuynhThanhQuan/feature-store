INSERT INTO CINS_TMP_CREDIT_CARD_LOAN_6M_15092023
SELECT CUSTOMER_CDE, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE ORDER BY ACTIVATION_DT DESC) RN
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
WHERE SUBSTR(CARD_CDE,1,1) = '3' 
    AND PLASTIC_CDE = ' ' 
    AND STATUS_CDE = ' '
    AND TO_DATE('15-09-2023','DD-MM-YY') - TO_DATE(ACTIVATION_DT) >= 180
