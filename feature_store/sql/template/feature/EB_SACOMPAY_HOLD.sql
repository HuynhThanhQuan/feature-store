/*
Feature Name: EB_SACOMPAY_HOLD
Derived From: DW_EWALL_USER_DIM, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT DISTINCT CUSTOMER_CDE,
    'EB_SACOMPAY_HOLD' FTR_NM,
    1 AS FTR_VAL,
    TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_USER_DIM DIM
RIGHT JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL} TMP ON DIM.CUSTOMER_CDE = TMP.CUSTOMER_CDE
WHERE DIM.FIRST_SIGNED_ON < TO_DATE('{RPT_DT}', 'DD-MM-YY')