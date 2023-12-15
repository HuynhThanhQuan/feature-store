/*
Feature Name: CARD_CREDIT_NON_DEBT_GRP_6M
Derived From:
  CINS_TMP_DATA_RPT_LOAN_{RPT_DT_TBL}:
Tags:
TW: 6M
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
'CARD_CREDIT_NON_DEBT_GRP_6M' AS FTR_NM,
CASE WHEN CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_DATA_RPT_LOAN_{RPT_DT_TBL} WHERE TT_LOAN_GROUP = 1) THEN 1
ELSE 0 END AS FTR_VAL,
TO_DATE('{RPT_DT}','DD-MM-YY') AS RPT_DT,
CURRENT_TIMESTAMP AS ADD_TSTP
FROM AA WHERE CUSTOMER_CDE IS NOT NULL
AND CUSTOMER_CDE <> '-1' AND CUSTOMER_CDE <> '1'
AND RN = 1;