/*
Feature Name: AGE
Derived From: 
  DW_ANALYTICS.DW_CUSTOMER_DIM: 
    - CUSTOMER_CDE
    - SUB_SECTOR_CDE
  DW_ANALYTICS.DW_ORG_LOCATION_DIM:
    - SUB_BRANCH_CDE
    - AREA_CDE
Tags: 
  - DEMOGRAPHIC
TW: ALL
*/

INSERT INTO {TBL_NM}
WITH 
T1 AS (
SELECT
DISTINCT T1.CUSTOMER_CDE, T2.AREA_CDE
FROM DW_ANALYTICS.DW_CUSTOMER_FULL_DIM T1
INNER JOIN DW_ANALYTICS.DW_ORG_LOCATION_DIM T2 ON T1.SUB_BRANCH_CDE = T2.SUB_BRANCH_CDE
RIGHT JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL} T3 ON T1.CUSTOMER_CDE = T3.CUSTOMER_CDE
)
SELECT 
CUSTOMER_CDE,
'AREA' AS FTR_NM, 
AREA_CDE AS FTR_VAL,
TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
CURRENT_TIMESTAMP ADD_TSTP
FROM T1
