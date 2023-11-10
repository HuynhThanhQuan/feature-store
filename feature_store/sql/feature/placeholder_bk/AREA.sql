/*
Feature Name: AREA, BRANCH
Derived From: DW_ORG_LOCATION_DIM, DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM} WITH CUST AS
  (SELECT CUSTOMER_CDE,
          COMPANY_BOOK
   FROM DW_ANALYTICS.DW_CUSTOMER_DIM
   WHERE ACTIVE = 1
     AND COMPANY_KEY = 1
     AND SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND CUS_OPEN_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY') )
SELECT C.CUSTOMER_CDE,
       'AREA' FTR_NM,
              O.AREA_CDE FTR_VAL,
              TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
              CURRENT_TIMESTAMP ADD_TSTP
FROM CUST C
JOIN DW_ANALYTICS.DW_ORG_LOCATION_DIM O ON C.COMPANY_BOOK = O.SUB_BRANCH_CDE
WHERE O.ACTIVE = 1
UNION ALL
SELECT C.CUSTOMER_CDE,
       'BRANCH' FTR_NM,
                O.BRANCH_CDE FTR_VAL,
                TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                CURRENT_TIMESTAMP ADD_TSTP
FROM CUST C
JOIN DW_ANALYTICS.DW_ORG_LOCATION_DIM O ON C.COMPANY_BOOK = O.SUB_BRANCH_CDE
WHERE O.ACTIVE = 1