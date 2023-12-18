/*
Feature Name: ADDR_TOWN
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM}
SELECT A.CUSTOMER_CDE,
       'ADDR_TOWN' FTR_NM,
                   A.TOWN_COUNTRY FTR_VAL,
                   TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                   CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          TOWN_COUNTRY
   FROM DW_ANALYTICS.DW_CUSTOMER_DIM
   WHERE SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND ACTIVE = '1'
     AND COMPANY_KEY = '1' ) A