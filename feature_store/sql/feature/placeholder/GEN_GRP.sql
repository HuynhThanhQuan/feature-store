/*
Feature Name: GEN_GRP
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'GEN_GRP' FTR_NM,
                 CASE
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) < 1965 THEN 'Trước Gen X'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 1965 AND 1980 THEN 'Gen X'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 1981 AND 1996 THEN 'Gen Y'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 1997 AND 2012 THEN 'Gen Z'
                     WHEN EXTRACT(YEAR
                                  FROM BIRTHDAY) BETWEEN 2013 AND 2025 THEN 'Gen A'
                 END FTR_VAL,
                 TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                 CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUS_OPEN_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')