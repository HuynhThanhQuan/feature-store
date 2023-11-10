/*
Feature Name: LIFE_STG
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'LIFE_STG' FTR_NM,
                  CASE
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('{RPT_DT}', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 18 AND 26 THEN 'Bắt đầu sự nghiệp'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('{RPT_DT}', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 27 AND 35 THEN 'Lập gia đình'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('{RPT_DT}', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 36 AND 45 THEN 'Thiết lập tài sản'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('{RPT_DT}', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 46 AND 54 THEN 'Bảo vệ tài sản'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('{RPT_DT}', 'DD-MM-YY'), BIRTHDAY)/12) BETWEEN 55 AND 64 THEN 'Cuối sự nghiệp'
                      WHEN FLOOR(MONTHS_BETWEEN(TO_DATE('{RPT_DT}', 'DD-MM-YY'), BIRTHDAY)/12) >= 65 THEN 'Nghỉ hưu'
                      ELSE NULL
                  END FTR_VAL,
                  TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                  CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CUSTOMER_DIM
WHERE ACTIVE = 1
  AND COMPANY_KEY = 1
  AND SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND CUS_OPEN_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')