/*
Feature Name: LOR
Derived From: DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'LOR' FTR_NM,
             TO_DATE('{RPT_DT}', 'DD-MM-YY') - TO_DATE(CUS_OPEN_DT) FTR_VAL,
             TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
             CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.dw_customer_dim
WHERE SUB_SECTOR_CDE IN ('1700',
                         '1602')
  AND ACTIVE = '1'
  AND COMPANY_KEY = '1'
  AND TO_DATE(CUS_OPEN_DT) <= TO_DATE('{RPT_DT}', 'DD-MM-YY')