/*
Feature Name: CASA_CROSS_SELL_LABEL3
Derived From: DW_ACCOUNT_MASTER_DIM
*/
INSERT INTO {TBL_NM} 
WITH A AS
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT
   FROM
     (SELECT CUSTOMER_CDE,
             ACCT_ID,
             OPEN_DT,
             ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, ACCT_ID
                               ORDER BY UPDATE_DT DESC) RN
      FROM DW_ANALYTICS.DW_ACCOUNT_MASTER_DIM
      WHERE ACTIVE = 1
        AND COMPANY_KEY = 1
        AND SUB_SECTOR_CDE IN ('1700',
                               '1602')
        AND CATEGORY_CDE LIKE '10__'
        AND TO_CHAR(CLOSE_DT) = '01-JAN-00' )
   WHERE RN = 1 )
SELECT CUSTOMER_CDE,
       'CASA_CROSS_SELL_LABEL3' AS FTR_NM,
       TO_CHAR(OPEN_DT) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          ACCT_ID,
          OPEN_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY OPEN_DT DESC) RN
   FROM A
   WHERE OPEN_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY') )
WHERE RN = 3