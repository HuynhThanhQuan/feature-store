/*
Feature Name: CREDIT_SCORE
Derived From: STG_CRS_CUSTOMER_SCORE, DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM}
SELECT B.CUSTOMER_CDE,
       'CREDIT_SCORE' FTR_NM,
                      CASE
                          WHEN A.CREDIT_SCORE IS NULL THEN 0
                          ELSE A.CREDIT_SCORE
                      END FTR_VAL,
                      TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                      CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CREDIT_SCORE
   FROM
     (SELECT CUSTOMER_CDE,
             CREDIT_SCORE,
             DATE_1,
             PROCESS_DATE,
             ROW_NUMBER()OVER(PARTITION BY CUSTOMER_CDE
                              ORDER BY CREDIT_SCORE DESC) RN
      FROM
        (SELECT trim(' '
                     FROM(CUSTOMER_CDE))CUSTOMER_CDE,
                NVL(FINANCIALSCORE, NONFINANCIALSCORE) CREDIT_SCORE,
                RANK()OVER(PARTITION BY CUSTOMER_CDE
                           ORDER BY DATE_1 DESC)RANK_SCORE,
                      date_1,
                      PROCESS_DATE
         FROM DW_ANALYTICS.STG_CRS_CUSTOMER_SCORE)
      WHERE RANK_SCORE = 1
        AND TO_DATE(DATE_1) < TO_DATE('{RPT_DT}', 'DD-MM-YY') )
   WHERE RN = 1 ) A
RIGHT JOIN
  (SELECT CUSTOMER_CDE
   FROM DW_ANALYTICS.DW_CUSTOMER_DIM
   WHERE SUB_SECTOR_CDE IN ('1700',
                            '1602')
     AND ACTIVE = '1'
     AND COMPANY_KEY = '1') B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE