/*
Feature Name: EB_MB_CROSS_SELL_LABEL2
Derived From: CINS_TMP_EB_MB_CROSSELL_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'EB_MB_CROSS_SELL_LABEL2' AS FTR_NM,
       TO_CHAR(INPUT_DT) AS FTR_VAL,
       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
       CURRENT_TIMESTAMP AS ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          CORP_ID,
          INPUT_DT,
          ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE
                            ORDER BY INPUT_DT DESC) RN
   FROM CINS_TMP_EB_MB_CROSSELL_{RPT_DT_TBL}
   WHERE INPUT_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY') )
WHERE RN = 2