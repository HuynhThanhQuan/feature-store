/*
Feature Name: FAV_POS_6M_SM
Derived From: CINS_TMP_POS_TERMINAL_AMT_6M_{RPT_DT_TBL}, CINS_TMP_POS_MERCHANT_AMT_6M_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'FAV_POS_6M_SM' FTR_NM,
                       MERCHANT_CDE ||'-'||terminal_id FTR_VAL,
                       TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                       CURRENT_TIMESTAMP ADD_TSTP
FROM
  (SELECT CUSTOMER_CDE,
          MERCHANT_CDE,
          TERMINAL_ID,
          RPT_DT,
          AMT_BILL,
          ADD_TSTP,
          ROW_NUMBER()OVER(PARTITION BY CUSTOMER_CDE
                           ORDER BY AMT_BILL DESC) RN1
   FROM (
           (SELECT *
            FROM CINS_TMP_POS_TERMINAL_AMT_6M_{RPT_DT_TBL}
            WHERE to_date(RPT_DT, 'DD-MM-YY') = to_date('{RPT_DT}', 'DD-MM-YY'))
         UNION  ALL
           (SELECT *
            FROM CINS_TMP_POS_MERCHANT_AMT_6M_{RPT_DT_TBL}
            WHERE to_date(RPT_DT, 'DD-MM-YY') = to_date('{RPT_DT}', 'DD-MM-YY'))))
WHERE RN1 = 1