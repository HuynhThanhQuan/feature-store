/* 
Feature Name: CASA_DAY_SINCE_LTST_TXN
Derived From: DWA_STMT_EBANK, TRANSACTION_CODE, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/ 
INSERT INTO {TBL_NM}
SELECT CUSTOMER_ID CUSTOMER_CDE,
       'CASA_DAY_SINCE_LTST_TXN' FTR_NM,
                                 TO_DATE('{RPT_DT}', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)) FTR_VAL,
                                 TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                                 CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
JOIN
  (SELECT TRANSACTION_CODE
   FROM DW_ANALYTICS.TRANSACTION_CODE
   WHERE INITIATION = 'CUSTOMER') TC ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
WHERE PRODUCT_CATEGORY LIKE '10__'
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
  AND CUSTOMER_ID IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
GROUP BY CUSTOMER_ID