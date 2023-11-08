/*
Feature Name: EB_SACOMPAY_DAY_SINCE_LTST_TXN
Derived From: DW_EWALL_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'EB_SACOMPAY_DAY_SINCE_LTST_TXN' FTR_NM,
                                        TO_DATE('{RPT_DT}', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)) FTR_VAL,
                                        TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                                        CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
WHERE PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
  AND TXN_STATUS = 'S'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
GROUP BY CUSTOMER_CDE