/*
Feature Name: CARD_CREDIT_DAY_SINCE_LTST_TXN
Derived From: DW_CARD_TRANSACTION_FCT, DW_CUSTOMER_DIM
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_DAY_SINCE_LTST_TXN' FTR_NM,
                                        TO_DATE('{RPT_DT}', 'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)) FTR_VAL,
                                        TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                                        CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT
WHERE PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
  AND CARD_CDE LIKE '3%'
  AND TRAN_STATUS = 'S'
  AND POST_DT IS NOT NULL
  AND (MCC_CDE NOT IN (0,
                       6010,
                       6011,
                       6012,
                       4829,
                       6051)
       AND MCC_CDE IS NOT NULL
       OR MCC_CDE IN (6010,
                      6011,
                      6211,
                      6012,
                      6051))
  AND ACQUIRER_REFNO NOT LIKE '% %'
  AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM DW_ANALYTICS.DW_CUSTOMER_DIM
     WHERE ACTIVE = 1
       AND COMPANY_KEY = 1
       AND SUB_SECTOR_CDE IN ('1700',
                              '1602'))
GROUP BY CUSTOMER_CDE