/*
Feature Name: CARD_CREDIT_CT_CARD_ACTIVE
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
SELECT CUSTOMER_CDE,
       'CARD_CREDIT_CT_CARD_ACTIVE' FTR_NM,
                                    COUNT(DISTINCT CARD_CDE) FTR_VAL,
                                    TO_DATE('{RPT_DT}', 'DD-MM-YY') AS RPT_DT,
                                    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM DIM
WHERE ACTIVE = 1
  AND CARD_CDE LIKE '3%'
  AND CUSTOMER_CDE IN
    (SELECT CUSTOMER_CDE
     FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL})
  AND EXISTS
    (SELECT CUSTOMER_CDE,
            CARD_CDE
     FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT FCT
     WHERE DIM.CUSTOMER_CDE = FCT.CUSTOMER_CDE
       AND DIM.CARD_CDE = FCT.CARD_CDE
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
       AND PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
       AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -12))
GROUP BY CUSTOMER_CDE