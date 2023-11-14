/*
Table Name: CINS_TMP_LST_{RPT_DT_TBL}
Derived From: 
    CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}: 
        - CUSTOMER_CDE
        - PROCESS_DT
    CINS_2M_PART:
        - PRODUCT
        - CUSTOMER_CDE
    DW_ANALYTICS.DW_EB_TRANSACTION_FCT:
        - CUSTOMER_CDE
        - TXN_DT
        - TXN_STATUS
    DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT:
        - CUSTOMER_CDE
        - PROCESS_DT
        - TXN_STATUS
    DW_ANALYTICS.DWA_STMT_EBANK:
        - CUSTOMER_ID
        - PRODUCT_CATEGORY
        - PROCESS_DT
        - TRANSACTION_CODE
    DW_ANALYTICS.TRANSACTION_CODE:
        - INITIATION
        - TRANSACTION_CODE
*/
---CREDIT
INSERT INTO CINS_TMP_LST_{RPT_DT_TBL}
SELECT 'CARD' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE,
        'CARD_CREDIT_CT_TXN_6M' FTR_NM,
        COUNT(*) FTR_VAL,
        TO_DATE('{RPT_DT}','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM CINS_TMP_CREDIT_CARD_TRANSACTION_{RPT_DT_TBL}
    WHERE PROCESS_DT < TO_DATE('{RPT_DT}','DD-MM-YY')
        AND PROCESS_DT >= TO_DATE('{RPT_DT}','DD-MM-YY') - INTERVAL '6' MONTH
    GROUP BY CUSTOMER_CDE
) E
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'CARD'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE E.FTR_VAL > 0;


COMMIT;


---IB/MB
INSERT INTO CINS_TMP_LST_{RPT_DT_TBL}
SELECT 'IBMB' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE,
        'EB_MBIB_CT_TXN_6M' FTR_NM,
        COUNT(TXN_ID) FTR_VAL,
        TO_DATE('{RPT_DT}','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT
    WHERE TXN_DT < TO_DATE('{RPT_DT}','DD-MM-YY')
        AND TXN_DT >= TO_DATE('{RPT_DT}','DD-MM-YY') - INTERVAL '6' MONTH
        AND TXN_STATUS = 'SUC'
        AND CUSTOMER_CDE IN (
            SELECT CUSTOMER_CDE 
            FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL}
        )
    GROUP BY CUSTOMER_CDE
) E 
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'OTHER'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE FTR_VAL > 0;


COMMIT;


---SACOMPAY
INSERT INTO CINS_TMP_LST_{RPT_DT_TBL}
SELECT 'SACOMPAY' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE,
        'EB_SACOMPAY_CT_TXN_6M' FTR_NM,
        COUNT(DISTINCT TXN_ID) FTR_VAL,
        TO_DATE('{RPT_DT}','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT
    WHERE PROCESS_DT < TO_DATE('{RPT_DT}','DD-MM-YY') 
        AND PROCESS_DT >= TO_DATE('{RPT_DT}','DD-MM-YY') - INTERVAL '6' MONTH
        AND TXN_STATUS = 'S'
        AND CUSTOMER_CDE IN (
            SELECT CUSTOMER_CDE 
            FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL}
        )
    GROUP BY CUSTOMER_CDE
) E 
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'OTHER'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE FTR_VAL > 0;


COMMIT;


---CASA
INSERT INTO CINS_TMP_LST_{RPT_DT_TBL}
SELECT 'CASA' PRODUCT, E.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_ID CUSTOMER_CDE,
        'CASA_CT_TXN_6M' FTR_NM,
        COUNT(STMT_ENTRY_ID) FTR_VAL,
        TO_DATE('{RPT_DT}','DD-MM-YY') RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP
    FROM DW_ANALYTICS.DWA_STMT_EBANK TXN
    JOIN (
        SELECT TRANSACTION_CODE 
        FROM DW_ANALYTICS.TRANSACTION_CODE 
        WHERE INITIATION = 'CUSTOMER'
    ) TC
    ON TXN.TRANSACTION_CODE = TC.TRANSACTION_CODE
    WHERE PRODUCT_CATEGORY LIKE '10__'
        AND PROCESS_DT < TO_DATE('{RPT_DT}','DD-MM-YY')
        AND PROCESS_DT >= TO_DATE('{RPT_DT}','DD-MM-YY') - INTERVAL '6' MONTH
        AND CUSTOMER_ID IN (
            SELECT CUSTOMER_CDE 
            FROM CINS_TMP_CUSTOMER_{RPT_DT_TBL}
        )
    GROUP BY CUSTOMER_ID
) E 
JOIN (
    SELECT CUSTOMER_CDE 
    FROM CINS_2M_PART 
    WHERE PRODUCT = 'OTHER'
) F 
ON E.CUSTOMER_CDE = F.CUSTOMER_CDE
WHERE FTR_VAL > 0;