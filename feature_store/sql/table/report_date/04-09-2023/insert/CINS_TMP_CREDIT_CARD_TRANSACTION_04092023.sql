INSERT INTO CINS_TMP_CREDIT_CARD_TRANSACTION_04092023 
SELECT 
    CUSTOMER_CDE, 
    PROCESS_DT, 
    APPROVAL_CDE, 
    RETRVL_REFNO, 
    AMT_BILL, 
    ACQ_CNTRY_CDE, 
    MERCHANT_CDE, 
    TXN_CURR_CDE, 
    BILL_CURR_CDE, 
    PRODUCT_CDE, 
    TXN_OL_CDE, 
    MCC_CDE, 
    TXN_OM_CDE, 
    AMT_FEE
FROM (
    SELECT 
        A.*,
        ROW_NUMBER() OVER (
            PARTITION BY 
                CUSTOMER_CDE, 
                CARD_CDE, 
                PROCESS_DT, 
                APPROVAL_CDE, 
                RETRVL_REFNO 
            ORDER BY NULL
        ) RN
    FROM (
        SELECT 
            T.CUSTOMER_CDE, 
            T.CARD_CDE, 
            T.PROCESS_DT, 
            T.APPROVAL_CDE, 
            T.RETRVL_REFNO, 
            T.AMT_BILL, 
            T.ACQ_CNTRY_CDE, 
            T.MERCHANT_CDE, 
            T.TXN_CURR_CDE, 
            T.BILL_CURR_CDE, 
            T.PRODUCT_CDE, 
            T.MCC_CDE, 
            T.TXN_OL_CDE, 
            T.TXN_OM_CDE, 
            T.AMT_FEE
        FROM 
            DW_ANALYTICS.DW_CARD_TRANSACTION_FCT T
            JOIN CINS_TMP_CUSTOMER_04092023 C 
                ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
        WHERE 
            T.PROCESS_DT >= ADD_MONTHS(TO_DATE('04-09-2023', 'DD-MM-YY'), -36)
            AND T.PROCESS_DT <= TO_DATE('04-09-2023', 'DD-MM-YY')
            AND T.CARD_CDE LIKE '3%'
            AND T.TRAN_STATUS = 'S'
            AND REGEXP_LIKE(T.TXN_OL_CDE, '^[A-Z]$')
            AND T.COMPANY_KEY = 1
            AND T.SUB_SECTOR_CDE IN ('1700', '1602')
    ) A
)
WHERE RN = 1