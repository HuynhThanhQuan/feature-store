/*
Table Name: CINS_2M_PART
Derived From: 
    DW_ANALYTICS.DWD_TOI_FCT: 
        - CUSTOMER_CDE
        - NII_DEPOSIT_MTH 
        - NII_LOAN_MTH 
        - NII_FEE_MTH 
        - NII_FX_MTH
        - PROCESS_DT
    DW_CUSTOMER_DIM:
        - ACTIVE
        - CUSTOMER_CDE
        - COMPANY_KEY
        - SUB_SECTOR_CDE
*/
INSERT INTO CINS_2M_PART
SELECT 'CARDS' PRODUCT, A.CUSTOMER_CDE 
FROM DW_ANALYTICS.DWA_CARD_TOI_FCT A
JOIN (
    SELECT CUSTOMER_CDE 
    FROM DW_ANALYTICS.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1 
    AND COMPANY_KEY = 1 
    AND SUB_SECTOR_CDE IN ('1700', '1602')
) B 
ON A.CUSTOMER_CDE=B.CUSTOMER_CDE
WHERE TRUNC(A.PROCESS_DT) IN (
    '31-JAN-2022',
    '28-FEB-2022',
    '31-MAR-2022',
    '30-APR-2022',
    '31-MAY-2022',
    '30-JUN-2022',
    '31-JUL-2022',
    '31-AUG-2022',
    '30-SEP-2022',
    '31-OCT-2022',
    '30-NOV-2022',
    '31-DEC-2022'
)
GROUP BY A.CUSTOMER_CDE
HAVING SUM(A.NII_CARD_MTD)>=300000


COMMIT;


INSERT INTO CINS_2M_PART
SELECT 'OTHER' PRODUCT, A.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE, PROCESS_DT, (NII_DEPOSIT_MTH + NII_LOAN_MTH + NII_FEE_MTH + NII_FX_MTH) NII_CARD_MTD 
    FROM DW_ANALYTICS.DWD_TOI_FCT
) A
JOIN (
    SELECT CUSTOMER_CDE 
    FROM DW_ANALYTICS.DW_CUSTOMER_DIM
    WHERE ACTIVE = 1 
    AND COMPANY_KEY = 1 
    AND SUB_SECTOR_CDE IN ('1700', '1602')
) B 
ON A.CUSTOMER_CDE=B.CUSTOMER_CDE
WHERE TRUNC(A.PROCESS_DT) IN (
    '31-JAN-2022',
    '28-FEB-2022',
    '31-MAR-2022',
    '30-APR-2022',
    '31-MAY-2022',
    '30-JUN-2022',
    '31-JUL-2022',
    '31-AUG-2022',
    '30-SEP-2022',
    '31-OCT-2022',
    '30-NOV-2022',
    '31-DEC-2022'
)
GROUP BY A.CUSTOMER_CDE
HAVING SUM(A.NII_CARD_MTD)>=300000