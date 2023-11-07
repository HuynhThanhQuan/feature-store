INSERT INTO CINS_TMP_CUSTOMER_03092023 
SELECT A.CUSTOMER_CDE 
FROM (
    SELECT CUSTOMER_CDE 
    FROM DW_ANALYTICS.DW_CUSTOMER_DIM
    WHERE SUB_SECTOR_CDE IN ('1700', '1602') 
    AND ACTIVE = '1' 
    AND COMPANY_KEY = '1'
) A 
JOIN (
    SELECT DISTINCT CUSTOMER_CDE 
    FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT
    WHERE CUST_STATUS = 'HOAT DONG' 
    AND PROCESS_DT = TO_DATE('03-09-2023', 'DD-MM-YYYY')
) B ON A.CUSTOMER_CDE = B.CUSTOMER_CDE