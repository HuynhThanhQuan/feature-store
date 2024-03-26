CREATE TABLE CINS_REACTIVATED_CASA_CUSTOMER_01052023 AS
SELECT DISTINCT CUSTOMER_CDE 
FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT
WHERE SD_TKTT = 1
AND PROCESS_DT = TO_DATE('30-04-2023', 'DD-MM-YY')