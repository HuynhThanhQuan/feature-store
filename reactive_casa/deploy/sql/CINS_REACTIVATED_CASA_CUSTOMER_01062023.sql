CREATE TABLE CINS_REACTIVATED_CASA_CUSTOMER_01062023 AS
SELECT DISTINCT CUSTOMER_CDE 
FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT
WHERE SD_TKTT = 1
AND PROCESS_DT = TO_DATE('31-05-2023', 'DD-MM-YY')