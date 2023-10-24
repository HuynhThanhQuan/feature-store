-- This query creates a temporary table called CINS_TMP_CUST that contains all customer codes that meet the following criteria:
-- 1. They belong to the sub-sectors with codes '1700' or '1602'
-- 2. They are active
-- 3. They belong to company key '1'
-- 4. They have a customer status of 'HOAT DONG' on the report date specified by the user
CREATE TABLE CINS_CUSTOMER_12062023 AS
    SELECT A.CUSTOMER_CDE FROM (
        SELECT customer_cde 
        FROM dw_analytics.dw_customer_dim
        WHERE SUB_SECTOR_CDE IN ('1700','1602') AND ACTIVE = '1' AND COMPANY_KEY = '1') A 
    JOIN (
        select distinct customer_cde 
        from dw_analytics.dw_cust_product_loc_fct
        WHERE CUST_STATUS = 'HOAT DONG' AND PROCESS_DT = TO_DATE('12-06-2023','DD-MM-YYYY')) B 
    ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;