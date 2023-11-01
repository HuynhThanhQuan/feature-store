INSERT INTO CINS_TMP_CUSTOMER_{RPT_DT_TBL} 
    SELECT A.CUSTOMER_CDE FROM (
        SELECT customer_cde 
        FROM dw_analytics.dw_customer_dim
        WHERE SUB_SECTOR_CDE IN ('1700','1602') AND ACTIVE = '1' AND COMPANY_KEY = '1') A 
    JOIN (
        select distinct customer_cde 
        from dw_analytics.dw_cust_product_loc_fct
        WHERE CUST_STATUS = 'HOAT DONG' AND PROCESS_DT = TO_DATE('{RPT_DT}','DD-MM-YYYY')) B 
    ON A.CUSTOMER_CDE = B.CUSTOMER_CDE