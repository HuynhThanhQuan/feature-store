/*
Feature Name: CARD_BRANCH_LOC_3M
Derived From: DW_CARD_TRANSACTION_FCT, CINS_TMP_CUSTOMER_{RPT_DT_TBL}
*/
INSERT INTO {TBL_NM}
with temp_table_01 as
(
SELECT a.customer_cde,
        a.sub_branch_cde,
        ROW_NUMBER()OVER(PARTITION BY a.CUSTOMER_CDE ORDER BY a.process_dt DESC) RN
FROM  DW_ANALYTICS.DW_CARD_TRANSACTION_FCT a
INNER JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL}  b on a.CUSTOMER_CDE = b.CUSTOMER_CDE
WHERE tran_status = 'S'
    AND process_dt < TO_DATE('{RPT_DT}','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -3)

)
SELECT  customer_cde, 
        'CARD_BRANCH_LOC_3M' FTR_NM, 
        sub_Branch_cde FTR_VAL, 
        TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP    
from temp_table_01
WHERE RN = 1