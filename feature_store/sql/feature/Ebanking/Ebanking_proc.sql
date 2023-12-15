create or replace PROCEDURE ebanking_proc (v_date in DATE  )
as 

begin

EXECUTE IMMEDIATE 'TRUNCATE TABLE ebanking_features_store'; COMMIT; 

INSERT INTO ebanking_features_store
--bang CINS_TMP_CUST 
with 
temp_table_01 as
(
SELECT a.customer_cde,
        a.sub_branch_cde,
        ROW_NUMBER()OVER(PARTITION BY a.CUSTOMER_CDE ORDER BY a.process_dt DESC) RN
FROM  DW_ANALYTICS.DW_CARD_TRANSACTION_FCT a
INNER JOIN CINS_TMP_CUST  b on a.CUSTOMER_CDE = b.CUST_CDE
WHERE tran_status = 'S'
    AND process_dt < TO_DATE(v_date,'DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -3)
   --and trunc( process_dt) = v_date
)
SELECT  customer_cde, 
        'CARD_BRANCH_LOC_3M' FTR_NM, 
        sub_Branch_cde FTR_VAL, 
        TO_DATE(v_date,'DD-MM-YY') AS  RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP       
from temp_table_01
WHERE RN = 1
;
 

INSERT INTO ebanking_features_store
with 
temp_table_01 as
(
SELECT a.customer_cde,
        a.sub_branch_cde,
        ROW_NUMBER()OVER(PARTITION BY a.CUSTOMER_CDE ORDER BY a.process_dt DESC) RN
FROM  DW_ANALYTICS.DW_CARD_TRANSACTION_FCT a
INNER JOIN CINS_TMP_CUST  b on a.CUSTOMER_CDE = b.CUST_CDE
WHERE tran_status = 'S'
    AND process_dt < TO_DATE(v_date,'DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -6)

)
SELECT  customer_cde, 
        'CARD_BRANCH_LOC_6M' FTR_NM, 
        sub_Branch_cde FTR_VAL, 
        TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
        CURRENT_TIMESTAMP ADD_TSTP    
from temp_table_01
WHERE RN = 1
;

INSERT INTO ebanking_features_store
with temp_02 as 
(
 select a.customer_cde, trim(' ' from(sub_branch_cde)) sub_branch_cde,cardhdr_no, 
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,
        process_dt
from DW_ANALYTICS.dw_card_transaction_fct a
INNER JOIN CINS_TMP_CUST  b on a.CUSTOMER_CDE = b.CUST_CDE
where 1 =1 
    and process_dt < TO_DATE(v_date,'DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -6)
    and tran_status = 'S' 
)
, temp_03 as
(
select customer_cde, sub_branch_cde,cardhdr_no, 
        approval_cde, retrvl_refno,
        process_dt, 
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn 
from  temp_02 
)
,temp_04 as
(
select customer_cde, sub_branch_cde, 
       count(*) ct_txn_sub_branch,
       row_number()over(partition by customer_cde order by count(*) desc) rn1
from temp_03
where rn =1
group by customer_cde, sub_branch_cde
)
select customer_cde,
    'CARD_FAV_BRANCH_LOC_6M' FTR_NM ,
    sub_branch_cde FTR_VAL, 
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP 
from temp_04
where rn1 = 1;



INSERT INTO ebanking_features_store
with temp_01 as 
(
select a.customer_cde, sub_branch_cde, process_dt 
from DW_ANALYTICS.DW_CARD_TRANSACTION_FCT a
inner join  CINS_TMP_CUST  b  on a.CUSTOMER_CDE = b.CUST_CDE
where tran_status = 'S' 
)

select customer_cde,
    'CARD_CT_VAR_BRANCH_3M' FTR_NM,
    count(distinct sub_branch_cde) FTR_VAL, 
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP 
from temp_01
where 1 =1
    and process_dt < TO_DATE(v_date,'DD-MM-YY')
    AND process_dt >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -3)
group by customer_cde
;

/*
Feature Name: CASA_CT_VAR_BRANCH_REG_3M
Derived From: 
  DW_ANALYTICS.dw_account_master_dim: 
    - customer_cde
    - acct_id
    - sub_branch_cde
    - update_dt
    - company_key
    - active
    - category_cde
    
Tags: 
    - GEOGRAPHIC, CASA
TW: 3M
*/

INSERT INTO ebanking_features_store
with temp_01 as
(
select a.customer_cde, acct_id, sub_branch_cde ,
        row_number()over(partition by a.customer_cde,acct_id order by update_dt desc) rn
from  DW_ANALYTICS.dw_account_master_dim a
inner join CINS_TMP_CUST  b on a.customer_cde = b.CUST_CDE
where 1 =1
    and open_dt < TO_DATE(v_date,'DD-MM-YY') AND open_dt >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -3)
    and company_key = 1 
    and active = 1 
    and category_cde like '10__'
)
select customer_cde, 
    'CASA_CT_VAR_BRANCH_REG_3M' FTR_NM, 
    count(distinct sub_branch_cde) FTR_VAL, 
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT, 
    CURRENT_TIMESTAMP ADD_TSTP
from temp_01
where rn = 1  
group by customer_cde;


--  EB_SACOMPAY_CT_INACTIVE ( so lan tk sacompay ngu dong)

/*
Feature Name: EB_SACOMPAY_CT_INACTIVE
Derived From: 
  DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT: 
    - customer_cde
    - sub_branch_cde
    - process_dt
Tags: 
    - BEHAVIORAL, EBANKING
TW: 6M
*/

INSERT INTO ebanking_features_store
with temp_02 AS
(
SELECT a.CUSTOMER_CDE,
      NVL(MONTHS_BETWEEN(LEAD(TXN_DT) OVER (PARTITION BY a.CUSTOMER_CDE ORDER BY TXN_DT), TXN_DT), MONTHS_BETWEEN(TO_DATE(v_date,'DD-MM-YY'), TXN_DT)) TXN_GAP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT a
INNER JOIN CINS_TMP_CUST  c on a.CUSTOMER_CDE = c.CUST_CDE
WHERE 1= 1
    AND TXN_DT < TO_DATE(v_date,'DD-MM-YY')
    AND TXN_STATUS = 'S'
    
UNION ALL

SELECT b.CUSTOMER_CDE,
        MONTHS_BETWEEN(MIN(TXN_DT),MAX(IDENTIFY_DT)) TXN_GAP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT A 
INNER JOIN DW_ANALYTICS.DW_EWALL_USER_DIM B ON A.CUSTOMER_CDE = b.CUSTOMER_CDE
INNER JOIN CINS_TMP_CUST c on a.CUSTOMER_CDE = c.CUST_CDE
WHERE 1 =1
    AND TXN_DT < TO_DATE(v_date,'DD-MM-YY')
    AND TXN_STATUS = 'S'
    AND TXN_DT > IDENTIFY_DT
GROUP BY b.CUSTOMER_CDE
)
SELECT CUSTOMER_CDE,
    'EB_SACOMPAY_CT_INACTIVE' FTR_NM,
    COUNT(*) FTR_VAL,
	TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM temp_02
WHERE TXN_GAP > 12 AND TXN_GAP <= 36
GROUP BY CUSTOMER_CDE
;



---EB_MBIB_DAY_SINCE_ACTIVE

/*
Feature Name: EB_MBIB_DAY_SINCE_ACTIVE
Derived From: 
  DW_ANALYTICS.DW_EB_USER: 
    - customer_cde
    - ACTIVATE_DATE
Tags: 
    - BEHAVIORAL, EBANKING

*/
INSERT INTO ebanking_features_store
with temp_01 as
(
select t1.CUSTOMER_CDE,
    min(ACTIVATE_DT) ACTIVATE_DATE
from DW_ANALYTICS.DW_EB_USER t1
inner join CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE=t2.CUST_CDE
--where ACTIVATE_DT = TO_DATE('01/01/2400','DD/MM/YYYY')
group by t1.customer_cde
) 
SELECT CUSTOMER_CDE, 
	'EB_MBIB_DAY_SINCE_ACTIVE' FTR_NM, 
	round (sysdate - to_date( ACTIVATE_DATE,'DD-MM-YY'),0) FTR_VAL, 
	TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
	CURRENT_TIMESTAMP ADD_TSTP
FROM temp_01
; 


----EB_MBIB_DAY_SINCE_LTST_TXN
INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE, 
    'EB_MBIB_DAY_SINCE_LTST_TXN' FTR_NM,
    TO_DATE(v_date,'DD-MM-YY') - NVL( MAX(TXN_DT), ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -36)) FTR_VAL,-- chua hieu logic NVL
	TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 =1
    AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -36)
     AND TXN_DT < TO_DATE(v_date,'DD-MM-YY')
    AND TXN_STATUS = 'SUC'
GROUP BY t1.CUSTOMER_CDE;


---EB_SACOMPAY_DAY_SINCE_LTST_LOGIN
INSERT INTO ebanking_features_store
WITH temp_01 AS
(
SELECT t1.CUSTOMER_CDE,
    LAST_SIGNED_ON,
    ROW_NUMBER() OVER (PARTITION BY t2.CUST_CDE ORDER BY LAST_SIGNED_ON DESC) RN
FROM DW_ANALYTICS.DW_EWALL_USER_DIM t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE USER_STATUS = 'A'
)

SELECT CUSTOMER_CDE,
    'EB_SACOMPAY_DAY_SINCE_LTST_LOGIN' AS FTR_NM,
    AVG(TO_DATE(v_date,'DD-MM-YY') - TO_DATE(LAST_SIGNED_ON)) AS FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP AS ADD_TSTP
FROM temp_01
WHERE TO_DATE(LAST_SIGNED_ON) < TO_DATE(v_date,'DD-MM-YY')
    AND TO_DATE(LAST_SIGNED_ON) >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -36)
    AND RN = 1
GROUP BY CUSTOMER_CDE;

----- EB_SACOMPAY_DAY_SINCE_LTST_TXN

INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE,
    'EB_SACOMPAY_DAY_SINCE_LTST_TXN' FTR_NM,
    TO_DATE(v_date,'DD-MM-YY') - NVL(MAX(PROCESS_DT), ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -36)) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 =1
    AND PROCESS_DT < TO_DATE(v_date,'DD-MM-YY')
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -36)
    AND TXN_STATUS = 'S'
GROUP BY t1.CUSTOMER_CDE;

--EB_CT_TXN_3M
INSERT INTO ebanking_features_store
with temp_01 as
(
select t1.customer_cde,
        txn_id,
        txn_dt 
from DW_ANALYTICS.dw_eb_transaction_fct t1
inner join CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
where TXN_ENTRY_STATUS = 'SUC' --- mo ta chi noi la giao dich, ko noi la chi lay giao dich thanh cong ==> check lai
)
SELECT customer_cde,
    'EB_CT_TXN_3M' FTR_NM ,
    count(DISTINCT txn_id) FTR_VAL, 
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP 
FROM temp_01
where TXN_DT < TO_DATE(v_date,'DD-MM-YY') AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -3)
group by customer_cde;

---EB_MBIB_CT_TXN_1M
INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE,
    'EB_MBIB_CT_TXN_1M' FTR_NM,
    COUNT(TXN_ID) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 = 1 
    AND TXN_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -1)
    AND TXN_STATUS = 'SUC'
GROUP BY t1.CUSTOMER_CDE
;

---EB_MBIB_CT_TXN_3M
INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE,
    'EB_MBIB_CT_TXN_3M' FTR_NM,
    COUNT(TXN_ID) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 = 1 
    AND TXN_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -3)
    AND TXN_STATUS = 'SUC'
GROUP BY t1.CUSTOMER_CDE
;

---EB_MBIB_CT_TXN_6M
INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE,
    'EB_MBIB_CT_TXN_6M' FTR_NM,
    COUNT(TXN_ID) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 = 1 
    AND TXN_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -6)
    AND TXN_STATUS = 'SUC'
GROUP BY t1.CUSTOMER_CDE
;

------EB_SACOMPAY_CT_TXN_1M
INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE,
    'EB_SACOMPAY_CT_TXN_1M' FTR_NM,
    COUNT(DISTINCT TXN_ID) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 = 1
    and PROCESS_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -1)
    AND TXN_STATUS = 'S'
GROUP BY t1.CUSTOMER_CDE
;


------EB_SACOMPAY_CT_TXN_3M
INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE,
    'EB_SACOMPAY_CT_TXN_3M' FTR_NM,
    COUNT(DISTINCT TXN_ID) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 = 1
    AND PROCESS_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -3)
    AND TXN_STATUS = 'S'
GROUP BY t1.CUSTOMER_CDE
;


------EB_SACOMPAY_CT_TXN_6M
INSERT INTO ebanking_features_store
SELECT t1.CUSTOMER_CDE,
    'EB_SACOMPAY_CT_TXN_6M' FTR_NM,
    COUNT(DISTINCT TXN_ID) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 = 1
    AND PROCESS_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -6)
    AND TXN_STATUS = 'S'
GROUP BY t1.CUSTOMER_CDE
;

---EB_MBIB_SUM_TXN_AMT_1M
INSERT INTO ebanking_features_store
with temp_01 as 
(
SELECT DISTINCT TXN_ID, CUSTOMER_CDE, TXN_DT, AMT_ENTRY_LCL 
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE  TXN_DT < TO_DATE(v_date,'DD-MM-YY') AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -1)
        AND TXN_STATUS = 'SUC'
)

SELECT CUSTOMER_CDE,
    'EB_MBIB_SUM_TXN_AMT_1M' FTR_NM,
    NVL(SUM(ABS(AMT_ENTRY_LCL)), 0) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM temp_01
GROUP BY CUSTOMER_CDE
;
---EB_MBIB_SUM_TXN_AMT_3M
INSERT INTO ebanking_features_store
with temp_01 as 
(
SELECT DISTINCT TXN_ID, CUSTOMER_CDE, TXN_DT, AMT_ENTRY_LCL 
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE  TXN_DT < TO_DATE(v_date,'DD-MM-YY') AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -3)
        AND TXN_STATUS = 'SUC'
)

SELECT CUSTOMER_CDE,
    'EB_MBIB_SUM_TXN_AMT_3M' FTR_NM,
    NVL(SUM(ABS(AMT_ENTRY_LCL)), 0) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM temp_01
GROUP BY CUSTOMER_CDE
;
---EB_MBIB_SUM_TXN_AMT_6M
INSERT INTO ebanking_features_store
with temp_01 as 
(
SELECT DISTINCT TXN_ID, CUSTOMER_CDE, TXN_DT, AMT_ENTRY_LCL 
FROM DW_ANALYTICS.DW_EB_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE  TXN_DT < TO_DATE(v_date,'DD-MM-YY') AND TXN_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -6)
        AND TXN_STATUS = 'SUC'
)

SELECT CUSTOMER_CDE,
    'EB_MBIB_SUM_TXN_AMT_6M' FTR_NM,
    NVL(SUM(ABS(AMT_ENTRY_LCL)), 0) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM temp_01
GROUP BY CUSTOMER_CDE
;
----EB_SACOMPAY_SUM_TXN_AMT_1M
INSERT INTO ebanking_features_store
with temp_01 as 
(
SELECT DISTINCT TXN_ID, t1.CUSTOMER_CDE, PROCESS_DT, ABS(TO_AMT) TO_AMT
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 =1
    AND PROCESS_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -1)
    AND TXN_STATUS = 'S'
)
SELECT CUSTOMER_CDE,
    'EB_SACOMPAY_SUM_TXN_AMT_1M' FTR_NM,
    NVL(SUM((TO_AMT)), 0) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP 
FROM temp_01
GROUP BY CUSTOMER_CDE
;


----EB_SACOMPAY_SUM_TXN_AMT_3M
INSERT INTO ebanking_features_store
WITH temp_01 AS (
    SELECT DISTINCT
        txn_id,
        t1.customer_cde,
        process_dt,
        abs(to_amt) to_amt
    FROM
        dw_analytics.dw_ewall_transaction_fct t1
    INNER JOIN CINS_TMP_CUST  t2 ON t1.customer_cde = t2.CUST_CDE
    WHERE
        1 = 1
        AND process_dt < TO_DATE(v_date, 'DD-MM-YY')
        AND process_dt >= add_months( TO_DATE(v_date, 'DD-MM-YY'), - 3
        )
        AND txn_status = 'S'
)
SELECT
    customer_cde,
    'EB_SACOMPAY_SUM_TXN_AMT_3M' ftr_nm,
    nvl(  SUM((to_amt)), 0 )    ftr_val,
    TO_DATE(v_date, 'DD-MM-YY')   AS rpt_dt,
    current_timestamp            add_tstp
FROM
    temp_01
GROUP BY
    customer_cde;


----EB_SACOMPAY_SUM_TXN_AMT_6M
INSERT INTO ebanking_features_store
with temp_01 as 
(
SELECT DISTINCT TXN_ID, t1.CUSTOMER_CDE, PROCESS_DT, ABS(TO_AMT) TO_AMT
FROM DW_ANALYTICS.DW_EWALL_TRANSACTION_FCT t1
INNER JOIN CINS_TMP_CUST  t2 on t1.CUSTOMER_CDE = t2.CUST_CDE
WHERE 1 =1
    AND PROCESS_DT < TO_DATE(v_date,'DD-MM-YY') 
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE(v_date,'DD-MM-YY'), -6)
    AND TXN_STATUS = 'S'
)
SELECT CUSTOMER_CDE,
    'EB_SACOMPAY_SUM_TXN_AMT_6M' FTR_NM,
    NVL(SUM((TO_AMT)), 0) FTR_VAL,
    TO_DATE(v_date,'DD-MM-YY') AS RPT_DT,
    CURRENT_TIMESTAMP ADD_TSTP
FROM temp_01
GROUP BY CUSTOMER_CDE
;

commit;
end;