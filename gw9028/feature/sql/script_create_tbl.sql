CREATE TABLE CINS_TMP_CUST AS
SELECT A.CUSTOMER_CDE FROM 
 (SELECT customer_cde FROM dw_analytics.dw_customer_dim
 WHERE SUB_SECTOR_CDE IN ('1700','1602') AND ACTIVE = '1' AND COMPANY_KEY = '1') A 
 JOIN (select distinct customer_cde from dw_analytics.dw_cust_product_loc_fct
 WHERE CUST_STATUS = 'HOAT DONG' AND PROCESS_DT = TO_DATE('{RPT_DT}','DD-MM-YYYY')) B 
 ON A.CUSTOMER_CDE = B.CUSTOMER_CDE;
 
 CREATE TABLE CINS_TMP_CARD_DIM AS SELECT DISTINCT CARD_CDE FROM DW_ANALYTICS.DW_CARD_MASTER_DIM WHERE STATUS_CDE = ' ' AND PLASTIC_CDE = ' ';
 
CREATE TABLE CINS_TMP_EB_MB_CROSSELL AS SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT 
FROM ( SELECT CUSTOMER_CDE, CORP_ID, INPUT_DT, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE, CORP_ID ORDER BY REC_UPDATE_DT DESC) RN FROM DW_ANALYTICS.DW_EB_USER  WHERE CUSTOMER_CDE IN (SELECT CUSTOMER_CDE FROM CINS_TMP_CUST) AND LOGIN_ALLOWED NOT IN ('N') AND DEL_FLG NOT IN ('N') ) WHERE RN = 1;

CREATE TABLE CINS_TMP_CREDIT_CARD_TRANSACTION AS SELECT CUSTOMER_CDE
    , PROCESS_DT
    , APPROVAL_CDE
    , RETRVL_REFNO
    , AMT_BILL
    , ACQ_CNTRY_CDE
    , MERCHANT_CDE
    , TXN_CURR_CDE
    , BILL_CURR_CDE
    , PRODUCT_CDE
    , TXN_OL_CDE
    , MCC_CDE
    , TXN_OM_CDE
    , AMT_FEE
FROM
    (SELECT A.*,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE, CARD_CDE, PROCESS_DT, APPROVAL_CDE, RETRVL_REFNO ORDER BY NULL) RN
    FROM
        (SELECT T.CUSTOMER_CDE
            , T.CARD_CDE
            , T.PROCESS_DT
            , T.APPROVAL_CDE
            , T.RETRVL_REFNO
            , T.AMT_BILL
            , T.ACQ_CNTRY_CDE
            , T.MERCHANT_CDE
            , T.TXN_CURR_CDE
            , T.BILL_CURR_CDE
            , T.PRODUCT_CDE
            , T.MCC_CDE
            , T.TXN_OL_CDE
            , T.TXN_OM_CDE
            , T.AMT_FEE
        FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT T
  JOIN CINS_TMP_CUST C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
        WHERE T.PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
            AND T.PROCESS_DT <= TO_DATE('{RPT_DT}', 'DD-MM-YY')
            AND T.CARD_CDE LIKE '3%'
            AND T.TRAN_STATUS = 'S'
            AND REGEXP_LIKE(T.TXN_OL_CDE, '^[A-Z]$')
   AND T.COMPANY_KEY = 1
            AND T.SUB_SECTOR_CDE IN ('1700', '1602')
        ) A
    )
WHERE RN = 1;
CREATE TABLE CINS_TMP_CUSTOMER_STATUS AS SELECT A.CUSTOMER_CDE, A.RPT_DT, A.CUST_STT,
    A.CUST_STT - LAG(A.CUST_STT) OVER (PARTITION BY A.CUSTOMER_CDE ORDER BY A.RPT_DT) CUST_STT_CHG
FROM
    (SELECT T.CUSTOMER_CDE,
        T.PROCESS_DT RPT_DT,
        MAX(CASE
            WHEN T.CUST_STATUS = 'HOAT DONG' THEN 2
            WHEN T.CUST_STATUS = 'NGU DONG' THEN 1
            WHEN T.CUST_STATUS = 'DONG BANG' THEN 0
        END) CUST_STT
    FROM DW_ANALYTICS.DW_CUST_PRODUCT_LOC_FCT T
    JOIN CINS_TMP_CUST C
        ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
    WHERE T.PROCESS_DT = ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -1)
        OR T.PROCESS_DT = TO_DATE('{RPT_DT}', 'DD-MM-YY')
    GROUP BY T.CUSTOMER_CDE, T.PROCESS_DT
    ) A;
CREATE TABLE CINS_TMP_DATA_RPT_CARD_{RPT_DT}
AS
SELECT CUSTOMER_CDE, CARD_CDE, PROCESS_DT, TT_CARD_LIMIT
FROM
(
SELECT T.CUSTOMER_CDE,
T.CARD_CDE,
T.PROCESS_DT,
T.TT_CARD_LIMIT,
ROW_NUMBER() OVER (PARTITION BY T.CUSTOMER_CDE, T.CARD_CDE ORDER BY T.PROCESS_DT DESC) RN
FROM DW_ANALYTICS.DATA_RPT_CARD_493 T
JOIN CINS_TMP_CUST C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
JOIN CINS_TMP_CARD_DIM D ON T.CARD_CDE=D.CARD_CDE
AND SUBSTR(T.CARD_CDE,1,1) = '3'
AND T.PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -36)
AND T.PROCESS_DT < TO_DATE('{RPT_DT}', 'DD-MM-YY')
)
WHERE RN = 1;
CREATE TABLE CINS_TMP_DATA_RPT_LOAN_{RPT_DT}
AS
SELECT CUSTOMER_CDE, MAX(TT_LOAN_GROUP) AS TT_LOAN_GROUP
FROM
(
SELECT T.CUSTOMER_CDE,
CAST(SUBSTR(T.TT_LOAN_GROUP,2,1) AS INT) TT_LOAN_GROUP
FROM DW_ANALYTICS.DATA_RPT_CARD_493 T
JOIN CINS_TMP_CUST C ON T.CUSTOMER_CDE=C.CUSTOMER_CDE
JOIN CINS_TMP_CARD_DIM D ON T.CARD_CDE=D.CARD_CDE
WHERE T.COMPANY_KEY = 1
AND SUBSTR(T.CARD_CDE,1,1) = '3'
AND ADD_MONTHS(TO_DATE('{RPT_DT}', 'DD-MM-YY'), -6) <= T.PROCESS_DT AND T.PROCESS_DT < TO_DATE('{RPT_DT}','DD-MM-YY')
)
GROUP BY CUSTOMER_CDE;


CREATE TABLE POS_MERCHANT_AMT_6M (
CUSTOMER_CDE VARCHAR2(25 BYTE),
MERCHANT_CDE VARCHAR2(25 BYTE),
TERMINAL_ID VARCHAR2(20 BYTE),
RPT_DT VARCHAR2(25 BYTE),
AMT_BILL NUMBER,
ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

CREATE TABLE POS_MERCHANT_6M (
CUSTOMER_CDE VARCHAR2(25 BYTE),
MERCHANT_CDE VARCHAR2(25 BYTE),
TERMINAL_ID VARCHAR2(20 BYTE),
RPT_DT VARCHAR2(25 BYTE),
CT_TXN_TERMINAL NUMBER,
ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);
CREATE TABLE POS_TERMINAL_AMT_6M (
CUSTOMER_CDE VARCHAR2(25 BYTE),
MERCHANT_CDE VARCHAR2(25 BYTE),
TERMINAL_ID VARCHAR2(20 BYTE),
RPT_DT VARCHAR2(25 BYTE),
AMT_BILL NUMBER,
ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);
CREATE TABLE POS_TERMINAL_6M (
CUSTOMER_CDE VARCHAR2(25 BYTE),
MERCHANT_CDE VARCHAR2(25 BYTE),
TERMINAL_ID VARCHAR2(20 BYTE),
RPT_DT DATE,
CT_TXN_TERMINAL NUMBER,
ADD_TSTP TIMESTAMP(6) WITH TIME ZONE
);

INSERT INTO  POS_MERCHANT_AMT_6M 
select E.customer_cde, F.MERCHANT_ID  , NULL TERMINAL_ID,TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL,CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
select customer_cde, MERCHANT_CDE,  AMT_BILL FROM
(
select customer_cde, merchant_cde,
       SUM(AMT_BILL) AMT_BILL, row_number()over(partition by customer_cde order by SUM(AMT_BILL) desc) rn1
from
(
select customer_cde, merchant_cde,cardhdr_no,
        approval_cde, retrvl_refno,
        process_dt, 
        AMT_BILL,
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn

from 
(
 select customer_cde, trim(' ' from(merchant_cde)) merchant_cde, cardhdr_no,
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,ABS(AMT_BILL) AMT_BILL,
        process_dt
        from DW_ANALYTICS.dw_card_transaction_fct T1
        where process_dt < TO_DATE('{RPT_DT}','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
        and tran_status = 'S' 
        and  exists (select 1 from CINS_TMP_CUST t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
 )    
         )
where rn = 1
group by customer_cde, merchant_cde
             )
where rn1 = 1
) E
JOIN
(SELECT MERCHANT_ID
FROM DW_ANALYTICS.DW_CARD_MERCHANT_DIM) F
ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL;

INSERT INTO POS_MERCHANT_6M
select E.customer_cde, F.MERCHANT_ID  , NULL TERMINAL_ID,TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, ct_txn_pos,CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
select customer_cde, MERCHANT_CDE,  ct_txn_pos FROM
(
select customer_cde, merchant_cde,
       count(*) ct_txn_pos, row_number()over(partition by customer_cde order by count(*) desc) rn1
from
(
select customer_cde, merchant_cde,cardhdr_no,
        approval_cde, retrvl_refno,
        process_dt, 
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn

from 
(
 select customer_cde, trim(' ' from(merchant_cde)) merchant_cde, cardhdr_no,
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,
        process_dt
        from DW_ANALYTICS.dw_card_transaction_fct T1
        where process_dt < TO_DATE('{RPT_DT}','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
        and tran_status = 'S' 
        and  exists (select 1 from CINS_TMP_CUST t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
 )    
         )
where rn = 1
group by customer_cde, merchant_cde
             )
where rn1 = 1
) E
JOIN
(SELECT MERCHANT_ID
FROM DW_ANALYTICS.DW_CARD_MERCHANT_DIM) F
ON E.MERCHANT_CDE = F.MERCHANT_ID
WHERE E.MERCHANT_CDE IS NOT NULL;

INSERT INTO POS_TERMINAL_AMT_6M
select E.customer_cde,
    F.MERCHANT_CDE, F.TERMINAL_ID
    ,TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, AMT_BILL ,CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
select customer_cde, MERCHANT_CDE, TERMINAL_ID ,  AMT_BILL FROM
(
select customer_cde, merchant_cde, TERMINAL_ID,
       SUM(AMT_BILL) AMT_BILL, row_number()over(partition by customer_cde order by SUM(AMT_BILL) desc) rn1
from
(
select customer_cde, merchant_cde,cardhdr_no, TERMINAL_ID,
        approval_cde, retrvl_refno,
        process_dt, AMT_BILL,
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn

from 
(
 select customer_cde, trim(' ' from(merchant_cde)) merchant_cde, cardhdr_no, TERMINAL_ID,
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,ABS(AMT_BILL) AMT_BILL,
        process_dt
        from DW_ANALYTICS.dw_card_transaction_fct T1
        where process_dt < TO_DATE('{RPT_DT}','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
        and tran_status = 'S' 
        and  exists (select 1 from CINS_TMP_CUST t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
 )    
         )
where rn = 1
group by customer_cde, merchant_cde, terminal_id
             )
where rn1 = 1
) E
LEFT JOIN
(
SELECT MERCHANT_CDE, TERMINAL_ID, TERMINAL_TYPE FROM DW_ANALYTICS.DW_CARD_TERMINAL_DIM 
WHERE TERMINAL_TYPE = 'POS'
) F
ON E.MERCHANT_CDE = F.MERCHANT_CDE AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE F.MERCHANT_CDE IS NOT NULL;


INSERT INTO POS_TERMINAL_6M
select E.customer_cde,
    F.MERCHANT_CDE, F.TERMINAL_ID
    ,TO_DATE('{RPT_DT}','DD-MM-YY') AS RPT_DT, CT_TXN_TERMINAL ,CURRENT_TIMESTAMP ADD_TSTP  
FROM
(
select customer_cde, MERCHANT_CDE, TERMINAL_ID ,  ct_txn_terminal FROM
(
select customer_cde, merchant_cde, TERMINAL_ID,
       count(*) ct_txn_terminal, row_number()over(partition by customer_cde order by count(*) desc) rn1
from
(
select customer_cde, merchant_cde,cardhdr_no, TERMINAL_ID,
        approval_cde, retrvl_refno,
        process_dt, 
        row_number()over(partition by customer_cde,cardhdr_no, approval_cde, retrvl_refno order by process_dt desc) rn

from 
(
 select customer_cde, trim(' ' from(merchant_cde)) merchant_cde, cardhdr_no, TERMINAL_ID,
        trim(' ' from (approval_cde)) approval_cde, retrvl_refno,
        process_dt
        from DW_ANALYTICS.dw_card_transaction_fct T1
        where process_dt < TO_DATE('{RPT_DT}','DD-MM-YY') AND process_dt >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
        and tran_status = 'S' 
        and  exists (select 1 from CINS_TMP_CUST t2 where t1.CUSTOMER_CDE=t2.CUSTOMER_CDE) 
 )    
         )
where rn = 1
group by customer_cde, merchant_cde, terminal_id
             )
where rn1 = 1
) E
LEFT JOIN
(
SELECT MERCHANT_CDE, TERMINAL_ID, TERMINAL_TYPE FROM DW_ANALYTICS.DW_CARD_TERMINAL_DIM 
WHERE TERMINAL_TYPE = 'POS'
) F
ON E.MERCHANT_CDE = F.MERCHANT_CDE AND E.TERMINAL_ID = F.TERMINAL_ID
WHERE F.MERCHANT_CDE IS NOT NULL;


CREATE TABLE CINS_TMP_CARD_CREDIT_LOAN_6M_{RPT_DT} AS 
SELECT 
CUSTOMER_CDE,
ROW_NUMBER() OVER(PARTITION BY CUSTOMER_CDE ORDER BY ACTIVATION_DT DESC) RN
FROM DW_ANALYTICS.DW_CARD_MASTER_DIM
WHERE 
SUBSTR(CARD_CDE,1,1) = '3' AND PLASTIC_CDE = ' ' AND STATUS_CDE = ' '
AND TO_DATE('{RPT_DT}','DD-MM-YY') - TO_DATE(ACTIVATION_DT) >= 180
