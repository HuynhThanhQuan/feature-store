/*
Table Name: CINS_TMP_POS_TERMINAL_6M_{RPT_DT_TBL}
Derived From:
  DW_ANALYTICS.DW_CARD_TRANSACTION_FCT:
    - CUSTOMER_CDE
    - MERCHANT_CDE
    - TERMINAL_ID
    - CARDHDR_NO
    - APPROVAL_CDE
    - RETRVL_REFNO
    - PROCESS_DT
    - AMT_BILL
  DW_ANALYTICS.DW_CARD_TERMINAL_DIM:
    - TERMINAL_TYPE
*/
INSERT INTO CINS_TMP_POS_TERMINAL_6M_{RPT_DT_TBL} 
WITH 
T1 AS (
  SELECT 
    CUSTOMER_CDE, 
    TRIM(' ' FROM(MERCHANT_CDE)) AS MERCHANT_CDE, 
    CARDHDR_NO, 
    TERMINAL_ID,
    TRIM(' ' FROM (APPROVAL_CDE)) AS APPROVAL_CDE, 
    RETRVL_REFNO,
    ABS(AMT_BILL) AS AMT_BILL,
    PROCESS_DT
  FROM DW_ANALYTICS.DW_CARD_TRANSACTION_FCT Ta1
  RIGHT JOIN CINS_TMP_CUSTOMER_{RPT_DT_TBL} Ta2 ON Ta1.CUSTOMER_CDE=Ta2.CUSTOMER_CDE
  WHERE 
    PROCESS_DT < TO_DATE('{RPT_DT}','DD-MM-YY') 
    AND PROCESS_DT >= ADD_MONTHS(TO_DATE('{RPT_DT}','DD-MM-YY'), -6)
    AND TRAN_STATUS = 'S' 
    AND Ta1.CUSTOMER_CDE IS NOT NULL
),
T2 AS (
  SELECT 
    CUSTOMER_CDE, 
    MERCHANT_CDE,
    CARDHDR_NO, 
    TERMINAL_ID,
    APPROVAL_CDE, 
    RETRVL_REFNO,
    PROCESS_DT, 
    AMT_BILL,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_CDE,CARDHDR_NO, APPROVAL_CDE, RETRVL_REFNO ORDER BY PROCESS_DT DESC) AS RN
  FROM T1
),
T3 AS (
  SELECT 
    CUSTOMER_CDE, 
    MERCHANT_CDE, 
    TERMINAL_ID,
    COUNT(*) AS CT_TXN_TERMINAL,
    SUM(AMT_BILL) AMT_BILL, 
    ROW_NUMBER()OVER(PARTITION BY CUSTOMER_CDE ORDER BY SUM(AMT_BILL) DESC) AS RN1
  FROM T2
  WHERE RN = 1
  GROUP BY CUSTOMER_CDE, MERCHANT_CDE, TERMINAL_ID
),
T4 AS (
  SELECT 
    CUSTOMER_CDE, 
    MERCHANT_CDE, 
    TERMINAL_ID, 
    CT_TXN_TERMINAL,
    AMT_BILL
  FROM T3
  WHERE RN1 = 1
),
T5 AS (
  SELECT 
    MERCHANT_CDE, 
    TERMINAL_ID, 
    TERMINAL_TYPE 
  FROM DW_ANALYTICS.DW_CARD_TERMINAL_DIM 
  WHERE TERMINAL_TYPE = 'POS'
),
T6 AS (
  SELECT 
    T4.CUSTOMER_CDE, 
    T5.MERCHANT_CDE, 
    T5.TERMINAL_ID, 
    T4.AMT_BILL,
    T4.CT_TXN_TERMINAL,
    TO_CHAR(TO_DATE('{RPT_DT}','DD-MM-YY'), 'DD-MM-YYYY') AS RPT_DT, 
    CURRENT_TIMESTAMP ADD_TSTP
  FROM T4
  LEFT JOIN T5 ON T4.MERCHANT_CDE = T5.MERCHANT_CDE AND T4.TERMINAL_ID = T5.TERMINAL_ID
  WHERE T5.MERCHANT_CDE IS NOT NULL
)

SELECT * FROM T6;