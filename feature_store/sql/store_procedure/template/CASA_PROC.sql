CREATE OR REPLACE PROCEDURE CASA_PROC (RPT_DT IN DATE)

AS 

BEGIN

EXECUTE IMMEDIATE 'TRUNCATE TABLE CINS_FEATURE_STORE_CASA'; 

COMMIT; 

${FEATURE_SCRIPTS}$

COMMIT;

END;
