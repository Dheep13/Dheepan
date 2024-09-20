CREATE OR REPLACE PROCEDURE EXT.TRYG_LOG ( IN IN_PROCNAME VARCHAR2(4000),
IN IN_COMMENTS VARCHAR2(4000),
IN IN_VALUE BIGINT	) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Author: Sharath K
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 19-April-2022
    |----------------------------------------------------------------------
    | Procedure Purpose: 
    | Version: 0.1	19-April-2022	Intial Version
    -----------------------------------------------------------------------
    */
BEGIN

	INSERT INTO EXT.TRYG_DEBUG_LOG (PROCNAME,COMMENTS,VALUE) VALUES (:IN_PROCNAME,:IN_COMMENTS,:IN_VALUE);
	COMMIT;
	
END
