CREATE PROCEDURE ext.ctas_error_log ( IN p_errorcode VARCHAR(10),
	 IN p_errormessage VARCHAR(1000),
	 IN p_count number DEFAULT 0 ) 
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
DEFAULT SCHEMA EXT
AS
BEGIN
DECLARE v_tenantid nvarchar(4);

SELECT
	DISTINCT TENANTID
INTO
	v_tenantid
FROM
	TCMP.CS_TENANT;

INSERT
	INTO
	ext.ctas_error_detail ( tenantid,
	 datetime,
	 errorcode,
	 errormessage,
	 count )
VALUES( :v_tenantid,
	 current_timestamp,
	 :p_errorcode,
	 :p_errormessage,
	 :p_count );

COMMIT;
END