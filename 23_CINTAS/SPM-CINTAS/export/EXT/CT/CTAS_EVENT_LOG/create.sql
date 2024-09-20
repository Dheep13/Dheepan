CREATE PROCEDURE ext.ctas_event_log ( IN p_comments VARCHAR(1000),
	 IN p_text VARCHAR(32000),
	 IN p_value DECIMAL(38,10) DEFAULT 0 ) 
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
	ext.ctas_debug ( tenantid,
	 datetime,
	 Comments,
	 text,
	 value )
VALUES( :v_tenantid,
	 current_timestamp,
	 :p_comments,
	 :p_text,
	 :p_value );

COMMIT;
END