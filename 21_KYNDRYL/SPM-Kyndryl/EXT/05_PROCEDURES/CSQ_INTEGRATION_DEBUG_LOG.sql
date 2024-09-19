CREATE COLUMN TABLE "EXT"."CSQ_INTEGRATION_DEBUG_LOG"(
	"OBJECT_NAME" VARCHAR(100),
	"DEBUG_LOCATION" VARCHAR(100),
	"DEBUG_MESSAGE" VARCHAR(1000),
	"DEBUG_TIMESTAMP" LONGDATE CS_LONGDATE
)
UNLOAD PRIORITY 5 AUTO MERGE;

CREATE COLUMN TABLE "EXT"."CSQ_INTEGRATION_DEBUG_CTL"(
	"TENANTID" VARCHAR(4),
	"OBJECT_NAME" VARCHAR(100),
	"DEBUGLEVEL" INTEGER CS_INT
)
UNLOAD PRIORITY 5 AUTO MERGE;



CREATE PROCEDURE EXT.CPQ_SP_INTEGRATION_DEBUG_WRITE(
		IN DEBUG_LEVEL INT,
		IN OBJECT_NAME VARCHAR(100),
		IN DEBUG_LOCATION VARCHAR(100),
		IN DEBUG_MESSAGE VARCHAR(1000),
		IN DEBUG_TIMESTAMP TIMESTAMP
	)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER
	DEFAULT SCHEMA EXT
AS
/**************************************************************************************************
	This stored procedure writes to the custom debug log table

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	0.01       01-JUL-2022   Karthik Raju     Initial creation

***************************************************************************************************/
BEGIN
	IF DEBUG_LEVEL = 1 THEN
		INSERT INTO EXT.CSQ_INTEGRATION_DEBUG_LOG VALUES(OBJECT_NAME, DEBUG_LOCATION, DEBUG_MESSAGE, DEBUG_TIMESTAMP);
	END IF;
	
	COMMIT;
END
