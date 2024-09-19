CREATE PROCEDURE EXT.PRC_LOGEVENT_R5
(
    IN in_periodName VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                    /* ORIGSQL: in_periodName IN VARCHAR2 */
    IN in_procName VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                  /* ORIGSQL: in_procName IN VARCHAR2 */
    IN in_status VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: in_status IN VARCHAR2 */
    IN in_statusDetail VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                      /* ORIGSQL: in_statusDetail IN VARCHAR2 */
    IN in_OracleErrormessage TIMESTAMP     /* ORIGSQL: in_OracleErrormessage IN TIMESTAMP */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodkey DECIMAL(38) = NULL;  /* ORIGSQL: v_periodkey NUMBER(38) := NULL; */

    /* ORIGSQL: PRAGMA AUTONOMOUS_TRANSACTION; */
    BEGIN AUTONOMOUS TRANSACTION
        --CREATE SEQUENCE  "RPT_REPORTING_LOG_SEQ"  MINVALUE 1 MAXVALUE 999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE ;
        --ALTER TABLE RPT_REPORTING_LOG ADD SNO INT

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_REPORTING_LOG_R5' not found */

        /* ORIGSQL: INSERT INTO RPT_REPORTING_LOG_R5 VALUES (null,vProcName,'R5 Report table Cursor (...) */
        INSERT INTO EXT.RPT_REPORTING_LOG_R5
        VALUES (
                NULL,
                --:vProcName,
                :in_procName,
                'R5 Report table Cursor Start',
                NULL,
                CURRENT_TIMESTAMP 
        );
    --);;/* NOT CONVERTED! */

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END;
END