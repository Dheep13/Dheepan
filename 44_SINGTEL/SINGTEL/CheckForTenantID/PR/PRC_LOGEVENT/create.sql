CREATE PROCEDURE EXT.PRC_LOGEVENT
(
    IN in_periodName VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                    /* ORIGSQL: in_periodName IN VARCHAR2 */
    IN in_procName VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                  /* ORIGSQL: in_procName IN VARCHAR2 */
    IN in_status VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: in_status IN VARCHAR2 */
    IN in_statusDetail VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                      /* ORIGSQL: in_statusDetail IN VARCHAR2 */
    IN in_OracleErrormessage VARCHAR(3900)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                           /* ORIGSQL: in_OracleErrormessage IN VARCHAR2 */
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

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_REPORTING_LOG' not found */

        /* ORIGSQL: INSERT INTO RPT_REPORTING_LOG VALUES (in_periodName, in_procName, in_status, in_(...) */
        INSERT INTO EXT.RPT_REPORTING_LOG
        VALUES (
                :in_periodName,
                :in_procName,
                :in_status,
                :in_statusDetail,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :in_OracleErrormessage,
                EXT.RPT_REPORTING_LOG_SEQ.NEXTVAL  /* RESOLVE: Identifier not found: Sequence not found: */
                                                               /* ORIGSQL: RPT_REPORTING_LOG_SEQ.NEXTVAL */
        );

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END;
END