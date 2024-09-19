CREATE PROCEDURE STEL_SP_LOGGER
(
    IN P_LogMessage VARCHAR2(2000),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */ /* ORIGSQL: P_LogMessage IN VARCHAR2 */
    IN P_LogType VARCHAR(100),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */ /* ORIGSQL: P_LogType IN VARCHAR2 */
    IN P_LogValue VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */ /* ORIGSQL: P_LogValue IN VARCHAR2 */
    IN P_ErrorCode VARCHAR2(2000),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */ /* ORIGSQL: P_ErrorCode IN VARCHAR2 */
    IN p_logdata NCLOB     /* ORIGSQL: p_logdata IN long */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* ORIGSQL: PRAGMA AUTONOMOUS_TRANSACTION; */
    BEGIN AUTONOMOUS TRANSACTION

        /* ORIGSQL: INSERT INTO STEL_logger (logseq, DATETIME, logtype, logmessage, VALUE, errorcode(...) */
        INSERT INTO ext.stel_logger
            (
                logseq,
                DATETIME,
                logtype,
                logmessage,
                VALUE,
                errorcode,
                logdata
            )
        VALUES
            (
                    ext.stel_loggerseq.nextval,  /* RESOLVE: Identifier not found: Sequence not found: */
                                                     /* ORIGSQL: stel_loggerseq.nextval */
                    CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                    SUBSTRING(:P_LogType,1,100),  /* ORIGSQL: substr(p_logType,1,100) */
                    SUBSTRING(:P_LogMessage,1,2000),  /* ORIGSQL: substr(p_logMessage,1,2000) */
                    :P_LogValue,
                    :P_ErrorCode,
                    SUBSTRING(:p_logdata,1,4000)  /* ORIGSQL: SUBSTR(p_logdata,1,4000) */
            );

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END;
END;
