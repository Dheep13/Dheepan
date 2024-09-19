CREATE PROCEDURE EXT.SP_RPT_POST_EXTPMTSUMMARY
(
    IN p_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_rpttype VARCHAR2 */
    IN p_periodseq DECIMAL(38,10)   /* ORIGSQL: p_periodseq NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/ 
    /* ORIGSQL: DELETE FROM ST_EXT_PAY_SUMMARY WHERE periodseq = p_periodseq; */
    DELETE
    FROM
        EXT.ST_EXT_PAY_SUMMARY
    WHERE
        periodseq = :p_periodseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.ST_EXT_PAY_SUMMARY' not found */

    /* ORIGSQL: INSERT INTO ST_EXT_PAY_SUMMARY (TENANTID, POSITIONNAME, VENDOR_NAME, GROUPFIELDL(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_EXTPAYSUMMARY' not found */
    INSERT INTO EXT.ST_EXT_PAY_SUMMARY
        (
            TENANTID,
            POSITIONNAME,
            VENDOR_NAME,
            GROUPFIELDLABEL,
            AMOUNT,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            USERID
        )
        SELECT   /* ORIGSQL: SELECT TENANTID, POSITIONNAME, VENDOR_NAME, GROUPFIELDLABEL, AMOUNT, PERIODSEQ, (...) */
            TENANTID,
            POSITIONNAME,
            VENDOR_NAME,
            GROUPFIELDLABEL,
            AMOUNT,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            USERID
        FROM
            EXT.STEL_RPT_DATA_EXTPAYSUMMARY
        WHERE
            periodseq = :p_periodseq
            AND rpttype = :p_rpttype;

    /* ORIGSQL: NULL; */
    DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
END