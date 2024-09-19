CREATE PROCEDURE EXT.STEL_MONACO_REPORT_HOOK
(
    IN IN_PERIODSEQ BIGINT,   /* ORIGSQL: IN_PERIODSEQ IN INTEGER */
    IN IN_PROCESSINGUNITSEQ BIGINT     /* ORIGSQL: IN_PROCESSINGUNITSEQ IN INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
READS SQL DATA
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /*
          *********          DEVELOPER :  OFF SHORE TEAM
          *********          StartDate :      01-NOV-2016
          *********          EndDate   :       30-DEC-2016
          *********          Purpose   : This Procedure will added in stage hook to populate data in required for crystal and WEBI reports.
     */

    -- Calling procedure to populate data for Crystal report.
    /* ORIGSQL: STEL_BUILD_REPORT_DATA.STEL_RPT_EXTRACT_DRIVER (in_periodseq, in_processingunits(...) */
    CALL EXT.STEL_BUILD_REPORT_DATA__STEL_RPT_EXTRACT_DRIVER(:IN_PERIODSEQ, :IN_PROCESSINGUNITSEQ);

    /* ORIGSQL: NULL; */
    DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
END