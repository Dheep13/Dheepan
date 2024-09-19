CREATE PROCEDURE EXT.STEL_BUILD_REPORT_DATA__STEL_RPT_EXTRACT_DRIVER
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

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_peirodseq BIGINT;  /* ORIGSQL: v_peirodseq INTEGER; */
    DECLARE v_reports VARCHAR(4000);  /* ORIGSQL: v_reports VARCHAR2(4000); */
    DECLARE V_RPT1 DECIMAL(38,10);  /* ORIGSQL: V_RPT1 NUMBER; */

    --Customer Detail Report
    DECLARE V_RPT2 DECIMAL(38,10);  /* ORIGSQL: V_RPT2 NUMBER; */

    -- Payment Summary Report
    DECLARE V_RPT3 DECIMAL(38,10);  /* ORIGSQL: V_RPT3 NUMBER; */

    -- Performance Summary Report
    DECLARE V_RPT4 DECIMAL(38,10);  /* ORIGSQL: V_RPT4 NUMBER; */

    -- Transaction Detail
    DECLARE V_RPT5 DECIMAL(38,10);  /* ORIGSQL: V_RPT5 NUMBER; */

    --Individual Payment Summary
    DECLARE V_RPT6 DECIMAL(38,10);  /* ORIGSQL: V_RPT6 NUMBER; */

    -- mremmit Payment Summary Report
    DECLARE V_RPT7 DECIMAL(38,10);  /* ORIGSQL: V_RPT7 NUMBER; */

    -- External Vendor Payment Summary
    DECLARE V_RPT8 DECIMAL(38,10);  /* ORIGSQL: V_RPT8 NUMBER; */

    --      External Vendor Payment Summary Midmonth
    DECLARE v_RPT9 DECIMAL(38,10);  /* ORIGSQL: v_RPT9 NUMBER; */

    -- Requisition Memo
    DECLARE V_RPT10 DECIMAL(38,10);  /* ORIGSQL: V_RPT10 NUMBER; */

    -- Cover Note
    DECLARE v_rpt11 DECIMAL(38,10);  /* ORIGSQL: v_rpt11 NUMBER; */

    -- 10-Pick and Go - Payment Detail Report
    DECLARE v_rpt12 DECIMAL(38,10);  /* ORIGSQL: v_rpt12 NUMBER; */

    --11-Pick and Go - Payment Summary Report
    DECLARE v_rpt13 DECIMAL(38,10);  /* ORIGSQL: v_rpt13 NUMBER; */

    --12-Pick and Go - Requisition Memo
    --DECLARE v_period cs_period%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_period ROW LIKE cs_period;

    /* ORIGSQL: NULL; */
    DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
    --removed by Arjun. This package is not to be used for anything.
END