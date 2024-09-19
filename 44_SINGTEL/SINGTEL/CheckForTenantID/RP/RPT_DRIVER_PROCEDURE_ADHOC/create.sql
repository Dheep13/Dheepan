CREATE PROCEDURE EXT.RPT_DRIVER_PROCEDURE_ADHOC
(
    IN vrpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                               /* ORIGSQL: vrpttype IN varchar2 */
    --IN vperiodseq CS_PERIODCALENDAR.periodseq%type,   /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.periodseq%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE_ADHOC.vperiodseq' */
                                                      /* ORIGSQL: vperiodseq IN CS_PERIODCALENDAR.periodseq%type */
    --IN vprocessingunitseq CS_PROCESSINGUNIT.processingunitseq%type,   /* RESOLVE: Identifier not found: Table/Column 'CS_PROCESSINGUNIT.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PROCESSINGUNIT.processingunitseq%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE_ADHOC.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN CS_PROCESSINGUNIT.processingunitseq%type */
    --IN vcalendarseq CS_PERIODCALENDAR.calendarseq%type,   /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT,
                                                          /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.calendarseq%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE_ADHOC.vcalendarseq' */
                                                          /* ORIGSQL: vcalendarseq IN CS_PERIODCALENDAR.calendarseq%type */
    --IN vtenantid CS_PERIODCALENDAR.tenantid%type      /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.tenantid' not found (for %TYPE declaration) */
    IN vtenantid BIGINT
                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.tenantid%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE_ADHOC.vtenantid' */
                                                      /* ORIGSQL: vtenantid IN CS_PERIODCALENDAR.tenantid%type */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_STRING_1 VARCHAR(5000); /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE vprocname VARCHAR(50) = 'RPT_DRIVER_PROCEDURE';  /* ORIGSQL: vprocname VARCHAR2(50) := 'RPT_DRIVER_PROCEDURE'; */

    /* ORIGSQL: VARCHAR2(50 BYTE) := 'RPT_DRIVER_PROCEDURE'; */
    DECLARE v_Sql VARCHAR(30000);  /* ORIGSQL: v_Sql VARCHAR2(30000); */
    DECLARE v_reportlist VARCHAR(30000);  /* ORIGSQL: v_reportlist VARCHAR2(30000); */
    DECLARE vStartIdx DECIMAL(38,10);  /* ORIGSQL: vStartIdx NUMBER; */
    DECLARE vEndIdx DECIMAL(38,10);  /* ORIGSQL: vEndIdx NUMBER; */
    DECLARE vcurValue VARCHAR(5000);  /* ORIGSQL: vcurValue varchar2(5000); */

    --loop for distint list of rpttypes for the list of cald reports. order by runorder
    --call the init, field and postsql
    /* ORIGSQL: for i in (select distinct rpttype, postproc, rewardortxn, runorder from stel_rpt(...) */
    DECLARE CURSOR dbmtk_cursor_10980
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_RPTTYPE' not found */
        SELECT   /* ORIGSQL: select distinct rpttype, postproc, rewardortxn, runorder from stel_rpt_cfg_rptty(...) */
            DISTINCT
            rpttype,
            postproc,
            rewardortxn,
            runorder
        FROM
            ext.stel_rpt_cfg_rpttype r
        INNER JOIN
            EXT.STEL_RPT_RUNNINGREPORTS rr
            ON LOCATE(r.rpttype,rr.caldreportname,1,1) > 0  /* ORIGSQL: instr(r.rpttype,rr.caldreportname) */
        ORDER BY
            runorder;

    /***************************************************************************
    The purpose of this procedure to create one entry point for
    running more than one report extract in the
    ODSReportsGenerationConfig.xml.
    Date Author Description
    --------------------------------------------------------------------
    
    /#******************************
    Check Pipeline table to see which reports are running
    *******************************/

    SELECT
        :vrpttype
    INTO
        v_reportlist
    FROM
        SYS.DUMMY;  /* ORIGSQL: FROM dual ; */

    /* ORIGSQL: execute immediate 'truncate table STEL_RPT_RUNNINGREPORTS'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_RUNNINGREPORTS' not found */

    /* ORIGSQL: truncate table STEL_RPT_RUNNINGREPORTS ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.STEL_RPT_RUNNINGREPORTS';

    /* ORIGSQL: insert into STEL_RPT_RUNNINGREPORTS values(v_reportlist); */
    INSERT INTO EXT.STEL_RPT_RUNNINGREPORTS
    VALUES(:v_reportlist);

    /* ORIGSQL: dbms_output.put_line(v_reportlist); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_reportlist);

    FOR i AS dbmtk_cursor_10980
    DO
        /*****************************************/
        /****Calls the main report logic**********/
        /*****************************************/
        /*****************************************/
        /* ORIGSQL: dbms_output.put_line(i.rpttype); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:i.rpttype);

        IF UPPER(:i.rewardortxn) = 'R' 
        THEN
            /* ORIGSQL: dbms_output.put_line('Calling Reward Init'); */
            --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Calling Reward Init');

            /* ORIGSQL: rpt_rewardinit(i.rpttype, vperiodseq, vprocessingunitseq, vtenantid) */
            CALL EXT.RPT_REWARDINIT(:i.rpttype, :vperiodseq, :vprocessingunitseq, :vtenantid);

            /* ORIGSQL: dbms_output.put_line('Calling Reward FIELD'); */
            --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Calling Reward FIELD');

            /* ORIGSQL: rpt_rewardfield(i.rpttype, vperiodseq, vprocessingunitseq,vtenantid) */
            CALL EXT.RPT_REWARDFIELD(:i.rpttype, :vperiodseq, :vprocessingunitseq, :vtenantid);
        END IF;

        IF UPPER(:i.rewardortxn) = 'T' 
        THEN
            /* ORIGSQL: rpt_txninit(i.rpttype, vperiodseq, vprocessingunitseq, vtenantid) */
            CALL EXT.RPT_TXNINIT(:i.rpttype, :vperiodseq, :vprocessingunitseq, :vtenantid);

            /* ORIGSQL: rpt_txnfield(i.rpttype, vperiodseq, vprocessingunitseq,vtenantid) */
            CALL EXT.RPT_TXNFIELD(:i.rpttype, :vperiodseq, :vprocessingunitseq, :vtenantid);
        END IF;

        v_Sql = :i.postproc;

        IF :v_Sql IS NOT NULL
        THEN
            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: execute immediate v_Sql using i.rpttype, vperiodseq, vprocessingunitseq; */
            --CALL sapdbmtk.sp_dbmtk_prepare_execute_sql(:v_Sql, :DBMTK_TMPVAR_STRING_1);
            EXECUTE IMMEDIATE :DBMTK_TMPVAR_STRING_1 USING :i.rpttype, :vperiodseq, :vprocessingunitseq;
        END IF;
    END FOR;  /* ORIGSQL: END LOOP; */

    -- Call specific reporting procedures.
    -- ...

    /* ORIGSQL: commit; */
    COMMIT;
END