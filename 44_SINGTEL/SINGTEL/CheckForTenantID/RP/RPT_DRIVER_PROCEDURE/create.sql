CREATE PROCEDURE EXT.RPT_DRIVER_PROCEDURE
(
    --IN vperiodseq CS_PERIODCALENDAR.periodseq%type,   /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.periodseq%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE.vperiodseq' */
                                                      /* ORIGSQL: vperiodseq IN CS_PERIODCALENDAR.periodseq%type */
    --IN vprocessingunitseq CS_PROCESSINGUNIT.processingunitseq%type,   /* RESOLVE: Identifier not found: Table/Column 'CS_PROCESSINGUNIT.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PROCESSINGUNIT.processingunitseq%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN CS_PROCESSINGUNIT.processingunitseq%type */
    --IN vcalendarseq CS_PERIODCALENDAR.calendarseq%type,   /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT,
                                                          /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.calendarseq%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE.vcalendarseq' */
                                                          /* ORIGSQL: vcalendarseq IN CS_PERIODCALENDAR.calendarseq%type */
    --IN vtenantid CS_PERIODCALENDAR.tenantid%type      /* RESOLVE: Identifier not found: Table/Column 'CS_PERIODCALENDAR.tenantid' not found (for %TYPE declaration) */
    IN vtenantid BIGINT
                                                      /* RESOLVE: Datatype unresolved: Datatype (CS_PERIODCALENDAR.tenantid%type) not resolved for parameter 'RPT_DRIVER_PROCEDURE.vtenantid' */
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
    DECLARE vLoop DECIMAL(38,10) = 0;  /* ORIGSQL: vLoop number:=0; */

    --loop for distint list of rpttypes for the list of cald reports. order by runorder
    --call the init, field and postsql
    /* ORIGSQL: for i in (select distinct runorder, rpttype, postproc, rewardortxn from stel_rpt(...) */
    DECLARE CURSOR dbmtk_cursor_10894
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_RPTTYPE' not found */
        SELECT   /* ORIGSQL: select distinct runorder, rpttype, postproc, rewardortxn from stel_rpt_cfg_rptty(...) */
            DISTINCT
            runorder,
            rpttype,
            postproc,
            rewardortxn
        FROM
            ext.stel_rpt_cfg_rpttype r
        INNER JOIN
            EXT.STEL_RPT_RUNNINGREPORTS rr
            ON LOCATE(UPPER(r.caldreportname),UPPER(rr.caldreportname),1,1) > 0  /* ORIGSQL: instr(upper(r.caldreportname), upper(rr.caldreportname)) */
        ORDER BY
            runorder;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: stel_Sp_logger ('Exception ', vprocname, null, SQLCODE, SQLERRM) */
            CALL EXT.STEL_SP_LOGGER('Exception ', :vprocname, NULL, ::SQL_ERROR_CODE 
                , ::SQL_ERROR_MESSAGE   /* ORIGSQL: SQLCODE */
            );  /* ORIGSQL: SQLERRM */
        END;

        /***************************************************************************
        The purpose of this procedure to create one entry point for
        running more than one report extract in the
        ODSReportsGenerationConfig.xml.
        Date Author Description
        --------------------------------------------------------------------
        
        /#******************************
        Check Pipeline table to see which reports are running
        *******************************/

        /* ORIGSQL: stel_Sp_logger ('Starting RPT_DRIVER+PROC', vprocname, null, NULL, null) */
        CALL EXT.STEL_SP_LOGGER('Starting RPT_DRIVER+PROC', :vprocname, NULL, NULL, NULL);

        /* ORIGSQL: STEL_CUSTOMERMASTER (vPERIODSEQ, vPROCESSINGUNITSEQ) */
        CALL EXT.STEL_CUSTOMERMASTER(:vperiodseq, :vprocessingunitseq);

        /* ORIGSQL: execute immediate 'truncate table stel_monthhierarchy_tbl' ; */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_MONTHHIERARCHY_TBL' not found */

        /* ORIGSQL: truncate table stel_monthhierarchy_tbl ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_monthhierarchy_tbl';

        /* ORIGSQL: insert into stel_monthhierarchy_tbl(CALENDARNAME, CALENDARSEQ, PERIODTYPELEVEL, (...) */
        INSERT INTO stel_monthhierarchy_tbl
            (
                CALENDARNAME,
                CALENDARSEQ,
                PERIODTYPELEVEL,
                PERIODTYPENAME,
                PERIODNAME,
                PERIODSEQ,
                STARTDATE,
                ENDDATE,
                MONTHPERIODSEQ1,
                MONTHNAME1,
                MONTHSTARTDATE1,
                MONTHENDDATE1,
                MONTHPERIODSEQTD,
                MONTHNAMETD,
                MONTHSTARTDATETD,
                MONTHENDDATETD
            )
            SELECT   /* ORIGSQL: SELECT CALENDARNAME, CALENDARSEQ, PERIODTYPELEVEL, PERIODTYPENAME, PERIODNAME, P(...) */
                CALENDARNAME,
                CALENDARSEQ,
                PERIODTYPELEVEL,
                PERIODTYPENAME,
                PERIODNAME,
                PERIODSEQ,
                STARTDATE,
                ENDDATE,
                MONTHPERIODSEQ1,
                MONTHNAME1,
                MONTHSTARTDATE1,
                MONTHENDDATE1,
                MONTHPERIODSEQTD,
                MONTHNAMETD,
                MONTHSTARTDATETD,
                MONTHENDDATETD
            FROM
                STEL_MONTHHIERARCHY;

        /* ORIGSQL: execute immediate 'truncate table stel_periodhierarchy_tbl' ; */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_PERIODHIERARCHY_TBL' not found */

        /* ORIGSQL: truncate table stel_periodhierarchy_tbl ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_periodhierarchy_tbl';

        /* ORIGSQL: insert into stel_periodhierarchy_tbl(CALENDARNAME, PERIODTYPELEVEL, PERIODTYPENA(...) */
        INSERT INTO stel_periodhierarchy_tbl
            (
                CALENDARNAME,
                PERIODTYPELEVEL,
                PERIODTYPENAME,
                PERIODNAME,
                PERIODSEQ,
                STARTDATE,
                ENDDATE,
                MONTHPERIODSEQ,
                MONTHNAME,
                MONTHSTARTDATE,
                MONTHENDDATE
            )
            SELECT   /* ORIGSQL: SELECT CALENDARNAME, PERIODTYPELEVEL, PERIODTYPENAME, PERIODNAME, PERIODSEQ, STA(...) */
                CALENDARNAME,
                PERIODTYPELEVEL,
                PERIODTYPENAME,
                PERIODNAME,
                PERIODSEQ,
                STARTDATE,
                ENDDATE,
                MONTHPERIODSEQ,
                MONTHNAME,
                MONTHSTARTDATE,
                MONTHENDDATE
            FROM
                STEL_PERIODHIERARCHY;

        /* ORIGSQL: stel_Sp_logger ('Get Report List', vprocname, null, NULL, null) */
        CALL EXT.STEL_SP_LOGGER('Get Report List', :vprocname, NULL, NULL, NULL);

        vStartIdx = 0;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PIPELINERUN' not found */

        SELECT
            /*substr(runparameters, instr(runparameters,'[odsReportList]')+15,
                        instr(runparameters,'[Mode]')-instr(runparameters,'[odsReportList]')-16)||',' */
            IFNULL(RTRIM(SUBSTRING_REGEXPR('\[odsReportList\]([^\[]+)' FLAG 'i' IN runparameters FROM 1 OCCURRENCE 1), ','),'') ||','   /* ORIGSQL: REGEXP_SUBSTR(runparameters, '\[odsReportList\]([^\[]+)', 1, 1, 'i', 1) */
        INTO
            v_reportlist
        FROM
            cs_pipelinerun
        WHERE
            command = 'PipelineRun'
            AND description LIKE '%ODS%'
            --and state<>'Done' and periodseq=vperiodseq --[arun reverted this change for time being]
            AND state <> 'Pending'
            AND state <> 'Done'
            AND periodseq = :vperiodseq;--[Added this on 30th Sep]

        /* ORIGSQL: stel_Sp_logger ('Report List:'||v_reportlist, vprocname, null, NULL, null) */
        CALL EXT.STEL_SP_LOGGER('Report List:'||IFNULL(:v_reportlist,''), :vprocname, NULL, NULL, NULL);

        vEndIdx = LOCATE(:v_reportlist,',',1,1);  /* ORIGSQL: instr(v_reportlist, ',') */

        IF :vEndIdx = 0
        THEN
            vEndIdx = LENGTH(:v_reportlist) +1;
        END IF;

        /* ORIGSQL: stel_Sp_logger ('Report List', vprocname, null, NULL, v_reportlist) */
        CALL EXT.STEL_SP_LOGGER('Report List', :vprocname, NULL, NULL, :v_reportlist);

        /* ORIGSQL: execute immediate 'truncate table STEL_RPT_RUNNINGREPORTS'; */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_RUNNINGREPORTS' not found */

        /* ORIGSQL: truncate table STEL_RPT_RUNNINGREPORTS ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE STEL_RPT_RUNNINGREPORTS';

        /* ORIGSQL: while(vEndIdx > 0) loop */
        WHILE (:vEndIdx > 0)
        DO
            vLoop = :vLoop + 1;

            --vcurValue = sapdbmtk.sp_f_dbmtk_substring(:v_reportlist,:vStartIdx+1,:vEndIdx - :vStartIdx - 1);  /* ORIGSQL: substr(v_reportlist, vStartIdx+1, vEndIdx - vStartIdx - 1) */
            vcurValue = substring(:v_reportlist,:vStartIdx+1,:vEndIdx - :vStartIdx - 1);

            /* ORIGSQL: stel_Sp_logger ('Report List Loop '||vLoop, vprocname, null, NULL, vCurValue) */
            CALL EXT.STEL_SP_LOGGER('Report List Loop '||IFNULL(TO_VARCHAR(:vLoop),''), :vprocname, NULL, NULL, :vcurValue); 

            /* ORIGSQL: insert into STEL_RPT_RUNNINGREPORTS values(vCurValue); */
            INSERT INTO EXT.STEL_RPT_RUNNINGREPORTS
            VALUES(:vcurValue);

            vStartIdx = :vEndIdx;

            vEndIdx = LOCATE(:v_reportlist,',',:vStartIdx + 1,1);  /* ORIGSQL: instr(v_reportlist, ',', vStartIdx + 1) */

            /* ORIGSQL: stel_Sp_logger ('vStartIdx: ', vStartIdx, null, NULL, null) */
            CALL EXT.STEL_SP_LOGGER('vStartIdx: ', :vStartIdx, NULL, NULL, NULL);

            /* ORIGSQL: stel_Sp_logger ('vEndIdx: ', vEndIdx, null, NULL, null) */
            CALL EXT.STEL_SP_LOGGER('vEndIdx: ', :vEndIdx, NULL, NULL, NULL);
        END WHILE;  /* ORIGSQL: end loop; */

        /* ORIGSQL: stel_Sp_logger ('Inserted Running Reports', vprocname, null, NULL, null) */
        CALL EXT.STEL_SP_LOGGER('Inserted Running Reports', :vprocname, NULL, NULL, NULL);

        FOR i AS dbmtk_cursor_10894
        DO
            /*****************************************/
            /****Calls the main report logic**********/
            /*****************************************/
            /*****************************************/

            /* ORIGSQL: stel_Sp_logger ('Report : '|| i.rpttype ||',' ||i.postproc ||',' ||i.rewardortxn(...) */
            CALL EXT.STEL_SP_LOGGER('Report : '|| IFNULL(:i.rpttype,'') ||','||IFNULL(:i.postproc,'') ||','||IFNULL(:i.rewardortxn,''), :vprocname, NULL, NULL, NULL);

            IF UPPER(:i.rewardortxn) = 'R' 
            THEN
                /* ORIGSQL: rpt_rewardinit(i.rpttype, vperiodseq, vprocessingunitseq, vtenantid) */
                CALL EXT.RPT_REWARDINIT(:i.rpttype, :vperiodseq, :vprocessingunitseq, :vtenantid);

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
            /* ORIGSQL: stel_Sp_logger ('Starting Postproc : '|| i.rpttype ||',' ||i.postproc ||',' ||i.(...) */
            CALL EXT.STEL_SP_LOGGER('Starting Postproc : '|| IFNULL(:i.rpttype,'') ||','||IFNULL(:i.postproc,'') ||','||IFNULL(:i.rewardortxn,''), :vprocname, NULL, NULL, NULL);

            v_Sql = :i.postproc;

            IF :v_Sql IS NOT NULL
            THEN
                /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                /* ORIGSQL: execute immediate v_Sql using i.rpttype, vperiodseq, vprocessingunitseq; */
                --CALL sapdbmtk.sp_dbmtk_prepare_execute_sql(:v_Sql, :DBMTK_TMPVAR_STRING_1);
                EXECUTE IMMEDIATE :DBMTK_TMPVAR_STRING_1 USING :i.rpttype, :vperiodseq, :vprocessingunitseq;
            END IF;
        END FOR;  /* ORIGSQL: END LOOP; */

        /* ORIGSQL: stel_Sp_logger ('Ending RPT_DRIVER+PROC', vprocname, null, NULL, null) */
        CALL EXT.STEL_SP_LOGGER('Ending RPT_DRIVER+PROC', :vprocname, NULL, NULL, NULL);

        -- Call specific reporting procedures.
        -- ...

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: exception when others then */
END