CREATE PROCEDURE EXT.PRC_COMM_SAA_SUMMARY
(
    --IN vrptname rpt_common_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_common_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                                      /* RESOLVE: Datatype unresolved: Datatype (rpt_common_mapping.reportname%TYPE) not resolved for parameter 'PRC_COMM_SAA_SUMMARY.vrptname' */
                                                      /* ORIGSQL: vrptname IN rpt_common_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE, /* --vmappedfor		IN rpt_common_mapping.MAPPEDFOR%TYPE,  */  /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                                                                                        /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
/* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE ) not resolved for parameter 'PRC_COMM_SAA_SUMMARY.vperiodseq' */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_COMM_SAA_SUMMARY.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT,
                                                  /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_COMM_SAA_SUMMARY.vcalendarseq' */
                                                  /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
    --IN vfrequency rpt_common_mapping.FREQUENCY%TYPE      /* RESOLVE: Identifier not found: Table/Column 'rpt_common_mapping.FREQUENCY' not found (for %TYPE declaration) */
    IN vfrequency BIGINT
                                                         /* RESOLVE: Datatype unresolved: Datatype (rpt_common_mapping.FREQUENCY%TYPE) not resolved for parameter 'PRC_COMM_SAA_SUMMARY.vfrequency' */
                                                         /* ORIGSQL: vfrequency IN rpt_common_mapping.FREQUENCY%TYPE */
)
--vrptname SAASUMMARY
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    -------------------------------------------------------------------------------------------------------------------
    -- Purpose:
    --
    -- Design objectives:
    -- Data for Dealer Statement Report
    -------------------------------------------------------------------------------------------------------------------
    -- Modification Log:
    -- Date             Author        Description
    -------------------------------------------------------------------------------------------------------------------
    -- 11-Jan-2017      Maria Monisha  Initial release
    -------------------------------------------------------------------------------------------------------------------
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_COMM_SAA_SUMMARY');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_COMM_SAA_SUMMARY') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_COMM_SAA_SUMMARY';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_COMM_SAA_SUMMARY'; */
    DECLARE vTenantId VARCHAR(4) = SUBSTRING(SESSION_USER,1,4);  /* ORIGSQL: vTenantId VARCHAR2(4) := SUBSTR(USER, 1, 4) ; */
    DECLARE vExtUser VARCHAR(7) = IFNULL(:vTenantId,'') || 'EXT';  /* ORIGSQL: vExtUser VARCHAR2(7) := vTenantId || 'EXT'; */
    DECLARE vSubPartitionPrefix VARCHAR(30) = 'P_';  /* ORIGSQL: vSubPartitionPrefix VARCHAR2(30) := 'P_'; */
    DECLARE vSubPartitionName VARCHAR(30);  /* ORIGSQL: vSubPartitionName VARCHAR2(30); */
    --DECLARE vPeriodRow CS_PERIOD%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.CS_PERIOD' not found (for %ROWTYPE declaration) */
    DECLARE vPeriodRow ROW LIKE CS_PERIOD;
    --DECLARE vCalendarRow CS_CALENDAR%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.CS_CALENDAR' not found (for %ROWTYPE declaration) */
    DECLARE vCalendarRow ROW LIKE CS_CALENDAR;
    --DECLARE vProcessingUnitRow CS_PROCESSINGUNIT%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found (for %ROWTYPE declaration) */
    DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;
    DECLARE vCurYrStartDate TIMESTAMP;  /* ORIGSQL: vCurYrStartDate DATE; */
    DECLARE vCurYrEndDate TIMESTAMP;  /* ORIGSQL: vCurYrEndDate DATE; */
    DECLARE v_envdtl VARCHAR(1000);  /* ORIGSQL: v_envdtl VARCHAR2(1000); */
    DECLARE v_sql NCLOB;  /* ORIGSQL: v_sql long; */
    DECLARE v_payable SMALLINT;  /* ORIGSQL: v_payable NUMBER(1); */
    DECLARE v_UserGroup VARCHAR(1) = 'N';  /* ORIGSQL: v_UserGroup VARCHAR2(1) := 'N'; */
    DECLARE v_payableflag SMALLINT;  /* ORIGSQL: v_payableflag NUMBER(1); */
    DECLARE v_reportgroup VARCHAR(127);  /* ORIGSQL: v_reportgroup VARCHAR2(127); */
    DECLARE v_classifierid NVARCHAR(127);  /* ORIGSQL: v_classifierid NVARCHAR2(127); */
    DECLARE v_sortorder VARCHAR(3) = NULL;  /* ORIGSQL: v_sortorder varchar2(3) := null; */
    DECLARE v_month VARCHAR(20) = NULL;  /* ORIGSQL: v_month varchar2(20) := null; */
    DECLARE cEndofTime date;
    
    /* ORIGSQL: for i in(select DISTINCT(Mappedfor) from rpt_common_mapping) Loop CALL DBMTK_USE(...) */
    DECLARE CURSOR dbmtk_cursor_5895
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_COMMON_MAPPING' not found */

        SELECT   /* ORIGSQL: select DISTINCT(Mappedfor) from rpt_common_mapping; */
            DISTINCT 
            (Mappedfor)
        FROM
            ext.rpt_common_mapping;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            vSQLERRM = SUBSTRING(::SQL_ERROR_MESSAGE,1,3900);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 3900) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'ERROR', NULL, vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ERROR', NULL, :vSQLERRM);

            /* ORIGSQL: raise_application_error(-20911,'Error raised: '||vprocname||' Failed: '|| DBMS_U(...) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT = 'Error raised: '||IFNULL(:vProcName,'')||' Failed: '
            --||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE

            || ' - '||IFNULL(:vSQLERRM,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;
        select * into vPeriodRow from cs_period where periodseq = :vperiodseq and removedate > current_date;
        /* initialize session variables, if not yet done */
        CALL EXT.init_session_global();

        --!!!!!!The below truncate and variable initialization will be executed in rpt_reporting_extract.prc_driver. Will remove unnecessary code after initial testing.
        /* ORIGSQL: prc_logevent (NULL, vProcName, 'Begin procedure', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(NULL, :vProcName, 'Begin procedure', NULL, :vSQLERRM);
        --------Add subpartitions to report table if needed----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AddTableSubpartition (vExtUser, vTCTemplateTable, v(...) */
       /* CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AddTableSubpartition(
                :vExtUser,
                :vTCTemplateTable,
                :vTCSchemaName,
                :vTenantId,
                :vprocessingunitseq,
                :vperiodseq,
                :vRptTableName
            ); */--Sanjay:Commenting out as partitions are not required

        --------Find subpartition name------------------------------------------------------------------------------------

        --vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);--Sanjay: commenting as subpartition are not required  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as AnalyzeTableSubpartition are not required

        --------Truncate report table subpartition------------------------------------------------------------------------
        --  pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName,
        --                                                      vSubpartitionName);
        --Commenting Since Deleting the records using  frequency
        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay:commenting as analyze is not required

        --------Turn on Parallel DML---------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */

        --------Initialize variables---------------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (NULL, vProcName, 'Setting up variables', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(NULL, :vProcName, 'Setting up variables', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

        SELECT
            per.*
        INTO
            vPeriodRow
        FROM
            cs_period per
        WHERE
            per.removedate = :cEndofTime  /* ORIGSQL: pkg_reporting_extract_r2.cEndofTime */
            AND per.periodseq = :vperiodseq;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

        SELECT
            pu.*
        INTO
            vProcessingUnitRow
        FROM
            cs_processingunit pu
        WHERE
            pu.processingunitseq = :vprocessingunitseq;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CALENDAR' not found */

        SELECT
            cal.*
        INTO
            vCalendarRow
        FROM
            cs_calendar cal
        WHERE
            cal.removedate = :cEndofTime  /* ORIGSQL: pkg_reporting_extract_r2.cEndofTime */
            AND cal.calendarseq = :vcalendarseq;

        --------Begin Insert-------------------------------------------------------------------------------
        -----DELETE EXISTING RECORDS BASED ON FREQUENCY 

        /* ORIGSQL: DELETE FROM RPT_COMM_SAA_SUMMARY WHERE periodseq=vperiodseq and processingunitse(...) */
        DELETE
        FROM
            EXT.RPT_COMM_SAA_SUMMARY
        WHERE
            periodseq = :vperiodseq
            AND processingunitseq = :vprocessingunitseq
            AND frequency = :vfrequency;

        /* ORIGSQL: commit; */
        COMMIT;

        SELECT
            substring(:vPeriodRow.name,0,(LENGTH(:vPeriodRow.name) -5))  /* ORIGSQL: substr(vPeriodRow.name,0,(length(vPeriodRow.name)-5)) */
        INTO
            v_month
        FROM
            SYS.DUMMY;  /* ORIGSQL: FROM dual ; */

        IF :v_month = 'April' 
        THEN
            v_sortorder = '1';
        ELSEIF :v_month = 'May'   /* ORIGSQL: elsif v_month = 'May' then */
        THEN
            v_sortorder = '2';
        ELSEIF :v_month = 'June'   /* ORIGSQL: elsif v_month = 'June' then */
        THEN
            v_sortorder = '3';
        ELSEIF :v_month = 'July'   /* ORIGSQL: elsif v_month = 'July' then */
        THEN
            v_sortorder = '4';
        ELSEIF :v_month = 'August'   /* ORIGSQL: elsif v_month = 'August' then */
        THEN
            v_sortorder = '5';
        ELSEIF :v_month = 'September'   /* ORIGSQL: elsif v_month = 'September' then */
        THEN
            v_sortorder = '6';
        ELSEIF :v_month = 'October'   /* ORIGSQL: elsif v_month = 'October' then */
        THEN
            v_sortorder = '7';
        ELSEIF :v_month = 'November'   /* ORIGSQL: elsif v_month = 'November' then */
        THEN
            v_sortorder = '8';
        ELSEIF :v_month = 'December'   /* ORIGSQL: elsif v_month = 'December' then */
        THEN
            v_sortorder = '9';
        ELSEIF :v_month = 'January'   /* ORIGSQL: elsif v_month = 'January' then */
        THEN
            v_sortorder = '10';
        ELSEIF :v_month = 'February'   /* ORIGSQL: elsif v_month = 'February' then */
        THEN
            v_sortorder = '11';
        ELSEIF :v_month = 'March'   /* ORIGSQL: elsif v_month = 'March' then */
        THEN
            v_sortorder = '12';
        END IF;

        FOR i AS dbmtk_cursor_5895
        DO
            --Months
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-12 Months', NULL, 'Month');
            --defectid 1179 included column value 2 to fix this defect

            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, OTC, SHOP,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''01'' sectionid,
            ''DETAIL'' sectionname,
            ''' ||IFNULL(:v_sortorder,'')||''' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            NULL OTC,
            pad.POSITIONGA10 SHOP,
            NULL PRONOOFMONTH,--need to check with babu
            ''' ||IFNULL(:vPeriodRow.name,'')||''' COLUMNDESC,
            case when COLUMNVALUE1=0 then COLUMNVALUE2 else COLUMNVALUE1 end as COLUMNVALUE,
            ''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                MAX(case when rmap.rptcolumnname = ''MONTH'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'MONTH', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE1,
                MAX(case when rmap.rptcolumnname = ''MONTH1'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'MONTH1', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE2
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''
            and pad.positiontitle not in (''Retail SER - Biz Dev Mgr'')';  /* ORIGSQL: fun_common_mapping(vrptname,'MONTH1',i.Mappedfor,vfrequency) */
                                                                           /* ORIGSQL: fun_common_mapping(vrptname,'MONTH',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-Months Completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            -- sudhir
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-12 Months for Retail SER - Biz Dev Mgr', NULL, 'Month');
            --defectid 1179 included column value 2 to fix this defect

            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, OTC, SHOP,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''01'' sectionid,
            ''DETAIL'' sectionname,
            ''' ||IFNULL(:v_sortorder,'')||''' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            NULL OTC,
            pad.POSITIONGA10 SHOP,
            NULL PRONOOFMONTH,--need to check with babu
            ''' ||IFNULL(:vPeriodRow.name,'')||''' COLUMNDESC,
            COLUMNVALUE1 COLUMNVALUE,
            ''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                MAX(case when rmap.rptcolumnname = ''MONTH_RTLBZDMGR'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'MONTH_RTLBZDMGR', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE1
                FROM EXT.RPT_BASE_MEASUREMENT CM,rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''
            and pad.positiontitle in (''Retail SER - Biz Dev Mgr'')';  /* ORIGSQL: fun_common_mapping(vrptname,'MONTH_RTLBZDMGR',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-Months Completed for Retail SER - Biz Dev Mgr', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --SUM
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-SUM', NULL, 'SUM');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''20'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''SUM %'' COLUMNDESC,
            COLUMNVALUE,
            ''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                SUM(case when rmap.rptcolumnname = ''SUM'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'SUM', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';  /* ORIGSQL: fun_common_mapping(vrptname,'SUM',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-SUM Completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --Average Before Proration
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-Average Before Proration', NULL, 'Average Before Proration');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''21'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''Average Before Proration'' COLUMNDESC,
            COLUMNVALUE,
            ''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                SUM(case when rmap.rptcolumnname = ''AVGBEFOREPRO'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'AVGBEFOREPRO', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';  /* ORIGSQL: fun_common_mapping(vrptname,'AVGBEFOREPRO',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-Average Before Proration Completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --New Average after Proration
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-New Average Before Proration', NULL, 'New Average Before Proration');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''22'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''New Average after Proration'' COLUMNDESC,
            COLUMNVALUE,
            ''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                SUM(case when rmap.rptcolumnname = ''NEWAVGPRO'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'NEWAVGPRO', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';  /* ORIGSQL: fun_common_mapping(vrptname,'NEWAVGPRO',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||''''

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-New Average Before Proration Completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --Total Workday
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-Total Workday', NULL, 'Total Workday');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''23'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''Total Workday'' COLUMNDESC,
            COLUMNVALUE,''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                SUM(case when rmap.rptcolumnname = ''TOTALWORKDAYS'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'TOTALWORKDAYS', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';  /* ORIGSQL: fun_common_mapping(vrptname,'TOTALWORKDAYS',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-Total Workday', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --Prorated Workday

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-Prorated Workday', NULL, 'Prorated Workday');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''24'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''Prorated Workday'' COLUMNDESC,
            COLUMNVALUE,
            ''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                SUM(case when rmap.rptcolumnname = ''PRORATEDWORKDAY'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'PRORATEDWORKDAY', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';  /* ORIGSQL: fun_common_mapping(vrptname,'PRORATEDWORKDAY',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-Prorated Workday', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --Proration Reason (HL/ML/ICT/NPL)  --need to change
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-Proration Reason', NULL, 'Proration Reason');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''25'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''Proration Reason (HL/ML/ICT/NPL)'' COLUMNDESC,
            COLUMNVALUE,''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                NULL COLUMNVALUE --need to change
                FROM EXT.RPT_BASE_MEASUREMENT CM,rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND rmap.rulename LIKE ''%Internal%''
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
                
                UNION
                
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                NULL COLUMNVALUE --need to change
                FROM EXT.RPT_BASE_MEASUREMENT CM,rpt_common_mapping rmap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                AND CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                AND rmap.rulename LIKE ''%BSC%''
                AND CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Measurement(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Measurement-Proration Reason', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --MTH --need to check for MTH1 or MTH2 in mapping
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Incen(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Incentive-MTH', NULL, 'MTH');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''26'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''MTH'' COLUMNDESC,
            (CASE WHEN mes.MAPPEDFOR=''Internal'' and pad.positiontitle like ''%Direct%Sales%'' then MTH1
                WHEN mes.MAPPEDFOR=''Internal'' and  pad.positiontitle Not like ''%Direct%Sales%'' then MTH2
                WHEN mes.MAPPEDFOR=''BSC'' and pad.POSITIONGA15 like ''MM'' then MTH3
                WHEN mes.MAPPEDFOR=''BSC'' and  pad.POSITIONGA15 Not like ''MM'' then MTH4
                END
                )COLUMNVALUE ,''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq,
                rmap.MAPPEDFOR,
                SUM(case when rmap.rptcolumnname = ''MTH1'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'MTH1', :i.Mappedfor, :vfrequency),'') ||' end) AS MTH1,
                SUM(case when rmap.rptcolumnname = ''MTH2'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'MTH2', :i.Mappedfor, :vfrequency),'') ||' end) AS MTH2,
                SUM(case when rmap.rptcolumnname = ''MTH3'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'MTH3', :i.Mappedfor, :vfrequency),'') ||' end) AS MTH3,
                SUM(case when rmap.rptcolumnname = ''MTH4'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'MTH4', :i.Mappedfor, :vfrequency),'') ||' end) AS MTH4
                FROM ext.rpt_base_incentive inc,ext.rpt_common_mapping rmap
                where inc.processingunitseq = ' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and  inc.name in rmap.rulename
                and rmap.reportname =  '''||IFNULL(:vrptname,'')||'''
                and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
                and rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq,
                rmap.MAPPEDFOR
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';  /* ORIGSQL: fun_common_mapping(vrptname,'MTH4',i.Mappedfor,vfrequency) */
                                                                     /* ORIGSQL: fun_common_mapping(vrptname,'MTH3',i.Mappedfor,vfrequency) */
                                                                     /* ORIGSQL: fun_common_mapping(vrptname,'MTH2',i.Mappedfor,vfrequency) */
                                                                     /* ORIGSQL: fun_common_mapping(vrptname,'MTH1',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Incentive-M(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Incentive-MTH', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --SAA
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Incen(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Incentive-SAA', NULL, 'SAA');
            v_sql = 'INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ,
                PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID,
                SECTIONNAME, SORTORDER, EMPFIRSTNAME, EMPLASTNAME, TITLENAME,
                POSITIONNAME, ANCESTOREUSERID, GEID, SALESMANCODE, NAME,
                JOBTITLE, GRADE, DATEJOINED, SHOP, OTC,
                PRONOOFMONTH,COLUMNDESC,COLUMNVALUE,FREQUENCY
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''78'' reportcode,
            ''02'' sectionid,
            ''DETAIL1'' sectionname,
            ''27'' sortorder,
            pad.firstname empfirstname,
            pad.lastname emplastname,
            pad.reporttitle titlename,
            pad.POSITIONNAME POSITIONNAME,
            pad.userid||''_''||''' ||IFNULL(:vTenantId,'')||''' ANCESTOREUSERID,
            pad.positionname GEID,
            pad.PARTICIPANTGA1 SALESMANCODE,
            pad.lastname NAME,
            pad.positiontitle JOBTITLE,
            pad.POSITIONGA4 GRADE,
            pad.PARTICIPANTHIREDATE DATEJOINED,
            pad.POSITIONGA10 SHOP,--need to change
            NULL OTC,
            NULL PRONOOFMONTH,--need to check with babu
            ''SAA'' COLUMNDESC,
            COLUMNVALUE ,
            ''' ||IFNULL(:vfrequency,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq,
                SUM(case when rmap.rptcolumnname = ''SAA'' then ' ||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'SAA', :i.Mappedfor, :vfrequency),'') ||' end) AS COLUMNVALUE
                FROM ext.rpt_base_incentive inc, ext.rpt_common_mapping rmap
                where inc.processingunitseq = ' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and  inc.name in rmap.rulename
                and rmap.reportname =  '''||IFNULL(:vrptname,'')||'''
                and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
                and rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                AND rmap.frequency ='''|| IFNULL(:vfrequency,'')||'''
                GROUP BY inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.frequency =''' || IFNULL(:vfrequency,'')||'''';  /* ORIGSQL: fun_common_mapping(vrptname,'SAA',i.Mappedfor,vfrequency) */

            --and pad.reportgroup = '''||v_reportgroup||'''';

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Incentive-S(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Incentive-SAA', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            --SAA remarks

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Measu(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Measurement-Proration Reason', NULL, 'Proration Reason');

            /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_COMM_SAA_SUMMARY' not found */
            /* ORIGSQL: INSERT INTO STELEXT.RPT_COMM_SAA_SUMMARY (TENANTID, POSITIONSEQ, PAYEESEQ, PROCE(...) */
            INSERT INTO EXT.RPT_COMM_SAA_SUMMARY
                (
                    TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                    PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                    EMPFIRSTNAME, EMPLASTNAME, TITLENAME, POSITIONNAME, ANCESTOREUSERID, GEID,
                    SALESMANCODE, NAME, JOBTITLE, GRADE, DATEJOINED, SHOP,
                    OTC, PRONOOFMONTH, COLUMNDESC, COLUMNVALUE, FREQUENCY
                )
                SELECT   /* ORIGSQL: SELECT vTenantID, saa.positionseq, saa.payeeseq, vProcessingUnitRow.processingun(...) */
                    :vTenantId,
                    saa.positionseq,
                    saa.payeeseq,
                    :vProcessingUnitRow.processingunitseq,
                    :vperiodseq,
                    :vPeriodRow.name,
                    :vProcessingUnitRow.name,
                    :vCalendarRow.name,
                    '78' AS reportcode,
                    '02' AS sectionid,
                    'DETAIL1' AS sectionname,
                    '28' AS sortorder,
                    saa.empfirstname,
                    saa.emplastname,
                    saa.titlename,
                    saa.POSITIONNAME AS POSITIONNAME,
                    saa.ANCESTOREUSERID,
                    saa.GEID,
                    saa.SALESMANCODE,
                    saa.NAME,
                    saa.JOBTITLE,
                    saa.GRADE,
                    saa.DATEJOINED,
                    saa.SHOP,/* --need to change */ NULL AS OTC,
                    NULL AS PRONOOFMONTH,/* --need to check with babu */   'SAA remarks' AS COLUMNDESC,
                    NULL AS COLUMNVALUE,
                    :vfrequency AS FREQUENCY
                FROM
                    (
                        SELECT   /* ORIGSQL: (SELECT distinct positionseq,payeeseq,empfirstname,emplastname,titlename,POSITIO(...) */
                            DISTINCT
                            positionseq,
                            payeeseq,
                            empfirstname,
                            emplastname,
                            titlename,
                            POSITIONNAME,
                            ANCESTOREUSERID,
                            GEID,
                            SALESMANCODE,
                            NAME,
                            JOBTITLE,
                            GRADE,
                            DATEJOINED,
                            SHOP,
                            OTC
                        FROM
                            EXT.RPT_COMM_SAA_SUMMARY
                        WHERE
                            processingunitseq = :vprocessingunitseq
                            AND periodseq = :vperiodseq
                    ) AS SAA;

            --Incentive OTC
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal SAA Summary Report Incen(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal SAA Summary Report Incentive', NULL, 'Incentive');
            v_sql = 'MERGE INTO EXT.RPT_COMM_SAA_SUMMARY rpt using
            (select inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq,
                '''||IFNULL(:i.Mappedfor,'')||''' mappedfor,
                SUM(case when rmap.rptcolumnname = ''OTC'' then '||IFNULL(EXT.FUN_COMMON_MAPPING(:vrptname, 'OTC', :i.Mappedfor, :vfrequency),'') ||' end) AS OTC
                from cs_incentive inc, ext.rpt_common_mapping rmap
                where inc.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and  inc.name in rmap.rulename
                and rmap.reportname =  '''||IFNULL(:vrptname,'')||'''
                and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
                and rmap.frequency = '''||IFNULL(:vfrequency,'')||'''
                and rmap.mappedfor='''||IFNULL(:i.Mappedfor,'')||'''
                GROUP BY inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
            inc.periodseq)qtr
            on (rpt.processingunitseq = qtr.processingunitseq(+)
                and rpt.periodseq = qtr.periodseq(+)
                and rpt.positionseq = qtr.positionseq(+)
                and rpt.payeeseq = qtr.payeeseq (+)
                and qtr.mappedfor = '''||IFNULL(:i.Mappedfor,'')||'''
                and rpt.frequency='''||IFNULL(:vfrequency,'')||''')
            when matched then update set rpt.OTC=qtr.OTC';  /* ORIGSQL: fun_common_mapping(vrptname,'OTC',i.Mappedfor,vfrequency) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal SAA Summary Report Incentive c(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal SAA Summary Report Incentive completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_COMM_SAA_SUMMARY' not found */
        /* ORIGSQL: update RPT_COMM_SAA_SUMMARY SET PRONOOFMONTH = (CASE WHEN TO_DATE(to_char(DATEJO(...) */
        UPDATE EXT.RPT_COMM_SAA_SUMMARY
            SET
            /* ORIGSQL: PRONOOFMONTH = */
            PRONOOFMONTH = (
                CASE 
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '01JAN2200' AND '15JAN2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '12'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16JAN2200' AND '15FEB2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '11'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16FEB2200' AND '15MAR2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '10'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16MAR2200' AND '15APR2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '9'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16APR2200' AND '15MAY2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '8'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16MAY2200' AND '15JUN2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '7'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16JUN2200' AND '15JUL2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '6'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16JUL2200' AND '15AUG2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '5'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16AUG2200' AND '15SEP2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '4'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16SEP2200' AND '15OCT2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '3'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16OCT2200' AND '15NOV2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '2'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16NOV2200' AND '15DEC2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '1'
                    WHEN TO_DATE(IFNULL(TO_VARCHAR(DATEJOINED,'DDMON'),'') ||'2200','DDMONYYYY') BETWEEN '16DEC2200' AND '31DEC2200'   /* ORIGSQL: to_char(DATEJOINED,'DDMON') */
                    THEN '0'
                    ELSE '0'
                END
            )
        FROM
            EXT.RPT_COMM_SAA_SUMMARY
        WHERE
            processingunitseq = :vprocessingunitseq
            AND periodseq = :vperiodseq
            AND datejoined IS NOT NULL;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --------End Insert---------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Report table insert complete', NULL, (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Report table insert complete', NULL, :vSQLERRM);

        --------Gather stats-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Start DBMS_STATS', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Start DBMS_STATS', NULL, :vSQLERRM);

        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTable (vrpttablename) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);Sanjay:commenting as analyze is not required

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END