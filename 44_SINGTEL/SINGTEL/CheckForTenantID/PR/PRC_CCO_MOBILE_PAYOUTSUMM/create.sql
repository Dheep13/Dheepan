CREATE PROCEDURE EXT.PRC_CCO_MOBILE_PAYOUTSUMM
(
    --IN vrptname rpt_cco_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_cco_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                                   /* RESOLVE: Datatype unresolved: Datatype (rpt_cco_mapping.reportname%TYPE) not resolved for parameter 'PRC_CCO_MOBILE_PAYOUTSUMM.vrptname' */
                                                   /* ORIGSQL: vrptname IN rpt_cco_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_CCO_MOBILE_PAYOUTSUMM.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_CCO_MOBILE_PAYOUTSUMM.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_CCO_MOBILE_PAYOUTSUMM.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();
   --DECLARE PKG_REPORTING_EXTRACT_R2__vcredittypeid_PayAdj VARCHAR(255); /* package/session variable */

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
    -- 01-Dec-2017      Tharanikumar  Initial release
    -------------------------------------------------------------------------------------------------------------------
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_CCO_MOBILE_PAYOUTSUMM');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_CCO_MOBILE_PAYOUTSUMM') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_CCO_MOBILE_PAYOUTSUMM';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_CCO_MOBILE_PAYOUTSUMM'; */
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
    DECLARE vcredittypeid_PayAdj NVARCHAR(50);
    DECLARE cEndofTime date;
   

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            vSQLERRM = SUBSTRING(::SQL_ERROR_MESSAGE,1,3900);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 3900) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'ERROR', NULL, vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ERROR', NULL, :vSQLERRM);

            /* ORIGSQL: raise_application_error(-20911,'Error raised: '||vprocname||' Failed: '|| DBMS_U(...) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT = 'Error raised: '||IFNULL(:vProcName,'')||' Failed: '
            --||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE : Sanjay: Need to check equivelant for HANA.

            || ' - '||IFNULL(:vSQLERRM,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;
        select * into vPeriodRow from cs_period where periodseq = :vperiodseq and removedate > current_date; 
        /* initialize session variables, if not yet done */
        CALL EXT.init_session_global(); -- Sanjay need to set the seeion context.
        /* retrieve the package/session variables referenced in this procedure */
        SELECT SESSION_CONTEXT('VCREDITTYPEID_PAYADJ') INTO vcredittypeid_PayAdj FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('cEndofTime') INTO cEndofTime FROM SYS.DUMMY ;
        
        /* end of package/session variables */

        --!!!!!!The below truncate and variable initialization will be executed in rpt_reporting_extract.prc_driver. Will remove unnecessary code after initial testing.
        /* ORIGSQL: prc_logevent (NULL, vProcName, 'Begin procedure', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(NULL, :vProcName, 'Begin procedure', NULL, :vSQLERRM);
        --------Add subpartitions to report table if needed----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AddTableSubpartition (vExtUser, vTCTemplateTable, v(...) */
        /*CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AddTableSubpartition(
                :vExtUser,
                :vTCTemplateTable,
                :vTCSchemaName,
                :vTenantId,
                :vprocessingunitseq,
                :vperiodseq,
                :vRptTableName
            );*/ --Sanjay Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

        --vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName); --Sanjay: commenting as subpartition are not required/* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName); --Sanjay: commenting as AnalyzeTableSubpartition are not required

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName); Sanjay:commenting as truncateTableSubpartition are not required

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName); Sanjay:commenting as analyze is not required

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

        /*
           SELECT   per.startdate, per.enddate - 1
             INTO   vCurYrStartDate, vCurYrEndDate
             FROM   cs_period per
            WHERE   per.periodSeq =
                       (    SELECT   per1.periodseq
                                  FROM   cs_period per1, cs_periodtype pt1
                                 WHERE   per1.PeriodTypeseq = pt1.PeriodTypeseq
             AND pt1.Name = 'year'
                            START WITH   per1.periodseq = vperiodseq
                        CONNECT BY   PRIOR per1.parentseq = per1.periodseq);
        */

        --------Begin Insert-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert Team Member',NULL,vsqlerrm(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert Team Member', NULL, :vSQLERRM);
        v_sql = 'INSERT INTO ext.RPT_CCO_MOBILE_PAYOUTSUMM
        (tenantid,
            positionseq,
            managerseq,
            payeeseq,
            processingunitseq,
            periodseq,
            periodname,
            processingunitname,
            calendarname,
            reportcode,
            sectionid,
            sectionname,
            sortorder,
            empfirstname,
            emplastname,
            titlename,
            periodstartdate,
            periodenddate,
            loaddttm,
            ID,
            CCONAME,
            TEAMFULLNAME,
            GEID,
            AGENCY,
            BASKET1ACTUALCONN,
            BASKET2ACTUALCONN,
            BASKET3ACTUALCONN,
            BASKET4ACTUALCONN,
            TOTALACTUALCONN,
            BASKET1ACTUALPAY,
            BASKET2ACTUALPAY,
            BASKET3ACTUALPAY,
            BASKET4ACTUALPAY,
            TOTALACTUALPAY,
            TARGET,
            ACHIEVEMENT,
            FTENO,
            TEAMLEADFULLNAME
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.managerseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''57'' reportcode,
        ''01'' sectionid,
        ''DETAIL'' sectionname,
        ''01'' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.startdate),'')||''',''dd-mon-yyyy hh:mi:ss''),
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.enddate),'')||''',''dd-mon-yyyy hh:mi:ss''),
        SYSDATE,
        NULL ID,
        pad.FULLNAME CCONAME,
        pad.POSITIONGA1 TEAMFULLNAME,
        pad.PARTICIPANTID GEID,
        pad.POSITIONGA9 AGENCY,
        BASKET1ACTUALCONN,
        BASKET2ACTUALCONN,
        BASKET3ACTUALCONN BASKET3ACTUALCONN,
        BASKET4ACTUALCONN BASKET4ACTUALCONN,
        TOTALACTUALCONN,
        BASKET1ACTUALPAY,
        BASKET2ACTUALPAY,
        BASKET3ACTUALPAY BASKET3ACTUALPAY,
        BASKET4ACTUALPAY BASKET4ACTUALPAY,
        (nvl(BASKET1ACTUALPAY,0)
            + nvl(BASKET2ACTUALPAY,0)
            + nvl(BASKET3ACTUALPAY,0)
            + nvl(BASKET4ACTUALPAY,0)
        ) TOTALACTUALPAY,
        TARGET,
        ACHIEVEMENT*100,
        null FTENO,
        (select pad1.LASTNAME from rpt_base_padimension pad1 where pad1.positionseq =pad.managerseq)
        FROM   rpt_base_padimension pad,
        (
            select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            max(case when rmap.rptcolumnname = ''BASKET1ACTUALCONN'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET1ACTUALCONN'),'') ||' end) BASKET1ACTUALCONN,
            max(case when rmap.rptcolumnname = ''BASKET2ACTUALCONN'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET2ACTUALCONN'),'') ||' end) BASKET2ACTUALCONN,
            max(case when rmap.rptcolumnname = ''BASKET3ACTUALCONN'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET3ACTUALCONN'),'') ||' end) BASKET3ACTUALCONN,
            max(case when rmap.rptcolumnname = ''BASKET4ACTUALCONN'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET4ACTUALCONN'),'') ||' end) BASKET4ACTUALCONN,
            max(case when rmap.rptcolumnname = ''TOTALACTUALCONN'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'TOTALACTUALCONN'),'') ||' end) TOTALACTUALCONN,
            max(case when rmap.rptcolumnname = ''BASKET1ACTUALPAY'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET1ACTUALPAY'),'') ||' end) BASKET1ACTUALPAY,
            max(case when rmap.rptcolumnname = ''BASKET2ACTUALPAY'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET2ACTUALPAY'),'') ||' end) BASKET2ACTUALPAY,
            max(case when rmap.rptcolumnname = ''BASKET3ACTUALPAY'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET3ACTUALPAY'),'') ||' end) BASKET3ACTUALPAY,
            max(case when rmap.rptcolumnname = ''BASKET4ACTUALPAY'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'BASKET4ACTUALPAY'),'') ||' end) BASKET4ACTUALPAY,
            max(case when rmap.rptcolumnname = ''TARGET'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'TARGET'),'') ||' end) TARGET,
            max(case when rmap.rptcolumnname = ''ACHIEVEMENT'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'ACHIEVEMENT'),'') ||' end) ACHIEVEMENT
            from rpt_base_measurement mes, rpt_cco_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq
        )mes
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.reportgroup = ''CCO MOBILE TM''';  /* ORIGSQL: funccogenericattribute(vrptname,'TOTALACTUALCONN') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'TARGET') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET4ACTUALPAY') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET4ACTUALCONN') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET3ACTUALPAY') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET3ACTUALCONN') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET2ACTUALPAY') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET2ACTUALCONN') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET1ACTUALPAY') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'BASKET1ACTUALCONN') */
                                                   /* ORIGSQL: funccogenericattribute(vrptname,'ACHIEVEMENT') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Inser Team Member completed',NULL(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Inser Team Member completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        v_sql = 'MERGE INTO RPT_CCO_MOBILE_PAYOUTSUMM rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            max(case when rmap.rptcolumnname = ''FTENO'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'FTENO'),'') ||' end) FTENO
            from rpt_base_credit mes, RPT_CCO_MAPPING rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and rmap.rptcolumnname = ''FTENO''
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
        mes.periodseq)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.sectionname = ''DETAIL''
        )
        when matched then update set rpt.FTENO=qtr.FTENO';  /* ORIGSQL: funccogenericattribute(vrptname,'FTENO') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Merge for FTENO completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Merge for FTENO completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert Team Lead',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert Team Lead', NULL, :vSQLERRM);
        v_sql = 'INSERT INTO ext.RPT_CCO_MOBILE_PAYOUTSUMM
        (tenantid,
            positionseq,
            managerseq,
            payeeseq,
            processingunitseq,
            periodseq,
            periodname,
            processingunitname,
            calendarname,
            reportcode,
            sectionid,
            sectionname,
            sortorder,
            empfirstname,
            emplastname,
            titlename,
            periodstartdate,
            periodenddate,
            loaddttm,
            TEAMLEADFULLNAME,
            TLGEID,
            TLAGENCY,
            TOTALTAKEUPS,
            TLINCENTIVEPAY,
            TLTARGET,
            TEAMTARGETACHIEVED,
            FTE,
            FINALTLINCENTIVE,
            TLREMARKS,
            TEAMFULLNAME
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.managerseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''57'' reportcode,
        ''99'' sectionid,
        ''DETAIL'' sectionname,
        ''99'' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.startdate),'')||''',''dd-mon-yyyy hh:mi:ss''),
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.enddate),'')||''',''dd-mon-yyyy hh:mi:ss''),
        SYSDATE,
        tmtol.FULLNAME TEAMLEADFULLNAME,
        pad.PARTICIPANTID TLGEID,
        pad.POSITIONGA9 TLAGENCY,
        mes.TOTALTAKEUPS,
        mes.TLINCENTIVEPAY,
        mes.TLTARGET,
        (mes.TEAMTARGETACHIEVED*100) TEAMTARGETACHIEVED,
        mes.FTE,
        mes.FINALTLINCENTIVE,
        NULL TLREMARKS,
        pad.POSITIONGA1 TEAMFULLNAME
        FROM   rpt_base_padimension pad,
        (
            select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            max(case when rmap.rptcolumnname = ''TOTALTAKEUPS'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'TOTALTAKEUPS'),'') ||' end) TOTALTAKEUPS,
            max(case when rmap.rptcolumnname = ''TLINCENTIVEPAY'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'TLINCENTIVEPAY'),'') ||' end) TLINCENTIVEPAY,
            max(case when rmap.rptcolumnname = ''TLTARGET'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'TLTARGET'),'') ||' end) TLTARGET,
            max(case when rmap.rptcolumnname = ''TEAMTARGETACHIEVED'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'TEAMTARGETACHIEVED'),'') ||' end) TEAMTARGETACHIEVED,
            max(case when rmap.rptcolumnname = ''FTE'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'FTE'),'') ||' end) FTE,
            max(case when rmap.rptcolumnname = ''FINALTLINCENTIVE'' then ' ||IFNULL(EXT.FUNCCOGENERICATTRIBUTE(:vrptname, 'FINALTLINCENTIVE'),'') ||' end) FINALTLINCENTIVE
            from rpt_base_measurement mes, rpt_cco_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq
        )mes,
        (
            select
            processingunitseq,
            periodseq,
            managerseq,
            TEAMLEADFULLNAME FULLNAME,
            count(distinct payeeseq) ID,
            sum(BASKET1ACTUALCONN) BASKET1ACTUALCONN,
            sum(BASKET2ACTUALCONN) BASKET2ACTUALCONN,
            sum(BASKET3ACTUALCONN) BASKET3ACTUALCONN,
            sum(BASKET4ACTUALCONN) BASKET4ACTUALCONN,
            sum(TOTALACTUALCONN) TOTALACTUALCONN,
            sum(BASKET1ACTUALPAY) BASKET1ACTUALPAY,
            sum(BASKET2ACTUALPAY) BASKET2ACTUALPAY,
            sum(BASKET3ACTUALPAY) BASKET3ACTUALPAY,
            sum(BASKET4ACTUALPAY) BASKET4ACTUALPAY,
            sum(TOTALACTUALPAY) TOTALACTUALPAY,
            sum(ADJUSTMENT) ADJUSTMENT,
            sum(FINALACTUALPAY) FINALACTUALPAY
            from ext.RPT_CCO_MOBILE_PAYOUTSUMM tab
            where tab.processingunitseq = ''' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'''
            and tab.periodseq = '''||IFNULL(:vperiodseq,'')||'''
            and tab.sectionname = ''DETAIL''
            and tab.sectionid = ''01''
            group by
            processingunitseq,
            periodseq,
            managerseq,
            TEAMLEADFULLNAME
        )tmtol
        
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        AND pad.periodseq = mes.periodseq
        AND mes.processingunitseq = tmtol.processingunitseq(+)
        AND mes.periodseq = tmtol.periodseq(+)
        AND pad.positionseq = tmtol.managerseq
        AND pad.reportgroup = ''CCO MOBILE TL''
        ';  /* ORIGSQL: funccogenericattribute(vrptname,'TOTALTAKEUPS') */
            /* ORIGSQL: funccogenericattribute(vrptname,'TLTARGET') */
            /* ORIGSQL: funccogenericattribute(vrptname,'TLINCENTIVEPAY') */
            /* ORIGSQL: funccogenericattribute(vrptname,'TEAMTARGETACHIEVED') */
            /* ORIGSQL: funccogenericattribute(vrptname,'FTE') */
            /* ORIGSQL: funccogenericattribute(vrptname,'FINALTLINCENTIVE') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Insert Team Lead completed',NULL,(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Insert Team Lead completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_CCO_MOBILE_PAYOUTSUMM' not found */

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_CCO_MOBILE_PAYOUTSUMM rpt using (SELECT mes.positionseq, mes.paye(...) */
        MERGE INTO RPT_CCO_MOBILE_PAYOUTSUMM AS rpt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_CREDIT' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, MAX(...) */
                    mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    MAX(mes.genericattribute3) AS Remarks,
                    SUM(mes.value) AS AdjAmt
                FROM
                    rpt_base_credit mes
                WHERE
                    mes.processingunitseq = :vprocessingunitseq
                    AND mes.periodseq = :vperiodseq
                    AND mes.credittypeid = :vcredittypeid_PayAdj  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_PayAdj */
                GROUP BY
                    mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionid = '01')
        WHEN MATCHED THEN
            UPDATE SET rpt.remarks = qtr.remarks, rpt.adjustment = qtr.AdjAmt;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_CCO_MOBILE_PAYOUTSUMM rpt using (SELECT tab.positionseq, tab.paye(...) */
        MERGE INTO RPT_CCO_MOBILE_PAYOUTSUMM AS rpt
            /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_CCO_MOBILE_PAYOUTSUMM' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select tab.positionseq, tab.payeeseq, tab.processingunitseq, tab.periodseq, NVL(...) */
                    tab.positionseq,
                    tab.payeeseq,
                    tab.processingunitseq,
                    tab.periodseq,
                    IFNULL(ADJUSTMENT,0) + IFNULL(TOTALACTUALPAY,0) AS FINALACTUALPAY  /* ORIGSQL: NVL(TOTALACTUALPAY,0) */
                                                                                       /* ORIGSQL: NVL(ADJUSTMENT,0) */
                FROM
                    ext.RPT_CCO_MOBILE_PAYOUTSUMM tab
                WHERE
                    tab.processingunitseq = :vprocessingunitseq
                    AND tab.periodseq = :vperiodseq
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionid = '01')
        WHEN MATCHED THEN
            UPDATE SET rpt.FINALACTUALPAY = qtr.FINALACTUALPAY;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TOTAL insert',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TOTAL insert', NULL, NULL);  

        /* ORIGSQL: INSERT INTO stelext.RPT_CCO_MOBILE_PAYOUTSUMM (tenantid, positionseq, payeeseq, (...) */
        INSERT INTO ext.RPT_CCO_MOBILE_PAYOUTSUMM
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                ID, BASKET1ACTUALCONN, BASKET2ACTUALCONN, BASKET3ACTUALCONN, BASKET4ACTUALCONN, TOTALACTUALCONN,
                BASKET1ACTUALPAY, BASKET2ACTUALPAY, BASKET3ACTUALPAY, BASKET4ACTUALPAY, TOTALACTUALPAY, ADJUSTMENT,
                FINALACTUALPAY, TOTALTAKEUPS, TLINCENTIVEPAY, TLTARGET, FTE, FINALTLINCENTIVE
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, null, null, vProcessingUnitRow.processingunitseq, vperiodseq, (...) */
                :vTenantId,
                NULL,
                NULL,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '57' AS reportcode,
                '99' AS sectionid,
                'TOTAL FOR CCO' AS sectionname,
                '2' AS sortorder,
                NULL AS empfirstname,
                NULL AS emplastname,
                NULL AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                ID,
                BASKET1ACTUALCONN,
                BASKET2ACTUALCONN,
                BASKET3ACTUALCONN,
                BASKET4ACTUALCONN,
                TOTALACTUALCONN,
                BASKET1ACTUALPAY,
                BASKET2ACTUALPAY,
                BASKET3ACTUALPAY,
                BASKET4ACTUALPAY,
                TOTALACTUALPAY,
                ADJUSTMENT,
                FINALACTUALPAY,
                TOTALTAKEUPS,
                TLINCENTIVEPAY,
                TLTARGET,
                FTE,
                FINALTLINCENTIVE
            FROM
                (
                    SELECT   /* ORIGSQL: (select SUM(ID) ID, SUM(BASKET1ACTUALCONN) BASKET1ACTUALCONN, SUM(BASKET2ACTUALC(...) */
                        SUM(ID) AS ID,
                        SUM(BASKET1ACTUALCONN) AS BASKET1ACTUALCONN,
                        SUM(BASKET2ACTUALCONN) AS BASKET2ACTUALCONN,
                        SUM(BASKET3ACTUALCONN) AS BASKET3ACTUALCONN,
                        SUM(BASKET4ACTUALCONN) AS BASKET4ACTUALCONN,
                        SUM(TOTALACTUALCONN) AS TOTALACTUALCONN,
                        SUM(BASKET1ACTUALPAY) AS BASKET1ACTUALPAY,
                        SUM(BASKET2ACTUALPAY) AS BASKET2ACTUALPAY,
                        SUM(BASKET3ACTUALPAY) AS BASKET3ACTUALPAY,
                        SUM(BASKET4ACTUALPAY) AS BASKET4ACTUALPAY,
                        SUM(TOTALACTUALPAY) AS TOTALACTUALPAY,
                        SUM(ADJUSTMENT) AS ADJUSTMENT,
                        SUM(FINALACTUALPAY) AS FINALACTUALPAY,
                        SUM(TOTALTAKEUPS) AS TOTALTAKEUPS,
                        SUM(TLINCENTIVEPAY) AS TLINCENTIVEPAY,
                        SUM(TLTARGET) AS TLTARGET,
                        SUM(FTE) AS FTE,
                        SUM(FINALTLINCENTIVE) AS FINALTLINCENTIVE
                    FROM
                        ext.RPT_CCO_MOBILE_PAYOUTSUMM tab
                    WHERE
                        tab.processingunitseq = :vprocessingunitseq
                        AND tab.periodseq = :vperiodseq
                        AND tab.sectionname = 'DETAIL'
                        AND tab.sectionid = '99'
                ) AS rpt;

        /* ORIGSQL: COMMIT; */
        COMMIT; 

        /* ORIGSQL: INSERT INTO stelext.RPT_CCO_MOBILE_PAYOUTSUMM (tenantid, positionseq, payeeseq, (...) */
        INSERT INTO ext.RPT_CCO_MOBILE_PAYOUTSUMM
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                FINALTLINCENTIVE
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, null, null, vProcessingUnitRow.processingunitseq, vperiodseq, (...) */
                :vTenantId,
                NULL,
                NULL,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '57' AS reportcode,
                '99' AS sectionid,
                'TOTAL FOR TLS AND CCO' AS sectionname,
                '2' AS sortorder,
                NULL AS empfirstname,
                NULL AS emplastname,
                NULL AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                FINALTLINCENTIVE
            FROM
                (
                    SELECT   /* ORIGSQL: (select NVL(FINALACTUALPAY,0) + NVL(FINALTLINCENTIVE,0) FINALTLINCENTIVE from st(...) */
                        IFNULL(FINALACTUALPAY,0) + IFNULL(FINALTLINCENTIVE,0) AS FINALTLINCENTIVE  /* ORIGSQL: NVL(FINALTLINCENTIVE,0) */
                    FROM
                        ext.RPT_CCO_MOBILE_PAYOUTSUMM tab
                    WHERE
                        tab.processingunitseq = :vprocessingunitseq
                        AND tab.periodseq = :vperiodseq
                        AND tab.sectionname = 'TOTAL FOR CCO'
                        AND tab.sectionid = '99'
                ) AS rpt;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        -- Team Location Update  
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_CCO_MOBILE_PAYOUTSUMM rpt using (SELECT processingunitseq, period(...) */
        MERGE INTO RPT_CCO_MOBILE_PAYOUTSUMM AS rpt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select processingunitseq, periodseq, positionseq, payeeseq, decode(pad.position(...) */
                    processingunitseq,
                    periodseq,
                    positionseq,
                    payeeseq,
                    MAP(pad.positionga3, 'KCC', 'Malaysia', 'NCC', 'Malaysia', 'Singapore') AS TEAMLOCATION  /* ORIGSQL: decode(pad.positionga3,'KCC','Malaysia','NCC','Malaysia','Singapore') */
                FROM
                    rpt_base_padimension pad
                WHERE
                    pad.processingunitseq = :vprocessingunitseq
                    AND pad.periodseq = :vperiodseq
                    AND pad.reportgroup IN ('CCO MOBILE TM','CCO MOBILE TL')
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            )
        WHEN MATCHED THEN
            UPDATE SET rpt.TEAMLOCATION = qtr.TEAMLOCATION;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */

        --------End Insert---------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Report table insert complete', NULL, (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Report table insert complete', NULL, :vSQLERRM);

        --------Gather stats-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Start DBMS_STATS', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Start DBMS_STATS', NULL, :vSQLERRM);

        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTable (vrpttablename) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END