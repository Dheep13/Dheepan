CREATE PROCEDURE EXT.PRC_BSC_ADV_COMM_PAY
(
    --IN vrptname rpt_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                               /* RESOLVE: Datatype unresolved: Datatype (rpt_mapping.reportname%TYPE) not resolved for parameter 'PRC_BSC_ADV_COMM_PAY.vrptname' */
                                               /* ORIGSQL: vrptname IN rpt_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_BSC_ADV_COMM_PAY.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_BSC_ADV_COMM_PAY.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_BSC_ADV_COMM_PAY.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT._pkg_reporting_extract_r2__cEndofTime();

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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_BSC_ADV_COMM_PAY');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_BSC_ADV_COMM_PAY') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_ADV_COMM_PAY';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_ADV_COMM_PAY'; */
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
    DECLARE cEndofTime CONSTANT date := to_date('2200-01-01','yyyy-mm-dd');

    
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
          -- CALL EXT.PKG_REPORTING_EXTRACT_R2:init_session_global();/*Deepan : replacing with session variable*/
        
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
            ); */ --Sanjay: Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

        --vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) 
        CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) 
        CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) 
        CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);*/

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
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);
        v_sql = 'INSERT INTO EXT.RPT_ADV_COMM_PAY
        (tenantid,
            positionseq,
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
            id,
            geid,
            salesmancode,
            OTC_QUARTERLY,
            OTC_MONTHLY,
            PAYABLECOMM,
            REMARKS
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''53'' reportcode,
        ''1'' sectionid,
        ''DETAIL'' sectionname,
        ''1'' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.startdate),'')||''',''DD-MON-YY''),
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.enddate),'')||''',''DD-MON-YY''),
        SYSDATE,
        NULL, --ID
        pad.PARTICIPANTID, --GEID
        pad.PARTICIPANTGA1, -- SALESMANCODE
        (nvl(OTCMONTHLY,0)*3) OTC_QUARTERLY, -- OTC_QUARTERLY
        OTCMONTHLY, -- OTC_MONTHLY
        PAYABLECOMM, -- PAYABLECOMM,
        null  -- REMARKS
        FROM   ext.rpt_base_padimension pad,
        (
            
            select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            max(case when rmap.rptcolumnname = ''OTCMONTHLY'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'OTCMONTHLY'),'') ||' end) OTCMONTHLY,
            max(case when rmap.rptcolumnname = ''PAYABLECOMM'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'PAYABLECOMM'),'') ||' end) PAYABLECOMM
            from rpt_base_incentive mes, rpt_mapping rmap
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
        and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'PAYABLECOMM') */
                                         /* ORIGSQL: fungenericattribute(vrptname,'OTCMONTHLY') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /*   v_sql :=  -- Not required mulitpled with 3
           'MERGE INTO RPT_ADV_COMM_PAY rpt using
                            (select mes.positionseq,
                                       mes.payeeseq,
                                       mes.processingunitseq,
                                       mes.periodseq,
                                       sum(case when rmap.rptcolumnname = ''OTCQUATERLY'' then  '||fungenericattribute(vrptname,'OTCMONTHLY')||' end) OTCMONTHLY
                                from rpt_base_incentive mes, rpt_mapping rmap
                                where mes.name in rmap.rulename
             and rmap.reportname = '''||vrptname||'''
             and mes.processingunitseq = '||vprocessingunitseq||'
             and mes.periodseq in
                                  (select mon.periodseq from cs_period mon, cs_period qtr
                                          where mon.removedate = ''01-JAN-2200''
                 and mon.parentseq = qtr.periodseq
                 and mon.removedate = qtr.removedate
                 and mon.calendarseq = qtr.calendarseq
                 and mon.parentseq in (select parentseq from cs_period
                                                                        where periodseq = '||vperiodseq||'
                     and calendarseq = '||vcalendarseq||'
             and removedate = '''||'01-JAN-2200'||'''))
                                group by mes.positionseq,
                                         mes.payeeseq,
                                         mes.processingunitseq,
                                     mes.periodseq)qtr
             on (rpt.processingunitseq = qtr.processingunitseq
             and rpt.periodseq = qtr.periodseq
             and rpt.positionseq = qtr.positionseq
         and rpt.payeeseq = qtr.payeeseq)
              when matched then update set rpt.OTC_QUARTERLY = qtr.otcmonthly';
        
           prc_logevent (vPeriodRow.name,vProcName,'Update Started',NULL,v_sql);
        
           EXECUTE IMMEDIATE v_sql;
        
           prc_logevent (vPeriodRow.name,vProcName,'Update completed',NULL,v_sql);
        
           COMMIT;
        */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TOTAL insert',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TOTAL insert', NULL, NULL);

        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_ADV_COMM_PAY' not found */
        /* ORIGSQL: INSERT INTO stelext.RPT_ADV_COMM_PAY (tenantid, positionseq, payeeseq, processin(...) */
        INSERT INTO ext.RPT_ADV_COMM_PAY
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                OTC_QUARTERLY, OTC_MONTHLY, PAYABLECOMM
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
                '53' AS reportcode,
                '1' AS sectionid,
                'TOTAL' AS sectionname,
                '2' AS sortorder,
                NULL AS empfirstname,
                NULL AS emplastname,
                'BSC Advance Commission Payout' AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                rpt.OTC_QUARTERLY,
                rpt.OTC_MONTHLY,
                rpt.PAYABLECOMM
            FROM
                (
                    SELECT   /* ORIGSQL: (select SUM(OTC_QUARTERLY) OTC_QUARTERLY, SUM(OTC_MONTHLY) OTC_MONTHLY, SUM(PAYA(...) */
                        SUM(OTC_QUARTERLY) AS OTC_QUARTERLY,
                        SUM(OTC_MONTHLY) AS OTC_MONTHLY,
                        SUM(PAYABLECOMM) AS PAYABLECOMM
                    FROM
                        ext.RPT_ADV_COMM_PAY tab
                    WHERE
                        tab.processingunitseq = :vprocessingunitseq
                        AND tab.periodseq = :vperiodseq
                        AND tab.sectionname = 'DETAIL'
                ) AS rpt;

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: UPDATE stelext.RPT_ADV_COMM_PAY tab SET ID = (SELECT COUNT(DISTINCT payeeseq) FR(...) */
        UPDATE ext.RPT_ADV_COMM_PAY tab
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
            SET
            /* ORIGSQL: ID = */
            ID = (
                SELECT   /* ORIGSQL: (select COUNT(DISTINCT payeeseq) from rpt_base_padimension where processingunits(...) */
                    COUNT(DISTINCT payeeseq)
                FROM
                    ext.rpt_base_padimension
                WHERE
                    processingunitseq = :vprocessingunitseq
                    AND periodseq = :vperiodseq
                    AND reportgroup = 'BSC'
            )
        WHERE
            tab.processingunitseq = :vprocessingunitseq
            AND tab.periodseq = :vperiodseq
            AND tab.sectionname = 'TOTAL';

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
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName) --Sanjay: Commenting out as Analyze is not required.
      
        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END