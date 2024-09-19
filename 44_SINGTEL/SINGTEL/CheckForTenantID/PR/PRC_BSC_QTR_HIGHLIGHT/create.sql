CREATE PROCEDURE EXT.PRC_BSC_QTR_HIGHLIGHT
(
    --IN vrptname rpt_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                               /* RESOLVE: Datatype unresolved: Datatype (rpt_mapping.reportname%TYPE) not resolved for parameter 'PRC_BSC_QTR_HIGHLIGHT.vrptname' */
                                               /* ORIGSQL: vrptname IN rpt_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_BSC_QTR_HIGHLIGHT.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_BSC_QTR_HIGHLIGHT.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_BSC_QTR_HIGHLIGHT.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = DBMTK_USER_NAME.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();

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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_BSC_QTR_HIGHLIGHT');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_BSC_QTR_HIGHLIGHT') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_BSC_QTR_HIGHLIGHT';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_BSC_QTR_HIGHLIGHT'; */
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
    DECLARE v_quaterly VARCHAR(100);  /* ORIGSQL: v_quaterly VARCHAR2(100); */
    DECLARE v_totalstaffs DECIMAL(38,10);  /* ORIGSQL: v_totalstaffs number; */
    DECLARE v_totalpayout DECIMAL(25,2);  /* ORIGSQL: v_totalpayout NUMBER(25,2); */
    DECLARE v_finyear VARCHAR(50);  /* ORIGSQL: v_finyear VARCHAR2(50); */
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
            --||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE

            || ' - '||IFNULL(:vSQLERRM,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;
        select * into vPeriodRow from cs_period where periodseq = :vperiodseq and removedate > current_date;
   /* initialize library variables, if not yet done */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2:init_session_global();/*Deepan : replacing with session variable*/
       

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
            ); */--Sanjay Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

       -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName); --Sanjay: commenting as subpartition are not required /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as AnalyzeTableSubpartition are not required

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);Sanjay:commenting as truncateTableSubpartition are not required

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);Sanjay:commenting as analyze is not required

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
        SELECT
            DISTINCT
            qtr.shortname
        INTO
            v_quaterly
        FROM
            cs_period mon,
            cs_period qtr
        WHERE
            mon.periodseq = :vperiodseq
            AND mon.calendarseq = :vcalendarseq
            AND mon.removedate = :cEndofTime  /* ORIGSQL: pkg_reporting_extract_r2.cEndofTime */
            AND mon.parentseq = qtr.periodseq
            AND mon.removedate = qtr.removedate;

        v_totalstaffs = 0;
        BEGIN 
            DECLARE EXIT HANDLER FOR SQLEXCEPTION
                /* ORIGSQL: when others then */
                BEGIN
                    v_totalstaffs = 0;
                END;


            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */

            SELECT
                COUNT(*)
            INTO
                v_totalstaffs
            FROM
                ext.rpt_base_padimension pad
            WHERE
                pad.processingunitseq = :vprocessingunitseq
                AND pad.periodseq = :vperiodseq
                AND pad.reportgroup = 'BSC';

            /* ORIGSQL: exception when others then */
        END;

        SELECT
            STRING_AGG('FY'||IFNULL(FYEAR,''),'/' ORDER BY FYEAR)   /* ORIGSQL: LISTAGG('FY'||FYEAR,'/') WITHIN GROUP (ORDER BY FYEAR) */
        INTO
            v_finyear
            /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_MONTHHIERARCHY_TBL' not found */
        FROM
            (
                SELECT   /* ORIGSQL: (SELECT DISTINCT substr(monthname1,-2,2) FYEAR FROM STELEXT.STEL_MONTHHIERARCHY_(...) */
                    DISTINCT
                    substring(monthname1,-2,2) AS FYEAR
                FROM
                    EXT.STEL_MONTHHIERARCHY_TBL
                WHERE
                    CALENDARNAME = :vCalendarRow.name
                    AND PERIODTYPENAME = 'year'
                    AND PERIODNAME LIKE '%'||IFNULL(substring(:vPeriodRow.name,-4),'') ||'%'   /* ORIGSQL: substr(vPeriodRow.name,-4) */
                    AND (MONTHNAME1 LIKE '%March%'
                        OR MONTHNAME1 LIKE '%June%'
                        OR MONTHNAME1 LIKE '%September%'
                    OR MONTHNAME1 LIKE '%December%')
            ) AS dbmtk_corrname_4379;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Individual Achivement',NULL,v_sq(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Individual Achivement', NULL, :v_sql);

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_PAYEE_MTHQTR_ACHIVEMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BSC_QTR_HIGHLIGHT' not found */

        /* ORIGSQL: insert into RPT_BSC_QTR_HIGHLIGHT (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUN(...) */
        INSERT INTO EXT.RPT_BSC_QTR_HIGHLIGHT
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                EMPFIRSTNAME, EMPLASTNAME, TITLENAME, PERIODSTARTDATE, PERIODENDDATE, LOADDTTM,
                QUARTER, TEAMPERCENTAGE, COMMISSIONTOTAL, DISPLAYNAME, DISPLAYVALUE, FINYEAR
            )
            
                SELECT   /* ORIGSQL: (select vTenantId, null, null, vprocessingunitseq, vperiodseq, vPeriodRow.name,v(...) */
                    :vTenantId,
                    NULL,
                    NULL,
                    :vprocessingunitseq,
                    :vperiodseq,
                    :vPeriodRow.name,
                    :vProcessingUnitRow.name,
                    :vCalendarRow.name,
                    '54',
                    1,
                    'GROUP',
                    1,
                    NULL,
                    NULL,
                    NULL,
                    :vPeriodRow.startdate,
                    :vPeriodRow.enddate,
                    CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                    :v_quaterly,
                    (cnt/:v_totalstaffs)*100,
                    NULL,
                    'Indv. Achievement >200% Threshold',
                    cnt,
                    :v_finyear
                FROM
                    (
                        SELECT   /* ORIGSQL: (SELECT COUNT(1) as cnt FROM RPT_PAYEE_MTHQTR_ACHIVEMENT ach WHERE ALLGROUPS = '(...) */
                            COUNT(1) AS cnt
                        FROM
                            EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT ach
                        WHERE
                            ALLGROUPS = 'OVERALL COMMISSION'
                            AND PRODUCT = 'INDIVIDUAL'
                            AND ach.processingunitseq = :vprocessingunitseq
                            AND ach.periodseq = :vperiodseq
                            AND CVPERACHIV >= 200
                    ) AS dbmtk_corrname_4382
                ;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Team Achivement',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Team Achivement', NULL, :v_sql);  

        /* ORIGSQL: insert into RPT_BSC_QTR_HIGHLIGHT (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUN(...) */
        INSERT INTO EXT.RPT_BSC_QTR_HIGHLIGHT
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                EMPFIRSTNAME, EMPLASTNAME, TITLENAME, PERIODSTARTDATE, PERIODENDDATE, LOADDTTM,
                QUARTER, TEAMPERCENTAGE, COMMISSIONTOTAL, DISPLAYNAME, DISPLAYVALUE, FINYEAR
            )
            SELECT   /* ORIGSQL: (select vTenantId, null, null, vprocessingunitseq, vperiodseq, vPeriodRow.name,v(...) */
                :vTenantId,
                NULL,
                NULL,
                :vprocessingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '54',
                1,
                'GROUP',
                2,
                NULL,
                NULL,
                NULL,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :v_quaterly,
                (cnt/:v_totalstaffs)*100,
                NULL,
                'Team Achievement >150% Threshold',
                cnt,
                :v_finyear
            FROM
                (
                    SELECT   /* ORIGSQL: (SELECT COUNT(1) as cnt FROM RPT_PAYEE_MTHQTR_ACHIVEMENT ach WHERE ALLGROUPS = '(...) */
                        COUNT(1) AS cnt
                    FROM
                        EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT ach
                    WHERE
                        ALLGROUPS = 'OVERALL COMMISSION'
                        AND PRODUCT = 'TEAM'
                        AND ach.processingunitseq = :vprocessingunitseq
                        AND ach.periodseq = :vperiodseq
                        AND CVPERACHIV >= 150
                ) AS dbmtk_corrname_4385
            ;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert No of Staffs',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert No of Staffs', NULL, :v_sql);  

        /* ORIGSQL: insert into RPT_BSC_QTR_HIGHLIGHT (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUN(...) */
        INSERT INTO EXT.RPT_BSC_QTR_HIGHLIGHT
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                EMPFIRSTNAME, EMPLASTNAME, TITLENAME, PERIODSTARTDATE, PERIODENDDATE, LOADDTTM,
                QUARTER, TEAMPERCENTAGE, COMMISSIONTOTAL, DISPLAYNAME, DISPLAYVALUE, FINYEAR
            )
            SELECT   /* ORIGSQL: (select vTenantId, null, null, vprocessingunitseq, vperiodseq, vPeriodRow.name,v(...) */
                :vTenantId,
                NULL,
                NULL,
                :vprocessingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '54',
                1,
                'GROUP',
                3,
                NULL,
                NULL,
                NULL,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :v_quaterly,
                NULL,
                NULL,
                'Total No. of Staffs',
                :v_totalstaffs,
                :v_finyear
            FROM
                (
                    SELECT   /* ORIGSQL: (SELECT COUNT(1) as cnt FROM RPT_PAYEE_MTHQTR_ACHIVEMENT ach WHERE ALLGROUPS = '(...) */
                        COUNT(1) AS cnt
                    FROM
                        EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT ach
                    WHERE
                        ALLGROUPS = 'OVERALL COMMISSION'
                        AND PRODUCT = 'TEAM'
                        AND ach.processingunitseq = :vprocessingunitseq
                        AND ach.periodseq = :vperiodseq
                        AND CVPERACHIV >= 150
                ) AS dbmtk_corrname_4388
            ;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Quaterly Comm Payout',NULL,v_sql(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Quaterly Comm Payout', NULL, :v_sql);

        v_sql = 'insert into EXT_BSC_QTR_HIGHLIGHT
        (
            TENANTID,
            POSITIONSEQ,
            PAYEESEQ,
            PROCESSINGUNITSEQ,
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            REPORTCODE,
            SECTIONID,
            SECTIONNAME,
            SORTORDER,
            EMPFIRSTNAME,
            EMPLASTNAME,
            TITLENAME,
            PERIODSTARTDATE,
            PERIODENDDATE,
            LOADDTTM,
            QUARTER,
            TEAMPERCENTAGE,
            COMMISSIONTOTAL,
            DISPLAYNAME,
            DISPLAYVALUE,
            FINYEAR
        )
        (
            select
            '''||IFNULL(:vTenantId,'')||''',
            null,
            null,
            '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||',
            '||IFNULL(:vperiodseq,'')||',
            '''||IFNULL(:vPeriodRow.name,'')||''',
            '''||IFNULL(:vProcessingUnitRow.name,'')||''',
            '''||IFNULL(:vCalendarRow.name,'')||''',
            ''54'',
            1,
            ''GROUP1'',
            1,
            null,
            null,
            null,
            '''||IFNULL(TO_VARCHAR(:vPeriodRow.startdate,'DD-MON-YYYY'),'') ||''',
            '''||IFNULL(TO_VARCHAR(:vPeriodRow.enddate,'DD-MON-YYYY'),'') ||''',
            sysdate,
            '''||IFNULL(:v_quaterly,'')||''',
            (mes.QTRTEAMPER*100),
            rpt.totaladv,
            ''Quarterly Comm Payout'',
            null,
            '''||IFNULL(:v_finyear,'')||'''
            from
            (
                select
                sum(ach.total) as totaladv
                from EXT_PAYEE_MTHQTR_ACHIVEMENT ach
                where ach.allgroups =''OVERALL COMMISSION''
                and ach.product in(''Earned Commission'')
                and ach.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and ach.periodseq = '||IFNULL(:vperiodseq,'')||'
            )rpt,
            (
                select
                max(case when rmap.rptcolumnname = ''QTRTEAMPER'' then '||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'QTRTEAMPER'),'') ||' end) QTRTEAMPER
                from rpt_base_measurement mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            )mes
            )';  /* ORIGSQL: to_char(vPeriodRow.startdate,'DD-MON-YYYY') */
                 /* ORIGSQL: to_char(vPeriodRow.enddate,'DD-MON-YYYY') */
                 /* ORIGSQL: fungenericattribute(vrptname,'QTRTEAMPER') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Complete Quaterly Comm Payout',NULL,v_s(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Complete Quaterly Comm Payout', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Less 50% Advance Comm paid in the first(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Less 50% Advance Comm paid in the first 2 months', NULL, :v_sql);  

        /* ORIGSQL: insert into RPT_BSC_QTR_HIGHLIGHT (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUN(...) */
        INSERT INTO EXT.RPT_BSC_QTR_HIGHLIGHT
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                EMPFIRSTNAME, EMPLASTNAME, TITLENAME, PERIODSTARTDATE, PERIODENDDATE, LOADDTTM,
                QUARTER, TEAMPERCENTAGE, COMMISSIONTOTAL, DISPLAYNAME, DISPLAYVALUE, FINYEAR
            )
            SELECT   /* ORIGSQL: (select vTenantId, null, null, vprocessingunitseq, vperiodseq, vPeriodRow.name,v(...) */
                :vTenantId,
                NULL,
                NULL,
                :vprocessingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '54',
                1,
                'GROUP1',
                2,
                NULL,
                NULL,
                NULL,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :v_quaterly,
                NULL,
                totaladv,
                'Less 50% Advance Comm paid in the first 2 months',
                NULL,
                :v_finyear
            FROM
                (
                    SELECT   /* ORIGSQL: (select SUM(total) as totaladv from RPT_PAYEE_MTHQTR_ACHIVEMENT ach where ach.al(...) */
                        SUM(total) AS totaladv
                    FROM
                        EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT ach
                    WHERE
                        ach.allgroups = 'COMMISSION ADJUSTMENT'
                        AND ach.product IN('Advance Payment Adjustment')
                        AND ach.processingunitseq = :vprocessingunitseq
                        AND ach.periodseq = :vperiodseq
                ) AS dbmtk_corrname_4391
            ;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Other Adjustments/Prorations',NULL,v_sq(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Other Adjustments/Prorations', NULL, :v_sql);  

        /* ORIGSQL: insert into RPT_BSC_QTR_HIGHLIGHT (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUN(...) */
        INSERT INTO EXT.RPT_BSC_QTR_HIGHLIGHT
            ---Other adjustments/prorations
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                EMPFIRSTNAME, EMPLASTNAME, TITLENAME, PERIODSTARTDATE, PERIODENDDATE, LOADDTTM,
                QUARTER, TEAMPERCENTAGE, COMMISSIONTOTAL, DISPLAYNAME, DISPLAYVALUE, FINYEAR
            )
            SELECT   /* ORIGSQL: (select vTenantId, null, null, vprocessingunitseq, vperiodseq, vPeriodRow.name,v(...) */
                :vTenantId,
                NULL,
                NULL,
                :vprocessingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '54',
                1,
                'GROUP1',
                3,
                NULL,
                NULL,
                NULL,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :v_quaterly,
                NULL,
                totaladv,
                'Other adjustments/prorations',
                NULL,
                :v_finyear
            FROM
                (
                    SELECT   /* ORIGSQL: (select SUM(total) as totaladv from RPT_PAYEE_MTHQTR_ACHIVEMENT ach where ach.al(...) */
                        SUM(total) AS totaladv
                    FROM
                        EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT ach
                    WHERE
                        ach.allgroups = 'COMMISSION ADJUSTMENT'
                        AND ach.product IN('Advance Protected Commission (New Staff)','Payment Adjustment','Prior Balance Adjustment')
                        AND ach.processingunitseq = :vprocessingunitseq
                        AND ach.periodseq = :vperiodseq
                ) AS dbmtk_corrname_4394
            ;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Total Payout',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Total Payout', NULL, :v_sql);  

        /* ORIGSQL: insert into RPT_BSC_QTR_HIGHLIGHT (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUN(...) */
        INSERT INTO EXT.RPT_BSC_QTR_HIGHLIGHT
            ---Quarterly Comm payout
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                EMPFIRSTNAME, EMPLASTNAME, TITLENAME, PERIODSTARTDATE, PERIODENDDATE, LOADDTTM,
                QUARTER, TEAMPERCENTAGE, COMMISSIONTOTAL, DISPLAYNAME, DISPLAYVALUE, FINYEAR
            )
            SELECT   /* ORIGSQL: (select vTenantId, null, null, vprocessingunitseq, vperiodseq, vPeriodRow.name,v(...) */
                :vTenantId,
                NULL,
                NULL,
                :vprocessingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '54',
                1,
                'GROUP1',
                4,
                NULL,
                NULL,
                NULL,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :v_quaterly,
                NULL,
                :v_totalpayout,
                'Total Payout',
                NULL,
                :v_finyear
            FROM
                (
                    SELECT   /* ORIGSQL: (select SUM(total) as v_totalpayout from RPT_PAYEE_MTHQTR_ACHIVEMENT ach where a(...) */
                        SUM(total) AS v_totalpayout
                    FROM
                        EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT ach
                    WHERE
                        ach.allgroups = 'COMMISSION ADJUSTMENT'
                        AND ach.product IN('Commission Final payout')
                        AND ach.processingunitseq = :vprocessingunitseq
                        AND ach.periodseq = :vperiodseq
                ) AS dbmtk_corrname_4397
            ;

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
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName); --Sanjay: commenting as analyze is not required

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END