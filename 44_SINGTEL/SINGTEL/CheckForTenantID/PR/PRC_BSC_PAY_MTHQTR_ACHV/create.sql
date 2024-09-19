CREATE PROCEDURE EXT.PRC_BSC_PAY_MTHQTR_ACHV
(
    --IN vrptname rpt_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                               /* RESOLVE: Datatype unresolved: Datatype (rpt_mapping.reportname%TYPE) not resolved for parameter 'PRC_BSC_PAY_MTHQTR_ACHV.vrptname' */
                                               /* ORIGSQL: vrptname IN rpt_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_BSC_PAY_MTHQTR_ACHV.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_BSC_PAY_MTHQTR_ACHV.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_BSC_PAY_MTHQTR_ACHV.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /*DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();
    DECLARE PKG_REPORTING_EXTRACT_R2__vcredittypeid_PayAdj VARCHAR(255); /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    -- BSCPAYEEACHIVEMENT
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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_BSC_PAY_MTHQTR_ACHV');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_BSC_PAY_MTHQTR_ACHV') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_PAYEE_MTHQTR_ACHIVEMENT';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_PAYEE_MTHQTR_ACHIVEMENT'; */
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
    DECLARE vcredittypeid_PayAdj NVARCHAR(50);
    DECLARE cEndofTime CONSTANT date := to_date('2200-01-01','yyyy-mm-dd');
    /* ORIGSQL: for i in (select distinct product,sortorder from rpt_mapping where reportname = (...) */
    DECLARE CURSOR dbmtk_cursor_3839
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_MAPPING' not found */

        SELECT   /* ORIGSQL: select distinct product,sortorder from rpt_mapping where reportname = vrptname a(...) */
            DISTINCT
            product,
            sortorder
        FROM
            rpt_mapping
        WHERE
            reportname = :vrptname
            AND product IS NOT NULL
            AND allgroups = 'INDIVIDUAL ACHIEVEMENT'
        ORDER BY
            sortorder;

    -- Connection Total

    /* ORIGSQL: for i in (select distinct product from rpt_mapping where reportname = vrptname a(...) */
    DECLARE CURSOR dbmtk_cursor_3842
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_mapping where reportname = vrptname and product(...) */
            DISTINCT
            product
        FROM
            rpt_mapping
        WHERE
            reportname = :vrptname
            AND product IS NOT NULL
            AND allgroups = 'TEAM ACHIEVEMENT';

    -- Will execute only for Quater periods
    /* ORIGSQL: for i in (select distinct product,sortorder from rpt_mapping where reportname = (...) */
    DECLARE CURSOR dbmtk_cursor_3845
    FOR 
        SELECT   /* ORIGSQL: select distinct product,sortorder from rpt_mapping where reportname = vrptname a(...) */
            DISTINCT
            product,
            sortorder
        FROM
            rpt_mapping
        WHERE
            reportname = :vrptname
            AND product IS NOT NULL
            AND allgroups = 'OVERALL COMMISSION'
        ORDER BY
            sortorder;

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

        /* initialize session variables, if not yet done */
              /* initialize library variables, if not yet done */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2:init_session_global();/*Deepan : replacing with session variable*/
        
     
        SET  'vcredittypeid_PayAdj' = 'Payment Adjustment';

        /* retrieve the package/session variables referenced in this procedure */
        --SELECT SESSION_CONTEXT('DBMTK_GLOBVAR_EXT_PKG_REPORTING_EXTRACT_R2_VCREDITTYPEID_PAYADJ') INTO PKG_REPORTING_EXTRACT_R2__vcredittypeid_PayAdj FROM SYS.DUMMY ;-- Sanjay need to set the session context.
        /* end of package/session variables */
        -- SELECT SESSION_CONTEXT('VCREDITTYPEID_PAYADJ') INTO vcredittypeid_PayAdj FROM SYS.DUMMY ;
        -- SELECT SESSION_CONTEXT('cEndofTime') INTO cEndofTime FROM SYS.DUMMY ;

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
            ); */--Sanjay Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

        --vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName); --Sanjay: commenting as subpartition are not required /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as AnalyzeTableSubpartition are not required

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);--Sanjay: commenting as TruncateTable is not required

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as AnalyzeTableSubpartition are not required

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
        v_UserGroup = 'N';

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_CLASSIFIER' not found */

        SELECT
            MAX(GENERICBOOLEAN1)
        INTO
            v_payable
        FROM
            EXT.STEL_CLASSIFIER
        WHERE
            CATEGORYTREENAME = 'Reporting Config'
            AND Categoryname = 'Payable'
            AND Classifierid = 'BSC Report Payable'
            AND effectiveenddate > :vPeriodRow.startdate
            AND effectivestartdate < :vPeriodRow.enddate;

        SELECT
            MAX('Y') 
        INTO
            v_UserGroup
            /* RESOLVE: Identifier not found: Table/view 'TCMP.CS_PIPELINERUN' not found */
        FROM
            (
                SELECT   /* ORIGSQL: (select RTRIM(REGEXP_SUBSTR(runparameters, '\[boGroupsList\]([^\[]+)', 1, 1, 'i'(...) */
                    IFNULL(RTRIM(SUBSTRING_REGEXPR('\[boGroupsList\]([^\[]+)' FLAG 'i' IN runparameters FROM 1 OCCURRENCE 1), ','),'') ||',' AS GroupList  /* ORIGSQL: REGEXP_SUBSTR(runparameters, '\[boGroupsList\]([^\[]+)', 1, 1, 'i', 1) */
                FROM
                    tcmp.cs_pipelinerun
                WHERE
                    command = 'PipelineRun'
                    AND description LIKE '%ODS%'
                    AND state <> 'Done'
                    AND periodseq = :vperiodseq
                    AND processingunitseq = :vprocessingunitseq
            ) AS dbmtk_corrname_3870
        WHERE
            Grouplist LIKE '%User Group%';

        IF :v_UserGroup = 'Y' 
        THEN
            v_payableflag = :v_payable;
        ELSE 
            v_payableflag = 1;
        END IF;

        FOR i AS dbmtk_cursor_3839
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Product',NULL,i.product) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Product', NULL, :i.product);

            v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                titlename,
                loaddttm,
                allgroups,
                product,
                conntarget,
                connactual,
                connactualtarget,
                cvactual,
                cvperachiv,
                multiplier,
                payable_flag
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''51'' reportcode,
            ''1'' sectionid,
            ''01INDIVIDUAL ACHIEVEMENT'' sectionname,
            ''' ||IFNULL(:i.sortorder,'')||''' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''INDIVIDUAL ACHIEVEMENT'' allgroups,
            ''' ||IFNULL(:i.product,'')||''' product,
            conntarget,
            connactual,
            (connactualtarget*100) connactualtarget,
            cvactual,
            (cvperachiv*100) cvperachiv,
            multiplier,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
            FROM   rpt_base_padimension pad,
            (
                select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                max(case when rmap.rptcolumnname = ''CONNTARGET'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONNTARGET', :i.product),'') ||' end) CONNTARGET,
                max(case when rmap.rptcolumnname = ''CONNACTUAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONNACTUAL', :i.product),'') ||' end) CONNACTUAL,
                max(case when rmap.rptcolumnname = ''CONNACTUALTARGET'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONNACTUALTARGET', :i.product),'') ||' end) CONNACTUALTARGET,
                max(case when rmap.rptcolumnname = ''CVACTUAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CVACTUAL', :i.product),'') ||' end) CVACTUAL,
                max(case when rmap.rptcolumnname = ''CVPERACHIV'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CVPERACHIV', :i.product),'') ||' end) CVPERACHIV,
                NULL MULTIPLIER
                from rpt_base_measurement mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''INDIVIDUAL ACHIEVEMENT''
                group by mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'CVPERACHIV',i.product) */
                                             /* ORIGSQL: fungenericattribute(vrptname,'CVACTUAL',i.product) */
                                             /* ORIGSQL: fungenericattribute(vrptname,'CONNTARGET',i.product) */
                                             /* ORIGSQL: fungenericattribute(vrptname,'CONNACTUALTARGET',i.product) */
                                             /* ORIGSQL: fungenericattribute(vrptname,'CONNACTUAL',i.product) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin completed',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            IF :i.product != 'POINTS PAYOUT' 
            THEN
                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Team Achievement for each produc(...) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Team Achievement for each product', NULL, :i.product);
                v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                    titlename,
                    loaddttm,
                    allgroups,
                    product,
                    conntarget,
                    connactual,
                    connactualtarget,
                    cvactual,
                    payable_flag
                )
                SELECT   ''' ||IFNULL(:vTenantId,'')||''',
                pad.positionseq,
                pad.payeeseq,
                ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
                ' ||IFNULL(:vperiodseq,'')||',
                ''' ||IFNULL(:vPeriodRow.name,'')||''',
                ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
                ''' ||IFNULL(:vCalendarRow.name,'')||''',
                ''51'' reportcode,
                ''1'' sectionid,
                ''03TEAM ACHIEVEMENT'' sectionname,
                ''' ||IFNULL(:i.sortorder,'')||''' sortorder,
                pad.reporttitle titlename,
                SYSDATE,
                ''TEAM ACHIEVEMENT'' allgroups,
                ''' ||IFNULL(:i.product,'')||''' product,
                conntarget,
                connactual,
                decode(conntarget,0,0,((connactual/conntarget)*100)) connactualtarget,
                cvactual,
                ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
                FROM   rpt_base_padimension pad,
                (
                    select
                    periodseq,
                    processingunitseq,
                    sum(conntarget) conntarget,
                    sum(connactual) connactual,
                    sum(cvactual) cvactual
                    from RPT_PAYEE_MTHQTR_ACHIVEMENT rpt
                    where rpt.periodseq = ' ||IFNULL(:vperiodseq,'')||'
                    and rpt.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                    and rpt.allgroups = ''INDIVIDUAL ACHIEVEMENT''
                    and rpt.product = '''||IFNULL(:i.product,'')||'''
                    group by
                    periodseq,
                    processingunitseq
                )mes
                
                WHERE   pad.processingunitseq = mes.processingunitseq
                and pad.periodseq = mes.periodseq
                and pad.reportgroup = ''BSC''';

                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Team Achievement Completed',NULL(...) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Team Achievement Completed', NULL, :v_sql);

                /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
                EXECUTE IMMEDIATE :v_sql;

                /* ORIGSQL: COMMIT; */
                COMMIT;
            END IF;
            --!= points payout
        END FOR;  /* ORIGSQL: End Loop; */

        -- Individual Acheviement

        IF :vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Point Payout only applicable to Quaterly periods

            v_sql = 'MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
            (select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                sum(case when rmap.rptcolumnname = ''TOTAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TOTAL', 'POINTS PAYOUT'),'') ||' end) TOTAL
                from ext.rpt_base_incentive mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and rmap.product = ''POINTS PAYOUT''
                and rmap.allgroups = ''INDIVIDUAL ACHIEVEMENT''
                group by mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
            mes.periodseq)qtr
            on (rpt.processingunitseq = qtr.processingunitseq
                and rpt.periodseq = qtr.periodseq
                and rpt.positionseq = qtr.positionseq
                and rpt.payeeseq = qtr.payeeseq
            and rpt.product = ''POINTS PAYOUT'')
            when matched then update set rpt.TOTAL=qtr.TOTAL, rpt.allgroups = ''INDIVIDUAL ACHIEVEMENT'', rpt.payable_flag = ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'';  /* ORIGSQL: fungenericattribute(vrptname,'TOTAL','POINTS PAYOUT') */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive Started',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive Started', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive completed',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive completed', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END IF;

        --Code for Aggregation of  Quarterly Values ..POINTS PAYOUT-CVTARGET
        v_sql = 'MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            sum(case when rmap.rptcolumnname = ''CVTARGET'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CVTARGET', 'POINTS PAYOUT'),'') ||' end) CVTARGET
            from cs_measurement mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and mes.periodseq in (select periodseq from cs_period where periodseq in (
                    select mon.periodseq from cs_period mon, cs_period qtr
                    where mon.removedate = ''01-JAN-2200''
                    and mon.parentseq = qtr.periodseq
                    and mon.removedate = qtr.removedate
                    and mon.calendarseq = qtr.calendarseq
                    and mon.parentseq in (select parentseq from cs_period
                        where periodseq = ' ||IFNULL(:vperiodseq,'')||'
                        and calendarseq ='||IFNULL(:vcalendarseq,'')||'
                        and removedate = '''||'01-JAN-2200'||''')) and periodseq <= ' ||IFNULL(:vperiodseq,'')||')
            group by mes.positionseq,
            mes.payeeseq,
        mes.processingunitseq)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.product = ''POINTS PAYOUT''
        and rpt.allgroups = ''INDIVIDUAL ACHIEVEMENT'')
        when matched then update set rpt.CVTARGET=qtr.CVTARGET';  /* ORIGSQL: fungenericattribute(vrptname,'CVTARGET','POINTS PAYOUT') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update POINTS PAYOUT QUARTER started',N(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update POINTS PAYOUT QUARTER started', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update POINTS PAYOUT QUARTER completed'(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update POINTS PAYOUT QUARTER completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
            titlename,
            loaddttm,
            allgroups,
            product,
            conntarget,
            connactual,
            connactualtarget,
            payable_flag
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''51'' reportcode,
        ''1'' sectionid,
        ''02CONNECTIONS'' sectionname,
        ''02'' sortorder,
        pad.reporttitle titlename,
        SYSDATE,
        ''CONNECTIONS'' allgroups,
        ''CONNECTIONS'' product,
        CONCONNTARGET,
        CONCONNACTUAL,
        (CONCONNACTUALTARGET*100) CONCONNACTUALTARGET,
        ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
        FROM   rpt_base_padimension pad,
        (
            select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            max(case when rmap.rptcolumnname = ''CONCONNTARGET'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONCONNTARGET'),'') ||' end) CONCONNTARGET,
            max(case when rmap.rptcolumnname = ''CONCONNACTUAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONCONNACTUAL'),'') ||' end) CONCONNACTUAL,
            max(case when rmap.rptcolumnname = ''CONCONNACTUALTARGET'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONCONNACTUALTARGET'),'') ||' end) CONCONNACTUALTARGET
            from rpt_base_measurement mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.allgroups = ''CONNECTIONS''
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq
        )mes
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'CONCONNTARGET') */
                                         /* ORIGSQL: fungenericattribute(vrptname,'CONCONNACTUALTARGET') */
                                         /* ORIGSQL: fungenericattribute(vrptname,'CONCONNACTUAL') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        v_sql = 'MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            sum(case when rmap.rptcolumnname = ''CONMUL'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONMUL'),'') ||' end) CONMUL
            from rpt_base_incentive mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and rmap.allgroups = ''CONNECTIONS''
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
        mes.periodseq)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
        and rpt.allgroups = ''CONNECTIONS'')
        when matched then update set rpt.MULTIPLIER = (qtr.CONMUL*100)';  /* ORIGSQL: fungenericattribute(vrptname,'CONMUL') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive Started',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive Started', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        IF :vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Connection Total only applicable to Quaterly periods

            v_sql = 'MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
            (select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                sum(case when rmap.rptcolumnname = ''CONTOTAL'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONTOTAL'),'') ||' end) CONTOTAL
                from rpt_base_incentive mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and rmap.allgroups = ''CONNECTIONS''
                group by mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
            mes.periodseq)qtr
            on (rpt.processingunitseq = qtr.processingunitseq
                and rpt.periodseq = qtr.periodseq
                and rpt.positionseq = qtr.positionseq
                and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''CONNECTIONS'')
            when matched then update set rpt.TOTAL=qtr.CONTOTAL,rpt.payable_flag = ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'';  /* ORIGSQL: fungenericattribute(vrptname,'CONTOTAL') */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive Started',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive Started', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive completed',NULL,v_sql) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive completed', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END IF;

        FOR i AS dbmtk_cursor_3842
        DO
            -- Team Payout

            IF :i.product = 'TEAM PAYOUT' 
            THEN
                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Team Payout',NULL,i.product) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Team Payout', NULL, :i.product);

                v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                    titlename,
                    loaddttm,
                    allgroups,
                    product,
                    CVTARGET,
                    CVACTUAL,
                    CVPERACHIV,
                    payable_flag
                )
                SELECT   ''' ||IFNULL(:vTenantId,'')||''',
                pad.positionseq,
                pad.payeeseq,
                ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
                ' ||IFNULL(:vperiodseq,'')||',
                ''' ||IFNULL(:vPeriodRow.name,'')||''',
                ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
                ''' ||IFNULL(:vCalendarRow.name,'')||''',
                ''51'' reportcode,
                ''1'' sectionid,
                ''03TEAM ACHIEVEMENT'' sectionname,
                ''03'' sortorder,
                pad.reporttitle titlename,
                SYSDATE,
                ''TEAM ACHIEVEMENT'' allgroups,
                ''' ||IFNULL(:i.product,'')||''' product,
                CVTARGET,
                CVACTUAL,
                (CVPERACHIV*100) CVPERACHIV,
                ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
                FROM   rpt_base_padimension pad,
                (
                    select mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    max(case when rmap.rptcolumnname = ''TEAMCVTARGET'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TEAMCVTARGET', :i.product),'') ||' end) CVTARGET,
                    max(case when rmap.rptcolumnname = ''TEAMCVACTUAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TEAMCVACTUAL', :i.product),'') ||' end) CVACTUAL,
                    max(case when rmap.rptcolumnname = ''TEAMCVPERACHIEVED'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TEAMCVPERACHIEVED', :i.product),'') ||' end) CVPERACHIV
                    from rpt_base_measurement mes, rpt_mapping rmap
                    where mes.name in rmap.rulename
                    and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                    and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                    and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                    and rmap.product = '''||IFNULL(:i.product,'')||'''
                    and rmap.allgroups = ''TEAM ACHIEVEMENT''
                    group by mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq
                )mes
                WHERE       pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                and pad.periodseq = mes.periodseq
                and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'TEAMCVTARGET',i.product) */
                                                 /* ORIGSQL: fungenericattribute(vrptname,'TEAMCVPERACHIEVED',i.product) */
                                                 /* ORIGSQL: fungenericattribute(vrptname,'TEAMCVACTUAL',i.product) */

                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin completed',NULL,v_sql) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin completed', NULL, :v_sql);

                /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
                EXECUTE IMMEDIATE :v_sql;

                /* ORIGSQL: COMMIT; */
                COMMIT;
            ELSE 
                v_sql = 'MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
                (select mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    max(case when rmap.rptcolumnname = ''TEAMCONNTARGET'' then TO_NUMBER(RTRIM( ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TEAMCONNTARGET', :i.product),'') ||','' quantity''),999999) end) TEAMCONNTARGET
                    from rpt_base_measurement mes, rpt_mapping rmap
                    where mes.name in rmap.rulename
                    and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                    and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                    and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                    and rmap.product =  '''||IFNULL(:i.product,'')||'''
                    and rmap.allgroups = ''TEAM ACHIEVEMENT''
                    group by mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                mes.periodseq)qtr
                on (rpt.processingunitseq = qtr.processingunitseq
                    and rpt.periodseq = qtr.periodseq
                    and rpt.positionseq = qtr.positionseq
                    and rpt.payeeseq = qtr.payeeseq
                    and rpt.allgroups = ''TEAM ACHIEVEMENT''
                    and rpt.product =  ''' ||IFNULL(:i.product,'')||'''
                )
                when matched then update set rpt.conntarget=qtr.TEAMCONNTARGET';  /* ORIGSQL: fungenericattribute(vrptname,'TEAMCONNTARGET',i.product) */

                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Team Total Completed-Not execute(...) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Team Total Completed-Not executed only for log purpose', NULL, :v_sql);

                -- EXECUTE IMMEDIATE v_sql;   --commented this line to not to execute the above script for the issue 51 Team total achievement conntarget.
                --COMMIT;
            END IF;
        END FOR;  /* ORIGSQL: End Loop; */

        IF :vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            -- Team Total only applicable to Quaterly periods

            v_sql = 'MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
            (select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                sum(case when rmap.rptcolumnname = ''TEAMTOTAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TEAMTOTAL', 'TEAM PAYOUT'),'') ||' end) TOTAL
                from rpt_base_incentive mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and rmap.product = ''TEAM PAYOUT''
                and rmap.allgroups = ''TEAM ACHIEVEMENT''
                group by mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
            mes.periodseq)qtr
            on (rpt.processingunitseq = qtr.processingunitseq
                and rpt.periodseq = qtr.periodseq
                and rpt.positionseq = qtr.positionseq
                and rpt.payeeseq = qtr.payeeseq
                and rpt.allgroups = ''TEAM ACHIEVEMENT''
                and rpt.product = ''TEAM PAYOUT''
            )
            when matched then update set rpt.TOTAL=qtr.TOTAL,rpt.payable_flag = ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'';  /* ORIGSQL: fungenericattribute(vrptname,'TEAMTOTAL','TEAM PAYOUT') */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Team Total Completed',NULL,v_sql(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Team Total Completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END IF;
        -- Team Total Quarterly

        -- OVERALL COMMISSION TOTAL

        IF :vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_PAYEE_MTHQTR_ACHIVEMENT' not found */
            /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_PAYEE_MTHQTR_ACHIVEMENT' not found */

            /* ORIGSQL: INSERT INTO STELEXT.RPT_PAYEE_MTHQTR_ACHIVEMENT (tenantid, positionseq, payeeseq(...) */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
            INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
                (
                    tenantid,
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
                    titlename,
                    loaddttm,
                    allgroups,
                    product,
                    TOTAL,
                    payable_flag
                )
                SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, vProcessingUnitRow.processingun(...) */
                    :vTenantId,
                    pad.positionseq,
                    pad.payeeseq,
                    :vProcessingUnitRow.processingunitseq,
                    :vperiodseq,
                    :vPeriodRow.name,
                    :vProcessingUnitRow.name,
                    :vCalendarRow.name,
                    '51' AS reportcode,
                    '1' AS sectionid,
                    '03ZTOTAL' AS sectionname,
                    '01' AS sortorder,
                    pad.reporttitle AS titlename,
                    CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                    'OVERALL COMMISSION' AS allgroups,
                    'OVERALL COMMISSION' AS product,
                    IFNULL(TOTAL,0),  /* ORIGSQL: nvl(TOTAL,0) */
                    :v_payableflag
                FROM
                    /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                    (
                        SELECT   /* ORIGSQL: (SELECT rpt.positionseq, rpt.payeeseq, rpt.processingunitseq, rpt.periodseq, SUM(...) */
                            rpt.positionseq,
                            rpt.payeeseq,
                            rpt.processingunitseq,
                            rpt.periodseq,
                            SUM(total) AS total
                        FROM
                            EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt
                        WHERE
                            rpt.PERIODSEQ = :vperiodseq
                            AND rpt.PROCESSINGUNITSEQ = :vprocessingunitseq
                            AND rpt.PRODUCT IN ('POINTS PAYOUT','CONNECTIONS','TEAM PAYOUT')
                        GROUP BY
                            rpt.positionseq,
                            rpt.payeeseq,
                            rpt.processingunitseq,
                            rpt.periodseq
                    ) AS mes
                RIGHT OUTER JOIN
                    EXT.rpt_base_padimension AS pad
                    ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                    AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                    AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                    AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                WHERE
                    pad.reportgroup = 'BSC';

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END IF;

        IF :vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec')
        THEN
            FOR i AS dbmtk_cursor_3845
            DO
                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Overall Commission Product',NULL,i.prod(...) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Overall Commission Product', NULL, :i.product);

                v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                    titlename,
                    loaddttm,
                    allgroups,
                    product,
                    CVPERACHIV,
                    CVACTUAL,   -- NEED TO CHANGE THIS COLUMN
                    payable_flag
                )
                SELECT   ''' ||IFNULL(:vTenantId,'')||''',
                pad.positionseq,
                pad.payeeseq,
                ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
                ' ||IFNULL(:vperiodseq,'')||',
                ''' ||IFNULL(:vPeriodRow.name,'')||''',
                ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
                ''' ||IFNULL(:vCalendarRow.name,'')||''',
                ''51'' reportcode,
                ''1'' sectionid,
                ''04OVERALL COMMISSION'' sectionname,
                ''' ||IFNULL(:i.sortorder,'')||''' sortorder,
                pad.reporttitle titlename,
                SYSDATE,
                ''OVERALL COMMISSION'' allgroups,
                ''' ||IFNULL(:i.product,'')||''' product,
                (ALLCVPERACHIEVED*100) ALLCVPERACHIEVED,
                (ALLACHIEVEMENT*100) ALLACHIEVEMENT,
                ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
                FROM   rpt_base_padimension pad,
                (
                    select positionseq,
                    payeeseq,
                    processingunitseq,
                    periodseq,
                    max(ALLCVPERACHIEVED) ALLCVPERACHIEVED,
                    max(ALLACHIEVEMENT) ALLACHIEVEMENT
                    from
                    (select mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        max(case when rmap.rptcolumnname = ''ALLCVPERACHIEVED'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ALLCVPERACHIEVED', :i.product),'') ||' end) ALLCVPERACHIEVED,
                        max(case when rmap.rptcolumnname = ''ALLACHIEVEMENT'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ALLACHIEVEMENT', :i.product),'') ||' end) ALLACHIEVEMENT
                        from rpt_base_measurement mes, rpt_mapping rmap
                        where mes.name in rmap.rulename
                        and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                        and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                        and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                        and rmap.product = '''||IFNULL(:i.product,'')||'''
                        and rmap.allgroups = ''OVERALL COMMISSION''
                        group by mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                        union all
                        select inc.positionseq,
                        inc.payeeseq,
                        inc.processingunitseq,
                        inc.periodseq,
                        NULL ALLCVPERACHIEVED,
                        max(case when rmap.rptcolumnname = ''ALLACHIEVEMENT'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ALLACHIEVEMENT', :i.product),'') ||' end) ALLACHIEVEMENT
                        from rpt_base_incentive inc, rpt_mapping rmap
                        where inc.name in rmap.rulename
                        and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                        and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
                        and inc.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                        and rmap.product = '''||IFNULL(:i.product,'')||'''
                        and rmap.allgroups = ''OVERALL COMMISSION''
                        group by inc.positionseq,
                        inc.payeeseq,
                        inc.processingunitseq,
                        inc.periodseq
                    )
                    group by positionseq,
                    payeeseq,
                    processingunitseq,
                    periodseq
                )tot
                WHERE       pad.payeeseq = tot.payeeseq
                AND pad.positionseq = tot.positionseq
                AND pad.processingunitseq = tot.processingunitseq
                and pad.periodseq = tot.periodseq
                and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'ALLCVPERACHIEVED',i.product) */
                                                 /* ORIGSQL: fungenericattribute(vrptname,'ALLACHIEVEMENT',i.product) */

                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert overall commission completed',NU(...) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert overall commission completed', NULL, :v_sql);

                /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
                EXECUTE IMMEDIATE :v_sql;

                /* ORIGSQL: COMMIT; */
                COMMIT;

                v_sql = 'MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
                (select mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    max(case when rmap.rptcolumnname = ''ALLACTUALWD'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ALLACTUALWD', :i.product),'') ||' end) ALLACTUALWD,
                    sum(case when rmap.rptcolumnname = ''ALLMULTIPLIER'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ALLMULTIPLIER', :i.product),'') ||' end) ALLMULTIPLIER,
                    sum(case when rmap.rptcolumnname = ''ALLTOTAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ALLTOTAL', :i.product),'') ||' end) ALLTOTAL
                    from rpt_base_incentive mes, rpt_mapping rmap
                    where mes.name in rmap.rulename
                    and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                    and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                    and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                    and rmap.product = '''||IFNULL(:i.product,'')||'''
                    and rmap.allgroups = ''OVERALL COMMISSION''
                    group by mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                mes.periodseq)qtr
                on (rpt.processingunitseq = qtr.processingunitseq
                    and rpt.periodseq = qtr.periodseq
                    and rpt.positionseq = qtr.positionseq
                    and rpt.payeeseq = qtr.payeeseq
                    and rpt.product = ''' ||IFNULL(:i.product,'')||'''
                    and rpt.allgroups = ''OVERALL COMMISSION''
                )
                when matched then update set rpt.CONNTARGET = (qtr.ALLACTUALWD*100),
                rpt.MULTIPLIER = (qtr.ALLMULTIPLIER*100),
                rpt.TOTAL = qtr.ALLTOTAL,
                rpt.payable_flag = ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'';  /* ORIGSQL: fungenericattribute(vrptname,'ALLTOTAL',i.product) */
                                                                                   /* ORIGSQL: fungenericattribute(vrptname,'ALLMULTIPLIER',i.product) */
                                                                                   /* ORIGSQL: fungenericattribute(vrptname,'ALLACTUALWD',i.product) */

                /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'update for overall commission completed(...) */
                CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'update for overall commission completed', NULL, :v_sql);

                /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
                EXECUTE IMMEDIATE :v_sql;

                /* ORIGSQL: COMMIT; */
                COMMIT;
            END FOR;  /* ORIGSQL: End Loop; */

            -- OVERALL COMMISSION

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Earned Commission',NULL,NULL) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Earned Commission', NULL, NULL);

            --OVERALL COMMISSION

            v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                titlename,
                loaddttm,
                allgroups,
                product,
                TOTAL,
                payable_flag
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''51'' reportcode,
            ''99'' sectionid,
            ''04OVERALL COMMISSION'' sectionname,
            ''99'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''OVERALL COMMISSION'' allgroups,
            ''Earned Commission'' product,
            EARNCOMM,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
            FROM   rpt_base_padimension pad,
            (
                select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                sum(case when rmap.rptcolumnname = ''EARNCOMM'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'EARNCOMM'),'') ||' end) EARNCOMM
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
            and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'EARNCOMM') */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Earned comission completed',NULL(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Earned comission completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'insert Advance Payment Adjustment',NULL(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'insert Advance Payment Adjustment', NULL, NULL);

            v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                titlename,
                loaddttm,
                allgroups,
                product,
                TOTAL,
                payable_flag
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''51'' reportcode,
            ''1'' sectionid,
            ''05COMMISSION ADJUSTMENT '' sectionname,
            ''05'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''COMMISSION ADJUSTMENT'' allgroups,
            ''Advance Payment Adjustment'' product,
            (nvl(ADVADJTOT,0)*-1) TOTAL,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
            FROM   rpt_base_padimension pad,
            (
                select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                sum(case when rmap.rptcolumnname = ''ADVADJTOT'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ADVADJTOT', 'Advance Payment Adjustment'),'') ||' end) ADVADJTOT
                from cs_incentive mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                and mes.periodseq in
                (select mon.periodseq from cs_period mon, cs_period qtr
                    where mon.removedate = ''01-JAN-2200''
                    and mon.parentseq = qtr.periodseq
                    and mon.removedate = qtr.removedate
                    and mon.calendarseq = qtr.calendarseq
                    and mon.parentseq in (select parentseq from cs_period
                        where periodseq = ' ||IFNULL(:vperiodseq,'')||'
                        and calendarseq = '||IFNULL(:vcalendarseq,'')||'
                    and removedate = '''||'01-JAN-2200'||''')
                    and mon.periodseq < ' ||IFNULL(:vperiodseq,'')||')
                and mes.processingunitseq = ' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.allgroups = ''COMMISSION ADJUSTMENT''
                group by mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq(+)
            AND pad.positionseq = mes.positionseq(+)
            AND pad.processingunitseq = mes.processingunitseq(+)
            and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'ADVADJTOT','Advance Payment Adjustment') */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'insert Advance Payment Adjustment compl(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'insert Advance Payment Adjustment completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Advance Protected Commission (Ne(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Advance Protected Commission (New Staff) Commission', NULL, NULL);

            v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                titlename,
                loaddttm,
                allgroups,
                product,
                TOTAL,
                payable_flag
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''51'' reportcode,
            ''1'' sectionid,
            ''05COMMISSION ADJUSTMENT '' sectionname,
            ''06'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''COMMISSION ADJUSTMENT'' allgroups,
            ''Advance Protected Commission (New Staff)'' product,
            (nvl(ADJPRDTOTAL,0)*-1) TOTAL,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
            FROM   rpt_base_padimension pad,
            (
                select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                sum(case when rmap.rptcolumnname = ''ADJPRDTOTAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ADJPRDTOTAL', 'Advance Protected Commission (New Staff)'),'') ||' end) ADJPRDTOTAL
                from rpt_base_incentive mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.allgroups = ''COMMISSION ADJUSTMENT''
                group by mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq(+)
            AND pad.positionseq = mes.positionseq(+)
            AND pad.processingunitseq = mes.processingunitseq(+)
            and pad.periodseq = mes.periodseq(+)
            and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'ADJPRDTOTAL','Advance Protected Commission (New St(...) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Advance Protected Commission (Ne(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Advance Protected Commission (New Staff) completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Payment Adjustment Commission',N(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Payment Adjustment Commission', NULL, NULL);

            v_sql = 'INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
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
                titlename,
                loaddttm,
                allgroups,
                product,
                TOTAL,
                payable_flag
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''51'' reportcode,
            ''1'' sectionid,
            ''05COMMISSION ADJUSTMENT '' sectionname,
            ''06'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''COMMISSION ADJUSTMENT'' allgroups,
            ''Payment Adjustment'' product,
            nvl(ADJTOTAL,0) TOTAL,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||'
            FROM   rpt_base_padimension pad,
            (
                select mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq,
                sum(case when rmap.rptcolumnname = ''ADJTOTAL'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ADJTOTAL', 'Payment Adjustment'),'') ||' end) ADJTOTAL
                from rpt_base_deposit mes, rpt_mapping rmap
                where mes.name in rmap.rulename
                and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
                and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
                and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.allgroups = ''COMMISSION ADJUSTMENT''
                group by mes.positionseq,
                mes.payeeseq,
                mes.processingunitseq,
                mes.periodseq
            )mes
            WHERE       pad.payeeseq = mes.payeeseq(+)
            AND pad.positionseq = mes.positionseq(+)
            AND pad.processingunitseq = mes.processingunitseq(+)
            and pad.periodseq = mes.periodseq(+)
            and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'ADJTOTAL','Payment Adjustment') */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Insert Payment Adjustment Commission co(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert Payment Adjustment Commission completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCE' not found */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCEPAYMENTTRACE' not found */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYMENT' not found */
            /* ORIGSQL: INSERT INTO STELEXT.RPT_PAYEE_MTHQTR_ACHIVEMENT (tenantid, positionseq, payeeseq(...) */
            INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
                (
                    tenantid,
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
                    titlename,
                    loaddttm,
                    allgroups,
                    product,
                    TOTAL,
                    payable_flag
                )
                SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, vProcessingUnitRow.processingun(...) */
                    :vTenantId,
                    pad.positionseq,
                    pad.payeeseq,
                    :vProcessingUnitRow.processingunitseq,
                    :vperiodseq,
                    :vPeriodRow.name,
                    :vProcessingUnitRow.name,
                    :vCalendarRow.name,
                    '51' AS reportcode,
                    '01' AS sectionid,
                    '05COMMISSION ADJUSTMENT' AS sectionname,
                    '07' AS sortorder,
                    pad.reporttitle AS titlename,
                    CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                    'COMMISSION ADJUSTMENT' AS allgroups,
                    'Prior Balance Adjustment' AS product,
                    IFNULL(TOTAL,0),  /* ORIGSQL: nvl(TOTAL,0) */
                    :v_payableflag
                FROM
                    /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                    (
                        SELECT   /* ORIGSQL: (select pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(...) */
                            pay.positionseq,
                            pay.payeeseq,
                            pay.processingunitseq,
                            pay.periodseq,
                            SUM(bal.value) AS Total
                        FROM
                            cs_balance bal,
                            cs_balancepaymenttrace baltrace,
                            cs_payment pay
                        WHERE
                            bal.periodseq = baltrace.sourceperiodseq
                            AND baltrace.targetperiodseq = pay.periodseq
                            AND bal.balanceseq = baltrace.balanceseq
                            AND bal.processingunitseq = pay.processingunitseq
                            AND bal.processingunitseq = baltrace.processingunitseq
                            AND bal.positionseq = pay.positionseq
                            AND pay.periodseq = :vperiodseq
                            AND pay.processingunitseq = :vprocessingunitseq
                        GROUP BY
                            pay.positionseq,
                            pay.payeeseq,
                            pay.processingunitseq,
                            pay.periodseq
                    ) AS mes
                RIGHT OUTER JOIN
                    EXT.rpt_base_padimension AS pad
                    ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                    AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                    AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                    AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                WHERE
                    pad.reportgroup = 'BSC';

            /* ORIGSQL: COMMIT; */
            COMMIT; 

            /* ORIGSQL: INSERT INTO STELEXT.RPT_PAYEE_MTHQTR_ACHIVEMENT (tenantid, positionseq, payeeseq(...) */
            INSERT INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
                (
                    tenantid,
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
                    titlename,
                    loaddttm,
                    allgroups,
                    product,
                    TOTAL,
                    payable_flag
                )
                SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, vProcessingUnitRow.processingun(...) */
                    :vTenantId,
                    pad.positionseq,
                    pad.payeeseq,
                    :vProcessingUnitRow.processingunitseq,
                    :vperiodseq,
                    :vPeriodRow.name,
                    :vProcessingUnitRow.name,
                    :vCalendarRow.name,
                    '51' AS reportcode,
                    '99' AS sectionid,
                    '05COMMISSION ADJUSTMENT' AS sectionname,
                    '99' AS sortorder,
                    pad.reporttitle AS titlename,
                    CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                    'COMMISSION ADJUSTMENT' AS allgroups,
                    'Commission Final payout' AS product,
                    IFNULL(TOTAL,0) AS TOTAL,  /* ORIGSQL: nvl(TOTAL,0) */
                    :v_payableflag
                FROM
                    /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                    (
                        SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                            mes.positionseq,
                            mes.payeeseq,
                            mes.processingunitseq,
                            mes.periodseq,
                            SUM(value) AS Total
                        FROM
                            cs_payment mes
                        WHERE
                            mes.periodseq = :vperiodseq
                            AND mes.processingunitseq = :vprocessingunitseq
                        GROUP BY
                            mes.positionseq,
                            mes.payeeseq,
                            mes.processingunitseq,
                            mes.periodseq
                    ) AS mes
                RIGHT OUTER JOIN
                    EXT.rpt_base_padimension AS pad
                    ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                    AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                    AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                    AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                WHERE
                    pad.reportgroup = 'BSC';

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
            /* ORIGSQL: MERGE INTO STELEXT.RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using (SELECT mes.positionseq(...) */
            MERGE INTO EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT AS rpt
                /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BASE_CREDIT' not found */
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
                        EXT.rpt_base_credit mes
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
                    AND rpt.sectionname = '05COMMISSION ADJUSTMENT'
                AND rpt.sectionid = '99')
            WHEN MATCHED THEN
                UPDATE SET rpt.remarks = qtr.remarks;
        END IF;
        -- Period shortname end

        --hided by Maria for OTC quarterly issue
        /*if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Point Payout only applicable to Quaterly periods
        
          v_sql :=
          'MERGE INTO RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
                        (select mes.positionseq,
                                   mes.payeeseq,
                                   mes.processingunitseq,
                                   mes.periodseq,
                                   sum(case when rmap.rptcolumnname = ''OTC'' then '||fungenericattribute(vrptname,'OTC')||' end) OTC
                            from cs_incentive mes, rpt_mapping rmap
                            where mes.name in rmap.rulename
             and rmap.reportname = '''||vrptname||'''
             and mes.processingunitseq = '||vprocessingunitseq||'
             and mes.periodseq in (select mon.periodseq from cs_period mon, cs_period qtr
                                                          where mon.removedate = ''01-JAN-2200''
                 and mon.parentseq = qtr.periodseq
                 and mon.removedate = qtr.removedate
                 and mon.calendarseq = qtr.calendarseq
                 and mon.parentseq in (select parentseq from cs_period
                                                                                        where periodseq = '||vperiodseq||'
                     and calendarseq = '||vcalendarseq||'
                 and removedate = '''||'01-JAN-2200'||''')
                                                                              )
                            group by mes.positionseq,
                                     mes.payeeseq,
                                     mes.processingunitseq,
                                 mes.periodseq)qtr
         on (rpt.processingunitseq = qtr.processingunitseq
             and rpt.periodseq = qtr.periodseq
             and rpt.positionseq = qtr.positionseq
         and rpt.payeeseq = qtr.payeeseq)
          when matched then update set rpt.OTC=(qtr.OTC*3)';   --hided by Maria for OTC quarterly issue
        else*/

        v_sql = 'MERGE INTO RPT_PAYEE_MTHQTR_ACHIVEMENT rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            sum(case when rmap.rptcolumnname = ''OTC'' then '||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'OTC'),'') ||' end) OTC
            from rpt_base_incentive mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
        mes.periodseq)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
        and rpt.payeeseq = qtr.payeeseq)
        when matched then update set rpt.OTC=qtr.OTC';  /* ORIGSQL: fungenericattribute(vrptname,'OTC') */

        -- End if;  -- OTC if    --hided by Maria for OTC quarterly issue
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'OTC Update',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OTC Update', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: UPDATE RPT_PAYEE_MTHQTR_ACHIVEMENT SET TARGETACHIEVE = (SELECT MAX(CVACTUAL) FRO(...) */
        UPDATE EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT 
            SET
            /* ORIGSQL: TARGETACHIEVE = */
            TARGETACHIEVE = (
                SELECT   /* ORIGSQL: (SELECT MAX(CVACTUAL) FROM RPT_PAYEE_MTHQTR_ACHIVEMENT WHERE processingunitseq =(...) */
                    MAX(CVACTUAL)
                FROM
                    EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
                WHERE
                    processingunitseq = :vprocessingunitseq
                    AND periodseq = :vperiodseq
                    AND product = 'INDIVIDUAL'
            )
        FROM
            EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
        WHERE
            processingunitseq = :vprocessingunitseq
            AND periodseq = :vperiodseq
            AND allgroups IN ('INDIVIDUAL ACHIEVEMENT');

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: UPDATE RPT_PAYEE_MTHQTR_ACHIVEMENT SET TARGETACHIEVE = (SELECT MAX(CVACTUAL) FRO(...) */
        UPDATE EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT 
            SET
            /* ORIGSQL: TARGETACHIEVE = */
            TARGETACHIEVE = (
                SELECT   /* ORIGSQL: (SELECT MAX(CVACTUAL) FROM RPT_PAYEE_MTHQTR_ACHIVEMENT WHERE processingunitseq =(...) */
                    MAX(CVACTUAL)
                FROM
                    EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
                WHERE
                    processingunitseq = :vprocessingunitseq
                    AND periodseq = :vperiodseq
                    AND product = 'TEAM'
            )
        FROM
            EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
        WHERE
            processingunitseq = :vprocessingunitseq
            AND periodseq = :vperiodseq
            AND allgroups IN ('TEAM ACHIEVEMENT');

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: UPDATE RPT_PAYEE_MTHQTR_ACHIVEMENT SET SORTORDER =99, SECTIONID =99 WHERE proces(...) */
        UPDATE EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
            SET
            /* ORIGSQL: SORTORDER = */
            SORTORDER = 99,
            /* ORIGSQL: SECTIONID = */
            SECTIONID = 99
        FROM
            EXT.RPT_PAYEE_MTHQTR_ACHIVEMENT
        WHERE
            processingunitseq = :vprocessingunitseq
            AND periodseq = :vperiodseq
            AND product IN ('POINTS PAYOUT','TEAM PAYOUT');

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'All Insert completed',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'All Insert completed', NULL, NULL);

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
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName); Sanjay Analyze is nit required

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END