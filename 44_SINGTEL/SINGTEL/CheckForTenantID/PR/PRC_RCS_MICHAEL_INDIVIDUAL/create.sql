CREATE PROCEDURE EXT.PRC_RCS_MICHAEL_INDIVIDUAL
(
    IN vrptname VARCHAR(200),   /* RESOLVE: Identifier not found: Table/Column 'rpt_sts_mapping.reportname' not found (for %TYPE declaration) */
                                                   /* RESOLVE: Datatype unresolved: Datatype (rpt_sts_mapping.reportname%TYPE) not resolved for parameter 'PRC_RCS_MICHAEL_INDIVIDUAL.vrptname' */
                                                   /* ORIGSQL: vrptname IN rpt_sts_mapping.reportname%TYPE */
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_RCS_MICHAEL_INDIVIDUAL.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_RCS_MICHAEL_INDIVIDUAL.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                  /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_RCS_MICHAEL_INDIVIDUAL.vcalendarseq' */
                                                  /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
    IN vPeriodtype VARCHAR(50)      /* RESOLVE: Identifier not found: Table/Column 'rpt_sts_mapping.REPORT_FREQUENCY' not found (for %TYPE declaration) */
                                                              /* RESOLVE: Datatype unresolved: Datatype (rpt_sts_mapping.REPORT_FREQUENCY%TYPE) not resolved for parameter 'PRC_RCS_MICHAEL_INDIVIDUAL.vPeriodtype' */
                                                              /* ORIGSQL: vPeriodtype IN rpt_sts_mapping.REPORT_FREQUENCY%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    -- DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();
    -- DECLARE PKG_REPORTING_EXTRACT_R2__vcredittypeid_PayAdj VARCHAR(255); /* package/session variable */
    


    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    --_RCS_,_STS_,_DS_
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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_RCS_MICHAEL_INDIVIDUAL');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_RCS_MICHAEL_INDIVIDUAL') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_STSRCSDC_INDIVIDUAL';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_STSRCSDC_INDIVIDUAL'; */
    DECLARE vTenantId VARCHAR(4) = SUBSTRING(SESSION_USER,1,4);  /* ORIGSQL: vTenantId VARCHAR2(4) := SUBSTR(USER, 1, 4) ; */
    DECLARE vExtUser VARCHAR(7) = IFNULL(:vTenantId,'') || 'EXT';  /* ORIGSQL: vExtUser VARCHAR2(7) := vTenantId || 'EXT'; */
    DECLARE vSubPartitionPrefix VARCHAR(30) = 'P_';  /* ORIGSQL: vSubPartitionPrefix VARCHAR2(30) := 'P_'; */
    DECLARE vSubPartitionName VARCHAR(30);  /* ORIGSQL: vSubPartitionName VARCHAR2(30); */
    DECLARE vPeriodRow ROW LIKE CS_PERIOD;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PERIOD' not found (for %ROWTYPE declaration) */
    DECLARE vCalendarRow ROW LIKE CS_CALENDAR;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_CALENDAR' not found (for %ROWTYPE declaration) */
    DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PROCESSINGUNIT' not found (for %ROWTYPE declaration) */
    DECLARE vCurYrStartDate TIMESTAMP;  /* ORIGSQL: vCurYrStartDate DATE; */
    DECLARE vCurYrEndDate TIMESTAMP;  /* ORIGSQL: vCurYrEndDate DATE; */
    DECLARE v_envdtl VARCHAR(1000);  /* ORIGSQL: v_envdtl VARCHAR2(1000); */
    DECLARE v_sql NCLOB;  /* ORIGSQL: v_sql LONG; */
    DECLARE v_payable SMALLINT;  /* ORIGSQL: v_payable NUMBER(1); */
    DECLARE v_UserGroup VARCHAR(1) = 'N';  /* ORIGSQL: v_UserGroup VARCHAR2(1) := 'N'; */
    DECLARE v_payableflag SMALLINT;  /* ORIGSQL: v_payableflag NUMBER(1); */
    DECLARE v_reportgroup VARCHAR(127);  /* ORIGSQL: v_reportgroup VARCHAR2(127); */
    DECLARE v_classifierid NVARCHAR(127);  /* ORIGSQL: v_classifierid NVARCHAR2(127); */
    DECLARE v_user VARCHAR(127);  /* ORIGSQL: v_user VARCHAR2(127); */
    DECLARE vcredittypeid_PayAdj NVARCHAR(50);
    DECLARE cEndofTime date;
  

    ----------INDIVIDUAL PRODUCT-MEASUREMENT ... INDIVIDUAL



    /* ORIGSQL: FOR i IN (SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptna(...) */
    DECLARE CURSOR dbmtk_cursor_8623
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_STS_MAPPING' not found */

        SELECT   /* ORIGSQL: SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptname AND all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'INDIVIDUAL PRODUCTS'
            AND PRODUCT IS NOT NULL;

    ----------TEAM PRODUCT-MEASUREMENT ... TEAM
    /* ORIGSQL: FOR i IN (SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptna(...) */
    DECLARE CURSOR dbmtk_cursor_8626
    FOR 
        SELECT   /* ORIGSQL: SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptname AND all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'TEAM PRODUCTS'
            AND PRODUCT IS NOT NULL;

    ----------TEAM AVERAGE-MEASUREMENT ... TEAM
    /* ORIGSQL: FOR i IN (SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptna(...) */
    DECLARE CURSOR dbmtk_cursor_8629
    FOR 
        SELECT   /* ORIGSQL: SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptname AND all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'TEAM AVERAGE';

    ---ADJUST COMMISSION PRODUCTS- Payment Adjustment  -Deposit

    /* ORIGSQL: FOR i IN (SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptna(...) */
    DECLARE CURSOR dbmtk_cursor_8632
    FOR 
        SELECT   /* ORIGSQL: SELECT DISTINCT product FROM rpt_sts_mapping WHERE reportname = vrptname AND all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'ADJUST COMMISSION'
            AND product <> 'CE Adjustment';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            vSQLERRM = SUBSTRING(::SQL_ERROR_MESSAGE,1,3900);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 3900) */

            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'ERROR', NULL, vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ERROR', NULL, :vSQLERRM);

            /* ORIGSQL: raise_application_error(-20911, 'Error raised: ' || vprocname || ' Failed: ' || (...) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT =
            'Error raised: '
            || IFNULL(:vProcName,'')
            || ' Failed: '
            -- ||

            -- DBMS_UTILITY.FORMAT_ERROR_BACKTRACE

            || ' - '
            || IFNULL(:vSQLERRM,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;

        /* initialize session variables, if not yet done */
        
        
    select * into vPeriodRow from cs_period where periodseq = :vperiodseq and removedate > current_date; 
    /* initialize session variables, if not yet done */
    CALL EXT.init_session_global(); -- Sanjay need to set the session context.
    /* retrieve the package/session variables referenced in this procedure */
    SELECT SESSION_CONTEXT('vcredittypeid_PayAdj') INTO vcredittypeid_PayAdj FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('cEndofTime') INTO cEndofTime FROM SYS.DUMMY ;
        
        
        --!!!!!!The below truncate and variable initialization will be executed in rpt_reporting_extract.prc_driver. Will remove unnecessary code after initial testing.
        /* ORIGSQL: prc_logevent (NULL, vProcName, 'Begin procedure', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(NULL, :vProcName, 'Begin procedure', NULL, :vSQLERRM);
        --------Add subpartitions to report table if needed----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AddTableSubpartition (vExtUser, vTCTemplateTable, v(...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AddTableSubpartition(
        --         :vExtUser,
        --         :vTCTemplateTable,
        --         :vTCSchemaName,
        --         :vTenantId,
        --         :vprocessingunitseq,
        --         :vperiodseq,
        --         :vRptTableName
        --     );---Deepan: Partition Not required

        --------Find subpartition name------------------------------------------------------------------------------------

        -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  --/*Deepan : Partition Not required*/ /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);/*Deepan : Partition Not required*/

        --------Truncate report table subpartition------------------------------------------------------------------------
        --pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName,
        --                                                   vSubpartitionName);
        --Since Deleting the records using reportgroup

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);/*Deepan : Partition Not required*/

        --------Turn on Parallel DML---------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */

        --------Initialize variables---------------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (NULL, vProcName, 'Setting up variables', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(NULL, :vProcName, 'Setting up variables', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */

        SELECT
            per.*
        INTO
            vPeriodRow
        FROM
            cs_period per
        WHERE
            per.removedate = :cEndofTime  /* ORIGSQL: pkg_reporting_extract_r2.cEndofTime */
            AND per.periodseq = :vperiodseq;

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PROCESSINGUNIT' not found */

        SELECT
            pu.*
        INTO
            vProcessingUnitRow
        FROM
            cs_processingunit pu
        WHERE
            pu.processingunitseq = :vprocessingunitseq;

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CALENDAR' not found */

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
        -----------------FUNCTIONALITY BEGINS HERE-----------
        --------Begin Insert-------------------------------------------------------------------------------
        v_UserGroup = 'N';

        /*
        If vrptname = 'STSPAYEEACHIVEMENT' Then
         v_reportgroup := 'STS';
         v_classifierid:='STS Report Payable';
        elsif vrptname = 'DSPAYEEACHIVEMENT' Then
         v_reportgroup := 'Digital Telesales';
         v_classifierid:='DS Report Payable';
        elsif vrptname = 'RCSPAYEEACHIVEMENT' Then
         v_reportgroup := 'RCS' ;
         v_classifierid:='RCS Report Payable';
        end if;
        */

        v_reportgroup = 'RCSMFONG';

        v_classifierid = 'RCS Report Payable';

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin insert', NULL, vsqlerrm) */
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
            AND Classifierid = :v_classifierid
            AND effectiveenddate > :vPeriodRow.startdate
            AND effectivestartdate < :vPeriodRow.enddate;

        SELECT
            MAX(GENERICATTRIBUTE6)
        INTO
            v_user
        FROM
            EXT.STEL_CLASSIFIER
        WHERE
            CATEGORYTREENAME = 'Reporting Config'
            AND Categoryname = 'RCS User'
            AND Classifierid = 'RCS Fong'
            AND effectiveenddate > :vPeriodRow.startdate
            AND effectivestartdate < :vPeriodRow.enddate;

        SELECT
            MAX('Y') 
        INTO
            v_UserGroup
            /* RESOLVE: Identifier not found: Table/view 'TCMP.CS_PIPELINERUN' not found */
        FROM
            (
                SELECT   /* ORIGSQL: (SELECT RTRIM(REGEXP_SUBSTR(runparameters, '\[boGroupsList\]([^\[]+)', 1, 1, 'i'(...) */
                    IFNULL(RTRIM(SUBSTRING_REGEXPR('\[boGroupsList\]([^\[]+)' FLAG 'i' IN runparameters FROM 1 OCCURRENCE 1),  /* ORIGSQL: REGEXP_SUBSTR(runparameters, '\[boGroupsList\]([^\[]+)', 1, 1, 'i', 1) */
                    ','),'')
                    || ',' AS GroupList
                FROM
                    tcmp.cs_pipelinerun
                WHERE
                    command = 'PipelineRun'
                    AND description LIKE '%ODS%'
                    AND state <> 'Done'
                    AND periodseq = :vperiodseq
                    AND processingunitseq = :vprocessingunitseq
            ) AS dbmtk_corrname_8657
        WHERE
            Grouplist LIKE '%User Group%';

        IF :v_UserGroup = 'Y' 
        THEN
            v_payableflag = :v_payable;
        ELSE 
            v_payableflag = 1;
        END IF;

        -----DELETE EXISTING RECORDS BASED ON REPORT GROUP 

        /* ORIGSQL: DELETE FROM RPT_STSRCSDC_INDIVIDUAL WHERE reportgroup = v_reportgroup AND period(...) */
        DELETE
        FROM
            RPT_STSRCSDC_INDIVIDUAL
        WHERE
            reportgroup = :v_reportgroup
            AND periodseq = :vperiodseq
            AND processingunitseq = :vprocessingunitseq
            AND name = :v_user
            AND FREQUENCY = :vPeriodtype;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        FOR i AS dbmtk_cursor_8623
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Individual PRODUCTS MEASUREMENT(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENTcompleted', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
            (tenantid, positionseq,payeeseq,processingunitseq,periodseq,periodname,processingunitname,calendarname,
                reportcode,sectionid,sectionname,subsectionname,sortorder,titlename,loaddttm,allgroups,geid,name,products,
            CONNWT,CONNTARGET,CONNACTUALS,CONNACTUALTARGET,shopname,teamvisible,payable_flag,reportgroup,FREQUENCY)
            SELECT   ''' 
            || IFNULL(:vTenantId,'')
            || ''',
            pad.positionseq,
            pad.payeeseq,
            '
            || IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')
            || ',
            '
            || IFNULL(:vperiodseq,'')
            || ',
            '''
            || IFNULL(:vPeriodRow.name,'')
            || ''',
            '''
            || IFNULL(:vProcessingUnitRow.name,'')
            || ''',
            '''
            || IFNULL(:vCalendarRow.name,'')
            || ''',
            ''63'' reportcode,
            ''01'' sectionid,
            ''INDIVIDUAL ACHIEVEMENT'' sectionname,
            ''INDIVIDUAL ACHIEVEMENT'' subsectionname,
            ''01'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''INDIVIDUAL PRODUCTS'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            (CONNWT*100) CONNWT,
            CONNTARGET,
            CONNACTUALS,
            CONNACTUALTARGET,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            '
            || IFNULL(TO_VARCHAR(:v_payableflag),'')
            || ' payable_flag,
            '''
            || IFNULL(:v_reportgroup,'')
            || ''' reportgroup,
            '''
            || IFNULL(:vPeriodtype,'')
            || ''' FREQUENCY
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                titlemap.titlename,
                rmap.allgroups allgroups,
                '''
                || IFNULL(:i.product,'')
                || ''' product,
                MAX(case when rmap.rptcolumnname = ''CONNWT'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNWT', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNWT', i.product, vPeriodtype) */
                || ' end) AS CONNWT,
                MAX(case when rmap.rptcolumnname = ''CONNTARGET'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNTARGET', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNTARGET', i.product, vPeriodtype) */
                || ' end) AS CONNTARGET,
                MAX(case when rmap.rptcolumnname = ''CONNACTUALS'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNACTUALS', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNACTUALS', i.product, vPeriodtype) */
                || ' end) AS CONNACTUALS,
                MAX(case when rmap.rptcolumnname = ''CONNACTUALTARGET'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNACTUALTARGET', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNACTUALTARGET', i.product, vPeriodtype) */
                || ' end) AS CONNACTUALTARGET
                FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                AND rmap.reportname= '''
                || IFNULL(:vrptname,'')
                || '''
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.reportname=titlemap.reportname
                and CM.periodseq = '
                || IFNULL(:vperiodseq,'')
                || '
                and CM.processingunitseq = '
                || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
                || '
                and rmap.product = '''
                || IFNULL(:i.product,'')
                || '''
                and rmap.allgroups = ''INDIVIDUAL PRODUCTS''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.reportgroup = '''
            || IFNULL(:v_reportgroup,'')
            || '''
            and pad.POSITIONGA1=''Channel individual Scheme''
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency='''
            || IFNULL(:vPeriodtype,'')
            || '''
            and pad.FULLNAME='''
            || IFNULL(:v_user,'')
            || '''';
            --need to change this pad.POSITIONGA1=''Channel individual Scheme''

            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Individual PRODUCTS MEASUREMENT(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENTcompleted', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: END LOOP; */

        ----------INDIVIDUAL AVERAGE-MEASUREMENT ... INDIVIDUAL POINTS PAYOUT

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Individual AVERAGE MEASUREMENT'(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT', NULL, 'POINTSPAYOUT');
        v_sql = 'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
            subsectionname,
            sortorder,
            titlename,
            loaddttm,
            allgroups,
            geid,
            name,
            products,
            TOTALPRODUCTS,
            PRDTARGET,
            MULTIPLIERPER,
            shopname,
            teamvisible,
            payable_flag,
            reportgroup,
            frequency
        )
        SELECT   ''' 
        || IFNULL(:vTenantId,'')
        || ''',
        pad.positionseq,
        pad.payeeseq,
        '
        || IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')
        || ',
        '
        || IFNULL(:vperiodseq,'')
        || ',
        '''
        || IFNULL(:vPeriodRow.name,'')
        || ''',
        '''
        || IFNULL(:vProcessingUnitRow.name,'')
        || ''',
        '''
        || IFNULL(:vCalendarRow.name,'')
        || ''',
        ''63'' reportcode,
        ''01'' sectionid,
        ''INDIVIDUAL ACHIEVEMENT'' sectionname,
        ''AVERAGE ACHIEVEMENT'' subsectionname,
        ''99'' sortorder,
        pad.reporttitle titlename,
        SYSDATE,
        ''POINTS PAYOUT'' allgroups,
        pad.PARTICIPANTID, --geid
        pad.FULLNAME, --name
        ''POINTSPAYOUT'' product,
        TOTALPRODUCTS,
        PRDTARGET,
        (MULTIPLIERPER*100) MULTIPLIERPER,
        pad.POSITIONGA1 shopname,
        nvl(pad.TITLEGB1,1) teamvisible,
        '
        || IFNULL(TO_VARCHAR(:v_payableflag),'')
        || ' payable_flag,
        '''
        || IFNULL(:v_reportgroup,'')
        || ''' reportgroup,
        '''
        || IFNULL(:vPeriodtype,'')
        || ''' frequency
        FROM   rpt_base_padimension pad,
        (
            SELECT
            CM.positionseq,
            CM.payeeseq,
            CM.processingunitseq,
            CM.periodseq,
            rmap.allgroups allgroups,
            titlemap.titlename,
            MAX(case when rmap.rptcolumnname = ''CONNTARGET'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNTARGET', 'POINTSPAYOUT', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNTARGET', 'POINTSPAYOUT', vPeriodtype) */
            || ' end) AS TOTALPRODUCTS,
            MAX(case when rmap.rptcolumnname = ''CONNACTUALS'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNACTUALS', 'POINTSPAYOUT', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNACTUALS', 'POINTSPAYOUT', vPeriodtype) */
            || ' end) AS PRDTARGET,
            MAX(case when rmap.rptcolumnname = ''CONNACTUALTARGET'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNACTUALTARGET', 'POINTSPAYOUT', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNACTUALTARGET', 'POINTSPAYOUT', vPeriodtype) */
            || ' end) AS MULTIPLIERPER
            FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
            WHERE CM.name in rmap.rulename
            and rmap.product=titlemap.product
            and rmap.allgroups=titlemap.allgroups
            and rmap.report_frequency=titlemap.report_frequency
            and rmap.reportname=titlemap.reportname
            AND rmap.reportname= '''
            || IFNULL(:vrptname,'')
            || '''
            and CM.periodseq = '
            || IFNULL(:vperiodseq,'')
            || '
            and CM.processingunitseq = '
            || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
            || '
            and rmap.product = ''POINTSPAYOUT''
            and rmap.allgroups = ''INDIVIDUAL AVERAGE''
            GROUP BY CM.positionseq,
            CM.payeeseq,
            CM.processingunitseq,
            CM.periodseq,
            rmap.allgroups,titlemap.titlename
        )mes
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.POSITIONTITLE=mes.titlename
        and pad.reportgroup = '''
        || IFNULL(:v_reportgroup,'')
        || '''
        and pad.POSITIONGA1=''Channel individual Scheme''
        and pad.frequency='''
        || IFNULL(:vPeriodtype,'')
        || '''
        and pad.FULLNAME='''
        || IFNULL(:v_user,'')
        || '''';
        --need to change this pad.POSITIONGA1=''Channel individual Scheme''

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Individual AVERAGE MEASUREMENT (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        FOR i AS dbmtk_cursor_8626
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin TEAM PRODUCTS MEASUREMENT compl(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS MEASUREMENT completed', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                CONNWT,
                CONNTARGET,
                CONNACTUALS,
                CONNACTUALTARGET,
                POINTSACTUALS,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   ''' 
            || IFNULL(:vTenantId,'')
            || ''',
            pad.positionseq,
            pad.payeeseq,
            '
            || IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')
            || ',
            '
            || IFNULL(:vperiodseq,'')
            || ',
            '''
            || IFNULL(:vPeriodRow.name,'')
            || ''',
            '''
            || IFNULL(:vProcessingUnitRow.name,'')
            || ''',
            '''
            || IFNULL(:vCalendarRow.name,'')
            || ''',
            ''63'' reportcode,
            ''02'' sectionid,
            ''TEAM ACHIEVEMENT'' sectionname,
            ''TEAM ACHIEVEMENT'' subsectionname,
            ''01'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''TEAM PRODUCTS'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            CONNWT,
            CONNTARGET,
            CONNACTUALS,
            CONNACTUALTARGET,
            POINTSACTUALS,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            '
            || IFNULL(TO_VARCHAR(:v_payableflag),'')
            || ' payable_flag,
            '''
            || IFNULL(:v_reportgroup,'')
            || ''' reportgroup,
            '''
            || IFNULL(:vPeriodtype,'')
            || ''' frequency
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                '''
                || IFNULL(:i.product,'')
                || ''' product,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNWT'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNWT', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMCONNWT', i.product, vPeriodtype) */
                || ' end) AS CONNWT,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNTARGET'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNTARGET', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMCONNTARGET', i.product, vPeriodtype) */
                || ' end) AS CONNTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNACTUALS'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNACTUALS', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMCONNACTUALS', i.product, vPeriodtype) */
                || ' end) AS CONNACTUALS,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNACTUALTARGET'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNACTUALTARGET', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMCONNACTUALTARGET', i.product, vPeriodtype) */
                || ' end ) AS CONNACTUALTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSACTUALS'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSACTUALS', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMPOINTSACTUALS', i.product, vPeriodtype) */
                || ' end) AS POINTSACTUALS
                FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                and rmap.product=titlemap.product
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.allgroups=titlemap.allgroups
                and rmap.reportname=titlemap.reportname
                AND rmap.reportname= '''
                || IFNULL(:vrptname,'')
                || '''
                and CM.periodseq = '
                || IFNULL(:vperiodseq,'')
                || '
                and CM.processingunitseq = '
                || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
                || '
                and rmap.product = '''
                || IFNULL(:i.product,'')
                || '''
                and rmap.allgroups = ''TEAM PRODUCTS''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.reportgroup = '''
            || IFNULL(:v_reportgroup,'')
            || '''
            and pad.frequency='''
            || IFNULL(:vPeriodtype,'')
            || '''
            and pad.POSITIONTITLE=mes.titlename
            and pad.POSITIONGA1=''Channel individual Scheme''
            and pad.FULLNAME='''
            || IFNULL(:v_user,'')
            || '''';

            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin TEAM PRODUCTS MEASUREMENTcomple(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS MEASUREMENTcompleted', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: END LOOP; */

        FOR i AS dbmtk_cursor_8629
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin TEAM AVERAGE MEASUREMENT', NULL(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM AVERAGE MEASUREMENT', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                GAPER,
                TOTALPRODUCTS,
                PRDTARGET,
                MULTIPLIERPER,
                POINTSTARGET,
                POINTSACTUALS,
                POINTSACHIEVEDPER,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   ''' 
            || IFNULL(:vTenantId,'')
            || ''',
            pad.positionseq,
            pad.payeeseq,
            '
            || IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')
            || ',
            '
            || IFNULL(:vperiodseq,'')
            || ',
            '''
            || IFNULL(:vPeriodRow.name,'')
            || ''',
            '''
            || IFNULL(:vProcessingUnitRow.name,'')
            || ''',
            '''
            || IFNULL(:vCalendarRow.name,'')
            || ''',
            ''63'' reportcode,
            ''02'' sectionid,
            ''TEAM ACHIEVEMENT'' sectionname,
            ''AVERAGE ACHIEVEMENT'' subsectionname,
            ''99'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''TEAM PAYOUT'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            (GAPER*100) GAPER,
            TOTALPRODUCTS,
            PRDTARGET,
            (MULTIPLIERPER*100) MULTIPLIERPER,
            POINTSTARGET,
            POINTSACTUALS,
            (POINTSACHIEVEDPER*100) POINTSACHIEVEDPER,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            '
            || IFNULL(TO_VARCHAR(:v_payableflag),'')
            || ' payable_flag,
            '''
            || IFNULL(:v_reportgroup,'')
            || ''' reportgroup,
            '''
            || IFNULL(:vPeriodtype,'')
            || ''' frequency
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                '''
                || IFNULL(:i.product,'')
                || ''' product,
                MAX(case when rmap.rptcolumnname = ''TEAMGAPER'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMGAPER', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMGAPER', i.product, vPeriodtype) */
                || ' end) AS GAPER,
                MAX(case when rmap.rptcolumnname = ''TEAMTOTALPRODUCTS'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMTOTALPRODUCTS', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMTOTALPRODUCTS', i.product, vPeriodtype) */
                || ' end) AS TOTALPRODUCTS,
                MAX(case when rmap.rptcolumnname = ''TEAMPRDTARGET'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPRDTARGET', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMPRDTARGET', i.product, vPeriodtype) */
                || ' end) AS PRDTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMMULTIPLIERPER'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMMULTIPLIERPER', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMMULTIPLIERPER', i.product, vPeriodtype) */
                || ' end) AS MULTIPLIERPER,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSTARGET'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSTARGET', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMPOINTSTARGET', i.product, vPeriodtype) */
                || ' end) AS POINTSTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSACTUALS'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSACTUALS', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMPOINTSACTUALS', i.product, vPeriodtype) */
                || ' end) AS POINTSACTUALS,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSACHIEVEDPER'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSACHIEVEDPER', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMPOINTSACHIEVEDPER', i.product, vPeriodtype) */
                || ' end) AS POINTSACHIEVEDPER
                FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.reportname=titlemap.reportname
                AND rmap.reportname= '''
                || IFNULL(:vrptname,'')
                || '''
                and CM.periodseq = '
                || IFNULL(:vperiodseq,'')
                || '
                and CM.processingunitseq = '
                || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
                || '
                and rmap.product = '''
                || IFNULL(:i.product,'')
                || '''
                and rmap.allgroups = ''TEAM AVERAGE''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.reportgroup = '''
            || IFNULL(:v_reportgroup,'')
            || '''
            and pad.frequency='''
            || IFNULL(:vPeriodtype,'')
            || '''
            and pad.POSITIONGA1=''Channel individual Scheme''
            and pad.FULLNAME='''
            || IFNULL(:v_user,'')
            || '''';

            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin TEAM AVERAGE MEASUREMENT comple(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM AVERAGE MEASUREMENT completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: END LOOP; */

        /*
          ----TEAM AVERAGE INCENTIVE
         prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM AVERAGE INCENTIVE',NULL,'POINTSPAYOUT');
          v_sql :=
         'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
                        (select inc.positionseq,
                                   inc.payeeseq,
                                   inc.processingunitseq,
                                   inc.periodseq,
                                   MAX(case when rmap.rptcolumnname = ''TEAMGAPER'' then '||fun_sts_mapping(vrptname,'TEAMGAPER','POINTSPAYOUT')||' end) AS GAPER
                            from rpt_base_incentive inc, rpt_sts_mapping rmap
                            where inc.processingunitseq = '||vprocessingunitseq||'
             and  inc.name in rmap.rulename
             and rmap.reportname =  '''||vrptname||'''
             and inc.periodseq = '||vperiodseq||'
             and rmap.product = ''POINTSPAYOUT''
             and rmap.allgroups=''TEAM AVERAGE''
                            GROUP BY inc.positionseq,
                              inc.payeeseq,
                              inc.processingunitseq,
                              inc.periodseq,
                          rmap.allgroups)qtr
         on (rpt.processingunitseq = qtr.processingunitseq
             and rpt.periodseq = qtr.periodseq
             and rpt.positionseq = qtr.positionseq
             and rpt.payeeseq = qtr.payeeseq
         and rpt.allgroups = ''TEAM AVERAGE'')
        when matched then update set rpt.GAPER=(qtr.GAPER*100)';
        
        prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS INCENTIVE completed',NULL,v_sql);
        
          EXECUTE IMMEDIATE v_sql;
          COMMIT;
         */

        -- individual   ...Individual

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Individual PRODUCTS INCENTIVE',(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            MAX(case when rmap.rptcolumnname = ''OTC'' then ' 
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OTC', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'OTC', NULL, vPeriodtype) */
            || ' end) AS OTC,
            MAX(case when rmap.rptcolumnname = ''CONNPER1'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNPER1', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNPER1', NULL, vPeriodtype) */
            || ' end) AS CONNPER1,
            MAX(case when rmap.rptcolumnname = ''CONNPER2'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNPER2', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CONNPER2', NULL, vPeriodtype) */
            || ' end) AS CONNPER2,
            MAX(case when rmap.rptcolumnname = ''ACHIEVEMENTPER'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'ACHIEVEMENTPER', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'ACHIEVEMENTPER', NULL, vPeriodtype) */
            || ' end) AS ACHIEVEMENTPER
            from rpt_base_measurement mes, rpt_sts_mapping rmap
            where mes.processingunitseq = '
            || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
            || '
            and mes.name in rmap.rulename
            and rmap.reportname = '''
            || IFNULL(:vrptname,'')
            || '''
            and mes.periodseq = '
            || IFNULL(:vperiodseq,'')
            || '
            and rmap.product IS NULL
            and rmap.allgroups=''INDIVIDUAL PRODUCTS''
            and rmap.report_frequency ='''
            || IFNULL(:vPeriodtype,'')
            || '''
            GROUP BY mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
        rmap.allgroups)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''INDIVIDUAL PRODUCTS''
            and rpt.frequency='''
            || IFNULL(:vPeriodtype,'')
            || '''
        )
        when matched then update set rpt.OTC = qtr.OTC,    rpt.CONNPER=(nvl(qtr.CONNPER1,0)/NULLIF((nvl(qtr.CONNPER1,0)+nvl(qtr.CONNPER2,0)),0)*100),
        rpt.POINTSPER=(nvl(qtr.CONNPER2,0)/NULLIF((nvl(qtr.CONNPER1,0)+nvl(qtr.CONNPER2,0)),0)*100),
        rpt.ACHIEVEMENTPER= ((qtr.ACHIEVEMENTPER)*100)';

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Individual PRODUCTS INCENTIVE c(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ----TEAM PRODUCT INCENTIVE
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin TEAM PRODUCT INCENTIVE', NULL, (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCT INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''TEAMOTC'' then ' 
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OTC', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'OTC', NULL, vPeriodtype) */
            || ' end) AS OTC,
            MAX(case when rmap.rptcolumnname = ''TEAMCONNPER'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNPER', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMCONNPER', NULL, vPeriodtype) */
            || ' end) AS CONNPER,
            MAX(case when rmap.rptcolumnname = ''TEAMPOINTSPER'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSPER', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'TEAMPOINTSPER', NULL, vPeriodtype) */
            || ' end) AS POINTSPER
            from rpt_base_incentive inc, rpt_sts_mapping rmap
            where inc.processingunitseq = '
            || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
            || '
            and  inc.name in rmap.rulename
            and rmap.reportname = '''
            || IFNULL(:vrptname,'')
            || '''
            and inc.periodseq = '
            || IFNULL(:vperiodseq,'')
            || '
            and rmap.product IS NULL
            and rmap.allgroups=''TEAM PRODUCTS''
            and rmap.report_frequency ='''
            || IFNULL(:vPeriodtype,'')
            || '''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
        rmap.allgroups)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups=''TEAM PRODUCTS''
            and rpt.frequency='''
            || IFNULL(:vPeriodtype,'')
        || ''')
        when matched then update set rpt.OTC = qtr.OTC,
        rpt.CONNPER=(nvl(qtr.CONNPER,0)/NULLIF((nvl(qtr.CONNPER,0)+nvl(qtr.POINTSPER,0)),0)*100),
        rpt.POINTSPER=(nvl(qtr.POINTSPER,0)/NULLIF((nvl(qtr.CONNPER,0)+nvl(qtr.POINTSPER,0)),0)*100),
        rpt.ACHIEVEMENTPER= ((nvl(qtr.CONNPER,0) + nvl(qtr.POINTSPER,0))*100)';

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin TEAM PRODUCTS INCENTIVE complet(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS INCENTIVE completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ---GA % Achieved ,Points % achieved ..OVERALL COMMISSION individual
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin OVERALL COMMISSION INDIVIDUAL G(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER', NULL, NULL);

        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_STSRCSDC_INDIVIDUAL' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_STSRCSDC_INDIVIDUAL' not found */

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_PADIMENSION' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_TITLE_PRODUCT_MAPPING' not found */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                GAPER,
                POINTSACHIEVEDPER,
                ACHIEVEMENTPER,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, :vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '59' AS reportcode,
                '03' AS sectionid,
                'OVERALL COMMISSION' AS sectionname,
                'EARNED COMMISSION' AS subsectionname,
                '01' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'EARNED COMMISSION' AS allgroups,
                pad.PARTICIPANTID,/* --geid */  pad.FULLNAME,/* --name */   'Individual' AS products,
                (mes.MULTIPLIERPER) AS GAPER,
                0 AS POINTSACHIEVEDPER,
                (mes_ach.ACHIEVEMENTPER) AS ACHIEVEMENTPER,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1, 1) AS teamvisible,  /* ORIGSQL: NVL(pad.TITLEGB1, 1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (SELECT ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, ind(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        ind.MULTIPLIERPER
                    FROM
                        RPT_STSRCSDC_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'INDIVIDUAL ACHIEVEMENT'
                        AND ind.subsectionname = 'AVERAGE ACHIEVEMENT'
                        AND ind.allgroups = 'POINTS PAYOUT'
                        AND ind.reportgroup = :v_reportgroup
                ) AS mes,
                (
                    SELECT   /* ORIGSQL: (SELECT ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, MAX(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        MAX(ind.ACHIEVEMENTPER) AS ACHIEVEMENTPER
                    FROM
                        RPT_STSRCSDC_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'INDIVIDUAL ACHIEVEMENT'
                        AND ind.allgroups = 'INDIVIDUAL PRODUCTS'
                        AND ind.reportgroup = :v_reportgroup
                    GROUP BY
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq
                ) AS mes_ach
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.payeeseq = mes_ach.payeeseq
                AND pad.positionseq = mes_ach.positionseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Individual'
                AND titlemap.allgroups = 'EARNED COMMISSION'
                AND pad.frequency = titlemap.report_frequency
                AND titlemap.reportname = :vrptname
                AND pad.processingunitseq = mes_ach.processingunitseq
                AND pad.periodseq = mes_ach.periodseq
                AND pad.reportgroup = :v_reportgroup
                AND pad.frequency = :vPeriodtype;

        --need to change

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin OVERALL COMMISSION INDIVIDUAL G(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --ACHIEVEMENTPER,GAPER,POINTSACHIEVEDPER for OVERALL TEAM
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin OVERALL COMMISSION TEAM GAPER,P(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION TEAM GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER', NULL, NULL);   

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                GAPER,
                POINTSACHIEVEDPER,
                ACHIEVEMENTPER,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, :vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '59' AS reportcode,
                '03' AS sectionid,
                'OVERALL COMMISSION' AS sectionname,
                'EARNED COMMISSION' AS subsectionname,
                '01' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'EARNED COMMISSION' AS allgroups,
                pad.PARTICIPANTID,/* --geid */  pad.FULLNAME,/* --name */   'Team' AS products,
                (mes.GAPER) AS GAPER,
                (mes.POINTSACHIEVEDPER) AS POINTSACHIEVEDPER,
                (mes_ach.ACHIEVEMENTPER) AS ACHIEVEMENTPER,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1, 1) AS teamvisible,  /* ORIGSQL: NVL(pad.TITLEGB1, 1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (SELECT ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, ind(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        ind.ACHIEVEMENTPER,
                        ind.GAPER,
                        ind.POINTSACHIEVEDPER
                    FROM
                        RPT_STSRCSDC_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'TEAM ACHIEVEMENT'
                        AND ind.subsectionname = 'AVERAGE ACHIEVEMENT'
                        AND ind.allgroups = 'TEAM PAYOUT'
                        AND ind.reportgroup = :v_reportgroup
                ) AS mes,
                (
                    SELECT   /* ORIGSQL: (SELECT ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, MAX(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        MAX(ind.ACHIEVEMENTPER) AS ACHIEVEMENTPER
                    FROM
                        RPT_STSRCSDC_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'TEAM ACHIEVEMENT'
                        AND ind.allgroups = 'TEAM PRODUCTS'
                        AND ind.reportgroup = :v_reportgroup
                    GROUP BY
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq
                ) AS mes_ach
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.payeeseq = mes_ach.payeeseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Team'
                AND titlemap.allgroups = 'EARNED COMMISSION'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
                AND pad.positionseq = mes_ach.positionseq
                AND pad.processingunitseq = mes_ach.processingunitseq
                AND pad.periodseq = mes_ach.periodseq
                AND pad.reportgroup = :v_reportgroup
                AND pad.frequency = :vPeriodtype;

        --need to change

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin OVERALL COMMISSION TEAM GAPER,P(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION TEAM GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        -----Overall Commission-Actual WD/Plan WD..measurement individual
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Overall Commission Measurement (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission Measurement working day', NULL, 'POINTSPAYOUT');
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            MAX(case when rmap.rptcolumnname = ''OVERALLPER'' then ' 
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OVERALLPER', 'Individual', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'OVERALLPER', 'Individual', vPeriodtype) */
            || ' end) AS OVERALLPER,
            MAX(case when rmap.rptcolumnname = ''BASICSAMT1'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'BASICSAMT1', 'Individual', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'BASICSAMT1', 'Individual', vPeriodtype) */
            || ' end) AS BASICSAMT1,
            MAX(case when rmap.rptcolumnname = ''BASICSAMT2'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'BASICSAMT2', 'Individual', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'BASICSAMT2', 'Individual', vPeriodtype) */
            || ' end) AS BASICSAMT2
            from RPT_BASE_MEASUREMENT mes, rpt_sts_mapping rmap
            where mes.processingunitseq = '
            || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
            || '
            and  mes.name in rmap.rulename
            and rmap.reportname = '''
            || IFNULL(:vrptname,'')
            || '''
            and mes.periodseq = '
            || IFNULL(:vperiodseq,'')
            || '
            and rmap.product = ''Individual''
            and rmap.allgroups=''EARNED COMMISSION''
            and rmap.report_frequency ='''
            || IFNULL(:vPeriodtype,'')
            || '''
            GROUP BY mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
        rmap.allgroups)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.products = ''Individual''
            and rpt.frequency='''
            || IFNULL(:vPeriodtype,'')
        || ''')
        when matched then update set rpt.OVERALLPER = (qtr.OVERALLPER*100),rpt.BASICSAMT=(nvl(qtr.BASICSAMT1,0)*nvl(qtr.BASICSAMT2,0))';

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Overall Commission Measurement workin(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Overall Commission Measurement working day completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        -----Overall Commission-Overall,Basic..measurement TEAM
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Overall Commission Measurement (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission Measurement Overall,Basic', NULL, 'POINTSPAYOUT');
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            MAX(case when rmap.rptcolumnname = ''OVERALLPER'' then ' 
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OVERALLPER', 'Team', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'OVERALLPER', 'Team', vPeriodtype) */
            || ' end) AS OVERALLPER,
            MAX(case when rmap.rptcolumnname = ''BASICSAMT1'' then '
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'BASICSAMT1', 'Team', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'BASICSAMT1', 'Team', vPeriodtype) */
            || ' end) AS BASICSAMT1
            from RPT_BASE_INCENTIVE mes, rpt_sts_mapping rmap
            where mes.processingunitseq = '
            || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
            || '
            and  mes.name in rmap.rulename
            and rmap.reportname = '''
            || IFNULL(:vrptname,'')
            || '''
            and mes.periodseq = '
            || IFNULL(:vperiodseq,'')
            || '
            and rmap.product = ''Team''
            and rmap.allgroups=''EARNED COMMISSION''
            and rmap.report_frequency ='''
            || IFNULL(:vPeriodtype,'')
            || '''
            GROUP BY mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
        rmap.allgroups)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.products = ''Team''
            and rpt.frequency='''
            || IFNULL(:vPeriodtype,'')
        || ''')
        when matched then update set rpt.OVERALLPER = (qtr.OVERALLPER*100), rpt.BASICSAMT=qtr.BASICSAMT1';

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Overall Commission Measurement Overal(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Overall Commission Measurement Overall,Basic completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Overall Commission-Actual WD/Plan WD
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin Overall Commission Measurement (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission Measurement working day', NULL, 'POINTSPAYOUT');
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            MAX(case when rmap.rptcolumnname = ''WDAYSPER'' then ' 
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'WDAYSPER', 'Team', :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'WDAYSPER', 'Team', vPeriodtype) */
            || ' end) AS WDAYSPER
            from RPT_BASE_MEASUREMENT mes, rpt_sts_mapping rmap
            where mes.processingunitseq = '
            || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
            || '
            and  mes.name in rmap.rulename
            and rmap.reportname = '''
            || IFNULL(:vrptname,'')
            || '''
            and mes.periodseq = '
            || IFNULL(:vperiodseq,'')
            || '
            and rmap.product = ''Team''
            and rmap.allgroups=''EARNED COMMISSION''
            and rmap.report_frequency ='''
            || IFNULL(:vPeriodtype,'')
            || '''
            GROUP BY mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
        rmap.allgroups)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.products = ''Team''
            and rpt.frequency='''
            || IFNULL(:vPeriodtype,'')
        || ''')
        when matched then update set rpt.WDAYSPER = (qtr.WDAYSPER*100)';

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Overall Commission Measurement workin(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Overall Commission Measurement working day completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /*
         -----Actual WD/Plan WD, Conn % Achieved, Total
        
          prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL MEASUREMENT',NULL,'Individual');
         v_sql :=
         'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                                                     subsectionname,
                                                     sortorder,
                                                     titlename,
                                                     loaddttm,
                                                     allgroups,
                                                     geid,
                                                     name,
                                                     products,
                                                     WDAYSPER,
                                                     POINTSACHIEVEDPER,
                                                     shopname,
                                                     teamvisible,
                                                     payable_flag,
                                                     reportgroup
                                                 )
         SELECT   '''||vTenantID||''',
                  pad.positionseq,
                  pad.payeeseq,
                  '||:vProcessingUnitRow.processingunitseq||',
                  '||vperiodseq||',
                  '''||vPeriodRow.name||''',
                  '''||vProcessingUnitRow.name||''',
                  '''||vCalendarRow.name||''',
                  ''63'' reportcode,
                  ''01'' sectionid,
                  ''OVERALL COMMISSION'' sectionname,
                  ''EARNED COMMISSION'' subsectionname,
                  ''99'' sortorder,
                  pad.reporttitle titlename,
                  SYSDATE,
                  ''EARNED COMMISSION'' allgroups,
                  pad.PARTICIPANTID, --geid
                  pad.FULLNAME, --name
                  ''Individual'' product,
                  WDAYSPER,
                  POINTSACHIEVEDPER,
                  pad.POSITIONGA1 shopname,
                  nvl(pad.TITLEGB1,1) teamvisible,
                  '||v_payableflag||' payable_flag,
                  '''||v_reportgroup||''' reportgroup
           FROM   rpt_base_padimension pad,
                  (
                           SELECT
                             CM.positionseq,
                             CM.payeeseq,
                             CM.processingunitseq,
                             CM.periodseq,
                             rmap.allgroups allgroups,
                             MAX(case when rmap.rptcolumnname = ''WDAYSPER'' then '||fun_sts_mapping(vrptname,'WDAYSPER','Individual')||' end) AS WDAYSPER,
                             MAX(case when rmap.rptcolumnname = ''CONPERACHIEVED'' then '||fun_sts_mapping(vrptname,'CONPERACHIEVED','Individual')||' end) AS POINTSACHIEVEDPER
                           FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap
                             WHERE CM.name in rmap.rulename
             AND rmap.reportname= '''||vrptname||'''
             and CM.periodseq = '||vperiodseq||'
             and CM.processingunitseq = '||vprocessingunitseq||'
             and rmap.product = ''Individual''
             and rmap.allgroups = ''INDIVIDUAL AVERAGE''
                           GROUP BY CM.positionseq,
                             CM.payeeseq,
                             CM.processingunitseq,
                             CM.periodseq,
                             rmap.allgroups
                   )mes
          WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         and pad.periodseq = mes.periodseq
         and pad.reportgroup = '''||v_reportgroup||'''
         and pad.FULLNAME='''||v_user||'''
         and pad.POSITIONGA1=''Channel individual Scheme''';--need to change
        
         prc_logevent (:vPeriodRow.name,vProcName,'OVERALL MEASUREMENT completed',NULL,v_sql);
        
         EXECUTE IMMEDIATE v_sql;
        
         COMMIT;
         */

        --TOTAL in EARNED COMMISSION   
        /* ORIGSQL: UPDATE RPT_STSRCSDC_INDIVIDUAL SET TOTAL = (NVL(BASICSAMT, 0) + NVL(MULTIPLIERAM(...) */
        UPDATE RPT_STSRCSDC_INDIVIDUAL
            SET
            /* ORIGSQL: TOTAL = */
            TOTAL = (IFNULL(BASICSAMT, 0) + IFNULL(MULTIPLIERAMT, 0)),  /* ORIGSQL: NVL(MULTIPLIERAMT, 0) */
                                                                        /* ORIGSQL: NVL(BASICSAMT, 0) */
            /* ORIGSQL: payable_flag = */
            payable_flag = :v_payableflag
        FROM
            RPT_STSRCSDC_INDIVIDUAL
        WHERE
            processingunitseq = :vprocessingunitseq
            AND periodseq = :vperiodseq
            AND allgroups = 'EARNED COMMISSION'
            AND sectionname = 'OVERALL COMMISSION' /* --need to change */
            AND subsectionname = 'EARNED COMMISSION';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --OVERALL SECTION EARNED COMISSION TOTAL
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin OVERALL SECTION EARNED COMISSIO(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL SECTION EARNED COMISSION TOTAL', NULL, 'Earned Commision');

        ---Nandini:  -- GB1 to be defaulted to TRUE--  

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                products,
                shopname,
                teamvisible,
                payable_flag,
                SECTION_COMMISSION,
                reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, :vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '63' AS reportcode,
                '03' AS sectionid,
                'OVERALL COMMISSION' AS sectionname,
                'EARNED COMMISSION' AS subsectionname,
                '99' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'EARNED COMMISSION' AS allgroups,
                'Earned Commission' AS products,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1, 1) AS teamvisible,  /* ORIGSQL: NVL(pad.TITLEGB1, 1) */
                :v_payableflag AS payable_flag,
                SECTION_COMMISSION,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (SELECT mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.TOTAL) AS SECTION_COMMISSION
                    FROM
                        RPT_STSRCSDC_INDIVIDUAL mes
                    WHERE
                        processingunitseq = :vprocessingunitseq
                        AND periodseq = :vperiodseq
                        AND allgroups = 'EARNED COMMISSION'
                        AND teamvisible = 1
                        ---Nandini:where GB1 TRUE--
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Earned Commission'
                AND titlemap.allgroups = 'EARNED COMMISSION'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
                AND pad.frequency = :vPeriodtype
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'OVERALL SECTION EARNED COMISSION TOTA(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL SECTION EARNED COMISSION TOTAL Completed', NULL, 'Earned Commision');

        FOR i AS dbmtk_cursor_8632
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin ADJUST COMMISSION PRODUCTS Depo(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION PRODUCTS Deposit', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                CECOMM,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   ''' 
            || IFNULL(:vTenantId,'')
            || ''',
            pad.positionseq,
            pad.payeeseq,
            '
            || IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')
            || ',
            '
            || IFNULL(:vperiodseq,'')
            || ',
            '''
            || IFNULL(:vPeriodRow.name,'')
            || ''',
            '''
            || IFNULL(:vProcessingUnitRow.name,'')
            || ''',
            '''
            || IFNULL(:vCalendarRow.name,'')
            || ''',
            ''63'' reportcode,
            ''04'' sectionid,
            ''ADJUST COMMISSION'' sectionname,
            ''ADJUST COMMISSION'' subsectionname,
            ''01'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''ADJUST COMMISSION'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            CECOMM,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            '
            || IFNULL(TO_VARCHAR(:v_payableflag),'')
            || ' payable_flag,
            '''
            || IFNULL(:v_reportgroup,'')
            || ''' reportgroup,
            '''
            || IFNULL(:vPeriodtype,'')
            || ''' frequency
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                titlemap.titlename,
                rmap.allgroups allgroups,
                '''
                || IFNULL(:i.product,'')
                || ''' product,
                MAX(case when rmap.rptcolumnname = ''CECOMM'' then '
                    || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CECOMM', :i.product, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'CECOMM', i.product, vPeriodtype) */
                || ' end) AS CECOMM
                FROM rpt_base_deposit CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.reportname=titlemap.reportname
                and rmap.report_frequency=titlemap.report_frequency
                
                AND rmap.reportname= '''
                || IFNULL(:vrptname,'')
                || '''
                and CM.periodseq = '
                || IFNULL(:vperiodseq,'')
                || '
                and CM.processingunitseq = '
                || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
                || '
                and rmap.product = '''
                || IFNULL(:i.product,'')
                || '''
                and rmap.allgroups = ''ADJUST COMMISSION''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.reportgroup = '''
            || IFNULL(:v_reportgroup,'')
            || '''
            and pad.FULLNAME='''
            || IFNULL(:v_user,'')
            || '''
            and pad.frequency='''
            || IFNULL(:vPeriodtype,'')
            || '''
            and pad.POSITIONGA1=''Channel individual Scheme''';
            --need to change

            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Completed ADJUST COMMISSION PRODUCTS (...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Completed ADJUST COMMISSION PRODUCTS Deposit', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: END LOOP; */

        --ADJUST commission BALANCE ..Prior Balance Adjustment
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin ADJUST COMMISSION Balance Prior(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Balance Prior Balance Adjustment', NULL, 'Balance Prior Balance Adjustment');

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCEPAYMENTTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYMENT' not found */
        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                CECOMM,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, :vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '63' AS reportcode,
                '04' AS sectionid,
                'ADJUST COMMISSION' AS sectionname,
                'ADJUST COMMISSION' AS subsectionname,
                '05' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'ADJUST COMMISSION' AS allgroups,
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'Prior Balance Adjustment' AS products,
                CECOMM,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1, 1) AS teamvisible,  /* ORIGSQL: NVL(pad.TITLEGB1, 1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (SELECT pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(...) */
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq,
                        SUM(bal.VALUE) AS CECOMM
                    FROM
                        cs_balance bal,
                        cs_balancepaymenttrace baltrace,
                        cs_payment pay
                    WHERE
                        bal.periodseq = baltrace.sourceperiodseq
                        AND baltrace.targetperiodseq = pay.periodseq
                        AND bal.balanceseq = baltrace.balanceseq
                        AND bal.payeeseq = pay.payeeseq
                        AND bal.processingunitseq = pay.processingunitseq
                        AND bal.processingunitseq = baltrace.processingunitseq
                        AND pay.periodseq = :vperiodseq
                        AND pay.processingunitseq = :vprocessingunitseq
                    GROUP BY
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Prior Balance Adjustment'
                AND titlemap.allgroups = 'ADJUST COMMISSION'
                AND pad.frequency = :vPeriodtype
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup
                AND pad.FULLNAME = :v_user
                AND pad.POSITIONGA1 = 'Channel individual Scheme';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'ADJUST COMMISSION Balance Prior Balan(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Balance Prior Balance Adjustment Completed', NULL, 'Balance Prior Balance Adjustment');

        /*
         -- Adjustment Commission : CE Adjustment
        prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION MEASUREMENT',NULL,'CE Adjustment');
         v_sql :=
          'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
                   (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                        processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                        sortorder,titlename,loaddttm,allgroups,geid,name,
                        products, CECOMM,shopname,teamvisible,payable_flag,reportgroup
                    )
             SELECT   '''||vTenantID||''', pad.positionseq, pad.payeeseq, '||:vProcessingUnitRow.processingunitseq||',
                         '||vperiodseq||','''||vPeriodRow.name||''',             '''||vProcessingUnitRow.name||''','''||vCalendarRow.name||''',''59'' reportcode,
                      04 sectionid,''ADJUST COMMISSION'' sectionname,''ADJUST COMMISSION'' subsectionname,
                      02 sortorder, pad.reporttitle titlename, SYSDATE,
                      ''ADJUST COMMISSION'' allgroups,pad.PARTICIPANTID,pad.FULLNAME,''CE Adjustment'' products, CECOMM,
                      pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,'||v_payableflag||' payable_flag,'''||v_reportgroup||''' reportgroup
             FROM   rpt_base_padimension pad,
               (   select mes.positionseq,
                                   mes.payeeseq,
                                   mes.processingunitseq,
                                   mes.periodseq,
                                    titlemap.titlename,
                                    MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||fun_sts_mapping(vrptname,'CECOMM','CE Adjustment')||' end) AS CECOMM
                                   from RPT_BASE_MEASUREMENT mes,rpt_sts_mapping rmap
                                where  mes.name in rmap.rulename
             and rmap.product=titlemap.product
             and rmap.allgroups=titlemap.allgroups
                              -- and rmap.report_frequency=titlemap.report_frequency
             and rmap.reportname=titlemap.reportname
             AND rmap.reportname= '''||vrptname||'''
             and mes.periodseq = '||vperiodseq||'
             and mes.processingunitseq = '||vprocessingunitseq||'
             and rmap.product = ''CE Adjustment''
             and rmap.allgroups = ''ADJUST COMMISSION''
                                group by mes.positionseq,
                                     mes.payeeseq,
                                     mes.processingunitseq,
                                     mes.periodseq ,titlemap.titlename
                )mes
             WHERE pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         and pad.periodseq = mes.periodseq
         and pad.POSITIONTITLE=mes.titlename
         and pad.reportgroup = '''||v_reportgroup||'''
         and pad.FULLNAME='''||v_user||'''
         and pad.POSITIONGA1=''Channel individual Scheme''';
        
          EXECUTE IMMEDIATE v_sql;
           prc_logevent (:vPeriodRow.name,vProcName,'END ADJUST COMMISSION MEASUREMENT',NULL,v_sql);
        
          COMMIT;
        
          --CE Adjustment merge
         prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Individual',NULL,'Individual Payout');
          MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
                        (select cre.positionseq,
                                   cre.payeeseq,
                                   cre.processingunitseq,
                                   cre.periodseq,
                                   max(cre.genericattribute4) REMARKS,
                                   sum(cre.genericnumber1) CEACTUAL,
                                   sum(cre.value) CEADJ
                            from rpt_base_credit cre
                            where cre.processingunitseq = vprocessingunitseq
             and cre.periodseq = vperiodseq
             and cre.credittypeid = pkg_reporting_extract_r2.vcredittypeid_CEAdj
                            group by cre.positionseq,
                                     cre.payeeseq,
                                     cre.processingunitseq,
                                 cre.periodseq)qtr
         on (rpt.processingunitseq = qtr.processingunitseq
             and rpt.periodseq = qtr.periodseq
             and rpt.positionseq = qtr.positionseq
             and rpt.payeeseq = qtr.payeeseq
             and rpt.allgroups='ADJUST COMMISSION'
             and rpt.products='CE Adjustment'
          )
         when matched then update set rpt.REMARKS =qtr.REMARKS,rpt.CEACTUAL=qtr.CEACTUAL,rpt.CEADJ=qtr.CEADJ;
        
        prc_logevent (:vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE Adjustment start',NULL,'CE Adjustment Start');
        COMMIT;
        prc_logevent (:vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE Adjustment complete',NULL,'CE Adjustment complete');
        
         */
        ---ADJUST commission-- Total Adjustment
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin ADJUST commission Total Adjustm(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST commission Total Adjustment', NULL, 'Total Adjustment');  

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                geid,
                name,
                allgroups,
                products,
                SECTION_COMMISSION,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, :vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '63' AS reportcode,
                '04' AS sectionid,
                'ADJUST COMMISSION' AS sectionname,
                'ADJUST COMMISSION' AS subsectionname,
                '08' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'ADJUST REMARKS' AS allgroups,
                'Total Adjustment' AS products,
                SECTION_COMMISSION,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1, 1) AS teamvisible,  /* ORIGSQL: NVL(pad.TITLEGB1, 1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (SELECT mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.CECOMM) AS SECTION_COMMISSION
                    FROM
                        RPT_STSRCSDC_INDIVIDUAL mes
                    WHERE
                        mes.allgroups = 'ADJUST COMMISSION'
                        AND mes.periodseq = :vperiodseq
                        AND mes.processingunitseq = :vprocessingunitseq
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Total Adjustment'
                AND titlemap.allgroups = 'ADJUST REMARKS'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup
                AND pad.FULLNAME = :v_user
                AND pad.POSITIONGA1 = 'Channel individual Scheme';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'ADJUST commission Total Adjustment Co(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST commission Total Adjustment Complete', NULL, 'Total Adjustment');

        --ADJUST COMMISSION CREDIT..Remarks
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin ADJUST COMMISSION Credit REMARK(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Credit REMARKS', NULL, 'REMARKS');

        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_CREDIT' not found */
        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                remarks,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, :vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '63' AS reportcode,
                '04' AS sectionid,
                'ADJUST COMMISSION' AS sectionname,
                'ADJUST COMMISSION' AS subsectionname,
                '09' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'ADJUST REMARKS' AS allgroups,
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'ADJUST REMARKS' AS products,
                REMARKS,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1, 1) AS teamvisible,  /* ORIGSQL: NVL(pad.TITLEGB1, 1) */
                :v_payableflag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (SELECT mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, MAX(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        MAX(mes.genericattribute3) AS Remarks
                    FROM
                        rpt_base_credit mes
                    WHERE
                        mes.processingunitseq = :vprocessingunitseq
                        AND mes.periodseq = :vperiodseq
                        AND mes.credittypeid =
                        :vcredittypeid_PayAdj  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_PayAdj */
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'ADJUST REMARKS'
                AND titlemap.allgroups = 'ADJUST REMARKS'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup
                AND pad.FULLNAME = :v_user
                AND pad.POSITIONGA1 = 'Channel individual Scheme';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'ADJUST COMMISSION Credit REMARKS Comp(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Credit REMARKS Completed', NULL, 'REMARKS');

        --Total Commission Payout
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin ADJUST COMMISSION Total Commiss(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Total Commission Payout', NULL, 'Total Commission Payout');  

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                subsectionname,
                sortorder,
                titlename,
                loaddttm,
                allgroups,
                geid,
                name,
                products,
                TOTALCOMMISSION,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, :vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '63' AS reportcode,
                '05' AS sectionid,
                'TOTAL COMMISSION' AS sectionname,
                'OVERALL COMMISSION' AS subsectionname,
                '99' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'ADJUST REMARKS' AS allgroups,
                pad.PARTICIPANTID,
                pad.FULLNAME,
                'Total Commission Payout' AS products,
                TOTALCOMMISSION,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1, 1) AS teamvisible,  /* ORIGSQL: NVL(pad.TITLEGB1, 1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (SELECT mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.SECTION_COMMISSION) AS TOTALCOMMISSION
                    FROM
                        RPT_STSRCSDC_INDIVIDUAL mes
                    WHERE
                        mes.allgroups IN ('ADJUST REMARKS',
                        'EARNED COMMISSION')
                        AND mes.periodseq = :vperiodseq
                        AND mes.processingunitseq = :vprocessingunitseq
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Total Commission Payout'
                AND titlemap.allgroups = 'ADJUST REMARKS'
                AND pad.frequency = :vPeriodtype
                AND titlemap.reportname = :vrptname
                AND pad.reportgroup = :v_reportgroup
                AND pad.FULLNAME = :v_user
                AND pad.POSITIONGA1 = 'Channel individual Scheme';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'OVERALL COMMISSION Total Commission P(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL COMMISSION Total Commission Payout Completed', NULL, 'Total Commission Payout');

        /*
        --INDIVIDUAL ACHIEVEMENT
        
         prc_logevent (:vPeriodRow.name,vProcName,'Begin INDIVIDUAL ACHIEVEMENT',NULL,NULL);
        
        v_sql :=
          'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
                      (select CM.positionseq,
                                 CM.payeeseq,
                                 CM.processingunitseq,
                                 CM.periodseq,
                                 MAX(case when rmap.rptcolumnname = ''ACHIEVEMENTPER'' then '||fun_sts_mapping(vrptname,'ACHIEVEMENTPER')||' end) AS ACHIEVEMENTPER
                          from RPT_BASE_MEASUREMENT CM, rpt_sts_mapping rmap
                          where CM.processingunitseq = '||vprocessingunitseq||'
             and CM.name in rmap.rulename
             and rmap.reportname = '''||vrptname||'''
             and CM.periodseq = '||vperiodseq||'
             and rmap.allgroups=''INDIVIDUAL PRODUCTS''
                          GROUP BY CM.positionseq,
                            CM.payeeseq,
                            CM.processingunitseq,
                            CM.periodseq,
                        rmap.allgroups)qtr
          on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''INDIVIDUAL PRODUCTS''
        )
         when matched then update set rpt.ACHIEVEMENTPER=(qtr.ACHIEVEMENTPER*100)';
        
         prc_logevent (:vPeriodRow.name,vProcName,'INDIVIDUAL ACHIEVEMENT completed',NULL,v_sql);
        
         EXECUTE IMMEDIATE v_sql;
         COMMIT;
         */

        --OTC
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Begin OTC INCENTIVE', NULL, NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OTC INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''OTC'' then ' 
                || IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OTC', NULL, :vPeriodtype),'')   /* ORIGSQL: fun_sts_mapping (vrptname, 'OTC', NULL, vPeriodtype) */
            || ' end) AS OTC
            from rpt_base_incentive inc, rpt_sts_mapping rmap
            where inc.processingunitseq = '
            || IFNULL(TO_VARCHAR(:vprocessingunitseq),'')
            || '
            and inc.name in rmap.rulename
            and rmap.reportname = '''
            || IFNULL(:vrptname,'')
            || '''
            and inc.periodseq = '
            || IFNULL(:vperiodseq,'')
            || '
            and rmap.product IS NULL
            and rmap.allgroups=''INDIVIDUAL PRODUCTS''
            GROUP BY inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
        rmap.allgroups)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.allgroups = ''INDIVIDUAL PRODUCTS''
        )
        when matched then update set rpt.OTC = qtr.OTC';

        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'OTC INCENTIVE completed', NULL, v_sql(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OTC INCENTIVE completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Update the null OTC,GEID,NAME 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt USING (SELECT DISTINCT mes.positionseq, m(...) */
        MERGE INTO RPT_STSRCSDC_INDIVIDUAL AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (SELECT DISTINCT mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.perio(...) */
                    DISTINCT
                    mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    mes.OTC,
                    mes.GEID,
                    mes.NAME,
                    mes.shopname
                FROM
                    RPT_STSRCSDC_INDIVIDUAL mes
                WHERE
                    mes.processingunitseq = :vprocessingunitseq
                    AND mes.periodseq = :vperiodseq
                    AND mes.sectionname = 'INDIVIDUAL ACHIEVEMENT'
                    AND mes.subsectionname = 'INDIVIDUAL ACHIEVEMENT'
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.frequency = :vPeriodtype)
        WHEN MATCHED THEN
            UPDATE SET rpt.OTC = qtr.OTC,
                rpt.GEID = qtr.GEID,
                rpt.NAME = qtr.NAME,
                rpt.shopname = qtr.shopname;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --BELOW UPDATE DONE BASED SITI REQUEST TO RENAME PRODUCT AS BELOW   
        /* ORIGSQL: Update EXT.RPT_STSRCSDC_INDIVIDUAL SET PRODUCTS='ENERGY' where PRODUCTS='DAS(...) */
        UPDATE EXT.RPT_STSRCSDC_INDIVIDUAL
            SET
            /* ORIGSQL: PRODUCTS = */
            PRODUCTS = 'ENERGY' 
        FROM
            EXT.RPT_STSRCSDC_INDIVIDUAL
        WHERE
            PRODUCTS = 'DASH';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */

        --------End Insert---------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Report table insert complete', NULL, (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Report table insert complete', NULL, :vSQLERRM);

        --------Gather stats-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'Start DBMS_STATS', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Start DBMS_STATS', NULL, :vSQLERRM);

        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTable (vrpttablename) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);--/*Deepan : Partition Not required*/

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END