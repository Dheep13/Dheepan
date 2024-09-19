CREATE PROCEDURE EXT.PRC_DIRECTSALES_INDIVIDUAL
(
    --IN vrptname rpt_directsales_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_directsales_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                                           /* RESOLVE: Datatype unresolved: Datatype (rpt_directsales_mapping.reportname%TYPE) not resolved for parameter 'PRC_DIRECTSALES_INDIVIDUAL.vrptname' */
                                                           /* ORIGSQL: vrptname IN rpt_directsales_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_DIRECTSALES_INDIVIDUAL.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_DIRECTSALES_INDIVIDUAL.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT,
                                                  /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_DIRECTSALES_INDIVIDUAL.vcalendarseq' */
                                                  /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
    --IN vPeriodtype rpt_directsales_mapping.REPORT_FREQUENCY%TYPE      /* RESOLVE: Identifier not found: Table/Column 'rpt_directsales_mapping.REPORT_FREQUENCY' not found (for %TYPE declaration) */
    IN vPeriodtype BIGINT
                                                                      /* RESOLVE: Datatype unresolved: Datatype (rpt_directsales_mapping.REPORT_FREQUENCY%TYPE) not resolved for parameter 'PRC_DIRECTSALES_INDIVIDUAL.vPeriodtype' */
                                                                      /* ORIGSQL: vPeriodtype IN rpt_directsales_mapping.REPORT_FREQUENCY%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();
    DECLARE vcredittypeid_CEAdj VARCHAR(255); /* package/session variable */
    DECLARE vcredittypeid_PayAdj VARCHAR(255); /* package/session variable */

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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_DIRECTSALES_INDIVIDUAL');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_DIRECTSALES_INDIVIDUAL') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'EXT.RPT_DIRECTSALES_INDIVIDUAL';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_DIRECTSALES_INDIVIDUAL'; */
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
    DECLARE cEndofTime date;
    
    -----------------FUNCTIONALITY BEGINS HERE-----------
    --------Begin Insert-------------------------------------------------------------------------------
    /* ORIGSQL: for i in (select distinct product from rpt_directsales_mapping where reportname (...) */
    DECLARE CURSOR dbmtk_cursor_6611
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_DIRECTSALES_MAPPING' not found */

        SELECT   /* ORIGSQL: select distinct product from rpt_directsales_mapping where reportname = vrptname(...) */
            DISTINCT
            product
        FROM
           ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'INDIVIDUAL PRODUCTS'
            AND PRODUCT IS NOT NULL;

    ----------INDIVIDUAL AVERAGE-MEASUREMENT ... INDIVIDUAL  POINTSPAYOUT
    /* ORIGSQL: for i in (select distinct product from rpt_directsales_mapping where reportname (...) */
    DECLARE CURSOR dbmtk_cursor_6614
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_directsales_mapping where reportname = vrptname(...) */
            DISTINCT
            product
        FROM
           ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'INDIVIDUAL AVERAGE';

    ----------TEAM PRODUCT-MEASUREMENT ... TEAM
    /* ORIGSQL: for i in (select distinct product from rpt_directsales_mapping where reportname (...) */
    DECLARE CURSOR dbmtk_cursor_6617
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_directsales_mapping where reportname = vrptname(...) */
            DISTINCT
            product
        FROM
           ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'TEAM PRODUCTS'
            AND PRODUCT IS NOT NULL;

    ----------TEAM AVERAGE-MEASUREMENT ... TEAM

    /* ORIGSQL: for i in (select distinct product from rpt_directsales_mapping where reportname (...) */
    DECLARE CURSOR dbmtk_cursor_6620
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_directsales_mapping where reportname = vrptname(...) */
            DISTINCT
            product
        FROM
           ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'TEAM AVERAGE';

    --OVERALL COMMISSION..Incentive..Overall % achieved,Basic, Multiplier.
    /* ORIGSQL: for i in (select distinct product from rpt_directsales_mapping where reportname (...) */
    DECLARE CURSOR dbmtk_cursor_6623
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_directsales_mapping where reportname = vrptname(...) */
            DISTINCT
            product
        FROM
           ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'EARNED COMMISSION'
            AND product NOT IN ('EARNED COMMISSION');

    /*
    --Adjustment section  First row
     prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION First Row',NULL,NULL);
           v_sql :=
          'INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL
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
                                                      CETARGET,
                                                      CEACTUAL,
                                                      CEADJ,
                                                      SECTION_COMMISSION,
                                                      shopname,
                                                      teamvisible,
                                                      payable_flag,
                                                      reportgroup
                                                  )
          SELECT  '''||vTenantID||''',
                   pad.positionseq,
                   pad.payeeseq,
                   '||vProcessingUnitRow.processingunitseq||',
                   '||vperiodseq||',
                   '''||vPeriodRow.name||''',
                   '''||vProcessingUnitRow.name||''',
                   '''||vCalendarRow.name||''',
                   ''85'' reportcode,
                   ''04'' sectionid,
                   ''ADJUST COMMISSION'' sectionname,
                   ''ADJUST COMMISSION'' subsectionname,
                   ''01'' sortorder,
                   pad.reporttitle titlename,
                   SYSDATE,
                   ''ADJUST COMMISSION'' allgroups,
                   pad.PARTICIPANTID, --geid
                   pad.FULLNAME, --name
                   ''First Row'' products,
                   CETARGET,
                   CEACTUAL,
                   CEADJ,
                   SECTION_COMMISSION,
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
                              MAX(case when rmap.rptcolumnname = ''CETARGET'' then '||fun_directsales_mapping(vrptname,'CETARGET','First Row')||' end) AS CETARGET,
                              MAX(case when rmap.rptcolumnname = ''CEACTUAL'' then '||fun_directsales_mapping(vrptname,'CEACTUAL','First Row')||' end) AS CEACTUAL,
                              MAX(case when rmap.rptcolumnname = ''CEADJ'' then '||fun_directsales_mapping(vrptname,'CEADJ','First Row')||' end) AS CEADJ,
                              MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||fun_directsales_mapping(vrptname,'CECOMM','First Row')||' end) AS SECTION_COMMISSION
                              FROM RPT_BASE_MEASUREMENT CM,rpt_directsales_mapping rmap
                              WHERE CM.name in rmap.rulename
         AND rmap.reportname= '''||vrptname||'''
         and CM.periodseq = '||vperiodseq||'
         and CM.processingunitseq = '||vprocessingunitseq||'
         and rmap.allgroups = ''ADJUST COMMISSION''
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
     and pad.reportgroup = '''||v_reportgroup||'''';
    
        prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust Commission First row completed START',NULL,v_sql);
       EXECUTE IMMEDIATE v_sql;
        prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust Commission First row completed FINISH',NULL,v_sql);
       COMMIT;
    
       --Adjustment section Commission Payout
    prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust Commission -Commission Payout',NULL,'Earned Commision');
    
      INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL
           (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                sortorder,titlename,loaddttm,
                allgroups,products,shopname,teamvisible,payable_flag,SECTION_COMMISSION,reportgroup
            )
      SELECT  vTenantID,pad.positionseq,pad.payeeseq, vProcessingUnitRow.processingunitseq,
              vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,vCalendarRow.name,
              '85' reportcode,'04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,'99' sortorder,
              pad.reporttitle titlename,SYSDATE,
              'ADJUST COMMISSION' allgroups, 'Commission Payout' products,pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,
              v_payableflag payable_flag,TOTAL,v_reportgroup reportgroup
      FROM   rpt_base_padimension pad,
            (  select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq,
                    SUM(mes.SECTION_COMMISSION) as TOTAL
                   FROM RPT_DIRECTSALES_INDIVIDUAL mes
                    WHERE processingunitseq = vprocessingunitseq
         AND periodseq           = vperiodseq
         AND allgroups in ('ADJUST COMMISSION','EARNED COMMISSION')
         and teamvisible = 1
                   GROUP BY mes.positionseq,
                      mes.payeeseq,
                      mes.processingunitseq,
                      mes.periodseq
            )mes
             WHERE   pad.payeeseq               = mes.payeeseq
     AND pad.positionseq        = mes.positionseq
     AND pad.processingunitseq  = mes.processingunitseq
     and pad.periodseq          = mes.periodseq
     and pad.reportgroup        = v_reportgroup;
      Commit;
    
      prc_logevent (vPeriodRow.name,vProcName,'Adjust Commission -Commission Payout Completed',NULL,'Commission Payout');
    
      --Adjustment Section Commission Final Payout
    
        INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL
           (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                sortorder,titlename,loaddttm,
                allgroups,products,shopname,teamvisible,payable_flag,SECTION_COMMISSION,reportgroup
            )
      SELECT  vTenantID,pad.positionseq,pad.payeeseq, vProcessingUnitRow.processingunitseq,
              vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,vCalendarRow.name,
              '85' reportcode,'04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,'99' sortorder,
              pad.reporttitle titlename,SYSDATE,
              'ADJUST COMMISSION' allgroups, 'Commission Final Payout' products,pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,
              v_payableflag payable_flag,TOTAL,v_reportgroup reportgroup
      FROM   rpt_base_padimension pad,
            (  select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq,
                    SUM(mes.SECTION_COMMISSION) as TOTAL
                   FROM RPT_DIRECTSALES_INDIVIDUAL mes
                    WHERE processingunitseq = vprocessingunitseq
         AND periodseq           = vperiodseq
         AND allgroups in ('ADJUST COMMISSION','EARNED COMMISSION')
         and teamvisible = 1
                   GROUP BY mes.positionseq,
                      mes.payeeseq,
                      mes.processingunitseq,
                      mes.periodseq
            )mes
             WHERE   pad.payeeseq               = mes.payeeseq
     AND pad.positionseq        = mes.positionseq
     AND pad.processingunitseq  = mes.processingunitseq
     and pad.periodseq          = mes.periodseq
     and pad.reportgroup        = v_reportgroup;
      Commit;
    
      prc_logevent (vPeriodRow.name,vProcName,'Adjust Commission -Commission Payout Completed',NULL,'Commission Payout');
    
      */

    ---ADJUST COMMISSION PRODUCTS- Payment Adjustment  -Deposit

    /* ORIGSQL: for i in (select distinct product from rpt_directsales_mapping where reportname (...) */
    DECLARE CURSOR dbmtk_cursor_6626
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_directsales_mapping where reportname = vrptname(...) */
            DISTINCT
            product
        FROM
           ext.rpt_directsales_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'ADJUST COMMISSION'
            AND product NOT IN ('CE Adjustment');

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
        CALL EXT.init_session_global();
        /* retrieve the package/session variables referenced in this procedure */
        SELECT SESSION_CONTEXT('VCREDITTYPEID_CEADJ') INTO vcredittypeid_CEAdj FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('VCREDITTYPEID_PAYADJ') INTO vcredittypeid_PayAdj FROM SYS.DUMMY ;
        /* end of package/session variables */

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
            );--Sanjay Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

        vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);-Sanjay: commenting as AnalyzeTableSubpartition are not require

        --------Truncate report table subpartition------------------------------------------------------------------------
        --pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName,
        --                                                   vSubpartitionName);
        --Since Deleting the records using reportgroup

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);-Sanjay: commenting as AnalyzeTableSubpartition are not require

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

        v_reportgroup = 'DirectSales';

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
            AND Classifierid = 'DIRECTSALESACHIVEMENT'
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
            ) AS dbmtk_corrname_6650
        WHERE
            Grouplist LIKE '%User Group%';

        IF :v_UserGroup = 'Y' 
        THEN
            v_payableflag = :v_payable;
        ELSE 
            v_payableflag = 1;
        END IF;

        -----DELETE EXISTING RECORDS BASED ON REPORT GROUP 

        /* ORIGSQL: DELETE FROM RPT_DIRECTSALES_INDIVIDUAL WHERE reportgroup='DirectSales' and perio(...) */
        DELETE
        FROM
            EXT.RPT_DIRECTSALES_INDIVIDUAL
        WHERE
            reportgroup = 'DirectSales'
            AND periodseq = :vperiodseq
            AND processingunitseq = :vprocessingunitseq
            AND FREQUENCY = :vPeriodtype;

        /* ORIGSQL: commit; */
        COMMIT;

        FOR i AS dbmtk_cursor_6611
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENT c(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENT completed', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
            (tenantid, positionseq,payeeseq,processingunitseq,periodseq,periodname,processingunitname,calendarname,
                reportcode,sectionid,sectionname,subsectionname,sortorder,titlename,loaddttm,allgroups,geid,name,products,
            CONNWT,CONNTARGET,CONNACTUALS,CONNACTUALTARGET,POINTSACTUALS,shopname,teamvisible,payable_flag,reportgroup,FREQUENCY)
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''85'' reportcode,
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
            (CONNWT) CONNWT,
            CONNTARGET,
            CONNACTUALS,
            CONNACTUALTARGET,
            POINTSACTUALS,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' FREQUENCY
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''CONNWT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNWT', :i.product, :vPeriodtype),'') ||' end) AS CONNWT,
                MAX(case when rmap.rptcolumnname = ''CONNTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNTARGET', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGET,
                MAX(case when rmap.rptcolumnname = ''CONNACTUALS'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNACTUALS', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALS,
                MAX(case when rmap.rptcolumnname = ''CONNACTUALTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNACTUALTARGET', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALTARGET,
                MAX(case when rmap.rptcolumnname = ''POINTSACTUALS'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'POINTSACTUALS', :i.product, :vPeriodtype),'') ||' end) AS POINTSACTUALS
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_directsales_mapping rmap,ext.RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.reportname=titlemap.reportname
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''INDIVIDUAL PRODUCTS''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'POINTSACTUALS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNWT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNACTUALTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNACTUALS',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENT c(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENT completed START', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENT c(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENT completed FINISH', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        FOR i AS dbmtk_cursor_6614
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT',N(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
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
                CONNTARGETCOUNT,
                CONNACTUALSCOUNT,
                GAPER,
                CVTARGETPPOINT,
                CVACTUALPOINT,
                CVACHIEVPER,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''85'' reportcode,
            ''01'' sectionid,
            ''INDIVIDUAL ACHIEVEMENT'' sectionname,
            ''AVERAGE ACHIEVEMENT'' subsectionname,
            ''99'' sortorder,
            pad.reporttitle titlename,
            SYSDATE,
            ''POINTS PAYOUT'' allgroups,
            pad.PARTICIPANTID, --geid
            pad.FULLNAME, --name
            product,
            CONNTARGETCOUNT,
            CONNACTUALSCOUNT,
            (GAPER*100) GAPER,
            CVTARGETPPOINT,
            CVACTUALPOINT,
            (CVACHIEVPER*100)CVACHIEVPER,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''CONNTARGETCOUNT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNTARGETCOUNT', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGETCOUNT,
                MAX(case when rmap.rptcolumnname = ''CONNACTUALSCOUNT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNACTUALSCOUNT', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALSCOUNT,
                MAX(case when rmap.rptcolumnname = ''GAPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'GAPER', :i.product, :vPeriodtype),'') ||' end) AS GAPER,
                MAX(case when rmap.rptcolumnname = ''CVTARGETPPOINT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CVTARGETPPOINT', :i.product, :vPeriodtype),'') ||' end) AS CVTARGETPPOINT,
                MAX(case when rmap.rptcolumnname = ''CVACTUALPOINT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CVACTUALPOINT', :i.product, :vPeriodtype),'') ||' end) AS CVACTUALPOINT,
                MAX(case when rmap.rptcolumnname = ''CVACHIEVPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CVACHIEVPER', :i.product, :vPeriodtype),'') ||' end) AS CVACHIEVPER
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.EXT.RPT_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.reportname=titlemap.reportname
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''INDIVIDUAL AVERAGE''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups  ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'GAPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CVTARGETPPOINT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CVACTUALPOINT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CVACHIEVPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNTARGETCOUNT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNACTUALSCOUNT',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT co(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT completed START', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT co(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT completed FINISH', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        FOR i AS dbmtk_cursor_6617
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS MEASUREMENT complet(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS MEASUREMENT completed', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
            (tenantid, positionseq,payeeseq,processingunitseq,periodseq,periodname,processingunitname,calendarname,
                reportcode,sectionid,sectionname,subsectionname,sortorder,titlename,loaddttm,allgroups,geid,name,products,
            CONNWT,CONNTARGET,CONNACTUALS,CONNACTUALTARGET,POINTSACTUALS,shopname,teamvisible,payable_flag,reportgroup,frequency )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''85'' reportcode,
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
            (CONNWT) CONNWT,
            CONNTARGET,
            CONNACTUALS,
            CONNACTUALTARGET,
            POINTSACTUALS,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNWT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNWT', :i.product, :vPeriodtype),'') ||' end) AS CONNWT,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNTARGET', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNACTUALS'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNACTUALS', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALS,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNACTUALTARGET'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNACTUALTARGET', :i.product, :vPeriodtype),'') ||' end ) AS CONNACTUALTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSACTUALS'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMPOINTSACTUALS', :i.product, :vPeriodtype),'') ||' end) AS POINTSACTUALS
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.EXT.RPT_directsales_mapping rmap,EXT.RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                and rmap.product=titlemap.product
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.allgroups=titlemap.allgroups
                and rmap.reportname=titlemap.reportname
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''TEAM PRODUCTS''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMPOINTSACTUALS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNWT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNACTUALTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNACTUALS',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS MEASUREMENT complet(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS MEASUREMENT completed START', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS MEASUREMENT complet(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS MEASUREMENT completed FINISH', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        FOR i AS dbmtk_cursor_6620
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM AVERAGE MEASUREMENT',NULL,i.(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM AVERAGE MEASUREMENT', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
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
                CONNTARGETCOUNT,
                CONNACTUALSCOUNT,
                CVTARGETPPOINT,
                CVACTUALPOINT,
                CVACHIEVPER,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
            )
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''85'' reportcode,
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
            CONNTARGETCOUNT,
            CONNACTUALSCOUNT,
            CVTARGETPPOINT,
            CVACTUALPOINT,
            (CVACHIEVPER*100)CVACHIEVPER,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''TEAMGAPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMGAPER', :i.product, :vPeriodtype),'') ||' end) AS GAPER,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNTARGETCOUNT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNTARGETCOUNT', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGETCOUNT,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNACTUALSCOUNT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNACTUALSCOUNT', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALSCOUNT,
                MAX(case when rmap.rptcolumnname = ''TEAMCVTARGETPPOINT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCVTARGETPPOINT', :i.product, :vPeriodtype),'') ||' end) AS CVTARGETPPOINT,
                MAX(case when rmap.rptcolumnname = ''TEAMCVACTUALPOINT'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCVACTUALPOINT', :i.product, :vPeriodtype),'') ||' end) AS CVACTUALPOINT,
                MAX(case when rmap.rptcolumnname = ''TEAMCVACHIEVPER'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCVACHIEVPER', :i.product, :vPeriodtype),'') ||' end) AS CVACHIEVPER
                FROM EXT.RPT_BASE_MEASUREMENT CM,ext.rpt_directsales_mapping rmap,EXT.RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.reportname=titlemap.reportname
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''TEAM AVERAGE''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups  ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMGAPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCVTARGETPPOINT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCVACTUALPOINT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCVACHIEVPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNTARGETCOUNT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNACTUALSCOUNT',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM AVERAGE MEASUREMENT complete(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM AVERAGE MEASUREMENT completed START', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM AVERAGE MEASUREMENT complete(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM AVERAGE MEASUREMENT completed FINISH', NULL, :v_sql);

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        /*removed based on ramya comments
        ----TEAM AVERAGE INCENTIVE
        prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM AVERAGE INCENTIVE',NULL,'POINTSPAYOUT');
         v_sql :=
        'MERGE INTO RPT_DIRECTSALES_INDIVIDUAL rpt using
                          (select inc.positionseq,
                                     inc.payeeseq,
                                     inc.processingunitseq,
                                     inc.periodseq,
                                     MAX(case when rmap.rptcolumnname = ''GAPER'' then '||fun_directsales_mapping(vrptname,'TEAMGAPER','CONNECTIONS')||' end) AS GAPER
                              from rpt_base_incentive inc, rpt_directsales_mapping rmap
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
        
          prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS INCENTIVE completed START',NULL,v_sql);
        
            EXECUTE IMMEDIATE v_sql;
             prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS INCENTIVE completed FINISH',NULL,v_sql);
            COMMIT;	*/

        --Incentive individual   ...Individual

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS INCENTIVE',NU(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO EXT.RPT_DIRECTSALES_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''OTC'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'OTC', NULL, :vPeriodtype),'') ||' end) AS OTC,
            MAX(case when rmap.rptcolumnname = ''CONNPOINTSPER1'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNPOINTSPER1', NULL, :vPeriodtype),'') ||' end) AS CONNPOINTSPER1,
            MAX(case when rmap.rptcolumnname = ''CONNPOINTSPER2'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CONNPOINTSPER2', NULL, :vPeriodtype),'') ||' end) AS CONNPOINTSPER2
            from ext.rpt_base_incentive inc, ext.rpt_directsales_mapping rmap
            where inc.processingunitseq = ' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and inc.name in rmap.rulename
            and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and rmap.product IS NULL
            and rmap.allgroups=''INDIVIDUAL PRODUCTS''
            and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
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
            and rpt.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
        )
        when matched then update set rpt.OTC = qtr.OTC, rpt.CONNPER=(nvl(qtr.CONNPOINTSPER1,0)/NULLIF((nvl(qtr.CONNPOINTSPER1,0)+nvl(qtr.CONNPOINTSPER2,0)),0)*100),
        rpt.POINTSPER=(nvl(qtr.CONNPOINTSPER2,0)/NULLIF((nvl(qtr.CONNPOINTSPER1,0)+nvl(qtr.CONNPOINTSPER2,0)),0)*100),rpt.ACHIEVEMENTPER= ((nvl(qtr.CONNPOINTSPER1,0) + nvl(qtr.CONNPOINTSPER2,0))*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'OTC',NULL,vPeriodtype) */
                                                                                                                                                                                                           /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNPOINTSPER2',NULL,vPeriodtype) */
                                                                                                                                                                                                           /* ORIGSQL: fun_directsales_mapping(vrptname,'CONNPOINTSPER1',NULL,vPeriodtype) */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS INCENTIVE com(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE completed START', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Individual PRODUCTS INCENTIVE com(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE completed FINISH', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ----TEAM PRODUCT INCENTIVE
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCT INCENTIVE',NULL,NULL(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCT INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO EXT.RPT_DIRECTSALES_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''TEAMOTC'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMOTC', NULL, :vPeriodtype),'') ||' end) AS TEAMOTC,
            MAX(case when rmap.rptcolumnname = ''TEAMCONNPOINTSPER1'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNPOINTSPER1', NULL, :vPeriodtype),'') ||' end) AS TEAMCONNPOINTSPER1,
            MAX(case when rmap.rptcolumnname = ''TEAMCONNPOINTSPER2'' then ' ||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'TEAMCONNPOINTSPER2', NULL, :vPeriodtype),'') ||' end) AS TEAMCONNPOINTSPER2
            from ext.rpt_base_incentive inc, ext.rpt_directsales_mapping rmap
            where inc.processingunitseq = ' ||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and  inc.name in rmap.rulename
            and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
            and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
            and rmap.product IS NULL
            and rmap.allgroups=''TEAM PRODUCTS''
            and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
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
            and rpt.frequency=''' ||IFNULL(:vPeriodtype,'')||''')
        when matched then update set rpt.OTC = qtr.TEAMOTC,
        rpt.CONNPER=(nvl(qtr.TEAMCONNPOINTSPER1,0)/NULLIF((nvl(qtr.TEAMCONNPOINTSPER1,0)+nvl(qtr.TEAMCONNPOINTSPER2,0)),0)*100),
        rpt.POINTSPER=(nvl(qtr.TEAMCONNPOINTSPER2,0)/NULLIF((nvl(qtr.TEAMCONNPOINTSPER1,0)+nvl(qtr.TEAMCONNPOINTSPER2,0)),0)*100),
        rpt.ACHIEVEMENTPER= ((nvl(qtr.TEAMCONNPOINTSPER1,0) + nvl(qtr.TEAMCONNPOINTSPER2,0))*100)';  /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMOTC',NULL,vPeriodtype) */
                                                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNPOINTSPER2',NULL,vPeriodtype) */
                                                                                                     /* ORIGSQL: fun_directsales_mapping(vrptname,'TEAMCONNPOINTSPER1',NULL,vPeriodtype) */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS INCENTIVE completed(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS INCENTIVE completed START', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS INCENTIVE completed(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS INCENTIVE completed FINISH', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /*changing as per STS overall commission comments provided by Sriramya
          --OVERALL COMMISSION individual
           prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL',NULL,NULL);
           v_sql :=
          'INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL
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
                                                      CVACHIEVPER,
                                                      shopname,
                                                      teamvisible,
                                                      payable_flag,
                                                      reportgroup
                                                  )
          SELECT  '''||vTenantID||''',
                   pad.positionseq,
                   pad.payeeseq,
                   '||vProcessingUnitRow.processingunitseq||',
                   '||vperiodseq||',
                   '''||vPeriodRow.name||''',
                   '''||vProcessingUnitRow.name||''',
                   '''||vCalendarRow.name||''',
                   ''85'' reportcode,
                   ''03'' sectionid,
                   ''OVERALL COMMISSION'' sectionname,
                   ''EARNED COMMISSION'' subsectionname,
                   ''01'' sortorder,
                   pad.reporttitle titlename,
                   SYSDATE,
                   ''EARNED COMMISSION'' allgroups,
                   pad.PARTICIPANTID, --geid
                   pad.FULLNAME, --name
                   ''Individual'' products,
                   (GAPER *100) GAPER,
                   (CVACHIEVPER*100) CVACHIEVPER,
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
                              CM.periodseq, titlemap.titlename,
                              MAX(case when rmap.rptcolumnname = ''GAPER'' then '||fun_directsales_mapping(vrptname,'GAPER','Individual Payout')||' end) AS GAPER,
                              MAX(case when rmap.rptcolumnname = ''CVACHIEVPER'' then '||fun_directsales_mapping(vrptname,'CVACHIEVPER','Individual Payout')||' end) AS CVACHIEVPER
                              FROM RPT_BASE_MEASUREMENT CM,rpt_directsales_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                              WHERE CM.name in rmap.rulename
             and rmap.product=titlemap.product
             and rmap.allgroups=titlemap.allgroups
             and rmap.report_frequency=titlemap.report_frequency
             and rmap.reportname=titlemap.reportname
             AND rmap.reportname= '''||vrptname||'''
             and CM.periodseq = '||vperiodseq||'
             and CM.processingunitseq = '||vprocessingunitseq||'
             and rmap.allgroups = ''EARNED COMMISSION''
                            GROUP BY CM.positionseq,
                              CM.payeeseq,
                              CM.processingunitseq,
                              CM.periodseq,
                              rmap.allgroups ,titlemap.titlename
                    )mes
        
           WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         and pad.periodseq = mes.periodseq
         and pad.POSITIONTITLE=mes.titlename
         and pad.frequency='''||vPeriodtype||'''
         and pad.reportgroup = '''||v_reportgroup||'''';
        
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed START',NULL,NULL);
         EXECUTE IMMEDIATE v_sql;
         prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed FINISH',NULL,NULL);
         COMMIT;
        
        --OVERALL COMMISSION team
           prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL',NULL,NULL);
           v_sql :=
          'INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL
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
                                                      CVACHIEVPER,
                                                      shopname,
                                                      teamvisible,
                                                      payable_flag,
                                                      reportgroup
                                                  )
          SELECT  '''||vTenantID||''',
                   pad.positionseq,
                   pad.payeeseq,
                   '||vProcessingUnitRow.processingunitseq||',
                   '||vperiodseq||',
                   '''||vPeriodRow.name||''',
                   '''||vProcessingUnitRow.name||''',
                   '''||vCalendarRow.name||''',
                   ''85'' reportcode,
                   ''03'' sectionid,
                   ''OVERALL COMMISSION'' sectionname,
                   ''EARNED COMMISSION'' subsectionname,
                   ''01'' sortorder,
                   pad.reporttitle titlename,
                   SYSDATE,
                   ''EARNED COMMISSION'' allgroups,
                   pad.PARTICIPANTID, --geid
                   pad.FULLNAME, --name
                   ''Team'' products,
                    (GAPER*100)GAPER,
                   (CVACHIEVPER) CVACHIEVPER,
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
                              CM.periodseq,titlemap.titlename ,
                              MAX(case when rmap.rptcolumnname = ''TEAMGAPER'' then '||fun_directsales_mapping(vrptname,'TEAMGAPER','Team Payout')||' end) AS GAPER,
                              MAX(case when rmap.rptcolumnname = ''TEAMCVACHIEVPER'' then '||fun_directsales_mapping(vrptname,'TEAMCVACHIEVPER','Team Payout')||' end) AS CVACHIEVPER
                              FROM RPT_BASE_MEASUREMENT CM,rpt_directsales_mapping rmap ,RPT_TITLE_PRODUCT_MAPPING titlemap
                              WHERE CM.name in rmap.rulename
             and rmap.product=titlemap.product
             and rmap.allgroups=titlemap.allgroups
             and rmap.report_frequency=titlemap.report_frequency
             and rmap.reportname=titlemap.reportname
             AND rmap.reportname= '''||vrptname||'''
             and CM.periodseq = '||vperiodseq||'
             and CM.processingunitseq = '||vprocessingunitseq||'
             and rmap.allgroups = ''EARNED COMMISSION''
                            GROUP BY CM.positionseq,
                              CM.payeeseq,
                              CM.processingunitseq,
                              CM.periodseq,
                              rmap.allgroups   ,titlemap.titlename
                    )mes
        
           WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         and pad.periodseq = mes.periodseq
         and pad.POSITIONTITLE=mes.titlename
         and pad.frequency='''||vPeriodtype||'''
         and pad.reportgroup = '''||v_reportgroup||'''';
        
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed START',NULL,NULL);
        EXECUTE IMMEDIATE v_sql;
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed FINISH',NULL,NULL);
         COMMIT;
        
        
        --OVERALL COMMISSION..Incentive..ACHIEVEPER%, Basic($), Multiplier($) Total($)  for Individual
         prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Individual',NULL,'Individual Payout');
         v_sql :=
        'MERGE INTO RPT_DIRECTSALES_INDIVIDUAL rpt using
                        (select inc.positionseq,
                                   inc.payeeseq,
                                   inc.processingunitseq,
                                   inc.periodseq,
                                   MAX(case when rmap.rptcolumnname = ''ACHIEVEMENTPER1'' then '||fun_directsales_mapping(vrptname,'ACHIEVEMENTPER1','Individual Payout')||' end) AS ACHIEVEMENTPER1,
                                   MAX(case when rmap.rptcolumnname = ''ACHIEVEMENTPER2'' then '||fun_directsales_mapping(vrptname,'ACHIEVEMENTPER2','Individual Payout')||' end) AS ACHIEVEMENTPER2,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER1'' then '||fun_directsales_mapping(vrptname,'OVERALLPER1','Individual Payout')||' end) AS OVERALLPER1,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER2'' then '||fun_directsales_mapping(vrptname,'OVERALLPER2','Individual Payout')||' end) AS OVERALLPER2,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER3'' then '||fun_directsales_mapping(vrptname,'OVERALLPER3','Individual Payout')||' end) AS OVERALLPER3,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER4'' then '||fun_directsales_mapping(vrptname,'OVERALLPER4','Individual Payout')||' end) AS OVERALLPER4,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER5'' then '||fun_directsales_mapping(vrptname,'OVERALLPER5','Individual Payout')||' end) AS OVERALLPER5,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER6'' then '||fun_directsales_mapping(vrptname,'OVERALLPER6','Individual Payout')||' end) AS OVERALLPER6,
                                   MAX(case when rmap.rptcolumnname = ''MULTIPLIERAMT'' then '||fun_directsales_mapping(vrptname,'MULTIPLIERAMT','Individual Payout')||' end) AS MULTIPLIERAMT,
                                   MAX(case when rmap.rptcolumnname = ''TOTAL'' then '||fun_directsales_mapping(vrptname,'TOTAL','Individual Payout')||' end) AS TOTAL
                            from rpt_base_incentive inc, rpt_directsales_mapping rmap
                            where inc.processingunitseq = '||vprocessingunitseq||'
             and inc.name in rmap.rulename
             and rmap.reportname = '''||vrptname||'''
             and inc.periodseq = '||vperiodseq||'
             and rmap.product=''Individual Payout''
             and rmap.allgroups=''EARNED COMMISSION''
                            GROUP BY inc.positionseq,
                              inc.payeeseq,
                              inc.processingunitseq,
                              inc.periodseq,
                          rmap.allgroups)qtr
         on (rpt.processingunitseq = qtr.processingunitseq
             and rpt.periodseq = qtr.periodseq
             and rpt.positionseq = qtr.positionseq
             and rpt.payeeseq = qtr.payeeseq
             and rpt.allgroups=''EARNED COMMISSION''
             and rpt.products=''Individual''
          )
        when matched then update set rpt.ACHIEVEMENTPER =((nvl(qtr.ACHIEVEMENTPER1,0) + nvl(qtr.ACHIEVEMENTPER2,0))*100),rpt.OVERALLPER=(nvl(qtr.OVERALLPER1,0)*nvl(qtr.OVERALLPER2,0))+(nvl(qtr.OVERALLPER3,0)*nvl(qtr.OVERALLPER2,0)),
        rpt.BASICSAMT=(nvl(qtr.OVERALLPER1,0)*nvl(qtr.OVERALLPER2,0)*nvl(qtr.OVERALLPER5,0))+(nvl(qtr.OVERALLPER3,0)*nvl(qtr.OVERALLPER2,0)*nvl(qtr.OVERALLPER6,0)),rpt.MULTIPLIERAMT=qtr.MULTIPLIERAMT,rpt.TOTAL=qtr.TOTAL';
        
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Individual START',NULL,v_sql);
        
        EXECUTE IMMEDIATE v_sql;
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Individual Completed FINISH',NULL,v_sql);
        COMMIT;
        
        
        --OVERALL COMMISSION..Incentive..ACHIEVEPER%, Basic($), Multiplier($) Total($) for Team Payout
         prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Individual',NULL,'Individual Payout');
         v_sql :=
        'MERGE INTO RPT_DIRECTSALES_INDIVIDUAL rpt using
                        (select inc.positionseq,
                                    inc.payeeseq,
                                    inc.processingunitseq,
                                    inc.periodseq,
                                   MAX(case when rmap.rptcolumnname = ''ACHIEVEMENTPER'' then '||fun_directsales_mapping(vrptname,'ACHIEVEMENTPER','Team Payout')||' end) AS ACHIEVEMENTPER,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER1'' then '||fun_directsales_mapping(vrptname,'OVERALLPER1','Team Payout')||' end) AS OVERALLPER1,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER2'' then '||fun_directsales_mapping(vrptname,'OVERALLPER2','Team Payout')||' end) AS OVERALLPER2,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER3'' then '||fun_directsales_mapping(vrptname,'OVERALLPER3','Team Payout')||' end) AS OVERALLPER3,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER4'' then '||fun_directsales_mapping(vrptname,'OVERALLPER4','Team Payout')||' end) AS OVERALLPER4,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER5'' then '||fun_directsales_mapping(vrptname,'OVERALLPER5','Team Payout')||' end) AS OVERALLPER5,
                                   MAX(case when rmap.rptcolumnname = ''OVERALLPER6'' then '||fun_directsales_mapping(vrptname,'OVERALLPER6','Team Payout')||' end) AS OVERALLPER6,
                                   MAX(case when rmap.rptcolumnname = ''MULTIPLIERAMT'' then '||fun_directsales_mapping(vrptname,'MULTIPLIERAMT','Team Payout')||' end) AS MULTIPLIERAMT,
                                   MAX(case when rmap.rptcolumnname = ''TOTAL'' then '||fun_directsales_mapping(vrptname,'TOTAL','Team Payout')||' end) AS TOTAL
                            from rpt_base_incentive inc, rpt_directsales_mapping rmap
                            where inc.processingunitseq = '||vprocessingunitseq||'
             and inc.name in rmap.rulename
             and rmap.reportname = '''||vrptname||'''
             and inc.periodseq = '||vperiodseq||'
             and rmap.product=''Team Payout''
             and rmap.allgroups=''EARNED COMMISSION''
                            GROUP BY inc.positionseq,
                              inc.payeeseq,
                              inc.processingunitseq,
                              inc.periodseq,
                          rmap.allgroups)qtr
         on (rpt.processingunitseq = qtr.processingunitseq
             and rpt.periodseq = qtr.periodseq
             and rpt.positionseq = qtr.positionseq
             and rpt.payeeseq = qtr.payeeseq
             and rpt.allgroups=''EARNED COMMISSION''
             and rpt.products=''Team''
          )
         when matched then update set rpt.ACHIEVEMENTPER =qtr.ACHIEVEMENTPER,rpt.OVERALLPER=(nvl(qtr.OVERALLPER1,0)*nvl(qtr.OVERALLPER2,0))+(nvl(qtr.OVERALLPER3,0)*nvl(qtr.OVERALLPER2,0)),
        rpt.BASICSAMT=(nvl(qtr.OVERALLPER1,0)*nvl(qtr.OVERALLPER2,0)*nvl(qtr.OVERALLPER5,0))+(nvl(qtr.OVERALLPER3,0)*nvl(qtr.OVERALLPER2,0)*nvl(qtr.OVERALLPER6,0)),rpt.MULTIPLIERAMT=qtr.MULTIPLIERAMT,rpt.TOTAL=qtr.TOTAL';
        
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Team Payout START',NULL,v_sql);
        
        EXECUTE IMMEDIATE v_sql;
        prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for Team Payout  Completed FINISH',NULL,v_sql);
        COMMIT;
        
        
        */

        --added on 05-04-2019 Common overall Commission

        ---GA % Achieved ,Points % achieved ..OVERALL COMMISSION individual
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAP(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER', NULL, NULL);

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_DIRECTSALES_INDIVIDUAL' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_DIRECTSALES_INDIVIDUAL' not found */

        /* ORIGSQL: INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL (tenantid, positionseq, payeeseq,(...) */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_TITLE_PRODUCT_MAPPING' not found */
        INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
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
                CVACHIEVPER,
                ACHIEVEMENTPER,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
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
                '59' AS reportcode,
                '03' AS sectionid,
                'OVERALL COMMISSION' AS sectionname,
                'EARNED COMMISSION' AS subsectionname,
                '01' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'EARNED COMMISSION' AS allgroups,
                pad.PARTICIPANTID,/* --geid */  pad.FULLNAME,/* --name */   'Individual' AS products,
                (mes.GAPER) AS GAPER,
                (mes.CVACHIEVPER) AS CVACHIEVPER,
                (mes_ach.ACHIEVEMENTPER) AS ACHIEVEMENTPER,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, ind(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        ind.ACHIEVEMENTPER,
                        ind.GAPER,
                        ind.CVACHIEVPER
                    FROM
                        RPT_DIRECTSALES_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'INDIVIDUAL ACHIEVEMENT'
                        AND ind.subsectionname = 'AVERAGE ACHIEVEMENT'
                        AND ind.allgroups = 'POINTS PAYOUT'
                ) AS mes,
                (
                    SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, MAX(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        MAX(ind.ACHIEVEMENTPER) AS ACHIEVEMENTPER
                    FROM
                        EXT.RPT_DIRECTSALES_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'INDIVIDUAL ACHIEVEMENT'
                        AND ind.allgroups = 'INDIVIDUAL PRODUCTS'
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
                AND pad.processingunitseq = mes_ach.processingunitseq
                AND pad.periodseq = mes_ach.periodseq

                AND pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Individual'
                AND titlemap.allgroups = 'EARNED COMMISSION'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        --need to change
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAP(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --ACHIEVEMENTPER,GAPER,POINTSACHIEVEDPER for OVERALL TEAM
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION TEAM GAPER,POI(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION TEAM GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER', NULL, NULL);   

        /* ORIGSQL: INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL (tenantid, positionseq, payeeseq,(...) */
        INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
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
                CVACHIEVPER,
                ACHIEVEMENTPER,
                shopname,
                teamvisible,
                payable_flag,
                reportgroup,
                frequency
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
                (mes.CVACHIEVPER) AS CVACHIEVPER,
                (mes_ach.ACHIEVEMENTPER) AS ACHIEVEMENTPER,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, ind(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        ind.ACHIEVEMENTPER,
                        ind.GAPER,
                        ind.CVACHIEVPER
                    FROM
                        RPT_DIRECTSALES_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'TEAM ACHIEVEMENT'
                        AND ind.subsectionname = 'AVERAGE ACHIEVEMENT'
                        AND ind.allgroups = 'TEAM PAYOUT'
                ) AS mes,
                (
                    SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, MAX(...) */
                        ind.positionseq,
                        ind.payeeseq,
                        ind.processingunitseq,
                        ind.periodseq,
                        MAX(ind.ACHIEVEMENTPER) AS ACHIEVEMENTPER
                    FROM
                        EXT.RPT_DIRECTSALES_INDIVIDUAL ind
                    WHERE
                        ind.processingunitseq = :vprocessingunitseq
                        AND ind.periodseq = :vperiodseq
                        AND ind.sectionname = 'TEAM ACHIEVEMENT'
                        AND ind.allgroups = 'TEAM PRODUCTS'
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
                AND pad.processingunitseq = mes_ach.processingunitseq
                AND pad.periodseq = mes_ach.periodseq
                AND pad.POSITIONTITLE = titlemap.titlename

                AND titlemap.product = 'Team'
                AND titlemap.allgroups = 'EARNED COMMISSION'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        --need to change
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION TEAM GAPER,POI(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION TEAM GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        FOR i AS dbmtk_cursor_6623
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for (...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION Incentive for Overall, Basic and Multiplier amount completed', NULL, :i.product);
            v_sql = 'MERGE INTO EXT.RPT_DIRECTSALES_INDIVIDUAL rpt using
            (select inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq,
                MAX(case when rmap.rptcolumnname = ''OVERALLPER'' then '||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'OVERALLPER', :i.product, :vPeriodtype),'') ||' end) AS OVERALLPER,
                MAX(case when rmap.rptcolumnname = ''BASICSAMT1'' then '||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'BASICSAMT1', :i.product, :vPeriodtype),'') ||' end) AS BASICSAMT,
                MAX(case when rmap.rptcolumnname = ''MULTIPLIERAMT'' then '||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'MULTIPLIERAMT', :i.product, :vPeriodtype),'') ||' end) AS MULTIPLIERAMT
                from ext.rpt_base_incentive inc, ext.rpt_directsales_mapping rmap
                where inc.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and inc.name in rmap.rulename
                and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
                and inc.periodseq = '||IFNULL(:vperiodseq,'')||'
                and rmap.product='''||IFNULL(:i.product,'')||'''
                and rmap.allgroups=''EARNED COMMISSION''
                and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
                GROUP BY inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq,
            rmap.allgroups)qtr
            on (rpt.processingunitseq = qtr.processingunitseq
                and rpt.periodseq = qtr.periodseq
                and rpt.positionseq = qtr.positionseq
                and rpt.payeeseq = qtr.payeeseq
                and rpt.PRODUCTS='''||IFNULL(:i.product,'')||'''
                and rpt.allgroups=''EARNED COMMISSION''
                and rpt.frequency='''||IFNULL(:vPeriodtype,'')||'''
            )
            when matched then update set rpt.OVERALLPER = (qtr.OVERALLPER*100), rpt.BASICSAMT=qtr.BASICSAMT,
            rpt.MULTIPLIERAMT=qtr.MULTIPLIERAMT';  /* ORIGSQL: fun_directsales_mapping(vrptname,'OVERALLPER',i.product,vPeriodtype) */
                                                   /* ORIGSQL: fun_directsales_mapping(vrptname,'MULTIPLIERAMT',i.product,vPeriodtype) */
                                                   /* ORIGSQL: fun_directsales_mapping(vrptname,'BASICSAMT1',i.product,vPeriodtype) */

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for (...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION Incentive for Overall, Basic and Multiplier amount completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        --TOTAL in EARNED COMMISSION   
        /* ORIGSQL: UPDATE RPT_DIRECTSALES_INDIVIDUAL SET TOTAL= (nvl(BASICSAMT,0) +nvl(MULTIPLIERAM(...) */
        UPDATE EXT.RPT_DIRECTSALES_INDIVIDUAL
            SET
            /* ORIGSQL: TOTAL = */
            TOTAL = (IFNULL(BASICSAMT,0) +IFNULL(MULTIPLIERAMT,0)),  /* ORIGSQL: nvl(MULTIPLIERAMT,0) */
                                                                     /* ORIGSQL: nvl(BASICSAMT,0) */
            /* ORIGSQL: payable_flag = */
            payable_flag = :v_payableflag
        FROM
            EXT.RPT_DIRECTSALES_INDIVIDUAL
        WHERE
            processingunitseq = :vprocessingunitseq
            AND periodseq = :vperiodseq
            AND allgroups = 'EARNED COMMISSION'
            AND sectionname = 'OVERALL COMMISSION' /* --need to change  */
            AND subsectionname = 'EARNED COMMISSION';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --end the common overall section

        ---ADJUST COMMISSION PRODUCTS-Advance Protected Commission, Payment Adjustment  -Deposit
        ---Nandini:  -- GB1 to be defaulted to TRUE--

        --OVERALL COMMISSION - EARNED COMMISSION
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION EARNED COMMISS(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION EARNED COMMISSION', NULL, NULL);  

        /* ORIGSQL: INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL (tenantid, positionseq, payeeseq,(...) */
        INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, products, shopname,
                teamvisible, payable_flag, SECTION_COMMISSION, reportgroup, frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID,pad.positionseq,pad.payeeseq, vProcessingUnitRow.processingunit(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
                '03' AS sectionid,
                'OVERALL COMMISSION' AS sectionname,
                'EARNED COMMISSION' AS subsectionname,
                '99' AS sortorder,
                pad.reporttitle AS titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                'EARNED COMMISSION' AS allgroups,
                'Earned Commission' AS products,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                SECTION_COMMISSION,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.TOTAL) AS SECTION_COMMISSION
                    FROM
                        RPT_DIRECTSALES_INDIVIDUAL mes
                    WHERE
                        processingunitseq = :vprocessingunitseq
                        AND periodseq = :vperiodseq
                        AND allgroups = 'EARNED COMMISSION'
                        AND products IN ('Individual','Team')
                        AND teamvisible = 1
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
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        FOR i AS dbmtk_cursor_6626
        DO
            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION PRODUCTS Deposi(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION PRODUCTS Deposit', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
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
            SELECT   '''||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            '||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            '||IFNULL(:vperiodseq,'')||',
            '''||IFNULL(:vPeriodRow.name,'')||''',
            '''||IFNULL(:vProcessingUnitRow.name,'')||''',
            '''||IFNULL(:vCalendarRow.name,'')||''',
            ''85'' reportcode,
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
            '||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            '''||IFNULL(:v_reportgroup,'')||''' reportgroup,
            '''||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   ext.rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                titlemap.titlename,
                rmap.allgroups allgroups,
                '''||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CECOMM', :i.product, :vPeriodtype),'') ||' end) AS CECOMM
                FROM ext.rpt_base_deposit CM,ext.rpt_directsales_mapping rmap,EXT.RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                
                and rmap.product=titlemap.product
                and rmap.allgroups=titlemap.allgroups
                and rmap.reportname=titlemap.reportname
                and rmap.report_frequency=titlemap.report_frequency
                
                AND rmap.reportname= '''||IFNULL(:vrptname,'')||'''
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
                and rmap.allgroups = ''ADJUST COMMISSION''
                GROUP BY CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency='''||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'CECOMM',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Completed ADJUST COMMISSION PRODUCTS De(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Completed ADJUST COMMISSION PRODUCTS Deposit', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        --ADJUST commission BALANCE ..Prior Balance Adjustment
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Balance Prior B(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Balance Prior Balance Adjustment', NULL, 'Balance Prior Balance Adjustment');

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCEPAYMENTTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYMENT' not found */
        /* ORIGSQL: INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL (tenantid, positionseq, payeeseq,(...) */
        INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, geid, name,
                products, CECOMM, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processinguni(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
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
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(...) */
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq,
                        SUM(bal.value) AS CECOMM
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
                AND titlemap.reportname = :vrptname
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'ADJUST COMMISSION Balance Prior Balance(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Balance Prior Balance Adjustment Completed', NULL, 'Balance Prior Balance Adjustment');

        -- Adjustment Commission : CE Adjustment
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION MEASUREMENT',NU(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION MEASUREMENT', NULL, 'CE Adjustment');
        v_sql = 'INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
        (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
            processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
            sortorder,titlename,loaddttm,allgroups,geid,name,
            products,CECOMM,shopname,teamvisible,payable_flag,reportgroup,frequency
        )
        SELECT   '''||IFNULL(:vTenantId,'')||''', pad.positionseq, pad.payeeseq, '||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        '||IFNULL(:vperiodseq,'')||','''||IFNULL(:vPeriodRow.name,'')||''',             '''||IFNULL(:vProcessingUnitRow.name,'')||''','''||IFNULL(:vCalendarRow.name,'')||''',''59'' reportcode,
        04 sectionid,''ADJUST COMMISSION'' sectionname,''ADJUST COMMISSION'' subsectionname,
        02 sortorder, pad.reporttitle titlename, SYSDATE,
        ''ADJUST COMMISSION'' allgroups,pad.PARTICIPANTID,pad.FULLNAME,''CE Adjustment'' products, CECOMM,
        pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,'||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,'''||IFNULL(:v_reportgroup,'')||''' reportgroup,'''||IFNULL(:vPeriodtype,'')||''' frequency
        FROM   rpt_base_padimension pad,
        (   select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            titlemap.titlename,
            MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||IFNULL(EXT.FUN_DIRECTSALES_MAPPING(:vrptname, 'CECOMM', 'CE Adjustment', :vPeriodtype),'') ||' end) AS CECOMM
            from EXT.RPT_BASE_MEASUREMENT mes,ext.rpt_directsales_mapping rmap,EXT.RPT_TITLE_PRODUCT_MAPPING titlemap
            WHERE mes.name in rmap.rulename
            and rmap.product=titlemap.product
            and rmap.allgroups=titlemap.allgroups
            and rmap.report_frequency=titlemap.report_frequency
            and rmap.reportname=titlemap.reportname
            AND rmap.reportname= '''||IFNULL(:vrptname,'')||'''
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and rmap.product = ''CE Adjustment''
            and rmap.allgroups = ''ADJUST COMMISSION''
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq  ,titlemap.titlename
        )mes
        WHERE pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.POSITIONTITLE=mes.titlename
        and pad.frequency='''||IFNULL(:vPeriodtype,'')||'''
        and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_directsales_mapping(vrptname,'CECOMM','CE Adjustment',vPeriodtype) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'END ADJUST COMMISSION MEASUREMENT',NULL(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'END ADJUST COMMISSION MEASUREMENT', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --CE Adjustment merge
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION Incentive for Individual', NULL, 'Individual Payout'); 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_DIRECTSALES_INDIVIDUAL rpt using (SELECT cre.positionseq, cre.pay(...) */
        MERGE INTO EXT.RPT_DIRECTSALES_INDIVIDUAL AS rpt
            /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_CREDIT' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select cre.positionseq, cre.payeeseq, cre.processingunitseq, cre.periodseq, MAX(...) */
                    cre.positionseq,
                    cre.payeeseq,
                    cre.processingunitseq,
                    cre.periodseq,
                    MAX(cre.genericattribute4) AS REMARKS,
                    SUM(cre.genericnumber1) AS CEACTUAL,
                    SUM(cre.value) *100 AS CEADJ
                FROM
                    ext.rpt_base_credit cre
                WHERE
                    cre.processingunitseq = :vprocessingunitseq
                    AND cre.periodseq = :vperiodseq
                    AND cre.credittypeid = :vcredittypeid_CEAdj  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_CEAdj */
                GROUP BY
                    cre.positionseq,
                    cre.payeeseq,
                    cre.processingunitseq,
                    cre.periodseq
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
                AND rpt.allgroups = 'ADJUST COMMISSION'
                AND rpt.products = 'CE Adjustment'
                AND rpt.frequency = :vPeriodtype
            )
        WHEN MATCHED THEN
            UPDATE SET rpt.REMARKS = qtr.REMARKS,rpt.CEACTUAL = qtr.CEACTUAL,rpt.CEADJ = qtr.CEADJ;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE A(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Adjust commssion  credit for CE Adjustment start', NULL, 'CE Adjustment Start');

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE A(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Adjust commssion  credit for CE Adjustment complete', NULL, 'CE Adjustment complete');
        ---ADJUST commission-- Total Adjustment
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST commission Total Adjustmen(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST commission Total Adjustment', NULL, 'Total Adjustment');  

        /* ORIGSQL: INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL (tenantid, positionseq, payeeseq,(...) */
        INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, geid, name, allgroups,
                products, SECTION_COMMISSION, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processinguni(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
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
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.CECOMM) AS SECTION_COMMISSION
                    FROM
                        EXT.RPT_DIRECTSALES_INDIVIDUAL mes
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
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'ADJUST commission Total Adjustment Comp(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST commission Total Adjustment Complete', NULL, 'Total Adjustment');

        --ADJUST COMMISSION CREDIT..Remarks
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Credit REMARKS'(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Credit REMARKS', NULL, 'REMARKS');  

        /* ORIGSQL: INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL (tenantid, positionseq, payeeseq,(...) */
        INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, geid, name,
                products, remarks, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processinguni(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
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
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, MAX(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        MAX(mes.genericattribute3) AS Remarks
                    FROM
                        ext.rpt_base_credit mes
                    WHERE
                        mes.processingunitseq = :vprocessingunitseq
                        AND mes.periodseq = :vperiodseq
                        AND mes.credittypeid = :vcredittypeid_PayAdj  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_PayAdj */
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
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'ADJUST COMMISSION Credit REMARKS Comple(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Credit REMARKS Completed', NULL, 'REMARKS');

        --Total Commission Payout
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Total Commissio(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Total Commission Payout', NULL, 'Total Commission Payout');  

        /* ORIGSQL: INSERT INTO STELEXT.RPT_DIRECTSALES_INDIVIDUAL (tenantid, positionseq, payeeseq,(...) */
        INSERT INTO EXT.RPT_DIRECTSALES_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, geid, name,
                products, TOTALCOMMISSION, shopname, teamvisible, payable_flag, reportgroup,
                frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processinguni(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '85' AS reportcode,
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
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.SECTION_COMMISSION) AS TOTALCOMMISSION
                    FROM
                        EXT.RPT_DIRECTSALES_INDIVIDUAL mes
                    WHERE
                        mes.allgroups IN ('ADJUST REMARKS','EARNED COMMISSION')
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
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'OVERALL COMMISSION Total Commission Pay(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL COMMISSION Total Commission Payout Completed', NULL, 'Total Commission Payout');

        --Update the null OTC,GEID,NAME 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_DIRECTSALES_INDIVIDUAL rpt using (SELECT distinct mes.positionseq(...) */
        MERGE INTO EXT.RPT_DIRECTSALES_INDIVIDUAL AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select distinct mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.perio(...) */
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
                    EXT.RPT_DIRECTSALES_INDIVIDUAL mes
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
            UPDATE SET rpt.OTC = qtr.OTC,rpt.GEID = qtr.GEID,rpt.NAME = qtr.NAME,rpt.shopname = qtr.shopname;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --BELOW UPDATE DONE BASED SITI REQUEST TO RENAME PRODUCT AS BELOW   
        /* ORIGSQL: Update RPT_DIRECTSALES_INDIVIDUAL SET PRODUCTS='ENERGY' where PRODUCTS='DASH'; */
        UPDATE EXT.RPT_DIRECTSALES_INDIVIDUAL
            SET
            /* ORIGSQL: PRODUCTS = */
            PRODUCTS = 'ENERGY' 
        FROM
            EXT.RPT_DIRECTSALES_INDIVIDUAL
        WHERE
            PRODUCTS = 'DASH';

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
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);-Sanjay: commenting as AnalyzeTable is not required

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END