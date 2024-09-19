CREATE PROCEDURE EXT.PRC_STS_RC_DS_PAYEE_INDIVIDUAL
(
    IN vrptname VARCHAR(255),   /* RESOLVE: Identifier not found: Table/Column 'rpt_sts_mapping.reportname' not found (for %TYPE declaration) */
                                                   /* RESOLVE: Datatype unresolved: Datatype (rpt_sts_mapping.reportname%TYPE) not resolved for parameter 'PRC_STS_RC_DS_PAYEE_INDIVIDUAL.vrptname' */
                                                   /* ORIGSQL: vrptname IN rpt_sts_mapping.reportname%TYPE */
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_STS_RC_DS_PAYEE_INDIVIDUAL.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_STS_RC_DS_PAYEE_INDIVIDUAL.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                  /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_STS_RC_DS_PAYEE_INDIVIDUAL.vcalendarseq' */
                                                  /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
    IN vPeriodtype NVARCHAR(255)      /* RESOLVE: Identifier not found: Table/Column 'rpt_sts_mapping.REPORT_FREQUENCY' not found (for %TYPE declaration) */
                                                              /* RESOLVE: Datatype unresolved: Datatype (rpt_sts_mapping.REPORT_FREQUENCY%TYPE) not resolved for parameter 'PRC_STS_RC_DS_PAYEE_INDIVIDUAL.vPeriodtype' */
                                                              /* ORIGSQL: vPeriodtype IN rpt_sts_mapping.REPORT_FREQUENCY%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN

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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_STS_RC_DS_PAYEE_INDIVIDUAL');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_STS_RC_DS_PAYEE_INDIVIDUAL') ; */
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
    DECLARE v_sql NCLOB;  /* ORIGSQL: v_sql long; */
    DECLARE v_payable SMALLINT;  /* ORIGSQL: v_payable NUMBER(1); */
    DECLARE v_UserGroup VARCHAR(1) = 'N';  /* ORIGSQL: v_UserGroup VARCHAR2(1) := 'N'; */
    DECLARE v_payableflag SMALLINT;  /* ORIGSQL: v_payableflag NUMBER(1); */
    DECLARE v_reportgroup VARCHAR(127);  /* ORIGSQL: v_reportgroup VARCHAR2(127); */
    DECLARE v_classifierid NVARCHAR(127);  /* ORIGSQL: v_classifierid NVARCHAR2(127); */
    DECLARE cEndofTime CONSTANT TIMESTAMP ;
    DECLARE vcredittypeid_CEAdj VARCHAR(255); /* package/session variable */
    DECLARE vcredittypeid_PayAdj VARCHAR(255); /* package/session variable */


    ----------INDIVIDUAL PRODUCT-MEASUREMENT ... INDIVIDUAL
    ---Nandini: GB1 to be defaulted to TRUE--

    /* ORIGSQL: for i in (select distinct product from rpt_sts_mapping where reportname = vrptna(...) */
    DECLARE CURSOR cusrsor_indv_products
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_STS_MAPPING' not found */

        SELECT   /* ORIGSQL: select distinct product from rpt_sts_mapping where reportname = vrptname and all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'INDIVIDUAL PRODUCTS'
            AND PRODUCT IS NOT NULL;

    ----------INDIVIDUAL AVERAGE-MEASUREMENT ... INDIVIDUAL
    ---Nandini: GB1 to be defaulted to TRUE--
    /* ORIGSQL: for i in (select distinct product from rpt_sts_mapping where reportname = vrptna(...) */
    DECLARE CURSOR cursor_indv_avg
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_sts_mapping where reportname = vrptname and all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'INDIVIDUAL AVERAGE';

    ----------TEAM PRODUCT-MEASUREMENT ... TEAM

    ---Nandini: GB1 to be taken from title GB1--
    /* ORIGSQL: for i in (select distinct product from rpt_sts_mapping where reportname = vrptna(...) */
    DECLARE CURSOR cursor_team_prds
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_sts_mapping where reportname = vrptname and all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'TEAM PRODUCTS'
            AND PRODUCT IS NOT NULL;

    ----------TEAM AVERAGE-MEASUREMENT ... TEAM

    ---Nandini: GB1 to be taken from title GB1--
    /* ORIGSQL: for i in (select distinct product from rpt_sts_mapping where reportname = vrptna(...) */
    DECLARE CURSOR cursor_team_avg
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_sts_mapping where reportname = vrptname and all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'TEAM AVERAGE';

    --OVERALL COMMISSION..Incentive..Overall % achieved,Basic, Multiplier.
    /* ORIGSQL: for i in (select distinct product from rpt_sts_mapping where reportname = vrptna(...) */
    DECLARE CURSOR cursor_earned_comm
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_sts_mapping where reportname = vrptname and all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'EARNED COMMISSION'
            AND product NOT IN ('EARNED COMMISSION');

    /*
     ---ADJUST commission-- Total Adjustment
    prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST commission Total Adjustment',NULL,'Total Adjustment');
    ---Nandini:  -- GB1 to be defaulted to TRUE--
    INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
           (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                sortorder,titlename,loaddttm,
                allgroups,products, SECTION_COMMISSION,shopname,teamvisible,payable_flag,reportgroup
            )
         SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                  vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,vCalendarRow.name,'00' reportcode,
                  '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                  '06' sortorder, pad.reporttitle titlename, SYSDATE,
                  'ADJUST REMARKS' allgroups,'Total Adjustment' products, SECTION_COMMISSION,
                  pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
         FROM   rpt_base_padimension pad,
           (    select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq,
                      sum(mes.basicsamt) SECTION_COMMISSION
                    from RPT_STSRCSDC_INDIVIDUAL mes
                      where mes.allgroups='ADJUST COMMISSION'
         and mes.periodseq = vperiodseq
         and mes.processingunitseq = vprocessingunitseq
                    group by mes.positionseq,
                      mes.payeeseq,
                      mes.processingunitseq,
                      mes.periodseq
            )mes
         WHERE pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = v_reportgroup;
    Commit;
    prc_logevent (:vPeriodRow.name,vProcName,'ADJUST commission Total Adjustment Complete',NULL,'Total Adjustment');
    
    --Total Commission Payout
    ---Nandini:  -- GB1 to be defaulted to TRUE--
    prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Total Commission Payout',NULL,'Total Commission Payout');
    INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
           (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                sortorder,titlename,loaddttm,
                allgroups,products, TOTALCOMMISSION,shopname,teamvisible,payable_flag,reportgroup
            )
         SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                  vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,vCalendarRow.name,'59' reportcode,
                  '04' sectionid,'ADJUST COMMISSION' sectionname,'OVERALL COMMISSION' subsectionname,
                  '99' sortorder, pad.reporttitle titlename, SYSDATE,
                  'ADJUST REMARKS' allgroups,'Total Commission Payout' products, TOTALCOMMISSION,
                  pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
         FROM   rpt_base_padimension pad,
           (    select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq,
                      sum(mes.SECTION_COMMISSION) TOTALCOMMISSION
                    from RPT_STSRCSDC_INDIVIDUAL mes
                      where mes.allgroups in ('ADJUST REMARKS','EARNED COMMISSION')
         and mes.periodseq = vperiodseq
         and mes.processingunitseq = vprocessingunitseq
                    group by mes.positionseq,
                      mes.payeeseq,
                      mes.processingunitseq,
                      mes.periodseq
            )mes
         WHERE pad.payeeseq = mes.payeeseq
     AND pad.positionseq = mes.positionseq
     AND pad.processingunitseq = mes.processingunitseq
     and pad.periodseq = mes.periodseq
     and pad.reportgroup = v_reportgroup;
    
    Commit;
    prc_logevent (:vPeriodRow.name,vProcName,'OVERALL COMMISSION Total Commission Payout Completed',NULL,'Total Commission Payout');
    */

    /*   Commented to implement mapping
    ---SUM CVACTUAL INDIVIDUAL --->POINTSACTUALS
    
     MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
                  (select mes.positionseq,
                             mes.payeeseq,
                             mes.processingunitseq,
                             mes.periodseq,
                             sum(nvl(POINTSACTUALS,0)) AS POINTSACTUALS
                      from RPT_STSRCSDC_INDIVIDUAL mes
                      where
                      mes.processingunitseq = vprocessingunitseq
         and mes.periodseq = vperiodseq
         and mes.sectionname='INDIVIDUAL ACHIEVEMENT'
         and mes. allgroups='INDIVIDUAL PRODUCTS'
                       GROUP BY mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                    )qtr
      on (rpt.processingunitseq = qtr.processingunitseq
        and rpt.periodseq = qtr.periodseq
        and rpt.positionseq = qtr.positionseq
    and rpt.payeeseq = qtr.payeeseq)
     when matched then update set rpt.POINTSACTUALS = qtr.POINTSACTUALS
     where rpt.sectionname ='INDIVIDUAL ACHIEVEMENT'
    and rpt.allgroups='POINTS PAYOUT'
    and  rpt.subsectionname= 'AVERAGE ACHIEVEMENT';
     COMMIT;
    
    
     ---SUM CVACTUAL TEAM --->POINTSACTUALS
    
    MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
                  (select mes.positionseq,
                             mes.payeeseq,
                             mes.processingunitseq,
                             mes.periodseq,
                             sum(nvl(POINTSACTUALS,0)) AS POINTSACTUALS
                      from RPT_STSRCSDC_INDIVIDUAL mes
                      where
                      mes.processingunitseq = vprocessingunitseq
         and mes.periodseq = vperiodseq
         and mes.sectionname='TEAM ACHIEVEMENT'
         and mes. allgroups='TEAM PRODUCTS'
                       GROUP BY mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                    )qtr
      on (rpt.processingunitseq = qtr.processingunitseq
        and rpt.periodseq = qtr.periodseq
        and rpt.positionseq = qtr.positionseq
    and rpt.payeeseq = qtr.payeeseq)
     when matched then update set rpt.POINTSACTUALS = qtr.POINTSACTUALS
     where rpt.sectionname ='TEAM ACHIEVEMENT'
    and rpt.allgroups='TEAM PAYOUT'
    and  rpt.subsectionname= 'AVERAGE ACHIEVEMENT';
    
      COMMIT;
    
     */

    --Commission Adjustment
    ---ADJUST COMMISSION PRODUCTS- Payment Adjustment  -Deposit   -- sudhir start

    /* ORIGSQL: for i in (select distinct product from rpt_sts_mapping where reportname = vrptna(...) */
    DECLARE CURSOR cursor_indv_products
    FOR 
        SELECT   /* ORIGSQL: select distinct product from rpt_sts_mapping where reportname = vrptname and all(...) */
            DISTINCT
            product
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname
            AND allgroups = 'ADJUST COMMISSION'
            AND product NOT IN ('CE Adjustment');

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            vSQLERRM = SUBSTRING(::SQL_ERROR_MESSAGE,1,3900);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 3900) */

            /* ORIGSQL: prc_logevent (:vPeriodRow.name, vProcName, 'ERROR', NULL, vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ERROR', NULL, :vSQLERRM);

            /* ORIGSQL: raise_application_error(-20911,'Error raised: '||vprocname||' Failed: '|| DBMS_U(...) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT = 'Error raised: '||IFNULL(:vProcName,'')||' Failed: '
            -- ||

            -- DBMS_UTILITY.FORMAT_ERROR_BACKTRACE

            || ' - '||IFNULL(:vSQLERRM,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;

        /* initialize session variables, if not yet done */
        CALL EXT.init_session_global();
        /* retrieve the package/session variables referenced in this procedure */
        SELECT SESSION_CONTEXT('vcredittypeid_CEAdj') INTO vcredittypeid_CEAdj FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('vcredittypeid_PayAdj') INTO vcredittypeid_PayAdj FROM SYS.DUMMY ;
        -- SELECT SESSION_CONTEXT('cEndofTime') INTO cEndofTime FROM SYS.DUMMY ;


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
        --     ); /*Deepan : Partition Not required*/

        --------Find subpartition name------------------------------------------------------------------------------------

        -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) *//*Deepan : Partition Not required*/

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

        IF :vrptname = 'STSPAYEEACHIVEMENT' 
        THEN
            v_reportgroup = 'STS';

            v_classifierid = 'STS Report Payable';
        ELSEIF :vrptname = 'DSPAYEEACHIVEMENT'   /* ORIGSQL: elsif vrptname = 'DSPAYEEACHIVEMENT' Then */
        THEN
            v_reportgroup = 'Digital Telesales';

            v_classifierid = 'DS Report Payable';
        ELSEIF :vrptname = 'RCSPAYEEACHIVEMENT'   /* ORIGSQL: elsif vrptname = 'RCSPAYEEACHIVEMENT' Then */
        THEN
            v_reportgroup = 'RCS';

            v_classifierid = 'RCS Report Payable';
        END IF;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
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
            ) AS dbmtk_corrname_9762
        WHERE
            Grouplist LIKE '%User Group%';

        IF :v_UserGroup = 'Y' 
        THEN
            v_payableflag = :v_payable;
        ELSE 
            v_payableflag = 1;
        END IF;

        -----DELETE EXISTING RECORDS BASED ON REPORT GROUP 

        /* ORIGSQL: DELETE FROM RPT_STSRCSDC_INDIVIDUAL WHERE reportgroup=v_reportgroup and periodse(...) */
        DELETE
        FROM
            RPT_STSRCSDC_INDIVIDUAL
        WHERE
            reportgroup = :v_reportgroup
            AND periodseq = :vperiodseq
            AND processingunitseq = :vprocessingunitseq
            AND FREQUENCY = :vPeriodtype;

        /* ORIGSQL: commit; */
        COMMIT;

        FOR i AS cusrsor_indv_products
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENTco(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENTcompleted', NULL, :i.product);
            v_sql = 'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
            ''59'' reportcode,
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
            CONNWT,
            CONNTARGET,
            CONNACTUALS,
            CONNACTUALTARGET,
            POINTSACTUALS,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' FREQUENCY
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''CONNWT'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNWT', :i.product, :vPeriodtype),'') ||' end) AS CONNWT,
                MAX(case when rmap.rptcolumnname = ''CONNTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNTARGET', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGET,
                MAX(case when rmap.rptcolumnname = ''CONNACTUALS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNACTUALS', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALS,
                MAX(case when rmap.rptcolumnname = ''CONNACTUALTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNACTUALTARGET', :i.product, :vPeriodtype),'') ||' end ) AS CONNACTUALTARGET,
                MAX(case when rmap.rptcolumnname = ''POINTSACTUALS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'POINTSACTUALS', :i.product, :vPeriodtype),'') ||' end) AS POINTSACTUALS
                FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
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
                rmap.allgroups,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_sts_mapping(vrptname,'POINTSACTUALS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'CONNWT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'CONNTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'CONNACTUALTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'CONNACTUALS',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Individual PRODUCTS MEASUREMENTco(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS MEASUREMENTcompleted', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

    	   FOR i AS cursor_indv_avg
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT',N(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT', NULL, :i.product);
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
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''59'' reportcode,
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
            (GAPER*100) GAPER,
            TOTALPRODUCTS,
            PRDTARGET,
            (MULTIPLIERPER*100) MULTIPLIERPER,
            POINTSTARGET,
            POINTSACTUALS,
            (POINTSACHIEVEDPER*100) POINTSACHIEVEDPER,
            pad.POSITIONGA1 shopname,
            nvl(pad.TITLEGB1,1) teamvisible,
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''GAPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'GAPER', :i.product, :vPeriodtype),'') ||' end) AS GAPER,
                MAX(case when rmap.rptcolumnname = ''TOTALPRODUCTS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TOTALPRODUCTS', :i.product, :vPeriodtype),'') ||' end) AS TOTALPRODUCTS,
                MAX(case when rmap.rptcolumnname = ''PRDTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'PRDTARGET', :i.product, :vPeriodtype),'') ||' end) AS PRDTARGET,
                MAX(case when rmap.rptcolumnname = ''MULTIPLIERPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'MULTIPLIERPER', :i.product, :vPeriodtype),'') ||' end) AS MULTIPLIERPER,
                MAX(case when rmap.rptcolumnname = ''POINTSTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'POINTSTARGET', :i.product, :vPeriodtype),'') ||' end) AS POINTSTARGET,
                MAX(case when rmap.rptcolumnname = ''POINTSACTUALS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'POINTSACTUALS', :i.product, :vPeriodtype),'') ||' end) AS POINTSACTUALS,
                MAX(case when rmap.rptcolumnname = ''POINTSACHIEVEDPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'POINTSACHIEVEDPER', :i.product, :vPeriodtype),'') ||' end) AS POINTSACHIEVEDPER
                FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
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
                rmap.allgroups ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_sts_mapping(vrptname,'TOTALPRODUCTS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'PRDTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'POINTSTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'POINTSACTUALS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'POINTSACHIEVEDPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'MULTIPLIERPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'GAPER',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Individual AVERAGE MEASUREMENT co(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual AVERAGE MEASUREMENT completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        FOR i AS cursor_team_prds
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS MEASUREMENT complet(...) */
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
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''59'' reportcode,
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
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNWT'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNWT', :i.product, :vPeriodtype),'') ||' end) AS CONNWT,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNTARGET', :i.product, :vPeriodtype),'') ||' end) AS CONNTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNACTUALS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNACTUALS', :i.product, :vPeriodtype),'') ||' end) AS CONNACTUALS,
                MAX(case when rmap.rptcolumnname = ''TEAMCONNACTUALTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNACTUALTARGET', :i.product, :vPeriodtype),'') ||' end ) AS CONNACTUALTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSACTUALS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSACTUALS', :i.product, :vPeriodtype),'') ||' end) AS POINTSACTUALS
                FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                WHERE CM.name in rmap.rulename
                and rmap.product=titlemap.product
                and rmap.report_frequency=titlemap.report_frequency
                and rmap.allgroups=titlemap.allgroups
                AND rmap.reportname= ''' ||IFNULL(:vrptname,'')||'''
                and rmap.reportname=titlemap.reportname
                and CM.periodseq = '||IFNULL(:vperiodseq,'')||'
                and CM.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
                and rmap.product = '''||IFNULL(:i.product,'')||'''
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
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMPOINTSACTUALS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMCONNWT',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMCONNTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMCONNACTUALTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMCONNACTUALS',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS MEASUREMENTcomplete(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS MEASUREMENTcompleted', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        FOR i AS cursor_team_avg
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM AVERAGE MEASUREMENT',NULL,i.(...) */
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
            SELECT   ''' ||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            ' ||IFNULL(:vperiodseq,'')||',
            ''' ||IFNULL(:vPeriodRow.name,'')||''',
            ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
            ''' ||IFNULL(:vCalendarRow.name,'')||''',
            ''59'' reportcode,
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
            ' ||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,
            ''' ||IFNULL(:v_reportgroup,'')||''' reportgroup,
            ''' ||IFNULL(:vPeriodtype,'')||''' frequency
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                rmap.allgroups allgroups,
                titlemap.titlename,
                ''' ||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''TEAMGAPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMGAPER', :i.product, :vPeriodtype),'') ||' end) AS GAPER,
                MAX(case when rmap.rptcolumnname = ''TEAMTOTALPRODUCTS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMTOTALPRODUCTS', :i.product, :vPeriodtype),'') ||' end) AS TOTALPRODUCTS,
                MAX(case when rmap.rptcolumnname = ''TEAMPRDTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPRDTARGET', :i.product, :vPeriodtype),'') ||' end) AS PRDTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMMULTIPLIERPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMMULTIPLIERPER', :i.product, :vPeriodtype),'') ||' end) AS MULTIPLIERPER,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSTARGET'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSTARGET', :i.product, :vPeriodtype),'') ||' end) AS POINTSTARGET,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSACTUALS'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSACTUALS', :i.product, :vPeriodtype),'') ||' end) AS POINTSACTUALS,
                MAX(case when rmap.rptcolumnname = ''TEAMPOINTSACHIEVEDPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSACHIEVEDPER', :i.product, :vPeriodtype),'') ||' end) AS POINTSACHIEVEDPER
                FROM RPT_BASE_MEASUREMENT CM,rpt_sts_mapping rmap ,RPT_TITLE_PRODUCT_MAPPING titlemap
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
                rmap.allgroups   ,titlemap.titlename
            )mes
            WHERE       pad.payeeseq = mes.payeeseq
            AND pad.positionseq = mes.positionseq
            AND pad.processingunitseq = mes.processingunitseq
            and pad.periodseq = mes.periodseq
            and pad.POSITIONTITLE=mes.titlename
            and pad.frequency=''' ||IFNULL(:vPeriodtype,'')||'''
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMTOTALPRODUCTS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMPRDTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMPOINTSTARGET',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMPOINTSACTUALS',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMPOINTSACHIEVEDPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMMULTIPLIERPER',i.product,vPeriodtype) */
                                                                         /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMGAPER',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM AVERAGE MEASUREMENT complete(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM AVERAGE MEASUREMENT completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

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

        --Incentive individual   ...Individual

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Individual PRODUCTS INCENTIVE',NU(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual PRODUCTS INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''OTC'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OTC', NULL, :vPeriodtype),'') ||' end) AS OTC,
            MAX(case when rmap.rptcolumnname = ''CONNPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CONNPER', NULL, :vPeriodtype),'') ||' end) AS CONNPER,
            MAX(case when rmap.rptcolumnname = ''POINTSPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'POINTSPER', NULL, :vPeriodtype),'') ||' end) AS POINTSPER
            from rpt_base_incentive inc, rpt_sts_mapping rmap
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
        when matched then update set  rpt.OTC = qtr.OTC,
        rpt.CONNPER=(nvl(qtr.CONNPER,0)/NULLIF((nvl(qtr.CONNPER,0)+nvl(qtr.POINTSPER,0)),0)*100),
        rpt.POINTSPER=(nvl(qtr.POINTSPER,0)/NULLIF((nvl(qtr.CONNPER,0)+nvl(qtr.POINTSPER,0)),0)*100),
        rpt.ACHIEVEMENTPER= ((nvl(qtr.CONNPER,0) + nvl(qtr.POINTSPER,0))*100)';  /* ORIGSQL: fun_sts_mapping(vrptname,'POINTSPER',NULL,vPeriodtype) */
                                                                                 /* ORIGSQL: fun_sts_mapping(vrptname,'OTC',NULL,vPeriodtype) */
                                                                                 /* ORIGSQL: fun_sts_mapping(vrptname,'CONNPER',NULL,vPeriodtype) */

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Individual1 PRODUCTS INCENTIVE co(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Individual1 PRODUCTS INCENTIVE completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ----TEAM PRODUCT INCENTIVE
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM PRODUCT INCENTIVE',NULL,NULL(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCT INCENTIVE', NULL, NULL);
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select inc.positionseq,
            inc.payeeseq,
            inc.processingunitseq,
            inc.periodseq,
            MAX(case when rmap.rptcolumnname = ''OTC'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OTC', NULL, :vPeriodtype),'') ||' end) AS OTC,
            MAX(case when rmap.rptcolumnname = ''TEAMCONNPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMCONNPER', NULL, :vPeriodtype),'') ||' end) AS CONNPER,
            MAX(case when rmap.rptcolumnname = ''TEAMPOINTSPER'' then ' ||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'TEAMPOINTSPER', NULL, :vPeriodtype),'') ||' end) AS POINTSPER
            from rpt_base_incentive inc, rpt_sts_mapping rmap
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
        when matched then update set rpt.OTC = qtr.OTC,
        rpt.CONNPER=(nvl(qtr.CONNPER,0)/NULLIF((nvl(qtr.CONNPER,0)+nvl(qtr.POINTSPER,0)),0)*100),
        rpt.POINTSPER=(nvl(qtr.POINTSPER,0)/NULLIF((nvl(qtr.CONNPER,0)+nvl(qtr.POINTSPER,0)),0)*100),
        rpt.ACHIEVEMENTPER= ((nvl(qtr.CONNPER,0) + nvl(qtr.POINTSPER,0))*100)';  /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMPOINTSPER',NULL,vPeriodtype) */
                                                                                 /* ORIGSQL: fun_sts_mapping(vrptname,'TEAMCONNPER',NULL,vPeriodtype) */
                                                                                 /* ORIGSQL: fun_sts_mapping(vrptname,'OTC',NULL,vPeriodtype) */

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin TEAM PRODUCTS INCENTIVE completed(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TEAM PRODUCTS INCENTIVE completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ---GA % Achieved ,Points % achieved ..OVERALL COMMISSION individual
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAP(...) */
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
                pad.PARTICIPANTID,/* --geid */  pad.FULLNAME,/* --name */   'Total Individual Payout' AS products,
                (mes.GAPER) AS GAPER,
                (mes.POINTSACHIEVEDPER) AS POINTSACHIEVEDPER,
                (mes_ach.ACHIEVEMENTPER) AS ACHIEVEMENTPER,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, ind(...) */
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
                        RPT_STSRCSDC_INDIVIDUAL ind
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
                AND titlemap.product = 'Total Individual Payout'
                AND titlemap.allgroups = 'EARNED COMMISSION'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        --need to change
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION INDIVIDUAL GAP(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION INDIVIDUAL GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --ACHIEVEMENTPER,GAPER,POINTSACHIEVEDPER for OVERALL TEAM
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION TEAM GAPER,POI(...) */
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
                pad.PARTICIPANTID,/* --geid */  pad.FULLNAME,/* --name */   'Total Team Payout' AS products,
                (mes.GAPER) AS GAPER,
                (mes.POINTSACHIEVEDPER) AS POINTSACHIEVEDPER,
                (mes_ach.ACHIEVEMENTPER) AS ACHIEVEMENTPER,
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, ind(...) */
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
                ) AS mes,
                (
                    SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, MAX(...) */
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

                AND titlemap.product = 'Total Team Payout'
                AND titlemap.allgroups = 'EARNED COMMISSION'
                AND titlemap.reportname = :vrptname
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        --need to change
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION TEAM GAPER,POI(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION TEAM GAPER,POINTSACHIEVEDPER and ACHIEVEMENTPER completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /*
        
        ---ADJUST COMMISSION PRODUCTS-Advance Protected Commission, Payment Adjustment  -Deposit
           ---Nandini:  -- GB1 to be defaulted to TRUE--
        
          for i in (select distinct product from rpt_sts_mapping where reportname = vrptname
         and allgroups = 'ADJUST COMMISSION' and product not in 'CE Adjustment')
         Loop
        
        prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION PRODUCTS Deposit',NULL,i.product);
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
                                                    BASICSAMT,
                                                    shopname,
                                                    teamvisible,
                                                    payable_flag,
                                                    reportgroup
                                                )
        SELECT   '''||vTenantID||''',
                 pad.positionseq,
                 pad.payeeseq,
                 '||vProcessingUnitRow.processingunitseq||',
                 '||vperiodseq||',
                 '''||vPeriodRow.name||''',
                 '''||vProcessingUnitRow.name||''',
                 '''||vCalendarRow.name||''',
                 ''59'' reportcode,
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
                 BASICSAMT,
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
                               titlemap.titlename,
                            '''||i.product||''' product,
                            MAX(case when rmap.rptcolumnname = ''BASICSAMT'' then '||fun_sts_mapping(vrptname,'BASICSAMT',i.product)||' end) AS BASICSAMT
                          FROM rpt_base_deposit CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                            WHERE CM.name in rmap.rulename
            
             and rmap.product=titlemap.product
             and rmap.allgroups=titlemap.allgroups
             and rmap.reportname=titlemap.reportname
             and rmap.report_frequency=titlemap.report_frequency
             AND rmap.reportname= '''||vrptname||'''
             and CM.periodseq = '||vperiodseq||'
             and CM.processingunitseq = '||vprocessingunitseq||'
             and rmap.product = '''||i.product||'''
             and rmap.allgroups = ''ADJUST COMMISSION''
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
         and pad.reportgroup = '''||v_reportgroup||'''';          --need to change
        
        prc_logevent (:vPeriodRow.name,vProcName,'Completed ADJUST COMMISSION PRODUCTS Deposit',NULL,v_sql);
        
        EXECUTE IMMEDIATE v_sql;
        
        COMMIT;
        
          end loop;
        
          -----------ADJUST commission --measurement
         ---Nandini:  -- GB1 to be defaulted to TRUE--
          prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION MEASUREMENT',NULL,'CE Adjustment');
          v_sql :=
        'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
                 (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                      processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                      sortorder,titlename,loaddttm,
                      allgroups,products, remarks,overallper,basicsamt,shopname,teamvisible,payable_flag,reportgroup
                  )
           SELECT   '''||vTenantID||''', pad.positionseq, pad.payeeseq, '||vProcessingUnitRow.processingunitseq||',
                       '||vperiodseq||','''||vPeriodRow.name||''',             '''||vProcessingUnitRow.name||''','''||vCalendarRow.name||''',''59'' reportcode,
                    04 sectionid,''ADJUST COMMISSION'' sectionname,''ADJUST COMMISSION'' subsectionname,
                    02 sortorder, pad.reporttitle titlename, SYSDATE,
                    ''ADJUST COMMISSION'' allgroups,''CE Adjustment'' products, REMARKS,(OVERALLPER*100) OVERALLPER,BASICSAMT,
                    pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,'||v_payableflag||' payable_flag,'''||v_reportgroup||''' reportgroup
           FROM   rpt_base_padimension pad,
             (   select mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq,
                                    titlemap.titlename,
                                 max(case when rmap.rptcolumnname = ''REMARKS'' then ''Band '' ||nvl('||fun_sts_mapping(vrptname,'CEBAND','CE Adjustment')||',''0'') end) REMARKS ,
                                 MAX(case when rmap.rptcolumnname = ''OVERALLPER'' then '||fun_sts_mapping(vrptname,'OVERALLPER','CE Adjustment')||' end) AS OVERALLPER,
                                 MAX(case when rmap.rptcolumnname = ''BASICSAMT'' then '||fun_sts_mapping(vrptname,'BASICSAMT','CE Adjustment')||' end) AS BASICSAMT
                                 from RPT_BASE_MEASUREMENT mes,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
                              where  mes.name in rmap.rulename
             and rmap.product=titlemap.product
             and rmap.allgroups=titlemap.allgroups
             and rmap.report_frequency=titlemap.report_frequency
             and rmap.reportname=titlemap.reportname
             AND rmap.reportname= '''||vrptname||'''
             and mes.periodseq = '||vperiodseq||'
             and mes.processingunitseq = '||vprocessingunitseq||'
             and rmap.product = ''CE Adjustment''
             and rmap.allgroups = ''ADJUST COMMISSION''
                              group by mes.positionseq,
                                   mes.payeeseq,
                                   mes.processingunitseq,
                                   mes.periodseq   	  ,titlemap.titlename
              )mes
           WHERE pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         and pad.periodseq = mes.periodseq
         and pad.POSITIONTITLE=mes.titlename
         and pad.reportgroup = '''||v_reportgroup||'''';
        
        EXECUTE IMMEDIATE v_sql;
         prc_logevent (:vPeriodRow.name,vProcName,'END ADJUST COMMISSION MEASUREMENT',NULL,v_sql);
        
        COMMIT;
        
          --ADJUST COMMISSION CREDIT..Remarks
         ---Nandini:  -- GB1 to be defaulted to TRUE--
          prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Credit REMARKS',NULL,'REMARKS');
          INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
                 (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                      processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                      sortorder,titlename,loaddttm,
                      allgroups,products, remarks,shopname,teamvisible,payable_flag,reportgroup
                  )
           SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,vCalendarRow.name,'59' reportcode,
                    '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                    '03' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST REMARKS' allgroups,'ADJUST REMARKS' products, REMARKS,pad.POSITIONGA1 shopname,
                    nvl(pad.TITLEGB1,1) teamvisible,v_payableflag,v_reportgroup reportgroup
           FROM   rpt_base_padimension pad,
             (    select mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq,
                                    titlemap.titlename,
                                 max(mes.genericattribute3) Remarks
            
                          from rpt_base_credit mes,RPT_TITLE_PRODUCT_MAPPING titlemap
                          where mes.processingunitseq = vprocessingunitseq
             and mes.periodseq = vperiodseq
             and titlemap.product='ADJUST REMARKS'
             and titlemap.allgroups='ADJUST REMARKS'
             and titlemap.reportname= vrptname
             and mes.credittypeid = pkg_reporting_extract_r2.vcredittypeid_PayAdj
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
         and pad.reportgroup = v_reportgroup;
        
           COMMIT;
          prc_logevent (:vPeriodRow.name,vProcName,'ADJUST COMMISSION Credit REMARKS Completed',NULL,'REMARKS');
        
         --ADJUST commission CREDIT..Operational Compliance Adjustment
          prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Credit Operational Compliance Adjustment',NULL,'Operational Compliance Adjustment');
         ---Nandini:  -- GB1 to be defaulted to TRUE--
          INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
                 (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                      processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
                      sortorder,titlename,loaddttm,
                      allgroups,products, BASICSAMT,shopname,teamvisible,payable_flag,reportgroup
                  )
           SELECT   vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,vCalendarRow.name,'59' reportcode,
                    '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                    '04' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST COMMISSION' allgroups,'Operational Compliance Adjustment' products, BASICSAMT,
                    pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
           FROM   rpt_base_padimension pad,
             (    select mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq,
                                    titlemap.titlename,
                                 sum(mes.value) BASICSAMT
                          from rpt_base_credit mes,RPT_TITLE_PRODUCT_MAPPING titlemap
                          where mes.processingunitseq = vprocessingunitseq
             and mes.periodseq = vperiodseq
             and titlemap.product='Operational Compliance Adjustment'
             and titlemap.allgroups='ADJUST COMMISSION'
             and titlemap.reportname= vrptname
             and mes.credittypeid = pkg_reporting_extract_r2.vcredittypeid_PayAdj
             and mes.genericattribute1 = pkg_reporting_extract_r2.vOperational_Compliance
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
         and pad.reportgroup = v_reportgroup;
        
           COMMIT;
          prc_logevent (:vPeriodRow.name,vProcName,'ADJUST COMMISSION Credit Operational Compliance Adjustment Completed',NULL,'Operational Compliance Adjustment');
        
        --ADJUST commission BALANCE ..Prior Balance Adjustment
        prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Balance Prior Balance Adjustment',NULL,'Balance Prior Balance Adjustment');
        ---Nandini:  -- GB1 to be defaulted to TRUE--
         INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
         (tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
              processingunitname, calendarname, reportcode, sectionid, sectionname,subsectionname,
              sortorder,titlename,loaddttm,
              allgroups, products, BASICSAMT,shopname,teamvisible,payable_flag,reportgroup
          )
         SELECT    vTenantID, pad.positionseq, pad.payeeseq,vProcessingUnitRow.processingunitseq,
                    vperiodseq,vPeriodRow.name,vProcessingUnitRow.name,vCalendarRow.name,'59' reportcode,
                    '04' sectionid,'ADJUST COMMISSION' sectionname,'ADJUST COMMISSION' subsectionname,
                    '05' sortorder, pad.reporttitle titlename, SYSDATE,
                    'ADJUST COMMISSION' allgroups,'Prior Balance Adjustment' products, BASICSAMT,
                    pad.POSITIONGA1 shopname, nvl(pad.TITLEGB1,1) teamvisible,v_payableflag payable_flag,v_reportgroup reportgroup
         FROM   rpt_base_padimension pad,RPT_TITLE_PRODUCT_MAPPING titlemap ,
         (     select pay.positionseq,
                       pay.payeeseq,
                       pay.processingunitseq,
                       pay.periodseq,
                       sum(bal.value) BASICSAMT
                  from cs_balance bal,
                    cs_balancepaymenttrace baltrace,
                    cs_payment pay
                  where
                    bal.periodseq=baltrace.sourceperiodseq
             and baltrace.targetperiodseq = pay.periodseq
             and bal.balanceseq = baltrace.balanceseq
             and bal.payeeseq=pay.payeeseq
             and bal.processingunitseq = pay.processingunitseq
             and bal.processingunitseq = baltrace.processingunitseq
             and pay.periodseq = vperiodseq
             and pay.processingunitseq = vprocessingunitseq
                  group by pay.positionseq,
                         pay.payeeseq,
                         pay.processingunitseq,
                         pay.periodseq
          )mes
           WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         and pad.periodseq = mes.periodseq
         and pad.POSITIONTITLE=titlemap.titlename
         and titlemap.product='Prior Balance Adjustment'
         and titlemap.allgroups='ADJUST COMMISSION'
         and titlemap.reportname= vrptname
         and pad.reportgroup = v_reportgroup;
        
          COMMIT;
        prc_logevent (:vPeriodRow.name,vProcName,'ADJUST COMMISSION Balance Prior Balance Adjustment Completed',NULL,'Balance Prior Balance Adjustment');
        
          */

        -----Overall Commission-Actual WD/Plan WD..measurement
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Overall Commission Measurement wo(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Overall Commission Measurement working day', NULL, 'POINTSPAYOUT');
        v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            MAX(case when rmap.rptcolumnname = ''WDAYSPER'' then '||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'WDAYSPER', 'Total Team Payout', :vPeriodtype),'') ||' end) AS WDAYSPER
            from RPT_BASE_MEASUREMENT mes, rpt_sts_mapping rmap
            where mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and  mes.name in rmap.rulename
            and rmap.reportname = '''||IFNULL(:vrptname,'')||'''
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and rmap.product = ''Total Team Payout''
            and rmap.allgroups=''EARNED COMMISSION''
            and rmap.report_frequency ='''||IFNULL(:vPeriodtype,'')||'''
            GROUP BY mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
        rmap.allgroups)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
            and rpt.payeeseq = qtr.payeeseq
            and rpt.products = ''Total Team Payout''
            and rpt.frequency='''||IFNULL(:vPeriodtype,'')||''')
        when matched then update set rpt.WDAYSPER = (qtr.WDAYSPER*100)';  /* ORIGSQL: fun_sts_mapping(vrptname,'WDAYSPER','Total Team Payout',vPeriodtype) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Overall Commission Measurement working (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Overall Commission Measurement working day completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        FOR i AS cursor_earned_comm
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for (...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION Incentive for Overall, Basic and Multiplier amount completed', NULL, :i.product);
            v_sql = 'MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using
            (select inc.positionseq,
                inc.payeeseq,
                inc.processingunitseq,
                inc.periodseq,
                MAX(case when rmap.rptcolumnname = ''OVERALLPER'' then '||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'OVERALLPER', :i.product, :vPeriodtype),'') ||' end) AS OVERALLPER,
                MAX(case when rmap.rptcolumnname = ''BASICSAMT1'' then '||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'BASICSAMT1', :i.product, :vPeriodtype),'') ||' end) AS BASICSAMT,
                MAX(case when rmap.rptcolumnname = ''MULTIPLIERAMT'' then '||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'MULTIPLIERAMT', :i.product, :vPeriodtype),'') ||' end) AS MULTIPLIERAMT
                from rpt_base_incentive inc, rpt_sts_mapping rmap
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
            rpt.MULTIPLIERAMT=qtr.MULTIPLIERAMT';  /* ORIGSQL: fun_sts_mapping(vrptname,'OVERALLPER',i.product,vPeriodtype) */
                                                   /* ORIGSQL: fun_sts_mapping(vrptname,'MULTIPLIERAMT',i.product,vPeriodtype) */
                                                   /* ORIGSQL: fun_sts_mapping(vrptname,'BASICSAMT1',i.product,vPeriodtype) */

            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for (...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION Incentive for Overall, Basic and Multiplier amount completed', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        --TOTAL in EARNED COMMISSION   
        /* ORIGSQL: UPDATE RPT_STSRCSDC_INDIVIDUAL SET TOTAL= (nvl(BASICSAMT,0) +nvl(MULTIPLIERAMT,0(...) */
        UPDATE RPT_STSRCSDC_INDIVIDUAL
            SET
            /* ORIGSQL: TOTAL = */
            TOTAL = (IFNULL(BASICSAMT,0) +IFNULL(MULTIPLIERAMT,0)),  /* ORIGSQL: nvl(MULTIPLIERAMT,0) */
                                                                     /* ORIGSQL: nvl(BASICSAMT,0) */
            /* ORIGSQL: payable_flag = */
            payable_flag = :v_payableflag
        FROM
            RPT_STSRCSDC_INDIVIDUAL
        WHERE
            processingunitseq = :vprocessingunitseq
            AND periodseq = :vperiodseq
            AND allgroups = 'EARNED COMMISSION'
            AND sectionname = 'OVERALL COMMISSION' /* --need to change  */
            AND subsectionname = 'EARNED COMMISSION';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --OVERALL SECTION EARNED COMISSION TOTAL
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL SECTION EARNED COMISSION (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL SECTION EARNED COMISSION TOTAL', NULL, 'Earned Commision');

        ---Nandini:  -- GB1 to be defaulted to TRUE--  

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, subsectionname,
                sortorder, titlename, loaddttm, allgroups, products, shopname,
                teamvisible, payable_flag, SECTION_COMMISSION, reportgroup, frequency
            )
            SELECT   /* ORIGSQL: SELECT vTenantID,pad.positionseq,pad.payeeseq, :vProcessingUnitRow.processingunit(...) */
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
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
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
                AND pad.frequency = :vPeriodtype
                AND pad.frequency = titlemap.report_frequency

                AND pad.frequency = titlemap.report_frequency
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'OVERALL SECTION EARNED COMISSION TOTAL (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL SECTION EARNED COMISSION TOTAL Completed', NULL, 'Earned Commision');

        FOR i AS cursor_indv_products
        DO
            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION PRODUCTS Deposi(...) */
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
            SELECT   '''||IFNULL(:vTenantId,'')||''',
            pad.positionseq,
            pad.payeeseq,
            '||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
            '||IFNULL(:vperiodseq,'')||',
            '''||IFNULL(:vPeriodRow.name,'')||''',
            '''||IFNULL(:vProcessingUnitRow.name,'')||''',
            '''||IFNULL(:vCalendarRow.name,'')||''',
            ''59'' reportcode,
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
            FROM   rpt_base_padimension pad,
            (
                SELECT
                CM.positionseq,
                CM.payeeseq,
                CM.processingunitseq,
                CM.periodseq,
                titlemap.titlename,
                rmap.allgroups allgroups,
                '''||IFNULL(:i.product,'')||''' product,
                MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CECOMM', :i.product, :vPeriodtype),'') ||' end) AS CECOMM
                FROM rpt_base_deposit CM,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
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
            and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_sts_mapping(vrptname,'CECOMM',i.product,vPeriodtype) */
            --need to change

            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Completed ADJUST COMMISSION PRODUCTS De(...) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Completed ADJUST COMMISSION PRODUCTS Deposit', NULL, :v_sql);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END FOR;  /* ORIGSQL: end loop; */

        --ADJUST commission BALANCE ..Prior Balance Adjustment
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Balance Prior B(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Balance Prior Balance Adjustment', NULL, 'Balance Prior Balance Adjustment');

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCEPAYMENTTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYMENT' not found */
        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                '59' AS reportcode,
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
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
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

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'ADJUST COMMISSION Balance Prior Balance(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Balance Prior Balance Adjustment Completed', NULL, 'Balance Prior Balance Adjustment');

        -- Adjustment Commission : CE Adjustment
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION MEASUREMENT',NU(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION MEASUREMENT', NULL, 'CE Adjustment');
        v_sql = 'INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
        pad.POSITIONGA1 shopname,nvl(pad.TITLEGB1,1) teamvisible,'||IFNULL(TO_VARCHAR(:v_payableflag),'')||' payable_flag,'''||IFNULL(:v_reportgroup,'')||''' reportgroup ,'''||IFNULL(:vPeriodtype,'')||''' frequency
        FROM   rpt_base_padimension pad,
        (   select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            titlemap.titlename,
            MAX(case when rmap.rptcolumnname = ''CECOMM'' then '||IFNULL(EXT.FUN_STS_MAPPING(:vrptname, 'CECOMM', 'CE Adjustment', :vPeriodtype),'') ||' end) AS CECOMM
            from RPT_BASE_MEASUREMENT mes,rpt_sts_mapping rmap,RPT_TITLE_PRODUCT_MAPPING titlemap
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
        and pad.reportgroup = '''||IFNULL(:v_reportgroup,'')||'''';  /* ORIGSQL: fun_sts_mapping(vrptname,'CECOMM','CE Adjustment',vPeriodtype) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'END ADJUST COMMISSION MEASUREMENT',NULL(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'END ADJUST COMMISSION MEASUREMENT', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --CE Adjustment merge
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin OVERALL COMMISSION Incentive for (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin OVERALL COMMISSION Incentive for Individual', NULL, 'Individual Payout'); 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using (SELECT cre.positionseq, cre.payees(...) */
        MERGE INTO RPT_STSRCSDC_INDIVIDUAL AS rpt
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
                    rpt_base_credit cre
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

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE A(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Adjust commssion  credit for CE Adjustment start', NULL, 'CE Adjustment Start');

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Adjust commssion  credit for CE A(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Adjust commssion  credit for CE Adjustment complete', NULL, 'CE Adjustment complete');
        ---ADJUST commission-- Total Adjustment
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST commission Total Adjustmen(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST commission Total Adjustment', NULL, 'Total Adjustment');  

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                '59' AS reportcode,
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
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
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
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'ADJUST commission Total Adjustment Comp(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST commission Total Adjustment Complete', NULL, 'Total Adjustment');

        --ADJUST COMMISSION CREDIT..Remarks
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Credit REMARKS'(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Credit REMARKS', NULL, 'REMARKS');  

        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                '59' AS reportcode,
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
                rpt_base_padimension pad,
                RPT_TITLE_PRODUCT_MAPPING titlemap,
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, MAX(...) */
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

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'ADJUST COMMISSION Credit REMARKS Comple(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'ADJUST COMMISSION Credit REMARKS Completed', NULL, 'REMARKS');

        --Total Commission Payout
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin ADJUST COMMISSION Total Commissio(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin ADJUST COMMISSION Total Commission Payout', NULL, 'Total Commission Payout');

        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_DEPOSIT' not found */
        /* ORIGSQL: INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL (tenantid, positionseq, payeeseq, pr(...) */
        INSERT INTO EXT.RPT_STSRCSDC_INDIVIDUAL
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
                '59' AS reportcode,
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
                IFNULL(TOTALCOMMISSION,0) AS TOTALCOMMISSION,  /* ORIGSQL: nvl(TOTALCOMMISSION,0) */
                pad.POSITIONGA1 AS shopname,
                IFNULL(pad.TITLEGB1,1) AS teamvisible,  /* ORIGSQL: nvl(pad.TITLEGB1,1) */
                :v_payableflag AS payable_flag,
                :v_reportgroup AS reportgroup,
                :vPeriodtype AS frequency
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                (
                    /*select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq,
                           sum(mes.SECTION_COMMISSION) TOTALCOMMISSION
                         from RPT_STSRCSDC_INDIVIDUAL mes
                           where mes.allgroups in ('ADJUST REMARKS','EARNED COMMISSION')
                     and mes.periodseq = vperiodseq
                     and mes.processingunitseq = vprocessingunitseq
                         group by mes.positionseq,
                           mes.payeeseq,
                           mes.processingunitseq,
                           mes.periodseq  */

                    SELECT   /* ORIGSQL: (select dep.positionseq, dep.payeeseq, dep.processingunitseq, dep.periodseq, SUM(...) */
                        dep.positionseq,
                        dep.payeeseq,
                        dep.processingunitseq,
                        dep.periodseq,
                        SUM(dep.value) AS TOTALCOMMISSION
                    FROM
                        rpt_base_deposit dep
                    WHERE
                        dep.processingunitseq = :vprocessingunitseq
                        AND dep.periodseq = :vperiodseq
                        AND dep.EARNINGGROUPID = 'Net Payment'
                    GROUP BY
                        dep.positionseq,
                        dep.payeeseq,
                        dep.processingunitseq,
                        dep.periodseq
                ) AS mes
            RIGHT OUTER JOIN
                EXT.rpt_base_padimension AS pad
                ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
            INNER JOIN
                EXT.RPT_TITLE_PRODUCT_MAPPING AS titlemap
                ON pad.POSITIONTITLE = titlemap.titlename
                AND titlemap.product = 'Total Commission Payout' 
                AND titlemap.allgroups = 'ADJUST REMARKS' 
                AND titlemap.reportname = :vrptname
                AND pad.frequency = titlemap.report_frequency
            WHERE
                pad.frequency = :vPeriodtype
                AND pad.reportgroup = :v_reportgroup;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'OVERALL COMMISSION Total Commission Pay(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'OVERALL COMMISSION Total Commission Payout Completed', NULL, 'Total Commission Payout');

        --sudhir end
        --Update the null OTC,GEID,NAME 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDC_INDIVIDUAL rpt using (SELECT distinct mes.positionseq, m(...) */
        MERGE INTO RPT_STSRCSDC_INDIVIDUAL AS rpt 
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
            UPDATE SET rpt.OTC = qtr.OTC,rpt.GEID = qtr.GEID,rpt.NAME = qtr.NAME,rpt.shopname = qtr.shopname;

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
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);/*Deepan : Partition Not required*/


        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END