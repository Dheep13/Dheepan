CREATE PROCEDURE EXT.PRC_STSRCSDS_PAYEE_SUMMARY
(
    --IN vrptname rpt_sts_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_sts_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                                   /* RESOLVE: Datatype unresolved: Datatype (rpt_sts_mapping.reportname%TYPE) not resolved for parameter 'PRC_STSRCSDS_PAYEE_SUMMARY.vrptname' */
                                                   /* ORIGSQL: vrptname IN rpt_sts_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_STSRCSDS_PAYEE_SUMMARY.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_STSRCSDS_PAYEE_SUMMARY.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_STSRCSDS_PAYEE_SUMMARY.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
   -- DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();

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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_STSRCSDS_PAYEE_SUMMARY');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_STSRCSDS_PAYEE_SUMMARY') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_STSRCSDS_PAYEE_SUMMARY';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_STSRCSDS_PAYEE_SUMMARY'; */
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
    DECLARE v_reportgroup1 VARCHAR(127);  /* ORIGSQL: v_reportgroup1 VARCHAR2(127); */
    DECLARE v_reportgroup2 VARCHAR(127);  /* ORIGSQL: v_reportgroup2 VARCHAR2(127); */
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

        /* initialize session variables, if not yet done */
        CALL EXT.init_session_global();

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
            );--Sanjay Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

        vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as AnalyzeTableSubpartition are not required

        --------Truncate report table subpartition------------------------------------------------------------------------
        --pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName,
        --                                                   vSubpartitionName);

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as AnalyzeTableSubpartition are not required

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
        IF :vrptname = 'STSPAYEEACHIVEMENT' 
        THEN
            v_reportgroup1 = 'STS';

            v_reportgroup2 = NULL;
        ELSEIF :vrptname = 'DSPAYEEACHIVEMENT'   /* ORIGSQL: elsif vrptname = 'DSPAYEEACHIVEMENT' Then */
        THEN
            v_reportgroup1 = 'Digital Telesales';

            v_reportgroup2 = NULL;
        ELSEIF :vrptname = 'RCSPAYEEACHIVEMENT'   /* ORIGSQL: elsif vrptname = 'RCSPAYEEACHIVEMENT' Then */
        THEN
            v_reportgroup1 = 'RCS';

            v_reportgroup2 = 'RCSMFONG';
            --need to change
        END IF;

        -----DELETE EXISTING RECORDS BASED ON REPORT GROUP 

        /* ORIGSQL: DELETE FROM RPT_STSRCSDS_PAYEE_SUMMARY WHERE periodseq=vperiodseq and processing(...) */
        DELETE
        FROM
            RPT_STSRCSDS_PAYEE_SUMMARY
        WHERE
            periodseq = :vperiodseq
            AND processingunitseq = :vprocessingunitseq
            AND reportgroup IN (:v_reportgroup1,:v_reportgroup2);

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);

        --TEAM OVERALL

        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_STSRCSDS_PAYEE_SUMMARY' not found */

        /* ORIGSQL: INSERT INTO STELEXT.RPT_STSRCSDS_PAYEE_SUMMARY (tenantid,positionseq,payeeseq,pr(...) */
        INSERT INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, shoppayeeseq,
                sortorder, titlename, loaddttm, shopname, id, geid,
                staffname, salesmancode, designation, clusterid, teamid, otc,
                TEAMGACONN, TEAMPOINTS, TEAMOVERALL, ACTUALPLANWD, reportgroup
            )
            SELECT   /* ORIGSQL: SELECT ind.tenantid,ind.positionseq, ind.payeeseq,ind.processingunitseq,ind.peri(...) */
                ind.tenantid,
                ind.positionseq,
                ind.payeeseq,
                ind.processingunitseq,
                ind.periodseq,
                ind.periodname,
                ind.processingunitname,
                ind.calendarname,
                '60' AS reportcode,
                01 AS sectionid,
                'DETAIL' AS sectionname,
                NULL,
                01 AS sortorder,
                ind.titlename,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                ind.shopname,
                NULL,
                ind.GEID,
                ind.NAME,
                pad.PARTICIPANTGA1,
                pad.POSITIONTITLE,
                NULL,
                pad.POSITIONGA1,
                ind.otc,
                GAPER,
                POINTSACHIEVEDPER,
                OVERALLPER,
                WDAYSPER,
                :v_reportgroup1
            FROM
                ext.rpt_base_padimension pad,
                EXT.RPT_STSRCSDC_INDIVIDUAL ind
            WHERE
                ind.periodseq = :vperiodseq
                AND ind.processingunitseq = :vprocessingunitseq
                AND ind.sectionname = 'OVERALL COMMISSION'
                AND ind.allgroups = 'EARNED COMMISSION'
                AND ind.products IN ('Total Team Payout','Team')
                AND pad.payeeseq = ind.payeeseq
                AND pad.positionseq = ind.positionseq
                AND pad.processingunitseq = ind.processingunitseq
                AND pad.periodseq = ind.periodseq
                AND pad.POSITIONTITLE NOT IN ('STS - Director')
                AND pad.reportgroup IN (:v_reportgroup1,:v_reportgroup2);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --INDIVIDUAL OVERALL

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_STSRCSDS_PAYEE_SUMMARY' not found */

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_STSRCSDC_INDIVIDUAL' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, GAP(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    GAPER,
                    POINTSACHIEVEDPER,
                    OVERALLPER
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'OVERALL COMMISSION'
                    AND ind.allgroups = 'EARNED COMMISSION'
                    AND ind.products IN ('Total Individual Payout','Individual')
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
                AND rpt.sectionname = 'DETAIL'
            )
        WHEN MATCHED THEN
            UPDATE SET rpt.INDGACONN = qtr.GAPER, rpt.INDPOINTS = qtr.POINTSACHIEVEDPER,
                rpt.INDOVERALL = qtr.OVERALLPER;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ---Advance Protected Commission (New Staff) 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, CEC(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    CECOMM
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST COMMISSION'
                    AND ind.products = 'Advance Protected Commission (New Staff)'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
                AND rpt.sectionname = 'DETAIL'
            )
        WHEN MATCHED THEN
            UPDATE SET rpt.ADVPROTCOMM = qtr.CECOMM;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ---Payment Adjustment 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, CEC(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    CECOMM
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST COMMISSION'
                    AND ind.products = 'Payment Adjustment'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
                AND rpt.sectionname = 'DETAIL'
            )
        WHEN MATCHED THEN
            UPDATE SET rpt.PAYMENTADJ = qtr.CECOMM;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Prior Balance Adjustment 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, CEC(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    CECOMM
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST COMMISSION'
                    AND ind.products = 'Prior Balance Adjustment'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.PRIORBAL = qtr.CECOMM;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --CE Adjustment 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, CEC(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    CECOMM
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST COMMISSION'
                    AND ind.products = 'CE Adjustment'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.CEADJ = qtr.CECOMM;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ---Operational Compliance Adjustment 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, CEC(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    CECOMM
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST COMMISSION'
                    AND ind.products = 'Operational Compliance Adjustment'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.COMPLIANCEADJ = qtr.CECOMM;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Total Adjustment 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, SEC(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    SECTION_COMMISSION
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST REMARKS'
                    AND ind.products = 'Total Adjustment'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.TOTALADJ = qtr.SECTION_COMMISSION;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Adjust Remarks 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, REM(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    REMARKS
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST REMARKS'
                    AND ind.products = 'Adjust Remarks'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.REMARKS = qtr.REMARKS;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Total Commission Payout 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, TOT(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    TOTALCOMMISSION
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'TOTAL COMMISSION'
                    AND ind.allgroups = 'ADJUST REMARKS'
                    AND ind.products = 'Total Commission Payout'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.TOTALCOMMPAYOUT = qtr.TOTALCOMMISSION;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --CE Band 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, REM(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    REMARKS
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'ADJUST COMMISSION'
                    AND ind.allgroups = 'ADJUST COMMISSION'
                    AND ind.products = 'CE Adjustment'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.CEBAND = qtr.REMARKS;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --EARNEDCOMM 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, SEC(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    SECTION_COMMISSION
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'OVERALL COMMISSION'
                    AND ind.allgroups = 'EARNED COMMISSION'
                    AND ind.products = 'Earned Commission'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.EARNEDCOMM = qtr.SECTION_COMMISSION;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --INDMULTIPLIER 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, MUL(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    MULTIPLIERPER
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'INDIVIDUAL ACHIEVEMENT'
                    AND ind.allgroups = 'POINTS PAYOUT'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.INDMULTIPLIER = qtr.MULTIPLIERPER;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --TEAMMULTIPLIER 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT ind.positionseq, ind.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select ind.positionseq, ind.payeeseq, ind.processingunitseq, ind.periodseq, MUL(...) */
                    ind.positionseq,
                    ind.payeeseq,
                    ind.processingunitseq,
                    ind.periodseq,
                    MULTIPLIERPER
                FROM
                    EXT.RPT_STSRCSDC_INDIVIDUAL ind
                WHERE
                    ind.processingunitseq = :vprocessingunitseq
                    AND ind.periodseq = :vperiodseq
                    AND ind.sectionname = 'TEAM ACHIEVEMENT'
                    AND ind.allgroups = 'TEAM PAYOUT'
                    AND ind.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.TEAMMULTIPLIER = qtr.MULTIPLIERPER;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --shoppayeeseq 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT mes.positionseq, mes.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, mes(...) */
                    mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    mes.lastname
                FROM
                    ext.rpt_base_padimension mes
                WHERE
                    mes.processingunitseq = :vprocessingunitseq
                    AND mes.periodseq = :vperiodseq
                    AND mes.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.shopname = qtr.lastname
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.shoppayeeseq = qtr.payeeseq;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Titlename 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STSRCSDS_PAYEE_SUMMARY rpt using (SELECT mes.positionseq, mes.pay(...) */
        MERGE INTO EXT.RPT_STSRCSDS_PAYEE_SUMMARY AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, mes(...) */
                    mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    mes.POSITIONTITLE
                FROM
                    EXT.RPT_base_padimension mes
                WHERE
                    mes.processingunitseq = :vprocessingunitseq
                    AND mes.periodseq = :vperiodseq
                    AND mes.reportgroup IN (:v_reportgroup1,:v_reportgroup2)
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionname = 'DETAIL')
        WHEN MATCHED THEN
            UPDATE SET rpt.TITLENAME = qtr.POSITIONTITLE;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --TOTAL for OTC, TOTALCOMMPAYOUT and ID
        /* -- This Total Part handled in Crystal report
        prc_logevent (vPeriodRow.name,vProcName,'Begin TOTAL insert',NULL,NULL);
        
        INSERT INTO RPT_STSRCSDS_PAYEE_SUMMARY
         (tenantid,positionseq,payeeseq,processingunitseq, periodseq,
                 periodname,processingunitname,calendarname,reportcode,sectionid,
                 sectionname,sortorder,titlename,loaddttm,reportgroup,OTC,TOTALCOMMPAYOUT
          )
        SELECT vTenantID, null, null, vProcessingUnitRow.processingunitseq, vperiodseq, vPeriodRow.name,
          vProcessingUnitRow.name, vCalendarRow.name, '60' reportcode, 99 sectionid, 'DETAIL' sectionname,
          99 sortorder,'Monthly Payee Summary Payout' titlename,SYSDATE,v_reportgroup,
          rpt.OTC, rpt.TOTALCOMMPAYOUT
        FROM
        (   select
                sum(OTC) OTC,
                sum(TOTALCOMMPAYOUT) TOTALCOMMPAYOUT
                from RPT_STSRCSDS_PAYEE_SUMMARY tab
                where tab.processingunitseq = vprocessingunitseq
             and tab.periodseq = vperiodseq
             and tab.sectionname = 'DETAIL'
             and reportgroup = v_reportgroup
        ) rpt;
        COMMIT;
        
         UPDATE RPT_STSRCSDS_PAYEE_SUMMARY tab SET ID =
        (select count(distinct payeeseq)
                from RPT_STSRCSDS_PAYEE_SUMMARY
              where processingunitseq = vprocessingunitseq
             and periodseq = vperiodseq
             and reportgroup = v_reportgroup
        )
         where tab.processingunitseq = vprocessingunitseq
         and tab.periodseq = vperiodseq
         and tab.sectionname = 'DETAIL'
         and tab.sectionid = 99;
        
          COMMIT;
        
         */

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
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END