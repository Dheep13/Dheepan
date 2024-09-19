CREATE PROCEDURE EXT.PRC_COMM_PAYOUT_MONTHLY
(
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_COMM_PAYOUT_MONTHLY.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_COMM_PAYOUT_MONTHLY.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_COMM_PAYOUT_MONTHLY.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_COMM_PAYOUT_MONTHLY');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_COMM_PAYOUT_MONTHLY') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_COMM_PAYOUT_MONTHLY';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_COMM_PAYOUT_MONTHLY'; */
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
            );--Sanjay: Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

        vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);Sanjay: Commenting out as Analyze are not required.

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);Sanjay: Commenting out as truncate are not required.

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);Sanjay: Commenting out as truncate are not required.

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
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin Internal Payroll Monthly report',(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Internal Payroll Monthly report', NULL, NULL);

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_UNITTYPE' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_COMM_PAYOUT_MONTHLY' not found */

        /* ORIGSQL: INSERT INTO STELEXT.RPT_COMM_PAYOUT_MONTHLY(TENANTID,POSITIONSEQ,PAYEESEQ,PROCES(...) */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
        INSERT INTO EXT.RPT_COMM_PAYOUT_MONTHLY
            (
                TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME,
                PROCESSINGUNITNAME, CALENDARNAME, REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER,
                EMPFIRSTNAME, EMPLASTNAME, TITLENAME, POSITIONNAME, ANCESTOREUSERID, GEID,
                EMPLOYEENAME, EFFECTIVEDATE, WAGETYPEDESCRIPTION, WAGETYPECODE, AMOUNT, CURRENCY,
                REMARKS
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
                '76' AS reportcode,
                '01' AS sectionid,
                'DETAIL' AS sectionname,
                '01' AS sortorder,
                pad.FIRSTNAME AS empfirstname,
                pad.LASTNAME AS emplastname,
                pad.REPORTTITLE AS titlename,
                pad.POSITIONNAME,
                IFNULL(TO_VARCHAR(pad.userid),'')||'_'||IFNULL(:vTenantId,''),
                pad.positionname AS GEID,
                pad.LASTNAME AS EMPLOYEENAME,
                pad.PERIODSTARTDATE,
                NULL AS WAGETYPEDESCRIPTION,
                NULL AS WAGETYPECODE,
                mes.AMOUNT AS AMOUNT,
                mes.CURRENCY AS CURRENCY,
                NULL AS REMARKS
            FROM
                ext.rpt_base_padimension pad,
                (
                    SELECT   /* ORIGSQL: (SELECT pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(...) */
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq,
                        SUM(pay.VALUE) AS AMOUNT,
                        /* -- pay.UNITTYPEFORVALUE CURRENCY  */
                        typ.name AS CURRENCY
                    FROM
                        CS_PAYMENT pay,
                        cs_unittype typ
                    WHERE
                        pay.periodseq = :vperiodseq
                        AND pay.processingunitseq = :vprocessingunitseq
                        AND typ.unittypeseq = pay.unittypeforvalue
                        AND typ.removedate = :cEndofTime  /* ORIGSQL: pkg_reporting_extract_r2.cEndofTime */
                        AND pay.EARNINGGROUPID = 'Commission'
                    GROUP BY
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq,
                        typ.name
                ) AS mes
            WHERE
                pad.payeeseq = mes.payeeseq
                AND pad.positionseq = mes.positionseq
                AND pad.processingunitseq = mes.processingunitseq
                AND pad.periodseq = mes.periodseq
                AND pad.POSITIONTITLE <> 'External Vendors_CSE';

        -- and = mes.CURRENCY

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Internal Payroll Monthly report complet(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Internal Payroll Monthly report completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Deposit.GA3 Merge for Wage code and Description
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_COMM_PAYOUT_MONTHLY' not found */

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_COMM_PAYOUT_MONTHLY rpt using (SELECT distinct Dep.genericattribu(...) */
        MERGE INTO EXT.RPT_COMM_PAYOUT_MONTHLY AS rpt 
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_APPLDEPOSITPAYMENTTRACE' not found */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSITAPPLDEPOSITTRACE' not found */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSIT' not found */
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct Dep.genericattribute3 DEPOSITVAL, Pay.payeeseq, Pay.positionseq(...) */
                    DISTINCT
                    Dep.genericattribute3 AS DEPOSITVAL,
                    Pay.payeeseq,
                    Pay.positionseq,
                    Pay.periodseq,
                    Pay.processingunitseq
                FROM
                    CS_PAYMENT Pay,
                    CS_APPLDEPOSITPAYMENTTRACE PayTrace,
                    CS_DEPOSITAPPLDEPOSITTRACE DepTrace,
                    CS_DEPOSIT Dep
                WHERE
                    Pay.paymentseq = PayTrace.paymentseq
                    AND Pay.processingunitseq = PayTrace.processingunitseq
                    AND PayTrace.applieddepositseq = DepTrace.applieddepositseq
                    AND PayTrace.processingunitseq = DepTrace.processingunitseq
                    AND DepTrace.Depositseq = Dep.Depositseq
                    AND DepTrace.processingunitseq = Dep.processingunitseq
                    AND pay.periodseq = :vperiodseq
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            )
        WHEN MATCHED THEN
            UPDATE SET rpt.DEPOSITVAL = qtr.DEPOSITVAL;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Update Null DEPOSITVAL to Monthly   
        /* ORIGSQL: UPDATE RPT_COMM_PAYOUT_MONTHLY SET DEPOSITVAL = 'Monthly' where DEPOSITVAL IS NU(...) */
        UPDATE EXT.RPT_COMM_PAYOUT_MONTHLY
            SET
            /* ORIGSQL: DEPOSITVAL = */
            DEPOSITVAL = 'Monthly' 
        FROM
            EXT.RPT_COMM_PAYOUT_MONTHLY
        WHERE
            DEPOSITVAL IS NULL;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --WAGETYPEDESCRIPTION    
        /* ORIGSQL: UPDATE RPT_COMM_PAYOUT_MONTHLY mon SET WAGETYPEDESCRIPTION = (SELECT stringvalue(...) */
        UPDATE EXT.RPT_COMM_PAYOUT_MONTHLY mon
            SET
            /* ORIGSQL: WAGETYPEDESCRIPTION = */
            WAGETYPEDESCRIPTION = (
                SELECT   /* ORIGSQL: (SELECT stringvalue FROM STEL_LOOKUP lkp WHERE name = 'LT_Wage Type_Reporting On(...) */
                    stringvalue
                FROM
                    STEL_LOOKUP lkp
                WHERE
                    name = 'LT_Wage Type_Reporting Only'
                    AND lkp.dim0 = mon.DEPOSITVAL
                    AND lkp.dim1 = 'Desc'
            )
        WHERE
            mon.DEPOSITVAL IS NOT NULL;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --WAGETYPECODE    
        /* ORIGSQL: UPDATE RPT_COMM_PAYOUT_MONTHLY mon SET WAGETYPECODE = (SELECT stringvalue FROM S(...) */
        UPDATE EXT.RPT_COMM_PAYOUT_MONTHLY mon
            SET
            /* ORIGSQL: WAGETYPECODE = */
            WAGETYPECODE = (
                SELECT   /* ORIGSQL: (SELECT stringvalue FROM STEL_LOOKUP lkp WHERE name = 'LT_Wage Type_Reporting On(...) */
                    stringvalue
                FROM
                    EXT.STEL_LOOKUP lkp
                WHERE
                    name = 'LT_Wage Type_Reporting Only'
                    AND lkp.dim0 = mon.DEPOSITVAL
                    AND lkp.dim1 = 'Code'
            )
        WHERE
            mon.DEPOSITVAL IS NOT NULL;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --------End Insert---------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Report table insert complete', NULL, (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Report table insert complete', NULL, :vSQLERRM);

        --------Gather stats-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Start DBMS_STATS', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Start DBMS_STATS', NULL, :vSQLERRM);

        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTable (vrpttablename) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName); Sanjay: Commenting out as Analyze are not required.

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END