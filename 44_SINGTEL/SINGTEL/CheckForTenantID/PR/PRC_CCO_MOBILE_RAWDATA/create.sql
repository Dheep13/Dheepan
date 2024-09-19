CREATE PROCEDURE EXT.PRC_CCO_MOBILE_RAWDATA
(
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_CCO_MOBILE_RAWDATA.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_CCO_MOBILE_RAWDATA.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_CCO_MOBILE_RAWDATA.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
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
    -- 12-Dec-2017      Tharanikumar  Initial release
    -------------------------------------------------------------------------------------------------------------------
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_CCO_MOBILE_RAWDATA');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_CCO_MOBILE_RAWDATA') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_CCO_MOBILE_RAWDATA';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_CCO_MOBILE_RAWDATA'; */
    DECLARE vTenantId VARCHAR(4) = SUBSTRING(SESSION_USER,1,4);  /* ORIGSQL: vTenantId VARCHAR2(4) := SUBSTR(USER, 1, 4) ; */
    DECLARE vExtUser VARCHAR(7) = IFNULL(:vTenantId,'') || 'EXT';  /* ORIGSQL: vExtUser VARCHAR2(7) := vTenantId || 'EXT'; */
    DECLARE vSubPartitionPrefix VARCHAR(30) = 'P_';  /* ORIGSQL: vSubPartitionPrefix VARCHAR2(30) := 'P_'; */
    DECLARE vSubPartitionName VARCHAR(30);  /* ORIGSQL: vSubPartitionName VARCHAR2(30); */
    DECLARE vPeriodRow ROW LIKE CS_PERIOD;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.CS_PERIOD' not found (for %ROWTYPE declaration) */
    DECLARE vCalendarRow ROW LIKE CS_CALENDAR;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.CS_CALENDAR' not found (for %ROWTYPE declaration) */
    DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found (for %ROWTYPE declaration) */
    DECLARE vCurYrStartDate TIMESTAMP;  /* ORIGSQL: vCurYrStartDate DATE; */
    DECLARE vCurYrEndDate TIMESTAMP;  /* ORIGSQL: vCurYrEndDate DATE; */
    DECLARE v_envdtl VARCHAR(1000);  /* ORIGSQL: v_envdtl VARCHAR2(1000); */
    DECLARE v_sql NCLOB;  /* ORIGSQL: v_sql long; */
    DECLARE vcredittypeid_Mobile NVARCHAR(50);
    DECLARE cEndofTime date;

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
        -- SELECT SESSION_CONTEXT('DBMTK_GLOBVAR_DBMTK_USER_NAME_PKG_REPORTING_EXTRACT_R2_VCREDITTYPEID_MOBILE') INTO PKG_REPORTING_EXTRACT_R2__vcredittypeid_Mobile FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('vcredittypeid_Mobile') INTO vcredittypeid_Mobile FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('cEndofTime') INTO cEndofTime FROM SYS.DUMMY ;
        
        /* end of package/session variables */

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
        --     );--/*Deepan : Partition Not required*/

        --------Find subpartition name------------------------------------------------------------------------------------

        -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */--/*Deepan : Partition Not required*/

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--/*Deepan : Partition Not required*/

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);/*Deepan : Partition Not required*/

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
        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_CCO_MOBILE_RAWDATA' not found */

        /* ORIGSQL: INSERT INTO EXT.RPT_CCO_MOBILE_RAWDATA (tenantid, positionseq, payeeseq, pro(...) */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_SALESTRANSACTION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_CREDIT' not found */
        INSERT INTO EXT.RPT_CCO_MOBILE_RAWDATA--/*Deepan: Table structure is not correct, need to re create this */
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
                empfirstname,
                emplastname,
                titlename,
                periodstartdate,
                periodenddate,
                loaddttm,
                ORDERNO,
                ORDERTYPE,
                SVCEFFECTIVEDATE,
                ORDERCLOSEDATE,
                ORDERCREATIONDATE,
                SVCNO,
                COMPID,
                ACTIONTYPE,
                COMPINSID,
                CUSTREQDATE,
                AITYEP,
                BIZPRODTYPE,
                BIZPRODGROUP,
                REJMSG,
                VENDORCODE,
                DEALERCODE,
                SALESMANCODE,
                NAME,
                GEID,
                REGION,
                TRANSACTIONTYPE,
                COMPONENTSTATUS
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
                '55' AS reportcode,
                '01' AS sectionid,
                'DETAIL' AS sectionname,
                '01' AS sortorder,
                pad.firstname AS empfirstname,
                pad.lastname AS emplastname,
                pad.reporttitle AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                st.alternateordernumber AS orderno,
                st.GENERICATTRIBUTE10 AS ordertype,
                st.GENERICDATE5 AS svceffectivedate,
                st.COMPENSATIONDATE AS orderclosedate,
                st.ACCOUNTINGDATE AS ordercreatedate,
                st.CONTACT AS svcno,
                st.GENERICATTRIBUTE24 AS compid,
                st.GENERICATTRIBUTE5 AS actiontype,
                st.postalcode AS compinsid,
                st.GENERICDATE1 AS custreqdate,
                MAP(st.postalcode, 'VAS Indicator', 'MTC', 'MTVAS') AS aitype,  /* ORIGSQL: decode(st.postalcode,'VAS Indicator','MTC','MTVAS') */
                (
                    SELECT   /* ORIGSQL: (SELECT MAX(GENERICATTRIBUTE13) FROM STEL_CLASSIFIER WHERE CATEGORYTREENAME ='Si(...) */
                        MAX(GENERICATTRIBUTE13)
                    FROM
                        STEL_CLASSIFIER
                    WHERE
                        CATEGORYTREENAME = 'Singtel-Internal-Products'
                        /* --CATEGORYNAME = 'PRODUCTS' */
                        /* -- and classifierid = st.productid */
                        AND classifierid = st.GENERICATTRIBUTE24
                        AND effectiveenddate >= :vPeriodRow.startdate
                        AND effectivestartdate <= :vPeriodRow.startdate
                ) AS bizprodtype,
                /* -- bizprodgroup also we can take from same query  */
                cr.GENERICATTRIBUTE1 AS bizprodgroup,
                NULL AS rejmsg,/* -- Keep Blank always */  st.GENERICATTRIBUTE4 AS vendorcode,
                st.GENERICATTRIBUTE3 AS dealercode,
                st.GENERICATTRIBUTE2 AS salesmancode,
                pad.FULLNAME AS name,
                pad.PARTICIPANTID AS geid,
                pad.POSITIONGA1 AS region,
                st.GENERICATTRIBUTE5 AS transactiontype,
                st.GENERICATTRIBUTE22 AS componentstatus
            FROM
                rpt_base_padimension pad,
                rpt_base_salestransaction st,
                rpt_base_credit cr
            WHERE
                st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                AND st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                --and st.businessunitmap = pad.businessunitmap
                --and st.trnsassignpositionname = pad.positionname
                AND st.processingunitseq = :vprocessingunitseq
                AND cr.periodseq = pad.periodseq
                AND cr.processingunitseq = pad.processingunitseq
                AND cr.payeeseq = pad.payeeseq
                AND cr.positionseq = pad.positionseq
                AND pad.reportgroup IN ('CCO MOBILE TM','CCO MOBILE TL')
                AND cr.credittypeid = :vcredittypeid_Mobile;  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_Mobile */

        --and st.eventtypeid = pkg_reporting_extract_r2.veventtypeid_ccomobile;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Insert completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert completed', NULL, :v_sql);

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