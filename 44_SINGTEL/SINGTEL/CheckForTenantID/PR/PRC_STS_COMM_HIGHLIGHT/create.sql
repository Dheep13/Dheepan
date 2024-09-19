CREATE PROCEDURE EXT.PRC_STS_COMM_HIGHLIGHT
(
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_STS_COMM_HIGHLIGHT.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_STS_COMM_HIGHLIGHT.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_STS_COMM_HIGHLIGHT.vcalendarseq' */
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
    -- 01-Dec-2017      NANDINI VARADHARAJAN  Initial release
    -------------------------------------------------------------------------------------------------------------------
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_STS_COMM_HIGHLIGHT');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_STS_COMM_HIGHLIGHT') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_STS_COMM_HIGHLIGHT_FYEAR';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_STS_COMM_HIGHLIGHT_FYEAR'; */
    DECLARE vTenantId VARCHAR(4) = SUBSTRING(SESSION_USER,1,4);  /* ORIGSQL: vTenantId VARCHAR2(4) := SUBSTR(USER, 1, 4) ; */
    DECLARE vExtUser VARCHAR(7) = IFNULL(:vTenantId,'') || 'EXT';  /* ORIGSQL: vExtUser VARCHAR2(7) := vTenantId || 'EXT'; */
    DECLARE vSubPartitionPrefix VARCHAR(30) = 'P_';  /* ORIGSQL: vSubPartitionPrefix VARCHAR2(30) := 'P_'; */
    DECLARE vSubPartitionName VARCHAR(30);  /* ORIGSQL: vSubPartitionName VARCHAR2(30); */
    DECLARE vPeriodRow ROW LIKE CS_PERIOD;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PERIOD' not found (for %ROWTYPE declaration) */
    DECLARE vCalendarRow  ROW LIKE CS_CALENDAR;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_CALENDAR' not found (for %ROWTYPE declaration) */
    DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PROCESSINGUNIT' not found (for %ROWTYPE declaration) */
    DECLARE vCurYrStartDate TIMESTAMP;  /* ORIGSQL: vCurYrStartDate DATE; */
    DECLARE vCurYrEndDate TIMESTAMP;  /* ORIGSQL: vCurYrEndDate DATE; */
    DECLARE v_envdtl VARCHAR(1000);  /* ORIGSQL: v_envdtl VARCHAR2(1000); */
    DECLARE v_sql NCLOB;  /* ORIGSQL: v_sql long; */

    DECLARE g_ytd_startdate TIMESTAMP;  /* ORIGSQL: g_ytd_startdate DATE; */
    DECLARE g_ytd_enddate TIMESTAMP;  /* ORIGSQL: g_ytd_enddate DATE; */
    DECLARE v_finyear VARCHAR(50);  /* ORIGSQL: v_finyear VARCHAR2(50); */
    DECLARE vReportGroup VARCHAR(30) = 'STS';  /* ORIGSQL: vReportGroup VARCHAR2(30) := 'STS'; */
    DECLARE cEndofTime date;
    DECLARE vprevperiodseq BIGINT;/* NOT CONVERTED! */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            vSQLERRM = SUBSTRING(::SQL_ERROR_MESSAGE,1,3900);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 3900) */

            /* ORIGSQL: prc_logevent (:vperiodrow.name, vProcName, 'ERROR', NULL, vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'ERROR', NULL, :vSQLERRM);

            /* ORIGSQL: raise_application_error(-20911,'Error raised: '||vprocname||' Failed: '|| DBMS_U(...) */
            -- sapdbmtk: mapped error code -20911 => 10911: (ABS(-20911)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10911 SET MESSAGE_TEXT = 'Error raised: '||IFNULL(:vProcName,'')||' Failed: '
            || ' - '||IFNULL(:vSQLERRM,'');  /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
        END;

        /* initialize session variables, if not yet done */
        CALL EXT.init_session_global();

        -- vperiodseq cs_period.periodseq%TYPE;
        

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
        --     );

        --------Find subpartition name------------------------------------------------------------------------------------

        -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vprocessingunitseq, vperiodse(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);

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

        g_ytd_startdate = to_date('01-04-'||IFNULL(TO_VARCHAR(EXTRACT(YEAR FROM ADD_MONTHS(:vPeriodRow.startdate, -3))),''),'dd-mm-yyyy');  /* ORIGSQL: to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (:vPeriodRow.startdate, -3)),'dd-(...) */

        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'Capture Start date',NULL,g_ytd_startdat(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Capture Start date', NULL, :g_ytd_startdate);

        --sudhir

        g_ytd_enddate =to_date('01-04-'||IFNULL(TO_VARCHAR(EXTRACT(YEAR FROM ADD_MONTHS(:vPeriodRow.startdate, 9))),''),'dd-mm-yyyy');  /* ORIGSQL: to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (:vPeriodRow.startdate, 9)),'dd-m(...) */

        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'Capture End date',NULL,g_ytd_enddate) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Capture End date', NULL, :g_ytd_enddate);

        --sudhir  

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIODTYPE' not found */

        SELECT
            prev.PERIODSEQ
        INTO
            vprevperiodseq
        FROM
            cs_period prev,
            cs_periodtype pt
        WHERE
            prev.startdate 
            =
            (
                SELECT   /* ORIGSQL: (select ADD_MONTHS(pe.startdate,-1) from cs_period pe where periodseq = vperiods(...) */
                    ADD_MONTHS(pe.startdate,-1)
                FROM
                    cs_period pe
                WHERE
                    periodseq = :vperiodseq
                    AND calendarseq = :vcalendarseq
            )
            AND prev.calendarseq = :vcalendarseq
            AND pt.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
            AND prev.periodtypeseq = pt.periodtypeseq
            AND pt.name = 'month';

        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'Prevperiodseq',NULL,vprevperiodseq) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Prevperiodseq', NULL, :vprevperiodseq);

        --sudhir

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
                    AND PERIODNAME LIKE '%'||IFNULL(substring(:vperiodrow.name,-4),'') ||'%'   /* ORIGSQL: substr(:vperiodrow.name,-4) */
            ) AS dbmtk_corrname_9354;

        /* ORIGSQL: DELETE from RPT_STS_COMM_HIGHLIGHT_FYEAR where PERIODSEQ not in (vperiodseq) and(...) */
        DELETE
        FROM
            RPT_STS_COMM_HIGHLIGHT_FYEAR
        WHERE
            PERIODSEQ NOT IN (:vperiodseq)
            AND SECTIONNAME IN ('Individual gt threshold','Shop Summary');
        --------Begin Insert-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);
        ---------------------INDIVDUAL GREATER THAN THRESHHOLD SECTION-----------------------
        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'Begin INDIVDUAL GREATER THAN THRESHHOLD(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Begin INDIVDUAL GREATER THAN THRESHHOLD SECTION', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_ROADSHOWACTUALS_1' not found */
        /* ORIGSQL: INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR (TENANTID, PROCESSINGUNITSEQ,PERIODSEQ,(...) */
        INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR
            (
                TENANTID, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME,
                REPORTCODE, POSITIONSEQ, PAYEESEQ, SHOPPAYEESEQ, SECTIONID, SECTIONNAME,
                SORTORDER, LOADDTTM, GEID, NAME, TEAMNAME, SMCODE,
                SHOPID, SHOPCODE, ROADSHOWS, CURRRANK, CURRINDOVERALL, CURRTEAMOVERALL,
                CURRCOMMPAY, CURRSHOPNAME, CURRMONTHNAME, FINYEAR
            )
            SELECT   /* ORIGSQL: select vTenantId TENANTID, ps.PROCESSINGUNITSEQ, ps.PERIODSEQ, ps.PERIODNAME, ps(...) */
                :vTenantId AS TENANTID,
                ps.PROCESSINGUNITSEQ,
                ps.PERIODSEQ,
                ps.PERIODNAME,
                ps.PROCESSINGUNITNAME,
                ps.CALENDARNAME,
                62 AS REPORTCODE,
                ps.POSITIONSEQ,
                ps.PAYEESEQ,
                ps.SHOPPAYEESEQ,
                1 AS SECTIONID,
                'Individual gt threshold' AS SECTIONNAME,
                1 AS SORTORDER,
                CURRENT_TIMESTAMP AS LOADDTTM,  /* ORIGSQL: sysdate */
                ps.GEID AS GEID,
                ps.STAFFNAME AS NAME,
                ps.SHOPNAME AS TEAMNAME,
                ps.SALESMANCODE AS SMCODE,
                NULL AS SHOPID,
                /* --pad.positionga1 SHOPID, */
                ps.SHOPNAME AS TEAMNAME,
                rs.roadshowname AS ROADSHOWS,
                ROW_NUMBER() OVER (ORDER BY IFNULL(ps.INDOVERALL,0) DESC) AS CURRRANK,  /* ORIGSQL: NVL(ps.INDOVERALL,0) */
                ps.INDOVERALL AS CURRINDOVERALL,
                ps.TEAMOVERALL AS CURRTEAMOVERALL,
                ps.TOTALCOMMPAYOUT AS CURRCOMMPAY,
                ps.SHOPNAME AS CURRSHOPNAME,
                IFNULL(TO_VARCHAR(:vPeriodRow.startdate,'MON'),'') ||'-'|| IFNULL(TO_VARCHAR(:vPeriodRow.startdate,'yyyy'),'') AS CURRMONTHNAME,  /* ORIGSQL: to_char(:vPeriodRow.startdate, 'yyyy') */
                                                                                                                                                /* ORIGSQL: to_char(:vPeriodRow.startdate, 'MON') */
                :v_finyear
            FROM
                RPT_STSRCSDS_PAYEE_SUMMARY PS
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select rs1.payeeid, listagg(rs1.roadshowname, ',') WITHIN GROUP (ORDER BY null)(...) */
                        rs1.payeeid,
                        STRING_AGG(rs1.roadshowname, ',' ORDER BY rs1.payeeid) AS roadshowname  /* ORIGSQL: listagg(rs1.roadshowname, ',') WITHIN GROUP (ORDER BY null) */
                    FROM

                        (
                            SELECT   /* ORIGSQL: (SELECT DISTINCT RSA.PAYEEID, SC.GENERICATTRIBUTE5 roadshowname FROM STEL_ROADSH(...) */
                                DISTINCT
                                RSA.PAYEEID,
                                SC.GENERICATTRIBUTE5 AS roadshowname
                            FROM
                                STEL_ROADSHOWACTUALS_1 RSA,
                                STEL_CLASSIFIER SC
                            WHERE
                                RSA.ROADSHOWCODE = SC.CLASSIFIERID
                        ) AS rs1
                    GROUP BY
                        rs1.payeeid
                ) AS RS
                ON RS.PAYEEID = PS.GEID
            WHERE
                PS.PROCESSINGUNITSEQ = :vprocessingunitseq
                AND PS.periodseq = :vperiodseq
                AND PS.REPORTGROUP = :vReportGroup;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'END INDIVDUAL GREATER THAN THRESHHOLD S(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'END INDIVDUAL GREATER THAN THRESHHOLD SECTION', NULL, :vSQLERRM);

        ---------------------Merge for previous month data-----------------------  
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STS_COMM_HIGHLIGHT_FYEAR rpt using (SELECT PS1.positionseq, PS1.p(...) */
        MERGE INTO RPT_STS_COMM_HIGHLIGHT_FYEAR AS rpt  
            USING
            (
                SELECT   /* ORIGSQL: (select PS1.positionseq, PS1.payeeseq, PS1.processingunitseq, PS1.periodseq, ROW(...) */
                    PS1.positionseq,
                    PS1.payeeseq,
                    PS1.processingunitseq,
                    PS1.periodseq,
                    ROW_NUMBER() OVER (ORDER BY IFNULL(PS1.INDOVERALL,0) DESC) AS PREVRANK,  /* ORIGSQL: NVL(PS1.INDOVERALL,0) */
                    PS1.INDOVERALL AS PREVINDOVERALL,
                    PS1.TEAMOVERALL AS PREVTEAMOVERALL,
                    PS1.TOTALCOMMPAYOUT AS PREVCOMMPAY,
                    pe.startdate
                FROM
                    RPT_STSRCSDS_PAYEE_SUMMARY PS1,
                    cs_period pe
                WHERE
                    PS1.processingunitseq = :vprocessingunitseq
                    AND PS1.periodseq = pe.periodseq
                    AND pe.calendarseq = :vcalendarseq
                    AND PS1.periodseq = :vprevperiodseq
                    AND PS1.payeeseq
                    /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_STS_COMM_HIGHLIGHT_FYEAR' not found */
                    IN
                    (
                        SELECT   /* ORIGSQL: (select distinct payeeseq from RPT_STS_COMM_HIGHLIGHT_FYEAR where periodseq = vp(...) */
                            DISTINCT
                            payeeseq
                        FROM
                            RPT_STS_COMM_HIGHLIGHT_FYEAR
                        WHERE
                            periodseq = :vperiodseq
                    )
                ) AS qtr
                ON (rpt.processingunitseq = qtr.processingunitseq
                    AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq)
        WHEN MATCHED THEN
            UPDATE SET rpt.PREVINDOVERALL = qtr.PREVINDOVERALL,
                rpt.PREVTEAMOVERALL = qtr.PREVTEAMOVERALL,
                rpt.PREVCOMMPAY = qtr.PREVCOMMPAY,
                rpt.PREVRANK = qtr.PREVRANK,
                rpt.PREVMONTHNAME =IFNULL(TO_VARCHAR(qtr.startdate,'MON'),'') ||'-'|| IFNULL(TO_VARCHAR(qtr.startdate,'yyyy'),'');  /* ORIGSQL: to_char(qtr.startdate, 'yyyy') */
                                                                                                                                    /* ORIGSQL: to_char(qtr.startdate, 'MON') */

        /* ORIGSQL: COMMIT; */
        COMMIT;

        ---------------------SHOP SUMMARY SECTION-----------------------

        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'Begin SHOP SUMMARY SECTION',NULL,vsqler(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Begin SHOP SUMMARY SECTION', NULL, :vSQLERRM); 

        /* ORIGSQL: INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR (TENANTID,PROCESSINGUNITSEQ,PERIODSEQ,P(...) */
        INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR
            (
                TENANTID, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME,
                REPORTCODE, SHOPPAYEESEQ, SECTIONID, SECTIONNAME, SORTORDER, LOADDTTM,
                SHOPID,/* --SHOPCODE, */ CURRSHOPNAME, CURRTEAMOVERALL, CURRCOMMPAY, CURRMONTHNAME, FINYEAR
            )
            SELECT   /* ORIGSQL: select ps.TENANTID, ps.PROCESSINGUNITSEQ, ps.PERIODSEQ, ps.PERIODNAME, ps.PROCES(...) */
                ps.TENANTID,
                ps.PROCESSINGUNITSEQ,
                ps.PERIODSEQ,
                ps.PERIODNAME,
                ps.PROCESSINGUNITNAME,
                ps.CALENDARNAME,
                62 AS REPORTCODE,
                ps.SHOPPAYEESEQ,
                2 AS SECTIONID,
                'Shop Summary' AS SECTIONNAME,
                2 AS SORTORDER,
                CURRENT_TIMESTAMP AS LOADDTTM,  /* ORIGSQL: sysdate */
                NULL AS shopid,
                /* --pa.participantid SHOPID, */
                /* -- null SHOPCODE, */
                ps.SHOPNAME,
                MAX(ps.TEAMOVERALL) AS CURRTEAMOVERALL,
                SUM(ps.TOTALCOMMPAYOUT) AS CURRCOMMPAY,
                IFNULL(TO_VARCHAR(:vPeriodRow.startdate,'MON'),'') ||'-'|| IFNULL(TO_VARCHAR(:vPeriodRow.startdate,'yyyy'),'') AS CURRMONTHNAME,  /* ORIGSQL: to_char(:vPeriodRow.startdate, 'yyyy') */
                                                                                                                                                /* ORIGSQL: to_char(:vPeriodRow.startdate, 'MON') */
                :v_finyear
            FROM
                RPT_STSRCSDS_PAYEE_SUMMARY ps
                --,rpt_base_padimension pa
            WHERE
                PS.PROCESSINGUNITSEQ = :vprocessingunitseq
                AND PS.periodseq = :vperiodseq
                --and ps.SHOPPAYEESEQ=pa.payeeseq
                --and ps.periodseq =pa.periodseq
                AND PS.REPORTGROUP = :vReportGroup
                AND PS.TITLENAME IN ('STS - Store Mgr')
            GROUP BY
                ps.TENANTID,
                ps.PROCESSINGUNITSEQ,
                ps.PERIODSEQ,
                ps.PERIODNAME,
                ps.PROCESSINGUNITNAME,
                ps.CALENDARNAME,
                ps.REPORTCODE,
                ps.SHOPPAYEESEQ,
                --pa.participantid,
                ps.SHOPNAME;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'END SHOP SUMMARY SECTION',NULL,vsqlerrm(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'END SHOP SUMMARY SECTION', NULL, :vSQLERRM);

        ---------------------SUMMARY SECTION Merge for previous month data-----------------------
        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'BEGIN SHOP SUMMARY SECTION MERGE',NULL,(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'BEGIN SHOP SUMMARY SECTION MERGE', NULL, :vSQLERRM); 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_STS_COMM_HIGHLIGHT_FYEAR rpt using (SELECT PS1.SHOPNAME, PS1.proc(...) */
        MERGE INTO RPT_STS_COMM_HIGHLIGHT_FYEAR AS rpt
            /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_STSRCSDS_PAYEE_SUMMARY' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select PS1.SHOPNAME, PS1.processingunitseq, PS1.periodseq, pe.startdate, MAX(ps(...) */
                    PS1.SHOPNAME,
                    PS1.processingunitseq,
                    PS1.periodseq,
                    pe.startdate,
                    MAX(ps1.TEAMOVERALL) AS PREVTEAMOVERALL,
                    SUM(ps1.TOTALCOMMPAYOUT) AS PREVCOMMPAY
                FROM
                    RPT_STSRCSDS_PAYEE_SUMMARY PS1,
                    cs_period pe
                WHERE
                    PS1.processingunitseq = :vprocessingunitseq
                    AND PS1.periodseq = pe.periodseq
                    AND pe.calendarseq = :vcalendarseq
                    AND PS1.periodseq = :vprevperiodseq
                    AND PS1.TITLENAME IN ('STS - Store Mgr')
                GROUP BY
                    PS1.SHOPNAME,
                    PS1.processingunitseq,
                    PS1.periodseq, pe.startdate
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.SECTIONNAME = 'Shop Summary'
            AND rpt.CURRSHOPNAME = qtr.SHOPNAME)
        WHEN MATCHED THEN
            UPDATE SET
                rpt.PREVTEAMOVERALL = qtr.PREVTEAMOVERALL,
                rpt.PREVCOMMPAY = qtr.PREVCOMMPAY,
                rpt.PREVMONTHNAME =IFNULL(TO_VARCHAR(qtr.startdate,'MON'),'') ||'-'|| IFNULL(TO_VARCHAR(qtr.startdate,'yyyy'),'');  /* ORIGSQL: to_char(qtr.startdate, 'yyyy') */
                                                                                                                                    /* ORIGSQL: to_char(qtr.startdate, 'MON') */
        --rpt.SECTIONNAME='Shop Summary';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'END SHOP SUMMARY SECTION MERGE',NULL,vs(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'END SHOP SUMMARY SECTION MERGE', NULL, :vSQLERRM);

        ---------------------FY section begins-----------------------
        /* ORIGSQL: prc_logevent (:vperiodrow.name,vProcName,'BEGIN FYEAR SUMMARY SECTION MERGE',NULL(...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'BEGIN FYEAR SUMMARY SECTION MERGE', NULL, :vSQLERRM);
        /*    Begin
            select count(*)
              into v_totalstaffs
            from rpt_base_padimension pad
              where pad.processingunitseq = vprocessingunitseq
         AND pad.periodseq =
         and pad.reportgroup = 'STS';
          exception when others then
            v_totalstaffs := 0;
          end;
        
        */
        ---------------------TOTAL FY SECTION data-----------------------      

        /* ORIGSQL: INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR (TENANTID, PROCESSINGUNITSEQ,PERIODSEQ,(...) */
        INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR
            (
                TENANTID, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME,
                REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER, LOADDTTM, SHOPID,
                CURRMONTHNAME, CURRSHOPNAME, GTTHRESHNUM, FINYEAR
            )
            SELECT   /* ORIGSQL: select vTenantId TENANTID, VPROCESSINGUNITSEQ,vperiodseq,:vperiodrow.name,vProces(...) */
                :vTenantId AS TENANTID,
                :vprocessingunitseq,
                :vperiodseq,
                :vperiodrow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                60 AS REPORTCODE,
                3 AS SECTIONID,
                'Fysection' AS SECTIONNAME,
                3 AS SORTORDER,
                CURRENT_TIMESTAMP AS LOADDTTM,  /* ORIGSQL: sysdate */
                3 AS SHOPID,
                IFNULL(TO_VARCHAR(summ.startdate,'MON'),'') ||'-'|| IFNULL(TO_VARCHAR(summ.startdate,'yyyy'),'') AS CURRMONTHNAME,  /* ORIGSQL: to_char(summ.startdate, 'yyyy') */
                                                                                                                                    /* ORIGSQL: to_char(summ.startdate, 'MON') */
                'Total no. of Hello Staffs' AS CURRSHOPNAME,
                cnt1 AS GTTHRESHNUM,
                :v_finyear
            FROM
                (
                    SELECT   /* ORIGSQL: (select ps.periodseq,ps.periodname,per.startdate,per.enddate,COUNT(1) cnt1 from (...) */
                        ps.periodseq,
                        ps.periodname,
                        per.startdate,
                        per.enddate,
                        COUNT(1) AS cnt1
                    FROM
                        EXT.RPT_STS_COMM_HIGHLIGHT_FYEAR PS,
                        cs_period per
                    WHERE
                        sectionid = 1
                        AND ps.periodseq IN
                        (
                            SELECT   /* ORIGSQL: (SELECT distinct pe.periodseq FROM cs_period pe, cs_periodtype pt WHERE pe.start(...) */
                                DISTINCT
                                pe.periodseq
                            FROM
                                cs_period pe,
                                cs_periodtype pt
                            WHERE
                                pe.startdate BETWEEN :g_ytd_startdate AND TO_DATE(ADD_SECONDS(:g_ytd_enddate,(86400*-1))) --  WHERE pe.startdate
                                -- BETWEEN to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate, -3)),'dd-mm-yyyy')
                                -- AND  to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate,  9)),'dd-mm-yyyy') -1
                                /* ORIGSQL: g_ytd_enddate -1 */
                                AND pe.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                                AND pt.removedate = to_date('1-JAN-2200','DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200','DD-MON-YYYY') */
                                AND pt.name = 'month'
                                AND pe.periodtypeseq = pt.periodtypeseq
                                AND pe.calendarseq = :vcalendarseq
                        ) -- and pe.calendarseq = 2251799813685251)
                        AND ps.periodseq = per.periodseq
                        AND per.calendarseq = :vcalendarseq
                        --and per.calendarseq = 2251799813685251
                        AND per.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                    GROUP BY
                        ps.periodseq,ps.periodname,per.startdate,per.enddate
                ) AS summ;

        /* ORIGSQL: commit; */
        COMMIT;

        ---------------------SUMMARY SECTION Team Achievement >150% Threshold-----------------------         

        /* ORIGSQL: INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR (TENANTID, PROCESSINGUNITSEQ,PERIODSEQ,(...) */
        INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR
            (
                TENANTID, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME,
                REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER, LOADDTTM, CURRMONTHNAME,
                CURRSHOPNAME, GTTHRESHNUM, GTTHRESHPERCENT, FINYEAR
            )
            SELECT   /* ORIGSQL: select vTenantId TENANTID, VPROCESSINGUNITSEQ,vperiodseq,:vperiodrow.name,vProces(...) */
                :vTenantId AS TENANTID,
                :vprocessingunitseq,
                :vperiodseq,
                :vperiodrow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                60 AS REPORTCODE,
                3 AS SECTIONID,
                'Fysection' AS SECTIONNAME,
                3 AS SORTORDER,
                CURRENT_TIMESTAMP AS LOADDTTM,  /* ORIGSQL: sysdate */
                IFNULL(TO_VARCHAR(summ.startdate,'MON'),'') ||'-'|| IFNULL(TO_VARCHAR(summ.startdate,'yyyy'),'') AS CURRMONTHNAME,  /* ORIGSQL: to_char(summ.startdate, 'yyyy') */
                                                                                                                                    /* ORIGSQL: to_char(summ.startdate, 'MON') */
                'Team Achievement >150% Threshold' AS CURRSHOPNAME,
                IFNULL(summ.cnt1,0) AS GTTHRESHNUM,  /* ORIGSQL: nvl(summ.cnt1,0) */
                IFNULL((summ.cnt1/tot.v_totalstaffs),0) AS GTTHRESHPERCENT,  /* ORIGSQL: nvl((summ.cnt1/tot.v_totalstaffs),0) */
                :v_finyear
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                (
                    SELECT   /* ORIGSQL: (select ps.periodseq,ps.periodname,per.startdate,per.enddate,COUNT(1) cnt1 from (...) */
                        ps.periodseq,
                        ps.periodname,
                        per.startdate,
                        per.enddate,
                        COUNT(1) AS cnt1
                    FROM
                        RPT_STS_COMM_HIGHLIGHT_FYEAR PS,
                        cs_period per
                    WHERE
                        (ps.currteamoverall) >= 150
                        AND sectionid = 1
                        AND ps.periodseq IN
                        (
                            SELECT   /* ORIGSQL: (SELECT distinct pe.periodseq FROM cs_period pe, cs_periodtype pt WHERE pe.start(...) */
                                DISTINCT
                                pe.periodseq
                            FROM
                                cs_period pe,
                                cs_periodtype pt
                            WHERE
                                pe.startdate BETWEEN :g_ytd_startdate AND TO_DATE(ADD_SECONDS(:g_ytd_enddate,(86400*-1))) --  WHERE pe.startdate
                                -- BETWEEN to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate, -3)),'dd-mm-yyyy')
                                -- AND  to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate,  9)),'dd-mm-yyyy') -1
                                /* ORIGSQL: g_ytd_enddate -1 */
                                AND pe.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                                AND pt.removedate = to_date('1-JAN-2200','DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200','DD-MON-YYYY') */
                                AND pt.name = 'month'
                                AND pe.periodtypeseq = pt.periodtypeseq
                                AND pe.calendarseq = :vcalendarseq
                        ) -- and pe.calendarseq = 2251799813685251)
                        AND ps.periodseq = per.periodseq
                        AND per.calendarseq = :vcalendarseq
                        --and per.calendarseq = 2251799813685251
                        AND per.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                    GROUP BY
                        ps.periodseq,ps.periodname,per.startdate,per.enddate
                ) AS summ
            RIGHT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select ps.periodseq,ps.periodname,GTTHRESHNUM v_totalstaffs from RPT_STS_COMM_H(...) */
                        ps.periodseq,
                        ps.periodname,
                        GTTHRESHNUM AS v_totalstaffs
                    FROM
                        RPT_STS_COMM_HIGHLIGHT_FYEAR PS
                    WHERE
                        sectionid = 3
                        AND SHOPID = 3
                        AND ps.periodseq IN
                        (
                            SELECT   /* ORIGSQL: (SELECT distinct pe.periodseq FROM cs_period pe, cs_periodtype pt WHERE pe.start(...) */
                                DISTINCT
                                pe.periodseq
                            FROM
                                cs_period pe,
                                cs_periodtype pt
                            WHERE
                                pe.startdate BETWEEN :g_ytd_startdate AND TO_DATE(ADD_SECONDS(:g_ytd_enddate,(86400*-1))) --  WHERE pe.startdate
                                -- BETWEEN to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate, -3)),'dd-mm-yyyy')
                                -- AND  to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate,  9)),'dd-mm-yyyy') -1
                                /* ORIGSQL: g_ytd_enddate -1 */
                                AND pe.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                                AND pt.removedate = to_date('1-JAN-2200','DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200','DD-MON-YYYY') */
                                AND pt.name = 'month'
                                AND pe.periodtypeseq = pt.periodtypeseq
                                AND pe.calendarseq = :vcalendarseq
                        ) -- and pe.calendarseq = 2251799813685251)
                    ) AS tot
                    ON summ.periodseq = tot.periodseq;  /* ORIGSQL: summ.periodseq(+)=tot.periodseq */

        /* ORIGSQL: commit; */
        COMMIT;

        ---------------------Indv. Achievement >200% Threshold-----------------------         

        /* ORIGSQL: INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR (TENANTID, PROCESSINGUNITSEQ,PERIODSEQ,(...) */
        INSERT INTO RPT_STS_COMM_HIGHLIGHT_FYEAR
            (
                TENANTID, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME,
                REPORTCODE, SECTIONID, SECTIONNAME, SORTORDER, LOADDTTM, CURRMONTHNAME,
                CURRSHOPNAME, GTTHRESHNUM, GTTHRESHPERCENT, FINYEAR
            )
            SELECT   /* ORIGSQL: select vTenantId TENANTID, VPROCESSINGUNITSEQ,vperiodseq,:vperiodrow.name,vProces(...) */
                :vTenantId AS TENANTID,
                :vprocessingunitseq,
                :vperiodseq,
                :vperiodrow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                60 AS REPORTCODE,
                3 AS SECTIONID,
                'Fysection' AS SECTIONNAME,
                3 AS SORTORDER,
                CURRENT_TIMESTAMP AS LOADDTTM,  /* ORIGSQL: sysdate */
                IFNULL(TO_VARCHAR(summ.startdate,'MON'),'') ||'-'|| IFNULL(TO_VARCHAR(summ.startdate,'yyyy'),'') AS CURRMONTHNAME,  /* ORIGSQL: to_char(summ.startdate, 'yyyy') */
                                                                                                                                    /* ORIGSQL: to_char(summ.startdate, 'MON') */
                'Indv. Achievement >200% Threshold ' AS CURRSHOPNAME,
                IFNULL(summ.cnt1,0) AS GTTHRESHNUM,  /* ORIGSQL: nvl(summ.cnt1,0) */
                IFNULL((summ.cnt1/tot.v_totalstaffs),0) AS GTTHRESHPERCENT,  /* ORIGSQL: nvl((summ.cnt1/tot.v_totalstaffs),0) */
                :v_finyear
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                (
                    SELECT   /* ORIGSQL: (select ps.periodseq,ps.periodname,per.startdate,per.enddate,COUNT(1) cnt1 from (...) */
                        ps.periodseq,
                        ps.periodname,
                        per.startdate,
                        per.enddate,
                        COUNT(1) AS cnt1
                    FROM
                        RPT_STS_COMM_HIGHLIGHT_FYEAR PS,
                        cs_period per
                    WHERE
                        (ps.currindoverall) >= 200
                        AND sectionid = 1
                        AND ps.periodseq IN
                        (
                            SELECT   /* ORIGSQL: (SELECT distinct pe.periodseq FROM cs_period pe, cs_periodtype pt WHERE pe.start(...) */
                                DISTINCT
                                pe.periodseq
                            FROM
                                cs_period pe,
                                cs_periodtype pt
                            WHERE
                                pe.startdate BETWEEN :g_ytd_startdate AND TO_DATE(ADD_SECONDS(:g_ytd_enddate,(86400*-1))) --  WHERE pe.startdate
                                -- BETWEEN to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate, -3)),'dd-mm-yyyy')
                                -- AND  to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate,  9)),'dd-mm-yyyy') -1
                                /* ORIGSQL: g_ytd_enddate -1 */
                                AND pe.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                                AND pt.removedate = to_date('1-JAN-2200','DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200','DD-MON-YYYY') */
                                AND pt.name = 'month'
                                AND pe.periodtypeseq = pt.periodtypeseq
                                AND pe.calendarseq = :vcalendarseq
                        ) -- and pe.calendarseq = 2251799813685251)
                        AND ps.periodseq = per.periodseq
                        AND per.calendarseq = :vcalendarseq
                        --and per.calendarseq = 2251799813685251
                        AND per.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                    GROUP BY
                        ps.periodseq,ps.periodname,per.startdate,per.enddate
                ) AS summ
            RIGHT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select ps.periodseq,ps.periodname,GTTHRESHNUM v_totalstaffs from RPT_STS_COMM_H(...) */
                        ps.periodseq,
                        ps.periodname,
                        GTTHRESHNUM AS v_totalstaffs
                    FROM
                        RPT_STS_COMM_HIGHLIGHT_FYEAR PS
                    WHERE
                        sectionid = 3
                        AND SHOPID = 3
                        AND ps.periodseq IN
                        (
                            SELECT   /* ORIGSQL: (SELECT distinct pe.periodseq FROM cs_period pe, cs_periodtype pt WHERE pe.start(...) */
                                DISTINCT
                                pe.periodseq
                            FROM
                                cs_period pe,
                                cs_periodtype pt
                            WHERE
                                pe.startdate BETWEEN :g_ytd_startdate AND TO_DATE(ADD_SECONDS(:g_ytd_enddate,(86400*-1))) --  WHERE pe.startdate
                                -- BETWEEN to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate, -3)),'dd-mm-yyyy')
                                -- AND  to_date('01-04-'||EXTRACT (YEAR FROM ADD_MONTHS (sysdate,  9)),'dd-mm-yyyy') -1
                                /* ORIGSQL: g_ytd_enddate -1 */
                                AND pe.removedate = to_date('1-JAN-2200', 'DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200', 'DD-MON-YYYY') */
                                AND pt.removedate = to_date('1-JAN-2200','DD-MON-YYYY')  /* ORIGSQL: TO_DATE('1-JAN-2200','DD-MON-YYYY') */
                                AND pt.name = 'month'
                                AND pe.periodtypeseq = pt.periodtypeseq
                                AND pe.calendarseq = :vcalendarseq
                        ) -- and pe.calendarseq = 2251799813685251)
                    ) AS tot
                    ON summ.periodseq = tot.periodseq;  /* ORIGSQL: summ.periodseq(+)=tot.periodseq */

        /* ORIGSQL: commit; */
        COMMIT;

        --------End Insert---------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (:vperiodrow.name, vProcName, 'Report table insert complete', NULL, (...) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Report table insert complete', NULL, :vSQLERRM);

        --------Gather stats-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (:vperiodrow.name, vProcName, 'Start DBMS_STATS', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vperiodrow.name, :vProcName, 'Start DBMS_STATS', NULL, :vSQLERRM);

        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTable (vrpttablename) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);/*Deepan : Partition Not required*/

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END