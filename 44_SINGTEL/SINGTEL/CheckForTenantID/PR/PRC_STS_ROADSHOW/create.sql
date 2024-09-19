CREATE PROCEDURE EXT.PRC_STS_ROADSHOW
(
    --IN vrptname rpt_sts_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_sts_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                                   /* RESOLVE: Datatype unresolved: Datatype (rpt_sts_mapping.reportname%TYPE) not resolved for parameter 'PRC_STS_ROADSHOW.vrptname' */
                                                   /* ORIGSQL: vrptname IN rpt_sts_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_STS_ROADSHOW.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq  BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_STS_ROADSHOW.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_STS_ROADSHOW.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --DECLARE PKG_REPORTING_EXTRACT_R2__vSTSRoadShowCategory VARCHAR(255); /* package/session variable */
    --DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    -------------------------------------------------------------------------------------------------------------------
    -- Purpose:
    --
    -- Design objectives:
    -- Data for Road show  Report
    -------------------------------------------------------------------------------------------------------------------
    -- Modification Log:
    -- Date             Author        Description
    -------------------------------------------------------------------------------------------------------------------
    -- 05-Jan-2017      Nandini Varadharajan  Initial release
    -------------------------------------------------------------------------------------------------------------------
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_STS_ROADSHOW');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_STS_ROADSHOW') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_STS_ROADSHOW';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_STS_ROADSHOW'; */
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
    DECLARE vSTSRoadShowCategory NVARCHAR(50);
    DECLARE cEndofTime date;
    
    --  vCurYrStartDate       DATE;
    -- vCurYrEndDate         DATE;
    DECLARE v_envdtl VARCHAR(1000);  /* ORIGSQL: v_envdtl VARCHAR2(1000); */
    DECLARE v_sql NCLOB;  /* ORIGSQL: v_sql long; */
    DECLARE cnt DECIMAL(38,10);  /* ORIGSQL: cnt number; */
    DECLARE cnt1 DECIMAL(38,10);  /* ORIGSQL: cnt1 number; */
    DECLARE v_nullinsert VARCHAR(255);  /* ORIGSQL: v_nullinsert varchar2(255); */
    --DECLARE v_nullingroup rpt_sts_mapping%rowtype;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.rpt_sts_mapping' not found (for %ROWTYPE declaration) */
    DECLARE v_nullingroup ROW LIKE ext.rpt_sts_mapping;
    DECLARE v_roadshowname VARCHAR(255);  /* ORIGSQL: v_roadshowname varchar2(255); */

    /* ORIGSQL: CURSOR c_nullingroups is select distinct REPORTNAME,RULENAME,COLUMNNAME, RPTCOLU(...) */
    DECLARE CURSOR c_nullingroups
    FOR
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_STS_MAPPING' not found */

        SELECT   /* ORIGSQL: SELECT distinct REPORTNAME,RULENAME,COLUMNNAME, RPTCOLUMNNAME,PRODUCT,ALLGROUPS,(...) */
            DISTINCT
            REPORTNAME,
            RULENAME,
            COLUMNNAME,
            RPTCOLUMNNAME,
            PRODUCT,
            ALLGROUPS,
            report_frequency
        FROM
            rpt_sts_mapping
        WHERE
            reportname = :vrptname;

    /* ORIGSQL: CURSOR c_roadshowname is select distinct cl.genericattribute5 ROADSHOWNAME from (...) */
    DECLARE CURSOR c_roadshowname
    FOR
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_CLASSIFIER' not found */

        SELECT   /* ORIGSQL: SELECT distinct cl.genericattribute5 ROADSHOWNAME from STELEXT.STEL_CLASSIFIER c(...) */
            DISTINCT
            cl.genericattribute5 AS ROADSHOWNAME
        FROM
            EXT.STEL_CLASSIFIER cl
        WHERE
            cl.categoryname = :vSTSRoadShowCategory  /* ORIGSQL: pkg_reporting_extract_r2.vSTSRoadShowCategory */
            AND cl.effectiveenddate > :vPeriodRow.startdate
            AND cl.effectivestartdate < :vPeriodRow.enddate;

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
        /* retrieve the package/session variables referenced in this procedure */
        --SELECT SESSION_CONTEXT('DBMTK_GLOBVAR_EXT_PKG_REPORTING_EXTRACT_R2_VSTSROADSHOWCATEGORY') INTO PKG_REPORTING_EXTRACT_R2__vSTSRoadShowCategory FROM SYS.DUMMY ;
        /* end of package/session variables */
        SELECT SESSION_CONTEXT('VSTSROADSHOWCATEGORY') INTO VSTSROADSHOWCATEGORY FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('cEndofTime') INTO cEndofTime FROM SYS.DUMMY ;

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
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName); --Sanjay: commenting as subpartition is not required

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);--Sanjay: commenting as Truncate is not required

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as sAnalyzeTable is not required

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
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);

        /* ORIGSQL: rpt_data_sts_roadshow('RoadshowDataCallidus', vperiodseq, vprocessingunitseq) */
        CALL EXT.RPT_DATA_STS_ROADSHOW('RoadshowDataCallidus', :vperiodseq, :vprocessingunitseq);

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_STS_ROADSHOW' not found */

        /* ORIGSQL: INSERT INTO STELEXT.RPT_STS_ROADSHOW (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSIN(...) */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_DATA_ROADSHOWACTUALS' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BASE_PADIMENSION' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_STS_MAPPING' not found */
        INSERT INTO EXT.RPT_STS_ROADSHOW
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
                ROADSHOWNAME,
                SHOWSTARTDATE,
                SHOWENDDATE,
                ROADSHOWCODE,
                PAYEEID,
                PAYEENAME,
                SHOPCODE,
                SHOPNAME,
                SALESMANCODE,
                REPORTCATEGORY,
                PRODUCT,
                TXNTYPE,
                VALUETYPE,
                VALUE,
                RPTPRODUCTNAME,
                RPTVALUENAME
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, pad.positionseq, pad.payeeseq, vProcessingUnitRow.processingun(...) */
                :vTenantId,
                pad.positionseq,
                pad.payeeseq,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq AS PERIODSEQ,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '61' AS reportcode,
                '01' AS sectionid,
                'DETAIL' AS sectionname,
                '01' AS sortorder,
                pad.firstname AS empfirstname,
                pad.lastname AS emplastname,
                pad.reporttitle AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                /* --rs.PERIODSEQ   PERIODSEQ, */
                cl.genericattribute5 AS ROADSHOWNAME,/* --(category name like roadshow), */  cl.effectivestartdate AS SHOWSTARTDATE,
                (TO_DATE(ADD_SECONDS(cl.effectiveenddate,(86400*-1)))) AS SHOWENDDATE,  /* ORIGSQL: cl.effectiveenddate-1 */
                RS.ROADSHOWCODE AS ROADSHOWCODE,
                RS.PAYEEID AS GEID,
                pad.lastname AS PAYEENAME,
                /* --pa.payeeseq, */
                RS.SHOPCODE AS SHOPCODE,
                (
                    SELECT   /* ORIGSQL: (select MAX(sh.fullname) from rpt_base_padimension sh where sh.participantid = p(...) */
                        MAX(sh.fullname) 
                    FROM
                        ext.rpt_base_padimension sh
                    WHERE
                        sh.participantid = pad.positionga1
                        AND sh.periodseq = :vperiodseq
                        AND sh.processingunitseq = :vprocessingunitseq
                        AND sh.reportgroup = 'STS'
                        AND sh.positiontitle = 'STS - Shop'
                ) AS shopname,
                pad.participantga1 AS SALESMANCODE,
                RS.REPORTCATEGORY AS REPORTCATEGORY,
                RS.PRODUCT AS PRODUCT,
                /* -- RS.TXNTYPE TXNTYPE, */
                rmap.rulename AS TXNTYPE,
                RS.VALUETYPE AS VALUETYPE,
                IFNULL(RS.VALUE,0) AS VALUE,  /* ORIGSQL: nvl(RS.VALUE,0) */
                rmap.allgroups AS RPTPRODUCTNAME,
                rmap.rptcolumnname AS RPTVALUENAME
            FROM
                EXT.STEL_RPT_DATA_ROADSHOWACTUALS RS,
                EXT.RPT_BASE_PADIMENSION pad,
                EXT.rpt_sts_mapping rmap,
                EXT.STEL_CLASSIFIER cl
            WHERE
                rs.PERIODSEQ = pad.periodseq
                AND pad.periodseq = :vperiodseq
                AND pad.processingunitseq = :vprocessingunitseq
                AND pad.participantid = rs.payeeid
                -- and cl.CATEGORYNAME ='Roadshow Salesman Map'
                --and cl.classifierid = RS.ROADSHOWCODE
                AND cl.genericattribute4 = RS.ROADSHOWCODE
                AND cl.categoryname = :vSTSRoadShowCategory  /* ORIGSQL: pkg_reporting_extract_r2.vSTSRoadShowCategory */
                AND cl.effectiveenddate > :vPeriodRow.startdate
                AND cl.effectivestartdate < :vPeriodRow.enddate
                AND LTRIM(RTRIM(rmap.product)) = LTRIM(RTRIM(rs.product))
                AND LTRIM(RTRIM(rmap.rulename)) = LTRIM(RTRIM(rs.txntype))
                AND rmap.reportname = :vrptname
                AND LTRIM(RTRIM(rmap.columnname)) = LTRIM(RTRIM(rs.valuetype))
                AND pad.PAYEESEQ IN
                (
                    SELECT   /* ORIGSQL: (select distinct c.PAYEESEQ from STEL_CLASSIFIER a, cs_payee b,cs_participant c (...) */
                        DISTINCT
                        c.PAYEESEQ
                    FROM
                        EXT.STEL_CLASSIFIER a,
                        cs_payee b,
                        cs_participant c
                    WHERE
                        b.payeeid = a.genericattribute2
                        AND b.PAYEESEQ = c.PAYEESEQ
                        AND a.categoryname = 'Roadshow Salesman Map'
                )

                /*and ltrim (rtrim (rmap.product)) = ltrim (rtrim (rs.product))(+)
                and ltrim (rtrim (rmap.rulename )) =  ltrim (rtrim (rs.txntype))(+)
                and rmap.reportname= vrptname
                and ltrim (rtrim (rmap.columnname ))  =    ltrim (rtrim (rs.valuetype))(+)   */
                AND rs.SHOPCODE = cl.genericattribute2;

        -- and rs.PAYEEID=cl.genericattribute2;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: Open c_roadshowname; */
        OPEN c_roadshowname;

        /* ORIGSQL: Loop */
        LOOP 
            /* ORIGSQL: fetch c_roadshowname into v_roadshowname; */
            FETCH c_roadshowname INTO v_roadshowname;

            /* ORIGSQL: EXIT WHEN c_roadshowname%NOTFOUND */
            IF c_roadshowname::NOTFOUND  
            THEN
                BREAK;
            END IF;

            /* ORIGSQL: open c_nullingroups; */
            OPEN c_nullingroups;

            /* ORIGSQL: loop */
            LOOP 
                /* ORIGSQL: fetch c_nullingroups into v_nullingroup; */
                FETCH c_nullingroups INTO v_nullingroup;

                /* ORIGSQL: EXIT WHEN c_nullingroups%NOTFOUND */
                IF c_nullingroups::NOTFOUND  
                THEN
                    BREAK;
                END IF;

                cnt = 0;

                /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_STS_ROADSHOW' not found */

                SELECT
                    COUNT(*)
                INTO
                    cnt
                FROM
                    EXT.RPT_STS_ROADSHOW rpt
                WHERE
                    rpt.processingunitseq = :vprocessingunitseq
                    AND rpt.periodseq = :vperiodseq
                    AND rpt.RPTPRODUCTNAME = :v_nullingroup.ALLGROUPS
                    AND rpt.RPTVALUENAME = :v_nullingroup.RPTCOLUMNNAME
                    AND rpt.ROADSHOWNAME = :v_roadshowname;

                IF :cnt = 0
                THEN  
                    /* ORIGSQL: INSERT INTO STELEXT.RPT_STS_ROADSHOW (TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSIN(...) */
                    INSERT INTO EXT.RPT_STS_ROADSHOW
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
                            ROADSHOWNAME,
                            SHOWSTARTDATE,
                            SHOWENDDATE,
                            ROADSHOWCODE,
                            PAYEEID,
                            PAYEENAME,
                            SHOPCODE,
                            SHOPNAME,
                            SALESMANCODE,
                            REPORTCATEGORY,
                            PRODUCT,
                            TXNTYPE,
                            VALUETYPE,
                            VALUE,
                            RPTPRODUCTNAME,
                            RPTVALUENAME
                        )
                       /* SELECT   /* ORIGSQL: select TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME(...) 
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
                            :v_roadshowname AS ROADSHOWNAME,
                            SHOWSTARTDATE,
                            SHOWENDDATE,
                            ROADSHOWCODE,
                            PAYEEID,
                            PAYEENAME,
                            SHOPCODE,
                            SHOPNAME,
                            SALESMANCODE,
                            REPORTCATEGORY,
                            PRODUCT,
                            TXNTYPE,
                            VALUETYPE,
                            0 AS VALUE,
                            :v_nullingroup.ALLGROUPS AS RPTPRODUCTNAME,
                            :v_nullingroup.RPTCOLUMNNAME AS RPTVALUENAME
                        FROM
                            EXT.RPT_STS_ROADSHOW
                        WHERE
                            processingunitseq = :vprocessingunitseq
                            AND periodseq = :vperiodseq
                            AND "rowid" IN  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                                            /* ORIGSQL: rowid 
                            (
                                SELECT   /* ORIGSQL: (select MAX(rowid) from RPT_STS_ROADSHOW where periodseq = vperio(...) 
                                    MAX("rowid")   /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly 
                                FROM
                                    EXT.RPT_STS_ROADSHOW
                                WHERE
                                    periodseq = :vperiodseq
                                    AND ROADSHOWNAME = :v_roadshowname
                            );*/
                            WITH RankedData AS (

    SELECT   /* ORIGSQL: select TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME(...) */
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
                            :v_roadshowname AS ROADSHOWNAME,
                            SHOWSTARTDATE,
                            SHOWENDDATE,
                            ROADSHOWCODE,
                            PAYEEID,
                            PAYEENAME,
                            SHOPCODE,
                            SHOPNAME,
                            SALESMANCODE,
                            REPORTCATEGORY,
                            PRODUCT,
                            TXNTYPE,
                            VALUETYPE,
                            0 AS VALUE,
                            :v_nullingroup.ALLGROUPS AS RPTPRODUCTNAME,
                            :v_nullingroup.RPTCOLUMNNAME AS RPTVALUENAME,

        ROW_NUMBER() OVER (PARTITION BY ROADSHOWNAME ORDER BY LOADDTTM DESC) AS rn

    FROM

        RPT_STS_ROADSHOW

    WHERE

        processingunitseq = :vprocessingunitseq

        AND periodseq = :vperiodseq

)
 
SELECT   /* ORIGSQL: select TENANTID, POSITIONSEQ, PAYEESEQ, PROCESSINGUNITSEQ, PERIODSEQ, PERIODNAME(...) */
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
                            :v_roadshowname AS ROADSHOWNAME,
                            SHOWSTARTDATE,
                            SHOWENDDATE,
                            ROADSHOWCODE,
                            PAYEEID,
                            PAYEENAME,
                            SHOPCODE,
                            SHOPNAME,
                            SALESMANCODE,
                            REPORTCATEGORY,
                            PRODUCT,
                            TXNTYPE,
                            VALUETYPE,
                            0 AS VALUE,
                            :v_nullingroup.ALLGROUPS AS RPTPRODUCTNAME,
                            :v_nullingroup.RPTCOLUMNNAME AS RPTVALUENAME

FROM

    RankedData

WHERE

  rn = 1
 AND periodseq = :vperiodseq
 AND ROADSHOWNAME = :v_roadshowname;

                    /* ORIGSQL: commit; */
                    COMMIT;
                END IF;
            END LOOP;  /* ORIGSQL: end loop; */

            /* ORIGSQL: close c_nullingroups; */
            CLOSE c_nullingroups;
        END LOOP;  /* ORIGSQL: end loop; */

        /* ORIGSQL: close c_roadshowname; */
        CLOSE c_roadshowname;

        --------Turn on Parallel DML---------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION ENABLE PARALLEL DML ; */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TOTAL insert',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TOTAL insert', NULL, NULL);

        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Report table insert complete', NULL, (...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Report table insert complete', NULL, :vSQLERRM);

        --------Gather stats-------------------------------------------------------------------------------
        /* ORIGSQL: prc_logevent (vPeriodRow.name, vProcName, 'Start DBMS_STATS', NULL, vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Start DBMS_STATS', NULL, :vSQLERRM);

        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTable (vrpttablename) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END