CREATE PROCEDURE EXT.PRC_CCO_TV_PAYOUTSUMMARY
(
    IN vrptname NVARCHAR(255),   /* RESOLVE: Identifier not found: Table/Column 'rpt_cco_mapping.reportname' not found (for %TYPE declaration) */
                                                   /* RESOLVE: Datatype unresolved: Datatype (rpt_cco_mapping.reportname%TYPE) not resolved for parameter 'PRC_CCO_TV_PAYOUTSUMMARY.vrptname' */
                                                   /* ORIGSQL: vrptname IN rpt_cco_mapping.reportname%TYPE */
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_CCO_TV_PAYOUTSUMMARY.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_CCO_TV_PAYOUTSUMMARY.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_CCO_TV_PAYOUTSUMMARY.vcalendarseq' */
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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_CCO_TV_PAYOUTSUMMARY');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_CCO_TV_PAYOUTSUMMARY') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_CCO_TV_PAYMENTSUMMARY';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_CCO_TV_PAYMENTSUMMARY'; */
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
    DECLARE rec_config ROW LIKE rpt_cco_rate_table_config;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.rpt_cco_rate_table_config' not found (for %ROWTYPE declaration) */
    DECLARE cnt DECIMAL(38,10);  /* ORIGSQL: cnt number; */
    DECLARE cnt1 DECIMAL(38,10);  /* ORIGSQL: cnt1 number; */
    DECLARE v_nullinsert VARCHAR(255);  /* ORIGSQL: v_nullinsert varchar2(255); */
    DECLARE v_nullingroup ROW LIKE RPT_CCO_MAPPING;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.RPT_CCO_MAPPING' not found (for %ROWTYPE declaration) */
    DECLARE v_sites VARCHAR(255);  /* ORIGSQL: v_sites varchar2(255); */
    DECLARE cEndofTime date;
    /* ORIGSQL: CURSOR c_config IS SELECT RM_NUMBER, RM_DISPLAY, LOWER_BOUND, UPPER_BOUND FROM r(...) */
    DECLARE CURSOR c_config
    FOR 
        SELECT   /* ORIGSQL: SELECT RM_NUMBER, RM_DISPLAY, LOWER_BOUND, UPPER_BOUND FROM rpt_cco_rate_table_c(...) */
            RM_NUMBER,
            RM_DISPLAY,
            LOWER_BOUND,
            UPPER_BOUND
        FROM
            rpt_cco_rate_table_config
        ORDER BY
            rm_number;

    /* ORIGSQL: CURSOR c_nullinsert is select RM_DISPLAY from rpt_cco_rate_table_config; */
    DECLARE CURSOR c_nullinsert
    FOR 
        SELECT   /* ORIGSQL: SELECT RM_DISPLAY from rpt_cco_rate_table_config; */
            RM_DISPLAY
        FROM
            rpt_cco_rate_table_config;

    /* ORIGSQL: CURSOR c_nullingroups is select * from RPT_CCO_MAPPING where reportname=vrptname(...) */
    DECLARE CURSOR c_nullingroups
    FOR
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_CCO_MAPPING' not found */

        SELECT   /* ORIGSQL: SELECT * from RPT_CCO_MAPPING where reportname=vrptname; */
            *
        FROM
            RPT_CCO_MAPPING
        WHERE
            reportname = :vrptname;

    /* ORIGSQL: CURSOR c_sites is select distinct positionga3 sites from rpt_base_padimension pa(...) */
    DECLARE CURSOR c_sites
    FOR
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_PADIMENSION' not found */

        SELECT   /* ORIGSQL: SELECT distinct positionga3 sites from rpt_base_padimension pad where pad.period(...) */
            DISTINCT
            positionga3 AS sites
        FROM
            rpt_base_padimension pad
        WHERE
            pad.periodseq = :vperiodseq
            AND pad.processingunitseq = :vprocessingunitseq
            AND pad.reportgroup = 'CCO TV';

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
      
        SELECT SESSION_CONTEXT('cEndofTime') INTO cEndofTime FROM SYS.DUMMY ;
        
        select * into vperiodRow from cs_period where periodseq=:vperiodseq and removedate = :cEndofTime;
        
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
        --     ); /*Deepan : Partition not required*/

        --------Find subpartition name------------------------------------------------------------------------------------

        -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) *//*Deepan : Partition not required*/

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);/*Deepan : Partition not required*/

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);/*Deepan : Partition not required*/

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);/*Deepan : Partition not required*/

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

        /* ORIGSQL: EXECUTE IMMEDIATE 'truncate table rpt_cco_rate_table_config '; */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_CCO_RATE_TABLE_CONFIG' not found */

        /* ORIGSQL: truncate table rpt_cco_rate_table_config ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE rpt_cco_rate_table_config';

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_RELATIONALMDLT' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_MDLTDIMENSION' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_MDLTINDEX' not found */
        /* ORIGSQL: INSERT INTO rpt_cco_rate_table_config SELECT * FROM (SELECT DISTINCT ind.ordinal(...) */
        INSERT INTO rpt_cco_rate_table_config
            SELECT   /* ORIGSQL: SELECT * FROM (SELECT DISTINCT ind.ordinal, '$<'||ind.maxvalue, ind.minvalue, in(...) */
                *
            FROM
                (
                    SELECT   /* ORIGSQL: (SELECT DISTINCT ind.ordinal, '$<' ||ind.maxvalue, ind.minvalue, ind.maxvalue FR(...) */
                        DISTINCT
                        ind.ordinal,
                        '$<'||IFNULL(TO_VARCHAR(ind.maxvalue),''),
                        ind.minvalue,
                        ind.maxvalue  /* --'RM'||ind.minvalue||'-'||'RM'||ind.maxvalue */
                        /* --INTO   var_ProcessingFrequency */
                    FROM
                        CS_RelationalMDLT Rmd,
                        CS_MDLTDIMENSION Dim,
                        CS_MDLTINDEX Ind
                    WHERE
                        Rmd.Name = 'LT_CCO_TV_Rate'
                        AND dim.name = 'ARPU Range'
                        AND Rmd.RemoveDate = '01-JAN-2200'
                        AND Rmd.EffectiveEndDate = '01-JAN-2200'
                        AND Dim.RemoveDate = '01-JAN-2200'
                        AND Dim.EffectiveEndDate = '01-JAN-2200'
                        AND Dim.RuleElementSeq = Rmd.RuleelementSeq
                        AND Ind.RemoveDate = '01-JAN-2200'
                        AND Ind.EffectiveEndDate = '01-JAN-2200'
                        AND Ind.RuleElementSeq = Rmd.RuleelementSeq
                        AND Ind.DimensionSeq = dim.DimensionSeq
                        AND (ind.minvalue = 0)-- order by ind.ordinal
            UNION ALL
                SELECT   /* ORIGSQL: SELECT DISTINCT ind.ordinal, '$' ||ind.minvalue ||'-' ||'$' ||ind.maxvalue, ind.(...) */
                    DISTINCT
                    ind.ordinal,
                    '$'||IFNULL(TO_VARCHAR(ind.minvalue),'')
                    ||'-'
                    ||'$'
                    ||IFNULL(TO_VARCHAR(ind.maxvalue),''),
                    ind.minvalue,
                    ind.maxvalue
                    /* --INTO   var_ProcessingFrequency */
                FROM
                    CS_RelationalMDLT Rmd,
                    CS_MDLTDIMENSION Dim,
                    CS_MDLTINDEX Ind
                WHERE
                    Rmd.Name = 'LT_CCO_TV_Rate'
                    AND dim.name = 'ARPU Range'
                    AND Rmd.RemoveDate = '01-JAN-2200'
                    AND Rmd.EffectiveEndDate = '01-JAN-2200'
                    AND Dim.RemoveDate = '01-JAN-2200'
                    AND Dim.EffectiveEndDate = '01-JAN-2200'
                    AND Dim.RuleElementSeq = Rmd.RuleelementSeq
                    AND Ind.RemoveDate = '01-JAN-2200'
                    AND Ind.EffectiveEndDate = '01-JAN-2200'
                    AND Ind.RuleElementSeq = Rmd.RuleelementSeq
                    AND Ind.DimensionSeq = dim.DimensionSeq
                    AND (ind.minvalue != 0
                    AND ind.maxvalue IS NOT NULL)
                    --order by ind.ordinal;;
            UNION ALL
                SELECT   /* ORIGSQL: SELECT DISTINCT ind.ordinal, '$>' ||ind.minvalue, ind.minvalue, ind.maxvalue FRO(...) */
                    DISTINCT
                    ind.ordinal,
                    '$>'||IFNULL(TO_VARCHAR(ind.minvalue),''),
                    ind.minvalue,
                    ind.maxvalue
                    /* --INTO   var_ProcessingFrequency */
                FROM
                    CS_RelationalMDLT Rmd,
                    CS_MDLTDIMENSION Dim,
                    CS_MDLTINDEX Ind
                WHERE
                    Rmd.Name = 'LT_CCO_TV_Rate'
                    AND dim.name = 'ARPU Range'
                    AND Rmd.RemoveDate = '01-JAN-2200'
                    AND Rmd.EffectiveEndDate = '01-JAN-2200'
                    AND Dim.RemoveDate = '01-JAN-2200'
                    AND Dim.EffectiveEndDate = '01-JAN-2200'
                    AND Dim.RuleElementSeq = Rmd.RuleelementSeq
                    AND Ind.RemoveDate = '01-JAN-2200'
                    AND Ind.EffectiveEndDate = '01-JAN-2200'
                    AND Ind.RuleElementSeq = Rmd.RuleelementSeq
                    AND Ind.DimensionSeq = dim.DimensionSeq
                    AND (ind.minvalue != 0
                    AND ind.maxvalue IS NULL)
            ) AS dbmtk_corrname_5367
        ORDER BY
            1;

     FOR rec_config as c_config DO /*Deepan :Changed from LOOP to FOR statement*/

            /* ORIGSQL: EXIT WHEN c_config%NOTFOUND */
        -- IF c_config::NOTFOUND  
        --  THEN
        --   BREAK;
        IF c_config::NOTFOUND  
            THEN
                    BREAK;
            END IF;
        IF rec_config.lower_bound = 0 THEN
            --------Begin Insert-------------------------------------------------------------------------------
            /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin insert RM',NULL,vsqlerrm) */
            CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert RM', NULL, :vSQLERRM);
                --RM<1

                /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_SALESTRANSACTION' not found */
                /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_CREDIT' not found */
                /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_CCO_TV_PAYMENTSUMMARY' not found */

                /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
                INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                        SITES,
                        TEAM,
                        CCONAME,
                        GEID,
                        DEALCODE,
                        AGENCY,
                        CURRENCY,
                        ALLGROUPS,
                        RMDISPLAY,
                        RMVALUE
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
                        '58' AS reportcode,
                        '01' AS sectionid,
                        'DETAIL' AS sectionname,
                        rec_config.RM_NUMBER AS sortorder,
                        pad.firstname AS empfirstname,
                        pad.lastname AS emplastname,
                        pad.reporttitle AS titlename,
                        :vPeriodRow.startdate,
                        :vPeriodRow.enddate,
                        CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                        pad.positionga3 AS SITES,
                        pad.positionga1 AS TEAM,
                        pad.fullname AS CCONAME,
                        pad.positionname AS GEID,
                        pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                        (
                            CASE
                                WHEN pad.positionga3 = 'CSSIN'
                                OR pad.positionga3 = 'CCO TV'
                                OR pad.positionga3 IS NULL
                                THEN 'S$'
                                WHEN pad.positionga3 = 'CSKCC'
                                OR pad.positionga3 = 'CSMCC'
                                THEN 'RM'
                            END
                        ) AS CURRENCY,
                        'No. of Orders' AS ALLGROUPS,
                        rec_config.RM_DISPLAY AS RMDISPLAY,
                        IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
                    FROM
                        /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                        (
                            SELECT   /* ORIGSQL: (SELECT cr.periodseq, cr.processingunitseq, cr.payeeseq, cr.positionseq, st.TRNS(...) */
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,
                                st.TRNSASSIGNPOSITIONNAME,
                                'No. of Orders' AS ALLGROUPS,
                                rec_config.RM_DISPLAY AS RMDISPLAY,
                                COUNT(*) AS RMVALUE
                            FROM
                                rpt_base_salestransaction st,
                                rpt_base_credit cr
                            WHERE
                                st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                                AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                                AND st.genericnumber3 < rec_config.lower_bound
                                AND cr.processingunitseq = :vprocessingunitseq
                                AND cr.periodseq = :vperiodseq
                            GROUP BY
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,st.TRNSASSIGNPOSITIONNAME
                        ) AS mes
                    RIGHT OUTER JOIN
                        EXT.rpt_base_padimension AS pad
                        ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                        AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                        AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                        AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                        AND pad.positionname = mes.TRNSASSIGNPOSITIONNAME  /* ORIGSQL: pad.positionname= mes.TRNSASSIGNPOSITIONNAME(+) */
                    WHERE
                        pad.periodseq = :vperiodseq
                        AND pad.processingunitseq = :vprocessingunitseq
                        AND pad.reportgroup = 'CCO TV';

                /* ORIGSQL: COMMIT; */
                COMMIT;

                /* RESOLVE: Identifier not found: Table/view 'TCMP.CS_CREDIT' not found */
                /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
                INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                        SITES,
                        TEAM,
                        CCONAME,
                        GEID,
                        DEALCODE,
                        AGENCY,
                        CURRENCY,
                        ALLGROUPS,
                        RMDISPLAY,
                        RMVALUE
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
                        '58' AS reportcode,
                        '03' AS sectionid,
                        'DETAIL' AS sectionname,
                        rec_config.RM_NUMBER AS sortorder,
                        pad.firstname AS empfirstname,
                        pad.lastname AS emplastname,
                        pad.reporttitle AS titlename,
                        :vPeriodRow.startdate,
                        :vPeriodRow.enddate,
                        CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                        pad.positionga3 AS SITES,
                        pad.positionga1 AS TEAM,
                        pad.fullname AS CCONAME,
                        pad.positionname AS GEID,
                        pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                        (
                            CASE
                                WHEN pad.positionga3 = 'CSSIN'
                                OR pad.positionga3 = 'CCO TV'
                                OR pad.positionga3 IS NULL
                                THEN 'S$'
                                WHEN pad.positionga3 = 'CSKCC'
                                OR pad.positionga3 = 'CSMCC'
                                THEN 'RM'
                            END
                        ) AS CURRENCY,
                        'ARPU' AS ALLGROUPS,
                        rec_config.RM_DISPLAY AS RMDISPLAY,
                        IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
                    FROM
                        /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                        (
                            SELECT   /* ORIGSQL: (select cr.periodseq, cr.processingunitseq, cr.payeeseq, cr.positionseq, st.TRNS(...) */
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,
                                st.TRNSASSIGNPOSITIONNAME,
                                'ARPU' AS ALLGROUPS,
                                rec_config.RM_DISPLAY AS RMDISPLAY,
                                SUM(cr.value) AS RMVALUE
                            FROM
                                rpt_base_salestransaction st,
                                TCMP.Cs_Credit cr
                            WHERE
                                st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                                AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                                AND st.genericnumber3 < rec_config.lower_bound
                                AND cr.processingunitseq = :vprocessingunitseq
                                AND cr.periodseq = :vperiodseq
                            GROUP BY
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,st.TRNSASSIGNPOSITIONNAME
                        ) AS mes
                    RIGHT OUTER JOIN
                        EXT.rpt_base_padimension AS pad
                        ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                        AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                        AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                        AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                        AND pad.positionname = mes.TRNSASSIGNPOSITIONNAME  /* ORIGSQL: pad.positionname= mes.TRNSASSIGNPOSITIONNAME(+) */
                    WHERE
                        pad.periodseq = :vperiodseq
                        AND pad.processingunitseq = :vprocessingunitseq
                        AND pad.reportgroup = 'CCO TV';

                /* ORIGSQL: COMMIT; */
                COMMIT;
                
           END IF;
           
           IF (rec_config.upper_bound IS NOT NULL)  THEN  /* ORIGSQL: elsif (rec_config.upper_bound IS NOT NULL) THEN */
         
                -- RM between 2 different Range   

                /* ORIGSQL: INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, processi(...) */
                INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY
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
                        SITES,
                        TEAM,
                        CCONAME,
                        GEID,
                        DEALCODE,
                        AGENCY,
                        CURRENCY,
                        ALLGROUPS,
                        RMDISPLAY,
                        RMVALUE
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
                        '58' AS reportcode,
                        '01' AS sectionid,
                        'DETAIL' AS sectionname,
                        rec_config.RM_NUMBER AS sortorder,
                        pad.firstname AS empfirstname,
                        pad.lastname AS emplastname,
                        pad.reporttitle AS titlename,
                        :vPeriodRow.startdate,
                        :vPeriodRow.enddate,
                        CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                        pad.positionga3 AS SITES,
                        pad.positionga1 AS TEAM,
                        pad.fullname AS CCONAME,
                        pad.positionname AS GEID,
                        pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                        (
                            CASE
                                WHEN pad.positionga3 = 'CSSIN'
                                OR pad.positionga3 = 'CCO TV'
                                OR pad.positionga3 IS NULL
                                THEN 'S$'
                                WHEN pad.positionga3 = 'CSKCC'
                                OR pad.positionga3 = 'CSMCC'
                                THEN 'RM'
                            END
                        ) AS CURRENCY,
                        'No. of Orders' AS ALLGROUPS,
                        rec_config.RM_DISPLAY AS RMDISPLAY,
                        IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
                    FROM
                        /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                        (
                            SELECT   /* ORIGSQL: (SELECT cr.periodseq, cr.processingunitseq, cr.payeeseq, cr.positionseq, st.TRNS(...) */
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,
                                st.TRNSASSIGNPOSITIONNAME,
                                'No. of Orders' AS ALLGROUPS,
                                rec_config.RM_DISPLAY AS RMDISPLAY,
                                COUNT(*) AS RMVALUE
                            FROM
                                rpt_base_salestransaction st,
                                TCMP.Cs_Credit cr
                            WHERE
                                st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                                AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                                AND (st.genericnumber3 > rec_config.lower_bound
                                AND st.genericnumber3 < rec_config.upper_bound)
                                AND cr.processingunitseq = :vprocessingunitseq
                                AND cr.periodseq = :vperiodseq
                            GROUP BY
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq, st.TRNSASSIGNPOSITIONNAME
                        ) AS mes
                    RIGHT OUTER JOIN
                        EXT.rpt_base_padimension AS pad
                        ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                        AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                        AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                        AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                        AND pad.positionname = mes.TRNSASSIGNPOSITIONNAME  /* ORIGSQL: pad.positionname= mes.TRNSASSIGNPOSITIONNAME(+) */
                    WHERE
                        pad.periodseq = :vperiodseq
                        AND pad.processingunitseq = :vprocessingunitseq
                        AND pad.reportgroup = 'CCO TV';

                /* ORIGSQL: COMMIT; */
                COMMIT;  

                /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
                INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                        SITES,
                        TEAM,
                        CCONAME,
                        GEID,
                        DEALCODE,
                        AGENCY,
                        CURRENCY,
                        ALLGROUPS,
                        RMDISPLAY,
                        RMVALUE
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
                        '58' AS reportcode,
                        '03' AS sectionid,
                        'DETAIL' AS sectionname,
                        rec_config.RM_NUMBER AS sortorder,
                        pad.firstname AS empfirstname,
                        pad.lastname AS emplastname,
                        pad.reporttitle AS titlename,
                        :vPeriodRow.startdate,
                        :vPeriodRow.enddate,
                        CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                        pad.positionga3 AS SITES,
                        pad.positionga1 AS TEAM,
                        pad.fullname AS CCONAME,
                        pad.positionname AS GEID,
                        pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                        (
                            CASE
                                WHEN pad.positionga3 = 'CSSIN'
                                OR pad.positionga3 = 'CCO TV'
                                OR pad.positionga3 IS NULL
                                THEN 'S$'
                                WHEN pad.positionga3 = 'CSKCC'
                                OR pad.positionga3 = 'CSMCC'
                                THEN 'RM'
                            END
                        ) AS CURRENCY,
                        'ARPU' AS ALLGROUPS,
                        rec_config.RM_DISPLAY AS RMDISPLAY,
                        IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
                    FROM
                        /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                        (
                            SELECT   /* ORIGSQL: (select cr.periodseq, cr.processingunitseq, cr.payeeseq, cr.positionseq, st.TRNS(...) */
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,
                                st.TRNSASSIGNPOSITIONNAME,
                                'ARPU' AS ALLGROUPS,
                                rec_config.RM_DISPLAY AS RMDISPLAY,
                                SUM(cr.value) AS RMVALUE
                            FROM
                                rpt_base_salestransaction st,
                                TCMP.Cs_Credit cr
                            WHERE
                                st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                                AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                                AND (st.genericnumber3 > rec_config.lower_bound
                                AND st.genericnumber3 < rec_config.upper_bound)
                                AND cr.processingunitseq = :vprocessingunitseq
                                AND cr.periodseq = :vperiodseq
                            GROUP BY
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,st.TRNSASSIGNPOSITIONNAME
                        ) AS mes
                    RIGHT OUTER JOIN
                        EXT.rpt_base_padimension AS pad
                        ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                        AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                        AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                        AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                        AND pad.positionname = mes.TRNSASSIGNPOSITIONNAME  /* ORIGSQL: pad.positionname= mes.TRNSASSIGNPOSITIONNAME(+) */
                    WHERE
                        pad.periodseq = :vperiodseq
                        AND pad.processingunitseq = :vprocessingunitseq
                        AND pad.reportgroup = 'CCO TV';
            ELSE 
                --RM>80   
                /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
                INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                        SITES,
                        TEAM,
                        CCONAME,
                        GEID,
                        DEALCODE,
                        AGENCY,
                        CURRENCY,
                        ALLGROUPS,
                        RMDISPLAY,
                        RMVALUE
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
                        '58' AS reportcode,
                        '01' AS sectionid,
                        'DETAIL' AS sectionname,
                        rec_config.RM_NUMBER AS sortorder,
                        pad.firstname AS empfirstname,
                        pad.lastname AS emplastname,
                        pad.reporttitle AS titlename,
                        :vPeriodRow.startdate,
                        :vPeriodRow.enddate,
                        CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                        pad.positionga3 AS SITES,
                        pad.positionga1 AS TEAM,
                        pad.fullname AS CCONAME,
                        pad.positionname AS GEID,
                        pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                        (
                            CASE
                                WHEN pad.positionga3 = 'CSSIN'
                                OR pad.positionga3 = 'CCO TV'
                                OR pad.positionga3 IS NULL
                                THEN 'S$'
                                WHEN pad.positionga3 = 'CSKCC'
                                OR pad.positionga3 = 'CSMCC'
                                THEN 'RM'
                            END
                        ) AS CURRENCY,
                        'No. of Orders' AS ALLGROUPS,
                        rec_config.RM_DISPLAY AS RMDISPLAY,
                        IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
                    FROM
                        /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                        (
                            SELECT   /* ORIGSQL: (SELECT cr.periodseq, cr.processingunitseq, cr.payeeseq, cr.positionseq, st.TRNS(...) */
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,
                                st.TRNSASSIGNPOSITIONNAME,
                                'No. of Orders' AS ALLGROUPS,
                                rec_config.RM_DISPLAY AS RMDISPLAY,
                                COUNT(*) AS RMVALUE
                            FROM
                                rpt_base_salestransaction st,
                                TCMP.Cs_Credit cr
                            WHERE
                                st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                                AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                                AND (st.genericnumber3 > rec_config.lower_bound)
                                AND cr.processingunitseq = :vprocessingunitseq
                                AND cr.periodseq = :vperiodseq
                            GROUP BY
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,st.TRNSASSIGNPOSITIONNAME
                        ) AS mes
                    RIGHT OUTER JOIN
                        EXT.rpt_base_padimension AS pad
                        ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                        AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                        AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                        AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                        AND pad.positionname = mes.TRNSASSIGNPOSITIONNAME  /* ORIGSQL: pad.positionname= mes.TRNSASSIGNPOSITIONNAME(+) */
                    WHERE
                        pad.periodseq = :vperiodseq
                        AND pad.processingunitseq = :vprocessingunitseq
                        AND pad.reportgroup = 'CCO TV';

                /* ORIGSQL: COMMIT; */
                COMMIT;  

                /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
                INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                        SITES,
                        TEAM,
                        CCONAME,
                        GEID,
                        DEALCODE,
                        AGENCY,
                        CURRENCY,
                        ALLGROUPS,
                        RMDISPLAY,
                        RMVALUE
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
                        '58' AS reportcode,
                        '03' AS sectionid,
                        'DETAIL' AS sectionname,
                        rec_config.RM_NUMBER AS sortorder,
                        pad.firstname AS empfirstname,
                        pad.lastname AS emplastname,
                        pad.reporttitle AS titlename,
                        :vPeriodRow.startdate,
                        :vPeriodRow.enddate,
                        CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                        pad.positionga3 AS SITES,
                        pad.positionga1 AS TEAM,
                        pad.fullname AS CCONAME,
                        pad.positionname AS GEID,
                        pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                        (
                            CASE
                                WHEN pad.positionga3 = 'CSSIN'
                                OR pad.positionga3 = 'CCO TV'
                                OR pad.positionga3 IS NULL
                                THEN 'S$'
                                WHEN pad.positionga3 = 'CSKCC'
                                OR pad.positionga3 = 'CSMCC'
                                THEN 'RM'
                            END
                        ) AS CURRENCY,
                        'ARPU' AS ALLGROUPS,
                        rec_config.RM_DISPLAY AS RMDISPLAY,
                        IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
                    FROM
                        /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                        (
                            SELECT   /* ORIGSQL: (select cr.periodseq, cr.processingunitseq, cr.payeeseq, cr.positionseq, st.TRNS(...) */
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq,
                                st.TRNSASSIGNPOSITIONNAME,
                                'ARPU' AS ALLGROUPS,
                                rec_config.RM_DISPLAY AS RMDISPLAY,
                                SUM(cr.value) AS RMVALUE
                            FROM
                                rpt_base_salestransaction st,
                                TCMP.Cs_Credit cr
                            WHERE
                                st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                                AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                                AND st.genericnumber3 > rec_config.lower_bound
                                AND cr.processingunitseq = :vprocessingunitseq
                                AND cr.periodseq = :vperiodseq
                            GROUP BY
                                cr.periodseq,
                                cr.processingunitseq,
                                cr.payeeseq,
                                cr.positionseq, st.TRNSASSIGNPOSITIONNAME
                        ) AS mes
                    RIGHT OUTER JOIN
                        EXT.rpt_base_padimension AS pad
                        ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                        AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                        AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                        AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
                        AND pad.positionname = mes.TRNSASSIGNPOSITIONNAME  /* ORIGSQL: pad.positionname= mes.TRNSASSIGNPOSITIONNAME(+) */
                    WHERE
                        pad.periodseq = :vperiodseq
                        AND pad.processingunitseq = :vprocessingunitseq
                        AND pad.reportgroup = 'CCO TV';

                /* ORIGSQL: COMMIT; */
                COMMIT;
            END IF;

            /* ORIGSQL: COMMIT; */
            COMMIT;


        /* ORIGSQL: CLOSE c_config; */
      END FOR;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin insert Monthly Total Take_ups',NU(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert Monthly Total Take_ups', NULL, :vSQLERRM); 

        /* ORIGSQL: INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, processi(...) */
        INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                SITES, TEAM, CCONAME, GEID, DEALCODE, AGENCY,
                CURRENCY, ALLGROUPS, RMDISPLAY, RMVALUE
            )
            SELECT   /* ORIGSQL: SELECT tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname(...) */
                tenantid,
                positionseq,
                payeeseq,
                processingunitseq,
                periodseq,
                periodname,
                processingunitname,
                calendarname,
                reportcode,
                2 AS sectionid,
                sectionname,
                1 AS sortorder,
                empfirstname,
                emplastname,
                titlename,
                periodstartdate,
                periodenddate,
                loaddttm,
                SITES,
                TEAM,
                CCONAME,
                GEID,
                DEALCODE,
                AGENCY,
                CURRENCY,
                'Monthy Total Take_ups' AS ALLGROUPS,
                NULL AS RMDISPLAY,
                SUM(rmvalue) AS RMVALUE
            FROM
                RPT_CCO_TV_PAYMENTSUMMARY
            WHERE
                processingunitseq = :vprocessingunitseq
                AND periodseq = :vperiodseq
                AND ALLGROUPS = 'No. of Orders'
            GROUP BY
                tenantid, positionseq, payeeseq, processingunitseq, periodseq,
                periodname, processingunitname, calendarname, reportcode, sectionid,
                sectionname, sortorder, empfirstname, emplastname, titlename,
                periodstartdate, periodenddate, loaddttm,
                SITES, TEAM, CCONAME, GEID,
                DEALCODE, AGENCY,CURRENCY, ALLGROUPS;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Insert Monthly Basket Payout ',NU(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Insert Monthly Basket Payout ', NULL, :vSQLERRM); 

        /* ORIGSQL: INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, processi(...) */
        INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                SITES, TEAM, CCONAME, GEID, DEALCODE, AGENCY,
                CURRENCY, ALLGROUPS, RMDISPLAY, RMVALUE
            )
            SELECT   /* ORIGSQL: SELECT tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname(...) */
                tenantid,
                positionseq,
                payeeseq,
                processingunitseq,
                periodseq,
                periodname,
                processingunitname,
                calendarname,
                reportcode,
                4 AS sectionid,
                sectionname,
                1 AS sortorder,
                empfirstname,
                emplastname,
                titlename,
                periodstartdate,
                periodenddate,
                loaddttm,
                SITES,
                TEAM,
                CCONAME,
                GEID,
                DEALCODE,
                AGENCY,
                CURRENCY,
                'Monthy Basket Payout' AS ALLGROUPS,
                NULL AS RMDISPLAY,
                SUM(rmvalue) AS RMVALUE
            FROM
                RPT_CCO_TV_PAYMENTSUMMARY
            WHERE
                processingunitseq = :vprocessingunitseq
                AND periodseq = :vperiodseq
                AND ALLGROUPS = 'ARPU'
            GROUP BY
                tenantid, positionseq, payeeseq, processingunitseq, periodseq,
                periodname, processingunitname, calendarname, reportcode, sectionid,
                sectionname, sortorder, empfirstname, emplastname, titlename,
                periodstartdate, periodenddate, loaddttm,
                SITES, TEAM, CCONAME, GEID,
                DEALCODE, AGENCY,CURRENCY, ALLGROUPS;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Insert Handling Fee ',NULL,vsqler(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Insert Handling Fee ', NULL, :vSQLERRM);  

        /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
        INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                SITES,
                TEAM,
                CCONAME,
                GEID,
                DEALCODE,
                AGENCY,
                CURRENCY,
                ALLGROUPS,
                RMDISPLAY,
                RMVALUE
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
                '58' AS reportcode,
                '05' AS sectionid,
                'DETAIL' AS sectionname,
                '01' AS sortorder,
                pad.firstname AS empfirstname,
                pad.lastname AS emplastname,
                pad.reporttitle AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                pad.positionga3 AS SITES,
                pad.positionga1 AS TEAM,
                pad.fullname AS CCONAME,
                pad.positionname AS GEID,
                pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                (
                    CASE
                        WHEN pad.positionga3 = 'CSSIN'
                        OR pad.positionga3 = 'CCO TV'
                        OR pad.positionga3 IS NULL
                        THEN 'S$'
                        WHEN pad.positionga3 = 'CSKCC'
                        OR pad.positionga3 = 'CSMCC'
                        THEN 'RM'
                    END
                ) AS CURRENCY,
                'Handing Fee' AS ALLGROUPS,
                NULL AS RMDISPLAY,
                IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.value) AS RMVALUE
                    FROM
                        TCMP.Cs_Credit mes
                    WHERE
                        mes.processingunitseq = :vprocessingunitseq
                        AND mes.periodseq = :vperiodseq
                        AND mes.credittypeseq = 16044073672509974
                        --nandini and mes.credittypeid = pkg_reporting_extract_r2.vcredittypeid_HandFee
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            RIGHT OUTER JOIN
                EXT.rpt_base_padimension AS pad
                ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
            WHERE
                pad.reportgroup = 'CCO TV';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Insert Balance From Previous Mont(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Insert Balance From Previous Month', NULL, :vSQLERRM);

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_BALANCEPAYMENTTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYMENT' not found */
        /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
        INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                SITES,
                TEAM,
                CCONAME,
                GEID,
                DEALCODE,
                AGENCY,
                CURRENCY,
                ALLGROUPS,
                RMDISPLAY,
                RMVALUE
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
                '58' AS reportcode,
                '06' AS sectionid,
                'DETAIL' AS sectionname,
                '01' AS sortorder,
                pad.firstname AS empfirstname,
                pad.lastname AS emplastname,
                pad.reporttitle AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                pad.positionga3 AS SITES,
                pad.positionga1 AS TEAM,
                pad.fullname AS CCONAME,
                pad.positionname AS GEID,
                pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                (
                    CASE
                        WHEN pad.positionga3 = 'CSSIN'
                        OR pad.positionga3 = 'CCO TV'
                        OR pad.positionga3 IS NULL
                        THEN 'S$'
                        WHEN pad.positionga3 = 'CSKCC'
                        OR pad.positionga3 = 'CSMCC'
                        THEN 'RM'
                    END
                ) AS CURRENCY,
                'Balance From Previous Month' AS ALLGROUPS,
                NULL AS RMDISPLAY,
                IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                (
                    SELECT   /* ORIGSQL: (select pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(...) */
                        pay.positionseq,
                        pay.payeeseq,
                        pay.processingunitseq,
                        pay.periodseq,
                        SUM(bal.value) AS RMVALUE
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
                        AND BAL.PAYEESEQ = PAY.PAYEESEQ
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
                AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
            WHERE
                pad.reportgroup = 'CCO TV';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* Business confirmed to leave it as blank for Clawback
        prc_logevent (:vPeriodRow.name,vProcName,'Begin Clawback',NULL,vsqlerrm);  -- Need to check with callidus team
        
        INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
            SITES,
            TEAM,
            CCONAME,
            GEID,
            DEALCODE,
            AGENCY,
            ALLGROUPS,
            RMDISPLAY,
            RMVALUE
          )
          SELECT
        vTenantID,
        pad.positionseq,
        pad.payeeseq,
        :vProcessingUnitRow.processingunitseq,
        vperiodseq,
        :vPeriodRow.name,
        :vProcessingUnitRow.name,
        :vCalendarRow.name,
        '58' reportcode,
        '01' sectionid,
        'DETAIL' sectionname,
        '01' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        :vPeriodRow.startdate,
        :vPeriodRow.enddate,
        SYSDATE,
        pad.positionga3 SITES,
        pad.positionga1 TEAM,
        pad.fullname CCONAME,
        pad.userid GEID,
        NULL DEALCODE,                 -- Business confirmed to leave it Blank
        pad.positionga9 AGENCY,
        NULL ALLGROUPS,
        'Clawback' RMDISPLAY,
        RMVALUE
          FROM   rpt_base_padimension pad,
          (
            select mes.positionseq,
                               mes.payeeseq,
                               mes.processingunitseq,
                               mes.periodseq,
                               sum(mes.value) RMVALUE
                        from rpt_base_credit mes
                        where mes.processingunitseq = vprocessingunitseq
             and mes.periodseq = vperiodseq
             and mes.value < 0
                        group by mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq
           )mes
        WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         AND pad.periodseq = mes.periodseq
         AND pad.reportgroup = 'CCO TV';
        COMMIT;
        */

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Insert Adjustment RM ',NULL,vsqle(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Insert Adjustment RM ', NULL, :vSQLERRM);  

        /* ORIGSQL: INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, (...) */
        INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                SITES,
                TEAM,
                CCONAME,
                GEID,
                DEALCODE,
                AGENCY,
                CURRENCY,
                ALLGROUPS,
                RMDISPLAY,
                RMVALUE
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
                '58' AS reportcode,
                '07' AS sectionid,
                'DETAIL' AS sectionname,
                '01' AS sortorder,
                pad.firstname AS empfirstname,
                pad.lastname AS emplastname,
                pad.reporttitle AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                pad.positionga3 AS SITES,
                pad.positionga1 AS TEAM,
                pad.fullname AS CCONAME,
                pad.positionname AS GEID,
                pad.PARTICIPANTGA3 AS DEALCODE /* -- zhu yun wanted to take it from participant ga3 --Business confirmed to leave it Blank */, pad.positionga9 AS AGENCY,
                (
                    CASE
                        WHEN pad.positionga3 = 'CSSIN'
                        OR pad.positionga3 = 'CCO TV'
                        OR pad.positionga3 IS NULL
                        THEN 'S$'
                        WHEN pad.positionga3 = 'CSKCC'
                        OR pad.positionga3 = 'CSMCC'
                        THEN 'RM'
                    END
                ) AS CURRENCY,
                'Adjustment RM' AS ALLGROUPS,
                NULL AS RMDISPLAY,
                IFNULL(RMVALUE,0)  /* ORIGSQL: nvl(RMVALUE,0) */
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                (
                    SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, SUM(...) */
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq,
                        SUM(mes.value) AS RMVALUE
                    FROM
                        TCMP.Cs_Credit mes
                    WHERE
                        mes.processingunitseq = :vprocessingunitseq
                        AND mes.periodseq = :vperiodseq
                        AND mes.credittypeseq = 16044073672507511
                        --nandini and mes.credittypeid = pkg_reporting_extract_r2.vcredittypeid_PayAdj
                    GROUP BY
                        mes.positionseq,
                        mes.payeeseq,
                        mes.processingunitseq,
                        mes.periodseq
                ) AS mes
            RIGHT OUTER JOIN
                EXT.rpt_base_padimension AS pad
                ON pad.payeeseq = mes.payeeseq  /* ORIGSQL: pad.payeeseq = mes.payeeseq(+) */
                AND pad.periodseq = mes.periodseq  /* ORIGSQL: pad.periodseq = mes.periodseq(+) */
                AND pad.positionseq = mes.positionseq  /* ORIGSQL: pad.positionseq = mes.positionseq(+) */
                AND pad.processingunitseq = mes.processingunitseq  /* ORIGSQL: pad.processingunitseq = mes.processingunitseq(+) */
            WHERE
                pad.reportgroup = 'CCO TV';

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Insert Monthy Actual Payout RM',N(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Insert Monthy Actual Payout RM', NULL, :vSQLERRM); 

        /* ORIGSQL: INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, processi(...) */
        INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                SITES, TEAM, CCONAME, GEID, DEALCODE, AGENCY,
                CURRENCY, ALLGROUPS, RMDISPLAY, RMVALUE
            )
            SELECT   /* ORIGSQL: SELECT tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname(...) */
                tenantid,
                positionseq,
                payeeseq,
                processingunitseq,
                periodseq,
                periodname,
                processingunitname,
                calendarname,
                reportcode,
                8 AS sectionid,
                sectionname,
                1 AS sortorder,
                empfirstname,
                emplastname,
                titlename,
                periodstartdate,
                periodenddate,
                loaddttm,
                SITES,
                TEAM,
                CCONAME,
                GEID,
                DEALCODE,
                AGENCY,
                CURRENCY,
                'Monthy Actual Payout' AS ALLGROUPS,
                NULL AS RMDISPLAY,
                SUM(rmvalue) AS RMVALUE
            FROM
                RPT_CCO_TV_PAYMENTSUMMARY
            WHERE
                processingunitseq = :vprocessingunitseq
                AND periodseq = :vperiodseq
                AND ALLGROUPS IN ('Monthy Basket Payout','Handing Fee','Balance From Previous Month','Clawback','Back Pay','Adjusment RM')
            GROUP BY
                tenantid, positionseq, payeeseq, processingunitseq, periodseq,
                periodname, processingunitname, calendarname, reportcode, sectionid,
                sectionname, sortorder, empfirstname, emplastname, titlename,
                periodstartdate, periodenddate, loaddttm,
                SITES, TEAM, CCONAME, GEID,
                DEALCODE, AGENCY,CURRENCY;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /*
         prc_logevent (:vPeriodRow.name,vProcName,'Begin Clawback',NULL,vsqlerrm); --CLAWBACK is empty
        INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
            SITES,
            TEAM,
            CCONAME,
            GEID,
            DEALCODE,
            AGENCY,
            CURRENCY,
            ALLGROUPS,
            RMDISPLAY,
            RMVALUE
          )
          SELECT
        vTenantID,
        pad.positionseq,
        pad.payeeseq,
        :vProcessingUnitRow.processingunitseq,
        vperiodseq,
        :vPeriodRow.name,
        :vProcessingUnitRow.name,
        :vCalendarRow.name,
        '58' reportcode,
        '01' sectionid,
        'DETAIL' sectionname,
        '01' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        :vPeriodRow.startdate,
        :vPeriodRow.enddate,
        SYSDATE,
        pad.positionga3 SITES,
        pad.positionga1 TEAM,
        pad.fullname CCONAME,
        pad.positionname GEID,
        NULL DEALCODE,                 -- Business confirmed to leave it Blank
        pad.positionga9 AGENCY,
        (case when pad.positionga3='CSSIN' then 'S$'
         when  pad.positionga3='CSKCC' or  pad.positionga3='CSMCC'  then 'RM' END) CURRENCY,
        'Clawback' ALLGROUPS,
        NULL RMDISPLAY,
        NULL RMVALUE
          FROM   rpt_base_padimension pad,
          (
            select mes.positionseq,
                               mes.payeeseq,
                               mes.processingunitseq,
                               mes.periodseq
                        from RPT_CCO_TV_PAYMENTSUMMARY mes
                        where mes.processingunitseq = vprocessingunitseq
             and mes.periodseq = vperiodseq
                        group by mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq
           )mes
        WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         AND pad.periodseq = mes.periodseq
         AND pad.reportgroup = 'CCO TV';
        COMMIT;
        
        
          prc_logevent (:vPeriodRow.name,vProcName,'Begin Back Pay',NULL,vsqlerrm); --Back Pay is empty
        INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
            SITES,
            TEAM,
            CCONAME,
            GEID,
            DEALCODE,
            AGENCY,
            CURRENCY,
            ALLGROUPS,
            RMDISPLAY,
            RMVALUE
          )
          SELECT
        vTenantID,
        pad.positionseq,
        pad.payeeseq,
        :vProcessingUnitRow.processingunitseq,
        vperiodseq,
        :vPeriodRow.name,
        :vProcessingUnitRow.name,
        :vCalendarRow.name,
        '58' reportcode,
        '01' sectionid,
        'DETAIL' sectionname,
        '01' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        :vPeriodRow.startdate,
        :vPeriodRow.enddate,
        SYSDATE,
        pad.positionga3 SITES,
        pad.positionga1 TEAM,
        pad.fullname CCONAME,
        pad.positionname GEID,
        NULL DEALCODE,                 -- Business confirmed to leave it Blank
        pad.positionga9 AGENCY,
        (case when pad.positionga3='CSSIN' then 'S$'
         when  pad.positionga3='CSKCC' or  pad.positionga3='CSMCC'  then 'RM' END) CURRENCY,
        'Back Pay' ALLGROUPS,
        NULL RMDISPLAY, --sudhir
        NULL RMVALUE
          FROM   rpt_base_padimension pad,
          (
            select mes.positionseq,
                               mes.payeeseq,
                               mes.processingunitseq,
                               mes.periodseq
                        from RPT_CCO_TV_PAYMENTSUMMARY mes
                        where mes.processingunitseq = vprocessingunitseq
             and mes.periodseq = vperiodseq
                        group by mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq
           )mes
        WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         AND pad.periodseq = mes.periodseq
         AND pad.reportgroup = 'CCO TV';
        COMMIT;
        
          prc_logevent (:vPeriodRow.name,vProcName,'Begin Remark',NULL,vsqlerrm); --Remark is empty
        INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
            SITES,
            TEAM,
            CCONAME,
            GEID,
            DEALCODE,
            AGENCY,
            CURRENCY,
            ALLGROUPS,
            RMDISPLAY,
            RMVALUE
          )
          SELECT
        vTenantID,
        pad.positionseq,
        pad.payeeseq,
        :vProcessingUnitRow.processingunitseq,
        vperiodseq,
        :vPeriodRow.name,
        :vProcessingUnitRow.name,
        :vCalendarRow.name,
        '58' reportcode,
        '01' sectionid,
        'DETAIL' sectionname,
        '01' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        :vPeriodRow.startdate,
        :vPeriodRow.enddate,
        SYSDATE,
        pad.positionga3 SITES,
        pad.positionga1 TEAM,
        pad.fullname CCONAME,
        pad.positionname GEID,
        NULL DEALCODE,                 -- Business confirmed to leave it Blank
        pad.positionga9 AGENCY,
        (case when pad.positionga3='CSSIN' then 'S$'
         when  pad.positionga3='CSKCC' or  pad.positionga3='CSMCC'  then 'RM' END) CURRENCY,
        'Remark' ALLGROUPS,
        NULL RMDISPLAY,   -- sudhir
        NULL RMVALUE
          FROM   rpt_base_padimension pad,
          (
            select mes.positionseq,
                               mes.payeeseq,
                               mes.processingunitseq,
                               mes.periodseq
                        from RPT_CCO_TV_PAYMENTSUMMARY mes
                        where mes.processingunitseq = vprocessingunitseq
             and mes.periodseq = vperiodseq
                        group by mes.positionseq,
                                 mes.payeeseq,
                                 mes.processingunitseq,
                                 mes.periodseq
           )mes
        WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         AND pad.periodseq = mes.periodseq
         AND pad.reportgroup = 'CCO TV';
        COMMIT;
        */

        /*
                prc_logevent (:vPeriodRow.name,vProcName,'Begin Currency',NULL,vsqlerrm); --Currency
          INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
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
                  SITES,
                  TEAM,
                  CCONAME,
                  GEID,
                  DEALCODE,
                  AGENCY,
                  ALLGROUPS,
                  RMDISPLAY,
                  RMVALUE
            )
            SELECT
              vTenantID,
              pad.positionseq,
              pad.payeeseq,
              :vProcessingUnitRow.processingunitseq,
              vperiodseq,
              :vPeriodRow.name,
              :vProcessingUnitRow.name,
              :vCalendarRow.name,
              '58' reportcode,
              '01' sectionid,
              'DETAIL' sectionname,
              '01' sortorder,
              pad.firstname empfirstname,
              pad.lastname emplastname,
              pad.reporttitle titlename,
              :vPeriodRow.startdate,
              :vPeriodRow.enddate,
              SYSDATE,
              pad.positionga3 SITES,
              pad.positionga1 TEAM,
              pad.fullname CCONAME,
              pad.userid GEID,
              NULL DEALCODE,                 -- Business confirmed to leave it Blank
              pad.positionga9 AGENCY,
              'Remark' ALLGROUPS,
              'RM' RMDISPLAY,
              (case when pad.positionga3='CSSIN' then 'S$'
               when  pad.positionga3='CSKCC' or  pad.positionga3='CSKCC'  then 'RM' END) RMVALUE
            FROM   rpt_base_padimension pad,
            (
                  select mes.positionseq,
                                     mes.payeeseq,
                                     mes.processingunitseq,
                                     mes.periodseq
                              from RPT_CCO_TV_PAYMENTSUMMARY mes
                              where mes.processingunitseq = vprocessingunitseq
             and mes.periodseq = vperiodseq
                              group by mes.positionseq,
                                       mes.payeeseq,
                                       mes.processingunitseq,
                                       mes.periodseq
             )mes
              WHERE       pad.payeeseq = mes.payeeseq
         AND pad.positionseq = mes.positionseq
         AND pad.processingunitseq = mes.processingunitseq
         AND pad.periodseq = mes.periodseq
         AND pad.reportgroup = 'CCO TV';
          COMMIT;*/

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Dummy Column Insert for Cross Tab(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Dummy Column Insert for Cross Tab', NULL, :vSQLERRM);

        /* ORIGSQL: Open c_sites; */
        OPEN c_sites;

        /* ORIGSQL: Loop */
        LOOP 
            /* ORIGSQL: fetch c_sites into v_sites; */
            FETCH c_sites INTO v_sites;

            /* ORIGSQL: EXIT WHEN c_sites%NOTFOUND */
            IF c_sites::NOTFOUND  
            THEN
                BREAK;
            END IF;

            /* ORIGSQL: open c_nullinsert; */
            OPEN c_nullinsert;

            /* ORIGSQL: loop */
            LOOP 
                /* ORIGSQL: fetch c_nullinsert into v_nullinsert; */
                FETCH c_nullinsert INTO v_nullinsert;

                /* ORIGSQL: EXIT WHEN c_nullinsert%NOTFOUND */
                IF c_nullinsert::NOTFOUND  
                THEN
                    BREAK;
                END IF;

                cnt = 0;

                /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_CCO_TV_PAYMENTSUMMARY' not found */

                SELECT
                    COUNT(*)
                INTO
                    cnt
                FROM
                    RPT_CCO_TV_PAYMENTSUMMARY rpt
                WHERE
                    rpt.processingunitseq = :vprocessingunitseq
                    AND rpt.periodseq = :vperiodseq
                    AND rpt.allgroups = 'No. of Orders'
                    AND rpt.sites = :v_sites
                    AND rpt.rmdisplay = :v_nullinsert;

                IF :cnt = 0
                THEN  
                    /* ORIGSQL: insert into RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, processi(...) */
                    /* Deepan : Commenting this out and replacing the entire insert, since rowid is not supported*/
                    -- INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY
                    --     (
                    --         tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                    --         processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                    --         empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                    --         SITES, TEAM, CCONAME, GEID, DEALCODE, AGENCY,
                    --         CURRENCY, ALLGROUPS, RMDISPLAY, RMVALUE
                    --     )
                    --     SELECT   /* ORIGSQL: select tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname(...) */
                    --         tenantid,
                    --         positionseq,
                    --         payeeseq,
                    --         processingunitseq,
                    --         periodseq,
                    --         periodname,
                    --         processingunitname,
                    --         calendarname,
                    --         reportcode,
                    --         01 AS sectionid,
                    --         sectionname,
                    --         sortorder,
                    --         empfirstname,
                    --         emplastname,
                    --         titlename,
                    --         periodstartdate,
                    --         periodenddate,
                    --         loaddttm,
                    --         SITES,
                    --         TEAM,
                    --         CCONAME,
                    --         GEID,
                    --         DEALCODE,
                    --         AGENCY,
                    --         CURRENCY,
                    --         'No. of Orders' AS ALLGROUPS,
                    --         :v_nullinsert AS RMDISPLAY,
                    --         NULL AS RMVALUE
                    --     FROM
                    --         RPT_CCO_TV_PAYMENTSUMMARY
                    --     WHERE
                    --         processingunitseq = :vprocessingunitseq
                    --         AND periodseq = :vperiodseq
                    --         AND "rowid" IN  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                    --                         /* ORIGSQL: rowid */
                    --         (
                    --             SELECT   /* ORIGSQL: (select MAX(rowid) from RPT_CCO_TV_PAYMENTSUMMARY where sites = v(...) */
                    --                 MAX("rowid")   /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                    --             FROM
                    --                 RPT_CCO_TV_PAYMENTSUMMARY
                    --             WHERE
                    --                 sites = :v_sites
                    --         );
                    
                    
                 /* Deepan : New logic without the rowid */
					INSERT INTO EXT.RPT_CCO_TV_PAYMENTSUMMARY
					                        (
                            tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                            processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                            empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                            SITES, TEAM, CCONAME, GEID, DEALCODE, AGENCY,
                            CURRENCY, ALLGROUPS, RMDISPLAY, RMVALUE
                        )
					WITH RankedData AS (
					    SELECT 
					        tenantid,
					        positionseq,
					        payeeseq,
					        processingunitseq,
					        periodseq,
					        periodname,
					        processingunitname,
					        calendarname,
					        reportcode,
					        01 AS sectionid,
					        sectionname,
					        sortorder,
					        empfirstname,
					        emplastname,
					        titlename,
					        periodstartdate,
					        periodenddate,
					        loaddttm,
					        SITES,
					        TEAM,
					        CCONAME,
					        GEID,
					        DEALCODE,
					        AGENCY,
					        CURRENCY,
					        'No. of Orders' AS ALLGROUPS,
					        :v_nullinsert AS RMDISPLAY,
					        NULL AS RMVALUE,
					        ROW_NUMBER() OVER (PARTITION BY sites ORDER BY loaddttm DESC) AS rn
					    FROM
					        EXT.RPT_CCO_TV_PAYMENTSUMMARY
					    WHERE
					        processingunitseq = :vprocessingunitseq
					        AND periodseq = :vperiodseq
					)
					
					SELECT 
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
					    SITES,
					    TEAM,
					    CCONAME,
					    GEID,
					    DEALCODE,
					    AGENCY,
					    CURRENCY,
					    ALLGROUPS,
					    RMDISPLAY,
					    RMVALUE
					    
					FROM
					    RankedData
					WHERE
					    rn = 1;


                    --and ALLGROUPS in ('Back Pay','Clawback','Remark');

                    /* ORIGSQL: commit; */
                    COMMIT;
                END IF;

                cnt = 0; 

                SELECT
                    COUNT(*)
                INTO
                    cnt
                FROM
                    RPT_CCO_TV_PAYMENTSUMMARY rpt
                WHERE
                    rpt.processingunitseq = :vprocessingunitseq
                    AND rpt.periodseq = :vperiodseq
                    AND rpt.allgroups = 'ARPU'
                    AND rpt.sites = :v_sites
                    AND rpt.rmdisplay = :v_nullinsert;

                IF :cnt = 0
                THEN  
                    /* ORIGSQL: insert into RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, processi(...) */
                    INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY
                        (
                            tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                            processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                            empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                            SITES, TEAM, CCONAME, GEID, DEALCODE, AGENCY,
                            CURRENCY, ALLGROUPS, RMDISPLAY, RMVALUE
                        )
                        
                        
                        /* Deepan : New logic without the rowid */
                        
                        WITH RankedData AS (
					    SELECT 
					        tenantid,
					        positionseq,
					        payeeseq,
					        processingunitseq,
					        periodseq,
					        periodname,
					        processingunitname,
					        calendarname,
					        reportcode,
					        03 AS sectionid,
					        sectionname,
					        sortorder,
					        empfirstname,
					        emplastname,
					        titlename,
					        periodstartdate,
					        periodenddate,
					        loaddttm,
					        SITES,
					        TEAM,
					        CCONAME,
					        GEID,
					        DEALCODE,
					        AGENCY,
					        CURRENCY,
					        'ARPU' AS ALLGROUPS,
					        :v_nullinsert AS RMDISPLAY,
					        0 AS RMVALUE,
					        ROW_NUMBER() OVER (PARTITION BY sites ORDER BY loaddttm DESC) AS rn
					    FROM
					        RPT_CCO_TV_PAYMENTSUMMARY
					    WHERE
					        processingunitseq = :vprocessingunitseq
					        AND periodseq = :vperiodseq
					)
					
					SELECT 
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
					    SITES,
					    TEAM,
					    CCONAME,
					    GEID,
					    DEALCODE,
					    AGENCY,
					    CURRENCY,
					    ALLGROUPS,
					    RMDISPLAY,
					    RMVALUE
					FROM
					    RankedData
					WHERE
					    rn = 1;

                        -- SELECT   /* ORIGSQL: select tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname(...) */
                        --     tenantid,
                        --     positionseq,
                        --     payeeseq,
                        --     processingunitseq,
                        --     periodseq,
                        --     periodname,
                        --     processingunitname,
                        --     calendarname,
                        --     reportcode,
                        --     03 AS sectionid,
                        --     sectionname,
                        --     sortorder,
                        --     empfirstname,
                        --     emplastname,
                        --     titlename,
                        --     periodstartdate,
                        --     periodenddate,
                        --     loaddttm,
                        --     SITES,
                        --     TEAM,
                        --     CCONAME,
                        --     GEID,
                        --     DEALCODE,
                        --     AGENCY,
                        --     CURRENCY,
                        --     'ARPU' AS ALLGROUPS,
                        --     :v_nullinsert AS RMDISPLAY,
                        --     0 AS RMVALUE
                        -- FROM
                        --     RPT_CCO_TV_PAYMENTSUMMARY
                        -- WHERE
                        --     processingunitseq = :vprocessingunitseq
                        --     AND periodseq = :vperiodseq
                        --     AND "rowid" IN  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                        --                     /* ORIGSQL: rowid */
                        --     (
                        --         SELECT   /* ORIGSQL: (select MAX(rowid) from RPT_CCO_TV_PAYMENTSUMMARY where sites = v(...) */
                        --             MAX("rowid")   /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                        --         FROM
                        --             RPT_CCO_TV_PAYMENTSUMMARY
                        --         WHERE
                        --             sites = :v_sites
                        --     );

                    --and ALLGROUPS in ('Back Pay','Clawback','Remark');

                    /* ORIGSQL: commit; */
                    COMMIT;
                END IF;
            END LOOP;  /* ORIGSQL: end loop; */

            /* ORIGSQL: close c_nullinsert; */
            CLOSE c_nullinsert;

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

                cnt1 = 0; 

                SELECT
                    COUNT(*)
                INTO
                    cnt1
                FROM
                    RPT_CCO_TV_PAYMENTSUMMARY rpt
                WHERE
                    rpt.processingunitseq = :vprocessingunitseq
                    AND rpt.periodseq = :vperiodseq
                    AND rpt.allgroups = :v_nullingroup.rptcolumnname
                    AND rpt.sites = :v_sites;

                IF :cnt1 = 0
                THEN  
                    /* ORIGSQL: insert into RPT_CCO_TV_PAYMENTSUMMARY (tenantid, positionseq, payeeseq, processi(...) */
                    INSERT INTO RPT_CCO_TV_PAYMENTSUMMARY
                        (
                            tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                            processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                            empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                            SITES, TEAM, CCONAME, GEID, DEALCODE, AGENCY,
                            CURRENCY, ALLGROUPS, RMDISPLAY, RMVALUE
                        )
                        
                        /* Deepan : New logic without the rowid */
					   WITH RankedData AS (
					    SELECT 
					        tenantid,
					        positionseq,
					        payeeseq,
					        processingunitseq,
					        periodseq,
					        periodname,
					        processingunitname,
					        calendarname,
					        reportcode,
					        :v_nullingroup.columnname AS sectionid,
					        sectionname,
					        sortorder,
					        empfirstname,
					        emplastname,
					        titlename,
					        periodstartdate,
					        periodenddate,
					        loaddttm,
					        SITES,
					        TEAM,
					        CCONAME,
					        GEID,
					        DEALCODE,
					        AGENCY,
					        CURRENCY,
					        :v_nullingroup.rptcolumnname AS ALLGROUPS,
					        NULL AS RMDISPLAY,
					        0 AS RMVALUE,
					        ROW_NUMBER() OVER (PARTITION BY sites ORDER BY loaddttm DESC) AS rn
					    FROM
					        RPT_CCO_TV_PAYMENTSUMMARY
					    WHERE
					        processingunitseq = :vprocessingunitseq
					        AND periodseq = :vperiodseq
					)
					
					SELECT 
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
					    SITES,
					    TEAM,
					    CCONAME,
					    GEID,
					    DEALCODE,
					    AGENCY,
					    CURRENCY,
					    ALLGROUPS,
					    RMDISPLAY,
					    RMVALUE
					FROM
					    RankedData
					WHERE
					    rn = 1
					    AND sites = :v_sites;

                        
                        
                        -- SELECT   /* ORIGSQL: select tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname(...) */
                        --     tenantid,
                        --     positionseq,
                        --     payeeseq,
                        --     processingunitseq,
                        --     periodseq,
                        --     periodname,
                        --     processingunitname,
                        --     calendarname,
                        --     reportcode,
                        --     :v_nullingroup.columnname AS sectionid,
                        --     sectionname,
                        --     sortorder,
                        --     empfirstname,
                        --     emplastname,
                        --     titlename,
                        --     periodstartdate,
                        --     periodenddate,
                        --     loaddttm,
                        --     SITES,
                        --     TEAM,
                        --     CCONAME,
                        --     GEID,
                        --     DEALCODE,
                        --     AGENCY,
                        --     CURRENCY,
                        --     :v_nullingroup.rptcolumnname AS ALLGROUPS,
                        --     NULL AS RMDISPLAY,
                        --     0 AS RMVALUE
                        -- FROM
                        --     RPT_CCO_TV_PAYMENTSUMMARY
                        -- WHERE
                        --     processingunitseq = :vprocessingunitseq
                        --     AND periodseq = :vperiodseq
                        --     AND "rowid" IN  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                        --                     /* ORIGSQL: rowid */
                        --     (
                        --         SELECT   /* ORIGSQL: (select MAX(rowid) from RPT_CCO_TV_PAYMENTSUMMARY where sites = v(...) */
                        --             MAX("rowid")   /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                        --         FROM
                        --             RPT_CCO_TV_PAYMENTSUMMARY
                        --         WHERE
                        --             sites = :v_sites
                        --     );

                    --and ALLGROUPS in ('Back Pay','Clawback','Remark');

                    /* ORIGSQL: commit; */
                    COMMIT;
                END IF;
            END LOOP;  /* ORIGSQL: end loop; */

            /* ORIGSQL: close c_nullingroups; */
            CLOSE c_nullingroups;
        END LOOP;  /* ORIGSQL: end loop; */

        /* ORIGSQL: close c_sites; */
        CLOSE c_sites;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'inserting No Of Staffs count',NULL,vsql(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'inserting No Of Staffs count', NULL, :vSQLERRM); 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_CCO_TV_PAYMENTSUMMARY RCC USING (SELECT team, sites, COUNT(*) AS (...) */
        MERGE INTO RPT_CCO_TV_PAYMENTSUMMARY AS RCC
            USING --- inserting CCONAME count based on team
            (
                SELECT   /* ORIGSQL: (SELECT team,sites, COUNT(*) cnt FROM (SELECT DISTINCT sites, cconame, team FROM(...) */
                    team,
                    sites,
                    COUNT(*) AS cnt  
                FROM
                    (
                        SELECT   /* ORIGSQL: (SELECT DISTINCT sites,cconame,team FROM RPT_CCO_TV_PAYMENTSUMMARY where periods(...) */
                            DISTINCT
                            sites,
                            cconame,
                            team
                        FROM
                            RPT_CCO_TV_PAYMENTSUMMARY
                        WHERE
                            periodseq = :vperiodseq
                    ) AS dbmtk_corrname_5404
                GROUP BY
                    team,sites
            ) AS src
            ON (RCC.team = src.team
                AND RCC.sites = src.sites
                AND RCC.processingunitseq = :vprocessingunitseq
            AND RCC.periodseq = :vperiodseq)
        WHEN MATCHED THEN
            UPDATE SET RCC.teamcount = src.cnt;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'inserting CCO team count',NULL,vsqlerrm(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'inserting CCO team count', NULL, :vSQLERRM); 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_CCO_TV_PAYMENTSUMMARY RCC USING (SELECT sites, COUNT(*) AS ccocnt(...) */
        MERGE INTO RPT_CCO_TV_PAYMENTSUMMARY AS RCC
            USING --- inserting CCONAME count based on sites
            (
                SELECT   /* ORIGSQL: (SELECT sites,COUNT(*) ccocnt FROM (SELECT DISTINCT cconame, team, sites FROM RP(...) */
                    sites,
                    COUNT(*) AS ccocnt  
                FROM
                    (
                        SELECT   /* ORIGSQL: (SELECT DISTINCT cconame,team,sites FROM RPT_CCO_TV_PAYMENTSUMMARY where periods(...) */
                            DISTINCT
                            cconame,
                            team,
                            sites
                        FROM
                            RPT_CCO_TV_PAYMENTSUMMARY
                        WHERE
                            periodseq = :vperiodseq
                    ) AS dbmtk_corrname_5407
                GROUP BY
                    sites
            ) AS src
            ON (RCC.sites = src.sites
                AND RCC.processingunitseq = :vprocessingunitseq
            AND RCC.periodseq = :vperiodseq)
        WHEN MATCHED THEN
            UPDATE SET RCC.CCONAMECOUNT = src.ccocnt;

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