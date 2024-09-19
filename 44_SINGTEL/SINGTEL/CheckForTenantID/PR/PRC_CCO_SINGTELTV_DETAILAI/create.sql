CREATE PROCEDURE EXT.PRC_CCO_SINGTELTV_DETAILAI
(
    IN vperiodseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_CCO_SINGTELTV_DETAILAI.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    IN vprocessingunitseq BIGINT,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_CCO_SINGTELTV_DETAILAI.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    IN vcalendarseq BIGINT      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_CCO_SINGTELTV_DETAILAI.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    -- DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();
    -- DECLARE PKG_REPORTING_EXTRACT_R2__vcredittypeid_TV VARCHAR(255); /* package/session variable */
    -- DECLARE PKG_REPORTING_EXTRACT_R2__vcredittypeid_HandFee VARCHAR(255); /* package/session variable */

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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_CCO_SINGTELTV_DETAILAI');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_CCO_SINGTELTV_DETAILAI') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_CCO_SINGTELTV_DETAILAI';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_CCO_SINGTELTV_DETAILAI'; */
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
     DECLARE cEndofTime date;
    DECLARE vcredittypeid_TV VARCHAR(255); /* package/session variable */
    DECLARE vcredittypeid_HandFee VARCHAR(255); /* package/session variable */

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
        SELECT SESSION_CONTEXT('VCREDITTYPEID_TV') INTO vcredittypeid_TV FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('VCREDITTYPEID_HANDFEE') INTO vcredittypeid_HandFee FROM SYS.DUMMY ;
        /* retrieve the package/session variables referenced in this procedure */

        
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
        --     );

        --------Find subpartition name------------------------------------------------------------------------------------

        -- vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName);  /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

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

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CLASSIFICATION' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CLASSIFIER' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PRODUCT' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_SALESTRANSACTION' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_CREDIT' not found */
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_CCO_SINGTELTV_DETAILAI' not found */

        /* ORIGSQL: INSERT INTO EXT.RPT_CCO_SINGTELTV_DETAILAI (tenantid, positionseq, payeeseq,(...) */
        /* RESOLVE: Identifier not found: Table/view 'EXT.RPT_BASE_PADIMENSION' not found */
        INSERT INTO EXT.RPT_CCO_SINGTELTV_DETAILAI
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
                ORDERACTIONID,
                ORDERID,
                ORDERTYPE,
                SVCNO,
                VENDORCODE,
                DEALERCODE,
                SALESMANCODE,
                MIOIND,
                BIZPRODUCTGROUP,
                COMPID,
                COMPDESC,
                ORDERLINETYPE,
                COMPONENTSTATUS,
                PREVIOUSDATAINDICATOR,
                SEQUENCENO,
                SUBSCRIPTION,
                PACKAGEID,
                PACKAGEDESC,
                SVCEFFDATE,
                ORDERCLOSEDDATE,
                ORDERCREATIONDATE,
                CUSTREQDATE,
                REMARKS,
                LISTPRICE,
                ARPU,
                SVCAI,
                CONTENTAI,
                BUNDLETYPE,
                SALESTRANSACTIONSEQ
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
                '56' AS reportcode,
                '01' AS sectionid,
                'DETAIL' AS sectionname,
                '01' AS sortorder,
                pad.firstname AS empfirstname,
                pad.lastname AS emplastname,
                pad.reporttitle AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                txncrd.orderactionid,
                txncrd.orderid,
                txncrd.ordertype,
                txncrd.svcno,
                txncrd.vendorcode,
                txncrd.dealercode,
                txncrd.salesmancode,
                NULL AS mioind,/* --NA */ NULL AS bizproductgroup,/* --NA */  txncrd.compid,
                clas.DESCRIPTION AS compdesc,
                txncrd.orderlinetype,
                txncrd.componentstatus,
                txncrd.previousdataindicator,
                txncrd.sequenceno,
                txncrd.subscription,
                NULL AS packageID,/* --NA */ NULL AS packageDesc,/* --NA */  txncrd.svceffdate,
                txncrd.ordercloseddate,
                txncrd.ordercreationdate,
                txncrd.custreqdate,
                NULL AS remarks,/* --NA */  txncrd.listprice,
                txncrd.ARPU,
                NULL AS svcai,/* --Keep Blank */  txncrd.contentai,
                txncrd.bundletype,
                txncrd.SALESTRANSACTIONSEQ
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                (
                    SELECT   /* ORIGSQL: (select distinct clss.DESCRIPTION,cls.SALESTRANSACTIONSEQ from cs_classification(...) */
                        DISTINCT
                        clss.DESCRIPTION,
                        cls.SALESTRANSACTIONSEQ
                    FROM
                        cs_classification cls,
                        cs_classifier clss,
                        cs_product prd
                    WHERE
                        cls.CLASSIFIERSEQ = clss.CLASSIFIERSEQ
                        AND cls.periodseq = :vperiodseq
                        AND cls.processingunitseq = :vprocessingunitseq
                        AND prd.classifierseq = clss.CLASSIFIERSEQ
                ) AS clas
            RIGHT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (SELECT st.salestransactionseq, cr.payeeseq, cr.positionseq, cr.periodseq, cr.pr(...) */
                        st.salestransactionseq,
                        cr.payeeseq,
                        cr.positionseq,
                        cr.periodseq,
                        cr.processingunitseq,
                        st.TRNSASSIGNPOSITIONNAME,
                        st.GENERICATTRIBUTE6 AS orderactionid,
                        st.ALTERNATEORDERNUMBER AS orderid,
                        st.GENERICATTRIBUTE10 AS ordertype,
                        st.CONTACT AS svcno,
                        st.GENERICATTRIBUTE3 AS vendorcode,
                        st.GENERICATTRIBUTE4 AS dealercode,
                        st.GENERICATTRIBUTE2 AS salesmancode,
                        st.PRODUCTID AS compid,
                        st.GENERICATTRIBUTE9 AS orderlinetype,
                        st.GENERICATTRIBUTE22 AS componentstatus,
                        st.GENERICATTRIBUTE31 AS previousdataindicator,
                        st.STATE AS sequenceno,
                        st.CITY AS subscription,
                        st.GENERICDATE5 AS svceffdate,
                        st.COMPENSATIONDATE AS ordercloseddate,
                        st.ACCOUNTINGDATE AS ordercreationdate,
                        st.GENERICDATE1 AS custreqdate,
                        st.GENERICNUMBER4 AS listprice,
                        st.GENERICNUMBER3 AS ARPU,
                        cr.VALUE AS contentai,
                        st.GENERICATTRIBUTE12 AS bundletype,
                        cr.credittypeid AS credittypeid
                    FROM
                        rpt_base_salestransaction st
                    LEFT OUTER JOIN
                        rpt_base_credit cr
                        ON (st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                            AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                        )
                        AND cr.credittypeid = :vcredittypeid_TV  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_TV */
                        AND cr.periodseq = :vperiodseq
                        AND cr.processingunitseq = :vprocessingunitseq
                    WHERE
                        st.GENERICATTRIBUTE31 = 'N'
                ) AS txncrd
                ON txncrd.SALESTRANSACTIONSEQ = clas.SALESTRANSACTIONSEQ  /* ORIGSQL: txncrd.SALESTRANSACTIONSEQ=clas.SALESTRANSACTIONSEQ(+) */
            RIGHT OUTER JOIN
                EXT.rpt_base_padimension AS pad
                ON pad.positionname = txncrd.TRNSASSIGNPOSITIONNAME --and st.businessunitmap = pad.businessunitmap
                /* ORIGSQL: pad.positionname= txncrd.TRNSASSIGNPOSITIONNAME(+) */
            WHERE
                pad.reportgroup = 'CCO TV' /* and txncrd.periodseq = pad.periodseq
                 and txncrd.processingunitseq = pad.processingunitseq
                 and txncrd.payeeseq = pad.payeeseq
                 and txncrd.positionseq = pad.positionseq
                
                 and txncrd.previousdataindicator='N' */;--need to capture only N for previousdataindicator
        --and st.eventtypeid = pkg_reporting_extract_r2.veventtypeid_ccotv;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Insert completed',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Insert completed', NULL, NULL);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        --Comp Description

        /*  prc_logevent (:vPeriodRow.name,vProcName,'Begin Comp Description Merge',NULL,v_sql);
        
        MERGE INTO EXT.RPT_CCO_SINGTELTV_DETAILAI rpt using
        (
              select
                          st.PRODUCTDESCRIPTION COMPDESC
                          from rpt_base_salestransaction st
                          where st.processingunitseq = vprocessingunitseq
             and st.SALESTRANSACTIONSEQ in
                          (
                              select SALESTRANSACTIONSEQ from cs_classification
                              where CLASSIFIERSEQ in (select CLASSIFIERSEQ from cs_product)
                          )
            
         )st
           on (rpt.processingunitseq = st.processingunitseq
        )when matched then update set rpt.COMPDESC = st.COMPDESC;
        
        prc_logevent (:vPeriodRow.name,vProcName,'Completed Comp Description Merge',NULL,NULL);
        
        COMMIT; */
        --Handing Fee

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Begin Handing fee Merge',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin Handing fee Merge', NULL, :v_sql); 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO EXT.RPT_CCO_SINGTELTV_DETAILAI rpt using (SELECT cr.positionseq, (...) */
        MERGE INTO EXT.RPT_CCO_SINGTELTV_DETAILAI AS rpt  
            USING
            (
                SELECT   /* ORIGSQL: (select cr.positionseq, cr.payeeseq, cr.processingunitseq, cr.periodseq, st.GENE(...) */
                    cr.positionseq,
                    cr.payeeseq,
                    cr.processingunitseq,
                    cr.periodseq,
                    st.GENERICATTRIBUTE6,
                    st.salestransactionseq,
                    MAX(cr.value) AS HANDINGFEE
                FROM
                    rpt_base_salestransaction st,
                    rpt_base_credit cr
                WHERE
                    st.SALESTRANSACTIONSEQ = cr.SALESTRANSACTIONSEQ
                    AND st.PROCESSINGUNITSEQ = cr.PROCESSINGUNITSEQ
                    AND cr.periodseq = :vperiodseq
                    AND cr.credittypeid = :vcredittypeid_HandFee  /* ORIGSQL: pkg_reporting_extract_r2.vcredittypeid_HandFee */
                    AND st.processingunitseq = :vprocessingunitseq
                GROUP BY
                    cr.positionseq,
                    cr.payeeseq,
                    cr.processingunitseq,
                    cr.periodseq,
                    st.GENERICATTRIBUTE6,
                    st.salestransactionseq
            ) AS crtr
            ON (rpt.processingunitseq = crtr.processingunitseq
                AND rpt.periodseq = crtr.periodseq
                AND rpt.positionseq = crtr.positionseq
                AND rpt.salestransactionseq = crtr.salestransactionseq
                AND rpt.payeeseq = crtr.payeeseq
                AND rpt.ORDERACTIONID = crtr.GENERICATTRIBUTE6
            ) WHEN MATCHED THEN
            UPDATE SET rpt.HANDINGFEE = crtr.HANDINGFEE;

        /* ORIGSQL: prc_logevent (:vPeriodRow.name,vProcName,'Completed Handing fee Merge',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Completed Handing fee Merge', NULL, NULL);

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
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName);

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END