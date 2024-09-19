CREATE PROCEDURE EXT.PRC_BSC_QTR_PAY_SUM
(
    --IN vrptname rpt_mapping.reportname%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'rpt_mapping.reportname' not found (for %TYPE declaration) */
    IN vrptname BIGINT,
                                               /* RESOLVE: Datatype unresolved: Datatype (rpt_mapping.reportname%TYPE) not resolved for parameter 'PRC_BSC_QTR_PAY_SUM.vrptname' */
                                               /* ORIGSQL: vrptname IN rpt_mapping.reportname%TYPE */
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_BSC_QTR_PAY_SUM.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_BSC_QTR_PAY_SUM.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_BSC_QTR_PAY_SUM.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
   --DECLARE PKG_REPORTING_EXTRACT_R2__cEndofTime CONSTANT TIMESTAMP = EXT.f_dbmtk_constant__pkg_reporting_extract_r2__cEndofTime();
    --DECLARE PKG_REPORTING_EXTRACT_R2__vcredittypeid_PayAdj VARCHAR(255); /* package/session variable */

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
    DECLARE vProcName VARCHAR(30) = UPPER('PRC_BSC_QTR_PAY_SUM');  /* ORIGSQL: vProcName VARCHAR2(30) := UPPER('PRC_BSC_QTR_PAY_SUM') ; */
    DECLARE vSQLERRM VARCHAR(3900);  /* ORIGSQL: vSQLERRM VARCHAR2(3900); */
    DECLARE vTCSchemaName VARCHAR(30) = 'TCMP';  /* ORIGSQL: vTCSchemaName VARCHAR2(30) := 'TCMP'; */
    DECLARE vTCTemplateTable VARCHAR(30) = 'CS_CREDIT';  /* ORIGSQL: vTCTemplateTable VARCHAR2(30) := 'CS_CREDIT'; */
    DECLARE vRptTableName VARCHAR(30) = 'RPT_BSC_QTR_PAY_SUM';  /* ORIGSQL: vRptTableName VARCHAR2(30) := 'RPT_BSC_QTR_PAY_SUM'; */
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
    DECLARE vcredittypeid_PayAdj NVARCHAR(50);
    DECLARE cEndofTime CONSTANT date := to_date('2200-01-01','yyyy-mm-dd');

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
      /* initialize library variables, if not yet done */
        -- CALL EXT.PKG_REPORTING_EXTRACT_R2:init_session_global();/*Deepan : replacing with session variable*/
         SET  'vcredittypeid_PayAdj' = 'Payment Adjustment';

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
            );*/ --Sanjay Commenting out as partitions are not required.

        --------Find subpartition name------------------------------------------------------------------------------------

        --vSubPartitionName = EXT.OD_GETPERIODSUBPARTITIONNAME(:vExtUser, :vTenantId, :vprocessingunitseq, :vperiodseq, :vRptTableName); --Sanjay: commenting as subpartition are not required /* ORIGSQL: OD_getPeriodSubPartitionName (vExtUser, vTenantId, vProcessingUnitSeq, vPeriodSe(...) */

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName);--Sanjay: commenting as AnalyzeTableSubpartition are not required

        --------Truncate report table subpartition------------------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_TruncateTableSubpartition (vRptTableName, vSubparti(...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_TruncateTableSubpartition(:vRptTableName, :vSubPartitionName);Sanjay:commenting as truncateTableSubpartition are not required

        --------Gather stats on report table subpartition-----------------------------------------------------------------
        /* ORIGSQL: pkg_reporting_extract_r2.prc_AnalyzeTableSubpartition (vExtUser, vRptTableName, (...) */
        --CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTableSubpartition(:vExtUser, :vRptTableName, :vSubPartitionName); Sanjay:commenting as analyze is not required

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
        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin insert',NULL,vsqlerrm) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin insert', NULL, :vSQLERRM);
        v_sql = 'INSERT INTO EXT.RPT_BSC_QTR_PAY_SUM
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
            sortorder,
            empfirstname,
            emplastname,
            titlename,
            periodstartdate,
            periodenddate,
            loaddttm,
            id,
            geid,
            salesmancode,
            INDPER,
            TEAMPER,
            CONNCOUNTPER
        )
        SELECT   ''' ||IFNULL(:vTenantId,'')||''',
        pad.positionseq,
        pad.payeeseq,
        ' ||IFNULL(TO_VARCHAR(:vProcessingUnitRow.processingunitseq),'')||',
        ' ||IFNULL(:vperiodseq,'')||',
        ''' ||IFNULL(:vPeriodRow.name,'')||''',
        ''' ||IFNULL(:vProcessingUnitRow.name,'')||''',
        ''' ||IFNULL(:vCalendarRow.name,'')||''',
        ''52'' reportcode,
        ''1'' sectionid,
        ''DETAIL'' sectionname,
        ''1'' sortorder,
        pad.firstname empfirstname,
        pad.lastname emplastname,
        pad.reporttitle titlename,
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.startdate),'')||''',''DD-MON-YY''),
        TO_DATE(''' ||IFNULL(TO_VARCHAR(:vPeriodRow.enddate),'')||''',''DD-MON-YY''),
        SYSDATE,
        NULL, --ID
        pad.PARTICIPANTID, --GEID
        pad.PARTICIPANTGA1, -- SALESMANCODE
        (INDPER*100) INDPER,
        (TEAMPER*100) TEAMPER,
        (CONNCNTPER*100) CONNCNTPER
        FROM   ext.rpt_base_padimension pad,
        (
            select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            max(case when rmap.rptcolumnname = ''INDPER'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'INDPER'),'') ||' end) INDPER,
            max(case when rmap.rptcolumnname = ''TEAMPER'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TEAMPER'),'') ||' end) TEAMPER,
            max(case when rmap.rptcolumnname = ''CONNCNTPER'' then ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'CONNCNTPER'),'') ||' end) CONNCNTPER
            from rpt_base_measurement mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq
        )mes
        WHERE       pad.payeeseq = mes.payeeseq
        AND pad.positionseq = mes.positionseq
        AND pad.processingunitseq = mes.processingunitseq
        and pad.periodseq = mes.periodseq
        and pad.reportgroup = ''BSC''';  /* ORIGSQL: fungenericattribute(vrptname,'TEAMPER') */
                                         /* ORIGSQL: fungenericattribute(vrptname,'INDPER') */
                                         /* ORIGSQL: fungenericattribute(vrptname,'CONNCNTPER') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin completed', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        v_sql = 'MERGE INTO EXT.RPT_BSC_QTR_PAY_SUM rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            sum(case when rmap.rptcolumnname = ''TEAMPROPER'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'TEAMPROPER'),'') ||' end) TEAMPROPER,
            sum(case when rmap.rptcolumnname = ''OTC'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'OTC'),'') ||' end) OTC,
            sum(case when rmap.rptcolumnname = ''EARNEDCOMM'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'EARNEDCOMM'),'') ||' end) EARNEDCOMM,
            sum(case when rmap.rptcolumnname = ''ADVADJ'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'ADVADJ'),'') ||' end) ADVADJ
            from rpt_base_incentive mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and mes.periodseq = '||IFNULL(:vperiodseq,'')||'
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
        mes.periodseq)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
        and rpt.payeeseq = qtr.payeeseq)
        when matched then update set rpt.TEAMPROPER=(qtr.TEAMPROPER*100),rpt.OTC = qtr.OTC,rpt.EARNEDCOMM = qtr.EARNEDCOMM,rpt.ADV_PROT_COMMISSION = decode((qtr.ADVADJ*-1),0,null,(qtr.ADVADJ*-1))';  /* ORIGSQL: fungenericattribute(vrptname,'TEAMPROPER') */
                                                                                                                                                                                                                                                                                                       /* ORIGSQL: fungenericattribute(vrptname,'OTC') */
                                                                                                                                                                                                                                                                                                       /* ORIGSQL: fungenericattribute(vrptname,'EARNEDCOMM') */
                                                                                                                                                                                                                                                                                                       /* ORIGSQL: fungenericattribute(vrptname,'ADVADJ') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive Started',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive Started', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Incentive completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Incentive completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        v_sql = 'MERGE INTO EXT.RPT_BSC_QTR_PAY_SUM rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            sum(case when rmap.rptcolumnname = ''PAYMENTADJ'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'PAYMENTADJ'),'') ||' end) PAYMENTADJ
            from cs_incentive mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and mes.periodseq in
            (select mon.periodseq from cs_period mon, cs_period qtr
                where mon.removedate = ''01-JAN-2200''
                and mon.parentseq = qtr.periodseq
                and mon.removedate = qtr.removedate
                and mon.calendarseq = qtr.calendarseq
                and mon.parentseq in (select parentseq from cs_period
                    where periodseq = ' ||IFNULL(:vperiodseq,'')||'
                    and calendarseq = '||IFNULL(:vcalendarseq,'')||'
                and removedate = '''||'01-JAN-2200'||''')
                and mon.periodseq < ' ||IFNULL(:vperiodseq,'')||')
            group by mes.positionseq,
            mes.payeeseq,
        mes.processingunitseq)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.positionseq = qtr.positionseq
        and rpt.payeeseq = qtr.payeeseq)
        when matched then update set rpt.PAYMENT_ADJ = decode ((qtr.PAYMENTADJ*-1),0,null,(qtr.PAYMENTADJ*-1))';  /* ORIGSQL: fungenericattribute(vrptname,'PAYMENTADJ') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update incentive qtr Started',NULL,v_sq(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update incentive qtr Started', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update incentive qtr completed',NULL,v_(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update incentive qtr completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        v_sql = 'MERGE INTO EXT.RPT_BSC_QTR_PAY_SUM rpt using
        (select mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
            mes.periodseq,
            sum(case when rmap.rptcolumnname = ''PAYADJ'' then  ' ||IFNULL(EXT.FUNGENERICATTRIBUTE(:vrptname, 'PAYADJ'),'') ||' end) PAYADJ
            from rpt_base_deposit mes, rpt_mapping rmap
            where mes.name in rmap.rulename
            and rmap.reportname = ''' ||IFNULL(:vrptname,'')||'''
            and mes.processingunitseq = '||IFNULL(TO_VARCHAR(:vprocessingunitseq),'')||'
            and mes.periodseq =  '||IFNULL(:vperiodseq,'')||'
            group by mes.positionseq,
            mes.payeeseq,
            mes.processingunitseq,
        mes.periodseq)qtr
        on (rpt.processingunitseq = qtr.processingunitseq
            and rpt.periodseq = qtr.periodseq
            and rpt.positionseq = qtr.positionseq
        and rpt.payeeseq = qtr.payeeseq)
        when matched then update set rpt.PAYMENT_ADJUSTMENT=decode(qtr.PAYADJ,0,null,qtr.PAYADJ)';  /* ORIGSQL: fungenericattribute(vrptname,'PAYADJ') */

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Deposit Started',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Deposit Started', NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Deposit completed',NULL,v_sql) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Deposit completed', NULL, :v_sql);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_BSC_QTR_PAY_SUM rpt using (SELECT mes.positionseq, mes.payeeseq, (...) */
        MERGE INTO EXT.RPT_BSC_QTR_PAY_SUM AS rpt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_CREDIT' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select mes.positionseq, mes.payeeseq, mes.processingunitseq, mes.periodseq, MAX(...) */
                    mes.positionseq,
                    mes.payeeseq,
                    mes.processingunitseq,
                    mes.periodseq,
                    MAX(mes.genericattribute3) AS Remarks,
                    SUM(mes.value) AS AdjAmt
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
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
                AND rpt.payeeseq = qtr.payeeseq
            AND rpt.sectionid = '1')
        WHEN MATCHED THEN
            UPDATE SET rpt.remarks = qtr.remarks;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_BSC_QTR_PAY_SUM rpt using (SELECT pay.positionseq, pay.payeeseq, (...) */
        MERGE INTO EXT.RPT_BSC_QTR_PAY_SUM AS rpt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCE' not found */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCEPAYMENTTRACE' not found */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYMENT' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(...) */
                    pay.positionseq,
                    pay.payeeseq,
                    pay.processingunitseq,
                    pay.periodseq,
                    SUM(bal.value) AS TotalBalance
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
                    AND bal.positionseq = pay.positionseq
                    AND pay.periodseq = :vperiodseq
                    AND pay.processingunitseq = :vprocessingunitseq
                GROUP BY
                    pay.positionseq,
                    pay.payeeseq,
                    pay.processingunitseq,
                    pay.periodseq
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
            AND rpt.payeeseq = qtr.payeeseq)
        WHEN MATCHED THEN
            UPDATE SET rpt.PRIOR_BALANCE =MAP(qtr.TotalBalance, 0, NULL, qtr.TotalBalance);  /* ORIGSQL: decode(qtr.TotalBalance,0,null,qtr.TotalBalance) */

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_BSC_QTR_PAY_SUM rpt using (SELECT tadj.positionseq, tadj.payeeseq(...) */
        MERGE INTO EXT.RPT_BSC_QTR_PAY_SUM AS rpt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BSC_QTR_PAY_SUM' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select tadj.positionseq, tadj.payeeseq, tadj.processingunitseq, tadj.periodseq,(...) */
                    tadj.positionseq,
                    tadj.payeeseq,
                    tadj.processingunitseq,
                    tadj.periodseq,
                    SUM(IFNULL(PAYMENT_ADJ,0) +IFNULL(ADV_PROT_COMMISSION,0) +IFNULL(PAYMENT_ADJUSTMENT,0) +IFNULL(PRIOR_BALANCE,0)) AS totaladj  /* ORIGSQL: nvl(PRIOR_BALANCE,0) */
                                                                                                                                                  /* ORIGSQL: nvl(PAYMENT_ADJUSTMENT,0) */
                                                                                                                                                  /* ORIGSQL: nvl(PAYMENT_ADJ,0) */
                                                                                                                                                  /* ORIGSQL: nvl(ADV_PROT_COMMISSION,0) */
                FROM
                    RPT_BSC_QTR_PAY_SUM tadj
                WHERE
                    tadj.processingunitseq = :vprocessingunitseq
                    AND tadj.periodseq = :vperiodseq
                GROUP BY
                    tadj.positionseq,
                    tadj.payeeseq,
                    tadj.processingunitseq,
                    tadj.periodseq
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
            AND rpt.payeeseq = qtr.payeeseq)
        WHEN MATCHED THEN
            UPDATE SET rpt.TOTAL_ADJUSTMENT = qtr.totaladj;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update TOTAL ADJUSTMENT completed',NULL(...) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update TOTAL ADJUSTMENT completed', NULL, NULL); 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO RPT_BSC_QTR_PAY_SUM rpt using (SELECT pay.positionseq, pay.payeeseq, (...) */
        MERGE INTO EXT.RPT_BSC_QTR_PAY_SUM AS rpt 
            USING
            (
                SELECT   /* ORIGSQL: (select pay.positionseq, pay.payeeseq, pay.processingunitseq, pay.periodseq, SUM(...) */
                    pay.positionseq,
                    pay.payeeseq,
                    pay.processingunitseq,
                    pay.periodseq,
                    SUM(value) AS payamount
                FROM
                    cs_payment pay
                WHERE
                    pay.processingunitseq = :vprocessingunitseq
                    AND pay.periodseq = :vperiodseq
                GROUP BY
                    pay.positionseq,
                    pay.payeeseq,
                    pay.processingunitseq,
                    pay.periodseq
            ) AS qtr
            ON (rpt.processingunitseq = qtr.processingunitseq
                AND rpt.periodseq = qtr.periodseq
                AND rpt.positionseq = qtr.positionseq
            AND rpt.payeeseq = qtr.payeeseq)
        WHEN MATCHED THEN
            UPDATE SET rpt.PAYABLECOMM = qtr.payamount;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Update Payment completed',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Update Payment completed', NULL, NULL);

        /* ORIGSQL: prc_logevent (vPeriodRow.name,vProcName,'Begin TOTAL insert',NULL,NULL) */
        CALL EXT.PRC_LOGEVENT(:vPeriodRow.name, :vProcName, 'Begin TOTAL insert', NULL, NULL);

        /* RESOLVE: Identifier not found: Table/view 'STELEXT.RPT_BSC_QTR_PAY_SUM' not found */
        /* ORIGSQL: INSERT INTO stelext.RPT_BSC_QTR_PAY_SUM (tenantid, positionseq, payeeseq, proces(...) */
        INSERT INTO ext.RPT_BSC_QTR_PAY_SUM
            (
                tenantid, positionseq, payeeseq, processingunitseq, periodseq, periodname,
                processingunitname, calendarname, reportcode, sectionid, sectionname, sortorder,
                empfirstname, emplastname, titlename, periodstartdate, periodenddate, loaddttm,
                OTC, PAYABLECOMM
            )
            SELECT   /* ORIGSQL: SELECT vTenantID, null, null, vProcessingUnitRow.processingunitseq, vperiodseq, (...) */
                :vTenantId,
                NULL,
                NULL,
                :vProcessingUnitRow.processingunitseq,
                :vperiodseq,
                :vPeriodRow.name,
                :vProcessingUnitRow.name,
                :vCalendarRow.name,
                '52' AS reportcode,
                '1' AS sectionid,
                'TOTAL' AS sectionname,
                '2' AS sortorder,
                NULL AS empfirstname,
                NULL AS emplastname,
                'BSC Quaterly Payout Summary' AS titlename,
                :vPeriodRow.startdate,
                :vPeriodRow.enddate,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                rpt.OTC,
                rpt.PAYABLECOMM
            FROM
                (
                    SELECT   /* ORIGSQL: (select SUM(OTC) OTC, SUM(PAYABLECOMM) PAYABLECOMM from stelext.RPT_BSC_QTR_PAY_(...) */
                        SUM(OTC) AS OTC,
                        SUM(PAYABLECOMM) AS PAYABLECOMM
                    FROM
                        ext.RPT_BSC_QTR_PAY_SUM tab
                    WHERE
                        tab.processingunitseq = :vprocessingunitseq
                        AND tab.periodseq = :vperiodseq
                        AND tab.sectionname = 'DETAIL'
                ) AS rpt;

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: UPDATE stelext.RPT_BSC_QTR_PAY_SUM tab SET ID = (SELECT COUNT(DISTINCT payeeseq)(...) */
        UPDATE ext.RPT_BSC_QTR_PAY_SUM tab
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_PADIMENSION' not found */
            SET
            /* ORIGSQL: ID = */
            ID = (
                SELECT   /* ORIGSQL: (select COUNT(DISTINCT payeeseq) from rpt_base_padimension where processingunits(...) */
                    COUNT(DISTINCT payeeseq)
                FROM
                    ext.rpt_base_padimension
                WHERE
                    processingunitseq = :vprocessingunitseq
                    AND periodseq = :vperiodseq
                    AND reportgroup = 'BSC'
            )
        WHERE
            tab.processingunitseq = :vprocessingunitseq
            AND tab.periodseq = :vperiodseq
            AND tab.sectionname = 'TOTAL';

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
       -- CALL EXT.PKG_REPORTING_EXTRACT_R2__prc_AnalyzeTable(:vRptTableName); Sanjay:commenting as analyze is not required

        --------Turn off Parallel DML-------------------------------------------------------------------------------------

        /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
        /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
        /* ALTER SESSION DISABLE PARALLEL DML ; */
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END