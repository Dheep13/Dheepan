CREATE PROCEDURE EXT.STEL_INTPRE_INDSUMMARY
(
    IN IN_RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: IN_RPTTYPE IN VARCHAR2 */
    IN IN_PERIODSEQ BIGINT,   /* ORIGSQL: IN_PERIODSEQ IN INTEGER */
    IN IN_PROCESSINGUNITSEQ BIGINT     /* ORIGSQL: IN_PROCESSINGUNITSEQ IN INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Tenant VARCHAR(10) = 'LGAP';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'LGAP'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'STEL_INTPRE_INDSUMMARY';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME,
        PERIODNAME
    INTO
        v_CalendarName,
        v_PeriodName
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_INDPAYSUMMARY WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITS(...) */
    DELETE
    FROM
        EXT.STEL_RPT_INDPAYSUMMARY
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_INDPAYSUMMARY') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_INDPAYSUMMARY');

    -- managing table partitions

    -- Individual achievement

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER_IP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_INDPAYSUMMARY' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_INDPAYSUMMARY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERI(...) */
    INSERT INTO EXT.STEL_RPT_INDPAYSUMMARY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPID,
            SALESREPTITLE,
            SECTIONNO,
            EVENTTYPE,
            PRODUCT,
            SUB,
            TARGET,
            ACTUAL,
            ACHIVED,
            ACTUALPTS,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
            T1.TENANTID,
            T1.PERIODSEQ,
            T1.PROCESSINGUNIT,
            T1.PERIODNAME,
            T1.PROCESSINGUNITNAME,
            :v_CalendarName,
            T1.POSITIONSEQ,
            T1.PAYEESEQ,
            T1.PAYEENAME,
            T1.PAYEEID,
            T1.TITLE,
            1 AS SECTIONNO,
            'INDIVIDUAL ACHIEVEMENT' AS EVENTTYPE,
            MES.GENERICATTRIBUTE2 AS PRODUCT,
            MES.GENERICATTRIBUTE4 AS SUB,
            MES.GENERICNUMBER2 AS TARGET,
            MES.GENERICNUMBER1 AS ACTUAL,
            MES.VALUE AS ACHIVED,
            MES.GENERICNUMBER1 AS ACTUALPTS,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_MEASUREMENT MES
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = MES.PERIODSEQ
            AND T1.POSITIONSEQ = MES.POSITIONSEQ
            AND T1.PAYEESEQ = MES.PAYEESEQ
            AND MES.GENERICATTRIBUTE3 <> 'Prepaid - Team'
            AND MES.NAME IN
            ('SM_Internal Prepaid_Hi Card+Tourist SIM_Achievement',
                'SM_Internal Prepaid_Weighted Product Achievement',
                'SM_Internal Prepaid_Phoenix Cards_Achievement',
            'SM_Internal Prepaid_Top Up_Achievement')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Team achievement   

    /* ORIGSQL: INSERT INTO STEL_RPT_INDPAYSUMMARY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERI(...) */
    INSERT INTO EXT.STEL_RPT_INDPAYSUMMARY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPID,
            SALESREPTITLE,
            SECTIONNO,
            EVENTTYPE,
            PRODUCT,
            SUB,
            TARGET,
            ACTUAL,
            ACHIVED,
            ACTUALPTS,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
            T1.TENANTID,
            T1.PERIODSEQ,
            T1.PROCESSINGUNIT,
            T1.PERIODNAME,
            T1.PROCESSINGUNITNAME,
            :v_CalendarName,
            T1.POSITIONSEQ,
            T1.PAYEESEQ,
            T1.PAYEENAME,
            T1.PAYEEID,
            T1.TITLE,
            2 AS SECTIONNO,
            'TEAM ACHIEVEMENT' AS EVENTTYPE,
            MES.GENERICATTRIBUTE2 AS PRODUCT,
            MES.GENERICATTRIBUTE4 AS SUB,
            MES.GENERICNUMBER2 AS TARGET,
            MES.GENERICNUMBER1 AS ACTUAL,
            MES.VALUE AS ACHIVED,
            MES.GENERICNUMBER1 AS ACTUALPTS,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_MEASUREMENT MES
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = MES.PERIODSEQ
            AND T1.POSITIONSEQ = MES.POSITIONSEQ
            AND MES.GENERICATTRIBUTE3 = 'Prepaid - Team'
            AND T1.PAYEESEQ = MES.PAYEESEQ
            AND MES.NAME IN
            ('SM_Internal Prepaid_Hi Card+Tourist SIM_Achievement',
                'SM_Internal Prepaid_Weighted Product Achievement',
                'SM_Internal Prepaid_Phoenix Cards_Achievement',
            'SM_Internal Prepaid_Top Up_Achievement')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Individual earning 

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_INDPAYSUMMARY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERI(...) */
    INSERT INTO EXT.STEL_RPT_INDPAYSUMMARY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPID,
            SALESREPTITLE,
            SECTIONNO,
            EVENTTYPE,
            PRODUCT,
            WEIGHT,
            WTACHIEVE,
            STANDARDSCORE,
            ProrateScore,
            MULTI,
            TGTMULTI,
            TOTAL,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
            T1.TENANTID,
            T1.PERIODSEQ,
            T1.PROCESSINGUNIT,
            T1.PERIODNAME,
            T1.PROCESSINGUNITNAME,
            :v_CalendarName,
            T1.POSITIONSEQ,
            T1.PAYEESEQ,
            T1.PAYEENAME,
            T1.PAYEEID,
            T1.TITLE,
            3 AS SECTIONNO,
            'EARNINGS' AS EVENTTYPE,
            'Individual Achievement' AS PRODUCT,
            MES.GENERICNUMBER1 AS WEIGHT,
            MES.GENERICNUMBER3 AS WTACHIEVE,
            NULL AS STANDARDSCORE,
            NULL AS ProrateScore,
            MES.GENERICNUMBER5 AS MULTI,
            MES.GENERICNUMBER6 AS TGTMULTI,
            MES.VALUE AS TOTAL,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_INCENTIVE MES
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = MES.PERIODSEQ
            AND T1.POSITIONSEQ = MES.POSITIONSEQ
            AND T1.PAYEESEQ = MES.PAYEESEQ
            AND MES.GENERICATTRIBUTE1 IN ('Internal Prepaid - Metric 1')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Team earning   

    /* ORIGSQL: INSERT INTO STEL_RPT_INDPAYSUMMARY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERI(...) */
    INSERT INTO EXT.STEL_RPT_INDPAYSUMMARY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPID,
            SALESREPTITLE,
            SECTIONNO,
            EVENTTYPE,
            PRODUCT,
            WEIGHT,
            WTACHIEVE,
            STANDARDSCORE,
            ProrateScore,
            MULTI,
            TGTMULTI,
            TOTAL,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
            T1.TENANTID,
            T1.PERIODSEQ,
            T1.PROCESSINGUNIT,
            T1.PERIODNAME,
            T1.PROCESSINGUNITNAME,
            :v_CalendarName,
            T1.POSITIONSEQ,
            T1.PAYEESEQ,
            T1.PAYEENAME,
            T1.PAYEEID,
            T1.TITLE,
            3 AS SECTIONNO,
            'EARNINGS' AS EVENTTYPE,
            'Team Achievement' AS PRODUCT,
            MES.GENERICNUMBER1 AS WEIGHT,
            MES.GENERICNUMBER3 AS WTACHIEVE,
            NULL AS STANDARDSCORE,
            MES.GENERICNUMBER4 AS ProrateScore,
            MES.GENERICNUMBER5 AS MULTI,
            MES.GENERICNUMBER6 AS TGTMULTI,
            MES.VALUE AS TOTAL,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_INCENTIVE MES
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = MES.PERIODSEQ
            AND T1.POSITIONSEQ = MES.POSITIONSEQ
            AND T1.PAYEESEQ = MES.PAYEESEQ
            AND MES.GENERICATTRIBUTE1 IN ('Internal Prepaid - Metric 2')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Commission Adjustment 

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSIT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_INDPAYSUMMARY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERI(...) */
    INSERT INTO EXT.STEL_RPT_INDPAYSUMMARY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPID,
            SALESREPTITLE,
            SECTIONNO,
            EVENTTYPE,
            PRODUCT,
            TOTAL,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
            T1.TENANTID,
            T1.PERIODSEQ,
            T1.PROCESSINGUNIT,
            T1.PERIODNAME,
            T1.PROCESSINGUNITNAME,
            :v_CalendarName,
            T1.POSITIONSEQ,
            T1.PAYEESEQ,
            T1.PAYEENAME,
            T1.PAYEEID,
            T1.TITLE,
            4 AS SECTIONNO,
            'Earned Commission' AS EVENTTYPE,
            'Commission Adjustment' AS PRODUCT,
            MES.VALUE AS TOTAL,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_DEPOSIT MES
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = MES.PERIODSEQ
            AND T1.POSITIONSEQ = MES.POSITIONSEQ
            AND T1.PAYEESEQ = MES.PAYEESEQ
            AND MES.GENERICATTRIBUTE1 IN ('Deposit - Pay Adj')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- CE/Compliance Adjustment due   

    /* ORIGSQL: INSERT INTO STEL_RPT_INDPAYSUMMARY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERI(...) */
    INSERT INTO EXT.STEL_RPT_INDPAYSUMMARY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPID,
            SALESREPTITLE,
            SECTIONNO,
            EVENTTYPE,
            PRODUCT,
            TOTAL,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
            T1.TENANTID,
            T1.PERIODSEQ,
            T1.PROCESSINGUNIT,
            T1.PERIODNAME,
            T1.PROCESSINGUNITNAME,
            :v_CalendarName,
            T1.POSITIONSEQ,
            T1.PAYEESEQ,
            T1.PAYEENAME,
            T1.PAYEEID,
            T1.TITLE,
            4 AS SECTIONNO,
            'Earned Commission' AS EVENTTYPE,
            'CE/Compliance Adjustment due' AS PRODUCT,
            MES.VALUE AS TOTAL,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_INCENTIVE MES
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = MES.PERIODSEQ
            AND T1.POSITIONSEQ = MES.POSITIONSEQ
            AND T1.PAYEESEQ = MES.PAYEESEQ
            AND MES.GENERICATTRIBUTE1 IN ('Internal Prepaid - Metric 3')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END