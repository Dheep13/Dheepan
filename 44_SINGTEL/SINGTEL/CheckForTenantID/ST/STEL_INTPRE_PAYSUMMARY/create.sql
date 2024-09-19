CREATE PROCEDURE EXT.STEL_INTPRE_PAYSUMMARY
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

    v_ComponentName = 'STEL_INTPRE_PAYSUMMARY';

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

    /* ORIGSQL: STEL_CUSTOMERMASTER (IN_PERIODSEQ, IN_PROCESSINGUNITSEQ) */
    CALL EXT.STEL_CUSTOMERMASTER(:IN_PERIODSEQ, :IN_PROCESSINGUNITSEQ);

    --- Calling WEBI reports procedure

    --STEL_INTPRE_PAYDETLWEBI ('Test',IN_PERIODSEQ, IN_PROCESSINGUNITSEQ);

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_PRPAYSUMMARY WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSE(...) */
    DELETE
    FROM
        EXT.STEL_RPT_PRPAYSUMMARY
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: DELETE STEL_RPT_PAYSUMMARY_TMP WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNIT(...) */
    DELETE
    FROM
        EXT.STEL_RPT_PAYSUMMARY_TMP
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_PRPAYSUMMARY') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_PRPAYSUMMARY');

    -- managing table partitions

    -- Individual achievement

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER_IP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_PAYSUMMARY_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PER(...) */
    INSERT INTO EXT.STEL_RPT_PAYSUMMARY_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            GEID,
            STAFFNAME,
            TEAM,
            SUBTEAM,
            DESIGNATION,
            INDACHEV,
            TEAMACHEV,
            TEAMPERC,
            TOTALPERC,
            OTC,
            INDCOMM,
            TEAMCOMM,
            CEDUE,
            L2_IND_ACH,
            L2_IND_COMMISSION,
            SAA,
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
            T1.PAYEEID,
            T1.PAYEENAME,
            T1.TEAM AS TEAM,
            T1.SUBTEAM AS SUBTEAM,
            T1.TITLE AS DESIGNATION,
            CASE
                WHEN INC.GENERICATTRIBUTE1 IN
                ('Internal Prepaid - Metric 1')
                THEN INC.GENERICNUMBER3 * 100
                ELSE 0
            END
            AS INDACHEV,
            CASE
                WHEN INC.GENERICATTRIBUTE1 IN
                ('Internal Prepaid - Metric 2')
                THEN INC.GENERICNUMBER3 * 100
                ELSE 0
            END
            AS TEAMACHEV,
            CASE
                WHEN INC.GENERICATTRIBUTE1 IN
                ('Internal Prepaid - Metric 2')
                THEN INC.GENERICNUMBER3 * INC.GENERICNUMBER4 * 100
                ELSE 0
            END
            AS TEAMPERC,
            0 AS TOTALPERC,
            CASE
                WHEN INC.GENERICATTRIBUTE1 IN
                ('Internal Prepaid - Metric 1')
                THEN INC.GENERICNUMBER1
                ELSE 0
            END
            AS OTC,
            CASE
                WHEN INC.GENERICATTRIBUTE1 IN
                ('Internal Prepaid - Metric 1')
                THEN INC.VALUE
                ELSE 0
            END
            AS INDCOMM,
            CASE
                WHEN INC.GENERICATTRIBUTE1 IN
                ('Internal Prepaid - Metric 2')
                THEN INC.VALUE
                ELSE 0
            END
            AS TEAMCOMM,
            0 AS CEDUE,/* --Measurement mapping changed on 04.04.2019 */  0 AS L2_IND_ACH,
            0 AS L2_IND_COMMISSION,
            CASE
                WHEN INC.GENERICATTRIBUTE1 = 'Internal_SAA'
                THEN INC.VALUE
                ELSE 0
            END
            AS SAA,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_INCENTIVE INC
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = INC.PERIODSEQ
            AND T1.POSITIONSEQ = INC.POSITIONSEQ
            AND T1.PAYEESEQ = INC.PAYEESEQ
            AND T1.TITLE LIKE 'Prepaid%'
            AND INC.GENERICATTRIBUTE1 IN
            ('Internal Prepaid - Metric 1',
                'Internal Prepaid - Metric 2',
                'Internal Prepaid - Metric 3',
            'Internal_SAA')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- CEDUE 

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_PAYSUMMARY_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PER(...) */
    INSERT INTO EXT.STEL_RPT_PAYSUMMARY_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            GEID,
            STAFFNAME,
            TEAM,
            SUBTEAM,
            DESIGNATION,
            INDACHEV,
            TEAMACHEV,
            TEAMPERC,
            TOTALPERC,
            OTC,
            INDCOMM,
            TEAMCOMM,
            CEDUE,
            L2_IND_ACH,
            L2_IND_COMMISSION,
            SAA,
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
            T1.PAYEEID,
            T1.PAYEENAME,
            T1.TEAM AS TEAM,
            T1.SUBTEAM AS SUBTEAM,
            T1.TITLE AS DESIGNATION,
            0 AS INDACHEV,
            0 AS TEAMACHEV,
            0 AS TEAMPERC,
            0 AS TOTALPERC,
            0 AS OTC,
            0 AS INDCOMM,
            0 AS TEAMCOMM,
            INC.VALUE AS CEDUE,/* --Measurement mapping changed on 04.04.2019 */  0 AS L2_IND_ACH,
            0 AS L2_IND_COMMISSION,
            0,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_MEASUREMENT INC
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = INC.PERIODSEQ
            AND T1.POSITIONSEQ = INC.POSITIONSEQ
            AND T1.PAYEESEQ = INC.PAYEESEQ
            AND T1.TITLE LIKE 'Prepaid%'
            AND INC.NAME = 'SM_CSI_CE_Adjustment'
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Commission Adjustment 

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSIT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_PAYSUMMARY_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PER(...) */
    INSERT INTO EXT.STEL_RPT_PAYSUMMARY_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            GEID,
            STAFFNAME,
            TEAM,
            SUBTEAM,
            DESIGNATION,
            COMMADJ,
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
            T1.PAYEEID,
            T1.PAYEENAME,
            T1.TEAM AS TEAM,
            T1.SUBTEAM AS SUBTEAM,
            T1.TITLE AS DESIGNATION,
            DEP.VALUE AS COMMADJ,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_DEPOSIT DEP
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = DEP.PERIODSEQ
            AND T1.POSITIONSEQ = DEP.POSITIONSEQ
            AND T1.PAYEESEQ = DEP.PAYEESEQ
            AND T1.TITLE LIKE 'Prepaid%'
            AND DEP.NAME IN ('Deposit - Pay Adj')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Remark 

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_PAYSUMMARY_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PER(...) */
    INSERT INTO EXT.STEL_RPT_PAYSUMMARY_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            GEID,
            STAFFNAME,
            TEAM,
            SUBTEAM,
            DESIGNATION,
            REMARKS,
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
            T1.PAYEEID,
            T1.PAYEENAME,
            T1.TEAM AS TEAM,
            T1.SUBTEAM AS SUBTEAM,
            T1.TITLE AS DESIGNATION,
            IFNULL(TO_VARCHAR(CRD.GENERICATTRIBUTE1),'') || IFNULL(TO_VARCHAR(CRD.GENERICATTRIBUTE3),''),
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_CREDIT CRD,
            CS_CREDITTYPE CTYPE
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = CRD.PERIODSEQ
            AND T1.POSITIONSEQ = CRD.POSITIONSEQ
            AND T1.PAYEESEQ = CRD.PAYEESEQ
            AND CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND T1.TITLE LIKE 'Prepaid%'
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND CTYPE.CREDITTYPEID = 'Payment Adjustment'
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Final table insertion.
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_PAYSUMMARY_TMP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_PRPAYSUMMARY' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_PRPAYSUMMARY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIO(...) */
    INSERT INTO EXT.STEL_RPT_PRPAYSUMMARY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            GEID,
            STAFFNAME,
            TEAM,
            SUBTEAM,
            DESIGNATION,
            INDACHEV,
            TEAMACHEV,
            TEAMPERC,
            TOTALPERC,
            OTC,
            INDCOMM,
            TEAMCOMM,
            EARNEDCOMM,
            COMMADJ,
            CEDUE,
            PAYCOMM,
            REMARKS,
            L2_IND_ACH,
            L2_IND_COMMISSION,
            SAA,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIODNAME, PROCESSINGUNITNAME, (...) */
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            GEID,
            STAFFNAME,
            TEAM,
            SUBTEAM,
            DESIGNATION,
            SUM(INDACHEV),
            SUM(TEAMACHEV),
            SUM(TEAMPERC),
            SUM(TOTALPERC),
            SUM(OTC),
            SUM(INDCOMM),
            SUM(TEAMCOMM),
            SUM(EARNEDCOMM),
            SUM(COMMADJ),
            SUM(CEDUE),
            SUM(PAYCOMM),
            REMARKS,
            SUM(L2_IND_ACH),
            SUM(L2_IND_COMMISSION),
            SUM(SAA),
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_RPT_PAYSUMMARY_TMP T1
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
        GROUP BY
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            GEID,
            STAFFNAME,
            TEAM,
            SUBTEAM,
            DESIGNATION,
            REMARKS
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Deleting prepaid team and director from table. 

    /* ORIGSQL: DELETE STEL_RPT_PRPAYSUMMARY WHERE DESIGNATION IN ('Prepaid - Team','Prepaid - D(...) */
    DELETE
    FROM
        EXT.STEL_RPT_PRPAYSUMMARY
    WHERE
        DESIGNATION IN ('Prepaid - Team','Prepaid - Director');

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END