CREATE PROCEDURE EXT.STEL_INTPRE_PAYDETLWEBI
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

    v_ComponentName = 'STEL_INTPRE_PAYDETLWEBI';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIODCALENDAR' not found */

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
    /* ORIGSQL: DELETE STEL_RPT_PAYDETLWEBI WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSEQ(...) */
    DELETE
    FROM
        EXT.STEL_RPT_PAYDETLWEBI
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_PAYDETLWEBI') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'STEL_RPT_PAYDETLWEBI');

    -- managing table partitions

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_UNITTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_PAYDETLWEBI' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_PAYDETLWEBI (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIOD(...) */
    INSERT INTO EXT.STEL_RPT_PAYDETLWEBI
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            ORDERTYPE,
            ORDERID,
            TRANDATE,
            DLRCODE,
            DLRNAME,
            AMNAME,
            SMNAME,
            DIRNAME,
            TEAM,
            SUBTEAM,
            PRODNAME,
            PRODID,
            DATASOURCE,
            TRANVALUE,
            UNIT,
            CREATEDATE
        )
        
            SELECT   /* ORIGSQL: (SELECT CRD.TENANTID, IN_PERIODSEQ, IN_PROCESSINGUNITSEQ, PRD.NAME, v_CalendarNa(...) */
                CRD.TENANTID,
                :IN_PERIODSEQ,
                :IN_PROCESSINGUNITSEQ,
                PRD.NAME,
                :v_CalendarName AS CALENDARNAME,
                PRINFO.POSITIONSEQ,
                PRINFO.PAYEESEQ,
                EV.EVENTTYPEID AS ORDERTYPE,
                SRD.ORDERID,
                STR.COMPENSATIONDATE AS TRANDATE,
                STR.GENERICATTRIBUTE4 AS DEALERCODE,
                STR.GENERICATTRIBUTE1 AS DEALERNAME,
                PRINFO.AM_NAME,
                PRINFO.SMNAME,
                PRINFO.DIRNAME,
                POS.GENERICATTRIBUTE1 AS TEAM,
                POS.GENERICATTRIBUTE2 AS SUBTEAM,
                STR.PRODUCTNAME,
                STR.PRODUCTID,
                STR.DATASOURCE,
                STR.NUMBEROFUNITS,
                UT.NAME,
                CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            FROM
                cs_eventtype ev,
                cs_salestransaction str,
                cs_salesorder srd,
                cs_unittype ut,
                cs_credit crd,
                cs_position pos,
                CS_PERIOD PRD,
                (
                    SELECT   /* ORIGSQL: (SELECT DISTINCT PRD.PERIODSEQ, PART.PAYEESEQ, POS.RULEELEMENTOWNERSEQ AS POSITI(...) */
                        DISTINCT
                        PRD.PERIODSEQ,
                        PART.PAYEESEQ,
                        POS.RULEELEMENTOWNERSEQ AS POSITIONSEQ,
                        IFNULL(TO_VARCHAR(PYE.PAYEEID),'') || ' - ' || IFNULL(PART.LASTNAME,'') AS AM_NAME,
                        IFNULL(TO_VARCHAR(MGRPYE.PAYEEID),'') || ' -  ' || IFNULL(MGRPART.LASTNAME,'') AS SMNAME,
                        IFNULL(TO_VARCHAR(LAMPYE.PAYEEID),'') || ' -  ' || IFNULL(LAMPART.LASTNAME,'') AS DIRNAME
                    FROM
                        CS_PARTICIPANT PART,
                        CS_POSITION POS,
                        CS_PAYEE PYE,
                        CS_TITLE TL,
                        CS_POSITION MGRPOS,
                        CS_TITLE MGRTL,
                        CS_PAYEE MGRPYE,
                        CS_PARTICIPANT MGRPART,
                        CS_POSITION LAMPOS,
                        CS_TITLE LAMTL,
                        CS_PAYEE LAMPYE,
                        CS_PARTICIPANT LAMPART,
                        CS_PERIOD PRD
                    WHERE
                        PART.REMOVEDATE = to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND PRD.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND POS.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND PYE.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND TL.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND PYE.ISLAST = 1
                        AND PRD.PERIODSEQ = :IN_PERIODSEQ
                        AND PART.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND PART.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND PART.PAYEESEQ = POS.PAYEESEQ
                        AND POS.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND POS.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND POS.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                        AND POS.PAYEESEQ = PYE.PAYEESEQ
                        AND PYE.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND PYE.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND TL.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND TL.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND TL.RULEELEMENTOWNERSEQ = POS.TITLESEQ
                        ----- SM Details
                        AND MGRPART.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND MGRPOS.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND MGRPYE.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND MGRTL.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND MGRPART.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND MGRPART.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND MGRPART.PAYEESEQ = MGRPOS.PAYEESEQ
                        AND MGRPOS.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND MGRPOS.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND MGRPOS.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                        AND MGRPOS.PAYEESEQ = MGRPYE.PAYEESEQ
                        AND MGRPYE.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND MGRPYE.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND MGRTL.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND MGRTL.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND MGRTL.RULEELEMENTOWNERSEQ = MGRPOS.TITLESEQ
                        AND MGRPOS.RULEELEMENTOWNERSEQ = POS.MANAGERSEQ
                        ----- DIR Details
                        AND LAMPART.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND LAMPOS.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND LAMPYE.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND LAMTL.REMOVEDATE =
                        to_date('01/01/2200', 'MM/dd/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/dd/yyyy') */
                        AND LAMPART.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND LAMPART.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND LAMPART.PAYEESEQ = LAMPOS.PAYEESEQ
                        AND LAMPOS.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND LAMPOS.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND LAMPOS.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                        AND LAMPOS.PAYEESEQ = LAMPYE.PAYEESEQ
                        AND LAMPYE.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND LAMPYE.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND LAMTL.EFFECTIVESTARTDATE < PRD.ENDDATE
                        AND LAMTL.EFFECTIVEENDDATE > PRD.STARTDATE
                        AND LAMTL.RULEELEMENTOWNERSEQ = LAMPOS.TITLESEQ
                        AND LAMPOS.RULEELEMENTOWNERSEQ = MGRPOS.MANAGERSEQ
                ) AS PRINFO
            WHERE
                ev.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRD.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND POS.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND POS.EFFECTIVESTARTDATE < PRD.ENDDATE
                AND POS.EFFECTIVEENDDATE > PRD.STARTDATE
                AND EV.DATATYPESEQ = STR.EVENTTYPESEQ
                AND STR.SALESORDERSEQ = SRD.SALESORDERSEQ
                AND STR.UNITTYPEFORLINENUMBER = UT.UNITTYPESEQ
                AND STR.SALESORDERSEQ = CRD.SALESORDERSEQ
                AND STR.SALESTRANSACTIONSEQ = CRD.SALESTRANSACTIONSEQ
                AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                AND CRD.PERIODSEQ = PRD.PERIODSEQ
                AND PRD.PERIODSEQ = :IN_PERIODSEQ
                AND PRINFO.POSITIONSEQ = CRD.POSITIONSEQ
                AND PRINFO.PAYEESEQ = CRD.PAYEESEQ
                AND PRINFO.PERIODSEQ = CRD.PERIODSEQ
                AND POS.RULEELEMENTOWNERSEQ = PRINFO.POSITIONSEQ
                AND POS.PAYEESEQ = PRINFO.PAYEESEQ
                AND EV.EVENTTYPEID IN
                ('Prepaid Card',
                    'SIM Registration',
                    'MTPOS Sales Order',
                    'Prepaid Mobile Adjustment',
                    'Top Up Revenue',
                    'Top up Revenue Adjustment',
                    'Electronic Top Up',
                    'Phoenix Card',
                'mRemit - Registration')
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END