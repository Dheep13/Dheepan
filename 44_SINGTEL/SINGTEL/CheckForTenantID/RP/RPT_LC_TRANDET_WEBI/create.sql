CREATE PROCEDURE EXT.RPT_LC_TRANDET_WEBI
(
    IN in_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_rpttype VARCHAR2 */
    IN in_periodseq BIGINT,   /* ORIGSQL: in_periodseq IN INTEGER */
    IN in_processingunitseq BIGINT     /* ORIGSQL: in_processingunitseq IN INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Tenantid VARCHAR(255) = 'STEL';  /* ORIGSQL: v_Tenantid VARCHAR2(255) := 'STEL'; */
    DECLARE v_Calendarname VARCHAR(255) = NULL;  /* ORIGSQL: v_Calendarname VARCHAR2(255) := NULL; */
    DECLARE v_periodname VARCHAR(255);  /* ORIGSQL: v_periodname VARCHAR2(255); */
    DECLARE v_ComponentName VARCHAR(255);  /* ORIGSQL: v_ComponentName VARCHAR2(255); */
    DECLARE v_StMsg VARCHAR(255);  /* ORIGSQL: v_StMsg VARCHAR2(255); */
    DECLARE v_EdMsg VARCHAR(255);  /* ORIGSQL: v_EdMsg VARCHAR2(255); */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    DECLARE v_puname VARCHAR(255);  /* ORIGSQL: v_puname VARCHAR2(255); */

    v_ComponentName = 'stelext.rpt_lc_trandet_webi';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --                select name into v_Calendarname
    --                from cs_calendar
    --                where name = 'Singtel Monthly Calendar';
    --
    --            select name into v_periodname
    --            from cs_period
    --            where periodseq = in_periodseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        periodname,
        calendarname
    INTO
        v_periodname,
        v_Calendarname
    FROM
        cs_periodcalendar
    WHERE
        periodseq = :in_periodseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_puname
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :in_processingunitseq;

    /* ORIGSQL: DELETE STELEXT.stel_rpt_lc_trandet_webi; */
    DELETE
    FROM
        EXT.stel_rpt_lc_trandet_webi;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'stel_rpt_lc_trandet_webi') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'stel_rpt_lc_trandet_webi');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONASSIGNMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BUSINESSUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_LC_TRANDET_WEBI' not found */

    /* ORIGSQL: INSERT INTO STELEXT.stel_rpt_lc_trandet_webi (TENANTID, PERIODSEQ, PERIODNAME, P(...) */
    INSERT INTO EXT.stel_rpt_lc_trandet_webi
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            POSITIONSEQ,
            POSITIONNAME,
            partnercode,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            custID,
            serviceNo,
            orderenterdt,
            orderno,
            productcode,
            paystatus,
            purchasetype,
            PLAN,
            ordertotal,
            chatdt,
            chatuniqueid,
            dealercode,
            agentname,
            payoutamt,
            Remark
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, pos.ruleelementownerseq AS posit(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            pos.ruleelementownerseq AS positionseq,
            par.lastname AS positionname,
            b.positionname AS partnercode,
            :in_processingunitseq AS processingunitseq,
            :v_puname AS processingunitname,
            :v_Calendarname AS calendarname,
            /* --d.custid as CustID, */
            NULL AS CustID,
            a.ponumber AS serviceNumber,
            NULL AS orderentereddate,
            c.orderid AS OrderID,
            a.productid AS ProductID,
            NULL AS paystatus,
            a.productname AS purchasetype,
            a.genericattribute7 AS PLAN,
            a.VALUE AS ordertotal,
            a.accountingdate AS chatdate,
            NULL AS custname,
            a.genericattribute3 AS delaercode,
            a.genericattribute2 AS agentname,
            NULL AS PayoutAmt,
            NULL AS Remarks
        FROM
            cs_salestransaction A,
            cs_transactionassignment b,
            cs_salesorder C,
            cs_position pos,
            cs_eventtype et,
            cs_participant par,
            cs_businessunit bus,
            cs_processingunit pu,
            cs_period prd
            --cs_transactionaddress d
        WHERE
            pos.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND par.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND prd.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND et.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND pos.effectivestartdate <= prd.enddate
            AND pos.effectiveenddate > prd.enddate
            AND par.effectivestartdate <= prd.enddate
            AND par.effectiveenddate > prd.enddate
            AND a.compensationdate BETWEEN prd.startdate AND prd.enddate
            AND par.payeeseq = pos.payeeseq
            AND pos.NAME = b.positionname
            AND A.SALESTRANSACTIONSEQ = b.salestransactionseq
            AND C.SALESORDERSEQ = b.salesorderseq
            AND b.salesorderseq = a.salesorderseq
            --and a.billtoaddressseq=d.transactionaddressseq
            AND et.datatypeseq = a.eventtypeseq
            AND BUS.MASK = A.BUSINESSUNITMAP
            AND bus.processingunitseq = pu.processingunitseq
            AND et.eventtypeid = 'LiveChat'
            AND pu.processingunitseq = :in_processingunitseq
            AND prd.periodseq = :in_periodseq
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END