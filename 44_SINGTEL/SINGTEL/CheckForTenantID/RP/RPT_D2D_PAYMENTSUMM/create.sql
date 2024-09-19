CREATE PROCEDURE EXT.RPT_D2D_PAYMENTSUMM
(
    IN IN_RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: IN_RPTTYPE IN VARCHAR2 */
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
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'dd/mm/yyyy');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'dd/mm/yyyy') ; */

    DECLARE v_puname VARCHAR(255);  /* ORIGSQL: v_puname VARCHAR2(255); */

    v_ComponentName = 'rpt_d2d_paymentsumm';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CALENDAR' not found */

    SELECT
        name
    INTO
        v_Calendarname
    FROM
        cs_calendar
    WHERE
        name = 'Singtel Monthly Calendar';

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT
        name
    INTO
        v_periodname
    FROM
        cs_period
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

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_D2D_PAYMENTSUMM' not found */

    /* ORIGSQL: DELETE FROM stelext.stel_rpt_d2d_paymentsumm WHERE periodseq = in_periodseq AND (...) */
    DELETE
    FROM
        ext.stel_rpt_d2d_paymentsumm
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, 'stel_rpt_d2d_paymentsumm Table data dele(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'ext.stel_rpt_d2d_paymentsumm Table data deleted');

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'stel_rpt_d2d_paymentsumm') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'ext.stel_rpt_d2d_paymentsumm');

    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, 'stel_rpt_d2d_paymentsumm Report partitio(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'ext.stel_rpt_d2d_paymentsumm Report partitions created');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_D2D_PAYMENTSUMM' not found */

    /* ORIGSQL: INSERT INTO stel_rpt_d2d_paymentsumm (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ,(...) */
    INSERT INTO ext.stel_rpt_d2d_paymentsumm
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            TITLESEQ,
            TITLENAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            GEID,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            AGENCY,
            SALESMANCODE,
            POSITION,
            DESCRIPTION,
            REMARKS,
            PAYOUT
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, par.payeeseq, pos.ruleelementown(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            par.payeeseq,
            pos.ruleelementownerseq,
            pos.name,
            pos.titleseq,
            t.name,
            :in_processingunitseq,
            :v_puname,
            :v_Calendarname,
            pos.name,
            par.firstname,
            NULL,
            par.lastname,
            pos.genericattribute1,/* --agnecy */  par.genericattribute1,/* --smcode */  t.name,/* --position */  d.genericattribute2,/* --desc? */ NULL,/* --remarks */  d.VALUE
        FROM
            cs_participant par,
            cs_payee p,
            cs_position pos,
            cs_deposit d,
            cs_period prd,
            cs_title t
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND t.ruleelementownerseq = pos.titleseq
            AND p.removedate = :v_eot
            AND par.removedate = :v_eot
            AND pos.removedate = :v_eot
            AND prd.removedate = :v_eot
            AND p.effectivestartdate <= prd.enddate
            AND p.effectiveenddate > prd.enddate
            AND par.effectivestartdate <= prd.enddate
            AND par.effectiveenddate > prd.enddate
            AND pos.effectivestartdate <= prd.enddate
            AND pos.effectiveenddate > prd.enddate
            AND d.periodseq = :in_periodseq
            AND d.positionseq = pos.ruleelementownerseq
            ---and  DEPOSIT RULE---
            AND prd.periodseq = :in_periodseq
            AND pos.processingunitseq = :in_processingunitseq
            AND t.name LIKE '%D2D%'
            AND t.removedate = :v_eot
            AND t.islast = 1
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END