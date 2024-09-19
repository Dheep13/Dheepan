CREATE PROCEDURE EXT.RPT_SCICOM_PAYSUMM
(
    IN p_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_rpttype VARCHAR2 */
    IN in_periodseq BIGINT,   /* ORIGSQL: in_periodseq IN integer */
    IN in_processingunitseq BIGINT     /* ORIGSQL: in_processingunitseq IN integer */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Tenantid VARCHAR(255) = 'STEL';  /* ORIGSQL: v_Tenantid varchar2(255) := 'STEL'; */
    DECLARE v_Calendarname VARCHAR(255) = NULL;  /* ORIGSQL: v_Calendarname varchar2(255) := NULL; */
    DECLARE v_periodname VARCHAR(255);  /* ORIGSQL: v_periodname varchar2(255); */
    DECLARE v_ComponentName VARCHAR(255);  /* ORIGSQL: v_ComponentName varchar2(255); */
    DECLARE v_StMsg VARCHAR(255);  /* ORIGSQL: v_StMsg varchar2(255); */
    DECLARE v_EdMsg VARCHAR(255);  /* ORIGSQL: v_EdMsg varchar2(255); */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200','dd/mm/yyyy');  /* ORIGSQL: v_eot constant date := to_date('01/01/2200','dd/mm/yyyy') ; */

    DECLARE v_puname VARCHAR(255);  /* ORIGSQL: v_puname varchar2(255); */

    v_ComponentName = 'rpt_scicom_paysumm';

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

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_D2D_PAYMENTDET' not found */

    /* ORIGSQL: delete from stelext.STEL_RPT_d2d_paymentdet where periodseq = in_periodseq and p(...) */
    DELETE
    FROM
        ext.STEL_RPT_d2d_paymentdet
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'STEL_rpt_scicom_paysumm') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'STEL_rpt_scicom_paysumm');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_SCICOM_PAYSUMM' not found */

    /* ORIGSQL: insert into STEL_rpt_scicom_paysumm (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, (...) */
    INSERT INTO EXT.STEL_rpt_scicom_paysumm
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
            lastname,
            alternatename,
            smcode,
            totalpoints,
            tier,
            sales_inc_pts,
            sales_inc_fixed,
            inc_payout
        )
        SELECT   /* ORIGSQL: (select v_Tenantid, in_periodseq, v_periodname, par.payeeseq, pos.ruleelementown(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            par.payeeseq,
            pos.ruleelementownerseq,
            pos.name,
            NULL,
            NULL,
            :in_processingunitseq,
            :v_puname,
            :v_Calendarname,
            par.lastname,
            NULL,
            pos.name,
            IFNULL(i.genericnumber1,0) AS totalpoints,  /* ORIGSQL: nvl(i.genericnumber1,0) */
            IFNULL(i.genericnumber2,0) AS tier,  /* ORIGSQL: nvl(i.genericnumber2,0) */
            IFNULL(i.value,0) AS total_inc_points,  /* ORIGSQL: nvl(i.value,0) */
            0,
            (IFNULL(i.value,0) + 0)   /* ORIGSQL: nvl(i.value,0) */
        FROM
            cs_participant par,
            cs_payee p,
            cs_position pos,
            cs_incentive i,
            cs_period prd
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND i.payeeseq = par.payeeseq
            AND i.periodseq = prd.periodseq
            AND i.positionseq = pos.ruleelementownerseq
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
            AND i.genericattribute1 = 'SCICOM SM Comm'
            AND i.periodseq = :in_periodseq
            AND i.processingunitseq = :in_processingunitseq
    ;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_rpt_scicom_paysumm tgt using (SELECT * FROM cs_incentive where g(...) */
    MERGE INTO EXT.STEL_rpt_scicom_paysumm AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select * from cs_incentive where genericattribute1='SCICOM_SM_Comm Payout' and (...) */
                *
            FROM
                cs_incentive
            WHERE
                genericattribute1 = 'SCICOM_SM_Comm Payout'
                AND periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
        ) AS src
        ON (tgt.periodseq = src.periodseq
        AND tgt.positionseq = src.positionseq
        	AND tgt.periodseq = :in_periodseq
        	AND tgt.processingunitseq = :in_processingunitseq
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.inc_payout = src.value
            , tgt.cap = src.genericnumber3
            ,tgt.teampayout = src.genericnumber2
        --WHERE
        --    periodseq = :in_periodseq
        --    AND processingunitseq = :in_processingunitseq
            ;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END