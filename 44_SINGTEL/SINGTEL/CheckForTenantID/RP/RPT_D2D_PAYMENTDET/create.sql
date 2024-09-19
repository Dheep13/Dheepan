CREATE PROCEDURE EXT.RPT_D2D_PAYMENTDET
(
    IN IN_RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: IN_RPTTYPE IN VARCHAR2 */
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

    v_ComponentName = 'RPT_d2d_paymentdet';

    v_StMsg = 'Start';

    v_EdMsg = 'End';

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'STEL_RPT_d2d_paymentdet Start') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'STEL_RPT_d2d_paymentdet Start');

    -- Add debug Log for Process START
    --       STEL_log (
        --          V_TENANTID,
        --          v_ComponentName,
        --        v_StMsg
        --      || ' PeriodSeq - '
        --      || in_PeriodSeq
        --      || ' PU - '
    --      || in_ProcessingUnitSeq);
    --
    --   COMMIT;

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
        EXT.STEL_RPT_d2d_paymentdet
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'EXT.STEL_RPT_d2d_paymentdet Table data deleted(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'STEL_RPT_d2d_paymentdet Table data deleted');

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'STEL_RPT_d2d_paymentdet') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'EXT.STEL_RPT_d2d_paymentdet');

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'STEL_RPT_d2d_paymentdet partitions created(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'EXT.STEL_RPT_d2d_paymentdet partitions created');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_LOOKUP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_D2D_PAYMENTDET' not found */

    /* ORIGSQL: insert into STEL_RPT_d2d_paymentdet (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, (...) */
    INSERT INTO EXT.STEL_RPT_d2d_paymentdet
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
            TEAMID,
            SALESMANCODE,
            AGENCY,
            PRODUCT,
            TYPE,
            QUANTITY,
            AI_RATE,
            PAYOUT
        )
        SELECT   /* ORIGSQL: (select v_Tenantid, in_periodseq, v_periodname, par.payeeseq, pos.ruleelementown(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            par.payeeseq,
            pos.ruleelementownerseq,
            pos.name,
            t.ruleelementownerseq,
            t.name AS titlename,
            :in_processingunitseq,
            :v_puname,
            :v_Calendarname,
            par.userid,
            firstname,
            NULL,
            par.lastname,
            pos.genericattribute1 AS teamid,
            par.genericattribute1 AS salesmancode,
            NULL AS agency,
            IFNULL(lkp.dim1,'') ||'-'|| IFNULL(lkp.dim2,'') AS product,
            lkp.dim3 AS type,
            0 AS quantity,
            lkp.value AS "AI Rate",
            0 AS Payout
        FROM
            cs_payee p,
            cs_participant par,
            cs_position pos,
            cs_period prd,
            cs_title t,
            EXT.STEL_LOOKUP lkp
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND pos.titleseq = t.ruleelementownerseq
            AND t.name = REPLACE(lkp.dim0,'-',' - ')
            AND p.removedate = :v_eot
            AND par.removedate = :v_eot
            AND pos.removedate = :v_eot
            AND prd.removedate = :v_eot
            AND t.removedate = :v_eot
            AND p.effectivestartdate <= prd.enddate
            AND p.effectiveenddate > prd.enddate
            AND par.effectivestartdate <= prd.enddate
            AND par.effectiveenddate > prd.enddate
            AND pos.effectivestartdate <= prd.enddate
            AND pos.effectiveenddate > prd.enddate
            AND t.effectivestartdate <= prd.enddate
            AND t.effectiveenddate > prd.enddate
            AND lkp.effectivestartdate <= prd.enddate
            AND lkp.effectiveenddate > prd.enddate
            AND lkp.name = 'LT_CSI_Internal Products_D2D_Rates'
            AND prd.periodseq = :in_periodseq
            AND pos.processingunitseq = :in_processingunitseq
    ;

    /* ORIGSQL: commit; */
    COMMIT;

    --Added below insert to include handling fee orders     
    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_LOOKUP_HANDLINGFEE' not found */
    /* ORIGSQL: insert into STEL_RPT_d2d_paymentdet (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, (...) */
    INSERT INTO EXT.STEL_RPT_d2d_paymentdet
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
            TEAMID,
            SALESMANCODE,
            AGENCY,
            PRODUCT,
            TYPE,
            QUANTITY,
            AI_RATE,
            PAYOUT
        )
        SELECT   /* ORIGSQL: (select v_Tenantid, in_periodseq, v_periodname, par.payeeseq, pos.ruleelementown(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            par.payeeseq,
            pos.ruleelementownerseq,
            pos.name,
            t.ruleelementownerseq,
            t.name AS titlename,
            :in_processingunitseq,
            :v_puname,
            :v_Calendarname,
            par.userid,
            firstname,
            NULL,
            par.lastname,
            pos.genericattribute1 AS teamid,
            par.genericattribute1 AS salesmancode,
            NULL AS agency,
            IFNULL(lkp.dim1,'') ||'-'|| IFNULL(lkp.dim2,'') AS product,
            lkp.dim3 AS type,
            0 AS quantity,
            lkp.value AS "AI Rate",
            0 AS Payout
        FROM
            cs_payee p,
            cs_participant par,
            cs_position pos,
            cs_period prd,
            cs_title t,
            EXT.STEL_LOOKUP_HANDLINGFEE lkp
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND pos.titleseq = t.ruleelementownerseq
            AND t.name = REPLACE(lkp.dim0,'-',' - ')
            AND p.removedate = :v_eot
            AND par.removedate = :v_eot
            AND pos.removedate = :v_eot
            AND prd.removedate = :v_eot
            AND t.removedate = :v_eot
            AND p.effectivestartdate <= prd.enddate
            AND p.effectiveenddate > prd.enddate
            AND par.effectivestartdate <= prd.enddate
            AND par.effectiveenddate > prd.enddate
            AND pos.effectivestartdate <= prd.enddate
            AND pos.effectiveenddate > prd.enddate
            AND t.effectivestartdate <= prd.enddate
            AND t.effectiveenddate > prd.enddate
            AND lkp.effectivestartdate <= prd.enddate
            AND lkp.effectiveenddate > prd.enddate
            AND lkp.name = 'LT_CSI_Internal Products_D2D_Rates'
            AND prd.periodseq = :in_periodseq
            AND pos.processingunitseq = :in_processingunitseq
    ;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'insert 1 complete') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'insert 1 complete'); 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_d2d_paymentdet tgt using (SELECT positionseq, SUM(value) AS (...) */
    MERGE INTO EXT.STEL_RPT_d2d_paymentdet AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select positionseq, SUM(value) value, SUM(genericnumber1) numberofcredits, c.ge(...) */
                positionseq,
                SUM(value) AS value,
                SUM(genericnumber1) AS numberofcredits,
                IFNULL(TO_VARCHAR(c.genericattribute4),'')||'-'||IFNULL(TO_VARCHAR(c.genericattribute5),'') AS product
            FROM
                cs_Credit c
            WHERE
                c.name IN ('DC_Direct Sales_D2D_Rate_Own Sale', 'DCR_Direct Sales_D2D_Rate_Own Sale','DCR_Direct Sales_D2D_Rate_Own Sale_TV GA')
                AND c.periodseq = :in_periodseq
                AND c.processingunitseq = :in_processingunitseq
                AND c.value <> 0
            GROUP BY
                positionseq, IFNULL(TO_VARCHAR(c.genericattribute4),'')||'-'||IFNULL(TO_VARCHAR(c.genericattribute5),'')
        ) AS src
        ON (tgt.positionseq = src.positionseq
            AND src.product = tgt.product
            AND tgt.periodseq = :in_periodseq
            AND tgt.processingunitseq = :in_processingunitseq
            AND tgt.type = 'Ownsales'
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.quantity = src.numberofcredits
            ,tgt.payout = src.value
        --WHERE
        --    tgt.periodseq = :in_periodseq
        --    AND tgt.processingunitseq = :in_processingunitseq
        --    AND tgt.type = 'Ownsales'
            ;

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'ownsales update complete') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'ownsales update complete'); 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_d2d_paymentdet tgt using (SELECT positionseq, SUM(value) AS (...) */
    MERGE INTO EXT.STEL_RPT_d2d_paymentdet AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select positionseq, SUM(value) value, SUM(genericnumber1) numberofcredits, c.ge(...) */
                positionseq,
                SUM(value) AS value,
                SUM(genericnumber1) AS numberofcredits,
                IFNULL(TO_VARCHAR(c.genericattribute4),'')||'-'||IFNULL(TO_VARCHAR(c.genericattribute5),'') AS product
            FROM
                cs_Credit c
                --where c.name = 'ICR_Direct Sales_D2D_Override'
            WHERE
                c.name IN ('ICR_Direct Sales_D2D_Override','IC_Direct Sales_D2D_Override_TV GA')
                AND c.value <> 0
                AND c.periodseq = :in_periodseq
                AND c.processingunitseq = :in_processingunitseq
            GROUP BY
                positionseq, IFNULL(TO_VARCHAR(c.genericattribute4),'')||'-'||IFNULL(TO_VARCHAR(c.genericattribute5),'')
        ) AS src
        ON (tgt.positionseq = src.positionseq
            AND src.product = tgt.product
            AND tgt.periodseq = :in_periodseq
            AND tgt.processingunitseq = :in_processingunitseq
            AND tgt.type = 'Override'
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.quantity = src.numberofcredits
            ,tgt.payout = src.value
        --WHERE
        --    tgt.periodseq = :in_periodseq
        --    AND tgt.processingunitseq = :in_processingunitseq
        --    AND tgt.type = 'Override'
            ;

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'override update complete') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'override update complete');

    /* ORIGSQL: commit; */
    COMMIT;

    -- Add debug Log for Process END
    --   STEL_log (
        --      V_TENANTID,
        --      v_ComponentName,
        --         v_EdMsg
        --      || ' PeriodSeq - '
        --      || in_PeriodSeq
        --      || ' PU - '
    --      || in_ProcessingUnitSeq);

    /* ORIGSQL: COMMIT; */
    COMMIT;
END