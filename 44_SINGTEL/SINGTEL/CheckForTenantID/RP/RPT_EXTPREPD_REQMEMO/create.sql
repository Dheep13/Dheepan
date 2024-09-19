CREATE PROCEDURE EXT.RPT_EXTPREPD_REQMEMO
(
    IN IN_RPTTYPE VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                  /* ORIGSQL: IN_RPTTYPE IN VARCHAR2 */
    IN IN_PERIODSEQ BIGINT,   /* ORIGSQL: IN_PERIODSEQ IN INTEGER */
    IN IN_PROCESSINGUNITSEQ BIGINT     /* ORIGSQL: IN_PROCESSINGUNITSEQ IN INTEGER */
)
SQL SECURITY DEFINER
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
    DECLARE v_isquarter BIGINT = 0;  /* ORIGSQL: v_isquarter INTEGER := 0; */

    v_ComponentName = 'stel_rpt_extprepd_reqmemo';

    -- Add debug Log for Process START
    /* ORIGSQL: ext.stel_log (V_TENANTID, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq || ' PU - ' || in_ProcessingUnitSeq) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CALENDAR' not found */

    SELECT
        name
    INTO
        v_Calendarname
    FROM
        cs_calendar
    WHERE
        name = 'Singtel Monthly Calendar';

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */

    SELECT
        name
    INTO
        v_periodname
    FROM
        cs_period
    WHERE
        periodseq = :IN_PERIODSEQ;

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_puname
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_EXTPREPD_REQMEMO' not found */

    /* ORIGSQL: DELETE FROM stelext.stel_rpt_extprepd_reqmemo WHERE periodseq = in_periodseq AND processingunitseq = in_processingunitseq; */
    DELETE
    FROM
        ext.stel_rpt_extprepd_reqmemo
    WHERE
        periodseq = :IN_PERIODSEQ
        AND processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: ext.stel_PROC_RPT_partitions (in_periodseq, 'stel_rpt_extprepd_reqmemo') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'stel_rpt_extprepd_reqmemo');

    ---insertion of Distributors data - section 1--

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_MEASUREMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_EXTPREPD_REQMEMO' not found */

    /* ORIGSQL: INSERT INTO ext.stel_rpt_extprepd_reqmemo (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, POSITIONSEQ, POSITIONNAME, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, CALENDARNAME, REPORTPERIODNAME, COMPANYNAME, REGNO, (...) */
    INSERT INTO ext.stel_rpt_extprepd_reqmemo
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            REPORTPERIODNAME,
            COMPANYNAME,
            REGNO,
            PAYEETYPE,
            PRT_FOOTER,
            RPT_FROMDET,
            DEALERCODE,
            DEALERNAME,
            PRODUCT,
            ACTUALS,
            TARGET,
            ACHIEVEMENT,
            RATE,
            COMMISSION,
            SECTION,
            TOTALUNITS,
            showdollar
        )
        
            SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, p.payeeseq, pos.ruleelementownerseq, pos.name, in_processingunitseq, v_puname, v_Calendarname, NULL, cl.companyname, cl.regno, 'Distributors', cl.rpt_fo(...) */
                :v_Tenantid,
                :IN_PERIODSEQ,
                :v_periodname,
                p.payeeseq,
                pos.ruleelementownerseq,
                pos.name,
                :IN_PROCESSINGUNITSEQ,
                :v_puname,
                :v_Calendarname,
                NULL,
                cl.companyname,
                cl.regno,
                'Distributors',
                cl.rpt_footer,
                cl.frm1,
                par.userid,
                par.lastname,
                m.genericattribute1 AS product,
                m.genericnumber1 AS Actuals,
                m.genericnumber2 AS Target,
                m.VALUE * 100 AS Achievement,
                /* --nvl(mrate.genericnumber3,0)*100*nvl(i.genericboolean1,0) */
                IFNULL(mrate.genericnumber3, 0) * 100 AS Rate,  /* ORIGSQL: NVL(mrate.genericnumber3, 0) */
                /* -- nvl(mrate.value,0)*nvl(i.genericboolean1,0) */
                IFNULL(mrate.VALUE, 0) AS Commission,  /* ORIGSQL: NVL(mrate.VALUE, 0) */
                '1',
                NULL,
                CASE
                    WHEN m.genericattribute1 = 'Regular + Tourist + BBOM'
                    THEN 'N'
                    ELSE 'Y'
                END
            FROM
                cs_participant par,
                cs_payee p,
                cs_period prd,
                cs_measurement m,
                cs_measurement mRate,
                cs_position pos,
                (
                    SELECT   /* ORIGSQL: (SELECT genericattribute1 companyname, genericattribute2 regno, genericattribute3 frm1, genericattribute4 rpt_footer FROM ext.stel_classifier WHERE effectiveenddate = v_eot AND categoryname = 'Requisition(...) */
                        genericattribute1 AS companyname,
                        genericattribute2 AS regno,
                        genericattribute3 AS frm1,
                        genericattribute4 AS rpt_footer
                    FROM
                        ext.stel_classifier
                    WHERE
                        effectiveenddate = :v_eot
                        AND categoryname = 'RequisitionMomo-External Prepaid'
                        AND categorytreename = 'Reporting Config'
                        AND classifierid = 'External Prepaid-ALL'
                ) AS cl
            WHERE
                p.payeeseq = par.payeeseq
                AND par.payeeseq = pos.payeeseq
                AND par.payeeseq = m.payeeseq
                AND pos.ruleelementownerseq = m.positionseq
                AND m.periodseq = prd.periodseq
                AND pos.ruleelementownerseq = mRate.positionseq
                AND mRate.periodseq = prd.periodseq
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
                AND m.genericattribute1 IN
                ('TUC + SIM',
                    'PhoenixCard',
                    'EMTU',
                'Regular + Tourist + BBOM')
                AND mrate.genericattribute1 IN
                ('TUC + SIM Rate',
                    'PhoenixCard Rate',
                    'EMTU Rate',
                'Regular + Tourist + BBOM Rate')
                AND m.periodseq = :IN_PERIODSEQ
                AND mrate.periodseq = :IN_PERIODSEQ
                AND m.genericnumber2 <> 0
                -- AND pos.ruleelementownerseq = i.positionseq
                -- AND i.periodseq = prd.periodseq
                -- and i.name='I_External Prepaid_Incentive for Distributors_Total Final_Payout'
                AND mrate.genericattribute1 LIKE IFNULL(TO_VARCHAR(m.genericattribute1),'') || '%' 
                AND m.processingunitseq = :IN_PROCESSINGUNITSEQ
                AND mrate.processingunitseq = :IN_PROCESSINGUNITSEQ
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Below block for CMM payee.
    -- Checking for quarterly periodsq
    BEGIN 
        DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            /* ORIGSQL: WHEN NO_DATA_FOUND THEN */
            BEGIN
                v_isquarter = 0;
            END;

             
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIODTYPE' not found */

        SELECT
            periodseq
        INTO
            v_isquarter
        FROM
            cs_period p1,
            cs_periodtype p2
        WHERE
            P1.ENDDATE   
            IN
            (
                SELECT   /* ORIGSQL: (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_periodtype p2 WHERE p1.removedate = TO_DATE('01/01/2200', 'dd/mm/yyyy') AND P2.REMOVEDATE = TO_DATE('01/01/2200', 'dd/mm/yyyy') AND P1.PERIODTYPESEQ =(...) */
                    DISTINCT
                    p1.enddate
                FROM
                    cs_period p1,
                    cs_periodtype p2
                WHERE
                    p1.removedate = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                    AND P2.REMOVEDATE =
                    to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                    AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
                    AND P2.NAME = 'quarter'
            )
            AND p1.removedate = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
            AND P2.REMOVEDATE = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
            AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
            AND P2.NAME = 'month'
            AND p1.periodseq = :IN_PERIODSEQ;

        /* ORIGSQL: EXCEPTION WHEN NO_DATA_FOUND THEN */
    END;

    IF :v_isquarter > 0
    THEN           
        /* ORIGSQL: INSERT INTO ext.stel_rpt_extprepd_reqmemo (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, POSITIONSEQ, POSITIONNAME, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, CALENDARNAME, REPORTPERIODNAME, COMPANYNAME, REGNO, (...) */
        INSERT INTO ext.stel_rpt_extprepd_reqmemo
            (
                TENANTID,
                PERIODSEQ,
                PERIODNAME,
                PAYEESEQ,
                POSITIONSEQ,
                POSITIONNAME,
                PROCESSINGUNITSEQ,
                PROCESSINGUNITNAME,
                CALENDARNAME,
                REPORTPERIODNAME,
                COMPANYNAME,
                REGNO,
                PAYEETYPE,
                PRT_FOOTER,
                RPT_FROMDET,
                DEALERCODE,
                DEALERNAME,
                PRODUCT,
                ACTUALS,
                TARGET,
                ACHIEVEMENT,
                RATE,
                COMMISSION,
                SECTION,
                TOTALUNITS,
                showdollar
            )
            
                SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, p.payeeseq, pos.ruleelementownerseq, pos.name, in_processingunitseq, v_puname, v_Calendarname, NULL, cl.companyname, cl.regno, 'Distributors', cl.rpt_fo(...) */
                    :v_Tenantid,
                    :IN_PERIODSEQ,
                    :v_periodname,
                    p.payeeseq,
                    pos.ruleelementownerseq,
                    pos.name,
                    :IN_PROCESSINGUNITSEQ,
                    :v_puname,
                    :v_Calendarname,
                    NULL,
                    cl.companyname,
                    cl.regno,
                    'Distributors',
                    cl.rpt_footer,
                    cl.frm1,
                    par.userid,
                    par.lastname,
                    IFNULL(PRD.NAME,'')||'-'|| IFNULL(TO_VARCHAR(m.genericattribute1),'') AS product,
                    m.genericnumber1 AS Actuals,
                    NULL AS Target,
                    NULL AS Achievement,
                    /* --nvl(mrate.genericnumber3,0)*100*nvl(i.genericboolean1,0) */
                    IFNULL(m.genericnumber3, 0) AS Rate,  /* ORIGSQL: NVL(m.genericnumber3, 0) */
                    /* -- nvl(mrate.value,0)*nvl(i.genericboolean1,0) */
                    IFNULL(m.VALUE, 0) AS Commission,  /* ORIGSQL: NVL(m.VALUE, 0) */
                    '1',
                    NULL,
                    CASE
                        WHEN m.genericattribute1 = 'Regular + Tourist + BBOM'
                        THEN 'N'
                        ELSE 'Y'
                    END
                FROM
                    cs_participant par,
                    cs_payee p,
                    cs_period prd,
                    cs_measurement m,
                    cs_position pos,
                    (
                        SELECT   /* ORIGSQL: (SELECT genericattribute1 companyname, genericattribute2 regno, genericattribute3 frm1, genericattribute4 rpt_footer FROM ext.stel_classifier WHERE effectiveenddate = v_eot AND categoryname = 'Requisition(...) */
                            genericattribute1 AS companyname,
                            genericattribute2 AS regno,
                            genericattribute3 AS frm1,
                            genericattribute4 AS rpt_footer
                        FROM
                            ext.stel_classifier
                        WHERE
                            effectiveenddate = :v_eot
                            AND categoryname =
                            'RequisitionMomo-External Prepaid'
                            AND categorytreename = 'Reporting Config'
                            AND classifierid = 'External Prepaid-ALL'
                    ) AS cl
                WHERE
                    p.payeeseq = par.payeeseq
                    AND par.payeeseq = pos.payeeseq
                    AND par.payeeseq = m.payeeseq
                    AND pos.ruleelementownerseq = m.positionseq
                    AND m.periodseq = prd.periodseq
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
                    AND m.periodseq IN
                    (
                        SELECT   /* ORIGSQL: (SELECT p3.periodseq FROM cs_period p3 WHERE p3.parentseq IN (SELECT DISTINCT parentseq FROM cs_period p1, cs_periodtype p2 WHERE P1.ENDDATE IN (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_period(...) */
                            p3.periodseq
                        FROM
                            cs_period p3
                        WHERE
                            p3.parentseq IN
                            (
                                SELECT   /* ORIGSQL: (SELECT DISTINCT parentseq FROM cs_period p1, cs_periodtype p2 WHERE P1.ENDDATE IN (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_periodtype p2 WHERE p1.removedate = TO_DATE('01/01/2200', 'dd/mm/yy(...) */
                                    DISTINCT
                                    parentseq
                                FROM
                                    cs_period p1,
                                    cs_periodtype p2
                                WHERE
                                    P1.ENDDATE IN
                                    (
                                        SELECT   /* ORIGSQL: (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_periodtype p2 WHERE p1.removedate = TO_DATE('01/01/2200', 'dd/mm/yyyy') AND P2.REMOVEDATE = TO_DATE('01/01/2200', 'dd/mm/yyyy') AND P1.PERIODTYPESEQ =(...) */
                                            DISTINCT
                                            p1.enddate
                                        FROM
                                            cs_period p1,
                                            cs_periodtype p2
                                        WHERE
                                            p1.removedate =
                                            to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                            AND P2.REMOVEDATE =
                                            to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                            'dd/mm/yyyy')
                                            AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
                                            AND P2.NAME = 'quarter'
                                    )
                                    AND p1.removedate =
                                    to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                    AND P2.REMOVEDATE =
                                    to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                    AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
                                    AND P2.NAME = 'month'
                                    AND p1.periodseq = :IN_PERIODSEQ
                            )
                            AND p3.removedate = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                    )
                    AND m.genericattribute1 IN ('CMM','CMM BONUS')
                    AND M.genericnumber3 <> 0
                    AND m.processingunitseq = :IN_PROCESSINGUNITSEQ
            ;

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END IF;

    ---insertion of Adjustment value - section 1---   

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CREDIT' not found */
    /* ORIGSQL: INSERT INTO ext.stel_rpt_extprepd_reqmemo (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, POSITIONSEQ, POSITIONNAME, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, CALENDARNAME, REPORTPERIODNAME, COMPANYNAME, REGNO, (...) */
    INSERT INTO ext.stel_rpt_extprepd_reqmemo
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            REPORTPERIODNAME,
            COMPANYNAME,
            REGNO,
            PAYEETYPE,
            PRT_FOOTER,
            RPT_FROMDET,
            DEALERCODE,
            DEALERNAME,
            PRODUCT,
            COMMISSION,
            SECTION,
            TOTALUNITS,
            showdollar
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, p.payeeseq, pos.ruleelementownerseq, pos.name, in_processingunitseq, v_puname, v_Calendarname, NULL, cl.companyname, cl.regno, 'Distributors', cl.rpt_fo(...) */
            :v_Tenantid,
            :IN_PERIODSEQ,
            :v_periodname,
            p.payeeseq,
            pos.ruleelementownerseq,
            pos.name,
            :IN_PROCESSINGUNITSEQ,
            :v_puname,
            :v_Calendarname,
            NULL,
            cl.companyname,
            cl.regno,
            'Distributors',
            cl.rpt_footer,
            cl.frm1,
            par.userid,
            par.lastname,
            'Adjustment' AS product,
            CRD.VALUE AS Commission,
            '1',
            NULL,
            'Y'
        FROM
            cs_participant par,
            cs_payee p,
            cs_period prd,
            cs_credit crd,
            cs_position pos,
            (
                SELECT   /* ORIGSQL: (SELECT genericattribute1 companyname, genericattribute2 regno, genericattribute3 frm1, genericattribute4 rpt_footer FROM ext.stel_classifier WHERE effectiveenddate = v_eot AND categoryname = 'Requisition(...) */
                    genericattribute1 AS companyname,
                    genericattribute2 AS regno,
                    genericattribute3 AS frm1,
                    genericattribute4 AS rpt_footer
                FROM
                    ext.stel_classifier
                WHERE
                    effectiveenddate = :v_eot
                    AND categoryname = 'RequisitionMomo-External Prepaid'
                    AND categorytreename = 'Reporting Config'
                    AND classifierid = 'External Prepaid-ALL'
            ) AS cl
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND par.payeeseq = crd.payeeseq
            AND pos.ruleelementownerseq = crd.positionseq
            AND crd.periodseq = prd.periodseq
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
            AND CRD.NAME = 'DC_Payment Adjustment'
            AND UPPER(crd.genericattribute1) LIKE '%PREPAID%' 
            AND crd.periodseq = :IN_PERIODSEQ
            AND crd.processingunitseq = :IN_PROCESSINGUNITSEQ
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    ---insertion of Retailers data - section 2--      
    /* ORIGSQL: INSERT INTO ext.stel_rpt_extprepd_reqmemo (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, POSITIONSEQ, POSITIONNAME, PROCESSINGUNITSEQ, PROCESSINGUNITNAME, CALENDARNAME, REPORTPERIODNAME, COMPANYNAME, REGNO, (...) */
    INSERT INTO ext.stel_rpt_extprepd_reqmemo
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            REPORTPERIODNAME,
            COMPANYNAME,
            REGNO,
            PAYEETYPE,
            PRT_FOOTER,
            RPT_FROMDET,
            DEALERCODE,
            DEALERNAME,
            PRODUCT,
            ACTUALS,
            TARGET,
            ACHIEVEMENT,
            RATE,
            COMMISSION,
            SECTION,
            TOTALUNITS
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, p.payeeseq, pos.ruleelementownerseq, pos.name, in_processingunitseq, v_puname, v_Calendarname, NULL, cl.companyname, cl.regno, 'Retailers', cl.rpt_foote(...) */
            :v_Tenantid,
            :IN_PERIODSEQ,
            :v_periodname,
            p.payeeseq,
            pos.ruleelementownerseq,
            pos.name,
            :IN_PROCESSINGUNITSEQ,
            :v_puname,
            :v_Calendarname,
            NULL,
            cl.companyname,
            cl.regno,
            'Retailers',
            cl.rpt_footer,
            cl.frm1,
            par.userid,
            par.lastname,
            NULL,
            NULL,
            NULL,
            NULL,
            IFNULL(m.genericnumber2, 0),  /* ORIGSQL: NVL(m.genericnumber2, 0) */
            NULL,
            '2',
            IFNULL(m.genericnumber1, 0)  /* ORIGSQL: NVL(m.genericnumber1, 0) */
        FROM
            cs_participant par,
            cs_payee p,
            cs_period prd,
            cs_measurement m,
            cs_position pos,
            (
                SELECT   /* ORIGSQL: (SELECT genericattribute1 companyname, genericattribute2 regno, genericattribute3 frm1, genericattribute4 rpt_footer FROM ext.stel_classifier WHERE effectiveenddate = v_eot AND categoryname = 'Requisition(...) */
                    genericattribute1 AS companyname,
                    genericattribute2 AS regno,
                    genericattribute3 AS frm1,
                    genericattribute4 AS rpt_footer
                FROM
                    ext.stel_classifier
                WHERE
                    effectiveenddate = :v_eot
                    AND categoryname = 'RequisitionMomo-External Prepaid'
                    AND categorytreename = 'Reporting Config'
                    AND classifierid = 'External Prepaid-ALL'
            ) AS cl
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND par.payeeseq = m.payeeseq
            AND pos.ruleelementownerseq = m.positionseq
            AND m.periodseq = prd.periodseq
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
            AND m.genericattribute1 = 'Incentive for Retailers'
            AND m.VALUE <> 0
            AND m.periodseq = :IN_PERIODSEQ
            AND m.processingunitseq = :IN_PROCESSINGUNITSEQ
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    ---updation of deposit value for section - 2---   
    /* ORIGSQL: UPDATE ext.stel_rpt_extprepd_reqmemo a SET commission = (SELECT d.VALUE FROM cs_deposit d WHERE d.payeeseq = a.payeeseq AND d.positionseq = a.positionseq AND d.periodseq = a.periodseq AND d.genericattribu(...) */
    UPDATE ext.stel_rpt_extprepd_reqmemo a
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_DEPOSIT' not found */
        SET
        /* ORIGSQL: commission = */
        commission = (
            SELECT   /* ORIGSQL: (SELECT d.VALUE FROM cs_deposit d WHERE d.payeeseq = a.payeeseq AND d.positionseq = a.positionseq AND d.periodseq = a.periodseq AND d.genericattribute1 = 'Retaliers Final Payout') */
                d.VALUE
            FROM
                cs_deposit d
            WHERE
                d.payeeseq = a.payeeseq
                AND d.positionseq = a.positionseq
                AND d.periodseq = a.periodseq
                AND d.genericattribute1 = 'Retaliers Final Payout'
        )
    WHERE
        a.section = 2;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE ext.stel_rpt_extprepd_reqmemo tgt SET reportperiodname = (SELECT CASE WHEN TRUNC(startdate, 'Q') <> TRUNC(startdate, 'MM') THEN TRIM(TO_CHAR(TRUNC (startdate, 'Q'), 'MON')) || ' - ' || TRIM(TO_CHAR(...) */
    UPDATE ext.stel_rpt_extprepd_reqmemo tgt 
        SET
        /* ORIGSQL: reportperiodname = */
        reportperiodname = (
            SELECT   /* ORIGSQL: (SELECT CASE WHEN TRUNC(startdate, 'Q') <> TRUNC(startdate, 'MM') THEN TRIM(TO_CHAR(TRUNC (startdate, 'Q'), 'MON')) || ' - ' || TRIM(TO_CHAR(TRUNC (startdate, 'MM'), 'MON')) || ' ' || TO_CHAR(startdat(...) */
                CASE
                        WHEN ext.trunc(startdate,'QUARTER') <>  /* ORIGSQL: TRUNC(startdate, 'Q') */
                         ext.trunc(startdate,'MONTH')  /* ORIGSQL: TRUNC(startdate, 'MM') */
                        THEN 
/* ORIGSQL: TRUNC(startdate, 'Q') */ /* ORIGSQL: TO_CHAR(TRUNC (startdate, 'Q'), 'Month') */
 /* ORIGSQL: TRUNC(startdate, 'MM') */
 /* ORIGSQL: TO_CHAR(startdate, 'YYYY') */
 /* ORIGSQL: TRUNC(startdate, 'MM') */
/* ORIGSQL: TO_CHAR(startdate, 'YYYY') */
                    EXT.FORMAT_QUARTER_PERIOD_NAME(
                   	MONTHNAME(ext.trunc(startdate, 'QUARTER'))
                   	||' - '||
                   	MONTHNAME(ADD_MONTHS(ext.trunc(startdate, 'QUARTER'),2))
                   	||' '|| EXTRACT_YEAR(ext.trunc(startdate, 'QUARTER'))
                   	) --Deepan :Quarter logic implemented in TRUNC function. Also created FORMAT_QUARTER_PERIOD_NAME to chage periodname from upper to camel case
                    END 
            FROM
                cs_period
            WHERE
                periodseq = :IN_PERIODSEQ
                AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        )
    WHERE
        periodseq = :IN_PERIODSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: ext.stel_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq || ' PU - ' || in_ProcessingUnitSeq) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END