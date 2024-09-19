CREATE PROCEDURE EXT.STEL_EXTPREPAIDPAYSUMM
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
    DECLARE v_Tenantid VARCHAR(255) = 'STEL';  /* ORIGSQL: v_Tenantid VARCHAR2(255) := 'STEL'; */
    DECLARE v_Calendarname VARCHAR(255) = NULL;  /* ORIGSQL: v_Calendarname VARCHAR2(255) := NULL; */
    DECLARE v_periodname VARCHAR(255);  /* ORIGSQL: v_periodname VARCHAR2(255); */
    DECLARE v_ComponentName VARCHAR(255);  /* ORIGSQL: v_ComponentName VARCHAR2(255); */
    DECLARE v_StMsg VARCHAR(255);  /* ORIGSQL: v_StMsg VARCHAR2(255); */
    DECLARE v_EdMsg VARCHAR(255);  /* ORIGSQL: v_EdMsg VARCHAR2(255); */
    DECLARE v_eot TIMESTAMP = to_date('01-JAN-2200', 'dd-mon-yyyy');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01-JAN-2200', 'dd-mon-yyyy') ; */

    DECLARE v_puname VARCHAR(255);  /* ORIGSQL: v_puname VARCHAR2(255); */
    DECLARE v_isquarter BIGINT = 0;  /* ORIGSQL: v_isquarter INTEGER := 0; */

    v_ComponentName = 'STEL_EXTPREPAIDPAYSUMM ';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
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

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_puname
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */

    SELECT
        name
    INTO
        v_periodname
    FROM
        cs_period
    WHERE
        periodseq = :IN_PERIODSEQ;

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_EXTPREPAIDPAYSUMM' not found */

    /* ORIGSQL: DELETE FROM stelext.STEL_RPT_EXTPREPAIDPAYSUMM WHERE periodseq = in_periodseq AN(...) */
    DELETE
    FROM
        EXT.STEL_RPT_EXTPREPAIDPAYSUMM
    WHERE
        periodseq = :IN_PERIODSEQ
        AND processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'STEL_RPT_EXTPREPAIDPAYSUMM') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'STEL_RPT_EXTPREPAIDPAYSUMM');

    ---insert from measurements:
    ----------------------------

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_MEASUREMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_EXTPREPAIDPAYSUMM' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_EXTPREPAIDPAYSUMM (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
    INSERT INTO STEL_RPT_EXTPREPAIDPAYSUMM
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
            VENDORCODE,
            VENDORNAME,
            PRODUCT,
            ACTUALS,
            TARGET,
            ACHIEVEMENT,
            RATE,
            COMMISSION,
            SECTION,
            showdollar
        )
        
            SELECT   /* ORIGSQL: (SELECT V_TENANTID, in_periodseq, v_periodname, par.payeeseq, pos.ruleelementown(...) */
                :v_Tenantid,
                :IN_PERIODSEQ,
                :v_periodname,
                par.payeeseq,
                pos.ruleelementownerseq,
                pos.name AS positionname,
                :IN_PROCESSINGUNITSEQ,
                :v_puname AS processingunitname,
                :v_Calendarname AS calendarname,
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
                1,
                CASE
                    WHEN m.genericattribute1 = 'Regular + Tourist + BBOM'
                    THEN 'N'
                    ELSE 'Y'
                END
            FROM
                cs_participant par,
                cs_payee p,
                cs_position pos,
                cs_measurement m,
                cs_measurement mRate,
                cs_period prd
            WHERE
                prd.removedate = :v_eot
                AND par.effectivestartdate < prd.enddate
                AND par.effectiveenddate >= prd.enddate
                AND par.removedate = :v_eot
                AND p.effectivestartdate < prd.enddate
                AND p.effectiveenddate >= prd.enddate
                AND p.removedate = :v_eot
                AND pos.effectivestartdate < prd.enddate
                AND pos.effectiveenddate >= prd.enddate
                AND pos.removedate = :v_eot
                AND prd.periodseq = m.periodseq
                AND pos.ruleelementownerseq = mRate.positionseq
                AND mRate.periodseq = prd.periodseq
                -- AND pos.ruleelementownerseq = i.positionseq
                -- AND i.periodseq = prd.periodseq
                -- and i.name='I_External Prepaid_Incentive for Distributors_Total Final_Payout'
                AND M.POSITIONSEQ = POS.RULEELEMENTOWNERSEQ
                AND P.PAYEESEQ = PAR.PAYEESEQ
                AND M.PAYEESEQ = PAR.PAYEESEQ
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
                AND mrate.genericattribute1 LIKE IFNULL(TO_VARCHAR(m.genericattribute1),'') || '%' 
                AND m.genericnumber2 <> 0
                AND m.processingunitseq = :IN_PROCESSINGUNITSEQ
                AND pos.name IN
                (
                    SELECT   /* ORIGSQL: (select dim0 from stel_lookup where name like 'LT_ExternalPrepaid_EligibleVendor(...) */
                        dim0
                    FROM
                        EXT.STEL_lookup
                    WHERE
                        name LIKE 'LT_ExternalPrepaid_EligibleVendors'
                        AND dim1 = 'Retailer Incentive'
                )
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --Adding Retailer
    --Retailer Report has to be configured to run first 

    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_EXTPREPAIDPAYSUMM (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
    /* RESOLVE: Identifier not found: Table/view 'EXT.EXT.STEL_RPT_EPRETPAY' not found */
    INSERT INTO EXT.STEL_RPT_EXTPREPAIDPAYSUMM
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
            VENDORCODE,
            VENDORNAME,
            PRODUCT,
            ACTUALS,
            TARGET,
            ACHIEVEMENT,
            RATE,
            COMMISSION,
            SECTION,
            showdollar, reportperiodname
        )
        SELECT   /* ORIGSQL: SELECT V_TENANTID, in_periodseq, v_periodname, oldMgr.payeeseq, oldMgr.ruleeleme(...) */
            :v_Tenantid,
            :IN_PERIODSEQ,
            :v_periodname,
            oldMgr.payeeseq,
            oldMgr.ruleelementownerseq,
            x.oldMgr AS positionname,
            :IN_PROCESSINGUNITSEQ,
            :v_puname AS processingunitname,
            :v_Calendarname AS calendarname,
            oldMgrpar.userid,
            oldMgrpar.lastname,
            'Retailer Hi Card Incentive' AS product,
            SUM(x.TotalUnits) AS Actuals,
            NULL AS Target,
            NULL AS Achievement,
            /* --nvl(mrate.genericnumber3,0)*100*nvl(i.genericboolean1,0) */
            /* --x.rateapplied */
            NULL AS Rate,
            /* -- nvl(mrate.value,0)*nvl(i.genericboolean1,0) */
            SUM(x.payoutgroup) AS Commission,
            1,
            'N',
            :v_periodname
        FROM
            EXT.STEL_rpt_epretpay x
        INNER JOIN
            cs_period prd
            ON prd.periodseq = x.periodseq
            AND prd.removedate = :v_eot
            AND prd.periodseq = :IN_PERIODSEQ
        INNER JOIN
            cs_position oldMgr
            ON x.oldMgr = oldMgr.name
            AND oldMgr.removedate = :v_eot
            AND oldMgr.effectivestartdate < prd.enddate
            AND oldMgr.effectiveenddate >= prd.enddate
        INNER JOIN
            cs_participant oldMgrPar
            ON oldMgrPar.payeeseq = oldMgr.payeeseq
            AND oldMgrPar.removedate = :v_eot
            AND oldMgr.effectivestartdate < prd.enddate
            AND oldMgr.effectiveenddate >= prd.enddate
        WHERE
            x.periodseq = :IN_PERIODSEQ
            AND x.processingunitseq = :IN_PROCESSINGUNITSEQ

        GROUP BY
            oldMgr.payeeseq,--rateapplied,
            oldMgr.ruleelementownerseq,
            x.oldMgr,

            oldMgrpar.userid,
            oldMgrpar.lastname;

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
                SELECT   /* ORIGSQL: (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_periodtype p2 WHERE p1.removed(...) */
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
        /* ORIGSQL: INSERT INTO STEL_RPT_EXTPREPAIDPAYSUMM (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
        INSERT INTO EXT.STEL_RPT_EXTPREPAIDPAYSUMM
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
                VENDORCODE,
                VENDORNAME,
                PRODUCT,
                ACTUALS,
                TARGET,
                ACHIEVEMENT,
                RATE,
                COMMISSION,
                SECTION,
                showdollar
            )
            
                SELECT   /* ORIGSQL: (SELECT V_TENANTID, in_periodseq, v_periodname, par.payeeseq, pos.ruleelementown(...) */
                    :v_Tenantid,
                    :IN_PERIODSEQ,
                    :v_periodname,
                    par.payeeseq,
                    pos.ruleelementownerseq,
                    pos.name AS positionname,
                    :IN_PROCESSINGUNITSEQ,
                    :v_puname AS processingunitname,
                    :v_Calendarname AS calendarname,
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
                    1,
                    CASE
                        WHEN m.genericattribute1 = 'Regular + Tourist + BBOM'
                        THEN 'N'
                        ELSE 'Y'
                    END
                FROM
                    cs_participant par,
                    cs_payee p,
                    cs_position pos,
                    cs_measurement m,
                    cs_period prd
                WHERE
                    prd.removedate = :v_eot
                    AND par.effectivestartdate < prd.enddate
                    AND par.effectiveenddate >= prd.enddate
                    AND par.removedate = :v_eot
                    AND p.effectivestartdate < prd.enddate
                    AND p.effectiveenddate >= prd.enddate
                    AND p.removedate = :v_eot
                    AND pos.effectivestartdate < prd.enddate
                    AND pos.effectiveenddate >= prd.enddate
                    AND pos.removedate = :v_eot
                    AND prd.periodseq = m.periodseq
                    AND M.POSITIONSEQ = POS.RULEELEMENTOWNERSEQ
                    AND P.PAYEESEQ = PAR.PAYEESEQ
                    AND M.PAYEESEQ = PAR.PAYEESEQ
                    AND m.genericattribute1 IN ('CMM','CMM BONUS')
                    AND M.genericnumber3 <> 0

                    AND m.periodseq IN
                    (
                        SELECT   /* ORIGSQL: (SELECT p3.periodseq FROM cs_period p3 WHERE p3.parentseq IN (SELECT DISTINCT pa(...) */
                            p3.periodseq
                        FROM
                            cs_period p3
                        WHERE
                            p3.parentseq IN
                            (
                                SELECT   /* ORIGSQL: (SELECT DISTINCT parentseq FROM cs_period p1, cs_periodtype p2 WHERE P1.ENDDATE (...) */
                                    DISTINCT
                                    parentseq
                                FROM
                                    cs_period p1,
                                    cs_periodtype p2
                                WHERE
                                    P1.ENDDATE IN
                                    (
                                        SELECT   /* ORIGSQL: (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_periodtype p2 WHERE p1.removed(...) */
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
                    AND m.processingunitseq = :IN_PROCESSINGUNITSEQ
            ;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        -- Adding Adjustment commission.   

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CREDIT' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_TITLE' not found */
        /* ORIGSQL: INSERT INTO STEL_RPT_EXTPREPAIDPAYSUMM (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
        INSERT INTO EXT.STEL_RPT_EXTPREPAIDPAYSUMM
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
                VENDORCODE,
                VENDORNAME,
                PRODUCT,
                ACTUALS,
                TARGET,
                ACHIEVEMENT,
                RATE,
                COMMISSION,
                SECTION,
                showdollar
            )
            SELECT   /* ORIGSQL: (SELECT V_TENANTID, in_periodseq, v_periodname, par.payeeseq, pos.ruleelementown(...) */
                :v_Tenantid,
                :IN_PERIODSEQ,
                :v_periodname,
                par.payeeseq,
                pos.ruleelementownerseq,
                pos.name AS positionname,
                :IN_PROCESSINGUNITSEQ,
                :v_puname AS processingunitname,
                :v_Calendarname AS calendarname,
                par.userid,
                par.lastname,
                'Adjustment' AS product,
                NULL AS Actuals,
                NULL AS Target,
                NULL AS Achievement,
                NULL AS Rate,
                CRD.VALUE AS Commission,
                2,
                'Y'
            FROM
                cs_participant par,
                cs_payee p,
                cs_position pos,
                cs_credit crd,
                cs_period prd,
                cs_title t
            WHERE
                prd.removedate = :v_eot
                AND par.effectivestartdate < prd.enddate
                AND par.effectiveenddate >= prd.enddate
                AND par.removedate = :v_eot
                AND t.ruleelementownerseq = pos.titleseq
                AND t.effectivestartdate < prd.enddate
                AND t.effectiveenddate >= prd.enddate
                AND t.removedate = :v_eot
                AND p.effectivestartdate < prd.enddate
                AND p.effectiveenddate >= prd.enddate
                AND p.removedate = :v_eot
                AND pos.effectivestartdate < prd.enddate
                AND pos.effectiveenddate >= prd.enddate
                AND pos.removedate = :v_eot
                AND prd.periodseq = crd.periodseq
                AND crd.POSITIONSEQ = POS.RULEELEMENTOWNERSEQ
                AND P.PAYEESEQ = PAR.PAYEESEQ
                AND crd.PAYEESEQ = PAR.PAYEESEQ
                AND CRD.NAME = 'DC_Payment Adjustment' -- This name need to confirm once data is available.
                AND crd.periodseq = :IN_PERIODSEQ
                AND UPPER(crd.genericattribute1) LIKE '%PREPAID%' 
                AND UPPER(t.name) LIKE '%EXTERN%VENDOR%' 
                AND crd.processingunitseq = :IN_PROCESSINGUNITSEQ
                AND pos.name IN
                (
                    SELECT   /* ORIGSQL: (select dim0 from stel_lookup where name like 'LT_ExternalPrepaid_EligibleVendor(...) */
                        dim0
                    FROM
                        EXT.STEL_lookup
                    WHERE
                        name LIKE 'LT_ExternalPrepaid_EligibleVendors'
                        --and dim1='Retailer Incentive'
                );

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: UPDATE STEL_RPT_EXTPREPAIDPAYSUMM tgt SET reportperiodname = (SELECT CASE WHEN T(...) */
        UPDATE EXT.STEL_RPT_EXTPREPAIDPAYSUMM tgt 
            SET
            /* ORIGSQL: reportperiodname = */
            reportperiodname = (
                SELECT   /* ORIGSQL: (SELECT CASE WHEN TRUNC(startdate, 'Q') <> TRUNC(startdate, 'MM') THEN TRIM(sapd(...) */
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
            periodseq = :IN_PERIODSEQ
            AND product NOT LIKE '%Retailer%Incentive%';  

        /* ORIGSQL: UPDATE STEL_RPT_EXTPREPAIDPAYSUMM tgt SET reportperiodname = (SELECT name FROM c(...) */
        UPDATE EXT.STEL_RPT_EXTPREPAIDPAYSUMM tgt 
            SET
            /* ORIGSQL: reportperiodname = */
            reportperiodname = (
                SELECT   /* ORIGSQL: (SELECT name FROM cs_period WHERE periodseq = in_periodseq AND removedate > SYSD(...) */
                    name
                FROM
                    cs_period
                WHERE
                    periodseq = :IN_PERIODSEQ
                    AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            )
        WHERE
            periodseq = :IN_PERIODSEQ
            AND product LIKE '%Retailer%Incentive%';

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END IF;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END