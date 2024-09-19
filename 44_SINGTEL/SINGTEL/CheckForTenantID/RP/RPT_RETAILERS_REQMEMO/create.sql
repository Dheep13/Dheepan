CREATE PROCEDURE EXT.RPT_RETAILERS_REQMEMO
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
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'dd/mm/yyyy');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'dd/mm/yyyy') ; */

    DECLARE v_puname VARCHAR(255);  /* ORIGSQL: v_puname VARCHAR2(255); */
    DECLARE v_isquarter BIGINT = 0;  /* ORIGSQL: v_isquarter INTEGER := 0; */

    v_ComponentName = 'rpt_retailers_reqmemo';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

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
        periodseq = :IN_PERIODSEQ;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_puname
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_RETAILERS_REQMEMO' not found */

    /* ORIGSQL: DELETE FROM stelext.STEL_RPT_RETAILERS_REQMEMO WHERE periodseq = in_periodseq AN(...) */
    DELETE
    FROM
        EXT.STEL_RPT_RETAILERS_REQMEMO
    WHERE
        periodseq = :IN_PERIODSEQ
        AND processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'STEL_RPT_RETAILERS_REQMEMO') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_RETAILERS_REQMEMO');

    ---insertion of Distributors data - section 1--

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_RETAILERS_REQMEMO' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_RETAILERS_REQMEMO (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_EPRETPAY' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    INSERT INTO EXT.STEL_RPT_RETAILERS_REQMEMO
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
        SELECT   /* ORIGSQL: SELECT v_Tenantid, in_periodseq, v_periodname, m.payeeseq, m.ruleelementownerseq(...) */
            :v_Tenantid,
            :IN_PERIODSEQ,
            :v_periodname,
            m.payeeseq,
            m.ruleelementownerseq,
            m.name,
            :IN_PROCESSINGUNITSEQ,
            :v_puname,
            :v_Calendarname,
            NULL,
            cl.companyname,
            cl.regno,
            'Distributors',
            cl.rpt_footer,
            cl.frm1,
            mpar.userid,
            mpar.lastname,
            '' AS product,
            0 AS Actuals,
            0 AS Target,
            SUM(s.totalunits) AS Achievement,
            /* --nvl(mrate.genericnumber3,0)*100*nvl(i.genericboolean1,0) */
            0 AS Rate,
            /* -- nvl(mrate.value,0)*nvl(i.genericboolean1,0) */
            SUM(s.payoutgroup) AS Commission,
            '1',
            NULL,
            ' '
        FROM
            ext.stel_rpt_epretpay s,
            cs_period prd,
            cs_position m,
            cs_participant mPar,
            (
                SELECT   /* ORIGSQL: (SELECT genericattribute1 companyname, genericattribute2 regno, genericattribute(...) */
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
                    AND classifierid = 'Retailers'
            ) AS cl
        WHERE
            s.oldmgr = m.name
            AND prd.periodseq = :IN_PERIODSEQ
            AND prd.removedate = :v_eot
            AND TO_DATE(ADD_SECONDS(prd.enddate,(86400*-1))) BETWEEN m.effectivestartdate AND TO_DATE(ADD_SECONDS(m.effectiveenddate,(86400*-1)))   /* ORIGSQL: prd.enddate-1 */
                                                                                                                                                    /* ORIGSQL: m.effectiveenddate-1 */
            AND m.removedate = :v_eot
            AND mPar.payeeseq = m.payeeseq
            AND mpar.removedate = :v_eot
            AND TO_DATE(ADD_SECONDS(prd.enddate,(86400*-1))) BETWEEN mpar.effectivestartdate AND TO_DATE(ADD_SECONDS(mpar.effectiveenddate,(86400*-1)))   /* ORIGSQL: prd.enddate-1 */
                                                                                                                                                          /* ORIGSQL: mpar.effectiveenddate-1 */
            AND s.payout <> 0
        GROUP BY
            m.payeeseq,
            m.ruleelementownerseq,
            m.name,
            cl.companyname,
            cl.regno,

            cl.rpt_footer,
            cl.frm1,
            mpar.userid,
            mpar.lastname;

    /*(SELECT v_Tenantid,
          in_periodseq,
          v_periodname,
          m.payeeseq,
          m.ruleelementownerseq,
          m.name,
          in_processingunitseq,
          v_puname,
          v_Calendarname,
          NULL,
          cl.companyname,
          cl.regno,
          'Distributors',
          cl.rpt_footer,
          cl.frm1,
          mpar.userid,
          mpar.lastname,
          '' AS product,
          0 AS Actuals,
          0 AS Target,
          sum(c.genericnumber1) AS Achievement,
          --nvl(mrate.genericnumber3,0)*100*nvl(i.genericboolean1,0)
          0 AS Rate,
          -- nvl(mrate.value,0)*nvl(i.genericboolean1,0)
          sum(NVL (i.VALUE, 0) ) AS Commission,
          '1',
          NULL,
          ' '
           FROM cs_participant par,
          cs_payee p,
          cs_period prd,
          cs_credit c,
          cs_position m,
          cs_participant mPar,
          cs_Credittype ct,
        
          --cs_measurement m,
          cs_incentive i,
          cs_position pos,
          (SELECT genericattribute1 companyname,
                      genericattribute2 regno,
                      genericattribute3 frm1,
                      genericattribute4 rpt_footer
                 FROM stel_classifier
                WHERE     effectiveenddate = v_eot
             AND categoryname = 'RequisitionMomo-External Prepaid'
             AND categorytreename = 'Reporting Config'
         AND classifierid = 'Retailers') cl
          WHERE     p.payeeseq = par.payeeseq
         AND par.payeeseq = pos.payeeseq
         AND par.payeeseq = c.payeeseq
         AND pos.ruleelementownerseq = c.positionseq
         AND c.periodseq = prd.periodseq
        -- AND pos.ruleelementownerseq = c.positionseq
         AND i.periodseq = prd.periodseq
         and m.ruleelementownerseq=pos.managerseq
         and c.compensationdate between m.effectivestartdate and m.effectiveenddate-1
         and m.removedate=v_eot
         and mPar.payeeseq=m.payeeseq
         and mpar.removedate=v_eot
         and c.compensationdate between mpar.effectivestartdate and mpar.effectiveenddate-1
         and ct.datatypeseq=c.credittypeseq
         and ct.removedate=v_Eot
         AND p.removedate = v_eot
         AND par.removedate = v_eot
         AND pos.removedate = v_eot
         AND prd.removedate = v_eot
         AND c.positionseq = i.positionseq
         AND c.periodseq = i.periodseq
         AND c.processingunitseq = i.processingunitseq
         AND c.tenantid = i.tenantid
         AND i.name = 'I_External Prepaid_Retailer SIM Incentive_Payout'
          --AND m.name = 'PM_External Prepaid_Retailer SIM Incentive_Count'
         AND I.VALUE <> 0
         and c.compensationdate between p.effectivestartdate and p.effectiveenddate-1
         and c.compensationdate between par.effectivestartdate and par.effectiveenddate-1
         and c.compensationdate between pos.effectivestartdate and pos.effectiveenddate-1
         and ct.credittypeid  = 'Retailer SIM Incentive'
         AND c.periodseq = in_periodseq
         AND i.periodseq = in_periodseq
         AND c.processingunitseq = in_processingunitseq
         AND i.processingunitseq = in_processingunitseq
          group by
          m.payeeseq,
          m.ruleelementownerseq,
          m.name,
           cl.companyname,
          cl.regno,
        
          cl.rpt_footer,
          cl.frm1,
          mpar.userid,
          mpar.lastname
        
        
      );
    */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END