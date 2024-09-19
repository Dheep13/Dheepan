CREATE PROCEDURE EXT.RPT_POST_EXTPPRETAILERPMT
(
    IN p_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_rpttype VARCHAR2 */
    IN p_periodseq DECIMAL(38,10),   /* ORIGSQL: p_periodseq NUMBER */
    IN p_processingunitseq DECIMAL(38,10)   /* ORIGSQL: p_processingunitseq NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    --DECLARE v_calendar cs_calendar%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_calendar' not found (for %ROWTYPE declaration) */
    DECLARE v_calendar ROW LIKE cs_calendar;
    DECLARE v_pu VARCHAR(255);  /* ORIGSQL: v_pu varchar2(255); */
    DECLARE v_eot TIMESTAMP = to_date('01-jan-2200', 'dd-mon-yyyy');  /* ORIGSQL: v_eot DATE := TO_DATE('01-jan-2200', 'dd-mon-yyyy') ; */

    --v_period     cs_period%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_period ROW LIKE cs_period;
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT *
    INTO
        v_period
    FROM
        cs_period
    WHERE
        periodseq = :p_periodseq
        AND removedate = :v_eot;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CALENDAR' not found */

    SELECT *
    INTO
        v_calendar
    FROM
        cs_calendar
    WHERE
        Calendarseq = :v_period.calendarseq
        AND removedate = :v_eot;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_pu
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :p_processingunitseq;

    /* ORIGSQL: dbms_output.put_line('in postproc'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('in postproc'); 

    /* ORIGSQL: delete from stel_rpt_epretpay where periodseq=p_periodseq; */
    DELETE
    FROM
        ext.stel_rpt_epretpay
    WHERE
        periodseq = :p_periodseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_EPRETPAY' not found */

    /* ORIGSQL: insert into stel_rpt_epretpay (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIODNAM(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    INSERT INTO ext.stel_rpt_epretpay
        (
            TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIODNAME, PROCESSINGUNITNAME, CALENDARNAME,
            POSITIONSEQ, PAYEESEQ, DISTCODE, DISTNAME, RETCODE, RETNAME,
            PRODUCT, TOTALUNITS, TIERACH, RATEAPPLIED, PAYOUT, CREATEDATE,
            totalunitsoverall, oldmgr, payoutgroup, currmgr, currmgrname
        )
        SELECT   /* ORIGSQL: SELECT c.TENANTID, c.PERIODSEQ, c.PROCESSINGUNITSEQ, v_period.name PERIODNAME, v(...) */
            c.TENANTID,
            c.PERIODSEQ,
            c.PROCESSINGUNITSEQ,
            :v_period.name AS PERIODNAME,
            :v_pu AS PROCESSINGUNITNAME,
            :v_calendar.name AS CALENDARNAME,
            c.POSITIONSEQ,
            c.PAYEESEQ,
            NULL AS DISTCODE,
            NULL AS DISTNAME,
            p.name AS RETCODE,
            NULL AS RETNAME,
            'Hi Card' AS PRODUCT,
            SUM(c.genericnumber1) AS TOTALUNITS,
            0 AS TIER,
            c.genericnumber2 AS RATEAPPLIED,
            0 AS FINALPAYOUT,
            CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
            0 AS TotalOVerallUnits,
            oldmgr.name AS RolledUpToDist,
            SUM(c.value) AS payoutgroup,
            currMGr.name,
            NULL
        FROM
            cs_Credit c
        INNER JOIN
            cs_period pd
            ON pd.periodseq = c.periodseq
            AND pd.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        INNER JOIN
            cs_calendar cl
            ON cl.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND cl.calendarseq = pd.calendarseq
        INNER JOIN
            cs_position p
            ON p.ruleelementownerseq = c.positionseq
            AND p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND c.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate,(86400*-1)))   /* ORIGSQL: p.effectiveenddate-1 */
        INNER JOIN
            cs_position pCurr
            ON pCurr.ruleelementownerseq = c.positionseq
            AND pCurr.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN pCurr.effectivestartdate AND TO_DATE(ADD_SECONDS(pCurr.effectiveenddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
                                                                                                                                                           /* ORIGSQL: pCurr.effectiveenddate-1 */
        INNER JOIN
            cs_salestransaction st
            ON c.salestransactionseq = st.salestransactionseq
            AND c.compensationdate = st.compensationdate
            /*  join cs_transactionassignment ta
               on c.salestransactionseq=ta.salestransactionseq
             and c.compensationdate=ta.compensationdate
             and ta.positionname = p.name
             */
        INNER JOIN
            cs_credittype ct
            ON ct.datatypeseq = c.credittypeseq
            AND ct.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        LEFT OUTER JOIN
            cs_position oldMgr
            ON oldMgr.ruleelementownerseq = p.managerseq  ---ta.genericattribute2
            AND oldMgr.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND c.compensationdate BETWEEN oldMgr.effectivestartdate AND TO_DATE(ADD_SECONDS(oldMgr.effectiveenddate,(86400*-1)))   /* ORIGSQL: oldMgr.effectiveenddate-1 */
        LEFT OUTER JOIN
            cs_position currMgr
            ON currMgr.ruleelementownerseq = pCurr.managerseq  ---ta.genericattribute2
            AND currMgr.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN currMgr.effectivestartdate AND TO_DATE(ADD_SECONDS(currMgr.effectiveenddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
                                                                                                                                                               /* ORIGSQL: currMgr.effectiveenddate-1 */
        WHERE
            ct.credittypeid = 'Retailer SIM Incentive'
            AND c.tenantid = 'STEL'
            AND c.processingunitseq = :p_processingunitseq
            AND c.periodseq = :p_periodseq
            AND st.tenantid = 'STEL'
            AND st.processingunitseq = :p_processingunitseq
            --and ta.tenantid='STEL'
            --and ta.processingunitseq=p_processingunitseq
            AND oldmgr.name IN
            (
                SELECT   /* ORIGSQL: (select dim0 from stel_lookup where name like 'LT_ExternalPrepaid_EligibleVendor(...) */
                    dim0
                FROM
                    ext.stel_lookup
                WHERE
                    name LIKE 'LT_ExternalPrepaid_EligibleVendors'
                    AND dim1 = 'Retailer Incentive'
            )
        GROUP BY
            c.TENANTID,
            c.PERIODSEQ,
            c.PROCESSINGUNITSEQ,
            c.POSITIONSEQ,
            c.PAYEESEQ,
            p.name,

            c.genericnumber2,
            oldmgr.name, currmgr.name;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_rpt_epretpay tgt using (SELECT m.positionseq, m.value AS totalun(...) */
    MERGE INTO ext.stel_rpt_epretpay AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select m.positionseq, m.value totalunits, i.value finalpayout, m.periodseq FROM(...) */
                m.positionseq,
                m.value AS totalunits,
                i.value AS finalpayout,
                m.periodseq
            FROM
                cs_measurement m
            INNER JOIN
                cs_incentive i
                ON m.positionseq = i.positionseq
                AND m.periodseq = i.periodseq
                AND m.processingunitseq = i.processingunitseq
                AND m.tenantid = i.tenantid
                AND i.name = 'I_External Prepaid_Retailer SIM Incentive_Payout'
            WHERE
                m.name = 'PM_External Prepaid_Retailer SIM Incentive_Count'
                AND m.tenantid = 'STEL'
                AND m.processingunitseq = :p_processingunitseq
                AND m.periodseq = :p_periodseq
        ) AS src
        ON (tgt.positionseq = src.positionseq 
        	AND tgt.periodseq = :p_periodseq
            AND tgt.processingunitseq = :p_processingunitseq
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.totalunitsoverall = src.totalunits
            ,tgt.payout = src.finalpayout, tgt.payoutgroup =
            CASE
                WHEN src.finalpayout = 0
                THEN 0
                ELSE tgt.payoutgroup
            END
        --WHERE
        --    tgt.periodseq = :p_periodseq
        --    AND tgt.processingunitseq = :p_processingunitseq
            ;--FROM STEL_RPT_DATA_EXTPPRETAILERPMT
    --where finalpayout<>0 ;

    /* ORIGSQL: dbms_output.put_line('inserted data'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('inserted data'); 

    /* ORIGSQL: delete from stel_rpt_epretpay tgt where tgt.periodseq=p_periodseq AND tgt.proces(...) */
    DELETE
    FROM
        ext.stel_rpt_epretpay
        tgt
    WHERE
        tgt.periodseq = :p_periodseq
        AND tgt.processingunitseq = :p_processingunitseq
        AND payoutgroup = 0;

    /**Retailer Name**/
    -- USe Retailer to look up cs_pariticipant/cs_payee, (Retailer=PAyeeID) , and put LASTNAME into the RETAILERNAME field 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO STEL_RPT_EPRETPAY curr_m USING (SELECT P.PAYEESEQ, PRD.PERIODSEQ, P.P(...) */
    MERGE INTO EXT.STEL_RPT_EPRETPAY AS curr_m
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (SELECT P.PAYEESEQ, PRD.PERIODSEQ, P.PAYEEID, PAR.LASTNAME FROM cs_participant p(...) */
                P.PAYEESEQ,
                PRD.PERIODSEQ,
                P.PAYEEID,
                PAR.LASTNAME
            FROM
                cs_participant par,
                cs_payee p,
                cs_period prd
            WHERE
                prd.removedate = :v_eot
                AND par.effectivestartdate < prd.enddate
                AND par.effectiveenddate >= prd.enddate
                AND par.removedate = :v_eot
                AND p.effectivestartdate < prd.enddate
                AND p.effectiveenddate >= prd.enddate
                AND p.removedate = :v_eot
                AND P.PAYEESEQ = PAR.PAYEESEQ
                AND prd.periodseq = :p_periodseq
        ) AS res
        ON (curr_m.payeeSeq = res.payeeSeq
            AND curr_m.periodSeq = :p_periodseq
        AND CURR_M.retcode = res.payeeid)
    WHEN MATCHED THEN
        UPDATE SET CURR_M.retNAME = res.LASTNAME;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_EPRETPAY tgt using (SELECT p.ruleelementownerseq, p.name AS (...) */
    MERGE INTO EXT.STEL_RPT_EPRETPAY AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select p.ruleelementownerseq, p.name mgrname, par.lastname from cs_position p j(...) */
                p.ruleelementownerseq,
                p.name AS mgrname,
                par.lastname
            FROM
                cs_position p
            INNER JOIN
                cs_participant par
                ON par.payeeseq = p.payeeseq
                AND par.removedate = :v_eot
            INNER JOIN
                cs_period pd
                ON pd.removedate = :v_eot
            WHERE
                pd.periodseq = :p_periodseq
                AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
                                                                                                                                                       /* ORIGSQL: p.effectiveenddate-1 */
                AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1))) --and pd.enddate-1 between m.effectivestartdate and m.effectiveenddate-1
                /* ORIGSQL: pd.enddate-1 */
                /* ORIGSQL: par.effectiveenddate-1 */
                AND p.removedate = :v_eot
                --and m.removedate=v_eot
        ) AS Src
        ON(tgt.oldmgr = src.mgrname)--replace positionseq
    WHEN MATCHED THEN
        UPDATE SET tgt.distcode = src.mgrname, tgt.distname = src.lastname;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_EPRETPAY tgt using (SELECT p.ruleelementownerseq, p.name AS (...) */
    MERGE INTO EXT.STEL_RPT_EPRETPAY AS tgt   
        USING
        (
            SELECT   /* ORIGSQL: (select p.ruleelementownerseq, p.name mgrname, par.lastname from cs_position p j(...) */
                p.ruleelementownerseq,
                p.name AS mgrname,
                par.lastname
            FROM
                cs_position p
            INNER JOIN
                cs_participant par
                ON par.payeeseq = p.payeeseq
                AND par.removedate = :v_eot
            INNER JOIN
                cs_period pd
                ON pd.removedate = :v_eot
            WHERE
                pd.periodseq = :p_periodseq
                AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
                                                                                                                                                       /* ORIGSQL: p.effectiveenddate-1 */
                AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1))) --and pd.enddate-1 between m.effectivestartdate and m.effectiveenddate-1
                /* ORIGSQL: pd.enddate-1 */
                /* ORIGSQL: par.effectiveenddate-1 */
                AND p.removedate = :v_eot
                --and m.removedate=v_eot
        ) AS Src
        ON(tgt.currMgr = src.mgrname)--replace positionseq
    WHEN MATCHED THEN
        UPDATE SET tgt.currMgrName = src.lastname;
    /*
      MERGE INTO STEL_RPT_EPRETPAY curr_m
      USING (SELECT P.PAYEESEQ,
                  PRD.PERIODSEQ,
                  P.PAYEEID,
                  PAR.LASTNAME
             FROM cs_participant par, cs_payee p, cs_period prd
            WHERE     prd.removedate = v_eot
         AND par.effectivestartdate < prd.enddate
         AND par.effectiveenddate >= prd.enddate
         AND par.removedate = v_eot
         AND p.effectivestartdate < prd.enddate
         AND p.effectiveenddate >= prd.enddate
         AND p.removedate = v_eot
         AND P.PAYEESEQ = PAR.PAYEESEQ
     AND prd.periodseq = p_periodseq) res
         ON (    curr_m.payeeSeq = res.payeeSeq
         AND curr_m.periodSeq = p_periodseq
     AND CURR_M.distcode = res.payeeid)
       WHEN MATCHED
       THEN
    UPDATE SET CURR_M.DISTNAME = res.LASTNAME;
    */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: dbms_output.put_line('updated names'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('updated names');

    /**Fianl payout**/
    --
    -- Units*RateApplied

    /*   UPDATE STEL_RPT_EPRETPAY
          SET PAYOUT = (TOTALUNITS * RATEAPPLIED)
        WHERE PERIODSEQ = p_periodseq;
    
       COMMIT;
    */

    /**Tier Achieved**/

    /*
    PMR_External Prepaid_Incentive for Retailers
     - 310
    
    Vendor - PositionNAme
    
     select dim1 tier, dim0 Vendor, effectivestartdate, effectiveenddate, substr(dim1,1,instr(dim1,'-')-1), substr(dim1,instr(dim1,'-')+1,100)
    from stel_lookup where name='LT External Prepaid Incentive Payout'
    
    and VENDORTOTALUNITS >=substr(dim1,1,instr(dim1,'-')-1)
    and VENDORTOTALUNITS< substr(dim1,instr(dim1,'-')+1,100)
    and Vendor = PositionName
    
      */

    -- Need to confirm below code once actual data is available.
    /*
      merge into STEL_RPT_EPRETPAY tgt
      using
      (
            select e.positionseq, e.periodseq,  v1.dim1 tier
            from STEL_RPT_EPRETPAY e
            join cs_position p
            on e.positionseq=p.ruleelementownerseq and p.removedate>sysdate and p.effectiveenddate>sysdate
            join  stel_lookup V1
            on v1.name ='LT External Prepaid Incentive Payout'
         AND e.TOTALUNITS >=
                                   SUBSTR (dim1, 1, INSTR (dim1, '-') - 1)
         AND e.TOTALUNITS <
                                   SUBSTR (dim1, INSTR (dim1, '-') + 1, 100)
         AND V1.dim0 = nvl(p.genericattribute16,'SET 1')
        
      ) src
      on (tgt.positionseq=src.positionseq and tgt.periodseq=src.periodseq)
      when matched then update
      set TIERACH = tier
      where periodseq=p_periodseq
      ;
      */   

    /* ORIGSQL: update STEL_RPT_EPRETPAY tgt SET (RATEUNIT, COMPANYNAME, REGNO) = (SELECT 'SGD',(...) */
    UPDATE EXT.STEL_RPT_EPRETPAY tgt
        SET
        /* ORIGSQL: (RATEUNIT, COMPANYNAME, REGNO) = */
        (RATEUNIT
            , COMPANYNAME
            , REGNO) = (
                SELECT   /* ORIGSQL: (select 'SGD', genericattribute1, genericattribute2 from stel_Classifier where c(...) */
                    'SGD',
                    genericattribute1,
                    genericattribute2
                FROM
                    ext.stel_Classifier
                WHERE
                    categorytreename = 'Reporting Config'
                    AND categoryname = 'RequisitionMomo-External Prepaid'
                    AND classifierid = 'External Prepaid-ALL'
            )
        WHERE
            periodseq = :p_periodseq;

    /*
      SET TIERACH =
             (SELECT V1.dim1 tier
                    --, dim0 Vendor, effectivestartdate, effectiveenddate, substr(dim1,1,instr(dim1,'-')-1) MinUnits, substr(dim1,instr(dim1,'-')+1,100) MaxUnits
                    FROM stel_lookup V1
                   WHERE name = 'LT External Prepaid Incentive Payout'
         AND m1.TOTALUNITS >=
                               SUBSTR (dim1, 1, INSTR (dim1, '-') - 1)
         AND m1.TOTALUNITS <
                               SUBSTR (dim1, INSTR (dim1, '-') + 1, 100)
     AND V1.dim0 = m1.DISTCODE)
    WHERE M1.PERIODSEQ = p_periodseq;
    */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: COMMIT; */
    COMMIT;
END