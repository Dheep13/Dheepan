CREATE PROCEDURE EXT.RPT_PICK_GO_PAYSUMMRY
(
    IN in_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_rpttype varchar2 */
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
    DECLARE v_eot TIMESTAMP = to_date('01-JAN-2200', 'dd-mon-yyyy');  /* ORIGSQL: v_eot DATE := TO_DATE('01-JAN-2200', 'dd-mon-yyyy') ; */

    DECLARE Proc_name VARCHAR(255) = 'RPT_PICK_GO_PAYSUMMRY';  /* ORIGSQL: Proc_name VARCHAR(255) := 'RPT_PICK_GO_PAYSUMMRY'; */
    DECLARE v_Tenant VARCHAR(4) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR(4) := 'STEL'; */
    --DECLARE v_period cs_period%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_period ROW LIKE cs_period;
    DECLARE v_calendarname VARCHAR(255);  /* ORIGSQL: v_calendarname VARCHAR(255); */
    DECLARE qtrname VARCHAR(255);  /* ORIGSQL: qtrname VARCHAR(255); */

    /* ORIGSQL: STEL_log (v_Tenant, Proc_name, 'Proc Started ' || ' PeriodSeq - ' || in_periodse(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :Proc_name, 'Proc Started '|| ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT *
    INTO
        v_period
    FROM
        cs_period
    WHERE
        removedate = :v_eot
        AND periodseq = :in_periodseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CALENDAR' not found */

    SELECT
        name
    INTO
        v_calendarname
    FROM
        cs_calendar
    WHERE
        removedate = :v_eot
        AND calendarseq = :v_period.calendarseq;

    SELECT
        name
    INTO
        qtrname
    FROM
        cs_period
    WHERE
        periodseq = :v_period.parentseq
        AND removedate = :v_eot;

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, Proc_name, 'Partitions ' || ' PeriodSeq - ' || in_periodseq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :Proc_name, 'Partitions '|| ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: stel_proc_rpt_partitions (in_periodseq, 'STEL_RPT_PICK_GO_PAYSUMMRY') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'EXT.STEL_RPT_PICK_GO_PAYSUMMRY');

    -- managing table partitions

    /* ORIGSQL: STEL_log (v_Tenant, Proc_name, 'Insert Started ' || ' PeriodSeq - ' || in_period(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :Proc_name, 'Insert Started '|| ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_PICK_GO_PAYSUMMRY' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_PICK_GO_PAYSUMMRY (AGENT, DEALERCODE, PRODUCT, PERIODNAME, (...) */
    INSERT INTO EXT.STEL_RPT_PICK_GO_PAYSUMMRY
        (
            AGENT, DEALERCODE, PRODUCT, PERIODNAME, PROCESSINGUNITNAME, PERIODSEQ,
            PROCESSINGUNITSEQ, PAYEESEQ, POSITIONSEQ, PAYOUT_COUNT, PAYOUT, AVERAGE,
            CLAW_BACK_ORDERS, CLAW_BACK, PRODUCT_TOTAL, TOTAL, QUARTERNAME, CalendarName,
            DATAPERIODNAME, DATAPERIODSEQ, PRODUCTGROUP, SORTBY, RATESYMBOL
        )
        SELECT   /* ORIGSQL: SELECT c.agent, c.dealercode, c.product, v_period.name periodname, c.processingu(...) */
            c.agent,
            c.dealercode,
            c.product,
            :v_period.name AS periodname,
            c.processingunitname,
            :v_period.periodseq AS periodseq,
            c.processingunitseq,
            c.payeeseq,
            c.positionseq,
            c.Payout_Count,
            c.Payout,
            c.average,
            c.Claw_back_Orders,
            c.Claw_back,
            Payout + Claw_back AS Product_total,
            m.VALUE AS total,
            :qtrname,
            :v_calendarname,
            c.periodname AS dataperiodname,
            c.periodseq AS dataperiodseq,
            CASE
                WHEN UPPER(product) LIKE '%SIM%ONLY%'
                THEN 'SIM Only'
                ELSE TRIM(REPLACE(UPPER(product), 'ADJUSTMENT', ''))  /* ORIGSQL: replace(upper(product),'ADJUSTMENT') */
            END
            AS productgroup,
            2 AS sortby,
            CASE
                WHEN UPPER(product) LIKE '%TOP%UP%'
                THEN '%'
                ELSE '$'
            END 
        FROM
            (
                SELECT   /* ORIGSQL: (SELECT agent, dealercode, product, periodname, processingunitname, periodseq, p(...) */
                    agent,
                    dealercode,
                    product,
                    periodname,
                    processingunitname,
                    periodseq,
                    processingunitseq,
                    payeeseq,
                    positionseq,
                    SUM(Payout_Count) AS Payout_Count,
                    SUM(Payout) AS Payout,
                    /* MAX (average)*/  average,
                    SUM(Claw_back_Orders) AS Claw_back_Orders,
                    SUM(Claw_back) AS Claw_back
                FROM
                    (
                        SELECT   /* ORIGSQL: (SELECT par.lastname Agent, pos.name DealerCode, CASE WHEN cf.name like '%Adjust(...) */
                            par.lastname AS Agent,
                            pos.name AS DealerCode,
                            CASE
                                WHEN cf.name LIKE '%Adjust%'

                                THEN REPLACE(cf.genericattribute1,'PICK '||IFNULL(CHAR(38),'') ||' GO ','') ||' Adjustment'   /* ORIGSQL: chr(38) */
                                ELSE cf.genericattribute3
                            END
                            AS Product,
                            pd.name AS PeriodName,
                            pu.name AS Processingunitname,
                            cf.periodseq,
                            pu.Processingunitseq,
                            pos.payeeseq,
                            pos.ruleelementownerseq AS positionseq,
                            CASE
                                WHEN SIGN(IFNULL(cf.genericnumber1, 0)) = 1  /* ORIGSQL: NVL(cf.genericnumber1, 0) */
                                THEN cf.genericnumber1
                                WHEN cf.name LIKE '%Adjustment%'
                                THEN 1
                                ELSE 0
                            END
                            AS Payout_Count,
                            CASE
                                WHEN SIGN(IFNULL(cf.genericnumber1, 0)) = 1  /* ORIGSQL: NVL(cf.genericnumber1, 0) */
                                OR cf.name LIKE '%Adjustment%'
                                THEN cf.VALUE
                                ELSE 0
                            END
                            AS Payout,
                            CASE
                                WHEN SIGN(IFNULL(cf.genericnumber1, 0)) = 1  /* ORIGSQL: NVL(cf.genericnumber1, 0) */
                                THEN cf.genericnumber2
                                ELSE 0
                            END
                            AS average,
                            CASE
                                WHEN SIGN(IFNULL(cf.genericnumber1, 0)) = -1  /* ORIGSQL: NVL(cf.genericnumber1, 0) */
                                THEN cf.genericnumber1
                                ELSE 0
                            END
                            AS Claw_back_Orders,
                            CASE
                                WHEN SIGN(IFNULL(cf.genericnumber1, 0)) = -1  /* ORIGSQL: NVL(cf.genericnumber1, 0) */
                                THEN cf.VALUE
                                ELSE 0
                            END
                            AS Claw_back
                        FROM
                            cs_credit cf,
                            cs_position pos,
                            cs_participant par,
                            cs_processingunit pu,
                            cs_period pd
                        WHERE
                            cf.name IN ('DC_Pick and Go Commissions','DC_Payment Adjustment','DCR_Pick and Go_Comm')
                            AND cf.periodseq = :in_periodseq /*IN
                                                                    (SELECT periodseq
                                                                           FROM cs_period
                                                                          WHERE removedate > SYSDATE
                                 AND parentseq =
                                                                                      v_period.parentseq
                             AND enddate <= v_period.enddate)*/
                            AND cf.periodseq = pd.periodseq
                            AND pd.removedate = :v_eot
                            AND pos.removedate = :v_eot
                            AND pos.effectivestartdate < :v_period.enddate
                            AND pos.effectiveenddate >= :v_period.enddate
                            AND pos.ruleelementownerseq = cf.positionseq
                            AND par.removedate = :v_eot
                            AND par.effectivestartdate < :v_period.enddate
                            AND par.effectiveenddate >= :v_period.enddate
                            AND par.payeeseq = cf.payeeseq
                            AND cf.processingunitseq = pu.processingunitseq
                    ) AS dbmtk_corrname_11626
                GROUP BY
                    agent, average,
                    dealercode,
                    product,
                    periodname,
                    processingunitname,
                    periodseq,
                    processingunitseq,
                    payeeseq,
                    positionseq
            ) AS c,
            (
                SELECT   /* ORIGSQL: (select SUM(value) value, periodseq, positionseq, payeeseq from cs_measurement w(...) */
                    SUM(value) AS value,
                    periodseq,
                    positionseq,
                    payeeseq
                FROM
                    cs_measurement
                WHERE
                    name IN ('PM_Payment Adjustment')
                    OR name LIKE 'PM%Pick%Go%Comm%' /*Arjun 0623*/
                GROUP BY
                    periodseq, positionseq, payeeseq
            ) AS m
        WHERE
            c.periodseq = m.periodseq
            AND c.positionseq = m.positionseq
            AND c.payeeseq = m.payeeseq
            AND c.periodseq = :in_periodseq
            AND c.payout <> 0 /*Atjun 0405*/;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, Proc_name, 'Classifier Insert Started ' || ' PeriodSeq - ' |(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :Proc_name, 'Classifier Insert Started '|| ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PIPELINERUN' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_PICK_GO_PAYSUMMRY (AGENT, DEALERCODE, PRODUCT, PERIODNAME, (...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    INSERT INTO EXT.STEL_RPT_PICK_GO_PAYSUMMRY
        (
            AGENT, DEALERCODE, PRODUCT, PERIODNAME, PROCESSINGUNITNAME, PERIODSEQ,
            PROCESSINGUNITSEQ, PAYEESEQ, POSITIONSEQ, PAYOUT_COUNT, PAYOUT, AVERAGE,
            CLAW_BACK_ORDERS, CLAW_BACK, PRODUCT_TOTAL, TOTAL, QUARTERNAME, CalendarName,
            DATAPERIODNAME, DATAPERIODSEQ, PRODUCTGROUP, sORTby
        )
        SELECT   /* ORIGSQL: select par.lastname, pos.name, 'Prior Balance '||REPLACE(earninggroupid,'Commiss(...) */
            par.lastname,
            pos.name,
            'Prior Balance '||IFNULL(REPLACE(earninggroupid,'Commission_',''),'') AS Product,
            :v_period.name,
            pu.name,
            :v_period.periodseq,
            pu.processingunitseq,
            b.payeeseq,
            b.positionseq,
            0 AS payoutcount,
            0 AS payout,
            0 AS average,
            0 AS clawbackorders,
            0 AS clawback,
            SUM(b.value) AS balance,
            SUM(b.value) AS balance,
            :v_period.name,
            :v_calendarname,
            :v_period.name,
            :v_period.periodseq,
            CASE
                WHEN earninggroupid = 'Commission_Music'
                THEN 'MUSIC'
                WHEN earninggroupid = 'Commission_Dash'
                THEN 'DASH'
                WHEN earninggroupid = 'Commission_SimOnly'
                THEN 'SIM ONLY'
                WHEN earninggroupid = 'Commissions_TopUpCards'
                THEN 'PREPAID TOP UP'
                WHEN earninggroupid = 'Commissions_Newsstand'
                THEN 'NEWSSTAND'
            END
            AS PRODGROUP,
            0
        FROM
            cs_balance b
        INNER JOIN
            cs_position pos
            ON pos.ruleelementownerseq = b.positionseq
            AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND TO_DATE(ADD_SECONDS(:v_period.enddate,(86400*-1))) BETWEEN pos.effectivestartdate AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_period.enddate-1 */
                                                                                                                                                             /* ORIGSQL: pos.effectiveenddate-1 */
        LEFT OUTER JOIN
            cs_participant par
            ON b.payeeseq = par.payeeseq
            AND par.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND TO_DATE(ADD_SECONDS(:v_period.enddate,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_period.enddate-1 */
                                                                                                                                                             /* ORIGSQL: par.effectiveenddate-1 */
        INNER JOIN
            cs_period pd
            ON pd.periodseq = b.periodseq
            AND pd.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND pd.startdate < :v_period.startdate
        INNER JOIN
            cs_processingunit pu
            ON pu.name = 'Singtel_PU'
            AND pu.processingunitseq = b.processingunitseq
        INNER JOIN
            cs_title t
            ON t.ruleelementownerseq = pos.titleseq
            AND t.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND t.name LIKE '%Pick%'
        WHERE
            (IFNULL(balancestatusid,'x') = 'posted'  /* ORIGSQL: nvl(balancestatusid,'x') */

                OR (IFNULL(balancestatusid,'x') = 'applied'  /* ORIGSQL: nvl(balancestatusid,'x') */
                    AND IFNULL(b.applypipelinerunseq,-123) IN  /* ORIGSQL: nvl(b.applypipelinerunseq,-123) */
                    (
                        SELECT   /* ORIGSQL: (select pipelinerunseq from cs_pipelinerun where periodseq=v_period.periodseq) */
                            pipelinerunseq
                        FROM
                            cs_pipelinerun
                        WHERE
                            periodseq = :v_period.periodseq
                    )
                )
            )
        GROUP BY
            pos.name,b.positionseq, b.payeeseq,
            CASE
                WHEN earninggroupid = 'Commission_Music'
                THEN 'MUSIC'
                WHEN earninggroupid = 'Commission_Dash'
                THEN 'DASH'
                WHEN earninggroupid = 'Commission_SimOnly'
                THEN 'SIM ONLY'
                WHEN earninggroupid = 'Commissions_TopUpCards'
                THEN 'PREPAID TOP UP'
                WHEN earninggroupid = 'Commissions_Newsstand'
                THEN 'NEWSSTAND'
            END
            , pu.processingunitseq, pu.name,REPLACE(earninggroupid,'Commission_',''), par.lastname;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Below code added for req. memo table update

    /* ORIGSQL: execute immediate 'Truncate table STEL_CLASSIFIER_TAB'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_CLASSIFIER_TAB' not found */

    /* ORIGSQL: Truncate table STEL_CLASSIFIER_TAB ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STEL_CLASSIFIER_TAB';

    /* ORIGSQL: insert into STEL_CLASSIFIER_TAB select * from STEL_CLASSIFIER; */
    INSERT INTO EXT.STEL_CLASSIFIER_TAB
        SELECT   /* ORIGSQL: select * from STEL_CLASSIFIER; */
            *
        FROM
            EXT.STEL_CLASSIFIER;

    -- Add debug Log for Process End
    /* ORIGSQL: STEL_log (v_Tenant, Proc_name, 'Proc Finished ' || ' PeriodSeq - ' || in_periods(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :Proc_name, 'Proc Finished '|| ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END