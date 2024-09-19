CREATE PROCEDURE EXT.STEL_PRC_DSPAYSUMADMIN
(
    IN in_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_rpttype IN VARCHAR2 */
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
    DECLARE v_tenant VARCHAR(10) = 'STEL';  /* ORIGSQL: v_tenant VARCHAR2(10) := 'STEL'; */
    DECLARE v_stmsg VARCHAR(10) = 'START';  /* ORIGSQL: v_stmsg VARCHAR2(10) := 'START'; */
    DECLARE v_edmsg VARCHAR(10) = 'END';  /* ORIGSQL: v_edmsg VARCHAR2(10) := 'END'; */
    DECLARE v_componentname VARCHAR(100) = NULL;  /* ORIGSQL: v_componentname VARCHAR2(100) := NULL; */
    DECLARE v_periodstartdate TIMESTAMP;  /* ORIGSQL: v_periodstartdate DATE; */
    DECLARE v_calendarname VARCHAR(100) = NULL;  /* ORIGSQL: v_calendarname VARCHAR2(100) := NULL; */
    DECLARE v_periodname VARCHAR(100) = NULL;  /* ORIGSQL: v_periodname VARCHAR2(100) := NULL; */
    DECLARE v_processingunitname VARCHAR(100) = NULL;  /* ORIGSQL: v_processingunitname VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200','MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200','MM/DD/YYYY') ; */

    DECLARE v_rowcount DECIMAL(38,10);  /* ORIGSQL: v_rowcount NUMBER; */
    DECLARE v_enddate TIMESTAMP;  /* ORIGSQL: v_enddate DATE; */

    v_componentname = 'STEL_PRC_DSPAYSUMADMIN';

    -- Add debug Log for Process START
    /* ORIGSQL: stel_log(v_tenant,v_componentname,v_stmsg || ' PeriodSeq - ' || in_periodseq || (...) */
    CALL EXT.STEL_LOG(:v_tenant, :v_componentname, IFNULL(:v_stmsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        calendarname,
        periodname,
        startdate,
        enddate
    INTO
        v_calendarname,
        v_periodname,
        v_periodstartdate,
        v_enddate
    FROM
        cs_periodcalendar
    WHERE
        periodseq = :in_periodseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_processingunitname
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :in_processingunitseq;

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE stel_rpt_dspaysumadmin WHERE periodseq = in_periodseq AND processingunits(...) */
    DELETE
    FROM
        ext.stel_rpt_dspaysumadmin
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: stel_proc_rpt_partitions(in_periodseq,'STEL_RPT_DSPAYSUMADMIN') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'EXT.STEL_RPT_DSPAYSUMADMIN');

    -- managing table partitions
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DSPAYSUMADMIN' not found */

    /* ORIGSQL: INSERT INTO stel_rpt_dspaysumadmin (tenantid, periodseq, processingunitseq, peri(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    INSERT INTO ext.stel_rpt_dspaysumadmin
        (
            tenantid,
            periodseq,
            processingunitseq,
            periodname,
            processingunitname,
            calendarname,
            positionseq,
            payeeseq,
            groupname,
            geid,
            staffname,
            repcode,
            jobtitle,
            indpoints,
            indconn,
            totalind,
            teampoints,
            teamconn,
            totalteam,
            teamprop,
            total,
            excessacv,
            multirate,
            otc,
            gross_pay,
            adj1,
            adj2,
            ceadjustment,
            threshold,
            finalpay,
            remarks,
            createdate
        )
        --v_CalendarName, v_PeriodName, v_periodstartdate

        SELECT   /* ORIGSQL: SELECT DISTINCT pos.tenantid, in_periodseq, in_processingunitseq, v_periodname p(...) */
            DISTINCT
            pos.tenantid,
            :in_periodseq,
            :in_processingunitseq,
            :v_periodname AS periodname,
            :v_processingunitname AS processingunitname,
            :v_calendarname AS calendarname,
            pos.ruleelementownerseq AS positionseq,
            pos.payeeseq AS payeeseq,
            pos.genericattribute1 AS groupname,
            /* -- pos.name geid, Commented by AK on 18.04.2019. As per new mapping received. */
            pye.payeeid AS geid,
            par.lastname AS staffname,
            par.genericattribute1 AS repcode,
            t.name AS jobtitle,
            0 AS indpoints,
            0 AS indconn,
            0 AS totalind,
            0 AS teampoints,
            0 AS teamconn,
            0 AS totalteam,
            0 AS teamprop,
            0 AS total,
            0 AS excessacv,
            0 AS multirate,
            0 AS otc,
            0 AS gross_pay,
            0 AS adj1,
            0 AS adj2,
            0 AS ceadjustment,
            NULL AS threshold,
            0 AS finalpay,
            NULL AS remarks,
            CURRENT_TIMESTAMP AS createdate  /* ORIGSQL: SYSDATE */
        FROM
            cs_position pos
        INNER JOIN
            cs_participant par
            ON pos.payeeseq = par.payeeseq
        INNER JOIN
            cs_title t
            ON t.ruleelementownerseq = pos.titleseq
        INNER JOIN
            cs_payee pye
            ON pye.payeeseq = pos.payeeseq
        WHERE
            pye.removedate = :v_eot
            AND t.removedate = :v_eot
            AND pos.removedate = :v_eot
            AND par.removedate = :v_eot
            AND par.removedate = :v_eot
            AND pos.effectivestartdate < :v_enddate
            AND pos.effectiveenddate >= :v_enddate
            AND par.effectivestartdate < :v_enddate
            AND par.effectiveenddate >= :v_enddate
            AND pye.effectivestartdate < :v_enddate
            AND pye.effectiveenddate >= :v_enddate
            AND t.effectivestartdate < :v_enddate
            AND t.effectiveenddate >= :v_enddate
            AND t.name LIKE '%Direct%Sales%';

    /* ORIGSQL: COMMIT; */
    COMMIT;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%rowcount */

    /* ORIGSQL: stel_log(v_tenant,v_componentname,'Insert 1:' || v_rowcount || ' PeriodSeq - ' |(...) */
    CALL EXT.STEL_LOG(:v_tenant, :v_componentname, 'Insert 1:'|| IFNULL(TO_VARCHAR(:v_rowcount),'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    -- Indiv Points %, Team Points  

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_measurement m WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_measurement m WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_measurement m
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    --  'SMR_Internal_Multiplier_ExcessAchievement'
                    'SM_Internal_Monthly_Achievement'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.indpoints = src.value * 100,
            tgt.teampoints = src.genericnumber4 * 100;

    -- Indiv conn %  

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_measurement m WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_measurement m WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_measurement m
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'SMR_Internal_ConnCount_AverageAchievement'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.indconn = src.value * 100;

    -- Total Ind % 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_incentive inc WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_incentive inc WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_incentive inc
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'I_Internal_Multiplier Commission'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.totalind = src.genericnumber1 * 100;

    -- Indiv Points %, Team Points  

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_measurement m WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_measurement m WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_measurement m
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'SM_Internal_Monthly_Achievement'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.indpoints = src.value * 100,
            tgt.teampoints = src.genericnumber4 * 100;

    -- Team Connection % 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_measurement m WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_measurement m WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_measurement m
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'SMR_Internal_ConnCount_Team Ach'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.teamconn = src.value * 100;

    -- Total Team % 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_incentive inc WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_incentive inc WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_incentive inc
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'I_Internal_Multiplier Commission_Team'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.totalteam = src.genericnumber1 * 100;
    -- Team Proration (%) -- Per chat with Arun on 18.4.2019 it should be blank for now.
    -- Pending to convert into number.
    --    MERGE INTO stel_rpt_dspaysumadmin tgt USING (
        --                                                   SELECT
        --                                                       *
        --                                                   FROM
        --                                                       cs_incentive inc
        --                                                   WHERE
        --                                                       periodseq = in_periodseq
        -- AND processingunitseq = in_processingunitseq
        -- AND name IN (
            --                                                           'I_INTERNAL_M4_Payout'
        --                                                       )
    --                                               )
    --    src ON ( src.positionseq = tgt.positionseq
        -- AND src.periodseq = tgt.periodseq
        -- AND src.payeeseq = tgt.payeeseq
    -- AND src.processingunitseq = tgt.processingunitseq )
    --    WHEN MATCHED THEN UPDATE SET tgt.TEAMPROP = src.genericattribute3 * 100;

    -- Total (%) 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_incentive inc WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_incentive inc WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_incentive inc
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'I_Internal_Multiplier Commission_Team'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.total = src.genericnumber1 * 100;

    -- Excess Achievement for Multiplier Multiplier Rate, otc 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_incentive inc WHER(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_incentive inc WHERE periodseq = in_periodseq AND processinguni(...) */
                *
            FROM
                cs_incentive inc
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'I_Internal_Multiplier Commission'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.excessacv = src.genericnumber3 * 100,
            tgt.multirate = src.genericnumber4 * 100,
            tgt.otc = src.genericnumber5;

    -- Gross Pay 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT positionseq, periodseq, paye(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT positionseq, periodseq, payeeseq, processingunitseq, SUM(value) incvalue(...) */
                positionseq,
                periodseq,
                payeeseq,
                processingunitseq,
                SUM(value) AS incvalue
            FROM
                cs_incentive inc
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'IR_Directsales_SAA Award_Payout',
                    'I_Internal_Multiplier Commission',
                    'I_Internal_Multiplier Commission_Team',
                    'I_INTERNAL_M4_Payout'
                )
            GROUP BY
                positionseq,
                periodseq,
                payeeseq,
                processingunitseq
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.gross_pay = src.incvalue;

    -- Gross2 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT positionseq, periodseq, paye(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT positionseq, periodseq, payeeseq, processingunitseq, SUM(inc.genericnumb(...) */
                positionseq,
                periodseq,
                payeeseq,
                processingunitseq,
                SUM(inc.genericnumber1 * inc.genericnumber4) AS incvalue
            FROM
                cs_incentive inc
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'I_Internal_Indv_M2_Payout'
                )
            GROUP BY
                positionseq,
                periodseq,
                payeeseq,
                processingunitseq
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.gross2 = src.incvalue;

    -- Adj1 (%) 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_deposit dep WHERE (...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSIT' not found */
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_deposit dep WHERE periodseq = in_periodseq AND processingunits(...) */
                *
            FROM
                cs_deposit dep
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'DR_Payment Adjustment'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.adj1 = src.value,
            tgt.adj2 = 0;

    -- CE Adjustment 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT * FROM cs_deposit dep WHERE (...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT * FROM cs_deposit dep WHERE periodseq = in_periodseq AND processingunits(...) */
                *
            FROM
                cs_deposit dep
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'D_CSI_CE_Adjustment'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.ceadjustment = src.value;

    -- Threshold Met? 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT positionseq, periodseq, paye(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT positionseq, periodseq, payeeseq, processingunitseq, CASE WHEN nvl(inc.g(...) */
                positionseq,
                periodseq,
                payeeseq,
                processingunitseq,
                CASE
                    WHEN IFNULL(inc.genericnumber5,0) = 0  /* ORIGSQL: nvl(inc.genericnumber5,0) */
                    THEN 'N'
                    ELSE 'Y'
                END
                AS trvalue
            FROM
                cs_incentive inc
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'I_Internal_Indv_M2_Payout'
                )
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.threshold = src.trvalue;

    -- Final Pay 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_dspaysumadmin tgt USING (SELECT positionseq, periodseq, paye(...) */
    MERGE INTO ext.stel_rpt_dspaysumadmin AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (SELECT positionseq, periodseq, payeeseq, processingunitseq, SUM(value) depvalue(...) */
                positionseq,
                periodseq,
                payeeseq,
                processingunitseq,
                SUM(value) AS depvalue
            FROM
                cs_deposit dep
            WHERE
                periodseq = :in_periodseq
                AND processingunitseq = :in_processingunitseq
                AND name IN (
                    'DR_Internal_Multiplier Commission_R2',
                    'D_CSI_CE_Adjustment',
                    'DR_Payment Adjustment',
                    'D_CSI_SHCC /FTA',
                    'D_Directsales_SAA Award_Payout',
                    'D_Internal_Indv_M2_Payout',
                    'DR_INTERNAL_M4_Payout',
                    'DR_Internal_Multiplier Commission_R2_Team'
                )
            GROUP BY
                positionseq,
                periodseq,
                payeeseq,
                processingunitseq
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
            AND src.payeeseq = tgt.payeeseq
        AND src.processingunitseq = tgt.processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET tgt.finalpay = src.depvalue;

    -- Deleting director data for monthly report. After discussion with comp added below condition by AK on 23.04.2019
    IF SUBSTRING(:v_periodname,1,3) NOT IN ('Mar',  /* ORIGSQL: substr(v_periodname,1,3) */
        'Jul',
        'Sep',
    'Dec')
    THEN 
        /* ORIGSQL: DELETE stel_rpt_dspaysumadmin WHERE periodseq = in_periodseq AND processingunits(...) */
        DELETE
        FROM
            ext.stel_rpt_dspaysumadmin
        WHERE
            periodseq = :in_periodseq
            AND processingunitseq = :in_processingunitseq
            AND jobtitle = 'DirectSales - Director';

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END IF;

    -- Add debug Log for Process END
    /* ORIGSQL: stel_log(v_tenant,v_componentname,v_edmsg || ' PeriodSeq - ' || in_periodseq || (...) */
    CALL EXT.STEL_LOG(:v_tenant, :v_componentname, IFNULL(:v_edmsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END