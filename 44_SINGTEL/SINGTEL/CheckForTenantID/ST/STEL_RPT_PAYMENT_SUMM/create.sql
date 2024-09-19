CREATE PROCEDURE EXT.STEL_RPT_PAYMENT_SUMM
(
    IN IN_RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: IN_RPTTYPE IN VARCHAR2 */
    IN IN_PERIODSEQ BIGINT,   /* ORIGSQL: IN_PERIODSEQ INTEGER */
    IN IN_PROCESSINGUNITSEQ BIGINT     /* ORIGSQL: IN_PROCESSINGUNITSEQ INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_TENANTID VARCHAR(255) = 'STEL';  /* ORIGSQL: V_TENANTID VARCHAR2(255) := 'STEL'; */
    DECLARE V_CALENDARNAME VARCHAR(255) = NULL;  /* ORIGSQL: V_CALENDARNAME VARCHAR2(255) := NULL; */
    DECLARE V_CALENDARSEQ BIGINT;  /* ORIGSQL: V_CALENDARSEQ INTEGER; */
    DECLARE V_QTR VARCHAR(255);  /* ORIGSQL: V_QTR VARCHAR2(255); */
    DECLARE V_PUNAME VARCHAR(255);  /* ORIGSQL: V_PUNAME VARCHAR2(255); */
    DECLARE V_PERIODTYPESEQ BIGINT;  /* ORIGSQL: V_PERIODTYPESEQ INTEGER; */
    DECLARE V_FV1 DECIMAL(38,10);  /* ORIGSQL: V_FV1 NUMBER; */
    DECLARE V_FV2 DECIMAL(38,10);  /* ORIGSQL: V_FV2 NUMBER; */
    DECLARE V_FV3 DECIMAL(38,10);  /* ORIGSQL: V_FV3 NUMBER; */
    DECLARE V_FV4 DECIMAL(38,10);  /* ORIGSQL: V_FV4 NUMBER; */
    DECLARE V_FV5 DECIMAL(38,10);  /* ORIGSQL: V_FV5 NUMBER; */
    DECLARE V_QTRPERIODSEQ BIGINT;  /* ORIGSQL: V_QTRPERIODSEQ INTEGER; */
    DECLARE v_Tenant VARCHAR(10) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'STEL'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    DECLARE v_Total_WrkDays BIGINT;  /* ORIGSQL: v_Total_WrkDays NUMBER(10); */
    DECLARE v_yearstartdate TIMESTAMP;  /* ORIGSQL: v_yearstartdate DATE; */
    DECLARE v_periodenddate TIMESTAMP;  /* ORIGSQL: v_periodenddate DATE; */
    DECLARE v_periodstartdate TIMESTAMP;  /* ORIGSQL: v_periodstartdate DATE; */

    v_ComponentName = 'STEL_RPT_PAYMENT_SUMM';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:V_TENANTID, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CALENDAR' not found */

    SELECT
        NAME,
        CALENDARSEQ
    INTO
        V_CALENDARNAME,
        V_CALENDARSEQ
    FROM
        CS_CALENDAR
    WHERE
        NAME = 'Singtel Monthly Calendar';

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT
        SUBSTRING(NAME,1,2),  /* ORIGSQL: SUBSTR(NAME,1,2) */
        PERIODTYPESEQ,
        PERIODSEQ
    INTO
        V_QTR,
        V_PERIODTYPESEQ,
        V_QTRPERIODSEQ
    FROM
        CS_PERIOD x
    WHERE
        x.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        AND x.PERIODSEQ  
        =
        (
            SELECT   /* ORIGSQL: (SELECT y.PARENTSEQ FROM CS_PERIOD y WHERE y.PERIODSEQ =IN_PERIODSEQ and y.remov(...) */
                y.PARENTSEQ
            FROM
                CS_PERIOD y
            WHERE
                y.PERIODSEQ = :IN_PERIODSEQ
                AND y.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        );

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        NAME
    INTO
        V_PUNAME
    FROM
        CS_PROCESSINGUNIT
    WHERE
        PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ; 

    SELECT
        m.startdate,
        m.enddate,
        yr.startdate
    INTO
        v_periodstartdate,
        v_periodenddate,
        v_yearstartdate
    FROM
        cs_period m,
        cs_period yr
    WHERE
        m.periodseq = IN_PERIODSEQ
        AND m.removedate = v_eot
        AND yr.removedate = v_eot
        AND yr.startdate < m.enddate
        AND yr.enddate > m.startdate
        AND m.calendarseq = yr.calendarseq
        AND yr.periodtypeseq
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODTYPE' not found */
        =
        (
            SELECT   /* ORIGSQL: (SELECT periodtypeseq FROM cs_periodtype WHERE name = 'year' AND removedate = v_(...) */
                periodtypeseq
            FROM
                cs_periodtype
            WHERE
                name = 'year'
                AND removedate = :v_eot
        );

    -- CAP and Threshold
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_FIXEDVALUE' not found */
    SELECT
        MAX(
            CASE 
                WHEN fv.name = 'FV_Advt_Min Achievement %'
                THEN FV.VALUE
                ELSE NULL
            END
        ),
        MAX(
            CASE 
                WHEN fv.name = 'FV_Advt_Revenue Multiplier above Min Achievement'
                THEN FV.VALUE
                ELSE NULL
            END
        ),
        MAX(
            CASE 
                WHEN fv.name = 'FV_Advt_Min Threshold Payment%'
                THEN FV.VALUE
                ELSE NULL
            END
        ),
        MAX(
            CASE 
                WHEN fv.name = 'FV_Rev Achv Cap'
                THEN FV.VALUE
                ELSE NULL
            END
        ),
        MAX(
            CASE 
                WHEN fv.name = 'FV_Rev Acc Achv Cap'
                THEN FV.VALUE
                ELSE NULL
            END
        )
    INTO
        V_FV1,
        V_FV2,
        V_FV3,
        V_FV4,
        V_FV5
    FROM
        CS_FIXEDVALUE FV,
        CS_PERIOD PER,
        CS_CALENDAR C
    WHERE
        FV.EFFECTIVESTARTDATE < PER.ENDDATE
        AND PER.CALENDARSEQ = C.CALENDARSEQ
        AND FV.EFFECTIVEENDDATE >= PER.ENDDATE
        AND FV.REMOVEDATE = to_date('01/01/2200','dd/mm/yyyy')  /* ORIGSQL: to_date('01/01/2200','dd/mm/yyyy') */
        AND PER.REMOVEDATE = to_date('01/01/2200','dd/mm/yyyy')  /* ORIGSQL: to_date('01/01/2200','dd/mm/yyyy') */
        AND PER.PERIODSEQ = :IN_PERIODSEQ
        AND FV.NAME IN ('FV_Advt_Min Achievement %', 'FV_Advt_Revenue Multiplier above Min Achievement',
        'FV_Advt_Min Threshold Payment%', 'FV_Rev Achv Cap', 'FV_Rev Acc Achv Cap')
        AND FV.PERIODTYPESEQ  
        IN
        (
            SELECT   /* ORIGSQL: (SELECT PERIODTYPESEQ FROM CS_PERIODTYPE WHERE NAME in ('quarter', 'year')) */
                PERIODTYPESEQ
            FROM
                CS_PERIODTYPE
            WHERE
                NAME IN ('quarter', 'year')
        )
        AND FV.MODELSEQ = 0
        AND C.CALENDARSEQ = :V_CALENDARSEQ;

    ----DELETE EXISTING DATA: 

    /* ORIGSQL: DELETE FROM STEL_RPT_ADV_PAY_SUM where PERIODSEQ = in_PeriodSeq AND PROCESSINGUN(...) */
    DELETE
    FROM
        EXT.STEL_RPT_ADV_PAY_SUM
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_ADV_PAY_SUM') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_ADV_PAY_SUM');

    -- managing table partitions

    ---------------------------------

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_ADV_PAY_SUM' not found */

    /* ORIGSQL: insert into STEL_RPT_ADV_PAY_SUM (TENANTID, PAYEESEQ, POSITIONSEQ, TITLESEQ, CAL(...) */
    INSERT INTO EXT.STEL_RPT_ADV_PAY_SUM
        (
            TENANTID,
            PAYEESEQ,
            POSITIONSEQ,
            TITLESEQ,
            CALENDARSEQ,
            CALENDARNAME,
            PERIODSEQ,
            PERIODNAME,
            MANAGERSEQ,
            SALESMANAGER,
            LINEMANAGER,
            TITLE,
            JOININGDATE,
            TERMINATIONDATE,
            ACTUAL_YEARLY_ACCELERATOR,
            TARGET_YEARLY_ACCELERATOR,
            YEARLY_ACHIEVED_PERCENT,
            YEARLY_PAYOUT,
            MONTHLY_OTI,
            QUARTERLY_OTI,
            M1_INDIVIDUAL_TARGET,
            QTRLY_IND_ACTUAL_TARGET,
            QTRLY_IND_TARGET_ACHIEVED,
            QTRLY_ACHIEVED_PERCENT,
            QTRLY_PAYOUT,
            QUARTER,
            QTR_MIN_THRESHOLD,
            QTR_MIN_THRESHOLD_EXTRA,
            THRESHOLD_PAYMENT,
            YEARLY_CAP,
            QTRLY_CAP,
            PRORATED_WORKING_DAYS,
            TEAM_ACHIEVEMENT,
            M2_COMPUTED_PAYOUT,
            M2_PRORATED_PAYOUT,
            M2_FINAL_PAYOUT,
            M3_COMPUTED_PAYOUT,
            M3_PRORATED_PAYOUT,
            M3_FINAL_PAYOUT,
            PAYOUTS,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            QTRPARENTSEQ,
            METRIC1_PERC
        )
        SELECT   /* ORIGSQL: (SELECT V_TENANTID AS TENENTID, M.PAYEESEQ AS PAYEESEQ, M.POSITIONSEQ AS POSITIO(...) */
            :V_TENANTID AS TENENTID,
            M.PAYEESEQ AS PAYEESEQ,
            M.POSITIONSEQ AS POSITIONSEQ,
            M.TITLESEQ AS TITLESEQ,
            :V_CALENDARSEQ AS CALENDARSEQ,
            :V_CALENDARNAME AS CALENDARNAME,
            M.PERIODSEQ AS PERIODSEQ,
            M.PERIODNAME AS PERIODNAME,
            M.MANAGERSEQ AS MANAGERSEQ,
            M.PAYEENAME AS PAYEENAME,
            M.LINEMANAGER AS LINEMANAGER,
            (
                CASE
                    WHEN M.TITLE = 'Advt-Director'
                    THEN 'Director'
                    WHEN M.TITLE = 'Advt-TL'
                    THEN 'Team Lead'
                    WHEN M.TITLE = 'Advt-SAM'
                    THEN 'Sales Account Manager'
                END
            ) AS TITLE,
            M.JOININGDATE AS JOININGDATE,
            M.TERMINATIONDATE AS TERMINATIONDATE,
            NULL AS ACTUAL_YEARLY_ACCELERATOR,
            NULL AS TARGET_YEARLY_ACCELERATOR,
            NULL AS YEARLY_ACHIEVED_PERCENT,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator'
                    THEN I.VALUE
                    ELSE 0
                END
            ) AS YEARLY_PAYOUT,
            NULL AS MONTHLY_OTI,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    THEN I.GENERICNUMBER1
                    ELSE 0
                END
            ) AS QUARTERLY_OTI,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    THEN (I.GENERICNUMBER1 * I.GENERICNUMBER2)
                    ELSE 0
                END
            ) AS M1_INDIVIDUAL_TARGET,
            NULL AS QTRLY_IND_ACTUAL_TARGET,
            NULL AS QTRLY_IND_TARGET_ACHIEVED,
            NULL AS QTRLY_ACHIEVED_PERCENT,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    THEN I.VALUE
                    ELSE 0
                END
            ) AS QTRLY_PAYOUT,
            :V_QTR AS QUARTERS,
            IFNULL(TO_VARCHAR(:V_FV1*100),'') || '%' AS QTR_MIN_THRESHOLD,
            IFNULL(TO_VARCHAR(:V_FV2*100),'') || '%' AS QTR_MIN_THRESHOLD_EXTRA,
            IFNULL(TO_VARCHAR(:V_FV3*100),'') || '%' AS THRESHOLD_PAYMENT,
            IFNULL(TO_VARCHAR(:V_FV5*100),'') || '%' AS YEARLY_CAP,
            IFNULL(TO_VARCHAR(:V_FV4*100),'') || '%' AS QTRLY_CAP,
            NULL AS WORKINGDAYS,
            SUM((
                    CASE 
                        WHEN I.GENERICATTRIBUTE1 = 'Advt_M2_Team/SA'
                        THEN I.GENERICNUMBER3
                        ELSE 0
                    END
            )*100) AS TEAM_ACHIEVEMENT,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_M2_Team/SA'
                    THEN (I.GENERICNUMBER1*I.GENERICNUMBER2)
                    ELSE 0
                END
            ) AS M2_COMPUTED_PAYOUT,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_M2_Team/SA'
                    THEN (I.GENERICNUMBER1*I.GENERICNUMBER2*I.GENERICNUMBER5)
                    ELSE 0
                END
            ) AS M2_PRORATED_PAYOUT,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_M2_Team/SA'
                    THEN (I.VALUE)
                    ELSE 0
                END
            ) AS M2_FINAL_PAYOUT,
            SUM((
                    CASE 
                        WHEN I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                        THEN (I.GENERICNUMBER1*I.GENERICNUMBER2*I.GENERICNUMBER3)
                        ELSE 0
                    END
            )) AS M3_COMPUTED_PAYOUT,
            SUM((
                    CASE 
                        WHEN I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                        THEN (I.GENERICNUMBER1*I.GENERICNUMBER2*I.GENERICNUMBER3*I.GENERICNUMBER5)
                        ELSE 0
                    END
            )) AS M3_PRORATED_PAYOUT,
            SUM((
                    CASE 
                        WHEN I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                        THEN (I.VALUE)
                        ELSE 0
                    END
            )) AS M3_FINAL_PAYOUT,
            NULL AS PAYOUTS,
            :IN_PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
            :V_PUNAME AS PROCESSINGUNITNAME,
            :V_QTRPERIODSEQ,
            SUM((
                    CASE 
                        WHEN I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                        THEN GENERICNUMBER2
                        ELSE 0
                    END
            )* 100) AS METRIC1_PERC
        FROM
            CS_INCENTIVE I,
            EXT.STEL_POSPART_MASTER M
        WHERE
            I.PAYEESEQ = M.PAYEESEQ
            AND I.POSITIONSEQ = M.POSITIONSEQ
            AND I.PERIODSEQ = M.PERIODSEQ
            AND M.PERIODSEQ = :IN_PERIODSEQ
            AND M.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND I.GENERICATTRIBUTE1 IN ('Advt_Yearly_Accelerator','Advt_M1_Indv','Advt_M2_Team/SA','Advt_M3_AR', 'Advt_M3_PR')
        GROUP BY
            :V_TENANTID,
            M.PAYEESEQ,
            M.POSITIONSEQ,
            M.TITLESEQ,
            :V_CALENDARSEQ,
            :V_CALENDARNAME,
            M.PERIODSEQ,
            M.PERIODNAME,
            M.MANAGERSEQ,
            M.PAYEENAME,
            M.LINEMANAGER,
            TITLE,
            M.JOININGDATE,
            M.TERMINATIONDATE,
            :V_QTR,
            IFNULL(TO_VARCHAR(:V_FV1*100),'') || '%',
            IFNULL(TO_VARCHAR(:V_FV2*100),'') || '%',
            IFNULL(TO_VARCHAR(:V_FV3*100),'') || '%',
            IFNULL(TO_VARCHAR(:V_FV5*100),'') || '%',
            IFNULL(TO_VARCHAR(:V_FV4*100),'') || '%',
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            :V_QTRPERIODSEQ
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    ----UPDATION OF MEASUREMENT:
    -------------------------------   

    /* ORIGSQL: UPDATE STEL_RPT_ADV_PAY_SUM PSR SET (ACTUAL_YEARLY_ACCELERATOR, TARGET_YEARLY_AC(...) */
    UPDATE EXT.STEL_RPT_ADV_PAY_SUM PSR
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
        SET
        /* ORIGSQL: (ACTUAL_YEARLY_ACCELERATOR, TARGET_YEARLY_ACCELERATOR, YEARLY_ACHIEVED_PERCENT) (...) */
        (ACTUAL_YEARLY_ACCELERATOR, TARGET_YEARLY_ACCELERATOR, YEARLY_ACHIEVED_PERCENT) = (
            SELECT   /* ORIGSQL: (SELECT MES.GENERICNUMBER1, MES.GENERICNUMBER2, (MES.VALUE*100) FROM CS_MEASUREM(...) */
                MES.GENERICNUMBER1,
                MES.GENERICNUMBER2,
                (MES.VALUE*100)
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND mes.processingunitseq = psr.processingunitseq
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        )
    WHERE
        psr.periodseq = :IN_PERIODSEQ
        AND psr.processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE STEL_RPT_ADV_PAY_SUM PSR SET MONTHLY_OTI = (SELECT MES.VALUE FROM CS_MEAS(...) */
    UPDATE EXT.STEL_RPT_ADV_PAY_SUM PSR 
        SET
        /* ORIGSQL: MONTHLY_OTI = */
        MONTHLY_OTI = (
            SELECT   /* ORIGSQL: (SELECT MES.VALUE FROM CS_MEASUREMENT MES WHERE MES.PERIODSEQ = PSR.PERIODSEQ AN(...) */
                MES.VALUE
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND mes.processingunitseq = psr.processingunitseq
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt_Monthly OTI'
        )
    WHERE
        psr.periodseq = :IN_PERIODSEQ
        AND psr.processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE STEL_RPT_ADV_PAY_SUM PSR SET (QTRLY_IND_ACTUAL_TARGET, QTRLY_IND_TARGET_A(...) */
    UPDATE EXT.STEL_RPT_ADV_PAY_SUM PSR 
        SET
        /* ORIGSQL: (QTRLY_IND_ACTUAL_TARGET, QTRLY_IND_TARGET_ACHIEVED, QTRLY_ACHIEVED_PERCENT) = */
        (QTRLY_IND_ACTUAL_TARGET, QTRLY_IND_TARGET_ACHIEVED, QTRLY_ACHIEVED_PERCENT) = (
            SELECT   /* ORIGSQL: (SELECT MES.GENERICNUMBER2, MES.GENERICNUMBER3, MES.GENERICNUMBER4*100 FROM CS_M(...) */
                MES.GENERICNUMBER2,
                MES.GENERICNUMBER3,
                MES.GENERICNUMBER4*100
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND mes.processingunitseq = psr.processingunitseq
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        )
    WHERE
        psr.periodseq = :IN_PERIODSEQ
        AND psr.processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    v_Total_WrkDays = EXT.STEL_GET_TOTAL_WORKINGDAYS(:IN_PERIODSEQ, 'Advertising');  /* ORIGSQL: STEL_GET_TOTAL_WORKINGDAYS(in_PeriodSeq, 'Advertising') */

    /* ORIGSQL: UPDATE STEL_RPT_ADV_PAY_SUM PSR SET (prorated_working_days) = (SELECT to_char(su(...) */
    UPDATE EXT.STEL_RPT_ADV_PAY_SUM PSR
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_VARIABLEASSIGNMENT' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_VARIABLE' not found */
        SET
        /* ORIGSQL: (prorated_working_days) = */
        (prorated_working_days) = (
            (
                SELECT   /* ORIGSQL: (select to_char(sum(fv.value)) || ' / '|| to_char(v_Total_WrkDays) from cs_fixed(...) */
                    IFNULL(TO_VARCHAR(SUM(fv.value)),'') || ' / '|| IFNULL(TO_VARCHAR(:v_Total_WrkDays),'')   /* ORIGSQL: to_char(v_Total_WrkDays) */
                FROM
                    cs_fixedvalue fv,
                    cs_variableassignment va,
                    cs_variable var
                WHERE
                    fv.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND va.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND var.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND fv.modelseq = 0
                    AND va.modelseq = 0
                    AND var.modelseq = 0
                    AND va.variableseq = var.ruleelementseq
                    AND var.name = 'FVV_Working Days Actual'
                    AND VA.ASSIGNMENTSEQ = FV.RULEELEMENTSEQ
                    AND va.effectivestartdate < :v_periodenddate
                    AND va.effectiveenddate > :v_periodstartdate
                    AND var.effectivestartdate < :v_periodenddate
                    AND var.effectiveenddate > :v_periodstartdate
                    AND fv.effectivestartdate < :v_periodenddate
                    AND fv.effectiveenddate > :v_yearstartdate
                    AND psr.positionseq = va.ruleelementownerseq
                    AND psr.periodseq = :IN_PERIODSEQ
                GROUP BY
                    va.ruleelementownerseq
            )
        )
    WHERE
        psr.periodseq = :IN_PERIODSEQ
        AND psr.processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:V_TENANTID, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END