CREATE PROCEDURE EXT.RPT_PAYMENT_SUMM
(
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
    DECLARE v_Tenant VARCHAR(10) = 'LGAP';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'LGAP'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'RPT_PAYMENT_SUMM';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

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
        SUBSTRING(x.NAME,1,2),  /* ORIGSQL: SUBSTR(x.NAME,1,2) */
        PERIODTYPESEQ
    INTO
        V_QTR,
        V_PERIODTYPESEQ
    FROM
        CS_PERIOD x
    WHERE
        x.PERIODSEQ  
        =
        (
            SELECT   /* ORIGSQL: (SELECT y.PARENTSEQ FROM CS_PERIOD y WHERE y.PERIODSEQ =IN_PERIODSEQ) */
                y.PARENTSEQ
            FROM
                CS_PERIOD y
            WHERE
                y.PERIODSEQ = :IN_PERIODSEQ
        )
        AND x.removedate >CURRENT_TIMESTAMP;  /* ORIGSQL: sysdate */

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
        PARENTSEQ
    INTO
        V_QTRPERIODSEQ
    FROM
        CS_PERIOD
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */

    ----MIN. THRESHOLD VALUE:

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_FIXEDVALUE' not found */
    SELECT
        FV.VALUE
    INTO
        V_FV1
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
        AND FV.NAME LIKE 'FV_Advt_Min Achievement%'
        AND FV.PERIODTYPESEQ
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODTYPE' not found */
        IN
        (
            SELECT   /* ORIGSQL: (SELECT PERIODTYPESEQ FROM CS_PERIODTYPE WHERE NAME ='quarter') */
                PERIODTYPESEQ
            FROM
                CS_PERIODTYPE
            WHERE
                NAME = 'quarter'
        )
        AND FV.MODELSEQ = 0
        AND C.CALENDARSEQ = :V_CALENDARSEQ;

    -----EXTRA INPUT:    
    SELECT
        FV.VALUE
    INTO
        V_FV2
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
        AND FV.NAME = 'FV_Advt_Revenue Multiplier above Min Achievement'
        AND FV.PERIODTYPESEQ  
        IN
        (
            SELECT   /* ORIGSQL: (SELECT PERIODTYPESEQ FROM CS_PERIODTYPE WHERE NAME ='quarter') */
                PERIODTYPESEQ
            FROM
                CS_PERIODTYPE
            WHERE
                NAME = 'quarter'
        )
        AND FV.MODELSEQ = 0
        AND C.CALENDARSEQ = :V_CALENDARSEQ;

    -----THRESHOLD PAYMENT:   

    SELECT
        FV.VALUE
    INTO
        V_FV3
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
        AND FV.NAME LIKE 'FV_Advt_Min Threshold Payment%'
        AND FV.PERIODTYPESEQ  
        IN
        (
            SELECT   /* ORIGSQL: (SELECT PERIODTYPESEQ FROM CS_PERIODTYPE WHERE NAME ='quarter') */
                PERIODTYPESEQ
            FROM
                CS_PERIODTYPE
            WHERE
                NAME = 'quarter'
        )
        AND FV.MODELSEQ = 0
        AND C.CALENDARSEQ = :V_CALENDARSEQ;

    ------QUARTERLY CAP:   

    SELECT
        FV.VALUE
    INTO
        V_FV4
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
        AND FV.NAME LIKE 'FV_Rev Achv Cap'
        AND FV.PERIODTYPESEQ  
        IN
        (
            SELECT   /* ORIGSQL: (SELECT PERIODTYPESEQ FROM CS_PERIODTYPE WHERE NAME ='quarter') */
                PERIODTYPESEQ
            FROM
                CS_PERIODTYPE
            WHERE
                NAME = 'quarter'
        )
        AND FV.MODELSEQ = 0
        AND C.CALENDARSEQ = :V_CALENDARSEQ;

    -----YEARLY CAP:   

    SELECT
        FV.VALUE
    INTO
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
        AND FV.NAME = 'FV_Rev Acc Achv Cap'
        AND FV.PERIODTYPESEQ  
        IN
        (
            SELECT   /* ORIGSQL: (SELECT PERIODTYPESEQ FROM CS_PERIODTYPE WHERE NAME ='year') */
                PERIODTYPESEQ
            FROM
                CS_PERIODTYPE
            WHERE
                NAME = 'year'
        )
        AND FV.MODELSEQ = 0
        AND C.CALENDARSEQ = :V_CALENDARSEQ;

    ----DELETE EXISTING DATA:

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_ADV_PAY_SUM' not found */

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
    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_ADV_PAY_SUM' not found */

    /* ORIGSQL: insert into STELEXT.STEL_RPT_ADV_PAY_SUM (TENANTID, PAYEESEQ, POSITIONSEQ, TITLE(...) */
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
            QTRPARENTSEQ
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
            :V_QTRPERIODSEQ
        FROM
            CS_INCENTIVE I,
            EXT.STEL_POSPART_MASTER M
        WHERE
            I.PAYEESEQ = M.PAYEESEQ
            AND I.POSITIONSEQ = M.POSITIONSEQ
            AND I.PERIODSEQ = M.PERIODSEQ
            AND M.PERIODSEQ = :IN_PERIODSEQ
            AND M.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND I.GENERICATTRIBUTE1 IN ('Advt_Yearly_Accelerator','Advt_M1_Indv','Advt_M2_Team/SA','Advt_M3_AR')
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

    /* ORIGSQL: UPDATE STELEXT.STEL_RPT_ADV_PAY_SUM PSR SET ACTUAL_YEARLY_ACCELERATOR = (SELECT (...) */
    UPDATE EXT.STEL_RPT_ADV_PAY_SUM PSR
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
        SET
        /* ORIGSQL: ACTUAL_YEARLY_ACCELERATOR = */
        ACTUAL_YEARLY_ACCELERATOR = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt YTD Achv' THEN MES.GENERICNUMBER(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt YTD Achv'
                        THEN MES.GENERICNUMBER1
                        ELSE 0
                    END
                )
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        ), 
        /* ORIGSQL: TARGET_YEARLY_ACCELERATOR = */
        TARGET_YEARLY_ACCELERATOR = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt YTD Achv' THEN MES.GENERICNUMBER(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt YTD Achv'
                        THEN MES.GENERICNUMBER2
                        ELSE 0
                    END
                )
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        ), 
        /* ORIGSQL: YEARLY_ACHIEVED_PERCENT = */
        YEARLY_ACHIEVED_PERCENT = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt YTD Achv' THEN (MES.VALUE*100) E(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt YTD Achv'
                        THEN (MES.VALUE*100)
                        ELSE 0
                    END
                )
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        ), 
        /* ORIGSQL: MONTHLY_OTI = */
        MONTHLY_OTI = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt_Monthly OTI' THEN MES.VALUE ELSE(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt_Monthly OTI'
                        THEN MES.VALUE
                        ELSE 0
                    END
                )
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt_Monthly OTI'
        ), 
        /* ORIGSQL: QTRLY_IND_ACTUAL_TARGET = */
        QTRLY_IND_ACTUAL_TARGET = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt_Qtrly Rev Achv_Payable' THEN MES(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
                        THEN MES.GENERICNUMBER2
                        ELSE 0
                    END
                )
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        ), 
        /* ORIGSQL: QTRLY_IND_TARGET_ACHIEVED = */
        QTRLY_IND_TARGET_ACHIEVED = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt_Qtrly Rev Achv_Payable' THEN MES(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
                        THEN MES.GENERICNUMBER3
                        ELSE 0
                    END
                )
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        ), 
        /* ORIGSQL: QTRLY_ACHIEVED_PERCENT = */
        QTRLY_ACHIEVED_PERCENT = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt_Qtrly Rev Achv_Payable' THEN ((M(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
                        THEN ((MES.GENERICNUMBER4)*100)
                        ELSE 0
                    END
                )
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        ), 
        /* ORIGSQL: PRORATED_WORKING_DAYS = */
        PRORATED_WORKING_DAYS = (
            SELECT   /* ORIGSQL: (SELECT (CASE WHEN MES.GENERICATTRIBUTE1 ='Advt_Yearly Proration' THEN (SUBSTR(M(...) */
                (
                    CASE
                        WHEN MES.GENERICATTRIBUTE1 = 'Advt_Yearly Proration'
                        THEN (IFNULL(substring(MES.GENERICATTRIBUTE2,0,3),'') ||'/'||IFNULL(substring(MES.GENERICATTRIBUTE3,0,3),''))  /* ORIGSQL: SUBSTR(MES.GENERICATTRIBUTE3,0,3) */
                                                                                                                                                                                                /* ORIGSQL: SUBSTR(MES.GENERICATTRIBUTE2,0,3) */
                        ELSE '0'
                    END
                ) 
            FROM
                CS_MEASUREMENT MES
            WHERE
                MES.PERIODSEQ = PSR.PERIODSEQ
                AND MES.PAYEESEQ = PSR.PAYEESEQ
                AND MES.POSITIONSEQ = PSR.POSITIONSEQ
                AND MES.GENERICATTRIBUTE1 = 'Advt_Yearly Proration'
        );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END