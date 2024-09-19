CREATE PROCEDURE EXT.RPT_ADVT_IND_PS
(
    IN in_rpttype VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                  /* ORIGSQL: in_rpttype varchar2 */
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
    DECLARE v_calendarseq BIGINT;  /* ORIGSQL: v_calendarseq INTEGER; */
    DECLARE V_FV5 DECIMAL(38,10);  /* ORIGSQL: V_FV5 number; */
    DECLARE v_period ROW LIKE CS_PERIOD;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_periodtypeseq_yr BIGINT;
    DECLARE v_periodtypeseq_mon BIGINT;
    DECLARE v_periodtypeseq_qtr BIGINT;
    
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CALENDAR' not found */

select * from cs_tenant;

    SELECT
        NAME,
        CALENDARSEQ
    INTO
        v_Calendarname,
        v_calendarseq
    FROM
        CS_CALENDAR
    WHERE
        NAME = 'Singtel Monthly Calendar';

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_FIXEDVALUE' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */
    SELECT
        (FV.VALUE*100)
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
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIODTYPE' not found */
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
        AND C.CALENDARSEQ = :v_calendarseq;

    SELECT *
    INTO
        v_period
    FROM
        cs_period
    WHERE
        periodseq = :IN_PERIODSEQ
        --and calendarseq=2251799813685251
        AND removedate >CURRENT_TIMESTAMP;  /* ORIGSQL: sysdate */

    /* ORIGSQL: DELETE FROM EXT.STEL_RPT_ADV_IND_PS WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSEQ = IN_PROCESSINGUNITSEQ; */
    DELETE
    FROM
        EXT.STEL_RPT_ADV_IND_PS
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: EXT.STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_ADV_IND_PS') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'STEL_RPT_ADV_IND_PS');

    -- managing table partitions

    ---insertion of Incentive values for section -1
    ------------------------------------------------

    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_INCENTIVE' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_POSPART_MASTER' not found */
    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_ADV_IND_PS (TENANTID, PAYEESEQ, POSITIONSEQ, PERIODSEQ, CALENDARSEQ, PROCESSINGUNITSEQ, PROCESSINGUNIT, PAYEEID, CALENDARNAME, SECTION, HIREDATE, FIRSTNAME, MIDDLENAME, LASTNAME, (...) */
    INSERT INTO ext.STEL_RPT_ADV_IND_PS
        (
            TENANTID,
            PAYEESEQ,
            POSITIONSEQ,
            PERIODSEQ,
            CALENDARSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNIT,
            PAYEEID,
            CALENDARNAME,
            SECTION,
            HIREDATE,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            POSITIONNAME,
            COMMISSION,
            INDIVIUALTARGET_M1,
            TEAMTARGET_M2,
            AR_M3,
            /* ---ANNUAL_TARGET , */
            PERIODNAME,
            M1_WEIGHTAGE,
            M2_WEIGHTAGE,
            M3_WEIGHTAGE,
            YEARLY_CAP,
            TITLESEQ,
            TITLENAME,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, P.PAYEESEQ, P.POSITIONSEQ, P.PERIODSEQ, v_calendarseq, IN_PROCESSINGUNITSEQ, P.PROCESSINGUNITNAME, P.PAYEEID, v_Calendarname AS CALENDARNAME, '1', P.HIREDATE, P.FIRSTNAME, P.MIDDLE(...) */
            :v_Tenantid,
            P.PAYEESEQ,
            P.POSITIONSEQ,
            P.PERIODSEQ,
            :v_calendarseq,
            :IN_PROCESSINGUNITSEQ,
            P.PROCESSINGUNITNAME,
            P.PAYEEID,
            :v_Calendarname AS CALENDARNAME,
            '1',
            P.HIREDATE,
            P.FIRSTNAME,
            P.MIDDLENAME,
            P.LASTNAME,
            P.POSITIONNAME,
            SUM(
                CASE 
                    WHEN (I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    AND P.periodname NOT LIKE 'March%')
                    THEN I.GENERICNUMBER1
                    WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA')
                    AND P.periodname LIKE 'March%')
                    THEN I.GENERICNUMBER1
                    ELSE 0
                END
            ) AS commission_OTI,
            SUM(
                CASE 
                    WHEN (I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    AND P.periodname NOT LIKE 'March%')
                    THEN I.GENERICNUMBER1
                    WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA')
                    AND P.periodname LIKE 'March%')
                    THEN I.GENERICNUMBER1
                    ELSE 0
                END
                ) * SUM(
                CASE
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    THEN I.GENERICNUMBER2
                    ELSE 0
                END
            ) AS INDIVIDUALTARGET_M1,
            SUM(
                CASE 
                    WHEN (I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    AND P.periodname NOT LIKE 'March%')
                    THEN 0
                    WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA')
                    AND P.periodname LIKE 'March%')
                    THEN I.GENERICNUMBER1
                    ELSE 0
                END
                ) * SUM(
                CASE
                    WHEN I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA')
                    THEN I.GENERICNUMBER2
                    ELSE 0
                END
            ) AS TEAMTARGET_M2,
            SUM(
                CASE 
                    WHEN (I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                    AND P.periodname NOT LIKE 'March%')
                    THEN 0
                    WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA')
                    AND P.periodname LIKE 'March%')
                    THEN I.GENERICNUMBER1
                    ELSE 0
                END
                ) * SUM(
                CASE
                    WHEN I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                    THEN I.GENERICNUMBER2
                    ELSE 0
                END
            ) AS AR_M3,
            /* --P.SALARY, */
            /* --SUM(CASE WHEN  I.GENERICATTRIBUTE1 ='Advt_M1_Indv' THEN (I.GENERICNUMBER1 * I.GENERICNUMBER2) ELSE 0 END) AS INDIVIDDUALTARGET_M1, */
            /* --SUM((CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA') THEN (I.GENERICNUMBER1 * I.GENERICNUMBER2) ELSE 0 END)) AS TEAMTARGET_M2, */
            /* --SUM((CASE WHEN  I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR') THEN  (I.GENERICNUMBER1 * I.GENERICNUMBER2) ELSE 0 END)) AS AR_M3, */
            /* ---SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER6 ELSE 0 END)) AS ANNUAL_TARGET, */
            P.PERIODNAME AS PERIODNAME,
            IFNULL(TO_VARCHAR(SUM((
                            CASE 
                                WHEN I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
                                THEN I.GENERICNUMBER2
                                ELSE 0
                            END
            )*100)),'') || '%' AS M1_WEIGHTAGE,
            IFNULL(TO_VARCHAR(SUM(((
                                CASE 
                                    WHEN I.GENERICATTRIBUTE1 IN('Advt_M2_Team/SA','Advt_M2_SA')
                                    THEN I.GENERICNUMBER2
                                    ELSE 0
                                END
            )*100))),'') || '%' AS M2_WEIGHTAGE,
            IFNULL(TO_VARCHAR(SUM(((
                                CASE 
                                    WHEN I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                                    THEN I.GENERICNUMBER2
                                    ELSE 0
                                END
            )*100))),'') || '%' AS M3_WEIGHTAGE,
            /* --SUM((CASE WHEN  I.GENERICATTRIBUTE1 ='Advt_Yearly_Accelerator' THEN I.GENERICNUMBER3 ELSE 0  END)*100)|| '%' AS YEARLY_CAP, */
            IFNULL(TO_VARCHAR(:V_FV5),'')||'%' AS YEARLY_CAP,
            P.TITLESEQ AS TITLESEQ,
            P.TITLE AS TITLENAME,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            CS_INCENTIVE I,
            EXT.STEL_POSPART_MASTER P
        WHERE
            I.PAYEESEQ = P.PAYEESEQ
            AND I.POSITIONSEQ = P.POSITIONSEQ
            AND I.PERIODSEQ = P.PERIODSEQ
            AND P.PERIODSEQ = :IN_PERIODSEQ
            AND P.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND I.GENERICATTRIBUTE1 IN('Advt_M1_Indv','Advt_M2_Team/SA','Advt_M3_AR','Advt_Yearly_Accelerator','Advt_M2_SA','Advt_M3_PR')
        GROUP BY
            P.PAYEESEQ,
            P.POSITIONSEQ,
            P.PERIODSEQ,
            P.PROCESSINGUNITNAME,
            P.PAYEEID,
            '1',
            P.HIREDATE,
            P.FIRSTNAME,
            P.MIDDLENAME,
            P.LASTNAME,
            P.POSITIONNAME,
            P.PERIODNAME,
            IFNULL(TO_VARCHAR(:V_FV5),'')||'%',
            P.TITLESEQ,
            P.TITLE
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    ---updation of measurement values in section- 1
    ------------------------------------------------

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_ADV_IND_PS' not found */
    /* ORIGSQL: UPDATE EXT.STEL_RPT_ADV_IND_PS T SET MIN_THRESHOLD_PERCENT = (SELECT M.GENERICATTRIBUTE7 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 =(...) */
    UPDATE EXT.STEL_RPT_ADV_IND_PS T
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_MEASUREMENT' not found */
        SET
        /* ORIGSQL: MIN_THRESHOLD_PERCENT = */
        MIN_THRESHOLD_PERCENT = (
            SELECT   /* ORIGSQL: (SELECT M.GENERICATTRIBUTE7 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable') */
                M.GENERICATTRIBUTE7
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        ), 
        /* ORIGSQL: MIN_THRESHOLD_EXTRA_PERCENT = */
        MIN_THRESHOLD_EXTRA_PERCENT = (
            SELECT   /* ORIGSQL: (SELECT M.GENERICATTRIBUTE4 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable') */
                M.GENERICATTRIBUTE4
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        ), 
        /* ORIGSQL: QUARTERLY_CAP = */
        QUARTERLY_CAP = (
            SELECT   /* ORIGSQL: (SELECT M.GENERICATTRIBUTE6 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable') */
                M.GENERICATTRIBUTE6
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        ), 
        /* ORIGSQL: THRESHOLD_PAYMENT = */
        THRESHOLD_PAYMENT = (
            SELECT   /* ORIGSQL: (SELECT M.GENERICATTRIBUTE5 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable') */
                M.GENERICATTRIBUTE5
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
        ), 
        /* ORIGSQL: ANNUAL_TARGET = */
        ANNUAL_TARGET = (
            SELECT   /* ORIGSQL: (SELECT M.GENERICNUMBER2 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv') */
                M.GENERICNUMBER2
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        )
    WHERE
        T.SECTION = 1;

    /* ORIGSQL: COMMIT; */
    COMMIT;
    
    select * from EXT.STEL_RPT_ADV_IND_PS;
    

    ---insertion of measurement for section -2
    -------------------------------------------           
    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_ADV_IND_PS (TENANTID, PAYEESEQ, POSITIONSEQ, PERIODSEQ, CALENDARSEQ, PROCESSINGUNITSEQ, PROCESSINGUNIT, PAYEEID, CALENDARNAME, SECTION, FIRSTNAME, MIDDLENAME, LASTNAME, POSITIONNA(...) */
    INSERT INTO EXT.STEL_RPT_ADV_IND_PS
        (
            TENANTID,
            PAYEESEQ,
            POSITIONSEQ,
            PERIODSEQ,
            CALENDARSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNIT,
            PAYEEID,
            CALENDARNAME,
            SECTION,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            POSITIONNAME,
            PERIODNAME,
            QUARTERS,
            QUARTERACHIEVEMENTS,
            QUARTERTARGETS,
            QUARTERACHIEVEDPERCENT,
            TITLESEQ,
            TITLENAME,
            CREATEDATE,
            sourceperiodseq, sourceperiodname
        )
        
            SELECT   /* ORIGSQL: (SELECT V_TENANTID, P.PAYEESEQ, P.POSITIONSEQ, v_period.periodseq, V_CALENDARSEQ, IN_PROCESSINGUNITSEQ, P.PROCESSINGUNITNAME, P.PAYEEID, V_CALENDARNAME AS CALENDARNAME, '2', P.FIRSTNAME, P.MIDDLENAME,(...) */
                :v_Tenantid,
                P.PAYEESEQ,
                P.POSITIONSEQ,
                :v_period.periodseq,
                /* --       qtdata.PERIODSEQ, */
                :v_calendarseq,
                :IN_PROCESSINGUNITSEQ,
                P.PROCESSINGUNITNAME,
                P.PAYEEID,
                :v_Calendarname AS CALENDARNAME,
                '2',
                P.FIRSTNAME,
                P.MIDDLENAME,
                P.LASTNAME,
                P.POSITIONNAME,
                /* -- QTDATA.MONTHNAME AS PERIODNAME, */
                :v_period.name AS periodname,
                QTDATA.QTR AS QUARTERS,
                SUM(QTDATA.QUARTERACHIEVEMENTS) AS QUARTERACHIEVEMENTS,
                SUM(QTDATA.QUARTERTARGETS) AS QUARTERTARGETS,
                SUM((QTDATA.QUARTERACHIEVEDPERCENT)*100) AS QUARTERACHIEVEDPERCENT,
                P.TITLESEQ AS TITLESEQ,
                P.TITLE AS TITLENAME,
                CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
                QTDATA.PERIODSEQ,
                QTDATA.MONTHNAME
            FROM
                EXT.STEL_POSPART_MASTER P,
                (
                    SELECT   /* ORIGSQL: (SELECT POSITIONSEQ, PERIODSEQ, QTRNAME, PAYEESEQ, SUM(QUARTERACHIEVEMENTS) AS QUARTERACHIEVEMENTS, SUM(QUARTERTARGETS) AS QUARTERTARGETS,SUM(QUARTERACHIEVEDPERCENT) AS QUARTERACHIEVEDPERCENT, QTR,END(...) */
                        POSITIONSEQ,
                        PERIODSEQ,
                        QTRNAME,
                        PAYEESEQ,
                        SUM(QUARTERACHIEVEMENTS) AS QUARTERACHIEVEMENTS,
                        SUM(QUARTERTARGETS) AS QUARTERTARGETS,
                        SUM(QUARTERACHIEVEDPERCENT) AS QUARTERACHIEVEDPERCENT,
                        QTR,
                        ENDDATE,
                        MONTHNAME,
                        GENERICATTRIBUTE1
                    FROM
                        (
                            SELECT   /* ORIGSQL: (SELECT MES.POSITIONSEQ, QTR.ENDDATE AS ENDDATE, QTR.NAME AS QTRNAME, MES.PAYEESEQ,PRD.NAME AS MONTHNAME,GENERICATTRIBUTE1, MES.GENERICNUMBER2 AS QUARTERACHIEVEMENTS, MES.GENERICNUMBER3 AS QUARTERTARG(...) */
                                MES.POSITIONSEQ,
                                QTR.ENDDATE AS ENDDATE,
                                QTR.NAME AS QTRNAME,
                                MES.PAYEESEQ,
                                PRD.NAME AS MONTHNAME,
                                GENERICATTRIBUTE1,
                                MES.GENERICNUMBER2 AS QUARTERACHIEVEMENTS,
                                MES.GENERICNUMBER3 AS QUARTERTARGETS,
                                MES.GENERICNUMBER4 AS QUARTERACHIEVEDPERCENT,
                                MES.PERIODSEQ,
                                substring(QTR.NAME,0,2) AS QTR  /* ORIGSQL: SUBSTR(QTR.NAME,0,2) */
                            FROM
                                CS_MEASUREMENT MES,
                                CS_PERIOD PRD,
                                CS_PERIOD QTR
                            WHERE
                                MES.PERIODSEQ = PRD.PERIODSEQ
                                AND PRD.PARENTSEQ = QTR.PERIODSEQ
                                AND prd.periodseq IN
                                (select p.periodseq
-- , p.name 
from cs_period p,

(SELECT
m.name as PERIODNAME,
q.name as QUARTERNAME,
y.name as YEARNAME,
q.q_node_id as QTR_PERIODSEQ
    -- hierarchy_distance
FROM HIERARCHY (
    SOURCE (select periodseq as node_id , parentseq as parent_id, name 
    from cs_period where removedate = to_date('01/01/2200','dd/mm/yyyy') and periodtypeseq=:v_periodtypeseq_mon) 
    START WHERE periodseq = :v_period.periodseq ) m ,
    
    (select periodseq as q_node_id , parentseq as q_parent_id, name
    from cs_period where removedate = to_date('01/01/2200','dd/mm/yyyy') and periodtypeseq=:v_periodtypeseq_qtr
     ) q,
    
    (select periodseq as y_node_id , parentseq as y_parent_id, name
    from cs_period where removedate = to_date('01/01/2200','dd/mm/yyyy') and periodtypeseq=:v_periodtypeseq_yr
     ) y

where m.parent_id =  q.q_node_id
and q.q_parent_id = y.y_node_id) src

where p.periodtypeseq=:v_periodtypeseq_mon
and p.removedate = to_date('01/01/2200','dd/mm/yyyy')
and src.QTR_PERIODSEQ =p.parentseq)
                                AND PRD.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                                AND QTR.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                                AND MES.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
                        ) AS dbmtk_corrname_10480
                    GROUP BY
                        POSITIONSEQ, PERIODSEQ, PAYEESEQ, QTR,ENDDATE, QTRNAME, MONTHNAME,GENERICATTRIBUTE1
                ) AS QTDATA
            WHERE
                QTDATA.PAYEESEQ = P.PAYEESEQ
                AND QTDATA.POSITIONSEQ = P.POSITIONSEQ
                AND P.PERIODSEQ = :IN_PERIODSEQ
                --AND QTDATA.PERIODSEQ IN (SELECT YTDPERIODSEQ FROM cs_periodcalendarytd WHERE periodseq = IN_PERIODSEQ AND calendarname =V_CALENDARNAME)
                --AND QTDATA.PERIODSEQ = IN_PERIODSEQ
                AND P.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
                AND QTDATA.GENERICATTRIBUTE1 = 'Advt_Qtrly Rev Achv_Payable'
            GROUP BY
                P.PAYEESEQ,
                P.POSITIONSEQ,
                qtdata.PERIODSEQ,
                P.PROCESSINGUNITNAME,
                P.PAYEEID,
                P.FIRSTNAME,
                P.MIDDLENAME,
                P.LASTNAME,
                P.POSITIONNAME,
                '2',
                QTDATA.MONTHNAME,
                QTDATA.QTR,
                P.TITLESEQ,
                P.TITLE,
                CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --updation of payouts for section-2
    -----------------------------------   

    /* ORIGSQL: UPDATE EXT.STEL_RPT_ADV_IND_PS T SET PAYOUTS =nvl((SELECT nvl(i.value,0) FROM CS_INCENTIVE I WHERE I.PERIODSEQ = T.sourceperiodseq AND T.PAYEESEQ = I.PAYEESEQ AND I.GENERICATTRIBUTE1 = 'Advt_M1_In(...) */
    UPDATE EXT.STEL_RPT_ADV_IND_PS T
        SET
        /* ORIGSQL: PAYOUTS = */
        PAYOUTS = IFNULL   /* ORIGSQL: nvl((SELECT nvl(i.value,0) FROM CS_INCENTIVE I WHERE I.PERIODSEQ = T.sourceperiodseq AND T.PAYEESEQ = I.PAYEESEQ AND I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'),0) */
        (
            (
                SELECT   /* ORIGSQL: (SELECT nvl(i.value,0) FROM CS_INCENTIVE I WHERE I.PERIODSEQ = T.sourceperiodseq AND T.PAYEESEQ = I.PAYEESEQ AND I.GENERICATTRIBUTE1 = 'Advt_M1_Indv') */
                    IFNULL(i.value,0)
                FROM
                    CS_INCENTIVE I
                WHERE
                    I.PERIODSEQ = T.sourceperiodseq
                    AND T.PAYEESEQ = I.PAYEESEQ
                    AND I.GENERICATTRIBUTE1 = 'Advt_M1_Indv'
            )
        ,0)
    WHERE
        T.SECTION = 2
        AND t.periodseq = :v_period.periodseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    ----insertion of incentive values for section-3
    -------------------------------------------------   

    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_ADV_IND_PS (TENANTID, PAYEESEQ, POSITIONSEQ, PERIODSEQ, CALENDARSEQ, PROCESSINGUNITSEQ, PROCESSINGUNIT, PAYEEID, CALENDARNAME, SECTION, FIRSTNAME, MIDDLENAME, LASTNAME, POSITIONNA(...) */
    INSERT INTO EXT.STEL_RPT_ADV_IND_PS
        (
            TENANTID,
            PAYEESEQ,
            POSITIONSEQ,
            PERIODSEQ,
            CALENDARSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNIT,
            PAYEEID,
            CALENDARNAME,
            SECTION,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            POSITIONNAME,
            PERIODNAME,
            /* --OVERALL_FY_ACHIEVEMENT, */
            ANNUAL_OTI,
            /* --YEARLY_ACHIEVEMENTS, */
            /* -- YEARLY_ACHV_PERCENT , */
            PAYOUT,
            QUARTERS,
            M2_IS_ACHIEVED,
            M2_ACHIVED_INC,
            M3_IS_ACHIEVED,
            M3_ACHIEVED_INC,
            TITLESEQ,
            TITLENAME,
            CREATEDATE,
            ANNUAL_ACC_PERCENT,
            M1_PAYOUT
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, P.PAYEESEQ, P.POSITIONSEQ, P.PERIODSEQ, v_calendarseq, IN_PROCESSINGUNITSEQ, P.PROCESSINGUNITNAME, P.PAYEEID, v_Calendarname AS CALENDARNAME, '3', P.FIRSTNAME, P.MIDDLENAME, P.LAST(...) */
            :v_Tenantid,
            P.PAYEESEQ,
            P.POSITIONSEQ,
            P.PERIODSEQ,
            :v_calendarseq,
            :IN_PROCESSINGUNITSEQ,
            P.PROCESSINGUNITNAME,
            P.PAYEEID,
            :v_Calendarname AS CALENDARNAME,
            '3',
            P.FIRSTNAME,
            P.MIDDLENAME,
            P.LASTNAME,
            P.POSITIONNAME,
            P.PERIODNAME AS PERIODNAME,
            /* --SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER6 ELSE 0 END)) AS OVERALL_FY_ACHIEVEMENTS, */
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                    THEN I.GENERICNUMBER1
                    ELSE 0
                END
            ) AS ANNUAL_OTI,
            /* --SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER5 ELSE 0 END)) AS YEARLY_ACHIEVEMENT, */
            /* --SUM((CASE WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator' THEN I.GENERICNUMBER4 ELSE 0 END)*100) AS YEARLY_ACHIEVED_PERCENT, */
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator'
                    THEN I.VALUE
                    ELSE 0
                END
            ) AS PAYOUT,
            NULL AS QUARTERS,
            MAX(
                CASE 
                    WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_SA', 'Advt_M2_Team/SA')
                    AND I.GENERICBOOLEAN1 = 1)
                    THEN 'YES'
                    ELSE 'NO'
                END
            ) AS M2_IS_ACHIEVED,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 IN('Advt_M2_SA', 'Advt_M2_Team/SA')
                    THEN I.VALUE
                    ELSE 0
                END
            ) AS M2_ACHIEVED_INC,
            MAX(
                CASE 
                    WHEN (I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                    AND I.GENERICBOOLEAN1 = 1)
                    THEN 'YES'
                    ELSE 'NO'
                END
            ) AS M3_IS_ACHIEVED,
            SUM(
                CASE 
                    WHEN I.GENERICATTRIBUTE1 IN('Advt_M3_AR','Advt_M3_PR')
                    THEN I.VALUE
                    ELSE 0
                END
            ) AS M3_ACHIEVED_INC,
            P.TITLESEQ AS TITLESEQ,
            P.TITLE AS TITLENAME,
            CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
            /* --(ADVT_IND_PAYMENT.ANNUAL_OTI * 1.2) +(ADVT_IND_PAYMENT.ANNUAL_OTI * 1.6)+(ADVT_IND_PAYMENT.ANNUAL_OTI * 2)+(ADVT_IND_PAYMENT.ANNUAL_OTI * 2.5)+(ADVT_IND_PAYMENT.ANNUAL_OTI * 3)  */
            MAX((
                    CASE 
                        WHEN I.GENERICATTRIBUTE1 = 'Advt_Yearly_Accelerator'
                        THEN I.GENERICNUMBER2
                        ELSE 0
                    END
            )*100) AS ANNUAL_ACC_PERCENT,
            NULL AS M1_PAYOUT
        FROM
            CS_INCENTIVE I,
            EXT.STEL_POSPART_MASTER P
        WHERE
            I.PAYEESEQ = P.PAYEESEQ
            AND I.POSITIONSEQ = P.POSITIONSEQ
            AND I.PERIODSEQ = :IN_PERIODSEQ
            AND P.PERIODSEQ = :IN_PERIODSEQ
            AND P.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND I.GENERICATTRIBUTE1 IN('Advt_M1_Indv','Advt_M2_Team/SA','Advt_M3_AR','Advt_Yearly_Accelerator','Advt_M3_PR','Advt_M2_SA')
        GROUP BY
            :v_Tenantid,
            P.PAYEESEQ,
            P.POSITIONSEQ,
            P.PERIODSEQ,
            :v_calendarseq,
            :IN_PROCESSINGUNITSEQ,
            P.PROCESSINGUNITNAME,
            P.PAYEEID,
            P.FIRSTNAME,
            P.MIDDLENAME,
            P.LASTNAME,
            P.POSITIONNAME,
            :v_Calendarname,
            '3',
            P.PERIODNAME,
            --(CASE WHEN (I.GENERICATTRIBUTE1 IN('Advt_M2_SA', 'Advt_M2_Team/SA') AND I.GENERICBOOLEAN1 = 1) THEN 'YES' ELSE 'NO' END),
            --(CASE WHEN (I.GENERICATTRIBUTE1 IN( 'Advt_M3_AR','Advt_M3_PR') AND I.GENERICBOOLEAN1 = 1) THEN 'YES' ELSE 'NO' END),
            P.TITLESEQ,
            P.TITLE,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE EXT.STEL_RPT_ADV_IND_PS T SET M1_PAYOUT = (SELECT SUM(PAYOUTS) FROM EXT.STEL_RPT_ADV_IND_PS x where t.periodseq=x.periodseq and t.positionseq=x.positionseq and section=2) WHERE T.SECTION = 3 an(...) */
    UPDATE EXT.STEL_RPT_ADV_IND_PS T
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_RPT_ADV_IND_PS' not found */
        SET
        /* ORIGSQL: M1_PAYOUT = */
        M1_PAYOUT = (
            SELECT   /* ORIGSQL: (select SUM(PAYOUTS) from EXT.STEL_RPT_ADV_IND_PS x where t.periodseq=x.periodseq and t.positionseq=x.positionseq and section=2) */
                SUM(PAYOUTS)
            FROM
                EXT.STEL_RPT_ADV_IND_PS x
            WHERE
                t.periodseq = x.periodseq
                AND t.positionseq = x.positionseq
                AND section = 2
        )
    WHERE
        T.SECTION = 3
        AND t.periodseq = :v_period.periodseq;  

    /* ORIGSQL: UPDATE EXT.STEL_RPT_ADV_IND_PS T SET OVERALL_FY_ACHIEVEMENT = (SELECT M.GENERICNUMBER2 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD(...) */
    UPDATE EXT.STEL_RPT_ADV_IND_PS T 
        SET
        /* ORIGSQL: OVERALL_FY_ACHIEVEMENT = */
        OVERALL_FY_ACHIEVEMENT = (
            SELECT   /* ORIGSQL: (SELECT M.GENERICNUMBER2 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv') */
                M.GENERICNUMBER2
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        ), 
        /* ORIGSQL: YEARLY_ACHIEVEMENTS = */
        YEARLY_ACHIEVEMENTS = (
            SELECT   /* ORIGSQL: (SELECT M.GENERICNUMBER1 FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv') */
                M.GENERICNUMBER1
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        ), 
        /* ORIGSQL: YEARLY_ACHV_PERCENT = */
        YEARLY_ACHV_PERCENT = (
            SELECT   /* ORIGSQL: (SELECT (M.VALUE*100) FROM CS_MEASUREMENT M WHERE T.PAYEESEQ = M.PAYEESEQ AND T.PERIODSEQ = M.PERIODSEQ AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv') */
                (M.VALUE*100)
            FROM
                CS_MEASUREMENT M
            WHERE
                T.PAYEESEQ = M.PAYEESEQ
                AND T.PERIODSEQ = M.PERIODSEQ
                AND M.GENERICATTRIBUTE1 = 'Advt YTD Achv'
        )
    WHERE
        T.SECTION = 3;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --insertion of PR table section-4(sub-report)
    ---------------------------------------------   
    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_ADV_IND_PS (TENANTID, PAYEESEQ, POSITIONSEQ, PERIODSEQ, CALENDARSEQ, PROCESSINGUNITSEQ, PROCESSINGUNIT, PAYEEID, CALENDARNAME, SECTION, FIRSTNAME, MIDDLENAME, LASTNAME, POSITIONNA(...) */
    INSERT INTO EXT.STEL_RPT_ADV_IND_PS
        (
            TENANTID,
            PAYEESEQ,
            POSITIONSEQ,
            PERIODSEQ,
            CALENDARSEQ,
            PROCESSINGUNITSEQ,
            PROCESSINGUNIT,
            PAYEEID,
            CALENDARNAME,
            SECTION,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            POSITIONNAME,
            PERIODNAME,
            PR_RATINGS,
            PR_VALUE,
            TITLESEQ,
            TITLENAME,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, P.PAYEESEQ, P.POSITIONSEQ, P.PERIODSEQ, v_calendarseq, IN_PROCESSINGUNITSEQ, P.PROCESSINGUNITNAME, P.PAYEEID, v_Calendarname AS CALENDARNAME, '4', P.FIRSTNAME, P.MIDDLENAME, P.LAST(...) */
            :v_Tenantid,
            P.PAYEESEQ,
            P.POSITIONSEQ,
            P.PERIODSEQ,
            :v_calendarseq,
            :IN_PROCESSINGUNITSEQ,
            P.PROCESSINGUNITNAME,
            P.PAYEEID,
            :v_Calendarname AS CALENDARNAME,
            '4',
            P.FIRSTNAME,
            P.MIDDLENAME,
            P.LASTNAME,
            P.POSITIONNAME,
            :v_period.name AS periodname,
            ('PR'|| IFNULL(LKP.DIM0,'')) AS PR_RATINGS,
            IFNULL(TO_VARCHAR(LKP.VALUE*100),'')||'%' AS PR_VALUE,
            P.TITLESEQ AS TITLESEQ,
            P.TITLE AS TITLENAME,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_LOOKUP LKP,
            EXT.STEL_POSPART_MASTER P,
            CS_PERIOD PER
        WHERE
            P.PERIODSEQ = PER.PERIODSEQ
            AND lkp.NAME = 'LT_Advt_TL/Direc_PR Rank'
            AND LKP.EFFECTIVESTARTDATE < PER.ENDDATE
            AND LKP.EFFECTIVEENDDATE > PER.STARTDATE
            AND PER.REMOVEDATE = to_date('01/01/2200','MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200','MM/DD/YYYY') */
            AND PER.PERIODSEQ = :IN_PERIODSEQ
            AND P.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;
END