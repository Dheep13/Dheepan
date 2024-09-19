CREATE PROCEDURE EXT.STEL_RPT_INDPAYSUMM_TS
(
    IN in_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_rpttype VARCHAR2 */
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
    DECLARE v_Tenantid VARCHAR(255) = 'STEL';  /* ORIGSQL: v_Tenantid VARCHAR2(255) := 'STEL'; */
    DECLARE v_Calendarname VARCHAR(255) = NULL;  /* ORIGSQL: v_Calendarname VARCHAR2(255) := NULL; */
    DECLARE v_periodname VARCHAR(255);  /* ORIGSQL: v_periodname VARCHAR2(255); */
    DECLARE v_ComponentName VARCHAR(255);  /* ORIGSQL: v_ComponentName VARCHAR2(255); */
    DECLARE v_StMsg VARCHAR(255);  /* ORIGSQL: v_StMsg VARCHAR2(255); */
    DECLARE v_EdMsg VARCHAR(255);  /* ORIGSQL: v_EdMsg VARCHAR2(255); */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    DECLARE v_rowcount DECIMAL(38,10);  /* ORIGSQL: v_rowcount number; */

    v_ComponentName = 'stel_rpt_indpaysumm_ts';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME,
        PERIODNAME
    INTO
        v_Calendarname,
        v_periodname
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :in_periodseq;

    /* ORIGSQL: STEL_RPT_TSCUSTOMER (IN_PERIODSEQ, in_processingunitseq) */
    CALL EXT.STEL_RPT_TSCUSTOMER(:in_periodseq, :in_processingunitseq); 

    /* ORIGSQL: DELETE FROM stelext.stel_rpt_ts_indpaysumm WHERE periodseq = in_periodseq AND pr(...) */
    DELETE
    FROM
        ext.stel_rpt_ts_indpaysumm
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Start report partitions') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Start report partitions');

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'stel_rpt_ts_indpaysumm') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'ext.stel_rpt_ts_indpaysumm');

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 1') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 1');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_DATA_TSCUSTMAST' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_TS_INDPAYSUMM' not found */

    /* ORIGSQL: INSERT INTO stel_rpt_ts_indpaysumm (tenantid, periodseq, periodname, payeeseq, p(...) */
    INSERT INTO ext.stel_rpt_ts_indpaysumm
        (
            tenantid,
            periodseq,
            periodname,
            payeeseq,
            positionseq,
            positionname,
            titleseq,
            titlename,
            processingunitseq,
            processingunitname,
            calendarname,
            geid,
            firstname,
            middlename,
            lastname,
            team,
            otc,
            weight,
            weightachieved,
            standardscore,
            proratescore,
            target,
            excesstarget,
            totaltarget,
            section,
            EVENTTYPE,
            SALESREPCODE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, m.payeeseq, m.positionseq, m.pos(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            m.payeeseq,
            m.positionseq,
            m.positionname,
            m.titleseq,
            m.titlename,
            m.processingunitseq,
            m.processingunitname,
            :v_Calendarname,
            m.payeeid,/* --arun changed it from geid to payeeid - as geid populated Username */  m.firstname,
            m.middlename,
            m.lastname,
            m.team,
            i.genericnumber1 AS otc,
            (i.genericnumber2 * 100) AS Weight,
            (i.genericnumber3 * 100) AS weight_achieved,
            IFNULL(TRIM(SUBSTRING_REGEXPR('[^ ]+' IN i.genericattribute4)), 0) AS standard_score,  /* ORIGSQL: REGEXP_SUBSTR(i.genericattribute4, '[^ ]+') */
                                                                                                   /* ORIGSQL: NVL(TRIM (REGEXP_SUBSTR (i.genericattribute4, '[^ ]+')), 0) */
            /* --              I.GENERICATTRIBUTE5 proratescore, --As per discussion with Balaji on 18.11 below added GA16 */
            CASE
                WHEN I.GENERICATTRIBUTE16 LIKE '%USD'
                THEN NULL
                ELSE /* --I.GENERICATTRIBUTE16 */
                CASE
                    WHEN LENGTH(I.GENERICATTRIBUTE16) <> 0
                    THEN IFNULL(Substring(I.GENERICATTRIBUTE16,0,LOCATE(I.GENERICATTRIBUTE16,'.',1,1) + 2),'')  /* ORIGSQL: SUBSTR(I.GENERICATTRIBUTE16, 0, INSTR (I.GENERICATTRIBUTE16, '.') + 2) */
                                                                                                                                                     /* ORIGSQL: INSTR(I.GENERICATTRIBUTE16, '.') */
                    || '%'
                END
            END
            AS proratescore,
            IFNULL(i.genericnumber6, 0) AS target,  /* ORIGSQL: NVL(i.genericnumber6, 0) */
            IFNULL(i.genericnumber5, 0) AS excess_target,  /* ORIGSQL: NVL(i.genericnumber5, 0) */
            CASE
                WHEN i.genericattribute2 = 'TeleSales - Director'
                THEN /* --  I.GENERICNUMBER5 + I.GENERICNUMBER6 */
                IFNULL(i.VALUE, 0)  /* ORIGSQL: NVL(i.VALUE, 0) */
                ELSE IFNULL(i.VALUE, 0)  /* ORIGSQL: NVL(i.VALUE, 0) */
            END
            AS total,
            1,
            CASE
                WHEN I.GENERICATTRIBUTE1 = 'STV-TS-Team'
                THEN '2Team Achievement'
                WHEN I.GENERICATTRIBUTE1 = 'STV-TS-Qtrly'
                THEN '3Quarterly Achievement'
                WHEN I.GENERICATTRIBUTE1 = 'STV-TS-SAA'
                THEN '4SAA'
                ELSE '1Individual Achievement'
            END
            AS EVENTTYPE,
            M.SALESREPCODE
        FROM
            cs_incentive i,
            ext.stel_rpt_data_tscustmast m
        WHERE
            i.periodseq = m.periodseq
            AND i.payeeseq = m.payeeseq
            AND i.positionseq = m.positionseq
            AND i.periodseq = :in_periodseq
            AND i.processingunitseq = :in_processingunitseq
            AND I.GENERICATTRIBUTE1 IN
            ('STV-TS-Team',
                'STV-TS-Indv',
                'STV-TS-Qtrly',
            'STV-TS-SAA')
            AND i.genericattribute2 IN
            ('TeleSales - Sales Executive',
                'TeleSales - Team Lead',
                'TeleSales - Manager',
            'TeleSales - Director')
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 2 Adj') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 2 Adj');

    --- Adjustment entry

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    /* ORIGSQL: INSERT INTO stel_rpt_ts_indpaysumm (tenantid, periodseq, periodname, payeeseq, p(...) */
    INSERT INTO ext.stel_rpt_ts_indpaysumm
        (
            tenantid,
            periodseq,
            periodname,
            payeeseq,
            positionseq,
            positionname,
            titleseq,
            titlename,
            processingunitseq,
            processingunitname,
            calendarname,
            geid,
            firstname,
            middlename,
            lastname,
            team,
            section,
            otc,
            EVENTTYPE,
            COMMISSIONADJ,
            SALESREPCODE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, m.payeeseq, m.positionseq, m.pos(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            m.payeeseq,
            m.positionseq,
            m.positionname,
            m.titleseq,
            m.titlename,
            m.processingunitseq,
            m.processingunitname,
            :v_Calendarname,
            m.geid,
            m.firstname,
            m.middlename,
            m.lastname,
            m.team,
            -1,
            2,
            'Commission Adjustment' AS EVENTTYPE,
            CR.VALUE AS COMMISSIONADJ,
            M.SALESREPCODE
        FROM
            cs_credit cr,
            ext.stel_rpt_data_tscustmast m,
            cs_credittype crd
        WHERE
            cr.periodseq = m.periodseq
            AND cr.payeeseq = m.payeeseq
            AND cr.positionseq = m.positionseq
            AND cr.periodseq = :in_periodseq
            AND CRD.REMOVEDATE = :v_eot
            AND cr.processingunitseq = :in_processingunitseq
            AND cr.CREDITTYPESEQ = CRD.DATATYPESEQ
            AND CRD.CREDITTYPEID = 'Payment Adjustment'
            AND CRD.REMOVEDATE = :v_eot
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Prior Balance entry
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Update 3') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Update 3');   

    /* ORIGSQL: UPDATE stel_rpt_ts_indpaysumm B1 SET ADJ2 = (SELECT SUM(VALUE) AS balance FROM c(...) */
    UPDATE ext.stel_rpt_ts_indpaysumm B1
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCEPAYMENTTRACE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BALANCE' not found */
        SET
        /* ORIGSQL: ADJ2 = */
        ADJ2 = (
            SELECT   /* ORIGSQL: (SELECT SUM(VALUE) balance FROM cs_balance B2, cs_balancepaymenttrace b3 WHERE B(...) */
                SUM(VALUE) AS balance
            FROM
                cs_balance B2,
                cs_balancepaymenttrace b3
            WHERE
                B2.balancestatusid IN ('applied')
                AND B3.TARGETPERIODSEQ = :in_periodseq
                AND B2.BALANCESEQ = B3.BALANCESEQ
                AND B1.POSITIONSEQ = B2.POSITIONSEQ
                AND B1.PAYEESEQ = B2.PAYEESEQ
        )
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Protected Balance entry
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Update 4') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Update 4');   

    /* ORIGSQL: UPDATE stel_rpt_ts_indpaysumm B1 SET B1.PROTECTEDADJ = (SELECT SUM(B2.VALUE) AS (...) */
    UPDATE ext.stel_rpt_ts_indpaysumm B1
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSIT' not found */
        SET
        /* ORIGSQL: B1.PROTECTEDADJ = */
        PROTECTEDADJ = (
            SELECT   /* ORIGSQL: (SELECT SUM(B2.VALUE) balance FROM CS_DEPOSIT B2 WHERE B2.GENERICATTRIBUTE1 = 'P(...) */
                SUM(B2.VALUE) AS balance
            FROM
                CS_DEPOSIT B2
            WHERE
                B2.GENERICATTRIBUTE1 = 'Protected Comm'
                AND B2.PERIODSEQ = :in_periodseq
                AND B1.POSITIONSEQ = B2.POSITIONSEQ
                AND B1.PAYEESEQ = B2.PAYEESEQ
        )
    WHERE
        b1.periodseq = :in_periodseq
        AND b1.processingunitseq = :in_processingunitseq
        AND B1.EVENTTYPE = '1Individual Achievement';

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Adding product details into custom table

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_TS_PRDDETAILS' not found */

    /* ORIGSQL: DELETE FROM stelext.STEL_RPT_TS_PRDDETAILS WHERE periodseq = in_periodseq AND pr(...) */
    DELETE
    FROM
        EXT.STEL_RPT_TS_PRDDETAILS
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'STEL_RPT_TS_PRDDETAILS') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'EXT.STEL_RPT_TS_PRDDETAILS');

    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 5') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 5');

    -- Fot title <> Directors   

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_TS_PRDDETAILS' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_TS_PRDDETAILS (PERIODNAME, PERIODSEQ, PROCESSINGUNITSEQ, PA(...) */
    INSERT INTO EXT.STEL_RPT_TS_PRDDETAILS
        (
            PERIODNAME,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PRODUCTS,
            PRODUCTACTUAL,
            ACTUALPOINTS
        )
        SELECT   /* ORIGSQL: (SELECT I.PERIODNAME, I.PERIODSEQ, I.PROCESSINGUNITSEQ, I.PAYEESEQ, I.POSITIONSE(...) */
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME,
            CRD.GENERICATTRIBUTE3,
            SUM(CRD.GENERICNUMBER1),
            SUM(CRD.VALUE)
        FROM
            ext.stel_rpt_data_tscustmast I,
            CS_CREDIT CRD,
            CS_CREDITTYPE CTYPE
        WHERE
            i.periodseq = :in_periodseq
            AND i.processingunitseq = :in_processingunitseq
            AND I.POSITIONSEQ = CRD.POSITIONSEQ
            AND I.PERIODSEQ = CRD.PERIODSEQ
            AND I.PAYEESEQ = CRD.PAYEESEQ
            AND I.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND I.TITLENAME <> 'Director'
            AND CTYPE.CREDITTYPEID IN
            ('Telesales Comm', 'Telesales Comm ARPU')
        GROUP BY
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME,
            CRD.GENERICATTRIBUTE3
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Points Adjustments
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 6 Payment Adj') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 6 Payment Adj');    

    /* ORIGSQL: INSERT INTO STEL_RPT_TS_PRDDETAILS (PERIODNAME, PERIODSEQ, PROCESSINGUNITSEQ, PA(...) */
    INSERT INTO EXT.STEL_RPT_TS_PRDDETAILS
        (
            PERIODNAME,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PRODUCTS,
            PRODUCTACTUAL,
            ACTUALPOINTS
        )
        SELECT   /* ORIGSQL: (SELECT I.PERIODNAME, I.PERIODSEQ, I.PROCESSINGUNITSEQ, I.PAYEESEQ, I.POSITIONSE(...) */
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME,
            'Points Adjustment',
            SUM(CRD.GENERICNUMBER1),
            SUM(CRD.VALUE)
        FROM
            ext.stel_rpt_data_tscustmast I,
            CS_CREDIT CRD,
            CS_CREDITTYPE CTYPE
        WHERE
            i.periodseq = :in_periodseq
            AND i.processingunitseq = :in_processingunitseq
            AND I.POSITIONSEQ = CRD.POSITIONSEQ
            AND I.PERIODSEQ = CRD.PERIODSEQ
            AND I.PAYEESEQ = CRD.PAYEESEQ
            AND I.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND CTYPE.CREDITTYPEID = 'Points Adjustment'
            AND I.TITLENAME <> 'Director'
        GROUP BY
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Fot title = Directors
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 7 Directors') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 7 Directors');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODTYPE' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_TS_PRDDETAILS (PERIODNAME, PERIODSEQ, PROCESSINGUNITSEQ, PA(...) */
    INSERT INTO EXT.STEL_RPT_TS_PRDDETAILS
        (
            PERIODNAME,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PRODUCTS,
            PRODUCTACTUAL,
            ACTUALPOINTS
        )
        
            SELECT   /* ORIGSQL: (SELECT v_PeriodName, in_periodseq, I.PROCESSINGUNITSEQ, I.PAYEESEQ, I.POSITIONS(...) */
                :v_periodname,
                :in_periodseq,
                I.PROCESSINGUNITSEQ,
                I.PAYEESEQ,
                I.POSITIONSEQ,
                I.POSITIONNAME,
                CRD.GENERICATTRIBUTE3,
                SUM(CRD.GENERICNUMBER1),
                SUM(CRD.VALUE)
            FROM
                ext.stel_rpt_data_tscustmast I,
                CS_CREDIT CRD,
                CS_CREDITTYPE CTYPE
            WHERE
                i.periodseq IN
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
                                        to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                        'dd/mm/yyyy')
                                        AND P2.REMOVEDATE =
                                        to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                        'dd/mm/yyyy')
                                        AND P1.PERIODTYPESEQ =
                                        P2.PERIODTYPESEQ
                                        AND P2.NAME =
                                        'quarter'
                                )
                                AND p1.removedate =
                                to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                'dd/mm/yyyy')
                                AND P2.REMOVEDATE =
                                to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                'dd/mm/yyyy')
                                AND P1.PERIODTYPESEQ =
                                P2.PERIODTYPESEQ
                                AND P2.NAME = 'month'
                                AND p1.periodseq = :in_periodseq
                        )
                        AND p3.removedate =
                        to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                )
                AND i.processingunitseq = :in_processingunitseq
                AND I.POSITIONSEQ = CRD.POSITIONSEQ
                AND I.PERIODSEQ = CRD.PERIODSEQ
                AND I.PAYEESEQ = CRD.PAYEESEQ
                AND I.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
                AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
                AND I.TITLENAME = 'Director'
                AND CTYPE.CREDITTYPEID IN
                ('Telesales Comm', 'Telesales Comm ARPU')
            GROUP BY
                I.PERIODNAME,
                I.PERIODSEQ,
                I.PROCESSINGUNITSEQ,
                I.PAYEESEQ,
                I.POSITIONSEQ,
                I.POSITIONNAME,
                CRD.GENERICATTRIBUTE3
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Points Adjustments
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 8') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 8');         

    /* ORIGSQL: INSERT INTO STEL_RPT_TS_PRDDETAILS (PERIODNAME, PERIODSEQ, PROCESSINGUNITSEQ, PA(...) */
    INSERT INTO EXT.STEL_RPT_TS_PRDDETAILS
        (
            PERIODNAME,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PRODUCTS,
            PRODUCTACTUAL,
            ACTUALPOINTS
        )
        SELECT   /* ORIGSQL: (SELECT v_PeriodName, in_periodseq, I.PROCESSINGUNITSEQ, I.PAYEESEQ, I.POSITIONS(...) */
            :v_periodname,
            :in_periodseq,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME,
            'Points Adjustment',
            SUM(CRD.GENERICNUMBER1),
            SUM(CRD.VALUE)
        FROM
            ext.stel_rpt_data_tscustmast I,
            CS_CREDIT CRD,
            CS_CREDITTYPE CTYPE
        WHERE
            i.periodseq IN
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
                                    to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                    'dd/mm/yyyy')
                                    AND P2.REMOVEDATE =
                                    to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                    'dd/mm/yyyy')
                                    AND P1.PERIODTYPESEQ =
                                    P2.PERIODTYPESEQ
                                    AND P2.NAME =
                                    'quarter'
                            )
                            AND p1.removedate =
                            to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                            'dd/mm/yyyy')
                            AND P2.REMOVEDATE =
                            to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                            'dd/mm/yyyy')
                            AND P1.PERIODTYPESEQ =
                            P2.PERIODTYPESEQ
                            AND P2.NAME = 'month'
                            AND p1.periodseq = :in_periodseq
                    )
                    AND p3.removedate =
                    to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
            )
            AND i.processingunitseq = :in_processingunitseq
            AND I.POSITIONSEQ = CRD.POSITIONSEQ
            AND I.PERIODSEQ = CRD.PERIODSEQ
            AND I.PAYEESEQ = CRD.PAYEESEQ
            AND I.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND CTYPE.CREDITTYPEID = 'Points Adjustment'
            AND I.TITLENAME = 'Director'
        GROUP BY
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --- Below code added on 19.01 as part of new CR given by Arjun in mail.
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 9 Platform Migration Handling fee') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 9 Platform Migration Handling fee');

    -- Platform Migration Handling fee 

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_TS_PRDDETAILS (PERIODNAME, PERIODSEQ, PROCESSINGUNITSEQ, PA(...) */
    INSERT INTO EXT.STEL_RPT_TS_PRDDETAILS
        (
            PERIODNAME,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PRODUCTS,
            PRODUCTACTUAL,
            ACTUALPOINTS
        )
        SELECT   /* ORIGSQL: (SELECT I.PERIODNAME, I.PERIODSEQ, I.PROCESSINGUNITSEQ, I.PAYEESEQ, I.POSITIONSE(...) */
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME,
            'Platform Migration Handling fee',
            0,
            SUM(MES.VALUE)
        FROM
            ext.stel_rpt_data_tscustmast I,
            CS_MEASUREMENT MES
        WHERE
            i.periodseq = :in_periodseq
            AND i.processingunitseq = :in_processingunitseq
            AND I.POSITIONSEQ = MES.POSITIONSEQ
            AND I.PERIODSEQ = MES.PERIODSEQ
            AND I.PAYEESEQ = MES.PAYEESEQ
            AND I.PROCESSINGUNITSEQ = MES.PROCESSINGUNITSEQ
            AND MES.VALUE <> 0
            AND MES.NAME = 'PMR_INTERNAL_Platform_Migration_Points'
        GROUP BY
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- TV Recon Handling Fee
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Insert 10 TV Recon Handling Fee') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Insert 10 TV Recon Handling Fee');   

    /* ORIGSQL: INSERT INTO STEL_RPT_TS_PRDDETAILS (PERIODNAME, PERIODSEQ, PROCESSINGUNITSEQ, PA(...) */
    INSERT INTO EXT.STEL_RPT_TS_PRDDETAILS
        (
            PERIODNAME,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PRODUCTS,
            PRODUCTACTUAL,
            ACTUALPOINTS
        )
        SELECT   /* ORIGSQL: (SELECT I.PERIODNAME, I.PERIODSEQ, I.PROCESSINGUNITSEQ, I.PAYEESEQ, I.POSITIONSE(...) */
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME,
            'TV Recon Handling Fee',
            0,
            SUM(MES.VALUE)
        FROM
            ext.stel_rpt_data_tscustmast I,
            CS_MEASUREMENT MES
        WHERE
            i.periodseq = :in_periodseq
            AND i.processingunitseq = :in_processingunitseq
            AND I.POSITIONSEQ = MES.POSITIONSEQ
            AND I.PERIODSEQ = MES.PERIODSEQ
            AND I.PAYEESEQ = MES.PAYEESEQ
            AND I.PROCESSINGUNITSEQ = MES.PROCESSINGUNITSEQ
            AND MES.VALUE <> 0
            AND mes.name = 'PMR_INTERNAL_TVRecon_Handling Fee'
        GROUP BY
            I.PERIODNAME,
            I.PERIODSEQ,
            I.PROCESSINGUNITSEQ,
            I.PAYEESEQ,
            I.POSITIONSEQ,
            I.POSITIONNAME
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Product target updation
    /* ORIGSQL: STEL_log (V_TENANTID,v_ComponentName,'Update 11 Prod Target') */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, 'Update 11 Prod Target');   

    /* ORIGSQL: UPDATE STEL_RPT_TS_PRDDETAILS A SET PRODUCTTARGET = (SELECT REPLACE(MAX (GENERIC(...) */
    UPDATE EXT.STEL_RPT_TS_PRDDETAILS A 
        SET
        /* ORIGSQL: PRODUCTTARGET = */
        PRODUCTTARGET = /* --             (SELECT REPLACE (MAX (GENERICATTRIBUTE16), 'quantity') -- Changed to GA6 on 18.11.2017 */
            (
                SELECT   /* ORIGSQL: (SELECT REPLACE(MAX (GENERICATTRIBUTE6), 'quantity') FROM CS_INCENTIVE INC WHERE(...) */
                    REPLACE(MAX(GENERICATTRIBUTE6), 'quantity', '') 
                FROM
                    CS_INCENTIVE INC
                WHERE
                    INC.PERIODSEQ = A.PERIODSEQ
                    AND INC.POSITIONSEQ = A.POSITIONSEQ
                    AND INC.PAYEESEQ = A.PAYEESEQ
                    AND INC.GENERICATTRIBUTE1 = 'STV-TS-Indv'
                    AND INC.genericattribute2 IN
                    ('TeleSales - Sales Executive',
                        'TeleSales - Team Lead',
                        'TeleSales - Manager',
                    'TeleSales - Director')
            )
        WHERE
            A.PERIODSEQ = :in_periodseq
            AND A.PROCESSINGUNITSEQ = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: update stel_rpt_ts_indpaysumm A SET PRORATESCORE = (SELECT MES.VALUE FROM stelex(...) */
    UPDATE ext.stel_rpt_ts_indpaysumm A
        /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_TS_INDPAYSUMM' not found */
        SET
        /* ORIGSQL: PRORATESCORE = */
        PRORATESCORE = (
            SELECT   /* ORIGSQL: (SELECT MES.VALUE FROM stelext.stel_rpt_ts_indpaysumm I, CS_MEASUREMENT MES WHER(...) */
                MES.VALUE
            FROM
                ext.stel_rpt_ts_indpaysumm I,
                CS_MEASUREMENT MES
            WHERE
                i.periodseq = :in_periodseq
                AND i.processingunitseq = :in_processingunitseq
                AND I.POSITIONSEQ = MES.POSITIONSEQ
                AND I.PERIODSEQ = MES.PERIODSEQ
                AND I.PAYEESEQ = MES.PAYEESEQ
                AND I.PROCESSINGUNITSEQ = MES.PROCESSINGUNITSEQ
                AND MES.VALUE >= 0
                AND MES.NAME = 'SM_Telesales_Monthly Achievement'
                AND I.EVENTTYPE = '1Individual Achievement'
                AND A.POSITIONSEQ = MES.POSITIONSEQ
                AND A.PERIODSEQ = MES.PERIODSEQ
                AND A.PAYEESEQ = MES.PAYEESEQ
                AND A.EVENTTYPE = '1Individual Achievement'
        ) *100
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq
        AND A.EVENTTYPE = '1Individual Achievement';

    /* ORIGSQL: commit; */
    COMMIT;  

    /* ORIGSQL: update stel_rpt_ts_indpaysumm A SET PRORATESCORE=CONCAT(REGEXP_SUBSTR(to_char(PR(...) */
    UPDATE ext.stel_rpt_ts_indpaysumm A
        SET
        /* ORIGSQL: PRORATESCORE = */
        PRORATESCORE = IFNULL(SUBSTRING_REGEXPR('(\d+\.\d{0,2})|\d+|.\d{0,2}' IN TO_VARCHAR(PRORATESCORE,NULL)),'') ||' % '   /* ORIGSQL: CONCAT(REGEXP_SUBSTR(to_char(PRORATESCORE),'(\d+\.\d{0,2})|\d+|.\d{0,2}'),' % ') */
    WHERE
        A.PRORATESCORE > 0
        AND a.periodseq = :in_periodseq
        AND A.EVENTTYPE = '1Individual Achievement'
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: delete from STEL_RPT_TS_PRDDETAILS where ACTUALPOINTS='0' and PRODUCTS in ('FBB'(...) */
    DELETE
    FROM
        STEL_RPT_TS_PRDDETAILS
    WHERE
        ACTUALPOINTS = '0'
        AND PRODUCTS IN ('FBB','FV','TV','TV Main','null')
        AND periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: delete from STEL_RPT_TS_PRDDETAILS where ACTUALPOINTS='0' and PRODUCTS IS NULL a(...) */
    DELETE
    FROM
        EXT.STEL_RPT_TS_PRDDETAILS
    WHERE
        ACTUALPOINTS = '0'
        AND PRODUCTS IS NULL
        AND periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END