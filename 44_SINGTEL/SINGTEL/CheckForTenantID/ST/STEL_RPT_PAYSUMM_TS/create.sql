CREATE PROCEDURE EXT.STEL_RPT_PAYSUMM_TS
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

    v_ComponentName = 'stel_rpt_paysumm_ts';

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

    /* ORIGSQL: DELETE FROM STELEXT.STEL_RPT_TS_PAYSUMM WHERE periodseq = in_periodseq AND proce(...) */
    DELETE
    FROM
        EXT.STEL_RPT_TS_PAYSUMM
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'stel_rpt_ts_paysumm') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'ext.stel_rpt_ts_paysumm');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_TS_INDPAYSUMM' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_TS_PAYSUMM' not found */

    /* ORIGSQL: INSERT INTO stel_rpt_ts_paysumm (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, POSI(...) */
    INSERT INTO ext.stel_rpt_ts_paysumm
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            TITLESEQ,
            TITLENAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            GEID,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            TEAM,
            OTC,
            WEIGHTACHIEVED,
            STANDARDSCORE,
            PRORATESCORE,
            TARGET,
            EXCESSTARGET,
            TOTALTARGET,
            PRODUCTS,
            PRODUCTTARGET,
            PRODUCTACTUAL,
            ACTUALPOINTS,
            COMMISSIONADJ,
            SALESREPCODE,
            REMARK_TXT,
            CATEGORY,
            ADJ2,
            PROTECTEDADJ
        )
        SELECT   /* ORIGSQL: (SELECT TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, POSITIONSEQ, POSITIONNAME, TI(...) */
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            TITLESEQ,
            TITLENAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            GEID,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            TEAM,
            MAX(OTC),
            SUM(WEIGHTACHIEVED),
            MAX(STANDARDSCORE),
            MAX(PRORATESCORE),
            SUM(TARGET),
            SUM(EXCESSTARGET),
            SUM(TOTALTARGET),
            PRODUCTS,
            PRODUCTTARGET,
            PRODUCTACTUAL,
            ACTUALPOINTS,
            SUM(COMMISSIONADJ),
            SALESREPCODE,
            NULL,
            NULL,
            MAX(ADJ2),
            SUM(PROTECTEDADJ)
        FROM
            ext.stel_rpt_ts_indpaysumm
        WHERE
            PERIODSEQ = :in_periodseq
            AND PROCESSINGUNITSEQ = :in_processingunitseq
        GROUP BY
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            TITLESEQ,
            TITLENAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            GEID,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            TEAM,
            PRODUCTS,
            PRODUCTTARGET,
            PRODUCTACTUAL,
            ACTUALPOINTS,
            SALESREPCODE
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --updation of remark   

    /* ORIGSQL: UPDATE STELEXT.stel_rpt_ts_paysumm a SET REMARK_TXT = (SELECT GENERICATTRIBUTE3 (...) */
    UPDATE EXT.stel_rpt_ts_paysumm a
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
        SET
        /* ORIGSQL: REMARK_TXT = */
        REMARK_TXT = (
            SELECT   /* ORIGSQL: (SELECT GENERICATTRIBUTE3 FROM cs_credit B, cs_credittype crd WHERE B.CREDITTYPE(...) */
                GENERICATTRIBUTE3
            FROM
                cs_credit B,
                cs_credittype crd
            WHERE
                B.CREDITTYPESEQ = CRD.DATATYPESEQ
                AND CRD.CREDITTYPEID = 'Payment Adjustment'
                AND CRD.REMOVEDATE =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND a.payeeseq = b.payeeseq
                AND a.positionseq = b.positionseq
                AND a.periodseq = b.periodseq
                AND a.processingunitseq = b.processingunitseq
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Updates on Ind and Team weight percentage   

    /* ORIGSQL: UPDATE STELEXT.stel_rpt_ts_paysumm a SET IND_WEIGHT = (SELECT WEIGHTACHIEVED FRO(...) */
    UPDATE EXT.stel_rpt_ts_paysumm a 
        SET
        /* ORIGSQL: IND_WEIGHT = */
        IND_WEIGHT = (
            SELECT   /* ORIGSQL: (SELECT WEIGHTACHIEVED FROM stel_rpt_ts_indpaysumm b WHERE PERIODSEQ = in_period(...) */
                WEIGHTACHIEVED
            FROM
                ext.stel_rpt_ts_indpaysumm b
            WHERE
                PERIODSEQ = :in_periodseq
                AND PROCESSINGUNITSEQ = :in_processingunitseq
                AND a.payeeseq = b.payeeseq
                AND a.positionseq = b.positionseq
                AND a.periodseq = b.periodseq
                AND b.EVENTTYPE = '1Individual Achievement'
                AND a.processingunitseq = b.processingunitseq
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE STELEXT.stel_rpt_ts_paysumm a SET TEAM_WEIGHT = (SELECT WEIGHTACHIEVED FR(...) */
    UPDATE EXT.stel_rpt_ts_paysumm a 
        SET
        /* ORIGSQL: TEAM_WEIGHT = */
        TEAM_WEIGHT = (
            SELECT   /* ORIGSQL: (SELECT WEIGHTACHIEVED FROM stel_rpt_ts_indpaysumm b WHERE PERIODSEQ = in_period(...) */
                WEIGHTACHIEVED
            FROM
                ext.stel_rpt_ts_indpaysumm b
            WHERE
                PERIODSEQ = :in_periodseq
                AND PROCESSINGUNITSEQ = :in_processingunitseq
                AND a.payeeseq = b.payeeseq
                AND a.positionseq = b.positionseq
                AND a.periodseq = b.periodseq
                AND b.EVENTTYPE = '2Team Achievement'
                AND a.processingunitseq = b.processingunitseq
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Individual Amount updation.   

    /* ORIGSQL: UPDATE STELEXT.stel_rpt_ts_paysumm a SET IND_AMOUNT = (SELECT TOTALTARGET FROM s(...) */
    UPDATE EXT.stel_rpt_ts_paysumm a 
        SET
        /* ORIGSQL: IND_AMOUNT = */
        IND_AMOUNT = (
            SELECT   /* ORIGSQL: (SELECT TOTALTARGET FROM stel_rpt_ts_indpaysumm b WHERE PERIODSEQ = in_periodseq(...) */
                TOTALTARGET
            FROM
                ext.stel_rpt_ts_indpaysumm b
            WHERE
                PERIODSEQ = :in_periodseq
                AND PROCESSINGUNITSEQ = :in_processingunitseq
                AND a.payeeseq = b.payeeseq
                AND a.positionseq = b.positionseq
                AND a.periodseq = b.periodseq
                AND b.EVENTTYPE = '1Individual Achievement'
                AND a.processingunitseq = b.processingunitseq
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Team Amount updation.   

    /* ORIGSQL: UPDATE STELEXT.stel_rpt_ts_paysumm a SET TEAM_AMOUNT = (SELECT TOTALTARGET FROM (...) */
    UPDATE EXT.stel_rpt_ts_paysumm a 
        SET
        /* ORIGSQL: TEAM_AMOUNT = */
        TEAM_AMOUNT = (
            SELECT   /* ORIGSQL: (SELECT TOTALTARGET FROM stel_rpt_ts_indpaysumm b WHERE PERIODSEQ = in_periodseq(...) */
                TOTALTARGET
            FROM
                ext.stel_rpt_ts_indpaysumm b
            WHERE
                PERIODSEQ = :in_periodseq
                AND PROCESSINGUNITSEQ = :in_processingunitseq
                AND a.payeeseq = b.payeeseq
                AND a.positionseq = b.positionseq
                AND a.periodseq = b.periodseq
                AND b.EVENTTYPE = '2Team Achievement'
                AND a.processingunitseq = b.processingunitseq
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Quarterly Amount updation.   

    /* ORIGSQL: UPDATE STELEXT.stel_rpt_ts_paysumm a SET QTR_AMOUNT = (SELECT TOTALTARGET FROM s(...) */
    UPDATE EXT.stel_rpt_ts_paysumm a 
        SET
        /* ORIGSQL: QTR_AMOUNT = */
        QTR_AMOUNT = (
            SELECT   /* ORIGSQL: (SELECT TOTALTARGET FROM stel_rpt_ts_indpaysumm b WHERE PERIODSEQ = in_periodseq(...) */
                TOTALTARGET
            FROM
                ext.stel_rpt_ts_indpaysumm b
            WHERE
                PERIODSEQ = :in_periodseq
                AND PROCESSINGUNITSEQ = :in_processingunitseq
                AND a.payeeseq = b.payeeseq
                AND a.positionseq = b.positionseq
                AND a.periodseq = b.periodseq
                AND b.EVENTTYPE = '3Quarterly Achievement'
                AND a.processingunitseq = b.processingunitseq
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Quarterly Amount updation.   

    /* ORIGSQL: UPDATE STELEXT.stel_rpt_ts_paysumm a SET SAA_AMOUNT = (SELECT TOTALTARGET FROM s(...) */
    UPDATE EXT.stel_rpt_ts_paysumm a 
        SET
        /* ORIGSQL: SAA_AMOUNT = */
        SAA_AMOUNT = (
            SELECT   /* ORIGSQL: (SELECT TOTALTARGET FROM stel_rpt_ts_indpaysumm b WHERE PERIODSEQ = in_periodseq(...) */
                TOTALTARGET
            FROM
                ext.stel_rpt_ts_indpaysumm b
            WHERE
                PERIODSEQ = :in_periodseq
                AND PROCESSINGUNITSEQ = :in_processingunitseq
                AND a.payeeseq = b.payeeseq
                AND a.positionseq = b.positionseq
                AND a.periodseq = b.periodseq
                AND b.EVENTTYPE = '4SAA'
                AND a.processingunitseq = b.processingunitseq
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: update STEL_RPT_TS_PAYSUMM A SET ACTUALPOINTS= ((SELECT MES.VALUE FROM stelext.S(...) */
    UPDATE EXT.STEL_RPT_TS_PAYSUMM A
        SET
        /* ORIGSQL: ACTUALPOINTS = */
        ACTUALPOINTS =   /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_TS_PAYSUMM' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
        (
            (
                SELECT   /* ORIGSQL: (SELECT MES.VALUE FROM stelext.STEL_RPT_TS_PAYSUMM I, CS_MEASUREMENT MES WHERE i(...) */
                    MES.VALUE
                FROM
                    EXT.STEL_RPT_TS_PAYSUMM I,
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
                    AND A.POSITIONSEQ = MES.POSITIONSEQ
                    AND A.PERIODSEQ = MES.PERIODSEQ
                    AND A.PAYEESEQ = MES.PAYEESEQ
            ) *100)
        WHERE
            a.periodseq = :in_periodseq
            AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;  

    /* ORIGSQL: update STEL_RPT_TS_PAYSUMM A SET PRORATESCORE = (SELECT MES.PRORATESCORE FROM st(...) */
    UPDATE EXT.STEL_RPT_TS_PAYSUMM A 
        SET
        /* ORIGSQL: PRORATESCORE = */
        PRORATESCORE = (
            SELECT   /* ORIGSQL: (SELECT MES.PRORATESCORE FROM stelext.STEL_RPT_TS_PAYSUMM I, stel_rpt_ts_indpays(...) */
                MES.PRORATESCORE
            FROM
                EXT.STEL_RPT_TS_PAYSUMM I,
                ext.stel_rpt_ts_indpaysumm MES
            WHERE
                i.periodseq = :in_periodseq
                AND i.processingunitseq = :in_processingunitseq
                AND I.POSITIONSEQ = MES.POSITIONSEQ
                AND I.PERIODSEQ = MES.PERIODSEQ
                AND I.PAYEESEQ = MES.PAYEESEQ
                AND I.PROCESSINGUNITSEQ = MES.PROCESSINGUNITSEQ
                AND MES.EVENTTYPE = '2Team Achievement'
                AND A.POSITIONSEQ = MES.POSITIONSEQ
                AND A.PERIODSEQ = MES.PERIODSEQ
                AND A.PAYEESEQ = MES.PAYEESEQ
        )
    WHERE
        a.periodseq = :in_periodseq
        AND a.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END