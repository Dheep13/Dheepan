CREATE PROCEDURE EXT.RPT_D2D_TRANSACTIONDET
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
    DECLARE v_Tenant VARCHAR(10) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'STEL'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_periodEndDate TIMESTAMP;  /* ORIGSQL: v_periodEndDate date; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'RPT_d2d_TRANSACTIONDET';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME,
        PERIODNAME,
        enddate
    INTO
        v_CalendarName,
        v_PeriodName,
        v_periodEndDate
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    -- Calling master procedure to populate participant

    /* ORIGSQL: STEL_CUSTOMERMASTER (IN_PERIODSEQ, IN_PROCESSINGUNITSEQ) */
    CALL EXT.STEL_CUSTOMERMASTER(:IN_PERIODSEQ, :IN_PROCESSINGUNITSEQ);

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_D2D_TRANSACTIONDET WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSING(...) */
    DELETE
    FROM
        EXT.STEL_RPT_D2D_TRANSACTIONDET
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_D2D_TRANSACTIONDET') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_D2D_TRANSACTIONDET');

    -- managing table partitions

    -- Individual achievement

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER_IP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_D2D_TRANSACTIONDET' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_D2D_TRANSACTIONDET (TENANTID, PERIODSEQ, PERIODNAME, PAYEES(...) */
    INSERT INTO EXT.STEL_RPT_D2D_TRANSACTIONDET
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
            ORDERID,
            PRODUCTS,
            SALESMANCODE,
            AGENCY,
            CREATEDATE,
            ORDERACTION,
            AI_TYPE,
            AI,
            TEAMLEADER,
            SENIOR_TL,
            MANAGER
        )
        SELECT   /* ORIGSQL: (SELECT P1.TENANTID, P1.PERIODSEQ, P1.PERIODNAME, P1.PAYEESEQ, P1.POSITIONSEQ, P(...) */
            P1.TENANTID,
            P1.PERIODSEQ,
            P1.PERIODNAME,
            P1.PAYEESEQ,
            P1.POSITIONSEQ,
            P1.POSITIONNAME,
            P1.TITLESEQ,
            P1.TITLE,
            P1.PROCESSINGUNIT,
            P1.PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            ORD.ORDERID,
            crd.genericattribute4,
            STR.GENERICATTRIBUTE1,
            STR.GENERICATTRIBUTE2,
            STR.COMPENSATIONDATE,
            STR.GENERICATTRIBUTE6,
            STR.PRODUCTID,
            STR.VALUE,
            NULL,
            NULL,
            NULL
        FROM
            EXT.STEL_POSPART_MASTER_IP P1,
            CS_CREDIT CRD,
            CS_SALESTRANSACTION STR,
            CS_SALESORDER ORD
        WHERE
            P1.PERIODSEQ = :IN_PERIODSEQ
            AND P1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND P1.POSITIONSEQ = CRD.POSITIONSEQ
            AND P1.PERIODSEQ = CRD.PERIODSEQ
            AND P1.PAYEESEQ = CRD.PAYEESEQ
            AND CRD.SALESORDERSEQ = STR.SALESORDERSEQ
            AND CRD.SALESTRANSACTIONSEQ = STR.SALESTRANSACTIONSEQ
            AND STR.SALESORDERSEQ = ORD.SALESORDERSEQ
            AND STR.VALUE <> 0
            AND P1.TITLE LIKE '%D2D%'
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_D2D_TRANSACTIONDET tgt using (SELECT p.ruleelementownerseq, (...) */
    MERGE INTO EXT.STEL_RPT_D2D_TRANSACTIONDET AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select p.ruleelementownerseq, p1.name m1, p2.name m2, p3.name m3, t.name title (...) */
                p.ruleelementownerseq,
                p1.name AS m1,
                p2.name AS m2,
                p3.name AS m3,
                t.name AS title /* --these may need to change to mpar1.ga1, mpar2.ga1 etc. */
            FROM
                cs_position p
            INNER JOIN
                cS_participant par
                ON par.payeeseq = p.payeeseq
                AND par.removedate = :v_eot
                AND TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_periodenddate-1 */
                                                                                                                                                                 /* ORIGSQL: par.effectiveenddate-1 */
            INNER JOIN
                cs_Title t
                ON p.titleseq = t.ruleelementownerseq
                AND t.name LIKE '%D2D%'
                AND t.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            LEFT OUTER JOIN
                cs_position p1
                ON p1.ruleelementownerseq = p.managerseq
                AND p1.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1))) BETWEEN p1.effectivestartdate AND TO_DATE(ADD_SECONDS(p1.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_periodenddate-1 */
                                                                                                                                                               /* ORIGSQL: p1.effectiveenddate-1 */
            LEFT OUTER JOIN
                cs_position p2
                ON p2.ruleelementownerseq = p1.managerseq
                AND p2.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1))) BETWEEN p2.effectivestartdate AND TO_DATE(ADD_SECONDS(p2.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_periodenddate-1 */
                                                                                                                                                               /* ORIGSQL: p2.effectiveenddate-1 */
            LEFT OUTER JOIN
                cs_position p3
                ON p3.ruleelementownerseq = p2.managerseq
                AND p3.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1))) BETWEEN p3.effectivestartdate AND TO_DATE(ADD_SECONDS(p3.effectiveenddate,(86400*-1))) /*
                left join cs_participant mpar1
                on mpar1.payeeseq=p1.payeeseq and mpar1.removedate>sysdate
                and v_periodenddate-1 between mpar1.effectivestartdate and mpar1.effectiveenddate-1
                left join cs_participant mpar2
                on mpar2.payeeseq=p2.payeeseq and mpar2.removedate>sysdate
                and v_periodenddate-1 between mpar2.effectivestartdate and mpar2.effectiveenddate-1
                left join cs_participant mpar3
                on mpar3.payeeseq=p3.payeeseq and mpar3.removedate>sysdate
                and v_periodenddate-1 between mpar3.effectivestartdate and mpar3.effectiveenddate-1*/   /* ORIGSQL: v_periodenddate-1 */
                                                                                                                       /* ORIGSQL: p3.effectiveenddate-1 */
            WHERE
                p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1))) BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_periodenddate-1 */
                                                                                                                                                             /* ORIGSQL: p.effectiveenddate-1 */
                AND TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1))) BETWEEN t.effectivestartdate AND TO_DATE(ADD_SECONDS(t.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_periodenddate-1 */
                                                                                                                                                             /* ORIGSQL: t.effectiveenddate-1 */
        ) AS src
        ON(tgt.positionseq = src.ruleelementownerseq AND tgt.PERIODSEQ = :IN_PERIODSEQ AND tgt.PROCESSINGUNITseq = :IN_PROCESSINGUNITSEQ)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.teamleader =
            CASE
                WHEN src.title = 'D2D - Sales Rep'
                THEN src.m1
                WHEN src.title = 'D2D - Team Lead'
                THEN TO_NVARCHAR(tgt.positionname,NULL)  /* ORIGSQL: to_nchar(tgt.positionname) */
            END
            ,tgt.senior_TL =
            CASE
                WHEN src.title = 'D2D - Sales Rep'
                THEN src.m2
                WHEN src.title = 'D2D - Team Lead'
                THEN src.m1
                WHEN src.title = 'D2D - Senior Team Lead'
                THEN TO_NVARCHAR(tgt.positionname,NULL)  /* ORIGSQL: to_nchar(tgt.positionname) */
            END
            ,tgt.manager =
            CASE
                WHEN src.title = 'D2D - Sales Rep'
                THEN src.m3
                WHEN src.title = 'D2D - Team Lead'
                THEN src.m2
                WHEN src.title = 'D2D - Senior Team Lead'
                THEN src.m1
                WHEN src.title = 'D2D - Manager'
                THEN TO_NVARCHAR(tgt.positionname,NULL)  /* ORIGSQL: to_nchar(tgt.positionname) */
            END
        --WHERE
        --    tgt.PERIODSEQ = :IN_PERIODSEQ
        --    AND tgt.PROCESSINGUNITseq = :IN_PROCESSINGUNITSEQ
        ;

    /* ORIGSQL: commit; */
    COMMIT;

    /*
    D2D - Manager
    D2D - Sales Rep
    D2D - Senior Team Lead
    D2D - Team Lead
    Dealer_D2D
    */
    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END