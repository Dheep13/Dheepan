CREATE PROCEDURE EXT.STEL_PRC_MMPINTPARTPOS
(
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
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'EXT.STEL_PRC_MMPINTPARTPOS';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME,
        PERIODNAME
    INTO
        v_CalendarName,
        v_PeriodName
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    -- Deleting  table data For participant 
    /* ORIGSQL: DELETE STEL_RPT_DATA_FTTHREQMEMO WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUN(...) */
    DELETE
    FROM
        EXT.STEL_RPT_DATA_FTTHREQMEMO
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_DATA_FTTHREQMEMO') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_DATA_FTTHREQMEMO');

    -- managing table partitions

    -- Inserting data for Integrated sales payee
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_FTTHREQMEMO' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_DATA_FTTHREQMEMO (PERIODSEQ, PERIODNAME, PROCESSINGUNITSEQ,(...) */
    INSERT INTO EXT.STEL_RPT_DATA_FTTHREQMEMO
        (
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            POSNAME,
            MGRPOSITIONSEQ,
            PAYEENAME,
            MGRNAME,
            MGCODE,
            SALESCHANNEL,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT IN_PERIODSEQ PERIODSEQ, v_periodname PERIODNAME, IN_PROCESSINGUNITSEQ PR(...) */
            :IN_PERIODSEQ AS PERIODSEQ,
            :v_PeriodName AS PERIODNAME,
            :IN_PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
            p.payeeseq,
            pos.ruleelementownerseq AS POSITIONSEQ,
            POS.NAME AS POSNAME,
            pos.ruleelementownerseq AS MGRPOSITIONSEQ,
            PAR.LASTNAME AS PAYEENAME,
            PAR.LASTNAME AS MGRNAME,
            POS.NAME AS MGCODE,
            'INT' AS SALESCHANNEL,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        FROM
            cs_participant par,
            cs_payee p,
            cs_period prd,
            cs_position pos,
            cs_title tlt,
            cs_processingunit prc
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND p.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND par.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND pos.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND prd.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND TLT.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND p.effectivestartdate <= prd.enddate
            AND p.effectiveenddate > prd.enddate
            AND par.effectivestartdate <= prd.enddate
            AND par.effectiveenddate > prd.enddate
            AND pos.effectivestartdate <= prd.enddate
            AND pos.effectiveenddate > prd.enddate
            AND tlt.effectivestartdate <= prd.enddate
            AND tlt.effectiveenddate > prd.enddate
            AND TLT.RULEELEMENTOWNERSEQ = POS.TITLESEQ
            -- AND TLT.NAME IN ('MMP Dummy Vendor', 'External Vendors')
            AND PRD.periodseq = :IN_PERIODSEQ
            AND PRC.processingunitseq = :IN_PROCESSINGUNITSEQ
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END