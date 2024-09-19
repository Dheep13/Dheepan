CREATE PROCEDURE EXT.STEL_PRC_MMPPAYSUM
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
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'STEL_PRC_MMPPAYSUM';

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

    -- Calling below procedure to populate dealer and vendor code data.

    /* ORIGSQL: STEL_PRC_MMPPARTPOSITION (IN_PERIODSEQ, IN_PROCESSINGUNITSEQ) */
    CALL EXT.STEL_PRC_MMPPARTPOSITION(:IN_PERIODSEQ, :IN_PROCESSINGUNITSEQ);

    -- Deleting temp table data 
    /* ORIGSQL: DELETE STEL_RPT_MMP_PAYSUMM_TMP WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNI(...) */
    DELETE
    FROM
        STEL_RPT_MMP_PAYSUMM_TMP
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --- FTTH Data insertion.
    -- Data insertion for  first section of report. 
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_PAYSUMM_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PE(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    INSERT INTO EXT.STEL_RPT_MMP_PAYSUMM_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            PRODUCT,
            SCHEMETYPE,
            SALESCHANNEL,
            SCHEME,
            CONTRACT_MONTH,
            CONNECTION,
            VALUE,
            SECTIONNO,
            RPTTYPE
        )
        SELECT   /* ORIGSQL: SELECT CRD.TENANTID, CRD.PERIODSEQ, CRD.PROCESSINGUNITSEQ, v_PeriodName PERIODNA(...) */
            CRD.TENANTID,
            CRD.PERIODSEQ,
            CRD.PROCESSINGUNITSEQ,
            :v_PeriodName AS PERIODNAME,
            CRD.PAYEESEQ,
            CRD.POSITIONSEQ,
            PRC.NAME AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            'FTTH Activation Incentive' AS PRODUCT,
            CRD.GENERICATTRIBUTE4 AS SCHEMETYPE,
            CRD.GENERICATTRIBUTE6 AS SALESCHANNEL,
            CRD.GENERICATTRIBUTE3 AS SCHEME,
            CRD.GENERICATTRIBUTE7 AS CONTRACT_MONTH,
            CRD.GENERICNUMBER2 AS CONNECTION,
            CRD.GENERICNUMBER1 AS VALUE,
            1,
            'FTTH' AS RPTTYPE
        FROM
            cs_credit crd,
            cs_credittype ctype,
            cs_processingunit prc
        WHERE
            CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND ctype.credittypeid IN ('MMP')
            AND CRD.GENERICATTRIBUTE9 = 'M'
            AND CRD.GENERICATTRIBUTE2 = 'BroadBand Closed'
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CRD.PERIODSEQ = :IN_PERIODSEQ
            AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Data insertion for  second section of report. 
    /* ORIGSQL: INSERT INTO ext._RPT_MMP_PAYSUMM_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PE(...) */
    INSERT INTO EXT.STEL_RPT_MMP_PAYSUMM_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            PRODUCT,
            SCHEMETYPE,
            SALESCHANNEL,
            SCHEME,
            CONTRACT_MONTH,
            CONNECTION,
            VALUE,
            SECTIONNO,
            RPTTYPE
        )
        SELECT   /* ORIGSQL: SELECT CRD.TENANTID, CRD.PERIODSEQ, CRD.PROCESSINGUNITSEQ, v_PeriodName PERIODNA(...) */
            CRD.TENANTID,
            CRD.PERIODSEQ,
            CRD.PROCESSINGUNITSEQ,
            :v_PeriodName AS PERIODNAME,
            CRD.PAYEESEQ,
            CRD.POSITIONSEQ,
            PRC.NAME AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            'FTTH Activation Incentive' AS PRODUCT,
            CRD.GENERICATTRIBUTE4 AS SCHEMETYPE,
            CRD.GENERICATTRIBUTE6 AS SALESCHANNEL,
            'SNBB VAS' AS SCHEME,
            CRD.GENERICATTRIBUTE7 AS CONTRACT_MONTH,
            CRD.GENERICNUMBER2 AS CONNECTION,
            CRD.GENERICNUMBER1 AS VALUE,
            2,
            'FTTH'
        FROM
            cs_credit crd,
            cs_credittype ctype,
            cs_processingunit prc
        WHERE
            CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND ctype.credittypeid = 'MMP'
            AND CRD.GENERICATTRIBUTE9 = 'S'
            AND CRD.GENERICATTRIBUTE2 = 'BroadBand Closed'
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CRD.PERIODSEQ = :IN_PERIODSEQ
            AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- MioTV data insertion

    -- Data insertion for  first section of report. 
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_PAYSUMM_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PE(...) */
    INSERT INTO EXT.STEL_RPT_MMP_PAYSUMM_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            PRODUCT,
            SCHEMETYPE,
            SALESCHANNEL,
            SCHEME,
            CONTRACT_MONTH,
            CONNECTION,
            VALUE,
            SECTIONNO,
            RPTTYPE
        )
        SELECT   /* ORIGSQL: SELECT CRD.TENANTID, CRD.PERIODSEQ, CRD.PROCESSINGUNITSEQ, v_PeriodName PERIODNA(...) */
            CRD.TENANTID,
            CRD.PERIODSEQ,
            CRD.PROCESSINGUNITSEQ,
            :v_PeriodName AS PERIODNAME,
            CRD.PAYEESEQ,
            CRD.POSITIONSEQ,
            PRC.NAME AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            'MioTV' AS PRODUCT,
            CRD.GENERICATTRIBUTE4 AS SCHEMETYPE,
            CRD.GENERICATTRIBUTE6 AS SALESCHANNEL,
            CRD.GENERICATTRIBUTE3 AS SCHEME,
            CRD.GENERICATTRIBUTE7 AS CONTRACT_MONTH,
            CRD.GENERICNUMBER2 AS CONNECTION,
            CRD.GENERICNUMBER1 AS VALUE,
            1,
            'MioTV' AS RPTTYPE
        FROM
            cs_credit crd,
            cs_credittype ctype,
            cs_processingunit prc
        WHERE
            CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND ctype.credittypeid IN
            ('MMP', 'MMP Smart Home', 'MMP TV Content')
            AND CRD.GENERICATTRIBUTE9 = 'M'
            AND CRD.GENERICATTRIBUTE2 = 'TV Closed'
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CRD.PERIODSEQ = :IN_PERIODSEQ
            AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Data insertion for  first section of report. 
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_PAYSUMM_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PE(...) */
    INSERT INTO EXT.STEL_RPT_MMP_PAYSUMM_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            PRODUCT,
            SCHEMETYPE,
            SALESCHANNEL,
            SCHEME,
            CONTRACT_MONTH,
            CONNECTION,
            VALUE,
            SECTIONNO,
            RPTTYPE
        )
        SELECT   /* ORIGSQL: SELECT CRD.TENANTID, CRD.PERIODSEQ, CRD.PROCESSINGUNITSEQ, v_PeriodName PERIODNA(...) */
            CRD.TENANTID,
            CRD.PERIODSEQ,
            CRD.PROCESSINGUNITSEQ,
            :v_PeriodName AS PERIODNAME,
            CRD.PAYEESEQ,
            CRD.POSITIONSEQ,
            PRC.NAME AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            'MioTV' AS PRODUCT,
            CRD.GENERICATTRIBUTE4 AS SCHEMETYPE,
            CRD.GENERICATTRIBUTE6 AS SALESCHANNEL,
            'Singtel TV AddOn' AS SCHEME,/* --CRD.GENERICATTRIBUTE3 SCHEME, */  CRD.GENERICATTRIBUTE7 AS CONTRACT_MONTH,
            CRD.GENERICNUMBER2 AS CONNECTION,
            CRD.GENERICNUMBER1 AS VALUE,
            1,
            'MioTV' AS RPTTYPE
        FROM
            cs_credit crd,
            cs_credittype ctype,
            cs_processingunit prc
        WHERE
            CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND ctype.credittypeid IN
            ('MMP', 'MMP Smart Home', 'MMP TV Content')
            AND CRD.GENERICATTRIBUTE9 = 'S'
            AND CRD.GENERICATTRIBUTE2 = 'TV Closed'
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CRD.PERIODSEQ = :IN_PERIODSEQ
            AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Adding data into final reporting table.

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_MMP_PAYSUMM WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSEQ(...) */
    DELETE
    FROM
        EXT.STEL_RPT_MMP_PAYSUMM
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_MMP_PAYSUMM') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_MMP_PAYSUMM');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_MMP_PAYSUMM_TMP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_FTTHREQMEMO' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_MMP_PAYSUMM' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_PAYSUMM (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIOD(...) */
    INSERT INTO EXT.STEL_RPT_MMP_PAYSUMM
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            PRODUCT,
            SCHEMETYPE,
            SALESCHANNEL,
            SCHEME,
            CONTRACT_MONTH,
            CONNECTION,
            VALUE,
            SECTIONNO,
            RPTTYPE
        )
        SELECT   /* ORIGSQL: (SELECT A1.TENANTID, A1.PERIODSEQ, A1.PROCESSINGUNITSEQ, A1.PERIODNAME, A2.PAYEE(...) */
            A1.TENANTID,
            A1.PERIODSEQ,
            A1.PROCESSINGUNITSEQ,
            A1.PERIODNAME,
            A2.PAYEESEQ,
            A2.MGRPOSITIONSEQ,
            A2.MGRNAME,
            A1.PROCESSINGUNITNAME,
            A1.CALENDARNAME,
            A1.PRODUCT,
            A1.SCHEMETYPE,
            A1.SALESCHANNEL,
            A1.SCHEME,
            A1.CONTRACT_MONTH,
            SUM(A1.CONNECTION) AS CONNECTION,
            MAX(A1.VALUE) AS VALUE,
            A1.SECTIONNO,
            A1.RPTTYPE
        FROM
            EXT.STEL_RPT_MMP_PAYSUMM_TMP A1,
            EXT.STEL_RPT_DATA_FTTHREQMEMO A2
        WHERE
            A1.PERIODSEQ = :IN_PERIODSEQ
            AND A1.PERIODSEQ = A2.PERIODSEQ
            AND A1.PROCESSINGUNITSEQ = A2.PROCESSINGUNITSEQ
            AND A1.POSITIONSEQ = A2.POSITIONSEQ
            AND A1.PAYEESEQ = A2.PAYEESEQ
        GROUP BY
            A1.TENANTID,
            A1.PERIODSEQ,
            A1.PROCESSINGUNITSEQ,
            A1.PERIODNAME,
            A2.PAYEESEQ,
            A2.MGRPOSITIONSEQ,
            A2.MGRNAME,
            A1.PROCESSINGUNITNAME,
            A1.CALENDARNAME,
            A1.PRODUCT,
            A1.SCHEMETYPE,
            A1.SALESCHANNEL,
            A1.SCHEME,
            A1.CONTRACT_MONTH,
            A1.SECTIONNO,
            A1.RPTTYPE
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Below block for Adjustment entry   
    /* ORIGSQL: UPDATE STEL_RPT_MMP_PAYSUMM A SET OTHER_adj = (SELECT SUM(VALUE) FROM cs_credit (...) */
    UPDATE EXT.STEL_RPT_MMP_PAYSUMM A
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
        SET
        /* ORIGSQL: OTHER_adj = */
        OTHER_adj = (
            SELECT   /* ORIGSQL: (SELECT SUM(VALUE) FROM cs_credit B, cs_credittype crd WHERE CRD.CREDITTYPEID = (...) */
                SUM(VALUE) 
            FROM
                cs_credit B,
                cs_credittype crd
            WHERE
                CRD.CREDITTYPEID = 'Payment Adjustment'
                AND CRD.REMOVEDATE =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND periodseq = :IN_PERIODSEQ
                AND VALUE <> 0
                AND CRD.DATATYPESEQ = B.CREDITTYPESEQ
                /* -- AND GENERICATTRIBUTE2 IN ('TV Closed', 'BroadBand Closed') */
                /* -- AND GENERICATTRIBUTE9 IN ('M', 'S') */
                AND B.GENERICATTRIBUTE6 = A.SALESCHANNEL
        );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END