CREATE PROCEDURE EXT.RPT_LC_PAYOUTSUMM
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

    v_ComponentName = 'RPT_LC_PAYOUTSUMM';

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

    /* RESOLVE: Identifier not found: Table/view 'STELEXT.STEL_RPT_LC_PAYOUTSUMM' not found */

    /* ORIGSQL: DELETE FROM stelext.STEL_RPT_LC_PAYOUTSUMM WHERE periodseq = in_periodseq AND pr(...) */
    DELETE
    FROM
        ext.STEL_RPT_LC_PAYOUTSUMM
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (in_periodseq, 'STEL_RPT_LC_PAYOUTSUMM') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'STEL_RPT_LC_PAYOUTSUMM');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_LC_PAYOUTSUMM' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_LC_PAYOUTSUMM (TENANTID, PERIODSEQ, PERIODNAME, PAYEESEQ, P(...) */
    INSERT INTO STEL_RPT_LC_PAYOUTSUMM
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            /* --                                       PARTNERCODE, */
            /* --                                       PARTNERNAME, */
            NAME,
            ALTERNATENAME,
            PRODUCT,
            QUANTITY
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenantid, in_periodseq, v_periodname, CRD.payeeseq, CRD.positionseq, N(...) */
            :v_Tenantid,
            :in_periodseq,
            :v_periodname,
            CRD.payeeseq,
            CRD.positionseq,
            NULL AS positionname,
            CRD.processingunitseq,
            PRC.NAME AS processingunitname,
            :v_Calendarname,
            STR.GENERICATTRIBUTE2 AS NAME,
            STR.GENERICATTRIBUTE4 AS ALTERNATENAME,
            IFNULL(TO_VARCHAR(CRD.GENERICATTRIBUTE3),'') || ' ' || IFNULL(TO_VARCHAR(CRD.GENERICATTRIBUTE4),'') AS PRODUCT,
            CRD.GENERICNUMBER1
        FROM
            cs_salestransaction str,
            cs_credit crd,
            cs_processingunit prc
        WHERE
            STR.SALESTRANSACTIONSEQ = CRD.SALESTRANSACTIONSEQ
            AND STR.SALESORDERSEQ = CRD.SALESORDERSEQ
            AND CRD.PERIODSEQ = :in_periodseq
            AND CRD.PROCESSINGUNITSEQ = PRC.PROCESSINGUNITSEQ
            AND CRD.PROCESSINGUNITSEQ = :in_processingunitseq
            AND CRD.GENERICATTRIBUTE1 = 'Dealer - Live Chat'
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --   --updation of deposit amount
    --
    --   UPDATE STELEXT.STEL_RPT_TS_INDPAYSUMM a
    --      SET COMMISSIONADJ =
    --             (SELECT NVL (VALUE, 0)
        --                FROM cs_deposit b
        --               WHERE     a.payeeseq = b.payeeseq
        -- AND a.positionseq = b.positionseq
        -- AND a.periodseq = b.periodseq
        -- AND a.processingunitseq = b.processingunitseq
    -- AND b.genericattribute1 = 'Deposit - Pay Adj')
    --    WHERE a.periodseq = in_periodseq
    -- AND a.processingunitseq = in_processingunitseq;
    --
    --   COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (V_TENANTID, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSe(...) */
    CALL EXT.STEL_LOG(:v_Tenantid, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END