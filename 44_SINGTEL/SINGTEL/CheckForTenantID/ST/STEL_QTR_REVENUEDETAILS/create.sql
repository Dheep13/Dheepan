CREATE PROCEDURE EXT.STEL_QTR_REVENUEDETAILS
(
    IN in_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_rpttype IN VARCHAR2 */
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
    DECLARE v_Tenant VARCHAR(10) = 'LGAP';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'LGAP'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_qtrperiodseq BIGINT;  /* ORIGSQL: v_qtrperiodseq INTEGER; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'STEL_QTR_REVENUEDETAILS';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME
    INTO
        v_CalendarName
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT
        PARENTSEQ
    INTO
        v_qtrperiodseq
    FROM
        cs_period
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_REVENUEDETAILS WHERE PERIODSEQ = v_qtrperiodseq AND PROCESSINGUN(...) */
    DELETE
    FROM
        EXT.STEL_RPT_REVENUEDETAILS
    WHERE
        PERIODSEQ = :v_qtrperiodseq
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_REVENUEDETAILS') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_REVENUEDETAILS');

    -- managing table partitions  

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_ADVPAYDETAILS' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_REVENUEDETAILS' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_REVENUEDETAILS (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PER(...) */
    INSERT INTO EXT.STEL_RPT_REVENUEDETAILS
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            QTRNAME,
            QTRPERIODSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            MANAGERSEQ,
            PAYEESEQ,
            EMPLOYEEID,
            EMPNAME,
            EMPDESIGNATION,
            SUBORDINATEID,
            SUBORDINATENAME,
            IO,
            STATUS,
            CONTRACT_CDATE,
            MC,
            PRODUCT_GROUP,
            PRODUCT,
            SPONSERSHIP,
            AGENCY,
            AGENCY_GROUP,
            CLIENT,
            Campaign,
            CONTRACT_AMT,
            INDUSTRY,
            REVENUE,
            COMMISSION_MONTH,
            COMMENTS,
            CRETEDATE
        )
        
            SELECT   /* ORIGSQL: (SELECT R1.TENANTID, IN_PERIODSEQ, R1.PROCESSINGUNITSEQ, R1.PERIODNAME, NULL QTR(...) */
                R1.TENANTID,
                /* --R1.PERIODSEQ, */
                :IN_PERIODSEQ,
                R1.PROCESSINGUNITSEQ,
                R1.PERIODNAME,
                NULL AS QTRNAME,
                :v_qtrperiodseq,
                R1.PROCESSINGUNITNAME,
                :v_CalendarName,
                R1.POSITIONSEQ,
                P1.MANAGERSEQ AS MANAGERSEQ,
                R1.PAYEESEQ,
                R1.POSITIONNAME,
                P1.PAYEENAME,
                P1.TITLE,
                NULL AS SUBORDINATEID,
                NULL AS SUBORDINATENAME,
                R1.ORDERID AS IO,
                NULL AS STATUS,
                R1.CONTRACTCLOSEDATE,
                R1.MCNUMBER,
                NULL AS PRODUCT_GROUP,
                R1.PRODUCTNAME,
                R1.SPONSORSHIP,
                R1.AGENCY,
                R1.AGENCYGROUP AS AGENCY_GROUP,
                R1.CLIENT,
                R1.CAMPAIGN,
                R1.CONTRACTAMT,
                R1.INDUSTRY,
                R1.CREDITVALUE AS REVENUE,
                R1.COMPENSATIONDATE,
                R1.COMMENTS,
                CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            FROM
                EXT.STEL_RPT_DATA_ADVPAYDETAILS R1,
                EXT.STEL_POSPART_MASTER P1
            WHERE
                R1.PERIODSEQ IN
                (
                    SELECT   /* ORIGSQL: (SELECT periodseq FROM cs_period WHERE parentseq IN (SELECT parentseq FROM cs_pe(...) */
                        periodseq
                    FROM
                        cs_period
                    WHERE
                        parentseq IN
                        (
                            SELECT   /* ORIGSQL: (SELECT parentseq FROM cs_period WHERE periodseq = IN_PERIODSEQ AND removedate =(...) */
                                parentseq
                            FROM
                                cs_period
                            WHERE
                                periodseq = :IN_PERIODSEQ
                                AND removedate =
                                to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                                'MM/DD/YYYY')
                        )
                        AND removedate =
                        to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                )
                AND R1.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                AND R1.POSITIONSEQ = P1.POSITIONSEQ
                AND R1.PAYEESEQ = P1.PAYEESEQ
                AND R1.PERIODSEQ = P1.PERIODSEQ
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE STEL_RPT_REVENUEDETAILS T1 SET T1.QTRNAME = (SELECT NAME FROM CS_PERIOD W(...) */
    UPDATE EXT.STEL_RPT_REVENUEDETAILS T1 
        SET
        /* ORIGSQL: T1.QTRNAME = */
        QTRNAME = (
            SELECT   /* ORIGSQL: (SELECT NAME FROM CS_PERIOD WHERE PERIODSEQ = (SELECT PARENTSEQ FROM CS_PERIOD W(...) */
                NAME
            FROM
                CS_PERIOD
            WHERE
                PERIODSEQ  
                =
                (
                    SELECT   /* ORIGSQL: (SELECT PARENTSEQ FROM CS_PERIOD WHERE PERIODSEQ = IN_PERIODSEQ AND removedate =(...) */
                        PARENTSEQ
                    FROM
                        CS_PERIOD
                    WHERE
                        PERIODSEQ = :IN_PERIODSEQ
                        AND removedate =
                        to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                )
                AND removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
        );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END