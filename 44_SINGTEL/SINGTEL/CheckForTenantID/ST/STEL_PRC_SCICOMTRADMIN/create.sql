CREATE PROCEDURE EXT.STEL_PRC_SCICOMTRADMIN
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
    DECLARE v_Tenant VARCHAR(10) = 'LGAP';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'LGAP'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'STEL_PRC_SCICOMTRADMIN';

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

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_SCICOMTRADMIN WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITS(...) */
    DELETE
    FROM
        EXT.STEL_RPT_SCICOMTRADMIN
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_SCICOMTRADMIN') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_SCICOMTRADMIN');

    -- managing table partitions

    -- MOBILE NEIN_PROCESSINGUNITSEQW Sales Matrix

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_ADDRESSTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONASSIGNMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_SCICOMTRADMIN' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_SCICOMTRADMIN (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERI(...) */
    INSERT INTO EXT.STEL_RPT_SCICOMTRADMIN
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            TEAM,
            DEALERCODE,
            SMCODE,
            CUSTID,
            CUSTNAME,
            SERVICENO,
            SALESTYPE,
            SOURCE,
            PRODNAME,
            COUNT,
            SINGDATE,
            NEWCUSTOMER,
            ORDERID,
            TVNUMBER,
            CREATEDATE
        )
        
            SELECT   /* ORIGSQL: (SELECT ST.TENANTID, IN_PERIODSEQ, IN_PROCESSINGUNITSEQ, v_PeriodName, NULL, v_C(...) */
                ST.TENANTID,
                :IN_PERIODSEQ,
                :IN_PROCESSINGUNITSEQ,
                :v_PeriodName,
                NULL,
                :v_CalendarName,
                POS.GENERICATTRIBUTE1 AS TEAM,
                ST.GENERICATTRIBUTE4 AS DEALERCODE,
                ST.GENERICATTRIBUTE2 AS SMCODE,
                ADDR.CUSTID,
                ADDR.address1 AS CUSTNAME,
                ADDR.contact AS SERVICENO,
                ST.GENERICATTRIBUTE5 AS SALESTYPE,
                ST.GENERICATTRIBUTE15 AS SOURCE,
                CSF.CLASSFIERNAME AS PRODUCTNAME,
                CRD.VALUE,
                CRD.COMPENSATIONDATE,
                CASE
                    WHEN ST.GENERICATTRIBUTE5 LIKE '%NEW%'
                    THEN 'Y'
                    ELSE 'N'
                END
                AS NEWCUSTOMER,
                ST.ALTERNATEORDERNUMBER AS ORDERID,
                CASE
                    WHEN ST.GENERICATTRIBUTE5 LIKE '%TV%'
                    THEN 'Y'
                    ELSE 'N'
                END
                AS TVNUMBER,
                CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            FROM
                CS_SALESTRANSACTION ST,
                CS_CREDIT CRD,
                cs_Credittype crdt,
                EXT.STEL_CLAssifier CSF,
                CS_PERIOD PRD,
                CS_TRANSACTIONASSIGNMENT TA,
                CS_POSITION POS,
                CS_TITLE TLT,
                (
                    SELECT   /* ORIGSQL: (SELECT ST.SALESTRANSACTIONSEQ, ST.SALESORDERSEQ, tad.custid, tad.contact, tad.a(...) */
                        ST.SALESTRANSACTIONSEQ,
                        ST.SALESORDERSEQ,
                        tad.custid,
                        tad.contact,
                        tad.address1
                    FROM
                        cs_salestransaction st
                    INNER JOIN
                        cs_transactionassignment ta
                        ON st.salestransactionseq = ta.salestransactionseq
                        AND st.salesorderseq = ta.salesorderseq
                        AND st.tenantid = ta.tenantid
                        AND st.processingunitseq = ta.processingunitseq
                        AND st.compensationdate = ta.compensationdate
                        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONADDRESS' not found */
                    LEFT OUTER JOIN
                        (
                            SELECT   /* ORIGSQL: (SELECT * FROM cs_transactionaddress WHERE addresstypeseq = (SELECT addresstypes(...) */
                                *
                            FROM
                                cs_transactionaddress
                            WHERE
                                addresstypeseq =
                                (
                                    SELECT   /* ORIGSQL: (SELECT addresstypeseq FROM cs_addresstype WHERE addresstypeid = 'BILLTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                    WHERE
                                        addresstypeid = 'BILLTO'
                                )
                            ) AS tad
                            ON st.salestransactionseq = tad.salestransactionseq
                            AND st.tenantid = tad.tenantid
                    ) AS ADDR
                WHERE
                    ST.SALESORDERSEQ = CRD.SALESORDERSEQ
                    AND ST.SALESTRANSACTIONSEQ = CRD.SALESTRANSACTIONSEQ
                    AND PRD.PERIODSEQ = CRD.PERIODSEQ
                    AND ST.SALESTRANSACTIONSEQ = TA.SALESTRANSACTIONSEQ
                    AND crd.credittypeseq = crdt.datatypeseq
                    AND crdt.removedate = :v_eot
                    AND crdt.credittypeid LIKE 'SCICOM%'
                    AND ST.SALESORDERSEQ = TA.SALESORDERSEQ
                    AND TLT.RULEELEMENTOWNERSEQ = POS.TITLESEQ
                    AND TA.POSITIONNAME = POS.NAME
                    AND ST.SALESTRANSACTIONSEQ = ADDR.SALESTRANSACTIONSEQ
                    AND ST.SALESORDERSEQ = ADDR.SALESORDERSEQ
                    AND PRD.REMOVEDATE >= :v_eot
                    AND CRD.PERIODSEQ = :IN_PERIODSEQ
                    AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                    AND TLT.NAME LIKE '%SCICOM%'
                    AND CSF.EFFECTIVESTARTDATE < PRD.ENDDATE
                    AND CSF.EFFECTIVEENDDATE >= PRD.ENDDATE
                    AND POS.REMOVEDATE >= :v_eot
                    AND POS.EFFECTIVESTARTDATE < PRD.ENDDATE
                    AND POS.EFFECTIVEENDDATE >= PRD.ENDDATE
                    AND TLT.REMOVEDATE >= :v_eot
                    AND TLT.EFFECTIVESTARTDATE < PRD.ENDDATE
                    AND TLT.EFFECTIVEENDDATE >= PRD.ENDDATE
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END