CREATE PROCEDURE EXT.STEL_PRC_EPTRANADMIN
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
    DECLARE v_StartDate TIMESTAMP;  /* ORIGSQL: v_StartDate date; */
    DECLARE v_enddate TIMESTAMP;  /* ORIGSQL: v_enddate date; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_Processingunitname VARCHAR(100) = NULL;  /* ORIGSQL: v_Processingunitname VARCHAR2(100) := NULL; */
    DECLARE V_credited VARCHAR(100) = 'Credited';  /* ORIGSQL: V_credited VARCHAR2(100) := 'Credited'; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE:= TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    DECLARE v_isquarter BIGINT = 0;  /* ORIGSQL: v_isquarter INTEGER := 0; */

    v_ComponentName = 'STEL_PRC_EPTRANADMIN';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME,
        PERIODNAME,
        startdate,
        enddate
    INTO
        v_CalendarName,
        v_PeriodName,
        v_StartDate,
        v_enddate
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_Processingunitname
    FROM
        cs_processingunit
    WHERE
        PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_EPTRANADMIN WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSEQ(...) */
    DELETE
    FROM
        EXT.STEL_RPT_EPTRANADMIN
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_EPTRANADMIN') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_EPTRANADMIN');

    -- managing table partitions

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_EPTRANADMIN' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_EPTRANADMIN (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIOD(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    INSERT INTO EXT.STEL_RPT_EPTRANADMIN
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            TRANSID,
            VENDORCODE,
            VENDORNAME,
            DEALERCODE,
            DEALERNAME,
            REASON,
            PRODUCT,
            TRANSDATE,
            DATASOURCE,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: SELECT distinct STR.TENANTID, in_periodseq PERIODSEQ, in_PROCESSINGUNITSEQ, v_Pe(...) */
            DISTINCT
            STR.TENANTID,
            :IN_PERIODSEQ AS PERIODSEQ,
            :IN_PROCESSINGUNITSEQ,
            :v_PeriodName AS PERIODNAME,
            :v_Processingunitname AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            ORD.ORDERID AS TRANSID,
            STR.GENERICATTRIBUTE3 AS VENDORCODE,
            venpart.lastname AS VENDORNAME,/* -- Mapping needed 17.01.2017 */  STR.GENERICATTRIBUTE4 AS DEALERCODE,
            retpart.lastname AS DEALERNAME,/* -- Mapping needed 17.01.2017 */
            CASE
                WHEN c.creditseq IS NOT NULL
                THEN :V_credited
                ELSE 'Vendor Code and Dealer Code not in Eligiblity List'
            END
            AS REASON,/* -- Mapping needed 17.01.2017 */  STR.PRODUCTNAME AS PRODUCT,
            STR.ACCOUNTINGDATE AS TRANSDATE,
            STR.DATASOURCE AS DATASOURCE,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
            /* --, str.salestransactionseq */
        FROM
            cs_salestransaction str
        INNER JOIN
            cs_salesorder ord
            ON STR.SALESORDERSEQ = ORD.SALESORDERSEQ
            AND ord.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND str.tenantid = ord.tenantid
            AND str.processingunitseq = ord.processingunitseq
        LEFT OUTER JOIN
            cs_Credit c
            ON STR.SALESTRANSACTIONSEQ = c.SALESTRANSACTIONSEQ
            AND str.tenantid = c.tenantid
            AND str.processingunitseq = c.processingunitseq
            -- and c.name='DCR_External Prepaid_Incentive for Retailers' -- Added below creditype after discussion with Arjun on 14.03.2018
            AND credittypeseq IN
            (
                SELECT   /* ORIGSQL: (select datatypeseq from cs_credittype ctype where CREDITTYPEID = 'Retailer SIM (...) */
                    datatypeseq
                FROM
                    cs_credittype ctype
                WHERE
                    CREDITTYPEID = 'Retailer SIM Incentive'
                    AND REMOVEDATE > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            )
        INNER JOIN
            cs_eventtype et
            ON et.datatypeseq = str.eventtypeseq
            AND et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        INNER JOIN
            cs_payee ret
            ON ret.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ret.payeeid = str.genericattribute4
            AND str.compensationdate BETWEEN ret.effectivestartdate AND TO_DATE(ADD_SECONDS(ret.effectiveenddate,(86400*-1)))   /* ORIGSQL: ret.effectiveenddate-1 */
        INNER JOIN
            cs_payee ven
            ON ven.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ven.payeeid = str.genericattribute3
            AND str.compensationdate BETWEEN ven.effectivestartdate AND TO_DATE(ADD_SECONDS(ven.effectiveenddate,(86400*-1)))   /* ORIGSQL: ven.effectiveenddate-1 */
        INNER JOIN
            cs_participant retpart
            ON retpart.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ret.payeeseq = retpart.payeeseq
            AND str.compensationdate BETWEEN retpart.effectivestartdate AND TO_DATE(ADD_SECONDS(retpart.effectiveenddate,(86400*-1)))   /* ORIGSQL: retpart.effectiveenddate-1 */
        INNER JOIN
            cs_participant venpart
            ON venpart.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ven.payeeseq = venpart.payeeseq
            AND str.compensationdate BETWEEN venpart.effectivestartdate AND TO_DATE(ADD_SECONDS(venpart.effectiveenddate,(86400*-1)))   /* ORIGSQL: venpart.effectiveenddate-1 */
        WHERE
            str.datasource = 'RMT0081'
            AND str.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND str.tenantid = :v_Tenant
            AND str.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_enddate,(86400*-1)));  /* ORIGSQL: v_enddate-1 */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_EPTRANADMIN tgt using (SELECT genericattribute1, genericattr(...) */
    MERGE INTO EXT.STEL_RPT_EPTRANADMIN AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select genericattribute1, genericattribute2 from stel_classifier where category(...) */
                genericattribute1,
                genericattribute2
            FROM
                ext.stel_classifier
            WHERE
                categorytreename = 'ExternalPrepaid_InEligibility'
                AND categoryname = 'Retailer Incentive'
                AND genericattribute4 = 'Retailer Incentive'
                /*
                (select genericattribute1, genericattribute2 from stel_classifier
                    where categorytreename = 'ExternalPrepaid_Eligibility'
                    and categoryname='Retailer Incentive'
                    and genericattribute3='TRUE'
                    and genericattribute4='Retailer Incentive'
                    and genericattribute2<>'*'
                    
                    union
                    
                    select c.genericattribute1, ch.name from stel_classifier c
                    join cs_position p
                    on p.name = c.genericattribute1 and p.removedate>sysdate
                    and p.effectiveenddate>sysdate
                    left join cs_position ch
                    on ch.managerseq=p.ruleelementownerseq
                    and ch.removedate>sysdate
                    and ch.effectiveenddate>sysdate
                    where categorytreename = 'ExternalPrepaid_Eligibility'
                    and categoryname='Retailer Incentive'
                    and c.genericattribute3='TRUE'
                    and c.genericattribute4='Retailer Incentive'
                    and c.genericattribute2='*'
                )*/
        ) AS src
        ON (tgt.vendorcode = src.genericattribute1
            AND tgt.dealercode = src.genericattribute2
        	AND tgt.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND tgt.tenantid = :v_Tenant
            AND tgt.periodseq = :IN_PERIODSEQ
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.reason = 'Dealer Code in Ineligible list'
        --WHERE
         --   tgt.processingunitseq = :IN_PROCESSINGUNITSEQ
         --   AND tgt.tenantid = :v_Tenant
        --    AND tgt.periodseq = :IN_PERIODSEQ
        ;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_EPTRANADMIN tgt using (SELECT p.name FROM cs_incentive i INN(...) */
    MERGE INTO EXT.STEL_RPT_EPTRANADMIN AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select p.name from cs_incentive i join cs_position p on p.ruleelementownerseq=i(...) */
                p.name
            FROM
                cs_incentive i
            INNER JOIN
                cs_position p
                ON p.ruleelementownerseq = i.positionseq
                AND p.removedate = :v_eot
            INNER JOIN
                cs_period pd
                ON pd.removedate = :v_eot
                AND pd.periodseq = i.periodseq
                AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
                                                                                                                                                       /* ORIGSQL: p.effectiveenddate-1 */
            WHERE
                i.name = 'I_External Prepaid_Retailer SIM Incentive_Payout'
                AND i.periodseq = :IN_PERIODSEQ
                AND i.processingunitseq = :IN_PROCESSINGUNITSEQ
                AND i.tenantid = :v_Tenant
                AND i.value = 0
        ) AS src
        ON (Src.name = tgt.dealercode
        	AND tgt.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND tgt.tenantid = :v_Tenant
            AND tgt.periodseq = :IN_PERIODSEQ
            AND tgt.reason = :V_credited
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.reason = 'Minimum Incentive Threshold not met. No Incentive paid'
     --   WHERE
        --    tgt.processingunitseq = :IN_PROCESSINGUNITSEQ
        --    AND tgt.tenantid = :v_Tenant
        --    AND tgt.periodseq = :IN_PERIODSEQ
        --    AND tgt.reason = :V_credited
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: INSERT INTO STEL_RPT_EPTRANADMIN (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIOD(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONASSIGNMENT' not found */
    INSERT INTO EXT.STEL_RPT_EPTRANADMIN
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            TRANSID,
            VENDORCODE,
            VENDORNAME,
            DEALERCODE,
            DEALERNAME,
            REASON,
            PRODUCT,
            TRANSDATE,
            DATASOURCE,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: SELECT distinct STR.TENANTID, in_periodseq PERIODSEQ, in_PROCESSINGUNITSEQ, v_Pe(...) */
            DISTINCT
            STR.TENANTID,
            :IN_PERIODSEQ AS PERIODSEQ,
            :IN_PROCESSINGUNITSEQ,
            :v_PeriodName AS PERIODNAME,
            :v_Processingunitname AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            ORD.ORDERID AS TRANSID,
            asg.positionname AS VENDORCODE,
            venpart.lastname AS VENDORNAME,/* -- Mapping needed 17.01.2017 */  STR.GENERICATTRIBUTE4 AS DEALERCODE,
            retpart.lastname AS DEALERNAME,/* -- Mapping needed 17.01.2017 */
            CASE
                WHEN c.creditseq IS NOT NULL
                THEN :V_credited
                ELSE 'Not Credited'
            END
            AS REASON,/* -- Mapping needed 17.01.2017 */  STR.PRODUCTNAME AS PRODUCT,
            CASE
                WHEN STR.DATASOURCE = 'EDW'
                THEN STR.COMPENSATIONDATE
                ELSE STR.ACCOUNTINGDATE
            END
            AS TRANSDATE,
            STR.DATASOURCE AS DATASOURCE,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
            /* --, str.salestransactionseq */
        FROM
            cs_salestransaction str
        INNER JOIN
            cs_salesorder ord
            ON STR.SALESORDERSEQ = ORD.SALESORDERSEQ
            AND ord.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND str.tenantid = ord.tenantid
            AND str.processingunitseq = ord.processingunitseq
        LEFT OUTER JOIN
            cs_Credit c
            ON STR.SALESTRANSACTIONSEQ = c.SALESTRANSACTIONSEQ
            AND str.tenantid = c.tenantid
            AND str.processingunitseq = c.processingunitseq
            AND c.name = 'DC_External Prepaid_Incentive for Distributors'
            AND c.compensationdate = str.compensationdate

            --and str.datasource <> 'EDW' -- Added as per email from User Joanna
        INNER JOIN
            cs_eventtype et
            ON et.datatypeseq = str.eventtypeseq
            AND et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        LEFT OUTER JOIN
            cs_payee ret
            ON ret.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ret.payeeid = str.genericattribute4
            AND str.compensationdate BETWEEN ret.effectivestartdate AND TO_DATE(ADD_SECONDS(ret.effectiveenddate,(86400*-1)))   /* ORIGSQL: ret.effectiveenddate-1 */
        INNER JOIN
            cs_transactionassignment Asg
            ON Asg.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND Asg.tenantid = :v_Tenant
            AND Asg.salestransactionseq = str.salestransactionseq
            AND Asg.compensationdate = str.compensationdate
        INNER JOIN
            cs_payee ven
            ON ven.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ven.payeeid = Asg.positionname
            AND str.compensationdate BETWEEN ven.effectivestartdate AND TO_DATE(ADD_SECONDS(ven.effectiveenddate,(86400*-1))) -- Added below block on 21.03.2018 per mail from Arjun
            /* ORIGSQL: ven.effectiveenddate-1 */
            AND asg.positionname IN
            (
                SELECT   /* ORIGSQL: (select dim0 from stel_lookup WHERE name LIKE 'LT_ExternalPrepaid_EligibleVendor(...) */
                    dim0
                FROM
                    ext.stel_lookup
                WHERE
                    name LIKE 'LT_ExternalPrepaid_EligibleVendors'
                    AND dim1 = 'Retailer Incentive'
            )
        LEFT OUTER JOIN
            cs_participant retpart
            ON retpart.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ret.payeeseq = retpart.payeeseq
            AND str.compensationdate BETWEEN retpart.effectivestartdate AND TO_DATE(ADD_SECONDS(retpart.effectiveenddate,(86400*-1)))   /* ORIGSQL: retpart.effectiveenddate-1 */
        LEFT OUTER JOIN
            cs_participant venpart
            ON venpart.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ven.payeeseq = venpart.payeeseq
            AND str.compensationdate BETWEEN venpart.effectivestartdate AND TO_DATE(ADD_SECONDS(venpart.effectiveenddate,(86400*-1)))   /* ORIGSQL: venpart.effectiveenddate-1 */
        WHERE
            1 = 1
            AND str.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND str.tenantid = :v_Tenant
            AND str.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_enddate,(86400*-1)))   /* ORIGSQL: v_enddate-1 */
            AND UPPER(str.datasource) IN ('EDW','SAP','EMTU','CONCERTO')
            AND et.eventtypeid IN ('Electronic Top Up','Phoenix Card','Prepaid Card','Top Up Revenue','Prepaid SIM Count','BBOM SIM Count');

    /* ORIGSQL: commit; */
    COMMIT;

    -- Block for CMM
    -- Checking for quarterly periodsq
    BEGIN 
        DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            /* ORIGSQL: WHEN NO_DATA_FOUND THEN */
            BEGIN
                v_isquarter = 0;
            END;


        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODTYPE' not found */

        SELECT
            periodseq
        INTO
            v_isquarter
        FROM
            cs_period p1,
            cs_periodtype p2
        WHERE
            P1.ENDDATE   
            IN
            (
                SELECT   /* ORIGSQL: (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_periodtype p2 WHERE p1.removed(...) */
                    DISTINCT
                    p1.enddate
                FROM
                    cs_period p1,
                    cs_periodtype p2
                WHERE
                    p1.removedate = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                    AND P2.REMOVEDATE =
                    to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                    AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
                    AND P2.NAME = 'quarter'
            )
            AND p1.removedate = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
            AND P2.REMOVEDATE = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
            AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
            AND P2.NAME = 'month'
            AND p1.periodseq = :IN_PERIODSEQ;

        /* ORIGSQL: EXCEPTION WHEN NO_DATA_FOUND THEN */
    END;

    IF :v_isquarter > 0
    THEN
        -- Date calculation for 3 months period 
        SELECT
            MIN(startdate),
            MAX(enddate)
        INTO
            v_StartDate,
            v_enddate
        FROM
            cs_periodcalendar
        WHERE
            PERIODSEQ     
            IN
            (
                SELECT   /* ORIGSQL: (SELECT p3.periodseq FROM cs_period p3 WHERE p3.parentseq IN (SELECT DISTINCT pa(...) */
                    p3.periodseq
                FROM
                    cs_period p3
                WHERE
                    p3.parentseq   
                    IN
                    (
                        SELECT   /* ORIGSQL: (SELECT DISTINCT parentseq FROM cs_period p1, cs_periodtype p2 WHERE P1.ENDDATE (...) */
                            DISTINCT
                            parentseq
                        FROM
                            cs_period p1,
                            cs_periodtype p2
                        WHERE
                            P1.ENDDATE   
                            IN
                            (
                                SELECT   /* ORIGSQL: (SELECT DISTINCT p1.enddate FROM cs_period p1, cs_periodtype p2 WHERE p1.removed(...) */
                                    DISTINCT
                                    p1.enddate
                                FROM
                                    cs_period p1,
                                    cs_periodtype p2
                                WHERE
                                    p1.removedate =
                                    to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                    AND P2.REMOVEDATE =
                                    to_date('01/01/2200',  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                                    'dd/mm/yyyy')
                                    AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
                                    AND P2.NAME = 'quarter'
                            )
                            AND p1.removedate =
                            to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                            AND P2.REMOVEDATE =
                            to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
                            AND P1.PERIODTYPESEQ = P2.PERIODTYPESEQ
                            AND P2.NAME = 'month'
                            AND p1.periodseq = :IN_PERIODSEQ
                    )
                    AND p3.removedate = to_date('01/01/2200', 'dd/mm/yyyy')  /* ORIGSQL: TO_DATE('01/01/2200', 'dd/mm/yyyy') */
            );

        /* ORIGSQL: INSERT INTO STEL_RPT_EPTRANADMIN (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIOD(...) */
        INSERT INTO EXT.STEL_RPT_EPTRANADMIN
            (
                TENANTID,
                PERIODSEQ,
                PROCESSINGUNITSEQ,
                PERIODNAME,
                PROCESSINGUNITNAME,
                CALENDARNAME,
                TRANSID,
                VENDORCODE,
                VENDORNAME,
                DEALERCODE,
                DEALERNAME,
                REASON,
                PRODUCT,
                TRANSDATE,
                DATASOURCE,
                CREATEDATE
            )
            SELECT   /* ORIGSQL: SELECT distinct STR.TENANTID, in_periodseq PERIODSEQ, in_PROCESSINGUNITSEQ, v_Pe(...) */
                DISTINCT
                STR.TENANTID,
                :IN_PERIODSEQ AS PERIODSEQ,
                :IN_PROCESSINGUNITSEQ,
                :v_PeriodName AS PERIODNAME,
                :v_Processingunitname AS PROCESSINGUNITNAME,
                :v_CalendarName AS CALENDARNAME,
                ORD.ORDERID AS TRANSID,
                asg.positionname AS VENDORCODE,
                venpart.lastname AS VENDORNAME,/* -- Mapping needed 17.01.2017 */  STR.GENERICATTRIBUTE4 AS DEALERCODE,
                retpart.lastname AS DEALERNAME,/* -- Mapping needed 17.01.2017 */   'Credited' AS REASON,/* -- Mapping needed 17.01.2017 */  STR.PRODUCTNAME AS PRODUCT,
                /* --     STR.ACCOUNTINGDATE TRANSDATE, -- Added below case statement on 21.03.2018 AK */
                CASE
                    WHEN STR.DATASOURCE = 'EDW'
                    THEN STR.COMPENSATIONDATE
                    ELSE STR.ACCOUNTINGDATE
                END
                AS TRANSDATE,
                STR.DATASOURCE AS DATASOURCE,
                CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
                /* --, str.salestransactionseq */
            FROM
                cs_salestransaction str
            INNER JOIN
                cs_salesorder ord
                ON STR.SALESORDERSEQ = ORD.SALESORDERSEQ
                AND ord.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND str.tenantid = ord.tenantid
                AND str.processingunitseq = ord.processingunitseq
            INNER JOIN
                cs_Credit c
                ON STR.SALESTRANSACTIONSEQ = c.SALESTRANSACTIONSEQ
                AND str.tenantid = c.tenantid
                AND str.processingunitseq = c.processingunitseq
                AND c.name = 'DC_External Prepaid_CMM Incentive'
                AND str.datasource <> 'EDW' -- Added as per email from User Joanna
            INNER JOIN
                cs_eventtype et
                ON et.datatypeseq = str.eventtypeseq
                AND et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            LEFT OUTER JOIN
                cs_payee ret
                ON ret.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND ret.payeeid = str.genericattribute4
                AND str.compensationdate BETWEEN ret.effectivestartdate AND TO_DATE(ADD_SECONDS(ret.effectiveenddate,(86400*-1))) /*    join cs_position pos
                   on pos.removedate>sysdate and POS.PAYEESEQ = RET.PAYEESEQ
                 and str.compensationdate between pos.effectivestartdate and pos.effectiveenddate-1
                 and pos.name in (select dim0 from stel_lookup
                             WHERE name LIKE 'LT_ExternalPrepaid_EligibleVendors'
                 AND dim1='Retailer Incentive')*/-- Added below block on 21.03.2018 based on Arjun's mail.
                -- Block Ended.
                /* ORIGSQL: ret.effectiveenddate-1 */
            INNER JOIN
                cs_transactionassignment Asg
                ON Asg.processingunitseq = :IN_PROCESSINGUNITSEQ
                AND Asg.tenantid = :v_Tenant
                AND Asg.salestransactionseq = str.salestransactionseq
                AND Asg.compensationdate = str.compensationdate
            INNER JOIN
                cs_payee ven
                ON ven.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND ven.payeeid = Asg.positionname
                AND str.compensationdate BETWEEN ven.effectivestartdate AND TO_DATE(ADD_SECONDS(ven.effectiveenddate,(86400*-1)))   /* ORIGSQL: ven.effectiveenddate-1 */
            LEFT OUTER JOIN
                cs_participant retpart
                ON retpart.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND ret.payeeseq = retpart.payeeseq
                AND str.compensationdate BETWEEN retpart.effectivestartdate AND TO_DATE(ADD_SECONDS(retpart.effectiveenddate,(86400*-1)))   /* ORIGSQL: retpart.effectiveenddate-1 */
            LEFT OUTER JOIN
                cs_participant venpart
                ON venpart.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND ven.payeeseq = venpart.payeeseq
                AND str.compensationdate BETWEEN venpart.effectivestartdate AND TO_DATE(ADD_SECONDS(venpart.effectiveenddate,(86400*-1)))   /* ORIGSQL: venpart.effectiveenddate-1 */
            WHERE
                1 = 1
                AND str.processingunitseq = :IN_PROCESSINGUNITSEQ
                AND str.tenantid = :v_Tenant
                AND str.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_enddate,(86400*-1)));  /* ORIGSQL: v_enddate-1 */

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END