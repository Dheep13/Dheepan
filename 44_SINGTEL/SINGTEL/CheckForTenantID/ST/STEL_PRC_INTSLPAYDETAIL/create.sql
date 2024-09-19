CREATE PROCEDURE EXT.STEL_PRC_INTSLPAYDETAIL
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

    DECLARE v_start TIMESTAMP;  /* ORIGSQL: v_start DATE; */
    DECLARE v_end TIMESTAMP;  /* ORIGSQL: v_end DATE; */

    v_ComponentName = 'EXT.STEL_PRC_INTSLPAYDETAIL';

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

    /* ORIGSQL: execute immediate 'truncate table stel_classifier_Tab'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_CLASSIFIER_TAB' not found */

    /* ORIGSQL: truncate table stel_classifier_Tab ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.stel_classifier_Tab';

    /* ORIGSQL: insert into stel_classifier_Tab select * from stel_classifier; */
    INSERT INTO ext.stel_classifier_Tab
        SELECT   /* ORIGSQL: select * from stel_classifier; */
            *
        FROM
            ext.stel_classifier;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Calling below procedure to populate dealer and vendor code data.

    /* ORIGSQL: STEL_PRC_MMPINTPARTPOS (IN_PERIODSEQ, IN_PROCESSINGUNITSEQ) */
    CALL EXT.STEL_PRC_MMPINTPARTPOS(:IN_PERIODSEQ, :IN_PROCESSINGUNITSEQ);

    -- Deleting  table data 
    /* ORIGSQL: DELETE STEL_RPT_MMP_INTSPAYDET WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNIT(...) */
    DELETE
    FROM
        EXT.STEL_RPT_MMP_INTSPAYDET
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_MMP_INTSPAYDET') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_MMP_INTSPAYDET');

    -- managing table partitions

    -- Below block for  section 1
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_MMP_INTSPAYDET' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_INTSPAYDET (TENANTID, PERIODSEQ, PERIODNAME, PROCESSING(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONADDRESS' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_ADDRESSTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PMCREDITTRACE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    INSERT INTO EXT.STEL_RPT_MMP_INTSPAYDET
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            ORDERNO,
            SERVICENO,
            CUSTOMERNAME,
            CRD,
            ORDERCREATIONDATE,
            ORDERCLOSEDATE,
            CUSTREQUESTDATE,
            COMPID,
            TYPE,
            VENDORCODE,
            DEALERCODE,
            SPEAR_DEALERCODE,
            ORDERTYPE,
            REMARKS,
            PRICE,
            AI,
            STATUS,
            COMMENTS,
            REC_TYPE,
            SALESTRANSACTIONSEQ,
            ACCOUNTTINGDATE,
            ENDDATE,
            MONTHDIFF,
            PRODUCTDESC,
            SECTIONNO,
            EVENTTYPE,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: SELECT v_Tenant TENANTID, IN_PERIODSEQ PERIODSEQ, v_PeriodName PERIODNAME, IN_PR(...) */
            :v_Tenant AS TENANTID,
            :IN_PERIODSEQ AS PERIODSEQ,
            :v_PeriodName AS PERIODNAME,
            :IN_PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
            PU.NAME AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            ORD.ORDERID,
            TAD.CONTACT,
            TAD.ADDRESS1,
            STR.GENERICDATE1,
            IFNULL(to_date(TO_VARCHAR(STR.DISCOUNTPERCENT,NULL), 'yyyy/mm/dd'), str.accountingdate) /* --accdate for TV */,  /* ORIGSQL: nvl(TO_DATE (TO_CHAR (STR.DISCOUNTPERCENT), 'yyyy/mm/dd'),str.accountingdate) */
                                                                                                                                                                   /* ORIGSQL: TO_DATE(TO_CHAR (STR.DISCOUNTPERCENT), 'yyyy/mm/dd') */
                                                                                                                                                                   /* ORIGSQL: TO_CHAR(STR.DISCOUNTPERCENT) */
            STR.COMPENSATIONDATE,
            STR.GENERICDATE1,
            STR.PRODUCTID,
            STR.GENERICATTRIBUTE5,
            STR.GENERICATTRIBUTE3,
            STR.GENERICATTRIBUTE4,
            NULL AS SPEAR_DEALERCODE,
            STR.GENERICATTRIBUTE10,
            STR.COMMENTS,
            STR.GENERICNUMBER2,
            /* --  CRD.VALUE, defect # 124  Per Arjun's mail on 09-03-2018 */
            CASE
                WHEN IFNULL(str.genericboolean1, 0) = 0  /* ORIGSQL: nvl(str.genericboolean1,0) */
                THEN IFNULL(STR.NUMBEROFUNITS, 1)  /* ORIGSQL: nvl(STR.NUMBEROFUNITS,1) */
                ELSE IFNULL(STR.NUMBEROFUNITS, 0)  /* ORIGSQL: nvl(STR.NUMBEROFUNITS,0) */
            END
            ,
            STR.GENERICATTRIBUTE22,
            STR.COMMENTS,
            NULL AS REC_TYPE,
            STR.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
            STR.ACCOUNTINGDATE AS ACCOUNTTINGDATE,
            :v_end,
            TO_VARCHAR(EXTRACT(MONTH FROM :v_end)  /* ORIGSQL: TO_CHAR(EXTRACT (MONTH FROM v_End) - EXTRACT (MONTH FROM STR.ACCOUNTINGDATE)) */
                - EXTRACT(MONTH FROM STR.ACCOUNTINGDATE)),
            IFNULL(CLS.DESCRIPTION, crd.genericattribute16),  /* ORIGSQL: nvl(CLS.DESCRIPTION, crd.genericattribute16) */
            1,
            CRD.GENERICATTRIBUTE2 AS EVENTTYPE,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            cs_salestransaction str
        INNER JOIN
            cS_transactionaddress tad
            ON str.salestransactionseq = tad.salestransactionseq
        INNER JOIN
            cs_addresstype a
            ON a.addresstypeid = 'BILLTO'
            AND a.addresstypeseq = tad.addresstypeseq
        INNER JOIN
            cs_Credit crd
            ON crd.salestransactionseq = str.salestransactionseq
            AND crd.processingunitseq = str.processingunitseq
            AND str.compensationdate = crd.compensationdate
        INNER JOIN
            cs_salesorder ord
            ON ord.salesorderseq = crd.salesorderseq
            AND ord.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ord.processingunitseq = crd.processingunitseq
        INNER JOIN
            cs_pmcredittrace pmt
            ON pmt.creditseq = crd.creditseq
            AND pmt.sourceperiodseq = crd.periodseq
            AND pmt.processingunitseq = crd.processingunitseq
            AND pmt.tenantid = crd.tenantid
        INNER JOIN
            cs_measurement m
            ON m.measurementseq = pmt.measurementseq
            AND m.name LIKE 'PM_MMP_DH Telesales North Star Integrated Team_Total Line CT'
            AND pmt.targetperiodseq = m.periodseq
            AND pmt.processingunitseq = m.processingunitseq
            AND pmt.tenantid = m.tenantid
        INNER JOIN
            cs_processingunit pu
            ON pu.processingunitseq = :IN_PROCESSINGUNITSEQ
        INNER JOIN
            CS_CREDITTYPE CTYPE
            ON CTYPE.REMOVEDATE = :v_eot
            AND CTYPE.DATATYPESEQ = CRD.CREDITTYPESEQ
            AND CTYPE.CREDITTYPEID IN
            ('North Star Credit', 'North Star Credit Rolled')
        LEFT OUTER JOIN
            ext.stel_classifier_Tab CLS
            ON CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
            AND str.compensationdate BETWEEN cls.effectivestartdate AND TO_DATE(ADD_SECONDS(cls.effectiveenddate,(86400*-1)))   /* ORIGSQL: cls.effectiveenddate-1 */
            AND CLS.CATEGORYTREENAME = 'Singtel'
        WHERE
            CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            AND CRD.PERIODSEQ = :IN_PERIODSEQ
            AND crd.tenantid = 'STEL'
            AND str.tenantid = 'STEL'
            AND ord.tenantid = 'STEL';

    /*
       FROM CS_SALESTRANSACTION STR,
      CS_SALESORDER ORD,
      --CS_STAGESALESTRANSACTION STG,
      CS_CREDIT CRD,
      CS_PROCESSINGUNIT PRC,
      CS_CREDITTYPE CTYPE,
      STEL_CLASSIFIER CLS
      --,
     -- STEL_RPT_DATA_FTTHREQMEMO S1
      WHERE   STR.SALESORDERSEQ = CRD.SALESORDERSEQ
     AND STR.SALESTRANSACTIONSEQ = CRD.SALESTRANSACTIONSEQ
     AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
     AND CTYPE.REMOVEDATE = V_EOT
     AND CTYPE.DATATYPESEQ = CRD.CREDITTYPESEQ
    
     and ord.removedate = v_eot
     -- AND S1.PAYEESEQ = CRD.PAYEESEQ
    -- AND S1.POSITIONSEQ = CRD.POSITIONSEQ
    -- AND S1.PERIODSEQ = CRD.PERIODSEQ
     AND CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
     and str.compensationdate between cls.effectivestartdate and cls.effectiveenddate-1
     AND CLS.CATEGORYTREENAME = 'Singtel'
     AND CRD.PROCESSINGUNITSEQ = IN_PROCESSINGUNITSEQ
 AND CRD.PERIODSEQ = IN_PERIODSEQ);
*/

/* ORIGSQL: COMMIT; */
COMMIT;

    SELECT
        CALENDARNAME,
        PERIODNAME,
        STARTDATE,
        ENDDATE
    INTO
        v_CalendarName,
        v_PeriodName,
        v_start,
        v_end
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    -- Below code added based on Arjun's mail about MMP changes after walkthrough 08.05.2016

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_STAGESALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_INTSPAYDET (TENANTID, PERIODSEQ, PERIODNAME, PROCESSING(...) */
    INSERT INTO EXT.STEL_RPT_MMP_INTSPAYDET
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            ORDERNO,
            SERVICENO,
            CUSTOMERNAME,
            CRD,
            ORDERCREATIONDATE,
            ORDERCLOSEDATE,
            CUSTREQUESTDATE,
            COMPID,
            TYPE,
            VENDORCODE,
            DEALERCODE,
            SPEAR_DEALERCODE,
            ORDERTYPE,
            REMARKS,
            PRICE,
            AI,
            STATUS,
            COMMENTS,
            REC_TYPE,
            SALESTRANSACTIONSEQ,
            ACCOUNTTINGDATE,
            ENDDATE,
            MONTHDIFF,
            PRODUCTDESC,
            SECTIONNO,
            EVENTTYPE,
            CREATEDATE
        )
        
            SELECT   /* ORIGSQL: (SELECT v_Tenant TENANTID, IN_PERIODSEQ PERIODSEQ, v_PeriodName PERIODNAME, IN_P(...) */
                :v_Tenant AS TENANTID,
                :IN_PERIODSEQ AS PERIODSEQ,
                :v_PeriodName AS PERIODNAME,
                :IN_PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
                PRC.NAME AS PROCESSINGUNITNAME,
                :v_CalendarName AS CALENDARNAME,
                ORD.ORDERID,
                STG.BILLTOCONTACT,
                STG.BILLTOADDRESS1,
                STR.GENERICDATE1,
                to_date(TO_VARCHAR(STG.DISCOUNTPERCENT), 'yyyy/mm/dd'),  /* ORIGSQL: TO_DATE(TO_CHAR (STG.DISCOUNTPERCENT), 'yyyy/mm/dd') */
                                                                                                               /* ORIGSQL: TO_CHAR(STG.DISCOUNTPERCENT) */
                STR.COMPENSATIONDATE,
                /* --As per discussion with Arjun on 04.2017 */
                /* --              CASE */
                    /* --                 WHEN st1.eventtypeid LIKE 'TV Submitted%' THEN NULL */
                    /* --                 ELSE STR.COMPENSATIONDATE */
                    /* --              END, */
                    STR.GENERICDATE1,
                    STR.PRODUCTID,
                    STR.GENERICATTRIBUTE5,
                    STR.GENERICATTRIBUTE3,
                    STR.GENERICATTRIBUTE4,
                    NULL AS SPEAR_DEALERCODE,
                    STR.GENERICATTRIBUTE10,
                    STR.COMMENTS,
                    STR.GENERICNUMBER2,
                    0,
                    STR.GENERICATTRIBUTE22,
                    STR.COMMENTS,
                    ST1.STATUS,
                    STR.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
                    STR.ACCOUNTINGDATE AS ACCOUNTTINGDATE,
                    :v_end,
                    /* --              TO_CHAR(EXTRACT (MONTH FROM v_End) */
                        /* --                      - EXTRACT (MONTH FROM STR.ACCOUNTINGDATE)), */
                    TO_VARCHAR(STR.ACCOUNTINGDATE,NULL),  /* ORIGSQL: TO_CHAR(STR.ACCOUNTINGDATE) */
                    CLS.DESCRIPTION,
                    3,
                    ST1.eventtypeid AS EVENTTYPE,
                    CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
                FROM
                    CS_SALESTRANSACTION STR,
                    CS_SALESORDER ORD,
                    CS_STAGESALESTRANSACTION STG,
                    CS_PROCESSINGUNIT PRC,
                    EXT.STEL_CLASSIFIER CLS,
                    -- Temp table created with Arjun's given code
                    (
                        SELECT   /* ORIGSQL: (SELECT salestransactionseq, STATUS, eventtypeid FROM (SELECT s.salestransaction(...) */
                            salestransactionseq,
                            STATUS,
                            eventtypeid
                        FROM
                            (
                                SELECT   /* ORIGSQL: (SELECT s.salestransactionseq, CASE WHEN ADD_MONTHS(s.compensationdate, 6) < v_e(...) */
                                    s.salestransactionseq,
                                    CASE
                                        WHEN ADD_MONTHS(s.compensationdate, 6) <
                                        :v_end
                                        THEN 'EXPIRED'
                                        ELSE 'NOT CLOSED'
                                    END
                                    AS Status,
                                    e.eventtypeid
                                FROM
                                    cs_salestransaction S
                                INNER JOIN
                                    cs_eventtype e
                                    ON S.eventtypeseq = e.datatypeseq
                                    AND e.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                INNER JOIN
                                    (
                                        SELECT   /* ORIGSQL: (SELECT COUNT(DISTINCT eventtypeid), st.alternateordernumber, st.genericattribut(...) */
                                            COUNT(DISTINCT eventtypeid),
                                            st.alternateordernumber,
                                            st.genericattribute6,
                                            MAX(et.eventtypeid),
                                            MAX(St.compensationdate) AS compdate
                                        FROM
                                            cs_Salestransaction st
                                        INNER JOIN
                                            cs_eventtype et
                                            ON st.eventtypeseq =
                                            et.datatypeseq
                                            AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                        WHERE
                                            st.datasource = 'BCC'
                                            AND (et.eventtypeid LIKE '% Submitted%'
                                            OR et.eventtypeid LIKE '% Closed%')
                                        GROUP BY
                                            st.alternateordernumber,
                                            st.genericattribute6
                                        HAVING
                                            COUNT(DISTINCT eventtypeid) = 1
                                    ) AS x
                                    ON x.genericattribute6 = s.genericattribute6
                                    AND x.alternateordernumber =
                                    s.alternateordernumber
                                INNER JOIN
                                    cs_position p
                                    ON p.NAME = s.genericattribute3
                                    AND p.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                    AND s.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate
                                            ,(86400*-1)))   /* ORIGSQL: p.effectiveenddate - 1 */
                                    AND p.genericattribute11 IS NOT NULL
                                WHERE
                                    e.eventtypeid LIKE '% Submitted'
                                    AND (S.COMPENSATIONDATE >=
                                        ADD_MONTHS(:v_start, -8)
                                        AND s.compensationdate <= TO_DATE(ADD_SECONDS(:v_end,(86400*-1))))  /* ORIGSQL: v_End - 1 */
                        UNION
                            SELECT   /* ORIGSQL: SELECT st.salestransactionseq, 'CEASED BEFORE CUTOFF' status, et.eventtypeid eve(...) */
                                st.salestransactionseq,
                                'CEASED BEFORE CUTOFF' AS status,
                                et.eventtypeid AS eventtypeid
                            FROM
                                cs_Salestransaction st
                            INNER JOIN
                                cs_eventtype et
                                ON st.eventtypeseq = et.datatypeseq
                                AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                            INNER JOIN
                                cs_position p
                                ON p.NAME = st.genericattribute3
                                AND p.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                AND st.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate
                                        ,(86400*-1)))   /* ORIGSQL: p.effectiveenddate - 1 */
                                AND p.genericattribute11 IS NOT NULL
                            WHERE
                                et.eventtypeid LIKE '% Closed%'
                                AND st.compensationdate BETWEEN :v_start AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                                AND st.genericboolean1 = 1
                        UNION
                            SELECT   /* ORIGSQL: SELECT st.salestransactionseq, 'CLAWBACK' status, et.eventtypeid FROM cs_Salestr(...) */
                                st.salestransactionseq,
                                'CLAWBACK' AS status,
                                et.eventtypeid
                            FROM
                                cs_Salestransaction st
                            INNER JOIN
                                cs_eventtype et
                                ON st.eventtypeseq = et.datatypeseq
                                AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                            INNER JOIN
                                cs_position p
                                ON p.NAME = st.genericattribute3
                                AND p.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                AND st.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate
                                        ,(86400*-1)))   /* ORIGSQL: p.effectiveenddate - 1 */
                                AND p.genericattribute11 IS NOT NULL
                            WHERE
                                et.eventtypeid LIKE '% Closed%'
                                AND st.numberofunits < 0
                                AND st.compensationdate BETWEEN :v_start AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                        ) AS dbmtk_corrname_23636
                    ) AS ST1
                WHERE
                    ORD.SALESORDERSEQ = STR.SALESORDERSEQ
                    AND STR.SALESORDERSEQ = STG.SALESORDERSEQ
                    AND STR.SALESTRANSACTIONSEQ = STG.SALESTRANSACTIONSEQ
                    AND ORD.ORDERID = STG.ORDERID
                    AND ORD.REMOVEDATE = :v_eot
                    AND STR.SALESTRANSACTIONSEQ = ST1.SALESTRANSACTIONSEQ
                    AND STR.GENERICATTRIBUTE5 <> 'Cease'
                    AND STR.GENERICATTRIBUTE31 <> 'P'
                    AND STR.genericattribute4 IN
                    (
                        SELECT   /* ORIGSQL: (select payeeid from cs_payee p join cs_participant par on p.payeeseq=par.payees(...) */
                            payeeid
                        FROM
                            cs_payee p
                        INNER JOIN
                            cs_participant par
                            ON p.payeeseq = par.payeeseq
                        WHERE
                            UPPER(par.lastname) LIKE '%NORTH%STAR%INTEGRATED%' 
                            AND p.removedate = :v_eot
                            AND par.removedate = :v_eot
                            AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1))) BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                                                                                                                                                               /* ORIGSQL: p.effectiveenddate-1 */
                            AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                                                                                                                                                                   /* ORIGSQL: par.effectiveenddate-1 */
                    )
                    AND CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
                    AND CLS.CATEGORYTREENAME = 'Singtel'
                    AND str.processingunitseq = prc.processingunitseq -- AND STR.compensationdate BETWEEN v_Start AND v_End - 1
                    AND (STR.COMPENSATIONDATE >= :v_start
                        AND STR.compensationdate <= TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))) --AND  STR.compensationdate <= v_End - 1 -- Original code.
                    /* ORIGSQL: v_End - 1 */
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --[Below block added by Arun to cater for Telecontinent Integrator Sales
    -- Below block for  section 1 
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_INTSPAYDET (TENANTID, PERIODSEQ, PERIODNAME, PROCESSING(...) */
    INSERT INTO EXT.STEL_RPT_MMP_INTSPAYDET
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            ORDERNO,
            SERVICENO,
            CUSTOMERNAME,
            CRD,
            ORDERCREATIONDATE,
            ORDERCLOSEDATE,
            CUSTREQUESTDATE,
            COMPID,
            TYPE,
            VENDORCODE,
            DEALERCODE,
            SPEAR_DEALERCODE,
            ORDERTYPE,
            REMARKS,
            PRICE,
            AI,
            STATUS,
            COMMENTS,
            REC_TYPE,
            SALESTRANSACTIONSEQ,
            ACCOUNTTINGDATE,
            ENDDATE,
            MONTHDIFF,
            PRODUCTDESC,
            SECTIONNO,
            EVENTTYPE,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: SELECT v_Tenant TENANTID, IN_PERIODSEQ PERIODSEQ, v_PeriodName PERIODNAME, IN_PR(...) */
            :v_Tenant AS TENANTID,
            :IN_PERIODSEQ AS PERIODSEQ,
            :v_PeriodName AS PERIODNAME,
            :IN_PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
            PU.NAME AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            ORD.ORDERID,
            TAD.CONTACT,
            TAD.ADDRESS1,
            STR.GENERICDATE1,
            IFNULL(to_date(TO_VARCHAR(STR.DISCOUNTPERCENT,NULL), 'yyyy/mm/dd'), str.accountingdate) /* --accdate for TV */,  /* ORIGSQL: nvl(TO_DATE (TO_CHAR (STR.DISCOUNTPERCENT), 'yyyy/mm/dd'),str.accountingdate) */
                                                                                                                                                                   /* ORIGSQL: TO_DATE(TO_CHAR (STR.DISCOUNTPERCENT), 'yyyy/mm/dd') */
                                                                                                                                                                   /* ORIGSQL: TO_CHAR(STR.DISCOUNTPERCENT) */
            STR.COMPENSATIONDATE,
            STR.GENERICDATE1,
            STR.PRODUCTID,
            STR.GENERICATTRIBUTE5,
            STR.GENERICATTRIBUTE3,
            STR.GENERICATTRIBUTE4,
            NULL AS SPEAR_DEALERCODE,
            STR.GENERICATTRIBUTE10,
            STR.COMMENTS,
            STR.GENERICNUMBER2,
            /* --  CRD.VALUE, defect # 124  Per Arjun's mail on 09-03-2018 */
            CASE
                WHEN IFNULL(str.genericboolean1, 0) = 0  /* ORIGSQL: nvl(str.genericboolean1,0) */
                THEN IFNULL(STR.NUMBEROFUNITS, 1)  /* ORIGSQL: nvl(STR.NUMBEROFUNITS,1) */
                ELSE IFNULL(STR.NUMBEROFUNITS, 0)  /* ORIGSQL: nvl(STR.NUMBEROFUNITS,0) */
            END
            ,
            STR.GENERICATTRIBUTE22,
            STR.COMMENTS,
            NULL AS REC_TYPE,
            STR.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
            STR.ACCOUNTINGDATE AS ACCOUNTTINGDATE,
            :v_end,
            TO_VARCHAR(EXTRACT(MONTH FROM :v_end)  /* ORIGSQL: TO_CHAR(EXTRACT (MONTH FROM v_End) - EXTRACT (MONTH FROM STR.ACCOUNTINGDATE)) */
                - EXTRACT(MONTH FROM STR.ACCOUNTINGDATE)),
            IFNULL(CLS.DESCRIPTION, crd.genericattribute16),  /* ORIGSQL: nvl(CLS.DESCRIPTION, crd.genericattribute16) */
            1,
            CRD.GENERICATTRIBUTE2 AS EVENTTYPE,
            CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
        FROM
            cs_salestransaction str
        INNER JOIN
            cS_transactionaddress tad
            ON str.salestransactionseq = tad.salestransactionseq
        INNER JOIN
            cs_addresstype a
            ON a.addresstypeid = 'BILLTO'
            AND a.addresstypeseq = tad.addresstypeseq
        INNER JOIN
            cs_Credit crd
            ON crd.salestransactionseq = str.salestransactionseq
            AND crd.processingunitseq = str.processingunitseq
            AND str.compensationdate = crd.compensationdate
        INNER JOIN
            cs_salesorder ord
            ON ord.salesorderseq = crd.salesorderseq
            AND ord.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND ord.processingunitseq = crd.processingunitseq
        INNER JOIN
            cs_pmcredittrace pmt
            ON pmt.creditseq = crd.creditseq
            AND pmt.sourceperiodseq = crd.periodseq
            AND pmt.processingunitseq = crd.processingunitseq
            AND pmt.tenantid = crd.tenantid
        INNER JOIN
            cs_measurement m
            ON m.measurementseq = pmt.measurementseq
            AND m.name LIKE 'PM_MMP_DH Telesales Telecontinent Integrated Team_Total Line CT'
            AND pmt.targetperiodseq = m.periodseq
            AND pmt.processingunitseq = m.processingunitseq
            AND pmt.tenantid = m.tenantid
        INNER JOIN
            cs_processingunit pu
            ON pu.processingunitseq = :IN_PROCESSINGUNITSEQ
        INNER JOIN
            CS_CREDITTYPE CTYPE
            ON CTYPE.REMOVEDATE = :v_eot
            AND CTYPE.DATATYPESEQ = CRD.CREDITTYPESEQ
            AND CTYPE.CREDITTYPEID IN
            ('Telecontinent Credit', 'Telecontinent Credit Rolled')
        LEFT OUTER JOIN
            ext.stel_classifier_Tab CLS
            ON CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
            AND str.compensationdate BETWEEN cls.effectivestartdate AND TO_DATE(ADD_SECONDS(cls.effectiveenddate,(86400*-1)))   /* ORIGSQL: cls.effectiveenddate-1 */
            AND CLS.CATEGORYTREENAME = 'Singtel'
        WHERE
            CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            AND CRD.PERIODSEQ = :IN_PERIODSEQ
            AND crd.tenantid = 'STEL'
            AND str.tenantid = 'STEL'
            AND ord.tenantid = 'STEL';

    /*
       FROM CS_SALESTRANSACTION STR,
      CS_SALESORDER ORD,
      --CS_STAGESALESTRANSACTION STG,
      CS_CREDIT CRD,
      CS_PROCESSINGUNIT PRC,
      CS_CREDITTYPE CTYPE,
      STEL_CLASSIFIER CLS
      --,
     -- STEL_RPT_DATA_FTTHREQMEMO S1
      WHERE   STR.SALESORDERSEQ = CRD.SALESORDERSEQ
     AND STR.SALESTRANSACTIONSEQ = CRD.SALESTRANSACTIONSEQ
     AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
     AND CTYPE.REMOVEDATE = V_EOT
     AND CTYPE.DATATYPESEQ = CRD.CREDITTYPESEQ
    
     and ord.removedate = v_eot
     -- AND S1.PAYEESEQ = CRD.PAYEESEQ
    -- AND S1.POSITIONSEQ = CRD.POSITIONSEQ
    -- AND S1.PERIODSEQ = CRD.PERIODSEQ
     AND CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
     and str.compensationdate between cls.effectivestartdate and cls.effectiveenddate-1
     AND CLS.CATEGORYTREENAME = 'Singtel'
     AND CRD.PROCESSINGUNITSEQ = IN_PROCESSINGUNITSEQ
 AND CRD.PERIODSEQ = IN_PERIODSEQ);
*/

/* ORIGSQL: COMMIT; */
COMMIT;

    SELECT
        CALENDARNAME,
        PERIODNAME,
        STARTDATE,
        ENDDATE
    INTO
        v_CalendarName,
        v_PeriodName,
        v_start,
        v_end
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    -- Below code added based on Arjun's mail about MMP changes after walkthrough 08.05.2016            

    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_INTSPAYDET (TENANTID, PERIODSEQ, PERIODNAME, PROCESSING(...) */
    INSERT INTO EXT.STEL_RPT_MMP_INTSPAYDET
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            ORDERNO,
            SERVICENO,
            CUSTOMERNAME,
            CRD,
            ORDERCREATIONDATE,
            ORDERCLOSEDATE,
            CUSTREQUESTDATE,
            COMPID,
            TYPE,
            VENDORCODE,
            DEALERCODE,
            SPEAR_DEALERCODE,
            ORDERTYPE,
            REMARKS,
            PRICE,
            AI,
            STATUS,
            COMMENTS,
            REC_TYPE,
            SALESTRANSACTIONSEQ,
            ACCOUNTTINGDATE,
            ENDDATE,
            MONTHDIFF,
            PRODUCTDESC,
            SECTIONNO,
            EVENTTYPE,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenant TENANTID, IN_PERIODSEQ PERIODSEQ, v_PeriodName PERIODNAME, IN_P(...) */
            :v_Tenant AS TENANTID,
            :IN_PERIODSEQ AS PERIODSEQ,
            :v_PeriodName AS PERIODNAME,
            :IN_PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
            PRC.NAME AS PROCESSINGUNITNAME,
            :v_CalendarName AS CALENDARNAME,
            ORD.ORDERID,
            STG.BILLTOCONTACT,
            STG.BILLTOADDRESS1,
            STR.GENERICDATE1,
            to_date(TO_VARCHAR(STG.DISCOUNTPERCENT), 'yyyy/mm/dd'),  /* ORIGSQL: TO_DATE(TO_CHAR (STG.DISCOUNTPERCENT), 'yyyy/mm/dd') */
                                                                                                           /* ORIGSQL: TO_CHAR(STG.DISCOUNTPERCENT) */
            STR.COMPENSATIONDATE,
            /* --As per discussion with Arjun on 04.2017 */
            /* --              CASE */
                /* --                 WHEN st1.eventtypeid LIKE 'TV Submitted%' THEN NULL */
                /* --                 ELSE STR.COMPENSATIONDATE */
                /* --              END, */
                STR.GENERICDATE1,
                STR.PRODUCTID,
                STR.GENERICATTRIBUTE5,
                STR.GENERICATTRIBUTE3,
                STR.GENERICATTRIBUTE4,
                NULL AS SPEAR_DEALERCODE,
                STR.GENERICATTRIBUTE10,
                STR.COMMENTS,
                STR.GENERICNUMBER2,
                0,
                STR.GENERICATTRIBUTE22,
                STR.COMMENTS,
                ST1.STATUS,
                STR.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
                STR.ACCOUNTINGDATE AS ACCOUNTTINGDATE,
                :v_end,
                /* --              TO_CHAR(EXTRACT (MONTH FROM v_End) */
                    /* --                      - EXTRACT (MONTH FROM STR.ACCOUNTINGDATE)), */
                TO_VARCHAR(STR.ACCOUNTINGDATE,NULL),  /* ORIGSQL: TO_CHAR(STR.ACCOUNTINGDATE) */
                CLS.DESCRIPTION,
                3,
                ST1.eventtypeid AS EVENTTYPE,
                CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
            FROM
                CS_SALESTRANSACTION STR,
                CS_SALESORDER ORD,
                CS_STAGESALESTRANSACTION STG,
                CS_PROCESSINGUNIT PRC,
                EXT.STEL_CLASSIFIER CLS,
                -- Temp table created with Arjun's given code
                (
                    SELECT   /* ORIGSQL: (SELECT salestransactionseq, STATUS, eventtypeid FROM (SELECT s.salestransaction(...) */
                        salestransactionseq,
                        STATUS,
                        eventtypeid
                    FROM
                        (
                            SELECT   /* ORIGSQL: (SELECT s.salestransactionseq, CASE WHEN ADD_MONTHS(s.compensationdate, 6) < v_e(...) */
                                s.salestransactionseq,
                                CASE
                                    WHEN ADD_MONTHS(s.compensationdate, 6) <
                                    :v_end
                                    THEN 'EXPIRED'
                                    ELSE 'NOT CLOSED'
                                END
                                AS Status,
                                e.eventtypeid
                            FROM
                                cs_salestransaction S
                            INNER JOIN
                                cs_eventtype e
                                ON S.eventtypeseq = e.datatypeseq
                                AND e.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                            INNER JOIN
                                (
                                    SELECT   /* ORIGSQL: (SELECT COUNT(DISTINCT eventtypeid), st.alternateordernumber, st.genericattribut(...) */
                                        COUNT(DISTINCT eventtypeid),
                                        st.alternateordernumber,
                                        st.genericattribute6,
                                        MAX(et.eventtypeid),
                                        MAX(St.compensationdate) AS compdate
                                    FROM
                                        cs_Salestransaction st
                                    INNER JOIN
                                        cs_eventtype et
                                        ON st.eventtypeseq =
                                        et.datatypeseq
                                        AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                    WHERE
                                        st.datasource = 'BCC'
                                        AND (et.eventtypeid LIKE '% Submitted%'
                                        OR et.eventtypeid LIKE '% Closed%')
                                    GROUP BY
                                        st.alternateordernumber,
                                        st.genericattribute6
                                    HAVING
                                        COUNT(DISTINCT eventtypeid) = 1
                                ) AS x
                                ON x.genericattribute6 = s.genericattribute6
                                AND x.alternateordernumber =
                                s.alternateordernumber
                            INNER JOIN
                                cs_position p
                                ON p.NAME = s.genericattribute3
                                AND p.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                AND s.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate
                                        ,(86400*-1)))   /* ORIGSQL: p.effectiveenddate - 1 */
                                AND p.genericattribute11 IS NOT NULL
                            WHERE
                                e.eventtypeid LIKE '% Submitted'
                                AND (S.COMPENSATIONDATE >=
                                    ADD_MONTHS(:v_start, -8)
                                    AND s.compensationdate <= TO_DATE(ADD_SECONDS(:v_end,(86400*-1))))  /* ORIGSQL: v_End - 1 */
                        UNION
                            SELECT   /* ORIGSQL: SELECT st.salestransactionseq, 'CEASED BEFORE CUTOFF' status, et.eventtypeid eve(...) */
                                st.salestransactionseq,
                                'CEASED BEFORE CUTOFF' AS status,
                                et.eventtypeid AS eventtypeid
                            FROM
                                cs_Salestransaction st
                            INNER JOIN
                                cs_eventtype et
                                ON st.eventtypeseq = et.datatypeseq
                                AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                            INNER JOIN
                                cs_position p
                                ON p.NAME = st.genericattribute3
                                AND p.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                AND st.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate
                                        ,(86400*-1)))   /* ORIGSQL: p.effectiveenddate - 1 */
                                AND p.genericattribute11 IS NOT NULL
                            WHERE
                                et.eventtypeid LIKE '% Closed%'
                                AND st.compensationdate BETWEEN :v_start AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                                AND st.genericboolean1 = 1
                        UNION
                            SELECT   /* ORIGSQL: SELECT st.salestransactionseq, 'CLAWBACK' status, et.eventtypeid FROM cs_Salestr(...) */
                                st.salestransactionseq,
                                'CLAWBACK' AS status,
                                et.eventtypeid
                            FROM
                                cs_Salestransaction st
                            INNER JOIN
                                cs_eventtype et
                                ON st.eventtypeseq = et.datatypeseq
                                AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                            INNER JOIN
                                cs_position p
                                ON p.NAME = st.genericattribute3
                                AND p.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                                AND st.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate
                                        ,(86400*-1)))   /* ORIGSQL: p.effectiveenddate - 1 */
                                AND p.genericattribute11 IS NOT NULL
                            WHERE
                                et.eventtypeid LIKE '% Closed%'
                                AND st.numberofunits < 0
                                AND st.compensationdate BETWEEN :v_start AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                        ) AS dbmtk_corrname_23641
                    ) AS ST1
                WHERE
                    ORD.SALESORDERSEQ = STR.SALESORDERSEQ
                    AND STR.SALESORDERSEQ = STG.SALESORDERSEQ
                    AND STR.SALESTRANSACTIONSEQ = STG.SALESTRANSACTIONSEQ
                    AND ORD.ORDERID = STG.ORDERID
                    AND ORD.REMOVEDATE = :v_eot
                    AND STR.SALESTRANSACTIONSEQ = ST1.SALESTRANSACTIONSEQ
                    AND STR.GENERICATTRIBUTE5 <> 'Cease'
                    AND STR.GENERICATTRIBUTE31 <> 'P'
                    AND STR.genericattribute4 IN
                    (
                        SELECT   /* ORIGSQL: (select payeeid from cs_payee p join cs_participant par on p.payeeseq=par.payees(...) */
                            payeeid
                        FROM
                            cs_payee p
                        INNER JOIN
                            cs_participant par
                            ON p.payeeseq = par.payeeseq
                        WHERE
                            UPPER(par.lastname) LIKE '%TC%INTEGRATED%' 
                            AND p.removedate = :v_eot
                            AND par.removedate = :v_eot
                            AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1))) BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(p.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                                                                                                                                                               /* ORIGSQL: p.effectiveenddate-1 */
                            AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                                                                                                                                                                   /* ORIGSQL: par.effectiveenddate-1 */
                    )
                    AND CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
                    AND CLS.CATEGORYTREENAME = 'Singtel'
                    AND str.processingunitseq = prc.processingunitseq -- AND STR.compensationdate BETWEEN v_Start AND v_End - 1
                    AND (STR.COMPENSATIONDATE >= :v_start
                        AND STR.compensationdate <= TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))) --AND  STR.compensationdate <= v_End - 1 -- Original code.
                    /* ORIGSQL: v_End - 1 */
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --[Arun - End of Block]

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END