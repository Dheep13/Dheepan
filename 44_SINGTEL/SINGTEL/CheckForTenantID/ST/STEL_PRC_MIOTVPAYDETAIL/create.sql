CREATE PROCEDURE EXT.STEL_PRC_MIOTVPAYDETAIL
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

    v_ComponentName = 'EXT.STEL_PRC_MIOTVPAYDETAIL';

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

    -- Deleting  table data 
    /* ORIGSQL: DELETE STEL_RPT_MMP_MIOTVPAYDET WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNI(...) */
    DELETE
    FROM
        EXT.STEL_RPT_MMP_MIOTVPAYDET
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_MMP_MIOTVPAYDET') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_MMP_MIOTVPAYDET');

    -- managing table partitions

    -- Below block for  SER Vendors
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_STAGESALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_MIOTVPAYDET (TENANTID, PERIODSEQ, PERIODNAME, PROCESSIN(...) */
    INSERT INTO EXT.STEL_RPT_MMP_MIOTVPAYDET
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
                STR.GENERICDATE1,
                STR.PRODUCTID,
                STR.GENERICATTRIBUTE5,
                STR.GENERICATTRIBUTE3,
                STR.GENERICATTRIBUTE4,
                NULL AS SPEAR_DEALERCODE,
                STR.GENERICATTRIBUTE10,
                STR.COMMENTS,
                STR.GENERICNUMBER2,
                CRD.VALUE,
                STR.GENERICATTRIBUTE22,
                STR.COMMENTS,
                NULL AS REC_TYPE,
                STR.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
                STR.ACCOUNTINGDATE AS ACCOUNTTINGDATE,
                :v_end,
                TO_VARCHAR(EXTRACT(MONTH FROM :v_end)  /* ORIGSQL: TO_CHAR(EXTRACT (MONTH FROM v_End) - EXTRACT (MONTH FROM STR.ACCOUNTINGDATE)) */
                    - EXTRACT(MONTH FROM STR.ACCOUNTINGDATE)),
                CLS.DESCRIPTION,
                1,
                CRD.GENERICATTRIBUTE2 AS EVENTTYPE,
                CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
            FROM
                CS_SALESTRANSACTION STR,
                CS_SALESORDER ORD,
                CS_STAGESALESTRANSACTION STG,
                CS_CREDIT CRD,
                CS_PROCESSINGUNIT PRC,
                CS_CREDITTYPE CTYPE,
                EXT.STEL_CLASSIFIER CLS
            WHERE
                ORD.SALESORDERSEQ = STR.SALESORDERSEQ
                AND STR.SALESORDERSEQ = STG.SALESORDERSEQ
                AND STR.SALESTRANSACTIONSEQ = STG.SALESTRANSACTIONSEQ
                AND ORD.ORDERID = STG.ORDERID
                AND STR.SALESORDERSEQ = CRD.SALESORDERSEQ
                AND STR.SALESTRANSACTIONSEQ = CRD.SALESTRANSACTIONSEQ
                AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
                AND CTYPE.REMOVEDATE = :v_eot
                AND CTYPE.DATATYPESEQ = CRD.CREDITTYPESEQ
                AND CTYPE.CREDITTYPEID IN ('MMP', 'MMP TV Content', 'SHCC/FTA','SER_TOG') -- Added new credittypeid SER_TOG on 19.02 after discussion with Arun
                AND CRD.GENERICATTRIBUTE2 IN ('TV Closed', 'TV Submitted')
                -- Below block added on 29.11.2017 per Arjun's mail
                AND CRD.GENERICATTRIBUTE6 = 'SER'
                AND IFNULL(CRD.GENERICBOOLEAN4,0) = 0 --[Arun/Avi - To filter out the Dealer credits and show only Vendor credits =1] 16.01.2019 / Changed to 0 on 29.08.2019
                /* ORIGSQL: NVL(CRD.GENERICBOOLEAN4,0) */
                AND CRD.NAME IN
                ('DC_MMP_Products_Commission',
                    'DC_MMP_mioTV Content',
                'DC_SHCC /FTA')
                AND CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
                AND CLS.CATEGORYTREENAME = 'Singtel'
                AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                AND CRD.PERIODSEQ = :IN_PERIODSEQ
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Below block for Non SER Vendors       
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_MIOTVPAYDET (TENANTID, PERIODSEQ, PERIODNAME, PROCESSIN(...) */
    INSERT INTO EXT.STEL_RPT_MMP_MIOTVPAYDET
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
            STR.GENERICDATE1,
            STR.PRODUCTID,
            STR.GENERICATTRIBUTE5,
            STR.GENERICATTRIBUTE3,
            STR.GENERICATTRIBUTE4,
            NULL AS SPEAR_DEALERCODE,
            STR.GENERICATTRIBUTE10,
            STR.COMMENTS,
            STR.GENERICNUMBER2,
            /* -- Case statement added on 06.12.2017 */
                CASE
                    WHEN CRD.NAME = 'ICR_MMP_mioTV Content'
                    AND CRD.GENERICATTRIBUTE6 = 'DH - External'
                    THEN 0
                    WHEN STR.GENERICATTRIBUTE4 IN ('D2581','D2566','D2590')
                    THEN 0 /* -- Added to remove value for 'D2581' , 'D2566' , 'D2590' As per mail from Joanna and converstaion with Arun. 03.01.2019 */
                    ELSE CRD.VALUE
                END,
                STR.GENERICATTRIBUTE22,
                STR.COMMENTS,
                NULL AS REC_TYPE,
                STR.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
                STR.ACCOUNTINGDATE AS ACCOUNTTINGDATE,
                :v_end,
                TO_VARCHAR(EXTRACT(MONTH FROM :v_end)  /* ORIGSQL: TO_CHAR(EXTRACT (MONTH FROM v_End) - EXTRACT (MONTH FROM STR.ACCOUNTINGDATE)) */
                    - EXTRACT(MONTH FROM STR.ACCOUNTINGDATE)),
                CLS.DESCRIPTION,
                2,
                CRD.GENERICATTRIBUTE2 AS EVENTTYPE,
                CURRENT_TIMESTAMP AS CREATEDATE  /* ORIGSQL: SYSDATE */
            FROM
                CS_SALESTRANSACTION STR,
                CS_SALESORDER ORD,
                CS_STAGESALESTRANSACTION STG,
                CS_CREDIT CRD,
                CS_PROCESSINGUNIT PRC,
                CS_CREDITTYPE CTYPE,
                EXT.STEL_CLASSIFIER CLS
            WHERE
                ORD.SALESORDERSEQ = STR.SALESORDERSEQ
                AND STR.SALESORDERSEQ = STG.SALESORDERSEQ
                AND STR.SALESTRANSACTIONSEQ = STG.SALESTRANSACTIONSEQ
                AND ORD.ORDERID = STG.ORDERID
                AND STR.SALESORDERSEQ = CRD.SALESORDERSEQ
                AND STR.SALESTRANSACTIONSEQ = CRD.SALESTRANSACTIONSEQ
                AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
                AND CTYPE.REMOVEDATE = :v_eot
                AND CTYPE.DATATYPESEQ = CRD.CREDITTYPESEQ
                AND CTYPE.CREDITTYPEID IN ('MMP', 'MMP TV Content', 'SHCC/FTA', 'SER_TOG') -- Added new credittype SER_TOG
                AND CRD.GENERICATTRIBUTE2 IN ('TV Closed', 'TV Submitted')
                -- Below block added on 29.11.2017 per Arjun's mail
                AND CRD.GENERICATTRIBUTE6 IN
                ('TEPL', 'DS - External', 'DH - External')
                -- AND CRD.GENERICATTRIBUTE6 <> 'SER'
                AND IFNULL(CRD.GENERICBOOLEAN4,0) = 1 --[Arun/Avi - To filter out the Dealer credits and show only Vendor credits =1] 16.01.2019
                /* ORIGSQL: NVL(CRD.GENERICBOOLEAN4,0) */
                AND CRD.NAME IN
                ('ICR_MMP_Products_Commission',
                    'ICR_MMP_mioTV Content',
                'DC_SHCC /FTA')
                AND CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
                AND CLS.CATEGORYTREENAME = 'Singtel'
                AND CRD.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                AND CRD.PERIODSEQ = :IN_PERIODSEQ
        ;

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

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_MMP_MIOTVPAYDET (TENANTID, PERIODSEQ, PERIODNAME, PROCESSIN(...) */
    INSERT INTO EXT.STEL_RPT_MMP_MIOTVPAYDET
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
                    TO_VARCHAR(EXTRACT(MONTH FROM :v_end)  /* ORIGSQL: TO_CHAR(EXTRACT (MONTH FROM v_End) - EXTRACT (MONTH FROM STR.ACCOUNTINGDATE)) */
                        - EXTRACT(MONTH FROM STR.ACCOUNTINGDATE)),
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
                                            AND (et.eventtypeid LIKE 'TV Submitted%'
                                            OR et.eventtypeid LIKE 'TV Closed%')
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
                                            ,(86400*-1))) -- AND p.genericattribute11 IS NOT NULL - -- Commented on 16.01.2019 After discussion with Arun to resolve issue raised by Joanna.
                                    /* ORIGSQL: p.effectiveenddate - 1 */
                                WHERE
                                    e.eventtypeid = 'TV Submitted'
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
                                AND st.compensationdate BETWEEN p.effectivestartdate AND TO_DATE(ADD_SECONDS(P.EFFECTIVEENDDATE
                                        ,(86400*-1))) -- AND p.genericattribute11 IS NOT NULL - -- Commented on 16.01.2019 After discussion with Arun to resolve issue raised by Joanna.
                                /* ORIGSQL: P.EFFECTIVEENDDATE - 1 */
                            WHERE
                                et.eventtypeid LIKE 'TV Closed%'
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
                                        ,(86400*-1))) -- AND p.genericattribute11 IS NOT NULL - -- Commented on 16.01.2019 After discussion with Arun to resolve issue raised by Joanna.
                                /* ORIGSQL: p.effectiveenddate - 1 */
                            WHERE
                                et.eventtypeid LIKE 'TV Closed%'
                                AND st.numberofunits < 0
                                AND st.compensationdate BETWEEN :v_start AND TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))   /* ORIGSQL: v_End - 1 */
                        ) AS dbmtk_corrname_23855
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
                    AND CLS.CLASSIFIERID = STR.GENERICATTRIBUTE32
                    AND CLS.CATEGORYTREENAME = 'Singtel'
                    AND str.processingunitseq = prc.processingunitseq -- AND STR.compensationdate BETWEEN v_Start AND v_End - 1
                    AND (STR.COMPENSATIONDATE >= ADD_MONTHS(:v_start, -8)
                        AND STR.compensationdate <= TO_DATE(ADD_SECONDS(:v_end,(86400*-1)))) --AND  STR.compensationdate <= v_End - 1 -- Original code.
                    /* ORIGSQL: v_End - 1 */
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --- Update for Mcode and Mgrname as per discussion with Arjun. 09.11.2017
    -- Updating vendor   

    /* ORIGSQL: UPDATE STEL_RPT_MMP_MIOTVPAYDET T2 SET (T2.VENDORCODE, T2.VENDORNAME) = (SELECT (...) */
    UPDATE EXT.STEL_RPT_MMP_MIOTVPAYDET T2
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_MMP_MIOTVPAYDET' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
        SET
        /* ORIGSQL: (T2.VENDORCODE, T2.VENDORNAME) = */
        (VENDORCODE,
            VENDORNAME
            ) = (
                SELECT   /* ORIGSQL: (SELECT DISTINCT MPOS.NAME MGCODE, PAR.LASTNAME FROM cs_participant par, cs_paye(...) */
                    DISTINCT
                    MPOS.NAME AS MGCODE,
                    PAR.LASTNAME
                FROM
                    cs_participant par,
                    cs_payee p,
                    cs_period prd,
                    cs_position pos,
                    cs_position Mpos,
                    EXT.STEL_RPT_MMP_MIOTVPAYDET T1
                WHERE
                    p.payeeseq = par.payeeseq
                    AND par.payeeseq = Mpos.payeeseq
                    AND p.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                    AND par.removedate =
                    to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                    AND pos.removedate =
                    to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                    AND prd.removedate =
                    to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                    AND MPOS.removedate =
                    to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                    AND MPOS.effectivestartdate <= prd.enddate
                    AND MPOS.effectiveenddate > prd.enddate
                    AND p.effectivestartdate <= prd.enddate
                    AND p.effectiveenddate > prd.enddate
                    AND par.effectivestartdate <= prd.enddate
                    AND par.effectiveenddate > prd.enddate
                    AND pos.effectivestartdate <= prd.enddate
                    AND pos.effectiveenddate > prd.enddate
                    AND T1.DEALERCODE = POS.NAME
                    AND PRD.periodseq = :IN_PERIODSEQ
                    AND T1.REC_TYPE = 'EXCEPTION'
                    AND T1.SALESTRANSACTIONSEQ = T2.SALESTRANSACTIONSEQ
                    AND POS.MANAGERSEQ = MPOS.RULEELEMENTOWNERSEQ
            )
        WHERE
            T2.REC_TYPE = 'EXCEPTION'
            AND T2.periodseq = :IN_PERIODSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO STEL_RPT_MMP_MIOTVPAYDET tgt USING (SELECT DISTINCT Mpos.NAME AS MGCO(...) */
    MERGE INTO EXT.STEL_RPT_MMP_MIOTVPAYDET AS tgt    
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITIONRELATION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITIONRELATIONTYPE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (SELECT DISTINCT Mpos.NAME MGCODE, Mpos.NAME MGRNAME, T1.DEALERCODE, T1.SALESTRA(...) */
                DISTINCT
                Mpos.NAME AS MGCODE,
                Mpos.NAME AS MGRNAME,
                T1.DEALERCODE,
                T1.SALESTRANSACTIONSEQ
            FROM
                cs_participant par,
                cs_payee p,
                cs_period prd,
                cs_position pos,
                CS_POSITIONRELATION PS,
                cs_position Mpos,
                cs_positionrelationtype p1,
                EXT.STEL_RPT_MMP_MIOTVPAYDET T1
            WHERE
                p.payeeseq = par.payeeseq
                AND par.payeeseq = pos.payeeseq
                AND p.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND par.removedate =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND pos.removedate =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND prd.removedate =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PS.removedate =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND p1.removedate =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PS.effectivestartdate <= prd.enddate
                AND PS.effectiveenddate > prd.enddate
                AND MPOS.removedate =
                to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND MPOS.effectivestartdate <= prd.enddate
                AND MPOS.effectiveenddate > prd.enddate
                AND p.effectivestartdate <= prd.enddate
                AND p.effectiveenddate > prd.enddate
                AND par.effectivestartdate <= prd.enddate
                AND par.effectiveenddate > prd.enddate
                AND pos.effectivestartdate <= prd.enddate
                AND pos.effectiveenddate > prd.enddate
                AND PS.CHILDPOSITIONSEQ = POS.RULEELEMENTOWNERSEQ
                AND PS.PARENTPOSITIONSEQ = MPOS.RULEELEMENTOWNERSEQ
                AND P1.DATATYPESEQ = PS.POSITIONRELATIONTYPESEQ
                AND T1.DEALERCODE = POS.NAME
                AND T1.REC_TYPE = 'EXCEPTION'
                AND P1.NAME = 'MMP Vendor Dealer'
                AND PRD.periodseq = :IN_PERIODSEQ
        ) AS DLT
        ON (DLT.DEALERCODE = tgt.DEALERCODE
        AND DLT.SALESTRANSACTIONSEQ = TGT.SALESTRANSACTIONSEQ AND tgt.periodseq = :IN_PERIODSEQ AND tgt.processingunitseq = :IN_PROCESSINGUNITSEQ)
    WHEN MATCHED THEN
        UPDATE SET
            TGT.VENDORCODE = DLT.MGCODE, TGT.VENDORNAME = DLT.MGRNAME;
        --WHERE /*Sanjay commented out*/
         --   tgt.periodseq = :IN_PERIODSEQ
         --   AND tgt.processingunitseq = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    ----------Block added by Arun[27 Aug 2019] 

    /* ORIGSQL: DELETE FROM STEL_RPT_MMP_MIOTVPAYDET WHERE SALESTRANSACTIONSEQ IN (SELECT SALEST(...) */
    DELETE
    FROM
        EXT.STEL_RPT_MMP_MIOTVPAYDET
    WHERE
        SALESTRANSACTIONSEQ  
        IN
        (
            SELECT   /* ORIGSQL: (SELECT SALESTRANSACTIONSEQ FROM CS_SALESTRANSACTION WHERE GENERICATTRIBUTE32 LI(...) */
                SALESTRANSACTIONSEQ
            FROM
                CS_SALESTRANSACTION
            WHERE
                GENERICATTRIBUTE32 LIKE 'CCO%'
        );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    ----------Block added by Arun[27 Aug 2019]

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END