CREATE PROCEDURE EXT.RPT_POST_DSDETAIL
(
    IN p_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_rpttype VARCHAR2 */
    IN p_periodseq DECIMAL(38,10),   /* ORIGSQL: p_periodseq NUMBER */
    IN p_processingunitseq DECIMAL(38,10)   /* ORIGSQL: p_processingunitseq NUMBER */
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

    v_ComponentName = 'RPT_POST_dSDETAIL';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || p_periodseq |(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:p_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:p_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- After discussion with Balaji and Arun, It has been decided to create individual procedure
    --  For this report. Commented out below code which was part of reporting framework.
    -- This development is done on 11.01.2019 by AK

    /*
     UPDATE STEL_RPT_DS_TRANSACTIONDET tgt
       SET calendarname = 'Singtel Monthly Calendar'
     WHERE tgt.periodseq = p_periodseq
     AND tgt.processingunitseq = p_processingunitseq;
    
    MERGE INTO STEL_RPT_DS_TRANSACTIONDET tgt
         USING (  SELECT MIN (compensationdate) closedate, alternateordernumber
                        FROM    vw_Salestransaction st
                             JOIN
                                cs_eventtype et
                             ON et.datatypeseq = st.eventtypeseq
         AND et.removedate > SYSDATE
                       WHERE et.eventtypeid LIKE '%Close%'
                GROUP BY alternateordernumber) src
            ON (src.alternateordernumber = tgt.alternateordernumber)
    WHEN MATCHED
    THEN
       UPDATE SET
          tgt.ordeRclose = src.closedate
               WHERE tgt.periodseq = p_periodseq
     AND tgt.processingunitseq = p_processingunitseq;
    
    
    
    MERGE INTO STEL_RPT_DS_TRANSACTIONDET tgt
         USING (  SELECT s.salestransactionseq,
                             SUM (Contributionvalue) contribvalue
                        FROM cs_pmcredittrace pc
                             JOIN cs_measurement m
                                ON pc.measurementseq = m.measurementseq
         AND m.periodseq = pc.targetperiodseq
                             JOIN cs_Credit c
                                ON pc.creditseq = c.creditseq
         AND c.periodseq = pc.targetperiodseq
                             JOIN cs_salestransaction s
                                ON s.salestransactionseq = c.salestransactionseq
         AND c.compensationdate = s.compensationdate
                       WHERE     m.name LIKE 'PM_INTERNAL_Points'
         AND m.VALUE <> 0
         AND s.tenantid = 'STEL'
         AND c.tenantid = 'STEL'
         AND pc.tenantid = 'STEL'
         AND m.tenantid = 'STEL'
         AND s.processingunitseq = p_processingunitseq
         AND m.processingunitseq = p_processingunitseq
         AND pc.processingunitseq = p_processingunitseq
         AND c.processingunitseq = p_processingunitseq
         AND pc.targetperiodseq = p_periodseq
                GROUP BY s.salestransactionseq) src
            ON (src.salestransactionseq = tgt.salestransactionseq)
    WHEN MATCHED
    THEN
       UPDATE SET tgt.points = src.contribvalue;
    
    COMMIT;
    */

    -- Deleting  table data 
    /* ORIGSQL: DELETE STEL_RPT_DSTRANADMIN WHERE PERIODSEQ = p_periodseq AND PROCESSINGUNITSEQ (...) */
    DELETE
    FROM
        EXT.STEL_RPT_DSTRANADMIN
    WHERE
        PERIODSEQ = :p_periodseq
        AND PROCESSINGUNITSEQ = :p_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (p_periodseq, 'STEL_RPT_DSTRANADMIN') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:p_periodseq, 'EXT.STEL_RPT_DSTRANADMIN');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONADDRESS' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_MONTHHIERARCHY_TBL' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_DSTRANADMIN (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIOD(...) */
    INSERT INTO EXT.STEL_RPT_DSTRANADMIN
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            ORDERTYPE,
            DATATYPE,
            APPTYPE,
            ORDERID,
            CUSTID,
            IPTVNO,
            ADSLNO,
            CREDITTYPEID,
            ORDERDATE,
            ORDERCLOSE,
            ORDERCANCEL,
            DEALERCODE,
            SALESMANCODE,
            PRODUCTNAME,
            SCHEMEID,
            COMPID,
            PACKAGEID,
            LISTPRICE,
            PREPRICE,/* -- Arun will confirm 11.01.2019 */
            INCVALUE,
            POINTS,
            ORDERSTATUS,
            COMMREMARK,
            COMMMONTH,
            CREATEDATE,
            SALESORDERSEQ,
            SALESTRANSACTIONSEQ,
            CREDITSEQ
        )
        
            SELECT   /* ORIGSQL: (SELECT CRD.TENANTID TENANTID, CRD.PERIODSEQ PERIODSEQ, CRD.PROCESSINGUNITSEQ PR(...) */
                CRD.TENANTID AS TENANTID,
                CRD.PERIODSEQ AS PERIODSEQ,
                CRD.PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
                PD.PERIODNAME AS PERIODNAME,
                'Singtel_PU' AS PROCESSINGUNITNAME,
                PD.CALENDARNAME AS CALENDARNAME,
                CRD.POSITIONSEQ AS POSITIONSEQ,
                CRD.PAYEESEQ AS PAYEESEQ,
                TRN.GENERICATTRIBUTE10 AS ORDERTYPE,
                ETYPE.EVENTTYPEID AS DATATYPE,
                TRN.GENERICATTRIBUTE5 AS APPTYPE,
                ORD.ORDERID AS ORDERID,
                BILLTO.CUSTID AS CUSTID,
                BILLTO.CONTACT AS IPTVNO,
                BILLTO.PHONE AS ADSLNO,
                CTYPE.CREDITTYPEID AS CREDITTYPEID,
                TRN.ACCOUNTINGDATE AS ORDERDATE,
                TRN.COMPENSATIONDATE AS ORDERCLOSE,
                TRN.GENERICDATE2 AS ORDERCANCEL,
                TRN.GENERICATTRIBUTE4 AS DEALERCODE,
                IFNULL(PAR.GENERICATTRIBUTE1, TRN.GENERICATTRIBUTE2) AS SALESMANCODE,/* -- AK Changed on 18.03.2019 after discussion with Dicson and Arun */
                CASE   /* ORIGSQL: NVL(PAR.GENERICATTRIBUTE1, TRN.GENERICATTRIBUTE2) */
                    WHEN ETYPE.EVENTTYPEID = 'TV Submitted'
                    THEN TRN.PRODUCTDESCRIPTION
                    WHEN ETYPE.EVENTTYPEID = 'BroadBand Submitted'
                    THEN TRN.GENERICATTRIBUTE31
                    WHEN ETYPE.EVENTTYPEID = 'Mobile Submitted'
                    THEN TRN.GENERICATTRIBUTE31
                END
                AS PRODUCTNAME,
                BILLTO.ADDRESS2 AS SCHEMEID,
                TRN.PRODUCTID AS COMPID,
                CASE
                    WHEN ETYPE.EVENTTYPEID = 'TV Submitted'
                    THEN TRN.PRODUCTNAME
                    WHEN ETYPE.EVENTTYPEID = 'BroadBand Submitted'
                    THEN TRN.GENERICATTRIBUTE28
                    WHEN ETYPE.EVENTTYPEID = 'Mobile Submitted'
                    THEN TRN.GENERICATTRIBUTE28
                END
                AS PACKAGEID,
                TRN.GENERICNUMBER2 AS LISTPRICE,
                NULL AS PREPRICE,/* -- Arun will confirm 11.01.2019 */  TRN.GENERICNUMBER3 AS INCVALUE,
                CRD.VALUE AS POINTS,
                CASE
                    WHEN TRN.GENERICBOOLEAN1 = 1
                    THEN 'Non-Payable'
                    ELSE 'Payable'
                END
                AS ORDERSTATUS,
                TRN.COMMENTS AS COMMREMARK,
                PD.MONTHNAME1 AS COMMMONTH,
                CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
                trn.SALESORDERSEQ AS SALESORDERSEQ,
                TRN.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
                CRD.CREDITSEQ
            FROM
                cs_salestransaction trn,
                cs_salesorder ord,
                cs_credit crd,
                CS_TRANSACTIONADDRESS billto,
                cs_eventtype etype,
                cs_credittype ctype,
                ext.stel_monthhierarchy_tbl pd,
                cs_position pos1,
                cs_participant par
            WHERE
                TRN.SALESORDERSEQ = ORD.SALESORDERSEQ
                AND TRN.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
                AND TRN.SALESORDERSEQ = CRD.SALESORDERSEQ
                AND BILLTO.SALESTRANSACTIONSEQ = TRN.SALESTRANSACTIONSEQ
                AND BILLTO.TRANSACTIONADDRESSSEQ = TRN.BILLTOADDRESSSEQ
                AND ETYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND ETYPE.DATATYPESEQ = TRN.EVENTTYPESEQ
                AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND CTYPE.DATATYPESEQ = CRD.CREDITTYPESEQ
                AND PAR.PAYEESEQ = CRD.PAYEESEQ
                -- AND PAR.GENERICATTRIBUTE1 = TRN.GENERICATTRIBUTE2
                AND CTYPE.CREDITTYPEID IN
                ('Submitted Order Points',
                    'Submitted Order Points TVGA',
                    'CSI SHCC/FTA',
                'PlatformMigration Points')
                AND ETYPE.EVENTTYPEID IN
                ('TV Submitted',
                    'BroadBand Submitted',
                'Mobile Submitted')
                -- AK- Added below Direct sales dealer code condition on 13.03.2019 after discussion with Arun.
                AND TRN.GENERICATTRIBUTE4 IN
                (
                    SELECT   /* ORIGSQL: (SELECT DISTINCT dim0 FROM stel_lookup WHERE stringvalue = 'DS - Internal') */
                        DISTINCT
                        dim0
                    FROM
                        ext.stel_lookup
                    WHERE
                        stringvalue = 'DS - Internal'
                )
                AND PD.PERIODSEQ = CRD.PERIODSEQ
                AND cRD.positionseq = pos1.ruleelementownerseq
                AND pos1.removedate = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND TRN.compensationdate BETWEEN pos1.effectivestartdate AND TO_DATE(ADD_SECONDS(pos1.effectiveenddate,(86400*-1)))   /* ORIGSQL: pos1.effectiveenddate - 1 */
                AND par.payeeseq = pos1.payeeseq
                AND par.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                AND TRN.compensationdate BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: par.effectiveenddate - 1 */
                AND par.tenantid = pos1.tenantid
                AND trn.compensationdate BETWEEN pd.monthstartdatetd AND TO_DATE(ADD_SECONDS(pd.monthenddatetd,(86400*-1)))   /* ORIGSQL: pd.monthenddatetd - 1 */
                AND trn.tenantid = 'STEL'
                AND TRN.processingunitseq = CRD.PROCESSINGUNITSEQ
                AND crd.periodseq = :p_periodseq
                AND CRD.PROCESSINGUNITSEQ = :p_processingunitseq
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- As per discussion with Arun and Dicson on 18.03.2019, added below block to show transaction without credits generated in cs_credit table.

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DSTRANADMIN' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_DSTRANADMIN (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIOD(...) */
    INSERT INTO EXT.STEL_RPT_DSTRANADMIN
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            ORDERTYPE,
            DATATYPE,
            APPTYPE,
            ORDERID,
            CUSTID,
            IPTVNO,
            ADSLNO,
            CREDITTYPEID,
            ORDERDATE,
            ORDERCLOSE,
            ORDERCANCEL,
            DEALERCODE,
            SALESMANCODE,
            PRODUCTNAME,
            SCHEMEID,
            COMPID,
            PACKAGEID,
            LISTPRICE,
            PREPRICE,/* -- Arun will confirm 11.01.2019 */
            INCVALUE,
            POINTS,
            ORDERSTATUS,
            COMMREMARK,
            COMMMONTH,
            CREATEDATE,
            SALESORDERSEQ,
            SALESTRANSACTIONSEQ,
            CREDITSEQ
        )
        
            SELECT   /* ORIGSQL: (SELECT 'STEL' TENANTID, p_periodseq PERIODSEQ, p_processingunitseq PROCESSINGUN(...) */
                'STEL' AS TENANTID,
                :p_periodseq AS PERIODSEQ,
                :p_processingunitseq AS PROCESSINGUNITSEQ,
                PD.PERIODNAME AS PERIODNAME,
                'Singtel_PU' AS PROCESSINGUNITNAME,
                PD.CALENDARNAME AS CALENDARNAME,
                NULL AS POSITIONSEQ,
                NULL AS PAYEESEQ,
                TRN.GENERICATTRIBUTE10 AS ORDERTYPE,
                ETYPE.EVENTTYPEID AS DATATYPE,
                TRN.GENERICATTRIBUTE5 AS APPTYPE,
                ORD.ORDERID AS ORDERID,
                BILLTO.CUSTID AS CUSTID,
                BILLTO.CONTACT AS IPTVNO,
                BILLTO.PHONE AS ADSLNO,
                NULL AS CREDITTYPEID,
                TRN.ACCOUNTINGDATE AS ORDERDATE,
                TRN.COMPENSATIONDATE AS ORDERCLOSE,
                TRN.GENERICDATE2 AS ORDERCANCEL,
                TRN.GENERICATTRIBUTE4 AS DEALERCODE,
                TRN.GENERICATTRIBUTE2 AS SALESMANCODE,/* -- AK Changed on 18.03.2019 after discussion with Dicson and Arun */
                CASE
                    WHEN ETYPE.EVENTTYPEID = 'TV Submitted'
                    THEN TRN.PRODUCTDESCRIPTION
                    WHEN ETYPE.EVENTTYPEID = 'BroadBand Submitted'
                    THEN TRN.GENERICATTRIBUTE31
                    WHEN ETYPE.EVENTTYPEID = 'Mobile Submitted'
                    THEN TRN.GENERICATTRIBUTE31
                END
                AS PRODUCTNAME,
                BILLTO.ADDRESS2 AS SCHEMEID,
                TRN.PRODUCTID AS COMPID,
                CASE
                    WHEN ETYPE.EVENTTYPEID = 'TV Submitted'
                    THEN TRN.PRODUCTNAME
                    WHEN ETYPE.EVENTTYPEID = 'BroadBand Submitted'
                    THEN TRN.GENERICATTRIBUTE28
                    WHEN ETYPE.EVENTTYPEID = 'Mobile Submitted'
                    THEN TRN.GENERICATTRIBUTE28
                END
                AS PACKAGEID,
                TRN.GENERICNUMBER2 AS LISTPRICE,
                NULL AS PREPRICE,/* -- Arun will confirm 11.01.2019 */  TRN.GENERICNUMBER3 AS INCVALUE,
                0 AS POINTS,
                CASE
                    WHEN TRN.GENERICBOOLEAN1 = 1
                    THEN 'Non-Payable'
                    ELSE 'Payable'
                END
                AS ORDERSTATUS,
                TRN.COMMENTS AS COMMREMARK,
                PD.MONTHNAME1 AS COMMMONTH,
                CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
                trn.SALESORDERSEQ AS SALESORDERSEQ,
                TRN.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
                NULL
            FROM
                (
                    SELECT   /* ORIGSQL: (SELECT * FROM cs_salestransaction WHERE SALESTRANSACTIONSEQ NOT IN (SELECT DIST(...) */
                        *
                    FROM
                        cs_salestransaction
                    WHERE
                        SALESTRANSACTIONSEQ NOT IN
                        (
                            SELECT   /* ORIGSQL: (SELECT DISTINCT SALESTRANSACTIONSEQ FROM STEL_RPT_DSTRANADMIN) */
                                DISTINCT
                                SALESTRANSACTIONSEQ
                            FROM
                                EXT.STEL_RPT_DSTRANADMIN
                        )
                    ) AS trn,
                    cs_salesorder ord,
                    CS_TRANSACTIONADDRESS billto,
                    cs_eventtype etype,
                    stel_monthhierarchy_tbl pd
                    --              cs_participant par
                WHERE
                    TRN.SALESORDERSEQ = ORD.SALESORDERSEQ
                    AND BILLTO.SALESTRANSACTIONSEQ = TRN.SALESTRANSACTIONSEQ
                    AND BILLTO.TRANSACTIONADDRESSSEQ = TRN.BILLTOADDRESSSEQ
                    AND ETYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                    AND ETYPE.DATATYPESEQ = TRN.EVENTTYPESEQ
                    -- AND PAR.GENERICATTRIBUTE1 = TRN.GENERICATTRIBUTE2
                    --AND ord.ORDERID = 'TV-507641A-507644A12468421527210923N'
                    AND ETYPE.EVENTTYPEID IN
                    ('TV Submitted',
                        'BroadBand Submitted',
                    'Mobile Submitted')
                    AND PD.PERIODSEQ = :p_periodseq
                    -- AK- Added below Direct sales dealer code condition on 13.03.2019 after discussion with Arun.
                    AND TRN.GENERICATTRIBUTE4 IN
                    (
                        SELECT   /* ORIGSQL: (SELECT DISTINCT dim0 FROM stel_lookup WHERE stringvalue = 'DS - Internal') */
                            DISTINCT
                            dim0
                        FROM
                            ext.stel_lookup
                        WHERE
                            stringvalue = 'DS - Internal'
                    ) -- AND par.removedate > SYSDATE
                    -- AND TRN.compensationdate BETWEEN par.effectivestartdate
                    -- AND  par.effectiveenddate - 1
                    AND trn.compensationdate BETWEEN pd.monthstartdatetd AND TO_DATE(ADD_SECONDS(pd.monthenddatetd,(86400*-1)))   /* ORIGSQL: pd.monthenddatetd - 1 */
                    AND trn.tenantid = 'STEL'
                    AND TRN.processingunitseq = :p_processingunitseq
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || p_periodseq |(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:p_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:p_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END