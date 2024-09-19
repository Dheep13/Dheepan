CREATE PROCEDURE EXT.RPT_POST_TSDETAIL
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
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_TS_TRANSACTIONDET' not found */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO stel_rpt_ts_transactiondet tgt USING (SELECT MIN(compensationdate) AS(...) */
    MERGE INTO ext.stel_rpt_ts_transactiondet AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (SELECT MIN(compensationdate) closedate, alternateordernumber FROM vw_Salestrans(...) */
                MIN(compensationdate) AS closedate,
                alternateordernumber
            FROM
                vw_Salestransaction st
            INNER JOIN
                cs_eventtype et
                ON et.datatypeseq = st.eventtypeseq
                AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            WHERE
                ET.eventtypeid LIKE '%Close%'
            GROUP BY
                alternateordernumber
        ) AS src
        ON (src.alternateordernumber = tgt.alternateordernumber AND tgt.periodseq = :p_periodseq AND tgt.rpttype = :p_rpttype AND tgt.processingunitseq = :p_processingunitseq)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.ordeR_closeddate = src.closedate
        --WHERE
         --   tgt.periodseq = :p_periodseq
         --   AND tgt.rpttype = :p_rpttype
         --   AND tgt.processingunitseq = :p_processingunitseq
            ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- 01.11.2017 As per discussion with Balaji updating below with COST, need to confirm on STEL_CLASSIFIER.PRICE with Babu.   

    /* ORIGSQL: UPDATE stel_rpt_ts_transactiondet T1 SET LISTPRICE = (SELECT DISTINCT COST FROM (...) */
    UPDATE ext.stel_rpt_ts_transactiondet T1
        SET
        /* ORIGSQL: LISTPRICE = */
        LISTPRICE = (
            SELECT   /* ORIGSQL: (SELECT DISTINCT COST FROM stel_classifier S1 WHERE T1.PRODUCTID = S1.CLASSIFIER(...) */
                DISTINCT
                COST
            FROM
                ext.stel_classifier S1
            WHERE
                T1.PRODUCTID = S1.CLASSIFIERID
                AND S1.CATEGORYNAME = 'PRODUCTS'
                AND S1.CATEGORYTREENAME = 'Singtel'
        )
    WHERE
        T1.PERIODSEQ = :p_periodseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Previous list price   

    /* ORIGSQL: UPDATE stel_rpt_ts_transactiondet T1 SET PRE_LISTPRICE = (SELECT MAX(GENERICNUMB(...) */
    UPDATE ext.stel_rpt_ts_transactiondet T1
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
        SET
        /* ORIGSQL: PRE_LISTPRICE = */
        PRE_LISTPRICE = (
            SELECT   /* ORIGSQL: (SELECT MAX(GENERICNUMBER4 - GENERICNUMBER5) PRE_LISTPRICE FROM CS_CREDIT CRD WH(...) */
                MAX(GENERICNUMBER4 - GENERICNUMBER5) AS PRE_LISTPRICE
            FROM
                CS_CREDIT CRD
            WHERE
                CRD.SALESTRANSACTIONSEQ = T1.SALESTRANSACTIONSEQ
                AND CRD.SALESORDERSEQ = T1.SALESORDERSEQ
                AND CRD.GENERICATTRIBUTE5 <> 'New'
                AND CRD.PERIODSEQ = T1.PERIODSEQ
        )
    WHERE
        T1.PERIODSEQ = :p_periodseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;
END