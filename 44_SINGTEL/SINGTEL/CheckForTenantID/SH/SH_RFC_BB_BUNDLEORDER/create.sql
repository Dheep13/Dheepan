CREATE PROCEDURE EXT.SH_RFC_BB_BundleOrder
(
    IN p_PERIODSEQ DECIMAL(38,10),   /* ORIGSQL: p_PERIODSEQ IN NUMBER */
    IN p_PROCESSINGUNITSEQ DECIMAL(38,10)   /* ORIGSQL: p_PROCESSINGUNITSEQ IN NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_StartDate TIMESTAMP;  /* ORIGSQL: v_StartDate date; */
    DECLARE v_EndDate TIMESTAMP;  /* ORIGSQL: v_EndDate date; */
    DECLARE v_delproductid VARCHAR(255);  /* ORIGSQL: v_delproductid varchar2(255); */
    DECLARE v_rowcount DECIMAL(38,10);  /* ORIGSQL: v_rowcount number; */
    

    /* ORIGSQL: dbms_stats.gather_table_stats(ownname =>'STELEXT', tabname => 'STEL_DATA_BCCBUND(...) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'STELEXT'|| '.'|| 'STEL_DATA_BCCBUNDLE';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_salestransaction tgt using (SELECT st.salestransactionseq, st.prod(...) */
    MERGE INTO cs_salestransaction AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_DATA_BCCBUNDLE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select st.salestransactionseq,st.productid,bundle.price_plan_component_id as bP(...) */
                st.salestransactionseq,
                st.productid,
                bundle.price_plan_component_id AS bProductid,
                st.tenantid,
                st.processingunitseq,
                st.compensationdate
            FROM
                cs_salestransaction st
            INNER JOIN
                EXT.STEL_DATA_BCCBUNDLE bundle
                ON bundle.ORDER_ID = st.alternateordernumber
                AND bundle.order_action_id = st.genericattribute6
                AND st.genericattribute9 = 'M'
                /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONADDRESS' not found */
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select salestransactionseq,tenantid,processingunitseq,compensationdate,contact (...) */
                        salestransactionseq,
                        tenantid,
                        processingunitseq,
                        compensationdate,
                        contact
                    FROM
                        cs_transactionaddress
                    WHERE
                        addresstypeseq
                        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_ADDRESSTYPE' not found */
                        =
                        (
                            SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                addresstypeseq
                            FROM
                                cs_addresstype
                            WHERE
                                addresstypeid = 'BILLTO'
                        )
                    ) AS tadb
                    ON st.salestransactionseq = tadb.salestransactionseq
                    AND st.tenantid = tadb.tenantid
                    AND tadb.processingunitseq = st.processingunitseq
                    AND tadb.compensationdate = st.compensationdate
                WHERE
                    st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND st.tenantid = 'STEL'
                    AND st.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND st.genericboolean1 = 0
                    AND st.eventtypeseq
                    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
                    IN
                    (
                        SELECT   /* ORIGSQL: (select datatypeseq from cs_eventtype where eventtypeid in ('BroadBand Submitted(...) */
                            datatypeseq
                        FROM
                            cs_eventtype
                        WHERE
                            eventtypeid IN ('BroadBand Submitted','BroadBand Closed')
                            AND removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    )
                ) AS src
                ON (Src.salestransactionseq = tgt.salestransactionseq
                    AND src.processingunitseq = tgt.processingunitseq
                    AND src.compensationdate = tgt.compensationdate
                AND src.tenantid = tgt.tenantid AND tgt.tenantid = 'STEL' AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1))))
    WHEN MATCHED THEN
        UPDATE SET --tgt.productid = src.bProductid,
            tgt.comments = 'Product ID updated based on the Bundle Order'
        --WHERE
            --tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
            --AND tgt.tenantid = 'STEL'
            --AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_NONPAYABLELINES','SH',0,v_rowcount,'Update BroadBand Orde(...) */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_NONPAYABLELINES', 'SH', 0, :v_rowcount, 'Update BroadBand Orders based on Bundled Order ProductID - Completed');

    /* ORIGSQL: commit; */
    COMMIT;
END