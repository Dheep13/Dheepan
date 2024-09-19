CREATE PROCEDURE EXT.RPT_TXNFIELD
(
    IN p_RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_RPTTYPE IN VARCHAR2 */
    IN p_PERIODSEQ DECIMAL(38,10),   /* ORIGSQL: p_PERIODSEQ IN NUMBER */
    IN p_processingunitseq DECIMAL(38,10),   /* ORIGSQL: p_processingunitseq IN NUMBER */
    IN p_tenantid VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: p_tenantid IN varchar2 */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_eot VARCHAR(100) = 'to_date(''22000101'',''YYYYMMDD'')';  /* ORIGSQL: v_eot VARCHAR(100) := 'to_date(''22000101'',''YYYYMMDD'')' ; */
    DECLARE v_PU DECIMAL(38,10);  /* ORIGSQL: v_PU NUMBER; */
    DECLARE v_sql VARCHAR(31000);  /* ORIGSQL: v_sql VARCHAR2(31000); */
    DECLARE v_merge1_cls VARCHAR(31000);  /* ORIGSQL: v_merge1_cls VARCHAR2(31000); */
    DECLARE v_merge2_cls VARCHAR(31000);  /* ORIGSQL: v_merge2_cls VARCHAR2(31000); */
    DECLARE v_select_cls VARCHAR(31000);  /* ORIGSQL: v_select_cls VARCHAR2(31000); */
    DECLARE v_from_cls VARCHAR(400);  /* ORIGSQL: v_from_cls VARCHAR2(400); */
    DECLARE v_where_cls VARCHAR(31000);  /* ORIGSQL: v_where_cls VARCHAR2(31000); */
    DECLARE v_comma VARCHAR(20) = ' ,';  /* ORIGSQL: v_comma VARCHAR(20) := ',' ; */
    DECLARE v_open_braces VARCHAR(20) = ' ( ';  /* ORIGSQL: v_open_braces VARCHAR(20) := ' (' ; */
    DECLARE v_close_braces VARCHAR(20) = ' ) ';  /* ORIGSQL: v_close_braces VARCHAR(20) := ') ' ; */
    DECLARE v_single_quote VARCHAR(20) = '''';  /* ORIGSQL: v_single_quote VARCHAR(20) := ''''; */

    --  v_source_table   VARCHAR (40) := 'INBOUND_DATA_STAGING';
    DECLARE v_rowcount DECIMAL(38,10);  /* ORIGSQL: v_rowcount NUMBER; */

    --  v_proc_name      VARCHAR (30) := 'SP_INBOUND_TXN_MAP';
    DECLARE v_tenant VARCHAR(20);  /* ORIGSQL: v_tenant VARCHAR2(20); */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm VARCHAR2(4000); */
    DECLARE v_Puname VARCHAR(400);  /* ORIGSQL: v_Puname VARCHAR2(400); */
    DECLARE v_tgttbl VARCHAR(400);  /* ORIGSQL: v_tgttbl VARCHAR2(400); */

    /* ORIGSQL: FOR i IN (SELECT r.rpttype rpttype, UPPER(r.tgtfield) tgtfield, UPPER(r.sourcetb(...) */
    DECLARE CURSOR dbmtk_cursor_13091
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_FIELDMAP' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_RPTTYPE' not found */

        SELECT   /* ORIGSQL: SELECT r.rpttype rpttype, UPPER(r.tgtfield) tgtfield, UPPER(r.sourcetbl) sourcet(...) */
            r.rpttype AS rpttype,
            UPPER(r.tgtfield) AS tgtfield,
            UPPER(r.sourcetbl) AS sourcetbl,
            r.sourceexpression AS sourceexpression,
            r.sourcefilter AS sourcefilter,
            UPPER(rt.tgttbl) AS tgttbl,
            UPPER(r.sourcegroup) AS sourcegroup,
            rt.txngroup,
            rt.txngroupselect,
            rt.txnorder
        FROM
            ext.stel_rpt_cfg_fieldmap r
        INNER JOIN
            EXT.STEL_RPT_CFG_RPTTYPE rt
            ON rt.rpttype = r.rpttype
        WHERE
            r.rpttype = :p_RPTTYPE;

    v_PU = :p_processingunitseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_Puname
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :v_PU;

    v_tenant = :p_tenantid;

    FOR i AS dbmtk_cursor_13091
    DO
        v_merge1_cls = 'Merge Into '||IFNULL(:i.tgttbl,'') ||' tgt using ( ';

        IF :i.sourcetbl = 'CS_CREDIT' 
        THEN
            v_select_cls = 'select c.creditseq
            ,  ' ||IFNULL(:i.sourceexpression,'') || ' sourceexpr
            from '|| IFNULL(:i.sourcetbl,'')||' c ) src
        on (tgt.creditseq=src.creditseq )';
        ELSEIF :i.sourcetbl IN ('VW_SALESTRANSACTION', 'CS_TRANSACTIONASSIGNMENT')  /* ORIGSQL: ELSIF i.sourcetbl in ('VW_SALESTRANSACTION', 'CS_TRANSACTIONASSIGNMENT') THEN */
        THEN
            v_select_cls = 'select DISTINCT c.salestransactionseq, c.salesorderseq, c.positionname
            ,  ' ||IFNULL(:i.sourceexpression,'') || ' sourceexpr
            from '|| IFNULL(:i.sourcetbl,'')||' c ) src
        on (tgt.salestransactionseq=src.salestransactionseq and tgt.salesorderseq=src.salesorderseq and tgt.positionname = src.positionname)';
        ELSEIF :i.sourcetbl = 'STEL_PARTICIPANT'   /* ORIGSQL: ELSIF i.sourcetbl ='STEL_PARTICIPANT' THEN */
        THEN
            v_select_cls = 'select payeeseq keyseq  , c.effectivestartdate, c.effectiveenddate
            ,  ' ||IFNULL(:i.sourceexpression,'') || ' sourceexpr
            from '|| IFNULL(:i.sourcetbl,'')||' c ) src
        on (tgt.payeeseq=src.keyseq and  tgt.compensationdate between src.effectivestartdate and src.effectiveenddate)
        ';
        ELSEIF :i.sourcetbl = 'STEL_POSITION'   /* ORIGSQL: ELSIF i.sourcetbl ='STEL_POSITION' THEN */
        THEN
            v_select_cls = 'select c.ruleelementownerseq keyseq, c.effectivestartdate, c.effectiveenddate
            ,  ' ||IFNULL(:i.sourceexpression,'') || ' sourceexpr
            from '|| IFNULL(:i.sourcetbl,'')||' c
            
        ) src
        on (tgt.positionseq=src.keyseq and tgt.compensationdate between src.effectivestartdate and src.effectiveenddate)
        ';
        ELSE 
            v_select_cls = 'select
            ,  ' ||IFNULL(:i.sourceexpression,'') || '
            from '|| IFNULL(:i.sourcetbl,'')||' c ) src
        on (tgt.salestransactionseq=src.keyseq )
        ';
        END IF;

        v_merge2_cls = '   when matched then update set
        '||IFNULL(:i.tgtfield,'')||' = src.sourceexpr
        where tgt.periodseq= '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' and rpttype='''||IFNULL(:p_RPTTYPE,'')||''' ';

        v_sql = IFNULL(:v_merge1_cls,'') || IFNULL(:v_select_cls,'') ||IFNULL(:v_merge2_cls,'');

        /* ORIGSQL: stel_Sp_logger ('Running SQL for '||i.tgtfield, 'RPT_TXNFIELD', NULL, NULL, v_sq(...) */
        CALL EXT.STEL_SP_LOGGER('Running SQL for '||IFNULL(:i.tgtfield,''), 'RPT_TXNFIELD', NULL, NULL, :v_sql);

        /* ORIGSQL: dbms_output.put_line(v_Sql); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql ; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END FOR;  /* ORIGSQL: END LOOP; */

    --Grouping and inserting into the main table can be done in post SQL.
    --the post SQL should be a simple insert as much as possible, with no business logic except field mappings.
END