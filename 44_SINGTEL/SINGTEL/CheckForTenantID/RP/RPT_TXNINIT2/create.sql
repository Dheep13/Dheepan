CREATE PROCEDURE EXT.RPT_TXNINIT2
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
    DECLARE v_sql VARCHAR(31000);  /* ORIGSQL: v_sql VARCHAR2(31000); */
    DECLARE v_insert_cls VARCHAR(31000);  /* ORIGSQL: v_insert_cls VARCHAR2(31000); */
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
    DECLARE v_PU DECIMAL(38,10);  /* ORIGSQL: v_PU number; */
    DECLARE v_puname VARCHAR(200);  /* ORIGSQL: v_puname varchar2(200); */
    DECLARE v_tenant VARCHAR(20);  /* ORIGSQL: v_tenant varchar2(20); */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm VARCHAR2(4000); */

    /* ORIGSQL: FOR i IN (select postproc, tgttbl, nvl(payeefilter, '1=1') payeefilter, nvl(titl(...) */
    DECLARE CURSOR dbmtk_cursor_13283
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_RPTTYPE' not found */

        SELECT   /* ORIGSQL: select postproc, tgttbl, nvl(payeefilter, '1=1') payeefilter, nvl(titlefilter,'1(...) */
            postproc,
            tgttbl,
            IFNULL(payeefilter, '1=1') AS payeefilter,  /* ORIGSQL: nvl(payeefilter, '1=1') */
            IFNULL(titlefilter,'1=1') AS titlefilter,  /* ORIGSQL: nvl(titlefilter,'1=1') */
            txnfiltertbl,
            IFNULL(txnfilterclause,'1=1') AS txnfilterclause,  /* ORIGSQL: nvl(txnfilterclause,'1=1') */
            txngroup,
            txnorder,
            includecredits
        FROM
            ext.stel_rpt_cfg_rpttype
        WHERE
            rpttype = :p_RPTTYPE;

    v_PU = :p_processingunitseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_puname
    FROM
        cs_processingunit
    WHERE
        processingunitseq = :v_PU;

    v_tenant = :p_tenantid;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_GENERICPARAMETER' not found */
    /* ORIGSQL: update stel_rpt_cfg_Genericparameter SET paramnumber=p_periodseq where param = p(...) */
    UPDATE ext.stel_rpt_cfg_Genericparameter
        SET
        /* ORIGSQL: paramnumber = */
        paramnumber = :p_PERIODSEQ
    FROM
        ext.stel_rpt_cfg_Genericparameter
    WHERE
        param = IFNULL(:p_RPTTYPE,'')||'-PERIODSEQ';

    /* ORIGSQL: commit; */
    COMMIT;

    FOR i AS dbmtk_cursor_13283
    DO
        v_insert_cls = 'Insert Into '||IFNULL(:i.tgttbl,'') ||' (
            rpttype, loaddate, salestransactionseq, salesorderseq, positionname, positionseq
            , tenantid, processingunitseq, processingunitname
            , payeeseq, creditseq,   creditname,  compensationdate,  periodseq , MonthEndDate
            , PEriodName, credittypeid
            , datasource, txnvalue, txnunits, creditvalue
            ,userid
            ,productid, productname, productdescription, paymentterms
            ,orderid, alternateordernumber, partname
        ) ';

        IF :i.txnfiltertbl IS NOT NULL
        THEN
            v_select_cls = '
            
            select distinct ''' ||IFNULL(:p_RPTTYPE,'')|| ''' RPTTYPE , sysdate, st.salestransactionseq, st.salesorderseq, ta.positionname, pos1.ruleelementownerseq
            , ''' ||IFNULL(:v_tenant,'')||''' ,  ' ||IFNULL(TO_VARCHAR(:v_PU),'') ||', ''' || IFNULL(:v_puname,'')||'''
            , pos1.payeeseq, c.creditseq, c.name creditname, st.compensationdate, pd.periodseq ,pd.enddate-1 MonthEndDate
            ,   pd.name, ct.credittypeid
            , st.datasource, st.value txnvalue, st.numberofunits txnunits, c.value creditvalue
            ,par.userid
            ,st.productid, st.productname, st.productdescription, st.paymentterms
            ,so.orderid, st.alternateordernumber, par.lastname
            
            from cs_Salestransaction st
            join cs_transactionassignment ta
            on st.salestransactionseq=ta.salestransactionseq and ta.setnumber=1
            and ta.processingunitseq=st.processingunitseq
            and st.tenantid=ta.tenantid
            join cs_salesorder so
            on so.salesorderseq=st.salesorderseq and so.removedate>sysdate
            join cs_period pd
            on pd.periodseq=  ' ||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||'
            and pd.removedate='||IFNULL(:v_eot,'')||'
            and pd.tenantid=st.tenantid
            join cs_position pos1
            on ta.positionname= pos1.name and pos1.removedate='||IFNULL(:v_eot,'')||'
            and st.compensationdate between pos1.effectivestartdate and pos1.effectiveenddate-1
            and pos1.tenantid=st.tenantid
            join cs_participant par
            on par.payeeseq=pos1.payeeseq and par.removedate>Sysdate
            and st.compensationdate between par.effectivestartdate and par.effectiveenddate-1
            and par.tenantid=pos1.tenantid
            join cs_title t
            on t.ruleelementownerseq=pos1.titleseq
            and t.removedate='||IFNULL(:v_eot,'')||'
            and (' ||IFNULL(:i.titlefilter,'')||')
            and pd.enddate-1 between t.effectivestartdate and t.effectiveenddate-1
            and st.tenantid=t.tenantid
            
            left join cs_position pos2
            on ta.positionname= pos2.name and pos2.removedate=' ||IFNULL(:v_eot,'')||'
            and  pd.enddate-1 between pos2.effectivestartdate and pos2.effectiveenddate-1
            and pos2.tenantid=st.tenantid
            left join cs_Credit c
            on c.salestransactionseq=st.salestransactionseq
            and c.tenantid=st.tenantid and c.processingunitseq=st.processingunitseq and (' ||IFNULL(:i.includecredits,'')||'=''1'')
            left join cs_Credittype ct
            on c.credittypeseq=ct.datatypeseq and ct.removedate>sysdate
            
            where st.compensationdate between pd.startdate and pd.enddate-1
            
            and   st.tenantid = ''' ||IFNULL(:v_tenant,'')||'''
            and st.processingunitseq = '||IFNULL(TO_VARCHAR(:v_PU),'')||'
            and (' ||IFNULL(:i.payeefilter,'')||')
            and (' ||IFNULL(:i.txnfilterclause,'')||')';
        END IF;

        v_sql = IFNULL(:v_insert_cls,'') || IFNULL(:v_select_cls,'');

        /* ORIGSQL: dbms_output.put_line(v_sql); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);
        BEGIN 
            DECLARE EXIT HANDLER FOR SQLEXCEPTION
                /* ORIGSQL: when others then */
                BEGIN
                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: execute immediate ' ALTER TABLE ' || i.tgttbl ||'  ADD PARTITION p_'||p_periodse(...) */
                    EXECUTE IMMEDIATE ' ALTER TABLE ' || IFNULL(:i.tgttbl,'') ||'  ADD PARTITION p_'||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' VALUES (' ||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||')';
                END;


            --truncate for period
            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: execute immediate 'ALTER TABLE ' || i.tgttbl ||'  TRUNCATE PARTITION p_'||p_peri(...) */
            EXECUTE IMMEDIATE 'ALTER TABLE ' || IFNULL(:i.tgttbl,'') ||'  TRUNCATE PARTITION p_'||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' ';

            /* ORIGSQL: exception when others then */
        END;

        /* ORIGSQL: stel_Sp_logger ('xxx', '', NULL, NULL, v_sql) */
        CALL EXT.STEL_SP_LOGGER('xxx', '', NULL, NULL, :v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql ; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END FOR;  /* ORIGSQL: END LOOP; */
END