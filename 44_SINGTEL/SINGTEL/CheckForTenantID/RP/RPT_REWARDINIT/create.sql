CREATE PROCEDURE EXT.RPT_REWARDINIT
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
    DECLARE v_PUNAme VARCHAR(200);  /* ORIGSQL: v_PUNAme varchar2(200); */
    DECLARE v_tenant VARCHAR(20);  /* ORIGSQL: v_tenant varchar2(20); */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm VARCHAR2(4000); */

    /* ORIGSQL: FOR i IN (select postproc, tgttbl, payeefilter, titlefilter, rewardfiltertbl, re(...) */
    DECLARE CURSOR dbmtk_cursor_12894
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_RPTTYPE' not found */

        SELECT   /* ORIGSQL: select postproc, tgttbl, payeefilter, titlefilter, rewardfiltertbl, rewardfilter(...) */
            postproc,
            tgttbl,
            payeefilter,
            titlefilter,
            rewardfiltertbl,
            rewardfilterclause,
            rewardinitgroup,
            IFNULL(periodtype,'month') AS periodtype  /* ORIGSQL: nvl(periodtype,'month') */
        FROM
            stel_rpt_cfg_rpttype
        WHERE
            rpttype = :p_RPTTYPE;

    v_PU = :p_processingunitseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */

    SELECT
        name
    INTO
        v_PUNAme
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

    FOR i AS dbmtk_cursor_12894
    DO
        v_insert_cls = 'Insert Into '||IFNULL(:i.tgttbl,'') ||' (positionseq, payeeseq, dataperiodseq, periodseq, rpttype, loaddate
        , positionname, periodname, dataperiodname, groupfield, tenantid, processingunitseq, processingunitname, lastname, userid, payperiodseq, calendarname) ';

        IF :i.rewardfiltertbl IS NOT NULL
        THEN
            v_select_cls = 'select m.positionseq, m.payeeseq, m.periodseq , pd.monthperiodseq1
            , ''' || IFNULL(:p_RPTTYPE,'')|| ''', sysdate , pos.name, pd.monthname1 , pd.monthnametd, nvl(' || IFNULL(:i.rewardinitgroup,'')|| ' ,''xx'')
            , ''' ||IFNULL(:v_tenant,'')||''' ,  ' ||IFNULL(TO_VARCHAR(:v_PU),'') ||', ''' || IFNULL(:v_PUNAme,'')||''', par.lastname, par.userid ,pd.periodseq, pd.calendarname
            from ' || IFNULL(:i.rewardfiltertbl,'')||' m
            
            
            join stel_monthhierarchy pd
            on -- pd.removedate=' ||IFNULL(:v_eot,'')||' and
            pd.monthperiodseq1 = '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' and pd.periodtypename = '''||IFNULL(:i.periodtype,'')||'''
            
            and pd.monthperiodseqtd=m.periodseq
            join cs_position pos
            on m.positionseq=pos.ruleelementownerseq
            and m.tenantid=pos.tenantid
            and pos.removedate='||IFNULL(:v_eot,'')||'
            and pd.enddate-1 between pos.effectivestartdate and pos.effectiveenddate-1
            join cs_title t
            on t.ruleelementownerseq=pos.titleseq
            and t.removedate='||IFNULL(:v_eot,'')||'
            and pd.enddate-1 between t.effectivestartdate and t.effectiveenddate-1
            and m.tenantid=t.tenantid
            join cs_participant par
            on m.payeeseq=par.payeeseq
            and par.removedate='||IFNULL(:v_eot,'')||'
            and m.tenantid=par.tenantid
            and pd.enddate-1 between par.effectivestartdate and par.effectiveenddate-1
            where m.tenantid = '''||IFNULL(:v_tenant,'')||''' and m.processingunitseq = '''||IFNULL(TO_VARCHAR(:v_PU),'')||'''
            and (' ||IFNULL(:i.rewardfilterclause,'')||')
            group by m.positionseq, m.payeeseq, m.periodseq , pd.monthperiodseq1,par.lastname, par.userid, pd.periodseq, pd.calendarname
            , ''' || IFNULL(:p_RPTTYPE,'')|| ''',  pos.name, pd.monthname1 , pd.monthnametd, nvl(' || IFNULL(:i.rewardinitgroup,'')|| ',''xx'')
            ';
        ELSE 
            v_select_cls = 'select distinct pos.ruleelementownerseq, pos.payeeseq, pd.monthperiodseqtd, pd.monthperiodseq1, ''' || IFNULL(:p_RPTTYPE,'')|| ''' , sysdate ,
            pos.name, pd.monthname1, pd.monthnametd, null  , ''' ||IFNULL(:v_tenant,'')||''' ,  ' ||IFNULL(TO_VARCHAR(:v_PU),'') ||', ''' || IFNULL(:v_PUNAme,'')||''', par.lastname, par.userid, pd.periodseq, pd.calendarname
            from   cs_position pos
            join stel_monthhierarchy pd
            on -- pd.removedate=' ||IFNULL(:v_eot,'')||' and
            pd.monthperiodseq1 = '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' and pd.periodtypename = '''||IFNULL(:i.periodtype,'')||'''
            
            join cs_title t
            on t.ruleelementownerseq=pos.titleseq
            and t.removedate='||IFNULL(:v_eot,'')||'
            and pd.enddate-1 between t.effectivestartdate and t.effectiveenddate-1
            and pos.tenantid=t.tenantid
            join cs_participant par
            on pos.payeeseq=par.payeeseq
            and par.removedate='||IFNULL(:v_eot,'')||'
            and pd.enddate-1 between par.effectivestartdate and par.effectiveenddate-1
            and pos.tenantid=par.tenantid
            where   pos.tenantid = '''||IFNULL(:v_tenant,'')||''' and pos.removedate='||IFNULL(:v_eot,'')||'
            and (' ||IFNULL(:i.payeefilter,'')||')';
        END IF;

        v_sql = IFNULL(:v_insert_cls,'') || IFNULL(:v_select_cls,'');
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

        --truncate for period
        --  execute immediate ' delete from '|| i.tgttbl ||' where periodseq = '||p_periodseq ||'';

        /* ORIGSQL: stel_Sp_logger ('Starting REWARDINIT SQL', 'RPT_REWARDINIT', NULL, NULL, v_sql) */
        CALL EXT.STEL_SP_LOGGER('Starting REWARDINIT SQL', 'RPT_REWARDINIT', NULL, NULL, :v_sql);

        /* ORIGSQL: dbms_output.put_line(v_sql); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql ; */
        EXECUTE IMMEDIATE :v_sql;

        v_sql = ' merge into '||IFNULL(:i.tgttbl,'') ||' tgt using stel_rpt_cfg_lookup src on (tgt.groupfield = src.inputname
        and src.rpttype=tgt.rpttype)
        when matched then update set tgt.groupfieldlabel = src.outputlabel
        where tgt.periodseq=' ||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' and rpttype = '''||IFNULL(:p_RPTTYPE,'')||''' ';

        /* ORIGSQL: stel_Sp_logger ('Starting REWARDINIT Merge for GroupLabel', 'RPT_REWARDINIT', NU(...) */
        CALL EXT.STEL_SP_LOGGER('Starting REWARDINIT Merge for GroupLabel', 'RPT_REWARDINIT', NULL, NULL, :v_sql);

        /* ORIGSQL: dbms_output.put_line(v_sql); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql ; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: stel_Sp_logger ('Starting REWARDINIT Update for Tenant and PU', 'RPT_REWARDINIT'(...) */
        CALL EXT.STEL_SP_LOGGER('Starting REWARDINIT Update for Tenant and PU', 'RPT_REWARDINIT', NULL, NULL, :v_sql);
        /*
        v_sql:=' update '||i.tgttbl ||' tgt  set tgt.tenantid = (select paramtext from stel_rpt_cfG_genericparameter where param=''TENANT'')
         , tgt.processingunitseq=(select paramnumber from stel_rpt_cfG_genericparameter where param=''PU'')
         , tgt.processingunitname= (select paramtext from stel_rpt_cfG_genericparameter where param=''PU'')
         where tgt.periodseq='||p_periodseq||' ';
        
        EXECUTE IMMEDIATE v_sql ;
         */

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END FOR;  /* ORIGSQL: END LOOP; */
END