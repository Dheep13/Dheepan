CREATE PROCEDURE EXT.RPT_REWARDFIELD
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
    DECLARE v_proc_name VARCHAR(30) = 'RPT_REWARDFIELD';  /* ORIGSQL: v_proc_name VARCHAR(30) := 'RPT_REWARDFIELD'; */
    DECLARE v_tenant VARCHAR(20);  /* ORIGSQL: v_tenant VARCHAR2(20); */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm VARCHAR2(4000); */
    DECLARE v_StartDate TIMESTAMP;  /* ORIGSQL: v_StartDate date; */
    DECLARE v_EndDate TIMESTAMP;  /* ORIGSQL: v_EndDate date; */

    /* ORIGSQL: FOR i IN (SELECT r.rpttype rpttype, UPPER(r.tgtfield) tgtfield, UPPER(r.sourcetb(...) */
    DECLARE CURSOR dbmtk_cursor_12630
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
            UPPER(rt.rewardinitgroup) AS rewardinitgroup,
            IFNULL(periodtype,'month') AS periodtype  /* ORIGSQL: nvl(periodtype,'month') */
        FROM
            ext.stel_rpt_cfg_fieldmap r
        INNER JOIN
            EXT.STEL_RPT_CFG_RPTTYPE rt
            ON rt.rpttype = r.rpttype
        WHERE
            r.rpttype = :p_RPTTYPE
            AND (r.sourcetbl IS NOT NULL
            AND r.sourceexpression IS NOT NULL);

    v_PU = :p_processingunitseq;

    v_tenant = :p_tenantid;

    FOR i AS dbmtk_cursor_12630
    DO
        v_merge1_cls = 'Merge Into '||IFNULL(:i.tgttbl,'') ||' tgt using ( ';

        IF :i.sourcegroup IS NULL
        THEN
            v_select_cls = 'select m.positionseq, m.payeeseq, m.periodseq
            ,  ' ||IFNULL(:i.sourceexpression,'') || ' sourceexpr
            , ' ||IFNULL(:i.rewardinitgroup,'''xx''') ||'  groupfield
            from '|| IFNULL(:i.sourcetbl,'')||' m
            join ext.stel_monthhierarchy pd
            on   pd.monthperiodseq1 = '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' and pd.periodtypename = '''||IFNULL(:i.periodtype,'')||'''
            and pd.monthperiodseqtd=m.periodseq
            join cs_position pos
            on m.positionseq=pos.ruleelementownerseq
            and pos.removedate='||IFNULL(:v_eot,'')||'
            and pd.enddate-1 between pos.effectivestartdate and pos.effectiveenddate-1
            and m.tenantid=pos.tenantid
            join cs_title t
            on t.ruleelementownerseq=pos.titleseq
            and t.removedate='||IFNULL(:v_eot,'')||'
            and pd.enddate-1 between t.effectivestartdate and t.effectiveenddate-1
            and m.tenantid=t.tenantid
            join cs_participant par
            on m.payeeseq=par.payeeseq
            and par.removedate='||IFNULL(:v_eot,'')||  /* ORIGSQL: nvl(i.rewardinitgroup,'''xx''') */
            '
            and pd.enddate-1 between par.effectivestartdate and par.effectiveenddate-1
            and m.tenantid=par.tenantid
            where m.tenantid = '''||IFNULL(:v_tenant,'')||''' and m.processingunitseq = '''||IFNULL(TO_VARCHAR(:v_PU),'')||'''
            and (' ||IFNULL(:i.sourcefilter,'')||')
            group by m.positionseq, m.payeeseq, m.periodseq , ' ||IFNULL(:i.rewardinitgroup,'''xx''') ||' ';  /* ORIGSQL: nvl(i.rewardinitgroup,'''xx''') */

            v_merge2_cls = ' ) src on  (tgt.positionseq=src.positionseq and tgt.dataperiodseq=src.periodseq
            and nvl(tgt.groupfield,''xx'')=nvl(src.groupfield,''xx'') )
        when matched then update set tgt.' ||IFNULL(:i.tgtField,'') ||' = src.sourceexpr
        where tgt.periodseq =  '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||'  ';
        ELSE 
            v_select_cls = 'select  pd.periodseq, ' || IFNULL(:i.sourceexpression,'')|| '
            from '|| IFNULL(:i.sourcetbl,'')||' m
            join cs_period pd
            on  pd.removedate='||IFNULL(:v_eot,'')||' and pd.periodseq = '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||'
            and m.tenantid=pd.tenantid
            and pd.enddate-1 between m.effectivestartdate and m.effectiveenddate-1
            where m.tenantid = '''||IFNULL(:v_tenant,'')||'''
            and (' ||IFNULL(:i.sourcefilter,'')||')
            group by  pd.periodseq, ' ||IFNULL(:i.sourcegroup,'') ||'
            ';

            IF :i.sourcetbl = 'EXT.STEL_POSITION' 
            THEN
                v_merge2_cls = ' ) src on  ((tgt.positionseq=src.keyseq) and tgt.periodseq=src.periodseq)
            when matched then update set tgt.' ||IFNULL(:i.tgtField,'') ||' = src.sourceexpr
            where tgt.periodseq =  '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||'  ';
            END IF;

            IF :i.sourcetbl = 'EXT.STEL_PARTICIPANT' 
            THEN
                v_merge2_cls = ' ) src on  (( tgt.payeeseq=src.keyseq ) and tgt.periodseq=src.periodseq)
            when matched then update set tgt.' ||IFNULL(:i.tgtField,'') ||' = src.sourceexpr
            where tgt.periodseq =  '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||'  ';
            END IF;
        END IF;

        v_sql = IFNULL(:v_merge1_cls,'') || IFNULL(:v_select_cls,'') || IFNULL(:v_merge2_cls,'');

        IF :i.sourcetbl = '[EXPR]' 
        THEN
            v_sql = ' update '||IFNULL(:i.tgttbl,'') ||' tgt set tgt.'||IFNULL(:i.tgtField,'') ||' = '||IFNULL(:i.sourceexpression,'')||' where tgt.periodseq =  '||IFNULL(TO_VARCHAR(:p_PERIODSEQ),'')||' ';
        END IF;

        /* ORIGSQL: stel_Sp_logger ('Running REWARDFIELD query for:'||i.tgtfield, v_proc_name, null,(...) */
        CALL EXT.STEL_SP_LOGGER('Running REWARDFIELD query for:'||IFNULL(:i.tgtfield,''), :v_proc_name, NULL, NULL, :v_sql);

        /* ORIGSQL: dbms_output.put_line(v_Sql); */
       --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql ; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END FOR;  /* ORIGSQL: END LOOP; */

    IF :p_RPTTYPE = 'MREMITSUMMARY' 
    THEN
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

        SELECT
            startdate,
            enddate
        INTO
            v_StartDate,
            v_EndDate
        FROM
            cs_period
        WHERE
            periodseq = :p_PERIODSEQ
            AND removedate >CURRENT_TIMESTAMP;  /* ORIGSQL: sysdate */

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_MREMITSUMMARY' not found */
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x
                /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_FIXEDVALUE' not found */
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Mremittance_WT_OPSMan(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Mremittance_WT_OPSManager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12648

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit OPS_Manager') WHEN MATCHED THE(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit OPS_Manager')
        WHEN MATCHED THEN
            UPDATE SET
                IND_FIRSTREMIT_WEIGH_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Prepaid_Individual_WT(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Prepaid_Individual_WT_OPS_Manager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12651

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit OPS_Manager') WHEN MATCHED THE(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit OPS_Manager')
        WHEN MATCHED THEN
            UPDATE SET
                IND_PREPAID_WEIGH_PERCENT = src.value; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Individual_Commission(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Individual_Commissions_WT_OpsManager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        --and  removedate=to_Date('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12654

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit OPS_Manager') WHEN MATCHED THE(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit OPS_Manager')
        WHEN MATCHED THEN
            UPDATE SET
                IND_COMMISSION_PERCENT = src.value; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Team_Commissions_WT_O(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Team_Commissions_WT_OPS_Manager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12657

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit OPS_Manager') WHEN MATCHED THE(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit OPS_Manager')
        WHEN MATCHED THEN
            UPDATE SET
                TEAM_COMMISSION_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Mremittance_WT_ShopMa(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Mremittance_WT_ShopManager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12660

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Shop Manager') WHEN MATCHED TH(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Shop Manager')
        WHEN MATCHED THEN
            UPDATE SET
                IND_FIRSTREMIT_WEIGH_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Prepaid_Individual_WT(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Prepaid_Individual_WT_ShopManager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12663

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Shop Manager') WHEN MATCHED TH(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Shop Manager')
        WHEN MATCHED THEN
            UPDATE SET
                IND_PREPAID_WEIGH_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Individual_Commission(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Individual_Commissions_WT_ShopManager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12666

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Shop Manager') WHEN MATCHED TH(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Shop Manager')
        WHEN MATCHED THEN
            UPDATE SET
                IND_COMMISSION_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Team_Commissions_WT_S(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Team_Commissions_WT_ShopManager'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12669

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Shop Manager') WHEN MATCHED TH(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Shop Manager')
        WHEN MATCHED THEN
            UPDATE SET
                TEAM_COMMISSION_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Mremittance_WT_ASM' a(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Mremittance_WT_ASM'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12672

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - ASM') WHEN MATCHED(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - ASM')
        WHEN MATCHED THEN
            UPDATE SET
                IND_FIRSTREMIT_WEIGH_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Prepaid_WT_ASM' and e(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Prepaid_WT_ASM'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12675

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - ASM') WHEN MATCHED(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - ASM')
        WHEN MATCHED THEN
            UPDATE SET
                IND_PREPAID_WEIGH_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Individual_Commission(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Individual_Commissions_WT_ASM'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12678

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - ASM') WHEN MATCHED(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - ASM')
        WHEN MATCHED THEN
            UPDATE SET
                IND_COMMISSION_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Team_Commissions_WT_A(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Team_Commissions_WT_ASM'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12681

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - ASM') WHEN MATCHED(...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - ASM')
        WHEN MATCHED THEN
            UPDATE SET
                TEAM_COMMISSION_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Mremittance_WT_SC' an(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Mremittance_WT_SC'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12684

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - SC') WHEN MATCHED (...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - SC')
        WHEN MATCHED THEN
            UPDATE SET
                IND_FIRSTREMIT_WEIGH_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Prepaid_WT_SC' and ef(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Prepaid_WT_SC'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12687

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - SC') WHEN MATCHED (...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - SC')
        WHEN MATCHED THEN
            UPDATE SET
                IND_PREPAID_WEIGH_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Individual_Commission(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Individual_Commissions_WT_SC'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12690

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - SC') WHEN MATCHED (...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - SC')
        WHEN MATCHED THEN
            UPDATE SET
                IND_COMMISSION_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT; 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into STEL_RPT_DATA_MREMITSUMMARY tgt using (WITH x AS (SELECT value*100 AS(...) */
        MERGE INTO EXT.STEL_RPT_DATA_MREMITSUMMARY AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (select value*100 value from cs_fixedvalue where name ='FV_Team_Commissions_WT_S(...) */
                        value*100 AS value
                    FROM
                        cs_fixedvalue
                    WHERE
                        name = 'FV_Team_Commissions_WT_SC'
                        AND effectivestartdate < :v_EndDate
                        AND effectiveenddate > :v_StartDate
                        AND removedate = TO_DATE('22000101','YYYYMMDD')
                
                )
                --AS dbmtk_corrname_12693

                SELECT   /* ORIGSQL: select value from x) src on (designation ='Mremit Sales Rep - SC') WHEN MATCHED (...) */
                    value
                FROM
                    x
                ) src
                ON (designation = 'Mremit Sales Rep - SC')
        WHEN MATCHED THEN
            UPDATE SET
                TEAM_COMMISSION_PERCENT = src.value;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* ORIGSQL: stel_Sp_logger ('RPT_REWARDFIELD completed query for:'||v_StartDate||v_EndDate, (...) */
    CALL EXT.STEL_SP_LOGGER('RPT_REWARDFIELD completed query for:'||IFNULL(TO_VARCHAR(:v_StartDate),'')||IFNULL(TO_VARCHAR(:v_EndDate),''), :v_proc_name, NULL, NULL, NULL);
END