CREATE PROCEDURE EXT.SP_FV_EFFDATEUPD
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_max DECIMAL(38,10);  /* ORIGSQL: v_max NUMBER; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    DECLARE v_tempRecStat DECIMAL(38,10) = 2;  /* ORIGSQL: v_tempRecStat NUMBER:=2; */
    DECLARE v_diff DECIMAL(38,10) = 0;  /* ORIGSQL: v_diff NUMBER:=0; */
    DECLARE i DECIMAL(38,10) = 0;  /* ORIGSQL: i NUMBER:=0; */
    DECLARE j DECIMAL(38,10) = 0;  /* ORIGSQL: j NUMBER:=0; */
    DECLARE iTxt VARCHAR(2);  /* ORIGSQL: iTxt VARCHAR2(2); */
    DECLARE jTxt VARCHAR(2);  /* ORIGSQL: jTxt VARCHAR2(2); */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        /* ORIGSQL: WHEN no_data_found THEN */
        BEGIN
            /* ORIGSQL: NULL; */
            DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
        END;

        /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
        /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
        --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

        SELECT *
        INTO
            v_inbound_cfg_parameter
        FROM
            EXT.INBOUND_CFG_PARAMETER;

        /* ORIGSQL: dbms_output.put_line('Started'); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Started');   

        /* ORIGSQL: UPDATE inbound_data_plfv a SET initialperiod =effectivestartdate, effectiveendda(...) */
        UPDATE EXT.inbound_data_plfv a
            SET
            /* ORIGSQL: initialperiod = */
            initialperiod = effectivestartdate,
            /* ORIGSQL: effectiveenddate = */
            --effectiveenddate = effectiveenddate+1,
            /* ORIGSQL: recordstatus = */
            recordstatus = :v_tempRecStat
        WHERE
            a.recordstatus = 0
            AND a.filename = :v_inbound_cfg_parameter.file_name
            AND a.filedate = :v_inbound_cfg_parameter.file_date;

        --dbms_output.put_line('First Update' || SQL%ROWCOUNT || ' ' ||v_inbound_cfg_parameter.file_date || ' '|| v_inbound_cfg_parameter.file_name);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge INTO inbound_data_plfv tgt USING (SELECT a.ASSIGNEENAME, a.initialperiod, (...) */
        MERGE INTO ext.inbound_data_plfv AS tgt 
            USING
            (
                SELECT   /* ORIGSQL: (SELECT a.ASSIGNEENAME, a.initialperiod, posFirst.effectivestartdate firsteffdat(...) */
                    a.ASSIGNEENAME,
                    a.initialperiod,
                    posFirst.effectivestartdate AS firsteffdate,
                    GREATEST(a.effectivestartdate,
                        CASE 
                            WHEN TO_VARCHAR(posFirst.effectivestartdate,'DD') = '01'  /* ORIGSQL: TO_CHAR(posFirst.effectivestartdate,'DD') */
                            THEN posFirst.effectivestartdate
                            ELSE TO_DATE(ADD_SECONDS(LAST_DAY(posFirst.effectivestartdate),(86400*1)))   /* ORIGSQL: LAST_DAY(posFirst.effectivestartdate) +1 */
                        END
                    ) AS startdate
                FROM
                    ext.inbound_data_plfv a
                LEFT OUTER JOIN
                    cs_position pos
                    ON pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND ASSIGNEENAME = pos.name
                    AND a.effectivestartdate BETWEEN pos.effectivestartdate AND Add_Days(pos.effectiveenddate,-1)
                INNER JOIN
                    cs_position posFirst
                    ON posFirst.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND posFirst.name = a.assigneename
                    AND posFirst.effectivestartdate =
                    (
                        SELECT   /* ORIGSQL: (SELECT MIN(effectivestartdate) FROM cs_position@STELEXT p2 WHERE p2.removedate>(...) */
                            MIN(effectivestartdate)
                        FROM
                            cs_position p2
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        WHERE
                            p2.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            AND p2.name = posFirst.name
                    )
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    a.recordstatus = :v_tempRecStat
                    AND a.filename = :v_inbound_cfg_parameter.file_name
                    AND a.filedate = :v_inbound_cfg_parameter.file_date
                    AND pos.effectivestartdate IS NULL
            ) AS src
            ON (tgt.ASSIGNEENAME = src.ASSIGNEENAME
                AND tgt.initialperiod = src.initialperiod
            	AND tgt.recordstatus = :v_tempRecStat
                AND filename = :v_inbound_cfg_parameter.file_name
                AND filedate = :v_inbound_cfg_parameter.file_date
            )
        WHEN MATCHED THEN
            UPDATE
                SET tgt.effectivestartdate = src.startdate--, tgt.firsteffdate = src.firsteffdate
            --WHERE
                --tgt.recordstatus = :v_tempRecStat
                --AND filename = v_inbound_cfg_parameter.file_name
                --AND filedate = v_inbound_cfg_parameter.file_date
                ;

        /* ORIGSQL: dbms_output.put_line('Merge 1' || SQL%ROWCOUNT); */
       -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Merge 1'|| ::ROWCOUNT);  

        /* merge INTO inbound_data_plva tgt
        using (Select * from inbound_data_fvvamon  WHERE recordstatus    =v_tempRecStat
            AND filename              =v_inbound_cfg_parameter.file_name
        AND filedate              =v_inbound_cfg_parameter.file_date) src
        on (src.assigneename = tgt.assigneename and tgt.variablename=src.variablename)
        when matched then update set
        tgt.assigneedate=src.period01_Startdate
        where tgt.recordstatus    =v_tempRecStat
        AND filename              =v_inbound_cfg_parameter.file_name
        AND filedate              =v_inbound_cfg_parameter.file_date;
        */
        /*VA needs a separate entry for each position version*/   

        /* ORIGSQL: update inbound_data_plva SET recordstatus=-1 where filename =v_inbound_cfg_param(...) */
        UPDATE ext.inbound_data_plva
            SET
            /* ORIGSQL: recordstatus = */
            recordstatus = -1
        FROM
            ext.inbound_data_plva
        WHERE
            filename = :v_inbound_cfg_parameter.file_name
            AND filedate = :v_inbound_cfg_parameter.file_date;

        /* ORIGSQL: dbms_output.put_line('Setting to -1: '||v_inbound_cfg_parameter.file_name); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Setting to -1: '||IFNULL(v_inbound_cfg_parameter.file_name,''));

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_PLFV' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_PLVA' not found */

        /* ORIGSQL: insert into inbound_data_plva select x.*, ROW_NUMBER() OVER (PARTITION BY x.assi(...) */
        INSERT INTO ext.inbound_data_plva
            SELECT   /* ORIGSQL: select x.*, ROW_NUMBER() OVER (PARTITION BY x.assigneename ORDER BY x.effectives(...) */
                x.*,
                ROW_NUMBER() OVER (PARTITION BY x.assigneename ORDER BY x.effectivestartdate),
                ROW_NUMBER() OVER (PARTITION BY x.assigneename ORDER BY x.effectivestartdate)
            FROM
                (
                    SELECT   /* ORIGSQL: (select distinct a.filedate, a.filename, 0, a.downloaded, a.assigneename, 'Posit(...) */
                        DISTINCT
                        a.filedate,
                        a.filename,
                        0,
                        a.downloaded,
                        a.assigneename,
                        'Position',
                        GREATEST(b.effectivestartdate,a.effectivestartdate) AS effectivestartdate,
                        a.variablename,
                        a.name,
                        'FixedValue' AS RuleElemType
                    FROM
                        ext.inbound_data_plfv a
                    INNER JOIN
                        cs_position b
                        ON b.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */ -- Sanjay; added the effective date from here
                        AND a.assigneename = b.name
                        --AND (b.effectivestartdate, b.effectiveenddate-1)
                        /*overlaps
                        (a.effectivestartdate, ADD_MONTHS(TO_DATE((IFNULL(TO_VARCHAR(a.effectivestartdate,'YYYY'),'') ||'0331'), 'YYYYMMDD'),12))  /* ORIGSQL: to_Char(a.effectivestartdate,'YYYY') */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        AND a.effectivestartdate < b.effectiveenddate -- Sanjay; added the effective date here
                        AND a.effectiveenddate > b.effectivestartdate -- Sanjay; added the effective date here
                    WHERE
                        a.recordstatus = :v_tempRecStat
                        AND a.filename = :v_inbound_cfg_parameter.file_name
                        AND a.filedate = :v_inbound_cfg_parameter.file_date
                ) AS x;

        /* ORIGSQL: dbms_output.put_line('Insert into plva: '||SQL%ROWCOUNT); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Insert into plva: '||::ROWCOUNT);  

        /*
        FOR r IN
        (SELECT *
            FROM inbound_data_fvvamon tgt
            WHERE tgt.recordstatus     =v_tempRecStat
            AND filename               =v_inbound_cfg_parameter.file_name
            AND filedate               =v_inbound_cfg_parameter.file_date
            AND tgt.period01_startdate<>tgt.initialperiod
        )
        LOOP
        v_diff   := ABS(months_between(r.period01_startdate, r.initialperiod));
        IF v_diff <=12 THEN
        dbms_output.put_line('Loop 1' );
          FOR i IN 1..12-v_diff
          LOOP
          dbms_output.put_line('Loop 3:'||i );
            iTxt  :=lpad(i,2,'0');
            jTxt  :=lpad(i+v_diff,2,'0');
            v_Sql:='update inbound_data_fvvamon tgt  set PERIOD'||iTxt||'_VALUE  =  PERIOD'||jTxt||'_VALUE
        where tgt.assigneename='''||r.assigneename||''' and tgt.initialperiod = '''||r.initialperiod||'''
        and tgt.recordstatus= '||v_tempRecStat||' ';
            execute immediate v_sql;
            dbms_output.put_line(v_sql);
          END LOOP;
        
         i:=0;
        
         FOR i IN 12-v_diff+1..12
          LOOP
          dbms_output.put_line('Loop 2:'||i );
          iTxt  :=lpad(i,2,'0');
            v_Sql:='update inbound_data_fvvamon tgt  set PERIOD'||iTxt||'_VALUE  =  null
        where tgt.assigneename='''||r.assigneename||''' and tgt.initialperiod = '''||r.initialperiod||'''
        and tgt.recordstatus= '||v_tempRecStat||' ';
            dbms_output.put_line(v_sql);
            execute immediate v_sql;
          end loop;
        
        
        END IF;
        END LOOP;
        */   

        /* ORIGSQL: UPDATE inbound_data_plfv a SET businessunitname = (SELECT MAX(bu.name) FROM DBMT(...) */
        UPDATE ext.inbound_data_plfv a
            /* --initialperiod   =period01_startdate, */
            SET
            /* ORIGSQL: businessunitname = */
            businessunitname = (
                SELECT   /* ORIGSQL: (select MAX(bu.name) from cs_ruleelementowner@stelext reo join cs_position@stele(...) */
                    MAX(bu.name)
                FROM
                    cs_ruleelementowner reo
                INNER JOIN
                    cs_position p
                    ON p.ruleelementownerseq = reo.ruleelementownerseq
                    AND p.effectivestartdate = reo.effectivestartdate
                    AND p.removedate = reo.removedate
                INNER JOIN
                    cs_businessunit bu
                    ON bu.mask = reo.businessunitmap /* --one pos can only have one bu */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_ruleelementowner@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_ruleelementowner_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_businessunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_businessunit_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    reo.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND p.name = a.ASSIGNEENAME
            ),
            /* ORIGSQL: recordstatus = */
            recordstatus = 0
        WHERE
            a.recordstatus = :v_tempRecStat
            AND a.filename = :v_inbound_cfg_parameter.file_name
            AND a.filedate = :v_inbound_cfg_parameter.file_date;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: EXCEPTION WHEN no_data_found THEN */
END