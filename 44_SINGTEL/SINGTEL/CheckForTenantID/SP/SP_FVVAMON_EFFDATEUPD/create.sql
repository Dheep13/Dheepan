CREATE PROCEDURE EXT.SP_FVVAMON_EFFDATEUPD
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

    /* ORIGSQL: FOR i IN 1..12-v_diff LOOP */
    DECLARE i_dbmtk_loopvar_1 INT;

    /* ORIGSQL: FOR i IN 12-v_diff+1..12 LOOP */
    DECLARE i_dbmtk_loopvar_2 INT;

    /* ORIGSQL: FOR r IN (SELECT * FROM inbound_data_fvvamon tgt WHERE tgt.recordstatus =v_tempR(...) */
    DECLARE CURSOR dbmtk_cursor_1126
    FOR 
        SELECT   /* ORIGSQL: SELECT * FROM inbound_data_fvvamon tgt WHERE tgt.recordstatus =v_tempRecStat AND(...) */
            *
        FROM
            inbound_data_fvvamon tgt
        WHERE
            tgt.recordstatus = :v_tempRecStat
            AND filename = :v_inbound_cfg_parameter.file_name
            AND filedate = :v_inbound_cfg_parameter.file_date
            AND tgt.period01_startdate <> tgt.initialperiod;

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        /* ORIGSQL: WHEN no_data_found THEN */
        BEGIN
            /* ORIGSQL: NULL; */
            DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
        END;

        /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
        /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
       -- CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

        SELECT *
        INTO
            v_inbound_cfg_parameter
        FROM
            EXT.INBOUND_CFG_PARAMETER;

        /* ORIGSQL: dbms_output.put_line('Started'); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Started');   

        /* ORIGSQL: UPDATE inbound_data_fvvamon a SET initialperiod =period01_startdate, recordstatu(...) */
        UPDATE ext.inbound_data_fvvamon a
            SET
            /* ORIGSQL: initialperiod = */
            initialperiod = period01_startdate,
            /* ORIGSQL: recordstatus = */
            recordstatus = :v_tempRecStat
        WHERE
            a.recordstatus = 0
            AND a.filename = :v_inbound_cfg_parameter.file_name
            AND a.filedate = :v_inbound_cfg_parameter.file_date;

        --dbms_output.put_line('First Update' || SQL%ROWCOUNT || ' ' ||v_inbound_cfg_parameter.file_date || ' '|| :v_inbound_cfg_parameter.file_name);

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge INTO inbound_data_fvvamon tgt USING (SELECT a.assigneename, a.initialperio(...) */
        MERGE INTO ext.inbound_data_fvvamon AS tgt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_FVVAMON' not found */
            USING
            (
                SELECT   /* ORIGSQL: (SELECT a.assigneename, a.initialperiod,posFirst.effectivestartdate firsteffdate(...) */
                    a.assigneename,
                    a.initialperiod,
                    posFirst.effectivestartdate AS firsteffdate,
                    CASE
                        WHEN TO_VARCHAR(posFirst.effectivestartdate,'DD') = '01'  /* ORIGSQL: TO_CHAR(posFirst.effectivestartdate,'DD') */
                        THEN posFirst.effectivestartdate
                        ELSE TO_DATE(ADD_SECONDS(LAST_DAY(posFirst.effectivestartdate),(86400*1)))   /* ORIGSQL: LAST_DAY(posFirst.effectivestartdate) +1 */
                    END
                    AS startdate
                FROM
                    ext.inbound_data_fvvamon a
                LEFT OUTER JOIN
                    cs_position pos
                    ON pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND pos.name = a.assigneename
                    AND a.period01_startdate BETWEEN pos.effectivestartdate AND ADD_DAYS(pos.effectiveenddate,-1)
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
            ON (tgt.assigneename = src.assigneename
                AND tgt.initialperiod = src.initialperiod
            	AND tgt.recordstatus = :v_tempRecStat
                AND filename = :v_inbound_cfg_parameter.file_name
                AND filedate = :v_inbound_cfg_parameter.file_date
            )
        WHEN MATCHED THEN
            UPDATE
                SET tgt.period01_startdate = src.startdate, tgt.firsteffdate = src.firsteffdate
            --WHERE
                --tgt.recordstatus = :v_tempRecStat
                --AND filename = :v_inbound_cfg_parameter.file_name
                --AND filedate = :v_inbound_cfg_parameter.file_date
                ;

        /* ORIGSQL: dbms_output.put_line('Merge 1' || SQL%ROWCOUNT); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Merge 1'|| ::ROWCOUNT);  

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

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_PLVA' not found */

        /* ORIGSQL: insert into inbound_data_plva select distinct a.filedate, a.filename, 0, a.downl(...) */
        INSERT INTO ext.inbound_data_plva
            SELECT   /* ORIGSQL: select distinct a.filedate, a.filename, 0, a.downloaded, a.assigneename, a.assig(...) */
                DISTINCT
                a.filedate,
                a.filename,
                0,
                a.downloaded,
                a.assigneename,
                a.assigneetype,
                b.effectivestartdate,
                a.variablename,
                a.fixedvaluename,
                'FixedValue' AS RuleElemType,
                ROW_NUMBER() OVER (PARTITION BY a.assigneename ORDER BY effectivestartdate),
                ROW_NUMBER() OVER (PARTITION BY a.assigneename ORDER BY effectivestartdate)
            FROM
                ext.inbound_data_fvvamon a
            INNER JOIN
                cs_position b
                ON b.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND b.name = a.assigneename
                -- AND (b.effectivestartdate, Add_Days(b.effectiveenddate,-1))
                -- overlaps
                -- (a.period01_startdate, ADD_MONTHS(TO_DATE((IFNULL(TO_VARCHAR(a.period01_startdate,'YYYY'),'') ||'0331'), 'YYYYMMDD'),12))  /* ORIGSQL: to_Char(a.period01_startdate,'YYYY') */
                -- /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            AND (
			    (b.effectivestartdate BETWEEN a.period01_startdate AND ADD_MONTHS(TO_DATE((TO_CHAR(a.period01_startdate, 'YYYY') || '0331'), 'YYYYMMDD'), 12) )
			    OR
			    (ADD_DAYS(b.effectiveenddate,-1) BETWEEN a.period01_startdate AND ADD_MONTHS(TO_DATE((TO_CHAR(a.period01_startdate, 'YYYY') || '0331'), 'YYYYMMDD'), 12) )
			    -- OR
			    -- (a.period01_startdate BETWEEN b.effectivestartdate AND ADD_DAYS(b.effectiveenddate, -1))
			    -- OR
			    -- (ADD_MONTHS(TO_DATE((TO_VARCHAR(a.period01_startdate, 'YYYY') || '0331'), 'YYYYMMDD'), 12)  BETWEEN b.effectivestartdate AND ADD_DAYS(b.effectiveenddate,-1))
			   )


            WHERE
                a.recordstatus = :v_tempRecStat
                AND a.filename = :v_inbound_cfg_parameter.file_name
                AND a.filedate = :v_inbound_cfg_parameter.file_date;

        /* ORIGSQL: dbms_output.put_line('Insert into plva: '||SQL%ROWCOUNT); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Insert into plva: '||::ROWCOUNT);  

        FOR r AS dbmtk_cursor_1126
        DO
            v_diff = ABS(MONTHS_BETWEEN(:r.period01_startdate, :r.initialperiod));

            IF :v_diff <= 12
            THEN
                /* ORIGSQL: dbms_output.put_line('Loop 1'); */
                --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Loop 1');

                FOR i_dbmtk_loopvar_1 IN 1 .. 12 - TO_INT(:v_diff)
                DO
                    /* ORIGSQL: dbms_output.put_line('Loop 3:'||i); */
                    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Loop 3:'||IFNULL(TO_VARCHAR(:i_dbmtk_loopvar_1),''));

                    iTxt = LPAD(:i_dbmtk_loopvar_1,2,'0');

                    jTxt = LPAD(:i_dbmtk_loopvar_1+:v_diff,2,'0');

                    v_Sql = 'update inbound_data_fvvamon tgt  set PERIOD'||IFNULL(:iTxt,'')||'_VALUE  =  PERIOD'||IFNULL(:jTxt,'')||'_VALUE
                    where tgt.assigneename='''||IFNULL(:r.assigneename,'')||''' and tgt.initialperiod = '''||IFNULL(:r.initialperiod,'')||'''
                    and tgt.recordstatus= '||IFNULL(TO_VARCHAR(:v_tempRecStat),'')||' ';

                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: execute immediate v_sql; */
                    EXECUTE IMMEDIATE :v_Sql;

                    /* ORIGSQL: dbms_output.put_line(v_sql); */
                    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_Sql);
                END FOR;  /* ORIGSQL: END LOOP; */

                i = 0;

                FOR i_dbmtk_loopvar_2 IN 12-TO_INT(:v_diff)+1 .. 12
                DO
                    /* ORIGSQL: dbms_output.put_line('Loop 2:'||i); */
                    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Loop 2:'||IFNULL(TO_VARCHAR(:i_dbmtk_loopvar_2),''));

                    iTxt = LPAD(:i_dbmtk_loopvar_2,2,'0');

                    v_Sql = 'update inbound_data_fvvamon tgt  set PERIOD'||IFNULL(:iTxt,'')||'_VALUE  =  null
                    where tgt.assigneename='''||IFNULL(r.assigneename,'')||''' and tgt.initialperiod = '''||IFNULL(r.initialperiod,'')||'''
                    and tgt.recordstatus= '||IFNULL(TO_VARCHAR(:v_tempRecStat),'')||' ';

                    /* ORIGSQL: dbms_output.put_line(v_sql); */
                    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_Sql);

                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: execute immediate v_sql; */
                    EXECUTE IMMEDIATE :v_Sql;
                END FOR;  /* ORIGSQL: end loop; */
            END IF;
        END FOR;  /* ORIGSQL: END LOOP; */

        /* ORIGSQL: UPDATE inbound_data_fvvamon a SET initialperiod =period01_startdate, businessuni(...) */
        UPDATE ext.inbound_data_fvvamon a
            SET
            /* ORIGSQL: initialperiod = */
            initialperiod = period01_startdate,
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