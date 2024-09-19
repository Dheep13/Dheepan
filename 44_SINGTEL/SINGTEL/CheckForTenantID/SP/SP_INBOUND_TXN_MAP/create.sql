CREATE PROCEDURE EXT.SP_INBOUND_TXN_MAP
(
    IN p_filetype VARCHAR(255) DEFAULT NULL,   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                               /* ORIGSQL: p_filetype IN VARCHAR2 DEFAULT NULL */
    IN p_filename VARCHAR(255) DEFAULT NULL,   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                               /* ORIGSQL: p_filename IN VARCHAR2 DEFAULT NULL */
    IN p_filedate TIMESTAMP DEFAULT NULL,   /* ORIGSQL: p_filedate IN DATE DEFAULT NULL */
    IN p_stage DECIMAL(38,10) DEFAULT NULL     /* ORIGSQL: p_stage IN number default null */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    -- DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_STRING_1 VARCHAR(5000); /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_sql VARCHAR(31000);  /* ORIGSQL: v_sql VARCHAR2(31000); */
    DECLARE v_disable VARCHAR(31000);  /* ORIGSQL: v_disable VARCHAR2(31000); */
    DECLARE v_insert_cls VARCHAR(31000);  /* ORIGSQL: v_insert_cls VARCHAR2(31000); */
    DECLARE v_select_cls VARCHAR(31000);  /* ORIGSQL: v_select_cls VARCHAR2(31000); */
    DECLARE v_from_cls VARCHAR(400);  /* ORIGSQL: v_from_cls VARCHAR2(400); */
    DECLARE v_where_cls VARCHAR(31000);  /* ORIGSQL: v_where_cls VARCHAR2(31000); */
    DECLARE v_comma VARCHAR(20) = ' ,';  /* ORIGSQL: v_comma VARCHAR(20) := ',' ; */
    DECLARE v_open_braces VARCHAR(20) = ' ( ';  /* ORIGSQL: v_open_braces VARCHAR(20) := ' (' ; */
    DECLARE v_close_braces VARCHAR(20) = ' ) ';  /* ORIGSQL: v_close_braces VARCHAR(20) := ') ' ; */
    DECLARE v_single_quote VARCHAR(20) = '''';  /* ORIGSQL: v_single_quote VARCHAR(20) := ''''; */
    DECLARE v_source_table VARCHAR(40) = 'INBOUND_DATA_STAGING';  /* ORIGSQL: v_source_table VARCHAR(40) := 'INBOUND_DATA_STAGING'; */
    DECLARE v_rowcount DECIMAL(38,10);  /* ORIGSQL: v_rowcount NUMBER; */
    DECLARE v_proc_name VARCHAR(30) = 'SP_INBOUND_TXN_MAP';  /* ORIGSQL: v_proc_name VARCHAR(30) := 'SP_INBOUND_TXN_MAP'; */
    DECLARE v_parameter ROW LIKE ext.Inbound_cfg_Parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm VARCHAR2(4000); */
    DECLARE v_stageNumber DECIMAL(38,10);  /* ORIGSQL: v_stageNumber number; */
    DECLARE v_error_flag_exists DECIMAL(38,10);  /* ORIGSQL: v_error_flag_exists number; */

    /* ORIGSQL: FOR i IN (select distinct filetype,nvl(b.tablename,a.tgttable) tgttable,sourcetable,a.tgttable txntable, nvl(b.seq,-1) seq, b.condition from inbound_Cfg_Txnfield a left outer join inbound_cfg_tgttable(...) */
    DECLARE CURSOR CUR_CFG_TXN_TGT
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_TXNFIELD' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_TGTTABLE' not found */

        SELECT   /* ORIGSQL: select distinct filetype,nvl(b.tablename,a.tgttable) tgttable,sourcetable,a.tgttable txntable, nvl(b.seq,-1) seq, b.condition from inbound_Cfg_Txnfield a left outer join inbound_cfg_tgttable b on a.tg(...) */
            DISTINCT
            filetype,
            IFNULL(b.tablename,a.tgttable) AS tgttable,  /* ORIGSQL: nvl(b.tablename,a.tgttable) */
            sourcetable,
            a.tgttable AS txntable,
            IFNULL(b.seq,-1) AS seq,  /* ORIGSQL: nvl(b.seq,-1) */
            b."CONDITION"  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'condition' (=reserved word in HANA) renamed to '"condition"'; ensure all other references are renamed accordingly */
        FROM
        -- select * from ext.inbound_cfg_tgttable
            ext.inbound_Cfg_Txnfield a
        LEFT OUTER JOIN
            ext.inbound_cfg_tgttable b/*Deepan : modified condition column name to "CONDITION"*/
            ON a.tgttable = b.tgttable
        WHERE
            UPPER(filetype) = UPPER(:v_parameter.file_type)
            AND IFNULL(stage_number,1) = :v_stageNumber  /* ORIGSQL: nvl(stage_number,1) */
        ORDER BY
            IFNULL(b.seq,-1);  /* ORIGSQL: nvl(b.seq,-1) */

    /* ORIGSQL: FOR j IN (SELECT * FROM INBOUND_CFG_TXNFIELD WHERE tgttable = i.txntable AND UPPER(filetype) = UPPER(i.filetype) and nvl(stage_number,1) = v_stageNumber ORDER BY id) LOOP IF j.filter_condition is not (...) */
    DECLARE CURSOR CUR_CFG_TXNFIELD
    FOR 
        SELECT   /* ORIGSQL: SELECT * FROM INBOUND_CFG_TXNFIELD WHERE tgttable = i.txntable AND UPPER(filetype) = UPPER(i.filetype) and nvl(stage_number,1) = v_stageNumber ORDER BY id; */
            *
        FROM
            ext.INBOUND_CFG_TXNFIELD
        WHERE
            -- tgttable = :i.txntable /*deepan: commenting this out , used the filter during the looping process*/
            -- AND UPPER(filetype) = UPPER(:i.filetype)/*deepan: commenting this out , used the filter during the looping process*/
            -- AND 
            IFNULL(stage_number,1) = :v_stageNumber  /* ORIGSQL: nvl(stage_number,1) */
        ORDER BY
            id;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            v_sqlerrm = SUBSTRING(::SQL_ERROR_MESSAGE,1,4000);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 4000) */

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'ERROR in :' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255), 'FIELDMAP Execution Error', NULL, NULL, SUBSTR(v_sqlerrm, 1, 4000)) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'ERROR in :'|| IFNULL(:p_filetype,'') || '-FileName:'|| IFNULL(:p_filename,'') || '-Date:'|| IFNULL(TO_VARCHAR(:p_filedate),''),1,255) 
                , 'FIELDMAP Execution Error', NULL, NULL, SUBSTRING(:v_sqlerrm,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || 'ERROR in :' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255) */
                );  /* ORIGSQL: SUBSTR(v_sqlerrm, 1, 4000) */

            /* ORIGSQL: raise; */
            RESIGNAL;
            --to raise the error, so that informatica job fails.

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END;

        /* ORIGSQL: execute immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
        /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
        -- CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');
        --this is to ensure that the date inserted into the data table has the full YYYY
        /* ORIGSQL: DBMS_OUTPUT.put_line ('1:' ||p_stage); */
        -- CALL SQLSCRIPT_PRINT:PRINT_LINE('1:' ||IFNULL(TO_VARCHAR(:p_stage),''));

        IF :p_filetype IS NULL
        OR :p_filename IS NULL
        OR :p_filedate IS NULL
        THEN
            /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

            SELECT
                DISTINCT
                *
            INTO
                v_parameter
            FROM
                ext.Inbound_cfg_Parameter
            WHERE
                object_name = :v_proc_name;

            v_stageNumber = 1;
        ELSE 
            v_parameter.file_type = :p_filetype;

            v_parameter.file_name = :p_filename;

            v_parameter.file_date = :p_filedate;

            v_stageNumber = :p_stage;
        END IF;

        /* ORIGSQL: DBMS_OUTPUT.put_line ('2'); */
        -- CALL SQLSCRIPT_PRINT:PRINT_LINE('2');

        FOR i AS CUR_CFG_TXN_TGT
        DO
            /* ORIGSQL: DBMS_OUTPUT.put_line ('2:' || i.tgttable || ' ' ||i.seq); */
            -- CALL SQLSCRIPT_PRINT:PRINT_LINE('2:' || IFNULL(:i.tgttable,'') || ' ' ||IFNULL(TO_VARCHAR(:i.seq),''));

            v_disable = 'update ' 
            || IFNULL(:i.tgttable,'')
            || ' set recordstatus=-1, filename=filename||''OLD'' where trim(filename)='''
            || IFNULL(TRIM(:p_filename),'')
            || '''  ';

            --Arjun 0315 Removed filedate criteria to avoid the same file getting loaded mutliple times into STEL_DATA tables

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || ' Disable Old Data done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255), 'FIELDMAP Disable Query Created', NULL, NULL, SU(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || ' Disable Old Data done for FileType:'|| IFNULL(:p_filetype,'') || '-FileName:'|| IFNULL(:p_filename,'') || '-Date:'|| IFNULL(TO_VARCHAR(:p_filedate),''),1,255) 
                , 'FIELDMAP Disable Query Created', NULL, NULL, SUBSTRING(:v_disable,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || ' Disable Old Data done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255) */
                );  /* ORIGSQL: SUBSTR(v_disable, 1, 4000) */

            v_select_cls = ' Select ';

            v_insert_cls = 'Insert Into ';

            v_from_cls = ' from ' || IFNULL(:i.sourcetable,'');

            v_where_cls = ' Where 1=1 and  '||IFNULL(:i."CONDITION",'1=1 ') ||' and FILETYPE = '''   /* ORIGSQL: nvl(i.condition,'1=1 ') */
            || IFNULL(:v_parameter.file_type,'')
            || ''' and FILENAME = '''
            || IFNULL(:v_parameter.file_name,'')
            || ''' and FILEDATE = to_Date('''
                || IFNULL(TO_VARCHAR(:v_parameter.file_date,'YYYYMMDD'),'')  /* ORIGSQL: TO_CHAR(v_parameter.file_date, 'YYYYMMDD') */
            || ''',''YYYYMMDD'') ';
            BEGIN 
                DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
                    /* ORIGSQL: when no_data_found then */
                    BEGIN
                       v_error_flag_exists = 0;
                    END;



                SELECT
                    COUNT(*)
                INTO
                    v_error_flag_exists
                FROM
                    SYS.TABLE_COLUMNS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_TAB_COLS': verify conversion */
                                       /* ORIGSQL: all_tab_cols (Oracle catalog) */
                WHERE
                    TABLE_NAME = :i.sourcetable  /* ORIGSQL: table_name (column in Oracle catalog 'ALL_TAB_COLS') */
                    AND COLUMN_NAME = 'ERROR_FLAG';  /* ORIGSQL: column_name (column in Oracle catalog 'ALL_TAB_COLS') */

                /* ORIGSQL: exception when no_data_found then */
            END;

            IF :v_error_flag_exists > 0
            THEN
                v_where_cls = IFNULL(:v_where_cls,'') || ' and nvl(error_flag,0) =0  ';
            END IF;
            -- to_date(''' v_parameter.file_date  || ''',''DD-MON-YY'')';

            v_insert_cls = IFNULL(:v_insert_cls,'') || IFNULL(:i.tgttable,'') || IFNULL(:v_open_braces,'');

            /* ORIGSQL: DBMS_OUTPUT.put_line ('3'); */
            -- CALL SQLSCRIPT_PRINT:PRINT_LINE('3');

            FOR j AS CUR_CFG_TXNFIELD
            DO
                /* ORIGSQL: DBMS_OUTPUT.put_line ('3: in loop'); */
                -- CALL SQLSCRIPT_PRINT:PRINT_LINE('3: in loop');
            IF :j.tgttable = :i.txntable  AND UPPER(:j.filetype) = UPPER(:i.filetype)/*deepan: using the filter during the looping process*/
            THEN
                v_insert_cls = IFNULL(:v_insert_cls,'') || IFNULL(:j.tgtfield,'') || IFNULL(:v_comma,'');

                v_select_cls = IFNULL(:v_select_cls,'')
                ||
                CASE
                    WHEN ((:j.sourcefield IS NOT NULL
                        AND :j.genericexpression IS NULL)
                        OR (:j.sourcefield IS NULL
                        AND :j.genericexpression IS NULL)
                    )
                    THEN
                    CASE
                        WHEN UPPER(:j.DATATYPE) = 'NUMBER' 
                        THEN 'to_number'
                        || IFNULL(:v_open_braces,'')
                        || IFNULL(:j.sourcefield, 'NULL')  /* ORIGSQL: NVL(j.sourcefield, 'NULL') */
                        || IFNULL(:v_close_braces,'')
                        WHEN UPPER(:j.DATATYPE) = 'DATE' 
                        THEN 'to_date'
                        || IFNULL(:v_open_braces,'')
                        || IFNULL(:j.sourcefield, 'NULL')  /* ORIGSQL: NVL(j.sourcefield, 'NULL') */
                        || IFNULL(:v_comma,'')
                        || IFNULL(:v_single_quote,'')
                        || IFNULL(:j.date_format,'')
                        || IFNULL(:v_single_quote,'')
                        || IFNULL(:v_close_braces,'')
                        ELSE IFNULL(:j.sourcefield, 'NULL')  /* ORIGSQL: NVL(j.sourcefield, 'NULL') */
                    END
                END
                --            || CASE
                    --                  WHEN j.genericexpression IS NOT NULL THEN ' || '
                    --                  ELSE NULL
                    --               END
                    ||
                    CASE
                        WHEN :j.sourcefield IS NULL
                        AND :j.genericexpression IS NOT NULL
                        THEN
                        CASE
                            WHEN UPPER(:j.DATATYPE) = 'NUMBER' 
                            THEN 'to_number'
                            || IFNULL(:v_open_braces,'')
                            ||IFNULL(:j.genericexpression,'')
                            || IFNULL(:v_close_braces,'')
                            WHEN UPPER(:j.DATATYPE) = 'DATE' 
                            THEN 'to_date'
                            || IFNULL(:v_open_braces,'')
                            || IFNULL(:j.genericexpression,'')
                            || IFNULL(:v_comma,'')
                            || IFNULL(:v_single_quote,'')
                            || IFNULL(:j.date_format,'')
                            || IFNULL(:v_single_quote,'')
                            || IFNULL(:v_close_braces,'')
                            ELSE :j.genericexpression
                        END
                    END
                    || ' as '
                    || IFNULL(:j.tgtfield,'')
                    || IFNULL(:v_comma,'');

                IF :j.filter_condition  IS NOT NULL
                THEN
                    v_where_cls = IFNULL(:v_where_cls,'') || 'AND '|| IFNULL(:j.filter_condition,'') ||' ';
                END IF;
             END IF;/* Deepan Ending this IF condition :j.tgttable = :i.txntable  AND UPPER(:j.filetype) = UPPER(:i.filetype)*/
            END FOR;  /* ORIGSQL: END LOOP; */
        
            
            v_insert_cls = IFNULL(substring(:v_insert_cls,1,LENGTH(:v_insert_cls) - 1),'')  /* ORIGSQL: SUBSTR(v_insert_cls, 1, LENGTH (v_insert_cls) - 1) */
            || IFNULL(:v_close_braces,'');

            v_select_cls = substring(:v_select_cls,1,LENGTH(:v_select_cls) - 1);  /* ORIGSQL: SUBSTR(v_select_cls, 1, LENGTH (v_select_cls) - 1) */

            v_sql = IFNULL(:v_insert_cls,'') || IFNULL(:v_select_cls,'') || IFNULL(:v_from_cls,'') || IFNULL(:v_where_cls,'');

            /* ORIGSQL: DBMS_OUTPUT.put_line ('4'); */
            -- CALL SQLSCRIPT_PRINT:PRINT_LINE('4');

            /* ORIGSQL: DBMS_OUTPUT.put_line (v_sql); */
            -- CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_sql);

            /* ORIGSQL: DBMS_OUTPUT.put_line(SUBSTR (v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255)); */
            -- CALL SQLSCRIPT_PRINT:PRINT_LINE(SUBSTRING(IFNULL(:v_proc_name,'') || ' FieldMap done for FileType:' || IFNULL(:p_filetype,'') || '-FileName:' || IFNULL(:p_filename,'') || '-Date:' || IFNULL(TO_VARCHAR(:p_filedate),''),1,255));  /* ORIGSQL: SUBSTR(v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255) */

            --  DBMS_OUTPUT.put_line(    SUBSTR (v_sql, 1, 4000));

            /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255), 'FIELDMAP Created', NULL, NULL, SUBSTR(v_sql, 1, 4000)) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || ' FieldMap done for FileType:'|| IFNULL(:p_filetype,'') || '-FileName:'|| IFNULL(:p_filename,'') || '-Date:'|| IFNULL(TO_VARCHAR(:p_filedate),''),1,255) 
                , 'FIELDMAP Created', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255) */
                );  /* ORIGSQL: SUBSTR(v_sql, 1, 4000) */

            /* ORIGSQL: DBMS_OUTPUT.put_line ('5'); */
            -- CALL SQLSCRIPT_PRINT:PRINT_LINE('5');

            /* ORIGSQL: DBMS_OUTPUT.put_line ('Before v_disable. seq = ' ||i.seq); */
            -- CALL SQLSCRIPT_PRINT:PRINT_LINE('Before');
            CALL SQLSCRIPT_PRINT:PRINT_LINE('Before v_disable. seq = ' ||IFNULL(TO_VARCHAR(:i.seq),''));

            IF :i.seq = -1
            THEN
                /* ORIGSQL: DBMS_OUTPUT.put_line ('Executing v_disable. seq = ' ||i.seq); */
                CALL SQLSCRIPT_PRINT:PRINT_LINE('Executing v_disable. seq = ' ||IFNULL(TO_VARCHAR(:i.seq),''));
                BEGIN 
                    DECLARE EXIT HANDLER FOR SQLEXCEPTION
                        /* ORIGSQL: when others then */
                        -- BEGIN
                        --     /* ORIGSQL: NULL; */
                        --     DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
                        -- END;

                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: EXECUTE IMMEDIATE v_disable; */
                    EXECUTE IMMEDIATE :v_disable;

                    /* ORIGSQL: exception when others then */
                END;
            END IF;

            /* ORIGSQL: DBMS_OUTPUT.put_line ('6'); */
            CALL SQLSCRIPT_PRINT:PRINT_LINE('6');

            /* ORIGSQL: execute immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
            /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
            -- CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

            /* ORIGSQL: dbms_output.put_line (v_parameter.file_date); */
            CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_parameter.file_date);

            /* ORIGSQL: dbms_output.put_line (:v_parameter.file_name); */
            CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_parameter.file_name);

            /* ORIGSQL: dbms_output.put_line (:v_parameter.file_type); */
            CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_parameter.file_type);

            /* ORIGSQL: dbms_output.put_line (v_sql); */
            CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_sql);
            BEGIN 
                DECLARE EXIT HANDLER FOR SQLEXCEPTION
                    /* ORIGSQL: when others then */
                    BEGIN
                        -- Balaji Error Capture - Mar 13 2019

                        v_sqlerrm = SUBSTRING(::SQL_ERROR_MESSAGE,1,4000);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 4000) */

                        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'ERROR in :' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255), 'FIELDMAP Execution Error', NULL, NULL, SUBSTR(v_sqlerrm, 1, 4000)) */
                        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'ERROR in :'|| IFNULL(:p_filetype,'') || '-FileName:'|| IFNULL(:p_filename,'') || '-Date:'|| IFNULL(TO_VARCHAR(:p_filedate),''),1,255) 
                            , 'FIELDMAP Execution Error', NULL, NULL, SUBSTRING(:v_sqlerrm,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || 'ERROR in :' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255) */
                            );  /* ORIGSQL: SUBSTR(v_sqlerrm, 1, 4000) */

                        /* ORIGSQL: raise; */
                        RESIGNAL;
                        --to raise the error, so that informatica job fails.

                        /* ORIGSQL: COMMIT; */
                        COMMIT;

                        -- Balaji Error Capture - Mar 13 2019

                        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                        /* ORIGSQL: EXECUTE IMMEDIATE v_sql ; */
                        EXECUTE IMMEDIATE :v_sql;

                        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

                        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255), 'FIELDMAP Execution Completed', v_rowcount, NULL, SUBSTR(...) */
                        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || ' FieldMap done for FileType:'|| IFNULL(:p_filetype,'') || '-FileName:'|| IFNULL(:p_filename,'') || '-Date:'|| IFNULL(TO_VARCHAR(:p_filedate),''),1,255) 
                            , 'FIELDMAP Execution Completed', :v_rowcount, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255) */
                            );  /* ORIGSQL: SUBSTR(v_sql, 1, 4000) */
                    END;



                /*EXECUTE IMMEDIATE v_sql
                   USING :v_parameter.file_date, :v_parameter.file_name;*/
                --added by kyap, bugfix as HRCentral-SCII-LeaveDetails does not require binding to file_date and file_name; INBOUND_DATA_LEAVE table does not have file_date and file_name field

                IF :v_parameter.file_type <> 'HRCentral-SCII-LeaveDetails' 
                THEN
                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: EXECUTE IMMEDIATE v_sql USING v_parameter.file_date, v_parameter.file_name; */
                    -- CALL sapdbmtk.sp_dbmtk_prepare_execute_sql(:v_sql, :DBMTK_TMPVAR_STRING_1);
                    EXECUTE IMMEDIATE :v_sql USING :v_parameter.file_date, :v_parameter.file_name;
                ELSE 
                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
                    EXECUTE IMMEDIATE :v_sql;
                END IF;

                v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

                /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255), 'FIELDMAP Execution Completed', v_rowcount, NULL, SUBSTR(...) */
                CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || ' FieldMap done for FileType:'|| IFNULL(:p_filetype,'') || '-FileName:'|| IFNULL(:p_filename,'') || '-Date:'|| IFNULL(TO_VARCHAR(:p_filedate),''),1,255) 
                    , 'FIELDMAP Execution Completed', :v_rowcount, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || ' FieldMap done for FileType:' || p_FILETYPE || '-FileName:' || p_filename || '-Date:' || p_filedate, 1, 255) */
                    );  /* ORIGSQL: SUBSTR(v_sql, 1, 4000) */

                /* ORIGSQL: COMMIT; */
                COMMIT;

                /* ORIGSQL: exception when others then */
            END;
        END FOR;  /* ORIGSQL: END LOOP; */

        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END