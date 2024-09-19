CREATE PROCEDURE EXT.SP_INBOUND_VALIDATOR
(
    IN in_File_type VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                   /* ORIGSQL: in_File_type IN varchar */
    IN in_file_name VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                   /* ORIGSQL: in_file_name IN varchar */
    IN in_file_date TIMESTAMP,   /* ORIGSQL: in_file_date IN date */
    IN in_stage DECIMAL(38,10)   /* ORIGSQL: in_stage IN number */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_callidus_opr_type VARCHAR(255);  /* ORIGSQL: v_callidus_opr_type varchar(255); */
    DECLARE v_sql VARCHAR(31999);  /* ORIGSQL: v_sql varchar2(31999); */
    DECLARE v_dupsql VARCHAR(31999);  /* ORIGSQL: v_dupsql varchar2(31999); */
    DECLARE v_exception_table VARCHAR(255) = 'INBOUND_VALIDATION_ERRORS';  /* ORIGSQL: v_exception_table varchar(255):= 'INBOUND_VALIDATION_ERRORS'; */
    DECLARE v_proc_name VARCHAR(40) = 'SP_INBOUND_VALIDATOR';  /* ORIGSQL: v_proc_name varchar(40) := 'SP_INBOUND_VALIDATOR'; */

    --v_src_table varchar(255):='INBOUND_DATA_STAGING';
    DECLARE v_unique_key VARCHAR(5000);  /* ORIGSQL: v_unique_key varchar2(5000); */
    DECLARE x DECIMAL(38,10);  /* ORIGSQL: x number; */
    DECLARE v_single_quote VARCHAR(20) = '''';  /* ORIGSQL: v_single_quote varchar(20) :=''''; */
    DECLARE v_single_space VARCHAR(1) = ' ';  /* ORIGSQL: v_single_space varchar(1) :=' '; */
    DECLARE v_comma VARCHAR(5) = ', ';  /* ORIGSQL: v_comma varchar(5) := ', ' ; */
    DECLARE v_temp_var DECIMAL(38,10);  /* ORIGSQL: v_temp_var number; */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm varchar2(4000); */
    DECLARE v_groupby VARCHAR(31999);  /* ORIGSQL: v_groupby varchar2(31999); */
    DECLARE v_groupbyfields VARCHAR(31999);  /* ORIGSQL: v_groupbyfields varchar2(31999); */
    DECLARE v_sortcolumn VARCHAR(31999);  /* ORIGSQL: v_sortcolumn varchar2(31999); */
    DECLARE v_sql_errorcode VARCHAR(4000);  /* ORIGSQL: v_sql_errorcode varchar2(4000); */
    DECLARE v_cnt DECIMAL(38,10);  /* ORIGSQL: v_cnt number; */
    DECLARE v_dup_tab_name VARCHAR(255);  /* ORIGSQL: v_dup_tab_name varchar(255); */

    /* ORIGSQL: for r in (select distinct callidus_operation_type from Inbound_cfg_inb_callidus (...) */
    DECLARE CURSOR cursor_cfg_inb_callidus
    FOR 
        SELECT   /* ORIGSQL: select distinct callidus_operation_type from Inbound_cfg_inb_callidus where inbo(...) */
            DISTINCT
            callidus_operation_type
        FROM
            ext.Inbound_cfg_inb_callidus
        WHERE
            inbound_file_type = :in_File_type;

    -- Data Validations
    /* ORIGSQL: for i in (SELECT * FROM inbound_cfg_Validator_checks where flag=1 and nvl(in_cla(...) */
    DECLARE CURSOR cursor_cfg_Validator_checks
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_VALIDATOR_CHECKS' not found */

        SELECT   /* ORIGSQL: SELECT * FROM ext.Inbound_cfg_Validator_checks where flag=1 and nvl(in_clause_flag,0(...) */
            *
        FROM
            ext.Inbound_cfg_Validator_checks
        WHERE
            flag = 1
            AND IFNULL(in_clause_flag,0) = 0  /* ORIGSQL: nvl(in_clause_flag,0) */
            AND TRIM(validation_type) = TRIM(:v_callidus_opr_type);

    /* ORIGSQL: for j in (select distinct filetype, nvl(sourcefield,genericexpression) srcfield,(...) */
    DECLARE CURSOR CURSOR_CFG_TXNFIELD_Y
    FOR 
        SELECT   /* ORIGSQL: select distinct filetype, nvl(sourcefield,genericexpression) srcfield,sourcetabl(...) */
            DISTINCT
            filetype,
            IFNULL(sourcefield,genericexpression) AS srcfield,  /* ORIGSQL: nvl(sourcefield,genericexpression) */
            sourcetable
        FROM
            ext.Inbound_CFG_TXNFIELD
        WHERE
            FILETYPE = :in_File_type
            AND IFNULL(stage_number,1) = :in_stage  /* ORIGSQL: nvl(stage_number,1) */
            AND DUPLICATECHECK = 'Y'
            AND IFNULL(sourcefield,genericexpression) IS NOT NULL;  /* ORIGSQL: nvl(sourcefield,genericexpression) */

    /* ORIGSQL: for j in (select distinct filetype,nvl(sourcefield,genericexpression) sourcefiel(...) */
    DECLARE CURSOR CURSOR_CFG_TXNFIELD
    FOR 
        SELECT   /* ORIGSQL: select distinct filetype,nvl(sourcefield,genericexpression) sourcefield,datatype(...) */
            DISTINCT
            filetype,
            IFNULL(sourcefield,genericexpression) AS sourcefield,  /* ORIGSQL: nvl(sourcefield,genericexpression) */
            datatype,
            date_format,
            remark,
            sourcetable
        FROM
            ext.INBOUND_CFG_TXNFIELD
        WHERE
            FILETYPE = :in_File_type
            AND IFNULL(stage_number,1) = :in_stage  /* ORIGSQL: nvl(stage_number,1) */
            AND NULLABLE = 'N'
            AND IFNULL(sourcefield,genericexpression) IS NOT NULL;  /* ORIGSQL: nvl(sourcefield,genericexpression) */

    /* ORIGSQL: for j in (select distinct filetype,nvl(sourcefield,genericexpression) sourcefiel(...) */
    DECLARE CURSOR CURSOR_CFG_TXNFIELD_NUM_BOOL
    FOR 
        SELECT   /* ORIGSQL: select distinct filetype,nvl(sourcefield,genericexpression) sourcefield,datatype(...) */
            DISTINCT
            filetype,
            IFNULL(sourcefield,genericexpression) AS sourcefield,  /* ORIGSQL: nvl(sourcefield,genericexpression) */
            datatype,
            date_format,
            remark,
            sourcetable
        FROM
            ext.Inbound_CFG_TXNFIELD
        WHERE
            FILETYPE = :in_File_type
            AND IFNULL(stage_number,1) = :in_stage  /* ORIGSQL: nvl(stage_number,1) */
            AND DATATYPE IN ('NUMBER','BOOL')
            AND (SOURCEFIELD IS NOT NULL
            OR GENERICEXPRESSION IS NOT NULL);

    /* ORIGSQL: for j in (select distinct filetype,nvl(sourcefield,genericexpression) sourcefiel(...) */
    DECLARE CURSOR CURSOR_CFG_TXNFIELD_DATE
    FOR 
        SELECT   /* ORIGSQL: select distinct filetype,nvl(sourcefield,genericexpression) sourcefield,datatype(...) */
            DISTINCT
            filetype,
            IFNULL(sourcefield,genericexpression) AS sourcefield,  /* ORIGSQL: nvl(sourcefield,genericexpression) */
            datatype,
            date_format,
            remark,
            sourcetable
        FROM
            ext.Inbound_CFG_TXNFIELD
        WHERE
            FILETYPE = :in_File_type
            AND IFNULL(stage_number,1) = :in_stage  /* ORIGSQL: nvl(stage_number,1) */
            AND DATATYPE = 'DATE'
            AND (SOURCEFIELD IS NOT NULL
            OR genericexpression IS NOT NULL)
            AND IFNULL(genericexpression,'XXX') NOT LIKE ':%';  /* ORIGSQL: nvl(genericexpression,'XXX') */

    -- Business Data Validations (Products, categories and Classifiers)

    /* ORIGSQL: for i in (SELECT * FROM inbound_cfg_Validator_checks where flag=1 and nvl(in_cla(...) */
    DECLARE CURSOR cursor_cfg_Validator_flag12
    FOR 
        SELECT   /* ORIGSQL: SELECT * FROM inbound_cfg_Validator_checks where flag=1 and nvl(in_clause_flag,0(...) */
            *
        FROM
            ext.Inbound_cfg_Validator_checks
        WHERE
            flag = 1
            AND IFNULL(in_clause_flag,0) IN (1,2)  /* ORIGSQL: nvl(in_clause_flag,0) */
            AND TRIM(validation_type) = TRIM(:v_callidus_opr_type);

    -- Update the Error Details on Staging table

    /* ORIGSQL: for i in (SELECT distinct UNIQUE_KEY FROM inbound_cfg_Validator_checks where fla(...) */
    DECLARE CURSOR cursor_cfg_Validator_flag
    FOR 
        SELECT   /* ORIGSQL: SELECT distinct UNIQUE_KEY FROM inbound_cfg_Validator_checks where flag=1 and TR(...) */
            DISTINCT
            UNIQUE_KEY
        FROM
            ext.Inbound_cfg_Validator_checks
        WHERE
            flag = 1
            AND TRIM(validation_type)
            /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_INB_CALLIDUS' not found */
            IN
            (
                SELECT   /* ORIGSQL: (select TRIM(callidus_operation_type) from Inbound_cfg_inb_callidus where inboun(...) */
                    TRIM(callidus_operation_type)
                FROM
                    ext.Inbound_cfg_inb_callidus
                WHERE
                    inbound_file_type = :in_File_type
            );

    /* ORIGSQL: for q in (select nvl(min(nvl(sourcefield, genericexpression)),'SEQ') sortcolumn,(...) */
    DECLARE CURSOR cursor_cfG_txnfield_dup_check
    FOR 
        SELECT   /* ORIGSQL: select nvl(min(nvl(sourcefield, genericexpression)),'SEQ') sortcolumn,sourcetabl(...) */
            IFNULL(MIN(IFNULL(sourcefield, genericexpression)),'SEQ') AS sortcolumn,  
            sourcetable
            /* --into v_sortcolumn  */
        FROM
            ext.Inbound_cfG_txnfield
        WHERE
            duplicatecheck = 'SORT'
            AND IFNULL(stage_number,1) = :in_stage  /* ORIGSQL: nvl(stage_number,1) */
            AND filetype = :in_File_type
        GROUP BY
            sourcetable;

    /* ORIGSQL: execute immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    -- CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* ORIGSQL: dbms_output.put_line('***********START VALIDATOR******************'); */
    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('***********START VALIDATOR******************');

    /* ORIGSQL: dbms_output.put_line('***********************8*******************'||in_Stage); */
    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('***********************8*******************'||IFNULL(TO_VARCHAR(:in_stage),''));

    /* ORIGSQL: sp_inbound_boolean(in_File_type,in_File_name,in_File_date,in_stage) */
    CALL EXT.SP_INBOUND_BOOLEAN(:in_File_type, :in_file_name, :in_file_date, :in_stage);

    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
    /* ORIGSQL: Execute immediate 'Truncate table '||v_exception_table||' drop storage' ; */
    EXECUTE IMMEDIATE 'Truncate table '||IFNULL(:v_exception_table,'')||' drop storage';

    FOR r AS cursor_cfg_inb_callidus
    DO
        /* ORIGSQL: dbms_output.put_line('in loop ' || r.callidus_operation_Type); */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('in loop ' || IFNULL(:r.callidus_operation_Type,''));

        v_callidus_opr_type = :r.callidus_operation_type;

        FOR i AS cursor_cfg_Validator_checks
        DO
            x = EXT.get_number_ofstrings(:i.UNIQUE_KEY, '||');  /* ORIGSQL: get_number_ofstrings_dbmtknested_SP_INBOUND_VALIDATOR (i.UNIQUE_KEY,'||') */

            v_unique_key = EXT.get_unique_expression(
                    in_stage => :in_stage,
                    in_str => :i.UNIQUE_KEY,
                    in_delimiters => :x,
                    in_filetype => :in_File_type
                    );  /* ORIGSQL: get_unique_expression_dbmtknested_SP_INBOUND_VALIDATOR(i.UNIQUE_KEY,x,in_File_ty(...) */

            --dbms_output.put_line('X: ' ||x);
            --dbms_output.put_line('v_unique_key: ' || v_unique_key);
            /* ORIGSQL: dbms_output.put_line(i.validation_name); */
            -- -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:i.validation_name);

            ------------------------

            IF UPPER(:i.VALIDATION_NAME) = 'CHECK_DUPLICATE' 
            THEN
                /* ORIGSQL: dbms_output.put_line('DUPLICATE'); */
                -- -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('DUPLICATE');

                SELECT
                    COUNT(*) 
                INTO
                    v_cnt
                FROM
                    ext.Inbound_CFG_TXNFIELD
                WHERE
                    FILETYPE = :in_File_type
                    AND IFNULL(stage_number,1) = :in_stage  /* ORIGSQL: nvl(stage_number,1) */
                    AND DUPLICATECHECK = 'Y';

                FOR j AS CURSOR_CFG_TXNFIELD_Y
                DO
                    v_groupby = IFNULL(:v_groupby,'')||',' ||IFNULL(:j.srcfield,'');

                    v_groupbyfields = IFNULL(:v_groupbyfields,'')|| ' || '' , '' ||  ' || IFNULL(:j.srcfield,'');

                    v_dup_tab_name = :j.sourcetable;

                    -- START SANKAR
                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation j.srcfield'|| i.VALIDATION_NAME|| '(...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation j.srcfield'|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_DUPLICATE', NULL, NULL, SUBSTRING(:j.srcfield,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation j.srcfield'|| i.VALIDATION_NAME|| ' - for :'||(...) */
                        );  /* ORIGSQL: substr(j.srcfield,1,4000) */

                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation v_groupby'|| i.VALIDATION_NAME|| ' (...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation v_groupby'|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_DUPLICATE', NULL, NULL, SUBSTRING(:v_groupby,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation v_groupby'|| i.VALIDATION_NAME|| ' - for :'|| (...) */
                        );  /* ORIGSQL: substr(v_groupby,1,4000) */

                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation v_groupbyfields'|| i.VALIDATION_NAM(...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation v_groupbyfields'|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_DUPLICATE', NULL, NULL, SUBSTRING(:v_groupbyfields,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation v_groupbyfields'|| i.VALIDATION_NAME|| ' - for(...) */
                        );  /* ORIGSQL: substr(v_groupbyfields,1,4000) */

                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation v_dup_tab_name'|| i.VALIDATION_NAME(...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation v_dup_tab_name'|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_DUPLICATE', NULL, NULL, SUBSTRING(:v_dup_tab_name,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation v_dup_tab_name'|| i.VALIDATION_NAME|| ' - for (...) */
                        );  /* ORIGSQL: substr(v_dup_tab_name,1,4000) */

                    --END SANKAR
                END FOR;  /* ORIGSQL: end loop; */

                /* ORIGSQL: dbms_output.put_line('Group by: ' ||v_groupby ||'|'|| v_groupbyfields); */
                -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Group by: ' ||IFNULL(:v_groupby,'') ||'|'|| IFNULL(:v_groupbyfields,''));

                IF :v_cnt > 0
                THEN
                    v_groupby = SUBSTRING(:v_groupby,2,31999);  /* ORIGSQL: substr(v_groupby,2,31999) */
                    --remove the first comma

                    IF :in_File_type <> 'HRCentral-SCII-SalesmanProfile' 
                    THEN
                        v_groupbyfields = SUBSTRING(:v_groupbyfields,2,31999);  /* ORIGSQL: substr(v_groupbyfields,2,31999) */
                        --remove the first comma
                    END IF;
                    --v_groupby := replace(v_groupby, v_single_quote, v_single_quote||v_single_quote);
                    --dbms_output.put_line(v_groupby);
                    --START SANKAR
                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation v_groupby'|| i.VALIDATION_NAME|| ' (...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation v_groupby'|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_DUPLICATE', NULL, NULL, SUBSTRING(:v_groupby,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation v_groupby'|| i.VALIDATION_NAME|| ' - for :'|| (...) */
                        );  /* ORIGSQL: substr(v_groupby,1,4000) */

                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation v_groupbyfields'|| i.VALIDATION_NAM(...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation v_groupbyfields'|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_DUPLICATE', NULL, NULL, SUBSTRING(:v_groupbyfields,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation v_groupbyfields'|| i.VALIDATION_NAME|| ' - for(...) */
                        );  /* ORIGSQL: substr(v_groupbyfields,1,4000) */

                    --END SANKAR

                    v_dupsql = 'insert into ext.Inbound_Validation_errors
                    select '''|| IFNULL(:in_File_type,'')||''' , ''' ||IFNULL(:in_file_name,'')||''', ''' ||IFNULL(TO_VARCHAR(:in_file_date),'')||''',  seq
                    , ''' || IFNULL(:i.error_message,'') ||' :Key: '' '|| IFNULL(:v_groupbyfields,'') ||' || ''''
                    , ''' || IFNULL(:i.validation_name,'') ||'''
                    from '||IFNULL(:v_dup_tab_name,'') --i.source_tablename 
                    ||'  Where 1=1 and FILETYPE = '''
                    || IFNULL(:in_File_type,'')
                    || ''' and FILENAME = '''
                    || IFNULL(:in_file_name,'')
                    || ''' and FILEDATE = to_date('''
                        || IFNULL(TO_VARCHAR(:in_file_date),'')
                    || ''',''DD-MON-YY'') and
                    (' ||IFNULL(:v_groupby,'')||') in
                    (select ' || IFNULL(:v_groupby,'')||'  from '|| IFNULL(:v_dup_tab_name,'') --i.source_tablename 
                        ||' group by '||IFNULL(:v_groupby,'')||' having count(*)>1 )';

                    /* ORIGSQL: dbms_output.put_line('v_dupsql: '||v_dupSql); */
                    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('v_dupsql: '||IFNULL(:v_dupsql,''));

                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|(...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation '|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_DUPLICATE', NULL, NULL, SUBSTRING(:v_dupsql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|| in_file_n(...) */
                        );  /* ORIGSQL: substr(v_dupsql,1,4000) */

                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: execute immediate v_dupSql; */
                    EXECUTE IMMEDIATE :v_dupsql;

                    IF :in_File_type = 'HRCentral-SCII-SalesmanProfile' 
                    THEN
                        v_groupby = '';
                        --remove the first comma

                        v_groupbyfields = '';
                    END IF;
                    /*
                    for j in (select distinct filetype,sourcefield,genericexpression,datatype,date_format,remark from INBOUND_CFG_TXNFIELD  where FILETYPE=in_File_type
                        
                    )
                    loop
                    v_sql:= 'Insert into '||v_exception_table ;
                    v_sql:=v_sql || ' select filetype,filename,filedate,';
                    v_sql:=v_sql || v_unique_key ;
                    v_sql:=v_sql ||v_comma ||v_single_quote|| nvl(j.remark,j.sourcefield) || v_single_space || i.ERROR_MESSAGE || v_single_quote;
                    v_sql:=v_sql ||v_single_space|| 'from ' || i.source_tablename;
                    v_sql:=v_sql ||v_single_space|| ' Where 1=1 and FILETYPE = '''
                             || j.filetype
                             || ''' and FILENAME = '''
                             || in_file_name
                             || ''' and FILEDATE = to_date('''
                                 || in_file_date
                             || ''',''DD-MON-YY'')';
                    v_sql:=v_sql ||v_single_space|| 'and ('|| v_groupby || ') in ('|| v_dupSQL ||')';
                    
                    
                     SP_LOGGER (SUBSTR (v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|| in_file_name|| '-Date:'|| in_file_date,1,255),'Query Created',null,null,substr(v_sql,1,4000));
                    
                    execute immediate v_sql;
                    
                    
                    commit;
                    
                    dbms_output.put_line ('v_sql: ' || v_sql);
                    end loop;
                    */
                END IF;
            END IF;

            ---------------------------

            IF UPPER(:i.VALIDATION_NAME) = 'CHECK_NULLABLE' 
            THEN
                --changed to check if expression IS NULL
                /* ORIGSQL: dbms_output.put_line('NULLABLE'); */
                -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('NULLABLE');

                FOR j AS CURSOR_CFG_TXNFIELD
                DO
                    v_sql = 'Insert into '||IFNULL(:v_exception_table,'');

                    v_sql = IFNULL(:v_sql,'') || ' select filetype,filename,filedate,';

                    v_sql = IFNULL(:v_sql,'') || IFNULL(:v_unique_key,'');

                    v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_comma,'') ||IFNULL(:v_single_quote,'')|| IFNULL(:j.remark, replace(:j.sourcefield, :v_single_quote, IFNULL(:v_single_quote,'')||IFNULL(:v_single_quote,'')))  /* ORIGSQL: replace(j.sourcefield, v_single_quote, v_single_quote||v_single_quote) */
                                                                                                                                                                                                                                                    /* ORIGSQL: nvl(j.remark, replace(j.sourcefield, v_single_quote, v_single_quote||v_single_qu(...) */
                    || IFNULL(:v_single_space,'') || IFNULL(:i.ERROR_MESSAGE,'') || IFNULL(:v_single_quote,'') ||IFNULL(:v_comma,'') || IFNULL(:v_single_quote,'') || IFNULL(:v_single_space,'') || IFNULL(:i.validation_name,'') || IFNULL(:v_single_quote,'');

                    v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| 'from ' || IFNULL(:j.sourcetable,'');
                    -- i.source_tablename;

                    v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| ' Where 1=1 and FILETYPE = ''' 
                    || IFNULL(:j.filetype,'')
                    || ''' and FILENAME = '''
                    || IFNULL(:in_file_name,'')
                    || ''' and FILEDATE = to_date('''
                        || IFNULL(TO_VARCHAR(:in_file_date),'')
                    || ''',''DD-MON-YY'')';

                    v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| 'and '|| IFNULL(:j.SOURCEFIELD,'') || ' IS NULL';

                    IF :j.filetype = 'BCC-SCII-SubmittedMobileOrders' 
                    AND :j.SOURCEFIELD = 'FIELD19'
                    THEN
                        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| ' and ' || ' FIELD4 <> ''CE'' ' || ' AND '|| 'FIELD5 <> ''Cease''';
                    END IF;

                    IF :i.where_clause IS NOT NULL
                    THEN
                        v_sql = IFNULL(:v_sql,'') || ' and '|| IFNULL(:i.where_clause,'');
                    END IF;

                    /* ORIGSQL: dbms_output.put_line(v_sql); */
                    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

                    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|(...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation '|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
                        , 'QueryCreated_CHECK_NULLABLE', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|| in_file_n(...) */
                        );  /* ORIGSQL: substr(v_sql,1,4000) */

                    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
                    /* ORIGSQL: execute immediate v_sql; */
                    EXECUTE IMMEDIATE :v_sql;

                    /* ORIGSQL: commit; */
                    COMMIT;

                    --dbms_output.put_line ('v_sql: ' || v_sql);
                END FOR;  /* ORIGSQL: end loop; */
            END IF;

            IF UPPER(:i.VALIDATION_NAME) = 'CHECK_NUMBER' 
            THEN
                /* ORIGSQL: dbms_output.put_line('NUMBER'); */
                -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('NUMBER');

                FOR j AS CURSOR_CFG_TXNFIELD_NUM_BOOL
                DO
                    v_sql = 'Insert into '||IFNULL(:v_exception_table,'');

                    v_sql = IFNULL(:v_sql,'') || ' select filetype,filename,filedate,';

                    v_sql = IFNULL(:v_sql,'') || IFNULL(:v_unique_key,'');

                    --nvl(j.remark,replace(j.sourcefield, v_single_quote, v_single_quote||v_single_quote))

                    v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_comma,'') ||IFNULL(:v_single_quote,'')|| IFNULL(:j.remark,replace(:j.sourcefield, :v_single_quote, IFNULL(:v_single_quote,'')||IFNULL(:v_single_quote,''))) || IFNULL(:v_single_space,'') || IFNULL(:i.ERROR_MESSAGE,'') || IFNULL(:v_single_quote,'') ||IFNULL(:v_comma,'')|| IFNULL(:v_single_space,'') || IFNULL(:v_single_quote,'') ||IFNULL(:i.validation_name,'') || IFNULL(:v_single_quote,'');  /* ORIGSQL: replace(j.sourcefield, v_single_quote, v_single_quote||v_single_quote) */
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              /* ORIGSQL: nvl(j.remark,replace(j.sourcefield, v_single_quote, v_single_quote||v_single_quo(...) */

                    v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| 'from ' || IFNULL(:j.sourcetable,'');
                    -- i.source_tablename;

                    v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| ' Where 1=1 and FILETYPE = ''' 
                    || IFNULL(:j.filetype,'')
                    || ''' and FILENAME = '''
                    || IFNULL(:in_file_name,'')
                    || ''' and FILEDATE = to_date('''
                        || IFNULL(TO_VARCHAR(:in_file_date),'')
                    || ''',''DD-MON-YY'') and';

                    v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| ' is_number(' ||IFNULL(:j.sourcefield,'')||')=0';

                    -- 'REGEXP_LIKE('||j.sourcefield||' , ''^-?[0-9]+(\.[0-9]+)?$''))' doens't work of r.5 and -.5 without a 0 in front
        --''^[[:digit:]]+$''))';         doesnt work fro decimals

        IF :i.where_clause IS NOT NULL
        THEN
            v_sql = IFNULL(:v_sql,'') || ' and '|| IFNULL(:i.where_clause,'');
        END IF;
        /* ORIGSQL: dbms_output.put_line(v_sql); */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation '|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
            , 'QueryCreated_CHECK_NUMBER', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|| in_file_n(...) */
            );  /* ORIGSQL: substr(v_sql,1,4000) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: commit; */
        COMMIT;

        --dbms_output.put_line ('v_sql: ' || v_sql);--
    END FOR;  /* ORIGSQL: end loop; */
END IF;

IF UPPER(:i.VALIDATION_NAME) = 'CHECK_DATE' 
THEN
    /* ORIGSQL: dbms_output.put_line('DATE'); */
    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('DATE');

    FOR j AS CURSOR_CFG_TXNFIELD_DATE
    DO
        v_sql = 'Insert into '||IFNULL(:v_exception_table,'');

        v_sql = IFNULL(:v_sql,'') || ' select filetype,filename,filedate,';

        v_sql = IFNULL(:v_sql,'') || IFNULL(:v_unique_key,'');

        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_comma,'') ||IFNULL(:v_single_quote,'')|| IFNULL(:j.remark,replace(:j.sourcefield, :v_single_quote, IFNULL(:v_single_quote,'')||IFNULL(:v_single_quote,''))) || IFNULL(:v_single_space,'') || IFNULL(:i.ERROR_MESSAGE,'') || IFNULL(:v_single_quote,'') ||IFNULL(:v_comma,'')|| IFNULL(:v_single_quote,'') || IFNULL(:v_single_space,'') || IFNULL(:i.validation_name,'') || IFNULL(:v_single_quote,'');  /* ORIGSQL: replace(j.sourcefield, v_single_quote, v_single_quote||v_single_quote) */
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   /* ORIGSQL: nvl(j.remark,replace(j.sourcefield, v_single_quote, v_single_quote||v_single_quo(...) */

        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| 'from ' || IFNULL(:j.sourcetable,'');
        --i.source_tablename;

        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| ' Where 1=1 and FILETYPE = ''' 
        || IFNULL(:j.filetype,'')
        || ''' and FILENAME = '''
        || IFNULL(:in_file_name,'')
        || ''' and FILEDATE = to_date('''
            || IFNULL(TO_VARCHAR(:in_file_date),'')
        || ''',''DD-MON-YY'') and';

        v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| 'is_date ( ' ||IFNULL(:j.sourcefield,'')||IFNULL(:v_comma,'')||IFNULL(:v_single_quote,'')|| IFNULL(:j.date_format,'') ||IFNULL(:v_single_quote,'') || ') =1';

        IF :i.where_clause IS NOT NULL
        THEN
            v_sql = IFNULL(:v_sql,'') || ' and '|| IFNULL(:i.where_clause,'');
        END IF;
        /* ORIGSQL: dbms_output.put_line('DATe check'); */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('DATe check');

        /* ORIGSQL: dbms_output.put_line(v_sql); */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation '|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
            , 'QueryCreated_CHECK_DATE', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|| in_file_n(...) */
            );  /* ORIGSQL: substr(v_sql,1,4000) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: commit; */
        COMMIT;

        --dbms_output.put_line ('v_sql: ' || v_sql);--
    END FOR;  /* ORIGSQL: end loop; */
END IF;
END FOR;  /* ORIGSQL: end loop; */

FOR i AS cursor_cfg_Validator_flag12
DO
    x = EXT.get_number_ofstrings(:i.UNIQUE_KEY, '||');  /* ORIGSQL: get_number_ofstrings_dbmtknested_SP_INBOUND_VALIDATOR (i.UNIQUE_KEY,'||') */

    v_unique_key = EXT.get_unique_expression(
            in_stage => :in_stage,
            in_str => :i.UNIQUE_KEY,
            in_delimiters => :x,
            in_filetype => :in_File_type
            );  /* ORIGSQL: get_unique_expression_dbmtknested_SP_INBOUND_VALIDATOR(i.UNIQUE_KEY,x,in_File_ty(...) */

    IF :i.CAT_CLAS_TAB_IDENTIFIER = 'TABLE' 
    THEN
        /* ORIGSQL: dbms_output.put_line('TABLE'); */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('TABLE');

        v_sql = 'Insert into '||IFNULL(:v_exception_table,'');

        v_sql = IFNULL(:v_sql,'') || ' select filetype,filename,filedate,';
        v_sql = IFNULL(:v_sql,'') || IFNULL(:v_unique_key,'');
        --nvl(j.remark,    replace(j.sourcefield, v_single_quote, v_single_quote||v_single_quote) )

        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_comma,'') ||IFNULL(:v_single_quote,'')|| IFNULL(replace(:i.source_fieldname, :v_single_quote, IFNULL(:v_single_quote,'')||IFNULL(:v_single_quote,'')),'') || IFNULL(:v_single_space,'') || IFNULL(:i.ERROR_MESSAGE,'') || IFNULL(:v_single_quote,'') ||IFNULL(:v_comma,'')|| IFNULL(:v_single_quote,'') || IFNULL(:v_single_space,'') || IFNULL(:i.validation_name,'') || IFNULL(:v_single_quote,'');  /* ORIGSQL: replace(i.source_fieldname, v_single_quote, v_single_quote||v_single_quote) */

        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| 'from ' || IFNULL(:i.source_tablename,'');
        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| ' Where 1=1 and FILETYPE = ''' 
        || IFNULL(:in_File_type,'')
        || ''' and FILENAME = '''
        || IFNULL(:in_file_name,'')
        || ''' and FILEDATE = to_date('''
            || IFNULL(TO_VARCHAR(:in_file_date),'')
        || ''',''DD-MON-YY'') and';

        IF :i.in_clause_flag = 1
        THEN
            v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| IFNULL(:i.source_fieldname,'') || ' Not In ( select ' ||IFNULL(:i.COLUMN_NAME,'')|| ' from';
        ELSE 
            v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| IFNULL(:i.source_fieldname,'') || ' In ( select ' ||IFNULL(:i.COLUMN_NAME,'')|| ' from';
        END IF;

        v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| IFNULL(:i.TABLE_NAME,'') || '';

        IF :i.where_clause IS NOT NULL
        THEN
            v_sql = IFNULL(:v_sql,'') || '  where '||IFNULL(:i.COLUMN_NAME,'')|| ' IS NOT NULL and (  ' || IFNULL(:i.where_clause,'') || ') )';
        ELSE 
            v_sql = IFNULL(:v_sql,'') || ' where '||IFNULL(:i.COLUMN_NAME,'')|| ' IS NOT NULL ) ';
        END IF;
        /* ORIGSQL: dbms_output.put_line(v_sql); */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation '|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
            , 'QueryCreated_TABLE', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|| in_file_n(...) */
            );  /* ORIGSQL: substr(v_sql,1,4000) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :i.CAT_CLAS_TAB_IDENTIFIER = 'CLASSIFIER' 
    THEN
        v_sql = 'Insert into '||IFNULL(:v_exception_table,'');

        v_sql = IFNULL(:v_sql,'') || ' select filetype,filename,filedate,';
        v_sql = IFNULL(:v_sql,'') || IFNULL(:v_unique_key,'');

        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_comma,'') ||IFNULL(:v_single_quote,'')|| IFNULL(:i.source_fieldname,'') || IFNULL(:v_single_space,'') || IFNULL(:i.ERROR_MESSAGE,'') || IFNULL(:v_single_quote,'') ||IFNULL(:v_comma,'')|| IFNULL(:v_single_quote,'') || IFNULL(:v_single_space,'') || IFNULL(:i.validation_name,'') || IFNULL(:v_single_quote,'');

        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| 'from ' || IFNULL(:i.source_tablename,'');
        v_sql = IFNULL(:v_sql,'') ||IFNULL(:v_single_space,'')|| ' Where 1=1 and FILETYPE = ''' 
        || IFNULL(:in_File_type,'')
        || ''' and FILENAME = '''
        || IFNULL(:in_file_name,'')
        || ''' and FILEDATE = to_date('''
            || IFNULL(TO_VARCHAR(:in_file_date),'')
        || ''',''DD-MON-YY'') and';

        IF :i.in_clause_flag = 1
        THEN
            v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| IFNULL(:i.source_fieldname,'') || ' Not In ( select classfiername from';
        ELSE 
            v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| IFNULL(:i.source_fieldname,'') || ' In ( select classfiername from';
        END IF;

        v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_space,'')|| IFNULL(:i.TABLE_NAME,'')||' where CATEGORYTREENAME =';

        v_sql = IFNULL(:v_sql,'') || IFNULL(:v_single_quote,'') || IFNULL(:i.tree_name,'') || IFNULL(:v_single_quote,'') || IFNULL(:v_single_space,'') || 'and ';

        v_sql = IFNULL(:v_sql,'') || 'CATEGORYNAME =' || IFNULL(:v_single_quote,'') || IFNULL(:i.parent_name,'') || IFNULL(:v_single_quote,'')|| ')';

        IF :i.where_clause IS NOT NULL
        THEN
            v_sql = IFNULL(:v_sql,'') || ' and '|| IFNULL(:i.where_clause,'');
        END IF;

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Validation '|| IFNULL(:i.VALIDATION_NAME,'')|| ' - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
            , 'QueryCreated_CLASSIFIER', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Validation '|| i.VALIDATION_NAME|| ' - for :'|| in_file_n(...) */
            );  /* ORIGSQL: substr(v_sql,1,4000) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: commit; */
        COMMIT;

        --dbms_output.put_line ('v_sql: ' || v_sql);--
    END IF;
END FOR;  /* ORIGSQL: end loop; */
END FOR;  /* ORIGSQL: end loop; */

FOR i AS cursor_cfg_Validator_flag
DO
    x = EXT.get_number_ofstrings(:i.UNIQUE_KEY, '||');  /* ORIGSQL: get_number_ofstrings_dbmtknested_SP_INBOUND_VALIDATOR (i.UNIQUE_KEY,'||') */

    v_unique_key = EXT.get_unique_expression(
            in_stage => :in_stage,
            in_str => :i.UNIQUE_KEY,
            in_delimiters => :x,
            in_filetype => :in_File_type
            );  /* ORIGSQL: get_unique_expression_dbmtknested_SP_INBOUND_VALIDATOR(i.UNIQUE_KEY,x,in_File_ty(...) */

    --dbms_output.put_line ('v_unique_key : ' ||v_unique_key);

    v_sql = 'merge into ext.Inbound_DATA_STAGING tgt
    using
    (select file_type,file_name,file_date,unique_key,listagg(error_message,'';'') within group (order by error_message asc) error_message
        from  (select distinct * from ext.Inbound_validation_errors)
    group by  file_type,file_name, file_date, unique_key) src
    on (tgt.filetype=src.file_type and tgt.filename=src.file_name and tgt.filedate=src.file_date
        and src.unique_key = '||IFNULL(:v_unique_key,'')||')
    when matched then update set
    tgt.error_message= src.error_message,
    tgt.error_flag = 1';

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Log Update on Staging table - for :'|| in_file(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Log Update on Staging table  - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
        , 'Query Created', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Log Update on Staging table  - for :'|| in_file_name|| '-(...) */
        );  /* ORIGSQL: substr(v_sql,1,4000) */

    ----for duplicates, take one
    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: dbms_output.put_line(v_sql); */
    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
    /* ORIGSQL: execute immediate v_sql; */
    EXECUTE IMMEDIATE :v_sql;

    /* ORIGSQL: commit; */
    COMMIT;

    --dbms_output.put_line ('v_sql: ' || v_sql);--
END FOR;  /* ORIGSQL: end loop; */

FOR q AS cursor_cfG_txnfield_dup_check
DO
    v_sql = 'merge into ext.Inbound_DATA_STAGING tgt
    using
    (
        select * from (select file_type,file_name,file_date,unique_key
            ,  row_number() over( partition by error_message order by sortcol ) rnk
            from  (select distinct x.*, '||IFNULL(:q.sortcolumn,'')||' sortcol from ext.Inbound_validation_errors x
                join '|| IFNULL(:q.sourcetable,'') ||' t
                on x.unique_key=t.seq
                where trim(error_type)=''CHECK_DUPLICATE''
                and x.unique_key not in (select z.unique_key from ext.Inbound_validation_errors z group by z.unique_key having count(*)>1)
            )
        ) a where a.rnk=1
    ) src
    on (tgt.filetype=src.file_type and tgt.filename=src.file_name and tgt.filedate=src.file_date
    and src.unique_key = tgt.seq)
    when matched then update set
    tgt.error_message= ''Duplicate selected for load'',tgt.error_flag = 0';

    /* ORIGSQL: dbms_output.put_line ('v_sql Duplicate Merge: ' || v_sql); */
    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('v_sql Duplicate Merge: '|| IFNULL(:v_sql,''));
    --

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Sort Control Query - for :'|| in_file_name|| '(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Sort Control Query  - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
        , 'Query Created', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Sort Control Query  - for :'|| in_file_name|| '-Date:'|| (...) */
        );  /* ORIGSQL: substr(v_sql,1,4000) */

    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
    /* ORIGSQL: execute immediate v_sql; */
    EXECUTE IMMEDIATE :v_sql;

    /* ORIGSQL: commit; */
    COMMIT;
END FOR;  /* ORIGSQL: end loop; */

/*exception when others then

      v_sqlerrm := SUBSTR (SQLERRM, 1, 4000);
v_sql_errorcode := SQLCODE;

 SP_LOGGER (SUBSTR (v_proc_name|| 'ERROR in :'|| in_File_type|| '-FileName:'|| in_file_name|| '-Date:'|| in_file_date,1,255),'FIELDMAP Execution Error',Null,NULL,substr(v_sqlerrm,1,4000) || '|'|| v_sql_errorcode);

     raise;
      COMMIT;

*/
END