CREATE PROCEDURE EXT.SP_INBOUND_BOOLEAN
(
    IN in_File_type VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                   /* ORIGSQL: in_File_type IN VARCHAR */
    IN in_file_name VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                   /* ORIGSQL: in_file_name IN VARCHAR */
    IN in_file_date TIMESTAMP,   /* ORIGSQL: in_file_date IN DATE */
    IN in_stage DECIMAL(38,10)   /* ORIGSQL: in_stage IN number */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_sql VARCHAR(31999);  /* ORIGSQL: v_sql varchar2(31999); */
    DECLARE v_proc_name VARCHAR(50) = 'sp_inbound_boolean';  /* ORIGSQL: v_proc_name varchar2(50):='sp_inbound_boolean'; */

    /* ORIGSQL: FOR i IN (SELECT distinct sourcetable,sourcefield FROM inbound_cfg_txnfield WHER(...) */
    DECLARE CURSOR dbmtk_cursor_1430
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_TXNFIELD' not found */

        SELECT   /* ORIGSQL: SELECT distinct sourcetable,sourcefield FROM inbound_cfg_txnfield WHERE datatype(...) */
            DISTINCT
            sourcetable,
            sourcefield
        FROM
            inbound_cfg_txnfield
        WHERE
            datatype = 'BOOL'
            AND sourcefield IS NOT NULL
            AND filetype = :in_File_type
            AND IFNULL(stage_number,1) = :in_stage;  /* ORIGSQL: nvl(stage_number,1) */

    /* ORIGSQL: execute immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    FOR i AS dbmtk_cursor_1430
    DO
        v_sql = 'update '||IFNULL(:i.sourcetable,'') || ' x
        set x.'||IFNULL(:i.sourcefield,'')||' = nvl((select tcvalue from inbound_cfg_bool where incomingvalue= trim(x.' ||IFNULL(:i.sourcefield,'')||' ) ),''INVALID'')
        Where 1=1 and FILETYPE = ''' 
        || IFNULL(:in_File_type,'')
        || ''' and FILENAME = '''
        || IFNULL(:in_file_name,'')
        || ''' and FILEDATE = to_date('''
            || IFNULL(TO_VARCHAR(:in_file_date),'')
        || ''',''DD-MON-YY'')
        and x.' ||IFNULL(:i.sourcefield,'')||' IS NOT NULL         ';

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Boolean Conversion - for :'|| in_file_name|| '(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Boolean Conversion  - for :'|| IFNULL(:in_file_name,'')|| '-Date:'|| IFNULL(TO_VARCHAR(:in_file_date),''),1,255) 
            , 'Query Created', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Boolean Conversion  - for :'|| in_file_name|| '-Date:'|| (...) */
            );  /* ORIGSQL: substr(v_sql,1,4000) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_sql; */
        EXECUTE IMMEDIATE :v_sql;
    END FOR;  /* ORIGSQL: end loop; */
END