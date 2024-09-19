CREATE PROCEDURE EXT.SP_INBOUND_ERRORFILEREMOVAL
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_date TIMESTAMP;  /* ORIGSQL: v_date date; */
    DECLARE v_Filename VARCHAR(500);  /* ORIGSQL: v_Filename varchar2(500); */
    DECLARE v_OrigFilename VARCHAR(500);  /* ORIGSQL: v_OrigFilename varchar2(500); */
    DECLARE v_Sql VARCHAR(4000);  /* ORIGSQL: v_Sql varchar2(4000); */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_ERRORFILEREMOVAL';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_ERRORFILEREMOVAL'; */
    DECLARE v_parameter ROW LIKE Inbound_cfg_Parameter;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    /* ORIGSQL: For x in (select distinct nvl(b.tablename, a.tgttable) tbl from inbound_cfg_txnf(...) */
    DECLARE CURSOR dbmtk_cursor_3057
    FOR 
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_TXNFIELD' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_TGTTABLE' not found */

        SELECT   /* ORIGSQL: select distinct nvl(b.tablename, a.tgttable) tbl from inbound_cfg_txnfield a lef(...) */
            DISTINCT   
            IFNULL(b.tablename, a.tgttable) AS tbl
        FROM
            EXT.inbound_cfg_txnfield a
        LEFT OUTER JOIN
            EXT.inbound_Cfg_tgttable b
            ON a.tgttable = b.tgttable
        WHERE
            LOCATE(:v_OrigFilename,a.filetype,1,1) > 0;  /* ORIGSQL: instr(v_ORigfilename, a.filetype) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT
        DISTINCT
        *
    INTO
        v_parameter
    FROM
        EXT.inbound_cfg_Parameter;

    SELECT
        file_Date
    INTO
        v_date
    FROM
        EXT.inbound_Cfg_parameter;

    SELECT
        file_name
    INTO
        v_Filename
    FROM
        EXT.inbound_Cfg_parameter;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.EXT.inbound_DATA_TXN' not found */

    SELECT
        genericattribute1
    INTO
        v_OrigFilename
    FROM
        EXT.inbound_data_txn
    WHERE
        filedate = :v_date
        AND filename = :v_Filename;

    FOR x AS dbmtk_cursor_3057
    DO
        v_Sql = 'update  '|| IFNULL(:x.tbl,'') ||' set recordstatus=-9, filename = ''REMOVED-''||filename where filename = ''' ||IFNULL(:v_OrigFilename,'') ||''' ';

        /* ORIGSQL: dbms_output.put_line(v_sql); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_Sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_sql; */
        EXECUTE IMMEDIATE :v_Sql;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'ErrorFile Removal :' || v_parameter.file_type (...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'ErrorFile Removal  :'|| IFNULL(:v_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_date,''),1,255) 
            , 'ErrorFile Removal Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'ErrorFile Removal  :' || v_parameter.file_type || '-FileN(...) */
    END FOR;  /* ORIGSQL: END LOOP; */

    /* ORIGSQL: commit; */
    COMMIT;
END