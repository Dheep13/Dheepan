CREATE PROCEDURE EXT.SP_ERRORS_MERGER
(
    IN in_tablename VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                   /* ORIGSQL: in_tablename IN varchar */
    IN in_unique_key VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                   /* ORIGSQL: in_unique_key IN varchar */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_sql VARCHAR(31000);  /* ORIGSQL: v_sql varchar2(31000); */
    DECLARE v_proc_name VARCHAR(255) = 'SP_ERRORS_MERGER';  /* ORIGSQL: v_proc_name varchar(255):='SP_ERRORS_MERGER'; */
    DECLARE v_prmtr ROW LIKE inbound_cfg_parameter; --%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: raise; */
            RESIGNAL;
        END;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

        SELECT *
        INTO
            v_prmtr
        FROM
            ext.inbound_cfg_parameter
        WHERE
            object_name = 'SP_INBOUND_TXN_MAP';

        v_sql = 'merge into INBOUND_DATA_STAGING tgt
        using
        (select file_type,file_name,file_date,unique_key,listagg(error_message,'';'') within group (order by error_message asc) error_message
            from  (select distinct * from ' ||IFNULL(:in_tablename,'') ||')
        group by  file_type,file_name, file_date, unique_key) src
        on (tgt.filetype=src.file_type and tgt.filename=src.file_name and tgt.filedate=src.file_date
            and src.unique_key = ' ||IFNULL(:in_unique_key,'')||')
        when matched then update set
        tgt.error_message= src.error_message,
        tgt.error_flag = 1';

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name|| ' Log Update on Staging table - for :'|| v_prmtr(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'')|| ' Log Update on Staging table  - for :'|| IFNULL(:v_prmtr.file_name,'')|| '-Date:'|| IFNULL(:v_prmtr.file_date,''),1,255) 
            , 'Query Created', NULL, NULL, SUBSTRING(:v_sql,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name|| ' Log Update on Staging table  - for :'|| v_prmtr.file_name(...) */
            );  /* ORIGSQL: substr(v_sql,1,4000) */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_sql; */
        EXECUTE IMMEDIATE :v_sql;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: exception when others then */
END