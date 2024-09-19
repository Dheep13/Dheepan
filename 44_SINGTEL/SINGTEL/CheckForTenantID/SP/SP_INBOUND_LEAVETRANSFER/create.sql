CREATE PROCEDURE EXT.SP_INBOUND_LEAVETRANSFER
(
    IN P_FILEDATE TIMESTAMP,   /* ORIGSQL: P_FILEDATE IN DATE */
    IN P_FILETYPE VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: P_FILETYPE IN VARCHAR2 */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_LEAVETRANSFER';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_LEAVETRANSFER'; */
    DECLARE v_parameter ROW LIKE Inbound_cfg_Parameter;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT
        DISTINCT
        *
    INTO
        v_parameter
    FROM
        ext.Inbound_cfg_Parameter;  

    /* ORIGSQL: update inbound_Cfg_parameter SET object_name='SP_INBOUND_TXN_MAP', file_Type=p_f(...) */
    UPDATE ext.Inbound_Cfg_parameter
        SET
        /* ORIGSQL: object_name = */
        object_name = 'SP_INBOUND_TXN_MAP',
        /* ORIGSQL: file_Type = */
        file_Type = :P_FILETYPE,
        /* ORIGSQL: file_name = */
        file_name = IFNULL(:P_FILETYPE,'')||'_'||IFNULL(TO_VARCHAR(ext.trunc(:P_FILEDATE, 'DD'),'YYYYMMDD'),''),  /* ORIGSQL: to_char(trunc(p_filedate),'YYYYMMDD') */
        /* ORIGSQL: file_date = */
        file_date = trunc(:P_FILEDATE, 'DD')  /* ORIGSQL: trunc(p_filedate) */
    FROM
        ext.Inbound_Cfg_parameter;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update inbound_Cfg_parameter :' || v_parameter(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update inbound_Cfg_parameter  :'|| IFNULL(:v_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_parameter.file_name,'') || '-Date:'|| IFNULL(:v_parameter.file_date,''),1,255) 
        , 'Update inbound_Cfg_parameter Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update inbound_Cfg_parameter  :' || v_parameter.file_type(...) */

    /* ORIGSQL: commit; */
    COMMIT;
END