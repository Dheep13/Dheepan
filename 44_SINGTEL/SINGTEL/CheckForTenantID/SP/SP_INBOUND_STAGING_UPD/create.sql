CREATE PROCEDURE EXT.SP_INBOUND_STAGING_UPD
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_STAGING_UPD';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_STAGING_UPD'; */
    DECLARE v_inbound_cfg_parameter ROW LIKE inbound_cfg_parameter;--%rowtype;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */
    
    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
    
        /* ORIGSQL: when no_data_found then */
        BEGIN
            /* ORIGSQL: NULL; */
            DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
        END;


    

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        inbound_cfg_parameter;

    /* ORIGSQL: sp_ogpo_hrcentraltitlemap () */
    CALL EXT.SP_OGPO_HRCENTRALTITLEMAP(/*v_inbound_cfg_parameter.file_type,
        v_inbound_cfg_parameter.file_name,
    v_inbound_cfg_parameter.file_date*/);

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_GENERICPARAMETER' not found */
    /* ORIGSQL: update inbound_cfg_genericparameter SET datevalue = CASE WHEN datevalue < (SELEC(...) */
    UPDATE inbound_cfg_genericparameter
        SET
        /* ORIGSQL: datevalue = */
        datevalue =
        CASE 
            WHEN datevalue
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
            <
            (
                SELECT   /* ORIGSQL: (select MAX(TO_DATE(field41, 'YYYYMMDD')) from inbound_data_staging) */
                    MAX(TO_DATE(field41, 'YYYYMMDD')) 
                FROM
                    inbound_data_staging
            )  
            THEN (
                SELECT   /* ORIGSQL: (select MAX(TO_DATE(field41, 'YYYYMMDD')) from inbound_data_staging) */
                    MAX(TO_DATE(field41, 'YYYYMMDD'))
                FROM
                    inbound_data_staging
            )
            ELSE datevalue
        END
    FROM
        inbound_cfg_genericparameter
    WHERE
        KEY = 'HR Profile MAX Last date Updated';

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update datevalue in inbound_cfg_genericparamet(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update datevalue in  inbound_cfg_genericparameter :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update datevalue in  inbound_cfg_genericparameter Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update datevalue in  inbound_cfg_genericparameter :' || v(...) */

    /* ORIGSQL: exception when no_data_found then */
END