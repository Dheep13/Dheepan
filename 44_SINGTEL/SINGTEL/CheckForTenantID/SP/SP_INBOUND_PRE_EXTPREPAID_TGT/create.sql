CREATE PROCEDURE EXT.SP_INBOUND_PRE_EXTPREPAID_TGT
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_EXTPREPAID_TGT';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_EXTPREPAID_TGT'; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /*
    FVV_Prepaid_EMTU_Target
    FVV_Prepaid_Hi Card_Target
    FVV_Prepaid_Phoenix Card_Target
    FVV_Prepaid_Topup Card_Target
    */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_staging SET field101 = CASE WHEN field16='Regular SIM + Tour(...) */
    UPDATE ext.inbound_Data_staging
        SET
        /* ORIGSQL: field101 = */
        field101 =
        CASE 
            WHEN field16 = 'Regular SIM + Tourist SIM + BBOM SIM (Count)'
            THEN 'FVV_Prepaid_Hi Card_Target'
            WHEN field16 = 'Phoenix Card'
            THEN 'FVV_Prepaid_Phoenix Card_Target'
            WHEN field16 = 'EMTU'
            THEN 'FVV_Prepaid_EMTU_Target'
            WHEN field16 = 'TUC+SIM'
            THEN 'FVV_Prepaid_Topup Card_Target'
            ELSE NULL
        END,
        /* ORIGSQL: field105 = */
        field105 =
        CASE 
            WHEN field16 = 'Regular SIM + Tourist SIM + BBOM SIM (Count)'
            THEN 'quantity'
            ELSE 'SGD'
        END
    FROM
        ext.inbound_Data_staging;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_paramet(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update FVV Targets :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update FVV Targets Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_parameter.file_typ(...) */
END