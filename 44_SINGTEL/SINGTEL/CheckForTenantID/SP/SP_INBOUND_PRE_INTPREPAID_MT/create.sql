CREATE PROCEDURE EXT.SP_INBOUND_PRE_INTPREPAID_MT
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_INTPREPAID_MT';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_INTPREPAID_MT'; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /*
    5.	Metric Caps
    FVV_Prepaid_M1_Cap
    FVV_Prepaid_M2_Cap
    6.
    7.	Metric Weights
    FVV_Internal Prep_M1_Indv_Weight
    FVV_Internal Prep_M4_L2_Indv_Weight
    
    */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_staging SET field101 = CASE WHEN field16='M1 Cap' THEN 'FVV_(...) */
    UPDATE ext.inbound_Data_staging
        SET
        /* ORIGSQL: field101 = */
        field101 =
        CASE 
            WHEN field16 = 'M1 Cap'
            THEN 'FVV_Prepaid_M1_Cap'
            WHEN field16 = 'M2 Cap'
            THEN 'FVV_Prepaid_M2_Cap'
            WHEN field16 = 'M1 Weight'
            THEN 'FVV_Internal Prep_M1_Indv_Weight'
            WHEN field16 = 'M4 L2 Weight'
            THEN 'FVV_Internal Prep_M4_L2_Indv_Weight'
            ELSE NULL
        END,
        /* ORIGSQL: field105 = */
        field105 = 'percent' 
    FROM
        ext.inbound_Data_staging;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_paramet(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update FVV Targets :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update FVV Targets Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_parameter.file_typ(...) */
END