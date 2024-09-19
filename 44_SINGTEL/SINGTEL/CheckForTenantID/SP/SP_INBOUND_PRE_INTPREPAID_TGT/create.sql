CREATE PROCEDURE EXT.SP_INBOUND_PRE_INTPREPAID_TGT
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_INTPREPAID_TGT';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_INTPREPAID_TGT'; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /*
    FVV_Internal Prep_Regular_SIM_Weight
    FVV_Internal Prep_mRemit_Weight
    FVV_Internal Prep_Mobile_Revenue_Weight
    FVV_Internal Prep_Phoenix_Card_Weight
    FVV_Internal Prep_Tourist_SIM_Weight
    
    
    mRemit
    Phoenix Card
    Topup Card
    Tourist SIM
    Regular SIM
    
    
    
    600-qty
    601-SGD
    FV_A12345_Internal Prepaid_mRemit_Target        2814749767106561    2    1970324836974600
    FV_A12345_Internal Prepaid_Phoenix Card_Target        2814749767106561    2    1970324836974601
    FV_A12345_External Prepaid_SIM and Topup Card_Target        2814749767106561    2    1970324836974601
    FV_V0076_External Prepaid_Hi Card_Target        2814749767106561    2    1970324836974600
    */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_staging SET field101 = CASE WHEN field16='mRemit' and field1(...) */
    UPDATE ext.inbound_Data_staging
        SET
        /* ORIGSQL: field101 = */
        field101 =
        CASE 
            WHEN field16 = 'mRemit'
            AND field17 = 'Weight'
            THEN 'FVV_Internal Prep_mRemit_Weight'
            WHEN field16 = 'Phoenix Card'
            AND field17 = 'Weight'
            THEN 'FVV_Internal Prep_Phoenix_Card_Weight'
            WHEN field16 = 'Topup Card'
            AND field17 = 'Weight'
            THEN 'FVV_Internal Prep_Mobile_Revenue_Weight'
            WHEN field16 = 'Tourist SIM'
            AND field17 = 'Weight'
            THEN 'FVV_Internal Prep_Tourist_SIM_Weight'
            WHEN field16 = 'Regular SIM'
            AND field17 = 'Weight'
            THEN 'FVV_Internal Prep_Regular_SIM_Weight'

            WHEN field16 = 'mRemit'
            AND field17 = 'Target'
            THEN 'FVV_Prepaid_mRemit_Target'
            WHEN field16 = 'Phoenix Card'
            AND field17 = 'Target'
            THEN 'FVV_Prepaid_Phoenix Card_Target'
            WHEN field16 = 'Topup Card'
            AND field17 = 'Target'
            THEN 'FVV_Prepaid_Topup Card_Target'
            WHEN field16 = 'Tourist SIM'
            AND field17 = 'Target'
            THEN 'FVV_Prepaid_Tourist SIM_Target'
            WHEN field16 = 'Regular SIM'
            AND field17 = 'Target'
            THEN 'FVV_Prepaid_Hi Card_Target'

            ELSE NULL
        END,
        /* ORIGSQL: field105 = */
        field105 =
        CASE 
            WHEN field17 = 'Weight'
            THEN 'percent'

            WHEN field16 = 'mRemit'
            AND field17 = 'Target'
            THEN 'quantity'
            WHEN field16 = 'Phoenix Card'
            AND field17 = 'Target'
            THEN 'SGD'
            WHEN field16 = 'Topup Card'
            AND field17 = 'Target'
            THEN 'SGD'
            WHEN field16 = 'Tourist SIM'
            AND field17 = 'Target'
            THEN 'quantity'
            WHEN field16 = 'Regular SIM'
            AND field17 = 'Target'
            THEN 'quantity'
        END
    FROM
        ext.inbound_Data_staging;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_paramet(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update FVV Targets :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update FVV Targets Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_parameter.file_typ(...) */

    /* ORIGSQL: update inbound_Data_staging SET field10= CASE WHEN field10 IS NULL THEN null ELS(...) */
    UPDATE ext.inbound_Data_staging
        SET
        /* ORIGSQL: field10 = */
        field10 =
        CASE 
            WHEN field10 IS NULL
            THEN NULL
            ELSE field10/100
        END,
        /* ORIGSQL: field4 = */
        field4 =
        CASE 
            WHEN field4 IS NULL
            THEN NULL
            ELSE field4/100
        END,
        /* ORIGSQL: field5 = */
        field5 =
        CASE 
            WHEN field5 IS NULL
            THEN NULL
            ELSE field5/100
        END,
        /* ORIGSQL: field6 = */
        field6 =
        CASE 
            WHEN field6 IS NULL
            THEN NULL
            ELSE field6/100
        END,
        /* ORIGSQL: field7 = */
        field7 =
        CASE 
            WHEN field7 IS NULL
            THEN NULL
            ELSE field7/100
        END,
        /* ORIGSQL: field8 = */
        field8 =
        CASE 
            WHEN field8 IS NULL
            THEN NULL
            ELSE field8/100
        END,
        /* ORIGSQL: field9 = */
        field9 =
        CASE 
            WHEN field9 IS NULL
            THEN NULL
            ELSE field9/100
        END,
        /* ORIGSQL: field11 = */
        field11 =
        CASE 
            WHEN field11 IS NULL
            THEN NULL
            ELSE field11/100
        END,
        /* ORIGSQL: field12 = */
        field12 =
        CASE 
            WHEN field12 IS NULL
            THEN NULL
            ELSE field12/100
        END,
        /* ORIGSQL: field13 = */
        field13 =
        CASE 
            WHEN field13 IS NULL
            THEN NULL
            ELSE field13/100
        END,
        /* ORIGSQL: field14 = */
        field14 =
        CASE 
            WHEN field14 IS NULL
            THEN NULL
            ELSE field14/100
        END,
        /* ORIGSQL: field15 = */
        field15 =
        CASE 
            WHEN field15 IS NULL
            THEN NULL
            ELSE field15/100
        END
    FROM
        ext.inbound_Data_staging
    WHERE
        field105 = 'percent';

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_paramet(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update FVV Targets :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update FVV Targets Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update FVV Targets :' || v_inbound_cfg_parameter.file_typ(...) */
END