CREATE PROCEDURE EXT.SP_INBOUND_PRE_MOBILE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_MOBILE';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_MOBILE'; */

    DECLARE v_prmtr ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    --CIS indicator

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_Staging SET field152 = CASE WHEN fIELD49 IS NULL THEN 'N' EL(...) */
    UPDATE inbound_Data_Staging
        SET
        /* ORIGSQL: field152 = */
        field152 =
        CASE 
            WHEN fIELD49 IS NULL
            THEN 'N'
            ELSE 'Y'
        END
    FROM
        inbound_Data_Staging
    WHERE
        filename = :v_prmtr.file_name
        AND filedate = :v_prmtr.file_Date;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update CIS Indicator :' || :v_prmtr.file_type |(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update CIS Indicator :'|| IFNULL(:v_prmtr.file_type,'') || '-FileName:'|| IFNULL(:v_prmtr.file_name,'') || '-Date:'|| IFNULL(:v_prmtr.file_date,''),1,255) 
        , 'Update CIS Indicator Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update CIS Indicator :' || :v_prmtr.file_type || '-FileNam(...) */

    /* ORIGSQL: commit; */
    COMMIT;  

    /* ORIGSQL: update inbound_Data_Staging SET error_flag = 1, error_message= CASE WHEN field19(...) */
    UPDATE inbound_Data_Staging
        --set error_flag = 1, error_message=case when field19 IS NULL then 'Dealer Code Field is mandatory' else 'Service Eff Date Field is mandatory ' end
        SET
        /* ORIGSQL: error_flag = */
        error_flag = 1,
        /* ORIGSQL: error_message = */
        error_message =
        CASE 
            WHEN field19 IS NULL
            THEN 'Dealer Code Field is mandatory'
        END
        /* --[arun mad thi chang on nd ocobr] */
    FROM
        inbound_Data_Staging
    WHERE
        filename = :v_prmtr.file_name
        AND filedate = :v_prmtr.file_Date
        AND (
            (field19 IS NULL
                AND (field4 = 'PR'
                    OR (field4 = 'CH'
            AND field9 = 'AC')))
            /* -- OR ----[arun mad thi chang on nd ocobr] */
            /* --    (field10 IS NULL and (field4='PR' or (field4='CH' and field9='AC')) )--[arun mad thi chang on nd ocobr] */
        );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update null dealder/serv eff date :' || v_prmt(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update null dealder/serv eff date :'|| IFNULL(:v_prmtr.file_type,'') || '-FileName:'|| IFNULL(:v_prmtr.file_name,'') || '-Date:'|| IFNULL(:v_prmtr.file_date,''),1,255) 
        , 'Update null Dealer/Serv Date Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update null dealder/serv eff date :' || :v_prmtr.file_type(...) */
END