CREATE PROCEDURE EXT.SP_INBOUND_PRE_MUSIC
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */
    DECLARE v_offset DECIMAL(38,10);  /* ORIGSQL: v_offset number ; */

    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    -- CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        INBOUND_CFG_PARAMETER;

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_GENERICPARAMETER' not found */

    SELECT
        value
    INTO
        v_offset
    FROM
        inbound_CfG_genericparameter
    WHERE
        KEY = 'MUSICOFFSET_DAYS';

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_staging SET field100= FLOOR(MONTHS_BETWEEN(TO_DATE(substr(nv(...) */
    UPDATE inbound_Data_staging
        /* --contract months */
        SET
        /* ORIGSQL: field100 = */
        field100 = FLOOR(MONTHS_BETWEEN(TO_DATE(ADD_SECONDS(TO_DATE(SUBSTRING(IFNULL(field9,'20091201'),1,8),'YYYYMMDD'),(86400*(:v_offset))))   /* ORIGSQL: substr(nvl(field9,'20091201'),1,8) */
                                                                                                                                                 /* ORIGSQL: TO_DATE(SUBSTRING(nvl(field9,'20091201'),1,8),'YYYYMMDD') +V_offset */
                , TO_DATE(SUBSTRING(IFNULL(field8,'20100101'),1,8),'YYYYMMDD'))) +1  /* ORIGSQL: substr(nvl(field8,'20100101'),1,8) */
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND IFNULL(error_flag,0) = 0;  /* ORIGSQL: nvl(error_flag,0) */

    /* ORIGSQL: update inbound_Data_staging SET field100=0 where filename = v_inbound_cfg_parame(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field100 = */
        field100 = 0
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND IFNULL(error_flag,0) = 0  /* ORIGSQL: nvl(error_flag,0) */
        AND field8 IS NULL;  

    /* ORIGSQL: update inbound_Data_staging SET field101= CASE WHEN field8 IS NULL THEN 'NoContr(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field101 = */
        field101 =
        CASE 
            WHEN field8 IS NULL
            THEN 'NoContract'
            ELSE ''
        END
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND IFNULL(error_flag,0) = 0;  /* ORIGSQL: nvl(error_flag,0) */

    --102 less than 30 days
    --103 in the same month   

    /* ORIGSQL: update inbound_Data_staging SET field102= CASE WHEN TO_DATE(substr(field11,1,8),(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field102 = */
        field102 =
        CASE 
            WHEN (SECONDS_BETWEEN(TO_DATE(SUBSTRING(field10,1,8),'YYYYMMDD'),TO_DATE(SUBSTRING(field11,1,8),'YYYYMMDD'))/86400)   /* ORIGSQL: substr(field11,1,8) */
            <= 30  /* ORIGSQL: substr(field10,1,8) */
                   /* ORIGSQL: TO_DATE(SUBSTRING(field11,1,8),'YYYYMMDD') - TO_DATE(SUBSTRING(field10,1,8),'YYY(...) */
            THEN 1
            ELSE 0
        END,
        /* ORIGSQL: field103 = */
        field103 =
        CASE 
            WHEN SUBSTRING(field11,1,6) <> SUBSTRING(field10,1,6)  /* ORIGSQL: substr(field11,1,6) */
                                                                   /* ORIGSQL: substr(field10,1,6) */
            THEN 1
            ELSE 0
        END
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND IFNULL(error_flag,0) = 0;  /* ORIGSQL: nvl(error_flag,0) */

    --102 less than 30 days
    --103 in the same month

    --field 104 = num units, 105=don't pay flag   
    /* ORIGSQL: update inbound_Data_staging SET field104=1, field105 = 0 where filename = v_inbo(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field104 = */
        field104 = 1,
        /* ORIGSQL: field105 = */
        field105 = 0
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND IFNULL(error_flag,0) = 0;  /* ORIGSQL: nvl(error_flag,0) */

    /* ORIGSQL: update inbound_Data_staging SET field105=1 where field102 = 1 and field103 =0 an(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field105 = */
        field105 = 1
    FROM
        inbound_Data_staging
    WHERE
        field102 = 1
        AND field103 = 0  /*less than 30 days and within the same month*/
        AND filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND IFNULL(error_flag,0) = 0;  /* ORIGSQL: nvl(error_flag,0) */

    /* ORIGSQL: update inbound_Data_staging SET field104=-1 where field102 = 1 and field103 =1 a(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field104 = */
        field104 = -1
    FROM
        inbound_Data_staging
    WHERE
        field102 = 1
        AND field103 = 1  /*less than 30 days and cross month*/
        AND filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND IFNULL(error_flag,0) = 0;  /* ORIGSQL: nvl(error_flag,0) */

    /* ORIGSQL: commit; */
    COMMIT;
END