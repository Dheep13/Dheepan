CREATE PROCEDURE EXT.SP_INBOUND_PRE_MREMIT
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_MREMIT';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_MREMIT'; */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */
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

    --update inbound_Data_staging
    -- set
    --  field100=fn_ValueLookup(FIELD17, 'STEL_PARTICIPANT@STELEXT'
        --  ,'sysdate between effectivestartdate and effectiveenddate'
    --   ,'GENERICATTRIBUTE2','PAYEEID') --regist

    --   , field101 = fn_ValueLookup(FIELD18, 'STEL_PARTICIPANT@STELEXT'
        --  ,'sysdate between effectivestartdate and effectiveenddate'
    --   ,'GENERICATTRIBUTE2','PAYEEID') --created
    --where filename  = :v_inbound_cfg_parameter.file_name and filedate=:v_inbound_cfg_parameter.file_Date;
    --MERGE INTO STELADMIN.INBOUND_DATA_STAGING x
    --USING (
        --select min(rowid) rowidvalue,field1,sum(field6) as value from inbound_data_staging
        --where FIELD19 ='KYC' and FIELD16 in ('PST','PST7')
        --group by field1
    --) y
    --ON (x.field1  = y.field1 and x.rowid=y.rowidvalue )
    --when matched then update set
    --"FIELD20" = y.value;
    --commit;

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_staging SET field100=EXT.FN_VALUELOOKUP(FIELD17,(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field100 = */
        field100 = EXT.FN_VALUELOOKUP(FIELD17, 'STEL_PARTICIPANT@STELEXT', 'sysdate between effectivestartdate and effectiveenddate', 'GENERICATTRIBUTE12', 'PAYEEID'),/* --regist */  /* ORIGSQL: fn_ValueLookup(FIELD17, 'STEL_PARTICIPANT@STELEXT','sysdate between effectivesta(...) */
        /* ORIGSQL: field101 = */
        field101 = EXT.FN_VALUELOOKUP(FIELD18, 'STEL_PARTICIPANT@STELEXT', 'sysdate between effectivestartdate and effectiveenddate', 'GENERICATTRIBUTE12', 'PAYEEID') /* --created */  /* ORIGSQL: fn_ValueLookup(FIELD18, 'STEL_PARTICIPANT@STELEXT','sysdate between effectivesta(...) */
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date;

    /***********
    UPDATE inbound_Data_staging SET field21 ='Rejected-Hub# already exists in OLD Records'
    where field2 in (select  HUB_SCR_REF FROM STEL_DATA_MREMIT_HUB where
        hub_date <
        ( select TRUNC(MAX(to_date(FIELD3,'DDMMYY')),'MM') from inbound_Data_staging)
    );
    commit;
    ***********/

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Regist and Created Payeeid on FIELD100 (...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Regist and Created Payeeid on FIELD100 and Field101 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update Regist and Created Payeeid on FIELD100 and Field101 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Regist and Created Payeeid on FIELD100 and Field10(...) */

    /* ORIGSQL: update inbound_Data_staging SET field101 = EXT.FN_VALUELOOKUP(FIELD1(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field101 = */
        field101 = EXT.FN_VALUELOOKUP(FIELD17, 'STEL_PARTICIPANT@STELEXT', 'sysdate between effectivestartdate and effectiveenddate', 'GENERICATTRIBUTE2', 'PAYEEID')   /* ORIGSQL: fn_ValueLookup(FIELD17, 'STEL_PARTICIPANT@STELEXT','sysdate between effectivesta(...) */
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND UPPER(FIELD12)
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_VALID_FIXEDLIST' not found */
        IN
        (
            SELECT   /* ORIGSQL: (select UPPER(validvalue) from inbound_Cfg_valid_Fixedlist where validationtype=(...) */
                UPPER(validvalue)
            FROM
                inbound_Cfg_valid_Fixedlist
            WHERE
                validationtype = 'mRemitPayeeException'
        );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Field101 for mRemitPayeeException :' ||(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Field101 for mRemitPayeeException  :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update Field101 for mRemitPayeeException Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Field101 for mRemitPayeeException  :' || v_inbound(...) */

    /* ORIGSQL: update inbound_Data_staging SET field102= CASE WHEN FIELD1 IS NULL THEN CASE WHE(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field102 = */
        field102 =
        CASE 
            WHEN FIELD1 IS NULL
            THEN
            CASE
                WHEN UPPER(FIELD12)  
                IN
                (
                    SELECT   /* ORIGSQL: (select UPPER(validvalue) from inbound_Cfg_valid_Fixedlist where validationtype=(...) */
                        UPPER(validvalue)
                    FROM
                        inbound_Cfg_valid_Fixedlist
                    WHERE
                        validationtype = 'mRemitPayeeException'
                )
                THEN FIELD100
                ELSE FIELD101
            END
            ELSE FIELD100
        END
    FROM
        inbound_Data_staging
    WHERE
        filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    --update inbound_Data_staging set FIELD100=field18,FIELD101=field18,field102=field18;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Field102 :' || :v_inbound_cfg_parameter.(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Field102 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update Field102 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Field102 :' || :v_inbound_cfg_parameter.file_type |(...) */

    /* ORIGSQL: commit; */
    COMMIT;
END