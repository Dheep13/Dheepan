CREATE PROCEDURE EXT.SP_INBOUND_PRE_PREPAIDBBOM
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_PrepaidBBOM';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_PrepaidBBOM'; */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */
    DECLARE v_inbound_cfg_parameter ROW LIKE EXT.INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    
    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */
    /*Sanjay : Using cursor approach since execute immediate is not allowed in functions in HANA. May have to change approach if performance is poor*/
    
    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        INBOUND_CFG_PARAMETER;  

    /* ORIGSQL: update inbound_Data_staging SET field100=DBMTK_USER_NAME.FN_VALUELOOKUP(FIELD5, (...) */
    update inbound_Data_staging tgt
      set  
     field100=/*fn_ValueLookup(FIELD2, 'STEL_PARTICIPANT@STELEXT'
    ,'sysdate between effectivestartdate and effectiveenddate'  
      ,'GENERICATTRIBUTE11','PAYEEID') */
      (select max(payeeid) from EXT.STEL_PARTICIPANT 
      where
      current_timestamp between effectivestartdate 
      and effectiveenddate
      and trim(GENERICATTRIBUTE13) = trim(tgt.field5))  
      where 
      filename  = :v_inbound_cfg_parameter.file_name
      and filedate=:v_inbound_cfg_parameter.file_Date;
    --UPDATE inbound_Data_staging
        --SET
        /* ORIGSQL: field100 = */
        --field100 = EXT.FN_VALUELOOKUP(FIELD5, 'STEL_PARTICIPANT@STELEXT', 'sysdate between effectivestartdate and effectiveenddate', 'GENERICATTRIBUTE13', 'PAYEEID') /* --regist */  /* ORIGSQL: fn_ValueLookup(FIELD5, 'STEL_PARTICIPANT@STELEXT','sysdate between effectivestar(...) */
        
    --FROM
        --inbound_Data_staging
    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */


    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Regist and Created Payeeid on FIELD100 (...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Regist and Created Payeeid on FIELD100 and Field101 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update Regist and Created Payeeid on FIELD100 and Field101 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Regist and Created Payeeid on FIELD100 and Field10(...) */

    /* ORIGSQL: UPDATE inbound_data_staging SET field15 = 'Rejected due to Refund Order' WHERE f(...) */
     
    UPDATE inbound_data_staging
        SET
        /* ORIGSQL: field15 = */
        field15 = 'Rejected due to Refund Order' 
    FROM
        inbound_data_staging
    WHERE
        field9
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
        IN
        (
            SELECT   /* ORIGSQL: (select FIELD9 from inbound_data_staging where UPPER(field8) like '%REFUND%') */
                FIELD9
            FROM
                inbound_data_staging
            WHERE
                UPPER(field8) LIKE '%REFUND%' 
        );

    -- UPDATE inbound_data_staging
    --SET field15 = 'Rejected due to Refund Order'
    --WHERE field3 IN (
        -- SELECT DISTINCT A.field3
        -- FROM inbound_data_staging A
        -- WHERE EXISTS (SELECT 1 FROM inbound_data_staging B WHERE B.field8 LIKE '%' || A.field3 || '%')
    --);   

    /* ORIGSQL: update inbound_data_staging SET field15 = 'Rejected due to Refund Order' where L(...) */
    UPDATE inbound_data_staging
        SET
        /* ORIGSQL: field15 = */
        field15 = 'Rejected due to Refund Order' 
    FROM
        inbound_data_staging
    WHERE
        LOWER(field8) LIKE '%refund%';

    /* ORIGSQL: commit; */
    COMMIT;  

    /* ORIGSQL: update inbound_data_staging SET field15 = field15||'Rejected Its not under Prepa(...) */
    UPDATE inbound_data_staging
        SET
        /* ORIGSQL: field15 = */
        field15 = IFNULL(field15,'')||'Rejected Its not under Prepaid Card' 
    FROM
        inbound_data_staging
    WHERE
        field10 <> 'PREPAID CARDS';

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Field102 :' || v_inbound_cfg_parameter.(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Field102 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update Field102 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Field102 :' || v_inbound_cfg_parameter.file_type |(...) */

    /* ORIGSQL: commit; */
    COMMIT;
END