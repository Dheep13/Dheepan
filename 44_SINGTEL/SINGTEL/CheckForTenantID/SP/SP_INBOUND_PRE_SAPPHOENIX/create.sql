CREATE PROCEDURE EXT.SP_INBOUND_PRE_SAPPHOENIX
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_SAPPHOENIX';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_SAPPHOENIX'; */
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
        EXT.INBOUND_CFG_PARAMETER;
        
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_staging SET field100=DBMTK_USER_NAME.FN_VALUELOOKUP(FIELD6, (...) */
    update inbound_Data_staging tgt
      set  
     field100=/*fn_ValueLookup(FIELD2, 'STEL_PARTICIPANT@STELEXT'
    ,'sysdate between effectivestartdate and effectiveenddate'  
      ,'GENERICATTRIBUTE11','PAYEEID') */
      (select max(payeeid) from EXT.STEL_PARTICIPANT 
      where
      current_timestamp between effectivestartdate 
      and effectiveenddate
      and trim(GENERICATTRIBUTE4) = trim(tgt.field6))  
      where 
      filename  = :v_inbound_cfg_parameter.file_name
      and filedate=:v_inbound_cfg_parameter.file_Date;
    --UPDATE ext.inbound_Data_staging
        --SET
        /* ORIGSQL: field100 = */
        --field100 = DBMTK_USER_NAME.FN_VALUELOOKUP(FIELD6, 'STEL_PARTICIPANT@STELEXT', 'sysdate between effectivestartdate and effectiveenddate', 'GENERICATTRIBUTE4', 'PAYEEID')   /* ORIGSQL: fn_ValueLookup(FIELD6, 'STEL_PARTICIPANT@STELEXT','sysdate between effectivestar(...) */
      
    --FROM
        --ext.inbound_Data_staging
  
    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update PayeeID on Field100 :' || v_inbound_cfg(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update PayeeID on Field100 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update PayeeID on Field100 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update PayeeID on Field100 :' || v_inbound_cfg_parameter.(...) */
END