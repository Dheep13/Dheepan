CREATE PROCEDURE EXT.SP_INBOUND_PRE_SAPSIM
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_SAPSIM';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_SAPSIM'; */
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
    
    /*
    update inbound_Data_staging
    set
    field100=fn_ValueLookup(replace(FIELD3,'''','') , 'STEL_PARTICIPANT@STELEXT'
        ,'sysdate between effectivestartdate and effectiveenddate'
          ,'replace(GENERICATTRIBUTE5,'''''','''') ','PAYEEID')
    where filename  = v_inbound_cfg_parameter.file_name and filedate=v_inbound_cfg_parameter.file_Date;
    */   

    /* ORIGSQL: update inbound_Data_staging SET field100=DBMTK_USER_NAME.FN_VALUELOOKUP(REPLACE((...) */
    update inbound_Data_staging tgt
      set  
     field100=/*fn_ValueLookup(FIELD2, 'STEL_PARTICIPANT@STELEXT'
    ,'sysdate between effectivestartdate and effectiveenddate'  
      ,'GENERICATTRIBUTE11','PAYEEID') */
      (select max(payeeid) from EXT.STEL_PARTICIPANT 
      where
      current_timestamp between effectivestartdate 
      and effectiveenddate
      and trim(GENERICATTRIBUTE5) = trim(tgt.field3))  
      where 
      filename  = :v_inbound_cfg_parameter.file_name
      and filedate=:v_inbound_cfg_parameter.file_Date;
    --UPDATE inbound_Data_staging
        --SET
        /* ORIGSQL: field100 = */
        --field100 = EXT.FN_VALUELOOKUP(REPLACE(FIELD3,'''','SINGLEQUOTE!'), 'STEL_PARTICIPANT@STELEXT', 'sysdate between effectivestartdate and effectiveenddate', 'replace(GENERICATTRIBUTE5 ,'''''''',''SINGLEQUOTE!'')', 'PAYEEID')   /* ORIGSQL: fn_ValueLookup(REPLACE(FIELD3,'''','SINGLEQUOTE!'), 'STEL_PARTICIPANT@STELEXT','(...) */
        
    --FROM
        --inbound_Data_staging
    /* ORIGSQL: commit; */
    COMMIT;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update PayeeID on Field100 :' || v_inbound_cfg(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update PayeeID on Field100 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update PayeeID on Field100 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update PayeeID on Field100 :' || v_inbound_cfg_parameter.(...) */

    -----------------------------[Arun 13th June 2019] - Start of block
    /*
    This block caters to udpate FIELD2 in desired format DD-MON-YYYY where the source file has DD-MON-YYYY/D-MON-YYYY
    */ 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_staging tgt using (SELECT seq, filetype, filename, CASE (...) */
    MERGE INTO inbound_data_staging AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select seq, filetype, filename, CASE WHEN LENGTH(FIELD2) =8 THEN '0'||substr(FI(...) */
                seq,
                filetype,
                filename,
                CASE
                    WHEN LENGTH(FIELD2) = 8
                    THEN '0'||IFNULL(SUBSTRING(FIELD2,1,6),'') ||IFNULL(SUBSTRING(TO_VARCHAR(CURRENT_TIMESTAMP,'DD-MON-YYYY'),8,2),'') ||IFNULL(SUBSTRING(FIELD2,9,2),'')  /* ORIGSQL: substr(to_char(sysdate,'DD-MON-YYYY'),8,2) */
                                                                                                                                                                                            /* ORIGSQL: substr(FIELD2,9,2) */
                                                                                                                                                                                            /* ORIGSQL: substr(FIELD2,1,6) */
                    ELSE IFNULL(SUBSTRING(FIELD2,1,7),'') ||IFNULL(SUBSTRING(TO_VARCHAR(CURRENT_TIMESTAMP,'DD-MON-YYYY'),8,2),'') ||IFNULL(SUBSTRING(FIELD2,10,2),'')  /* ORIGSQL: substr(to_char(sysdate,'DD-MON-YYYY'),8,2) */
                                                                                                                                                                                        /* ORIGSQL: substr(FIELD2,10,2) */
                                                                                                                                                                                        /* ORIGSQL: substr(FIELD2,1,7) */
                END
                AS ModifiedField2
            FROM
                inbound_data_staging
        ) AS src
        ON (tgt.seq = src.seq
        	AND tgt.filename = :v_inbound_cfg_parameter.file_name
            AND tgt.filedate = :v_inbound_cfg_parameter.file_Date
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.FIELD2 = src.ModifiedField2
        --WHERE
            --filename = v_inbound_cfg_parameter.file_name
            --AND filedate = v_inbound_cfg_parameter.file_Date
            ;

    /* ORIGSQL: commit; */
    COMMIT;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Appending date on Field2 :' || v_inbound_cfg_p(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Appending date on Field2 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update FIELD2 date format on Field2 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Appending date on Field2 :' || v_inbound_cfg_parameter.fi(...) */

    --------------------------[Arun 13th June 2019] - End of block
END