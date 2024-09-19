CREATE PROCEDURE EXT.SP_INBOUND_PRE_RMT0081
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_RMT0081';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_RMT0081'; */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        INBOUND_CFG_PARAMETER;

    /* field3 - Dealer code. field13 mm/dd/yyyy comp date*/
    /*
    merge into inbound_Data_assignment tgt
    using
    (
        select max(mgr.name) mgrname, ta.orderid, ta.linenumber, ta.sublinenumber from inbound_Data_assignment ta
        join inbound_data_txn st
        on st.orderid=ta.orderid and st.linenumber=ta.linenumber and st.sublinenumber=ta.sublinenumber
        and st.recordstatus=0 and st.filename  = v_inbound_cfg_parameter.file_name and st.filedate=v_inbound_cfg_parameter.file_Date
        join cs_position@stelext pos on st.genericattribute4=pos.name
        join cs_position@stelext mgr on pos.managerseq=mgr.ruleelementownerseq
                  where   pos.removedate>sysdate and mgr.removedate>sysdate
         and  st.compensationdate between pos.effectivestartdate and pos.effectiveenddate-1
         and  st.compensationdate between mgr.effectivestartdate and mgr.effectiveenddate-1
        group by  ta.orderid, ta.linenumber, ta.sublinenumber
    ) src
    on (src.orderid = tgt.orderid and src.linenumber=tgt.linenumber and src.sublinenumber=tgt.sublinenumber)
    when matched then update set tgt.genericattribute1 = src.mgrname
    where  filename  = v_inbound_cfg_parameter.file_name and filedate=v_inbound_cfg_parameter.file_Date and recordstatus=0;
    
     v_rowcount := SQL%ROWCOUNT;
    
      SP_LOGGER (
             SUBSTR (
                       v_proc_name
                    || 'Update ManagerName on GA1:'
                    || v_inbound_cfg_parameter.file_type
                    || '-FileName:'
                    || v_inbound_cfg_parameter.file_name
                    || '-Date:'
                    || v_inbound_cfg_parameter.file_date,
                    1,
                255),
             'Update ManagerName on GA1 Execution Completed',
             v_rowcount,
             NULL,
         null);
    
    
    
    merge into inbound_Data_assignment tgt
    using
    (
        select max(mgr.name) oldmgrname, ta.orderid, ta.linenumber, ta.sublinenumber from inbound_Data_assignment ta
        join inbound_data_txn st
        on st.orderid=ta.orderid and st.linenumber=ta.linenumber and st.sublinenumber=ta.sublinenumber
        and st.recordstatus=0 and st.filename  = v_inbound_cfg_parameter.file_name and st.filedate=v_inbound_cfg_parameter.file_Date
        join cs_position@stelext pos on st.genericattribute4=pos.name
        join cs_position@stelext mgr on pos.managerseq=mgr.ruleelementownerseq
                  where   pos.removedate>sysdate and mgr.removedate>sysdate
         and  st.compensationdate-nvl(pos.genericnumber1,0) between pos.effectivestartdate and pos.effectiveenddate-1
         and  st.compensationdate-nvl(pos.genericnumber1,0) between mgr.effectivestartdate and mgr.effectiveenddate-1
        group by  ta.orderid, ta.linenumber, ta.sublinenumber
    ) src
    on (src.orderid = tgt.orderid and src.linenumber=tgt.linenumber and src.sublinenumber=tgt.sublinenumber)
    when matched then update set tgt.genericattribute2 = src.oldmgrname
    where  filename  = v_inbound_cfg_parameter.file_name and filedate=v_inbound_cfg_parameter.file_Date and recordstatus=0;
    
       v_rowcount := SQL%ROWCOUNT;
    
      SP_LOGGER (
             SUBSTR (
                       v_proc_name
                    || 'Update Old ManagerName on GA2:'
                    || v_inbound_cfg_parameter.file_type
                    || '-FileName:'
                    || v_inbound_cfg_parameter.file_name
                    || '-Date:'
                    || v_inbound_cfg_parameter.file_date,
                    1,
                255),
             'Update Old ManagerName on GA2 Execution Completed',
             v_rowcount,
             NULL,
         null);
    
     */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_Data_staging SET field51 = field1, field52= field3, field53=field(...) */
    UPDATE inbound_Data_staging
        SET
        /* ORIGSQL: field51 = */
        field51 = field1,
        /* ORIGSQL: field52 = */
        field52 = field3,
        /* ORIGSQL: field53 = */
        field53 = field5,
        /* ORIGSQL: field1 = */
        field1 = substring(field5,1,LOCATE(Field5,'_',1,1) -1),  /* ORIGSQL: substr(field5,1,instr(Field5,'_')-1) */
        /* ORIGSQL: field3 = */
        field3 = substring(field5,LOCATE(field5,'_',1,1) +1)  /* ORIGSQL: substr(field5,instr(field5,'_')+1) */
    FROM
        inbound_Data_staging;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update1' || v_inbound_cfg_parameter.file_type (...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update1'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update1', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update1' || v_inbound_cfg_parameter.file_type || '-FileNa(...) */

    --handling for sheng siong, to disable the validation for dealer code
    /*update inbound_data_staging t
    set field3='V5225', field5='V5225_V5225', field50=field5
    where t.field1='V5225';
    
     v_rowcount := SQL%ROWCOUNT;
    
    SP_LOGGER (
           SUBSTR (
                     v_proc_name
                  || 'Update for sheng siong:'
                  || v_inbound_cfg_parameter.file_type
                  || '-FileName:'
                  || v_inbound_cfg_parameter.file_name
                  || '-Date:'
                  || v_inbound_cfg_parameter.file_date,
                  1,
              255),
           'Update for Sheng siong',
           v_rowcount,
           NULL,
       null);
    */

    /* ORIGSQL: commit; */
    COMMIT;
END