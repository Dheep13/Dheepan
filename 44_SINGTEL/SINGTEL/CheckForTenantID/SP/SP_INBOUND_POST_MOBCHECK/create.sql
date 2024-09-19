CREATE PROCEDURE EXT.SP_INBOUND_POST_MOBCHECK
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_flag DECIMAL(38,10);  /* ORIGSQL: v_flag number; */
    DECLARE v_date TIMESTAMP;  /* ORIGSQL: v_date date; */
    DECLARE v_filebatch VARCHAR(50) = 'MOBILE';  /* ORIGSQL: v_filebatch varchar2(50):='MOBILE'; */
    DECLARE v_param ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_MOBCHECK';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_MOBCHECK'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    /* ORIGSQL: NULL; */
    DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_param
    FROM
        ext.inbound_Cfg_parameter;

    /*Arjun 20190221 Ignore the filebatch stuff. logic will be moved to SH wherever possible*/
    --mark current file as received.
    /*
      insert into inbound_Work_receivedfiles(filetype, importdatetime,filedate, maxcompdate)
      select a.file_type, trunc(sysdate), a.file_date, b.maxcompdate
      from inbound_Cfg_parameter a
      left join (select max(Compensationdate) maxcompdate, filedate, filename from inbound_Data_txn where recordstatus=0
      group by filedate, filename) b
      on a.file_name=b.filename and a.file_Date=b.filedate
      minus
      select filetype, importdatetime,filedate, maxcompdate
      from inbound_Work_receivedfiles
      ;
    
              v_rowcount := SQL%ROWCOUNT;
    
          SP_LOGGER (
                 SUBSTR (
                           v_proc_name
                        || 'Mark Current file is recieved :'
                        || v_param.file_type
                        || '-FileName:'
                        || v_param.file_name
                        || '-Date:'
                        || v_param.file_date,
                        1,
                    255),
                 'Mark Current file is recieved Execution Completed',
                 v_rowcount,
                 NULL,
             null);
    
    
    
      --check if all files for the day have been received
       -- Assuming all files come after 12am, this logic should work fine for now
       -- For error handling/recovery etc. this check will need to be run manually
       --the above table may need to be cleared for the date when multiple files ar ebeing loaded
    
       select
       case when
      (select count(*) expCount
              from inbound_Cfg_filebatch
          where filebatch = v_filebatch)  =
      (select count(*) actCount
              from inbound_Work_receivedfiles a
              join inbound_Cfg_filebatch b
              on b.filetype=a.filetype
          where a.filedate=v_param.file_Date)
      then 1
      else 0
      end
      into v_flag
      from dual;
    
    
      /#*
    These fields need to be updated:
    Channel - SERS or TEPL - do in mobjoin (can be done earlier, but it might be better for error rereuns to do everything in one place)
    GA28 with stock code  - do in mobjoin
    ga18 with data only indicator(free/paid)  - do in mobprocess
    ga19 with Sim only indicator  - do in process
    ga20 - Mobile/Mobileshare  - do in mobprocess
    GN1 with Equip Code - do in mobjoin
    GN3 with Delta ARPU -Stahegook
    Gn5/6 with Discount and Voucher - do in mobjoin
    
    TAssignment
    Gn1 - Discount qty - do in mobjoin
    Gn2 - Voucher Qty - do in mobjoin
    
    
    **/

    --if yes (all files have been received), join and trigger validations
    /*  if v_flag=1 then*/
    --then join and load into inbound_data_txnmobile , the process
    IF :v_param.file_Type LIKE 'BCC%Mobile%' 
    THEN
        /*
         */
        /* ORIGSQL: sp_inbound_post_mobjoin() */
        CALL EXT.SP_INBOUND_POST_MOBJOIN();

        /* ORIGSQL: sp_inbound_post_mobprocess() */
        CALL EXT.SP_INBOUND_POST_MOBPROCESS();

        /* ORIGSQL: sp_inbound_txn_map (v_param.file_type, v_param.file_name, v_param.file_date, 2) */
        CALL EXT.SP_INBOUND_TXN_MAP(:v_param.file_type, :v_param.file_name, :v_param.file_date, 2);
    END IF;
    /*
      end if;
    */

    --if not, do nothing
END