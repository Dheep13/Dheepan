CREATE PROCEDURE EXT.SP_INBOUND_POST_DEL
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
    DECLARE v_productid VARCHAR(200);  /* ORIGSQL: v_productid VARCHAR2(200); */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_DEL';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_DEL'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        ext.INBOUND_CFG_PARAMETER;

    /*select max(classifierid) into v_productid
    from stel_classifier@stelext
    where categoryname='DEL' and categorytreename='Singtel'
    and classfiername='Residential DEL';*/
    --bugfix by kyap, 1st or 2nd residential product id is 8860942
    --DEL was developed before BU spilt, classifierid is now "Sales Channel - Product ID", hence use classfiername (product id)
    SELECT
        MAX(classfiername) 
    INTO
        v_productid
    FROM
        EXT.stel_classifier
        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_classifier_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
        categoryname = 'DEL'
        AND categorytreename = 'Singtel'
        AND UPPER(description) LIKE '%RESIDENTIAL%';

    /*
    
    -- comapre existing data loaded for this month and the current file, per customernric to determine the # of res DELS sold
    execute immediate 'truncate table stel_temp_Delresidential';
    insert into stel_Temp_Delresidential
    select custid,compdate, vendor, count(*)-1  from
    (
        select custid, last_Day(compensationdate) compdate ,genericattribute3 vendor from vw_Salestransaction@stelext st
        join cs_eventtype@stelext et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        where et.eventtypeid='DEL Closed'
        and productid= v_productid
        union all
        select field11, last_day(to_date(field22,'DDMMYYYY')), field14 from inbound_Data_Staging
        where filetype like 'BCC%FixedVoice%' and nvl(error_Flag,0)=0
    and field13 = v_productid) a
    group by custid, vendor , compdate
    having count(*)>1
    ;
    
           v_rowcount := SQL%ROWCOUNT;
    
        SP_LOGGER (
               SUBSTR (
                         v_proc_name
                      || 'Insert into STEL_TEMP_DELRESIDENTIAL  :'
                      || v_inbound_cfg_parameter.file_type
                      || '-FileName:'
                      || v_inbound_cfg_parameter.file_name
                      || '-Date:'
                      || v_inbound_cfg_parameter.file_date,
                      1,
                  255),
               'Insert into STEL_TEMP_DELRESIDENTIAL  Execution Completed',
               v_rowcount,
               NULL,
           null);
    
    
    commit;
    */
    /*
    INSERT INTO INBOUND_DATA_TXN
      (
            FILEDATE,    FILENAME,    RECORDSTATUS,
            DOWNLOADED,    ORDERID,    LINENUMBER,    SUBLINENUMBER,
            EVENTTYPEID,        PRODUCTID,    VALUE,    UNITTYPEFORVALUE,
            NUMBEROFUNITS,    COMPENSATIONDATE,        DATASOURCE,
        
            BILLTOCUSTID,     BUSINESSUNITNAME,     GENERICATTRIBUTE3, genericnumber1, unittypeforgenericnumber1
        
      )
     select  v_inbound_cfg_parameter.file_Date, v_inbound_cfg_parameter.file_name, 0,0,
    
     custid||'-'||to_Char(compdate,'YYYYMMDD')||'-'||vendor, 1, 1, 'DEL Closed', v_productid, 0, 'SGD', cnt
     , compdate, 'BCC>LandingPad', custid, 'EXTERNAL',
     vendor, cnt, 'quantity'
       from stel_temp_delresidential;
    
                v_rowcount := SQL%ROWCOUNT;
    
          SP_LOGGER (
                 SUBSTR (
                           v_proc_name
                        || 'Insert into INBOUND_DATA_TXN  :'
                        || v_inbound_cfg_parameter.file_type
                        || '-FileName:'
                        || v_inbound_cfg_parameter.file_name
                        || '-Date:'
                        || v_inbound_cfg_parameter.file_date,
                        1,
                    255),
                 'Insert into INBOUND_DATA_TXN  Execution Completed',
                 v_rowcount,
                 NULL,
             null);
    
    
      insert into inbound_Data_Assignment( FILEDATE,
          FILENAME,
          RECORDSTATUS,
          DOWNLOADED,
          ORDERID,
          LINENUMBER,
          SUBLINENUMBER,
          EVENTTYPEID,
       POSITIONNAME)
     select  v_inbound_cfg_parameter.file_Date, v_inbound_cfg_parameter.file_name, 0,0,
      custid||'-'||to_Char(compdate,'YYYYMMDD')||'-'||vendor, 1, 1, 'DEL Closed', vendor
       from stel_temp_delresidential;
    
              v_rowcount := SQL%ROWCOUNT;
    
          SP_LOGGER (
                 SUBSTR (
                           v_proc_name
                        || 'Insert into INBOUND_DATA_ASSIGNMENT  :'
                        || v_inbound_cfg_parameter.file_type
                        || '-FileName:'
                        || v_inbound_cfg_parameter.file_name
                        || '-Date:'
                        || v_inbound_cfg_parameter.file_date,
                        1,
                    255),
                 'Insert into INBOUND_DATA_ASSIGNMENT  Execution Completed',
                 v_rowcount,
                 NULL,
             null);
      */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_TXN' not found */
    /* ORIGSQL: update inbound_Data_txn tgt SET genericnumber1=-999, unittypeforgenericnumber1='(...) */
    UPDATE ext.inbound_Data_txn tgt
        SET
        /* ORIGSQL: genericnumber1 = */
        genericnumber1 = -999,
        /* ORIGSQL: unittypeforgenericnumber1 = */
        unittypeforgenericnumber1 = 'quantity' 
    WHERE
        productid = :v_productid
        AND filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date
        AND recordstatus = 0;/* -- and datasource<>'BCC>LandingPad' */

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update GN1 INBOUND_DATA_TXN :' || v_inbound_cf(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update GN1 INBOUND_DATA_TXN  :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update GN1 INBOUND_DATA_TXN Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update GN1 INBOUND_DATA_TXN  :' || v_inbound_cfg_paramete(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /**
    
    These are the possible scenarios for the first DEL record:
    
    -No del record in TC and single del comes in the file -leave as zero
    -No del record in TC and multiple del comes in the file - update all  except 1 to -999
    
    -First del record exists in TC and seocnd doesn't and  single del comes into the file -  set to -999
    -First del record exists in TC nd seocnd doesn't and  multiple dels comes into the file -  set to -999
    
    first 2nd del exist in TC and new one comes into file -  have to update the existing rec
    first 2nd del exist in TC and multiple one comes into file -  have to update the existing rec
    
    
    
    
    merge into  inbound_Data_txn tgt
    using (
        select custid, last_Day(compensationdate) compdate, count(*) cnt from vw_Salestransaction@stelext st
        join cs_eventtype@stelext et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        where et.eventtypeid='DEL Closed'
        and productid= v_productid
        --and compensationdate between
        group by custid, last_Day(compensationdate)
    ) src
    on (src.custid = tgt.billtocustid and compdate= last_Day(tgt.compensationdate))
    when matched then update set genericnumber6 = src.cnt
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0;
    
    
    merge into  inbound_Data_txn tgt
    using (
        select custid, last_Day(compensationdate) compdate, count(*) cnt from vw_Salestransaction@stelext st
        join cs_eventtype@stelext et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        where et.eventtypeid='DEL Closed'
        and productid= v_productid
        and datasource = 'BCC>LandingPad'
        --and compensationdate between
        group by custid, last_Day(compensationdate)
    ) src
    on (src.custid = tgt.billtocustid and compdate= last_Day(tgt.compensationdate))
    when matched then update set genericnumber5 = src.cnt
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0;
    
    
    merge into  inbound_Data_txn tgt
    using (
        select  st.billtocustid custid, last_Day(compensationdate) compdate, count(*) cnt
        from inbound_Data_Txn st
        
        where st.productid=v_productid
        and  st.filename=v_inbound_cfg_parameter.file_name
        and  st.filedate=v_inbound_cfg_parameter.file_Date
        and  st.recordstatus=0
        and  st.productid= v_productid
        --and compensationdate between
        group by st.billtocustid, last_Day(compensationdate)
    ) src
    on (src.custid = tgt.billtocustid and compdate= last_Day(tgt.compensationdate))
    when matched then update set genericnumber4 = src.cnt
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0;
    
    
    --gn4. count in this file
    --gn5 - does 2nd del exist in TC
    --gn6 - does first del exist in TC
    
    
    
    
    --No del record in TC and single del comes in the file -leave as zero
    --No del record in TC and multiple del comes in the file - update all  except 1 to -999
    
    
    update inbound_Data_txn tgt
    set genericnumber1=-999, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and nvl(genericnumber6,0)=0;
    
    update inbound_Data_txn tgt
    set genericnumber1=0, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and nvl(genericnumber6,0)=0 and nvl(genericnumber4,0)>0
    and rowid = (Select max(rowid) rid from inbound_Data_txn s
        where s.billtocustid=tgt.billtocustid
        and  s.productid=v_productid
        and s.filename=v_inbound_cfg_parameter.file_name
        and s.filedate=v_inbound_cfg_parameter.file_Date
        and s.recordstatus=0
    ) ;
    
    
    
    --First del record exists in TC and seocnd doesn't and  single del comes into the file -  set to -999
    --First del record exists in TC nd seocnd doesn't and  multiple dels comes into the file -  set to -999
    
    update inbound_Data_txn tgt
    set genericnumber1=-999, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and nvl(genericnumber6,0)>0 and nvl(Genericnumber5,0)=0;
    
    
    --first 2nd del exist in TC and new one comes into file -  have to update the existing rec
    -- first 2nd del exist in TC and multiple one comes into file -  have to update the existing rec
    
    
    update inbound_Data_txn tgt
    set genericnumber1=-999, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and nvl(genericnumber6,0)>0 and nvl(Genericnumber5,0)>0;
    
    --for one rec, change the value to -gn5
    
    update inbound_Data_txn tgt
    set genericnumber1=-genericnumber5, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and nvl(genericnumber6,0)>0 and nvl(Genericnumber5,0)>0
    and rowid = (Select max(rowid) rid from inbound_Data_txn s
        where s.billtocustid=tgt.billtocustid
        and  s.productid=v_productid
        and s.filename=v_inbound_cfg_parameter.file_name
        and s.filedate=v_inbound_cfg_parameter.file_Date
        and s.recordstatus=0
    ) ;
    
    
    
    
    /#
    --for the above list, for existing transactions, remove res del txns if they're not the first
    --  is there already a txn in TC? Set gn1=-9999
    
    update inbound_Data_txn tgt
    set genericnumber1=-999, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and ( exists (
            select 1 from vw_Salestransaction@stelext st
            join cs_eventtype@stelext et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
            where et.eventtypeid='DEL Closed'
            and productid= v_productid
            and last_Day(st.Compensationdate)= last_Day(tgt.compensationdate)
            and custid = tgt.billtocustid
        )
        or
        exists
        (
            select 1
            from inbound_data_txn s
            where s.productid = v_productid
            and s.filename=v_inbound_cfg_parameter.file_name
            and s.filedate=v_inbound_cfg_parameter.file_Date
            and s.recordstatus=0
            and s.billtocustid = tgt.billtocustid
            and tgt.rowid<>s.rowid
    ))
    
    ;
    
    v_rowcount := SQL%ROWCOUNT;
    
        SP_LOGGER (
               SUBSTR (
                         v_proc_name
                      || 'Update GN1=-999 for existing txns in INBOUND_DATA_TXN  :'
                      || v_inbound_cfg_parameter.file_type
                      || '-FileName:'
                      || v_inbound_cfg_parameter.file_name
                      || '-Date:'
                      || v_inbound_cfg_parameter.file_date,
                      1,
                  255),
               'Update GN1=-999 for existing txns in INBOUND_DATA_TXN Execution Completed',
               v_rowcount,
               NULL,
           null);
    
    
    commit;
    
    -- is there no txn in TC? pick ONE row from this current dataset
    
    update inbound_Data_txn tgt
    set genericnumber1=numberofunits, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and numberofunits<>1
    --/#and not exists (
        select 1 from vw_Salestransaction@stelext st
        join cs_eventtype@stelext et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        where et.eventtypeid='DEL Closed'
        and productid= v_productid
        and last_Day(st.Compensationdate)= last_Day(tgt.compensationdate))*/
    /*and (orderid, linenumber,sublinenumber, rowid) in (
        select orderid, linenumber,sublinenumber, max(rowid) rd
        from inbound_data_txn
        where productid = v_productid
        and filename=v_inbound_cfg_parameter.file_name
        and filedate=v_inbound_cfg_parameter.file_Date
        and recordstatus=0
        group by orderid, linenumber,sublinenumber
    )
    
    ;
    
    v_rowcount := SQL%ROWCOUNT;
    
        SP_LOGGER (
               SUBSTR (
                         v_proc_name
                      || 'Update GN1=-999 for for one row if not exit in INBOUND_DATA_TXN  :'
                      || v_inbound_cfg_parameter.file_type
                      || '-FileName:'
                      || v_inbound_cfg_parameter.file_name
                      || '-Date:'
                      || v_inbound_cfg_parameter.file_date,
                      1,
                  255),
               'Update GN1=-999 for for one row if not exit in INBOUND_DATA_TXN Execution Completed',
               v_rowcount,
               NULL,
           null);
    
    --set back to 0 for the first line record, where the recs don't exist in TC and multiple main lines are in the same file
    
    update inbound_Data_txn tgt
    set genericnumber1=0, unittypeforgenericnumber1='quantity'
    where productid=v_productid
    and filename=v_inbound_cfg_parameter.file_name
    and filedate=v_inbound_cfg_parameter.file_Date
    and recordstatus=0
    and genericnumber1=-999
    and numberofunits<>1
    /#and not exists (
        select 1 from vw_Salestransaction@stelext st
        join cs_eventtype@stelext et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        where et.eventtypeid='DEL Closed'
        and productid= v_productid
        and last_Day(st.Compensationdate)= last_Day(tgt.compensationdate))*/
    /*and (orderid, linenumber,sublinenumber, rowid) in (
        select orderid, linenumber,sublinenumber, max(rowid) rd
        from inbound_data_txn
        where productid = v_productid
        and filename=v_inbound_cfg_parameter.file_name
        and filedate=v_inbound_cfg_parameter.file_Date
        and recordstatus=0
        and genericnumber1=-999
        group by orderid, linenumber,sublinenumber
    )
    
    ;
    
    v_rowcount := SQL%ROWCOUNT;
    
        SP_LOGGER (
               SUBSTR (
                         v_proc_name
                      || 'Update GN1=0 for for first DEL row if not exist in INBOUND_DATA_TXN  :'
                      || v_inbound_cfg_parameter.file_type
                      || '-FileName:'
                      || v_inbound_cfg_parameter.file_name
                      || '-Date:'
                      || v_inbound_cfg_parameter.file_date,
                      1,
                  255),
               'Update GN1=-999 for for one row if not exit in INBOUND_DATA_TXN Execution Completed',
               v_rowcount,
               NULL,
           null);
    
    */

    /* ORIGSQL: commit; */
    COMMIT;
END