--liquibase formatted sql

--changeset jcadby:KYN_Lib_Schedule splitStatements:false stripComments:false
--comment: Create library
--ignoreLines:1
set schema ext;

create or replace library EXT.KYN_Lib_Schedule default schema ext as
begin

  private variable c_eot constant date := to_Date('22000101','YYYYMMDD');
  private variable c_process_name constant varchar(255) := ::CURRENT_OBJECT_NAME;
  private variable c_log_prefix constant varchar(100) := '['||::CURRENT_OBJECT_NAME||'] ';
  private variable v_uuid varchar(100);
  
  private procedure log(
    IN i_text      varchar(4000) default null,
    IN i_value     decimal(25,10) default null
  ) as
  begin
    kyn_prc_debug(:c_log_prefix||:i_text, :i_value, :v_uuid);
  end;

  PUBLIC procedure create_lock(in i_process_name varchar(255), out o_success boolean) as    
  begin
    -- if the insert fails due to unique index error then set status to failed
    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 301
    begin
      o_success := false;
    end;
    insert into ext.kyn_lock (process_name, connection_id, user_name) values (:i_process_name, current_connection, current_user);  
    commit;
    o_success := true;
  end;
  
  PUBLIC procedure remove_lock(in i_process_name varchar(255) ) as  
  begin
    
    update ext.kyn_lock 
    set remove_date = current_timestamp 
    where process_name = :i_process_name 
    and remove_date = :c_eot;
    
    commit;  
  end;
  
  PUBLIC procedure update_lock_message(in i_process_name varchar(255), in i_message varchar(4000)) as
  begin
    
    update ext.kyn_lock 
    set message = :i_message
    where process_name = :i_process_name 
    and remove_date = :c_eot;
    
    commit;  
  end;
  
  PUBLIC procedure delete_lock as
  begin
  
    delete from ext.kyn_lock
    where remove_date is not null
      and remove_date != :c_eot
      -- keep last hour info for testing
      and remove_date < add_seconds(current_timestamp, -3600);
      
    commit;

  end;
  
  public procedure set_due_date() as
  begin
    declare v_count integer;

    update kyn_schedule
    set last_due_date = start_date
    where last_due_date is null;

    update kyn_schedule
    set due_date = last_due_date
    where due_date is null
    and months + days + hours > 0;
    
    update kyn_schedule
    set due_date = add_days(last_due_date, days_between(last_due_date, current_timestamp))
    where due_date is null
    and last_due_date < current_timestamp
    and months + days + hours = 0
    and minutes > 0;
    
    v_count := 1;

    while :v_count > 0
    do
      update kyn_schedule
      set due_date = add_seconds(add_days(add_months(due_date, months), days), (hours*60+minutes)*60)
      where due_date < current_timestamp
        and months + days + hours + minutes > 0;
      v_count := ::ROWCOUNT;
    end while;
    commit;
  end;

  public procedure run() as
  begin
    declare v_lock_success boolean;
    declare v_schedule_row row like kyn_schedule;
    declare cursor c_schedule for
      select * from kyn_schedule 
      where current_timestamp >= due_date
      and active = 1
      order by call_order;
      
    v_uuid := SYSUUID;

    log('Start');
    
    create_lock(:c_process_name, v_lock_success);
    if :v_lock_success = false then
      log('Lock already exists - exit');
      return;
    end if;
    
    -- deactivate records
    update kyn_schedule
    set active = 0
    where current_timestamp > end_date
    and active = 1;
    commit;
   
    for x as c_schedule
    do
      log(:x.call_sql);

      v_schedule_row.message := null;

      begin      
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        begin
          rollback;
          kyn_prc_debug_error (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :v_uuid);
          v_schedule_row.message := ::SQL_ERROR_MESSAGE;
        END;
        execute immediate :x.call_sql;
      end;
      
      update kyn_schedule 
      set
        last_run_date = current_timestamp,
        last_due_date = :x.due_date,
        message = :v_schedule_row.message
      where schedule_key = :x.schedule_key;
      commit;
    end for;
    log('done loop');

    set_due_date();
    log('done set_due_date');
    
    remove_lock(:c_process_name);
    delete_lock();
    
    log('End');
    
  end;

end;