--liquibase formatted sql

--changeset jcadby:KYN_Lib_Pipeline splitStatements:false stripComments:false
--comment: Create library
--ignoreLines:1
set schema ext;

create or replace library EXT.KYN_Lib_Pipeline default schema ext as
begin

  public variable c_systemUser  constant varchar(50) := '__^^CallidusSystemUser^^__';

  public variable c_import      constant nvarchar(127) := 'Import';
  public variable c_pipelineRun constant nvarchar(127) := 'PipelineRun';

  public variable c_compensateAndPay    constant nvarchar(3999) := 'CompensateAndPay';
  public variable c_allocate            constant nvarchar(3999) := 'Allocate';
  public variable c_pay                 constant nvarchar(3999) := 'Pay';  
  public variable c_summarize           constant nvarchar(3999) := 'Summarize';
  public variable c_validateAndTransfer constant nvarchar(3999) := 'ValidateAndTransfer';
  public variable c_resetFromValidate   constant nvarchar(3999) := 'ResetFromValidate';
  public variable c_dataExtracts        constant nvarchar(3999) := 'DataExtracts';
  
  public variable c_full        constant nvarchar(30) := 'full';
  public variable c_incremental constant nvarchar(30) := 'incremental';
  
/*
Valid module names are: TransactionalData OrganizationData ClassificationData PlanRelatedData TP_SupplementData TP_OrganizationData TP_CredentialData TM_ObjectiveRelatedData TQ_AlignmentData TQ_HistoricalData TQ_TerritoryProgramData
*/   
  
  public variable c_ClassificationData    constant nvarchar(100) := 'ClassificationData';  
  public variable c_GenericClassifier     constant nvarchar(100) := 'GenericClassifier';
  public variable c_PostalCode            constant nvarchar(100) := 'PostalCode';
  public variable c_Product               constant nvarchar(100) := 'Product';
  public variable c_Category              constant nvarchar(100) := 'Category';
  public variable c_Customer              constant nvarchar(100) := 'Customer';
  public variable c_Category_Classifiers  constant nvarchar(100) := 'Category_Classifiers';

  public variable c_TransactionalData   constant nvarchar(100) := 'TransactionalData';  
  public variable c_TransactionAndCredit constant nvarchar(100) := 'TransactionAndCredit';
  public variable c_Deposit              constant nvarchar(100) := 'Deposit';
  
  public variable c_OrganizationData    constant nvarchar(100) := 'OrganizationData';
  public variable c_Participant         constant nvarchar(100) := 'Participant';
  public variable c_Position            constant nvarchar(100) := 'Position';
  
  public variable c_PlanRelatedData     constant nvarchar(100) := 'PlanRelatedData';
  public variable c_Quota               constant nvarchar(100) := 'Quota';
  
  public variable c_TQ_HistoricalData     constant nvarchar(100) := 'TQ_HistoricalData';
  
  public variable c_TQ_AlignmentData     constant nvarchar(100) := 'TQ_AlignmentData';
  public variable c_Account              constant nvarchar(100) := 'Account';
  
  public variable c_TQ_TerritoryProgramData     constant nvarchar(100) := 'TQ_TerritoryProgramData';
  public variable c_TerritoryQuota              constant nvarchar(100) := 'TerritoryQuota';
  
  public variable c_ETL constant nvarchar(100) := 'ETL';

  public variable c_max_wait_time constant integer := 3600; -- 1 hour
  public variable c_sleep_time    constant integer := 10;
  
  private variable c_log_prefix constant varchar(100) := '['||::CURRENT_OBJECT_NAME||'] ';
  
  
  public function get_config(i_name varchar(255), i_default varchar(4000) default null) returns o_value varchar(4000) as
  begin
    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299
    begin
      o_value := :i_default;
    end;
    select value into o_value from kyn_config where upper(name) = upper(:i_name);
  end;
  
  public function get_calculation_run_mode(i_runMode nvarchar(30)) returns v_ret nvarchar(30) as
  begin
    if :i_runMode is null then
      v_ret := get_config('pipeline_calculation_default_run_mode', :c_full);
    else
      v_ret := :i_runMode;
    end if;  
  end;
  
  public function get_pipelinerunseq (IN i_text varchar(4000)) returns v_ret bigint as
  begin
    v_ret := SUBSTR_REGEXPR('(PipelineRuns\()([[:digit:]]*)(\))' IN :i_text GROUP 2);
  end;
 
  public procedure run_pipeline(

    IN i_processingUnit nvarchar(3999) default null, 
    IN i_calendarName nvarchar(100) default 'Fiscal Calendar',
    IN i_periodName nvarchar(50) default null,
    
    IN i_command nvarchar(127) default null, 
    IN i_runMode nvarchar(30) default 'all',
    IN i_stageType nvarchar(3999) default null,
    
    IN i_batchName nvarchar(90) default null,
    IN i_module nvarchar(100) default null,
    IN i_stagetableNames nvarchar(1200) default null,
    IN i_revalidate nvarchar(100) default null,
    
    IN i_dataExtractsFileType nvarchar(255) default null,
    
    IN i_traceLevel nvarchar(30) default 'Status', 
    IN i_userId nvarchar(255) default 'PortalAdmin',

    IN i_startDateScheduled timestamp default null,
    OUT o_message varchar(1000),
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_startDateScheduled timestamp;
    declare v_onDemand varchar(5);
    declare v_schedule_sec integer := 10;
    declare v_sql varchar(4000);
    declare v_debug_message varchar(4000);

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 403
    begin
      kyn_prc_debug(:c_log_prefix||::SQL_ERROR_MESSAGE, ::SQL_ERROR_CODE);
      o_message := ::SQL_ERROR_CODE ||' : '||::SQL_ERROR_MESSAGE;
      o_pipelinerunseq := get_pipelinerunseq(::SQL_ERROR_MESSAGE);
    end;

    v_schedule_sec := get_config('pipeline_schedule_sec', :v_schedule_sec);

    v_startDateScheduled := add_seconds(current_utctimestamp,:v_schedule_sec);
    if :i_startDateScheduled is not null and :i_startDateScheduled > :v_startDateScheduled then
      v_startDateScheduled := :i_startDateScheduled;
    end if;
    
    begin
      declare exit handler for sql_error_code 1299 v_onDemand = 'false';
      select value into v_onDemand
      from cs_preferences
      where name = 'pipeline.onDemand'
      and username = :c_systemUser;
    end;
    
    -- debug message
    v_debug_message = 'run_pipeline(';
    if :i_command              is not null then v_debug_message = :v_debug_message || 'Command=>'              || :i_command                         || ','; end if; 
    if :i_stagetype            is not null then v_debug_message = :v_debug_message || 'StageType=>'            || :i_stagetype                       || ','; end if; 
    if :i_runmode              is not null then v_debug_message = :v_debug_message || 'RunMode=>'              || :i_runmode                         || ','; end if; 
    if :i_periodName           is not null then v_debug_message = :v_debug_message || 'PeriodName=>'           || :i_periodName                      || ','; end if; 
    if :i_batchname            is not null then v_debug_message = :v_debug_message || 'Batchname=>'            || :i_batchname                       || ','; end if; 
    if :i_dataExtractsFileType is not null then v_debug_message = :v_debug_message || 'DataExtractsFileType=>' || :i_dataExtractsFileType            || ','; end if; 
    if :i_stagetableNames      is not null then v_debug_message = :v_debug_message || 'StageTableNames=>'      || :i_stagetableNames                 || ','; end if; 
    if :i_tracelevel           is not null then v_debug_message = :v_debug_message || 'TraceLevel=>'           || :i_tracelevel                      || ','; end if; 
    if :i_userid               is not null then v_debug_message = :v_debug_message || 'UserId=>'               || :i_userid                          || ','; end if; 
    if :i_module               is not null then v_debug_message = :v_debug_message || 'Module=>'               || :i_module                          || ','; end if; 
    if :i_processingUnit       is not null then v_debug_message = :v_debug_message || 'ProcessingUnit=>'       || :i_processingUnit                  || ','; end if; 
    if :i_calendarname         is not null then v_debug_message = :v_debug_message || 'CalendarName=>'         || :i_calendarname                    || ','; end if; 
    if :v_startdatescheduled   is not null then v_debug_message = :v_debug_message || 'StartDateScheduled=>'   || to_varchar(:v_startdatescheduled)  || ','; end if; 
    if :v_onDemand			   is not null then v_debug_message = :v_debug_message || 'ShouldOnDemand=>'       || :v_onDemand						  || ','; end if;
    v_debug_message = rtrim(:v_debug_message,',')||')'; 
    kyn_prc_debug(:c_log_prefix||v_debug_message);
    
    -- this needs to be autonomous otherwise we get other errors	
    v_sql = 'INSERT INTO EXT.VT_PipelineRuns("Command", "StageType", "TraceLevel", "UserId", "RunMode", "BatchName", "Module", "ProcessingUnit", "CalendarName", "StartDateScheduled", "StageTableNames", "PeriodName", "DataExtractsFileType", "ShouldOnDemand", "Revalidate" )';
    v_sql = v_sql || 'values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)';

    begin
        declare exit handler for sqlexception
        begin
          kyn_prc_debug('Failed to run sql "'||:v_sql||'"');
          kyn_prc_debug_error(::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
          SIGNAL SQL_ERROR_CODE 10001 SET MESSAGE_TEXT = 'Error starting pipelines. Check KYN_Debug table.';
        end;
    --
    execute immediate :v_sql using 
    :i_command, 
        :i_stagetype, 
        :i_tracelevel, 
        :i_userid, 
        :i_runmode, 
        :i_batchname, 
        :i_module, 
        :i_processingUnit, 
        :i_calendarname, 
        :v_startdatescheduled, 
        :i_stagetableNames,
        :i_periodName,
        :i_dataExtractsFileType,
        :v_onDemand,
        :i_revalidate;
		-- 
		commit work;
	end;
	   
    begin
      declare exit handler for sqlexception
      begin
        -- do nothing
      end;      
      select pipelinerunseq into o_pipelinerunseq
      from cs_plrun
      where startdatescheduled = UTCTOLOCAL(:v_startdatescheduled);
    end;
  
  end;

  public procedure reset_from_validate(
    IN i_processingUnit nvarchar(3999) default null, 
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_periodName nvarchar(50) default null,  
    IN i_batchname nvarchar(90) default null,
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    declare v_message varchar(1000);
    
    run_pipeline(
      i_command => :c_import,
      i_stagetype => :c_resetFromValidate,       
      i_calendarName => :i_calendarName, 
      i_periodName => :i_periodName,  
      i_batchName => :i_batchname,
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq
    );

  end;
  
  public procedure r_f_v_batch_all(
    IN i_batchname nvarchar(90)
  ) as
  begin
    -- reset from validate all txns belonging to a batch across all relevant periods
    declare v_pipelinerunseq bigint;
    declare cursor c_runs for
    select pr.batchname, pu.name as processingUnit, cal.name as calendarName, per.startdate, per.name as periodName, count(*) as txnCount
    from cs_plrun pr
    join cs_processingunit pu on pr.processingunitseq = pu.processingunitseq
    join cs_calendar cal on pr.calendarseq = cal.calendarseq and cal.removedate > current_timestamp
    join cs_period per on cal.calendarseq = per.calendarseq and cal.minorperiodtypeseq = per.periodtypeseq and per.removedate > current_timestamp
    join cs_salestransaction st on pr.pipelinerunseq = st.pipelinerunseq and st.compensationdate >= per.startdate and st.compensationdate < per.enddate and st.processingunitseq = pr.processingunitseq
    where pr.batchname = :i_batchname
    group by pr.batchname, pu.name, cal.name, per.startDate, per.name
    order by pr.batchname, pu.name, cal.name, per.startDate;
    
    for x as c_runs
    do
      reset_from_validate(
        i_processingUnit  => :x.processingunit,
        i_calendarName    => :x.calendarName,
        i_periodName      => :x.periodName,
        i_batchname       => :x.batchName,
        o_pipelinerunseq  => v_pipelinerunseq
      );
      
    end for;
    
  end;
  
  public procedure r_f_v_all as
  begin
    -- reset from validate all txns that have been imported
    declare v_pipelinerunseq bigint;
    declare cursor c_runs for
    select pu.name as processingUnit, cal.name as calendarName, per.startdate, per.name as periodName, count(*) as txnCount
    from cs_plrun pr
    join cs_processingunit pu on pr.processingunitseq = pu.processingunitseq
    join cs_calendar cal on pr.calendarseq = cal.calendarseq and cal.removedate > current_timestamp
    join cs_period per on cal.calendarseq = per.calendarseq and cal.minorperiodtypeseq = per.periodtypeseq and per.removedate > current_timestamp
    join cs_salestransaction st on pr.pipelinerunseq = st.pipelinerunseq and st.compensationdate >= per.startdate and st.compensationdate < per.enddate and st.processingunitseq = pr.processingunitseq
    where st.origintypeid = 'imported'
    group by pu.name, cal.name, per.startDate, per.name
    order by pu.name, cal.name, per.startDate;
    
    for x as c_runs
    do
      reset_from_validate(
        i_processingUnit  => :x.processingunit,
        i_calendarName    => :x.calendarName,
        i_periodName      => :x.periodName,
        o_pipelinerunseq  => v_pipelinerunseq
      );
      
    end for;
    
  end;  

  public procedure validate_and_transfer(
    IN i_batchname nvarchar(90),
    IN i_module nvarchar(100),
    IN i_stagetableNames nvarchar(1200) default null,
    IN i_startdatescheduled timestamp default null,
    IN i_runMode nvarchar(50) default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_message varchar(1000);

    run_pipeline(
      i_command => :c_import, 
      i_stagetype => :c_validateAndTransfer, 
      i_batchName => :i_batchname,
      i_runMode => :i_runMode,
      i_module => :i_module,
      i_stagetableNames => :i_stagetableNames,
      i_revalidate => 'all',
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq);
    
  end;
  
  public procedure v_and_t_genericclassifier(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint    
  ) as
  begin
    validate_and_transfer(:i_batchname, :c_ClassificationData, :c_GenericClassifier, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end;
  
  public procedure v_and_t_category_classifiers(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin   
    validate_and_transfer(:i_batchname, :c_ClassificationData, :c_Category_Classifiers, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end;
  
  public procedure v_and_t_salestransaction(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    declare v_runMode nvarchar(50);
    v_runMode := get_config('pipeline_v_and_t_default_run_mode','new');
    validate_and_transfer(:i_batchname, :c_TransactionalData, :c_TransactionAndCredit, :i_startdatescheduled, :v_runMode, o_pipelinerunseq);
  end;
  
  public procedure v_and_t_participant(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    validate_and_transfer(:i_batchname, :c_OrganizationData, :c_Participant, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end;

  public procedure v_and_t_position(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    validate_and_transfer(:i_batchname, :c_OrganizationData, :c_Position, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end;

  public procedure v_and_t_quota(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    validate_and_transfer(:i_batchname, :c_PlanRelatedData, :c_Quota, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end;
  
  public procedure v_and_t_tq_historic(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    validate_and_transfer(:i_batchname, :c_TQ_HistoricalData, null, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end; 
  
  public procedure v_and_t_tq_account(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    validate_and_transfer(:i_batchname, :c_TQ_AlignmentData, :c_Account, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end; 

  public procedure v_and_t_tq_territory_quota(
    IN i_batchname nvarchar(90),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
    validate_and_transfer(:i_batchname, :c_TQ_TerritoryProgramData, :c_TerritoryQuota, :i_startdatescheduled, 'all', o_pipelinerunseq);
  end;

  public procedure data_extract(
    IN i_processingUnit nvarchar(3999) default null, 
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_periodName nvarchar(50),
    IN i_fileType nvarchar(255),
    IN i_startdatescheduled timestamp default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_message varchar(1000);
    
    run_pipeline(
      i_command => :c_pipelinerun, 
      i_stagetype => :c_dataExtracts, 
      i_processingUnit => :i_processingUnit,
      i_calendarName => :i_calendarName,
      i_periodName => :i_periodName,
      i_DataExtractsFileType => :i_fileType,
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq);  
  
  end;
  
  public procedure compensate_and_pay(
    IN i_processingUnit nvarchar(3999) default null, 
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_periodName nvarchar(50),
    IN i_startdatescheduled timestamp default null,
    IN i_runMode nvarchar(50) default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_message varchar(1000);
    declare v_runMode nvarchar(30) := get_calculation_run_mode(:i_runMode);
    
    run_pipeline(
      i_command => :c_pipelinerun, 
      i_stagetype => :c_compensateAndPay, 
      i_processingUnit => :i_processingUnit,
      i_calendarName => :i_calendarName,
      i_periodName => :i_periodName,
      i_runMode => :v_runMode,
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq);  
  
  end;
  
  public procedure allocate(
    IN i_processingUnit nvarchar(3999) default null, 
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_periodName nvarchar(50),
    IN i_startdatescheduled timestamp default null,
    IN i_runMode nvarchar(50) default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_message varchar(1000);
    declare v_runMode nvarchar(30) := get_calculation_run_mode(:i_runMode);
    
    run_pipeline(
      i_command => :c_pipelinerun, 
      i_stagetype => :c_allocate, 
      i_processingUnit => :i_processingUnit,
      i_calendarName => :i_calendarName,
      i_periodName => :i_periodName,
      i_runMode => :v_runMode,
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq);  
  
  end;
  
  public procedure pay(
    IN i_processingUnit nvarchar(3999) default null, 
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_periodName nvarchar(50),
    IN i_startdatescheduled timestamp default null,
    IN i_runMode nvarchar(50) default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_message varchar(1000);
    declare v_runMode nvarchar(30) := get_calculation_run_mode(:i_runMode);
    
    run_pipeline(
      i_command => :c_pipelinerun, 
      i_stagetype => :c_pay, 
      i_processingUnit => :i_processingUnit,
      i_calendarName => :i_calendarName,
      i_periodName => :i_periodName,
      i_runMode => :v_runMode,
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq);  
  
  end;  
  
  public procedure summarize(
    IN i_processingUnit nvarchar(3999) default null,
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_periodName nvarchar(50),
    IN i_startdatescheduled timestamp default null,
    IN i_runMode nvarchar(50) default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_message varchar(1000);
    declare v_runMode nvarchar(30) := get_calculation_run_mode(:i_runMode);

    run_pipeline(
      i_command => :c_pipelinerun, 
      i_stagetype => :c_summarize, 
      i_processingUnit => :i_processingUnit,
      i_calendarName => :i_calendarName,
      i_periodName => :i_periodName,
      i_runMode => :v_runMode,
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq);  

  end;
  
  public procedure update_analytics(
    IN i_processingUnit nvarchar(3999) default null,
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_periodName nvarchar(50),
    IN i_startdatescheduled timestamp default null,
    IN i_runMode nvarchar(50) default null,
    OUT o_pipelinerunseq bigint
  ) as
  begin
  
    declare v_message varchar(1000);
    declare v_runMode nvarchar(30) := get_calculation_run_mode(:i_runMode);

    run_pipeline(
      i_command => :c_pipelinerun, 
      i_stagetype => :c_etl, 
      i_processingUnit => :i_processingUnit,
      i_calendarName => :i_calendarName,
      i_periodName => :i_periodName,
      i_runMode => :v_runMode,
      i_startdatescheduled => :i_startdatescheduled,
      o_message => v_message,
      o_pipelinerunseq => o_pipelinerunseq);  

  end;  
  
  public procedure pipline_watch(IN i_pipelineRunSeq bigint, OUT o_status varchar(20)) as
  begin
    declare v_stop_time timestamp;
    declare v_count integer;

    select count(*) into v_count from cs_plrun where pipelinerunseq = :i_pipelineRunSeq;

    if :v_count = 0 then
      o_status := 'NOT FOUND';
      return;
    end if;

    v_stop_time := add_seconds(current_timestamp, :c_max_wait_time);

    while current_timestamp < :v_stop_time do
    
      select count(*) into v_count
      from cs_plrun 
      where pipelinerunseq = :i_pipelineRunSeq
        and stoptime is not null;
        
      if :v_count = 1 then
        break;
      end if;

      SQLScript_Sync:Sleep_Seconds(:c_sleep_time);

    end while;
    
    select status into o_status
    from cs_plrun 
    where pipelinerunseq = :i_pipelineRunSeq;

  end;
  
  public procedure summarize_runnable(
    IN i_processingUnit nvarchar(3999) default null,
    IN i_calendarName nvarchar(100) default 'Main Monthly Calendar',
    IN i_limit integer default 10
  ) as
  begin
    declare v_pipelinerunseq bigint;
    declare cursor c_runs for
    select * from (
      select
        per.startdate,
        per.name as periodname,
        count(*) as txn_count, 
        row_number() over (order by per.startdate) as rn
      from cs_salestransaction st
      join cs_period per on st.compensationdate >= per.startdate and st.compensationdate < per.enddate and per.removedate > current_timestamp
      join cs_calendar cal on per.calendarseq = cal.calendarseq and cal.removedate > current_timestamp and cal.minorperiodtypeseq = per.periodtypeseq
      join cs_processingunit pu on st.processingunitseq = pu.processingunitseq
      where st.isrunnable = 1 
      and cal.name = :i_calendarName
      and pu.name = ifnull(:i_processingUnit, 'Unassigned')
      and not exists (
        select 1 
        from cs_plrun pr
        join cs_stagetype st on pr.stagetypeseq = st.stagetypeseq        
        where pr.processingunitseq = pu.processingunitseq 
        and pr.periodseq = per.periodseq
        and pr.stoptime is null
        and st.name = :c_summarize
      )
      group by per.name, per.startdate
    )
    where rn <= :i_limit
    order by rn;
    
    for x as c_runs
    do
      summarize(
        i_processingUnit => :i_processingUnit,
        i_calendarName => :i_calendarName,
        i_periodName => :x.periodName,
        o_pipelinerunseq => v_pipelinerunseq
      );
      SQLScript_Sync:Sleep_Seconds(1); -- 1 second gap
    end for;

  end;

end;
