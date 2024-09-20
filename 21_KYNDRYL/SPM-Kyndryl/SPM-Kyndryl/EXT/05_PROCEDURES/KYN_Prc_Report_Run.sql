--liquibase formatted sql

--changeset jcadby:KYN_Prc_Report_Run splitStatements:false stripComments:false
--comment: Create procedure
--ignoreLines:1
set schema ext;

create or replace procedure EXT.KYN_Prc_Report_Run(i_date timestamp default null) default schema ext as
begin
  declare c_eot constant timestamp := '2200-01-01';
  declare c_calendar_name constant varchar(50) := 'Fiscal Calendar';
  declare c_year constant varchar(50) := 'year';
  declare v_date timestamp := ifnull(i_date, current_timestamp);
  declare v_pipelinerunseq bigint;
  declare cursor c_periods for
  select 
    cal.calendarseq,
    cal.name as calendar_name,
    per_y.periodseq year_periodseq,
    per_y.name year_name,
    per_sa.periodseq as semiannual_periodseq,
    per_sa.name as semiannual_name,
    per_m.periodseq as month_periodseq,
    per_m.name as month_name,
    per_m.startdate month_startdate
  from cs_calendar cal
  join cs_period per_y on per_y.calendarseq = cal.calendarseq and cal.removedate = :c_eot
  join cs_period per_sa on per_sa.parentseq = per_y.periodseq and per_sa.removedate = :c_eot
  join cs_period per_m on 
    per_m.periodtypeseq = cal.minorperiodtypeseq 
    and per_m.calendarseq= cal.calendarseq
    and per_m.removedate = :c_eot
    and per_m.startdate >= per_sa.startdate 
    and per_m.enddate <= per_sa.enddate
  where per_y.removedate = :c_eot
    and cal.name = :c_calendar_name
    and per_y.periodtypeseq = (select periodtypeseq from cs_periodtype where lower(name) = lower(:c_year) and removedate = :c_eot)
    and per_y.startdate <= :v_date 
    and per_y.enddate > :v_date
    --and per_m.startdate = per_sa.startdate -- only month for start of the semi annual period
  order by per_m.startdate;
  
  for x as c_periods
  do
  
    kyn_lib_pipeline:data_extract(
      i_calendarName => :x.calendar_name,
      i_periodName => :x.month_name,
      i_fileType => 'REPDAILY',
      o_pipelinerunseq => v_pipelinerunseq
    );

  end for;

end;