--liquibase formatted sql

--changeset jcadby:KYN_Lib_TQ2COM splitStatements:false stripComments:false
--comment: Create library
--ignoreLines:1
set schema ext;

CREATE or replace LIBRARY EXT.KYN_Lib_TQ2COM LANGUAGE SQLSCRIPT AS
BEGIN
  PUBLIC variable v_batchName varchar(100);
  PUBLIC variable v_pipelineRunSeq bigint;
  PUBLIC variable v_procName varchar(100);
  PUBLIC variable c_eot constant DATE := TO_DATE('01/01/2200','mm/dd/yyyy');
  PUBLIC VARIABLE v_sqlCount INT;
  PUBLIC VARIABLE v_tenantId varchar2(4);
  PUBLIC variable v_process_name constant varchar(255) := ::CURRENT_OBJECT_NAME;
  public variable c_default_revenue_percent constant number = 0.1;
  public variable c_pt_semiannual constant varchar(50) := 'semiannual';
  public variable c_pt_year constant varchar(50) := 'year';
  private variable c_log_prefix constant varchar(100) := '['||::CURRENT_OBJECT_NAME||'] ';
  private variable v_uuid varchar(100);
  
  private procedure log(
    IN i_text      varchar(4000) default null,
    IN i_value     decimal(25,10) default null
  ) as
  begin
    kyn_prc_debug(:c_log_prefix||:i_text, :i_value, :v_uuid);
  end;
  
  public function accepted_ipl() returns v_ret boolean as
  begin
    
    select case when count(1) = 1 then true else false end into v_ret
    from dummy 
    where exists (
      select 1 
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
    );
  
  end;

  PUBLIC procedure log_error_to_sync(
    IN i_error_code    integer,
    IN i_error_message varchar(4000),
    IN i_run_key       bigint
  ) as
  BEGIN
    begin autonomous transaction
      update ext.kyn_tq2com_sync set message = ('['||i_error_code||'] '|| i_error_message), process_flag = 1 where run_key = :i_run_key ;
      commit;
    end;
  END;


  PUBLIC procedure full_reset() as
  begin
    -- use for non production only
    declare cursor c_tabs for 
    select table_name, schema_name 
    from tables 
    where table_name like 'KYN$_TQ2COM$_%' escape '$' 
    and schema_name = 'EXT';
    for x as c_tabs
    do
      execute immediate 'delete from '||x.schema_name||'.'||x.table_name;
      end for;
    commit;
  end;


  public procedure reset(in i_run_key integer)  as
  begin
    -- remove previous data
    delete from ext.kyn_tq2com_tq_quota where run_key = :i_run_key;
    delete from ext.kyn_tq2com_prestage_quota where run_key = :i_run_key;    
    delete from ext.kyn_tq2com_account where run_key = :i_run_key;
    delete from ext.kyn_tq2com_product where run_key = :i_run_key;
    log('Done reset');
  end;
  

  PUBLIC procedure update_territoryprogram_data(in i_run_key integer) AS
  begin

    v_procName = 'update_territoryprogram_data';

    update ext.kyn_tq2com_sync s 
    set (
      territoryprogram_esd,
      territoryprogram_eed,
      territoryprogram_periodtype,
      territoryprogram_calendar,
      territoryprogram_periodseq,
      territoryprogram_period
    ) = (
    select 
      cast(tp.effectivestartdate as date) as esd,
      cast(tp.effectiveenddate as date) as eed,
      pt.name as periodtype,
      cal_tp.name as calendar,
      per_tp.periodseq as periodseq,
      per_tp.name as period
      from csq_territoryprogram tp
      left outer join cs_periodtype pt on tp.periodtypeseq = pt.periodtypeseq and pt.removedate= :c_eot
      join cs_period per_tp on tp.periodseq = per_tp.periodseq and per_tp.removedate= :c_eot
      join cs_calendar cal_tp on per_tp.calendarseq = cal_tp.calendarseq and cal_tp.removedate = :c_eot
      where tp.removedate = :c_eot
      and tp.name = s.territoryprogram_name
    )
    where s.run_key = :i_run_key;

    log(:v_procName||'=> For run_key '|| :i_run_Key ||' kyn_tq2com_sync table updated territory program effectivedates information', ::ROWCOUNT);	

    update ext.kyn_tq2com_sync x
    set (x.semiannual_periodseq, x.semiannual_name) = (
      select y.periodseq, y.name
      from cs_period y
      where y.removedate = :c_eot
      and y.parentseq = x.territoryprogram_periodseq
      and ((y.name like 'HY1%' and x.territoryprogram_name like 'FY%H1%')
      or (y.name like 'HY2%' and x.territoryprogram_name like 'FY%H2%'))
    )
    where x.run_key = :i_run_key;

    log(:v_procName||'=> kyn_tq2com_sync table updated with semi annual period information', ::ROWCOUNT);	  
  end;
  
  -- for testing
  PUBLIC procedure get_territoryprogram_data(in pTerritoryProgram  varchar(100)) AS
  begin

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      EXT.kyn_prc_debug_error (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :v_uuid);
      resignal;
    END;

    v_procName = 'get_territoryprogram_data';

    insert into ext.kyn_tq2com_sync (territoryprogramseq, territoryprogram_name)
    select tp.territoryprogramseq, tp.name
    from csq_territoryprogram tp
    where tp.removedate = :c_eot
    and tp.name = :pTerritoryProgram;
	
    log(:v_procName||'=> kyn_tq2com_sync table load complete ', ::ROWCOUNT);	

    commit;
    
  end;

  PUBLIC procedure get_tq_from_program( in i_run_key bigint ) AS
  begin
    declare v_count integer;
    declare v_error_message clob;
    declare cursor c_multi_territory for 
      select 
        '"'||position||'"['||string_agg('"'||territory_name||'"', ',' order by territory_name)||']' as error_message
      from (
        select distinct positionseq, position, territoryseq, territory_name
        from ext.kyn_tq2com_tq_quota
        where run_key = :i_run_key
        and positionseq in (
          select positionseq
          from ext.kyn_tq2com_tq_quota 
          where run_key = :i_run_key
          group by positionseq
          having count(distinct territoryseq) > 1
        )
      )
      group by position
      order by position;

    v_procName = 'get_tq_from_program';

    -- insert TQ data for all
    INSERT INTO ext.kyn_tq2com_tq_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, 
      territoryseq, territory_name, territory_esd, territory_eed, targettypeid, quota_esd, quota_eed, quotavalue, unittype, finalquotavalue,
      quota_casestatus, tpos_esd, tpos_eed, split, positionseq, position, payeeseq, payeeid, 
      titleseq, title, title_gn3, planseq, plan, country_code, period_cycle, element
    )
    (
      select
      qs.run_key,
      qs.territoryprogram_periodseq,
      qs.semiannual_periodseq,
      t.territoryseq,
      t.name as territory_name,
      cast(t.effectivestartdate as date) as territory_esd,
      cast(t.effectiveenddate as date) as territory_eed,  
      tt.targettypeid,
      cast(tq.effectivestartdate as date) as quota_esd,
      cast(tq.effectiveenddate as date) as quota_eed,  
      tq.quotavalue,
      ut_tq.name as unittype,
      tq.finalquotavalue,
      tq.casestatus as quota_casestatus,
      cast(tpos.effectivestartdate as date) as tpos_esd,
      cast(tpos.effectiveenddate as date) as tpos_eed,
      tpos.split,
      tpos.positionseq,
      pos.name as position,
      pos.payeeseq,
      pay.payeeid,
      pos.titleseq,
      ttl.name as title,
      ttl.genericnumber3 as title_gn3,
      pas.planseq,
      pln.name as plan,
      par.genericattribute1 as country_code,
      case when qs.semiannual_name like 'HY1%' then 1 else 2 end as period_cycle,
      case when tt.targettypeid = 'GP' then 'Profit' else tt.targettypeid end as element
      from ext.kyn_tq2com_sync qs
      join csq_territory t on 
        qs.territoryprogramseq = t.territoryprogramseq
        and t.createdate <= qs.run_date
        and t.removedate > qs.run_date
        and t.effectivestartdate < qs.territoryprogram_eed
        and t.effectiveenddate > qs.territoryprogram_esd
      join csq_territoryquota tq on 
        tq.territoryseq = t.territoryseq 
        and tq.createdate <= qs.run_date
        and tq.removedate > qs.run_date
        and tq.effectivestartdate < t.effectiveenddate
        and tq.effectiveenddate > t.effectivestartdate 
      join csq_targettype tt on tq.targettypeseq = tt.datatypeseq and tt.removedate= :c_eot
      join cs_unittype ut_tq on tq.unittypeforquotavalue = ut_tq.unittypeseq and ut_tq.removedate = :c_eot
      join csq_territoryposition tpos on 
        tpos.territoryseq = t.territoryseq 
        and tpos.createdate <= qs.run_date
        and tpos.removedate > qs.run_date
        and tpos.effectivestartdate < t.effectiveenddate
        and tpos.effectiveenddate > t.effectivestartdate 
      join cs_position pos on 
        tpos.positionseq = pos.ruleelementownerseq 
        and pos.createdate <= qs.run_date
        and pos.removedate > qs.run_date
        and pos.effectivestartdate < tpos.effectiveenddate
        and pos.effectiveenddate >= tpos.effectiveenddate
      join cs_title ttl on
        pos.titleseq = ttl.ruleelementownerseq
        and ttl.createdate <= qs.run_date
        and ttl.removedate > qs.run_date
        and ttl.effectivestartdate < pos.effectiveenddate
        and ttl.effectiveenddate >= pos.effectiveenddate
      left outer join cs_planassignable pas on
        ttl.ruleelementownerseq = pas.ruleelementownerseq
        and pas.createdate <= qs.run_date
        and pas.removedate > qs.run_date
        and pas.effectivestartdate < ttl.effectiveenddate
        and pas.effectiveenddate >= ttl.effectiveenddate
      left outer join cs_plan pln on
        pln.ruleelementownerseq = pas.planseq
        and pln.createdate <= qs.run_date
        and pln.removedate > qs.run_date
        and pln.effectivestartdate < pas.effectiveenddate
        and pln.effectiveenddate >= pas.effectiveenddate
      join cs_payee pay on
        pos.payeeseq = pay.payeeseq
        and pay.createdate <= qs.run_date
        and pay.removedate > qs.run_date
        and pay.effectivestartdate < pos.effectiveenddate
        and pay.effectiveenddate >= pos.effectiveenddate
      join cs_participant par on
        pay.payeeseq = par.payeeseq
        and pay.effectivestartdate = par.effectivestartdate
        and par.createdate <= qs.run_date
        and par.removedate > qs.run_date
      where qs.process_flag = 0
        and qs.run_key = :i_run_key
        and tpos.split > 0
    );
    
    log(:v_procName||'=> kyn_tq2com_tq_quota table loaded with TQ quotas complete', ::ROWCOUNT);
    
    -- if a position filter exists then delete all rows that are not for the positions
    select count(1) into v_count from ext.kyn_tq2com_filter where run_key = :i_run_key and filter_column = 'POSITION';
    
    if :v_count > 0 then
      delete from ext.kyn_tq2com_tq_quota tq
      where tq.run_key = :i_run_key
      and tq.position not in (
        select fl.filter_value
          from ext.kyn_tq2com_filter fl
         where fl.run_key = :i_run_key
           and fl.filter_column = 'POSITION'
      );
      log(:v_procName||'=> deleted records for position filter', ::ROWCOUNT);
    end if;
    
    -- if territory filter exists then delete all rows that are not for the territories
    select count(1) into v_count from ext.kyn_tq2com_filter where run_key = :i_run_key and filter_column = 'TERRITORY';    
    if :v_count > 0 then
      delete from ext.kyn_tq2com_tq_quota tq
      where tq.run_key = :i_run_key
      and not exists (
        select 1
        from 
        ext.kyn_tq2com_sync qs
        join ext.kyn_v_tq2com_wf_territory vt on 
          vt.territoryprogramseq = qs.territoryprogramseq
        join ext.kyn_tq2com_filter fl on 
          qs.run_key = fl.run_key 
          and vt.territory_path like '%['||fl.filter_value||']%' 
          and fl.filter_column = 'TERRITORY'
        where qs.run_key = :i_run_key
          and tq.territory_name = vt.territory_name
      );
      log(:v_procName||'=> deleted records for territory filter', ::ROWCOUNT);
    end if;
    
    -- if a position has multiple territories then raise an error
    for x as c_multi_territory
    do
      v_error_message := ifnull(:v_error_message||',','') ||:x.error_message;
    end for;
    
    if length(:v_error_message) > 0 then
      SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = substr('Positions with multiple territories: '||:v_error_message,1,5000);
    end if;

  end;
  
  public procedure override_min_quota(in i_run_key bigint) as
  begin
    declare v_periodRow row like cs_period;
    declare v_periodRow_year row like cs_period;
    
    v_procName = 'override_min_quota';

    select * into v_periodRow
    from cs_period
    where removedate = :c_eot
    and periodseq = (select semiannual_periodseq from ext.kyn_tq2com_sync where run_key = :i_run_key);
    
    select * into v_periodRow_year
    from cs_period
    where removedate = :c_eot
    and periodseq = :v_periodRow.parentSeq;
    
    t_lkp = select
            case when dim0.name = 'Position Name' then ind0.minstring else ind1.minstring end as position_name,
            case when dim0.name = 'Element' then ind0.minstring else ind1.minstring end as element,
            cell.effectivestartdate, cell.effectiveenddate,
            cell.value
            from cs_relationalmdlt mdlt
            join cs_mdltdimension dim0 on 
              mdlt.ruleelementseq = dim0.ruleelementseq 
              and dim0.removedate = :c_eot 
              and dim0.dimensionslot = 0
            join cs_mdltindex ind0 on
              ind0.ruleelementseq = dim0.ruleelementseq 
              and ind0.removedate = :c_eot 
              and ind0.dimensionseq = dim0.dimensionseq
            join cs_mdltdimension dim1 on 
              mdlt.ruleelementseq = dim1.ruleelementseq 
              and dim1.removedate = :c_eot 
              and dim1.dimensionslot = 1
            join cs_mdltindex ind1 on
              ind1.ruleelementseq = dim1.ruleelementseq 
              and ind1.removedate = :c_eot 
              and ind1.dimensionseq = dim1.dimensionseq
            join cs_mdltcell cell on
              cell.mdltseq = mdlt.ruleelementseq
              and cell.removedate = :c_eot
              and cell.dim0index = ind0.ordinal
              and cell.dim1index = ind1.ordinal
            where mdlt.name = 'LT_Minimum_Quota_Override'
            and mdlt.removedate = :c_eot
            and cell.effectivestartdate < :v_periodRow_year.endDate
            and cell.effectiveenddate > :v_periodRow_year.startDate;

    log(:v_procName||'=> cached lookup', ::ROWCOUNT);

    update ext.kyn_tq2com_tq_quota tq
      set
        lt_min_quota_override = (
          select x.value
          from :t_lkp x
          where tq.position = x.position_name
          and tq.element = x.element
          and x.effectivestartdate < :v_periodRow.enddate
          and x.effectiveenddate >= :v_periodRow.enddate
        ),
        year_lt_min_quota_override = (
          select x.value
          from :t_lkp x
          where tq.position = x.position_name
          and tq.element = x.element
          and x.effectivestartdate < :v_periodRow_year.enddate
          and x.effectiveenddate >= :v_periodRow_year.enddate
        )
    where tq.run_key = :i_run_key;
    
    log(:v_procName||'=> done update', ::ROWCOUNT);
    
  end;
  
  public procedure update_tq_quota(in i_run_key bigint) as
  begin
    declare v_periodRow row like cs_period;
    declare v_periodRow_year row like cs_period;
    declare v_revenue_percent number;
    
    v_procName = 'update_tq_quota';

    select * into v_periodRow
    from cs_period
    where removedate = :c_eot
    and periodseq = (select semiannual_periodseq from ext.kyn_tq2com_sync where run_key = :i_run_key);
    
    select * into v_periodRow_year
    from cs_period
    where removedate = :c_eot
    and periodseq = :v_periodRow.parentSeq;
    
    log(:v_procName||'=> year period '||:v_periodRow_year.name||' end date '||to_char(:v_periodRow_year.enddate,'YYYY/MM/DD'));
    
    v_revenue_percent := ifnull(to_number(kyn_lib_utils:get_config('DI10.Revenue%', :v_periodRow.startDate))/100, c_default_revenue_percent);
    log(:v_procName||'=> v_revenue_percent = '||:v_revenue_percent, :v_revenue_percent);
    
    update ext.kyn_tq2com_tq_quota tq
    set tq.year_finalquotavalue = (
      select sum(tq2.finalquotavalue)
      from ext.kyn_tq2com_tq_quota tq2
      where tq.positionseq = tq2.positionseq
      and tq.targettypeid = tq2.targettypeid
      and tq.territoryprogram_periodseq = tq2.territoryprogram_periodseq
      and tq2.run_key <= tq.run_key
      and tq2.run_key = (
        select max(tq3.run_key)
        from ext.kyn_tq2com_tq_quota tq3
        where tq3.positionseq = tq2.positionseq
          and tq3.semiannual_periodseq = tq2.semiannual_periodseq
          and tq3.run_key <= tq.run_key
      )
    )
    where tq.run_key = :i_run_key;
    
    log(:v_procName||'=> set year_finalquotavalue', ::ROWCOUNT);

    update ext.kyn_tq2com_tq_quota tq
    set 
      tq.lt_min_quota = KYN_FNC_TQ2COM_Get_Min_Quota(tq.element, tq.country_code, tq.period_cycle, add_seconds(:v_periodRow.enddate,-1)),
      tq.year_lt_min_quota = KYN_FNC_TQ2COM_Get_Min_Quota(tq.element, tq.country_code, 3, add_seconds(:v_periodRow_year.enddate,-1)),
      tq.subordinate_count = KYN_FNC_TQ2COM_Get_Subordinates_Count(tq.positionseq, :v_periodRow.startdate, :v_periodRow.enddate),
      tq.year_subordinate_count = KYN_FNC_TQ2COM_Get_Subordinates_Count(tq.positionseq, :v_periodRow_year.startdate, :v_periodRow_year.enddate)
    where run_key = :i_run_key;
    
    log(:v_procName||'=> set lt_min and subordinate counts', ::ROWCOUNT);

    override_min_quota(:i_run_key);

    update ext.kyn_tq2com_tq_quota
    set 
      min_quota = ifnull(lt_min_quota_override, lt_min_quota * greatest(subordinate_count,1)),
      year_min_quota = ifnull(year_lt_min_quota_override, year_lt_min_quota * greatest(year_subordinate_count,1))
    where run_key = :i_run_key;
    
    update ext.kyn_tq2com_tq_quota
    set
      final_quota = greatest(finalquotavalue, min_quota),
      year_final_quota = greatest(year_finalquotavalue, year_min_quota)
    where run_key = :i_run_key
      and element != 'Profit';
      
    update ext.kyn_tq2com_tq_quota tq
     set 
     tq.revenue_percent = ifnull((
        select :v_revenue_percent*tq2.final_quota
        from ext.kyn_tq2com_tq_quota tq2 
        where tq2.run_key = :i_run_key 
        and tq2.element = 'Revenue'
        and tq2.positionseq = tq.positionseq
        and tq2.territoryseq = tq.territoryseq
      ),0),      
     tq.year_revenue_percent = ifnull((
        select :v_revenue_percent*tq2.year_final_quota
        from ext.kyn_tq2com_tq_quota tq2 
        where tq2.run_key = :i_run_key 
        and tq2.element = 'Revenue'
        and tq2.positionseq = tq.positionseq
        and tq2.territoryseq = tq.territoryseq
      ),0)      
    where tq.run_key = :i_run_key
      and tq.element = 'Profit';
      
    update ext.kyn_tq2com_tq_quota
    set
      final_quota = greatest(finalquotavalue, min_quota, revenue_percent),
      year_final_quota = greatest(year_finalquotavalue, year_min_quota, year_revenue_percent)
    where run_key = :i_run_key
      and element = 'Profit';
      
    log(:v_procName||'=> set final quotas', ::ROWCOUNT);
  
  end;

  PUBLIC procedure load_quota_to_prestage( in i_run_key bigint) AS
  begin

    v_procName = 'load_quota_to_prestage';

    -- Original
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Original' as quotaname,
        tq.finalquotavalue as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.semiannual_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
    );

    log(:v_procName||'=> kyn_tq2com_prestage_quota table load with original quotas for IPL generation complete', ::ROWCOUNT);


    -- Minimum
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Minimum' as quotaname,
        min_quota as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.semiannual_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
    );
    
    -- Revenue Percent - only for profit
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Revenue%' as quotaname,
        revenue_percent as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.semiannual_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
        and tq.element = 'Profit'
    );


    log(:v_procName||'=> kyn_tq2com_prestage_quota table load with minimum quotas for IPL generation complete', ::ROWCOUNT);	

    -- Final
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Final' as quotaname,
        final_quota as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.semiannual_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
    );

    log(:v_procName||'=> kyn_tq2com_prestage_quota table load with final quotas for IPL generation complete',::ROWCOUNT);	
    COMMIT;
  end;



  PUBLIC procedure load_quota_to_prestage_year( in i_run_key bigint) AS
  begin

    v_procName = 'load_quota_to_prestage_year';

    -- Original
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Original' as quotaname,
        tq.year_finalquotavalue as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.territoryprogram_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
    );

    log(:v_procName||'=> kyn_tq2com_prestage_quota table load with original quotas for IPL generation complete', ::ROWCOUNT);


    -- Minimum
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Minimum' as quotaname,
        year_min_quota as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.territoryprogram_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
    );
    
    -- Revenue Percent - only for profit
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Revenue%' as quotaname,
        year_revenue_percent as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.territoryprogram_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
        and tq.element = 'Profit'
    );


    log(:v_procName||'=> kyn_tq2com_prestage_quota table load with minimum quotas for IPL generation complete', ::ROWCOUNT);	

    -- Final
    insert into ext.kyn_tq2com_prestage_quota (
      run_key, territoryprogram_periodseq, semiannual_periodseq, semiannual_name, effectivestartdate, effectiveenddate, 
      quotaname, value, unittypeforvalue, periodtypename,
      positionseq, positionname
    )
    (
      select
        qs.run_key,
        qs.territoryprogram_periodseq,
        qs.semiannual_periodseq,
        qs.semiannual_name,
        per.startdate as effectivestartdate,
        per.enddate as effectiveenddate,
        'Q_'||tq.element||'_Final' as quotaname,
        year_final_quota as value,
        tq.unittype as unittypeforvalue,
        pt.name as periodtypename,
        tq.positionseq,
        tq.position as positionname
      from ext.kyn_tq2com_tq_quota tq
      join ext.kyn_tq2com_sync qs on tq.run_key = qs.run_key
      join cs_period per on qs.territoryprogram_periodseq = per.periodseq and per.removedate = :c_eot
      join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :c_eot
      where qs.run_key = :i_run_key
    );

    log(:v_procName||'=> kyn_tq2com_prestage_quota table load with final quotas for IPL generation complete', ::ROWCOUNT);	
    COMMIT;
  end;

  PUBLIC procedure get_account_from_tq(in i_run_key bigint) AS
  begin
    declare v_count integer;

    v_procName = 'get_account_from_tq';

    insert into ext.kyn_tq2com_account 
    (select
      qs.run_key,
      qs.semiannual_periodseq,
      qs.semiannual_name,
      cast(0 as tinyint) as active_flag,
      cast(null as timestamp) as active_start,
      cast(null as timestamp) as active_end,
      t.name as territory,
      cast(t.effectivestartdate as date) t_esd,
      cast(t.effectiveenddate as date) t_eed,
      tpos.positionseq,
      pos.name as position,
      cast(pos.effectivestartdate as date) pos_esd,
      cast(pos.effectiveenddate as date) pos_eed,
      cast(tacc.effectivestartdate as date) tacc_esd,
      cast(tacc.effectiveenddate as date) tacc_edd,
      acc.accountseq,
      acc.accountid,
      cast(acc.effectivestartdate as date) as account_esd,
      cast(acc.effectiveenddate as date) as account_eed,
      acc.createdate as account_createdate,
      tacc.isaddedduetoparent
    from ext.kyn_tq2com_sync qs
    join csq_territory t on 
      qs.territoryprogramseq = t.territoryprogramseq
      and t.createdate <= qs.run_date
      and t.removedate > qs.run_date
      and t.effectivestartdate < qs.territoryprogram_eed
      and t.effectiveenddate > qs.territoryprogram_esd
    join csq_territoryaccount tacc on
      tacc.territoryseq = t.territoryseq
      and tacc.createdate <= qs.run_date
      and tacc.removedate > qs.run_date
      and tacc.effectivestartdate < t.effectiveenddate
      and tacc.effectiveenddate > t.effectivestartdate  
    join csq_account acc on
      acc.accountseq = tacc.accountseq
      and acc.createdate <= qs.run_date
      and acc.removedate > qs.run_date
      and acc.effectivestartdate < tacc.effectiveenddate
      and acc.effectiveenddate > tacc.effectivestartdate
    join csq_territoryposition tpos on 
      tpos.territoryseq = t.territoryseq 
      and tpos.createdate <= qs.run_date
      and tpos.removedate > qs.run_date
      and tpos.effectivestartdate < t.effectiveenddate
      and tpos.effectiveenddate > t.effectivestartdate
    join cs_position pos on 
      tpos.positionseq = pos.ruleelementownerseq 
      and pos.createdate <= qs.run_date
      and pos.removedate > qs.run_date
      and pos.effectivestartdate < tpos.effectiveenddate
      and pos.effectiveenddate > tpos.effectivestartdate
    where qs.run_key = :i_run_key
      and tpos.split > 0
    );

    log(:v_procName||'=> kyn_tq2com_account table load with accounts information complete', ::ROWCOUNT);	

    -- if a position filter exists then delete all rows that are not for the positions
    select count(1) into v_count from ext.kyn_tq2com_filter where run_key = :i_run_key and filter_column = 'POSITION';    
    if :v_count > 0 then
      delete from ext.kyn_tq2com_account tq
      where tq.run_key = :i_run_key
      and tq.position not in (
        select fl.filter_value
          from ext.kyn_tq2com_filter fl
         where fl.run_key = :i_run_key
           and fl.filter_column = 'POSITION'
      );
      log(:v_procName||'=> deleted records for position filter', ::ROWCOUNT);
    end if;
    
    -- if territory filter exists then delete all rows that are not for the territories
    select count(1) into v_count from ext.kyn_tq2com_filter where run_key = :i_run_key and filter_column = 'TERRITORY';    
    if :v_count > 0 then
      delete from ext.kyn_tq2com_account tq
      where tq.run_key = :i_run_key
      and not exists (
        select 1
        from 
        ext.kyn_tq2com_sync qs
        join ext.kyn_v_tq2com_wf_territory vt on 
          vt.territoryprogramseq = qs.territoryprogramseq
        join ext.kyn_tq2com_filter fl on 
          qs.run_key = fl.run_key 
          and vt.territory_path like '%['||fl.filter_value||']%' 
          and fl.filter_column = 'TERRITORY'
        where qs.run_key = :i_run_key
          and tq.territory = vt.territory_name
      );
      log(:v_procName||'=> deleted records for territory filter', ::ROWCOUNT);
    end if;

  end;


  PUBLIC procedure get_product_from_tq(in i_run_key bigint ) AS
  begin
    declare v_count integer;

    v_procName = 'get_product_from_tq';

    insert into ext.kyn_tq2com_product (
    select
    qs.run_key,
    qs.semiannual_periodseq,
    qs.semiannual_name,
    cast(0 as tinyint) as active_flag,
    cast(null as timestamp) as active_start,
    cast(null as timestamp) as active_end,
    t.name as territory,
    cast(t.effectivestartdate as date) t_esd, 
    cast(t.effectiveenddate as date) t_eed,
    tpos.positionseq,
    pos.name as position,
    cast(pos.effectivestartdate as date) pos_esd, 
    cast(pos.effectiveenddate as date) pos_eed,
    cast(tprd.effectivestartdate as date) tprd_esd, 
    cast(tprd.effectiveenddate as date) tprd_edd,
    cat.name as category,
    prd.classifierid,
    tprd.source,
    cast(0 as tinyint) as process_flag
    from ext.kyn_tq2com_sync qs
    join csq_territory t on 
      qs.territoryprogramseq = t.territoryprogramseq
      and t.createdate <= qs.run_date
      and t.removedate > qs.run_date
      and t.effectivestartdate < qs.territoryprogram_eed
      and t.effectiveenddate > qs.territoryprogram_esd
    join csq_territoryproduct tprd on
      tprd.territoryseq = t.territoryseq
      and tprd.createdate <= qs.run_date
      and tprd.removedate > qs.run_date
      and tprd.effectivestartdate < t.effectiveenddate
      and tprd.effectiveenddate > t.effectivestartdate      
    join cs_classifier prd on 
      prd.classifierseq = tprd.productseq
      and prd.createdate <= qs.run_date
      and prd.removedate > qs.run_date
      and prd.effectivestartdate < tprd.effectiveenddate
      and prd.effectiveenddate > tprd.effectivestartdate   
    left outer join cs_category cat on 
      tprd.categoryseq = cat.ruleelementseq
      and cat.createdate <= qs.run_date
      and cat.removedate > qs.run_date
      and cat.effectivestartdate < tprd.effectiveenddate
      and cat.effectiveenddate > tprd.effectivestartdate
    join csq_territoryposition tpos on 
      tpos.territoryseq = t.territoryseq 
      and tpos.createdate <= qs.run_date
      and tpos.removedate > qs.run_date
      and tpos.effectivestartdate < t.effectiveenddate
      and tpos.effectiveenddate > t.effectivestartdate
    join cs_position pos on 
      tpos.positionseq = pos.ruleelementownerseq 
      and pos.createdate <= qs.run_date
      and pos.removedate > qs.run_date
      and pos.effectivestartdate < tpos.effectiveenddate
      and pos.effectiveenddate > tpos.effectivestartdate  
    where qs.run_key = :i_run_key
      and tpos.split > 0);

    log(:v_procName||'=> kyn_tq2com_product table load with products information complete', ::ROWCOUNT);
    
   -- if a position filter exists then delete all rows that are not for the positions
    select count(1) into v_count from ext.kyn_tq2com_filter where run_key = :i_run_key and filter_column = 'POSITION';    
    if :v_count > 0 then
      delete from ext.kyn_tq2com_product tq
      where tq.run_key = :i_run_key
      and tq.position not in (
        select fl.filter_value
          from ext.kyn_tq2com_filter fl
         where fl.run_key = :i_run_key
           and fl.filter_column = 'POSITION'
      );
      log(:v_procName||'=> deleted records for position filter', ::ROWCOUNT);
    end if;
    
    -- if territory filter exists then delete all rows that are not for the territories
    select count(1) into v_count from ext.kyn_tq2com_filter where run_key = :i_run_key and filter_column = 'TERRITORY';    
    if :v_count > 0 then
      delete from ext.kyn_tq2com_product tq
      where tq.run_key = :i_run_key
      and not exists (
        select 1
        from 
        ext.kyn_tq2com_sync qs
        join ext.kyn_v_tq2com_wf_territory vt on 
          vt.territoryprogramseq = qs.territoryprogramseq
        join ext.kyn_tq2com_filter fl on 
          qs.run_key = fl.run_key 
          and vt.territory_path like '%['||fl.filter_value||']%' 
          and fl.filter_column = 'TERRITORY'
        where qs.run_key = :i_run_key
           and tq.territory = vt.territory_name
      );
      log(:v_procName||'=> deleted records for territory filter', ::ROWCOUNT);
    end if;

  end;
  
  public procedure load_report_run(in i_run_key bigint) as
  begin
  
    insert into kyn_tq2com_report_run (run_key, periodseq, period_name)  
    select ts.run_key, m.periodseq, m.name
    from kyn_tq2com_sync ts
    join cs_period sa on 
      ts.semiannual_periodseq = sa.periodseq
      and sa.removedate = :c_eot
    join cs_calendar cal on
      sa.calendarseq = cal.calendarseq 
      and cal.removedate = :c_eot
    join cs_period m on 
      sa.calendarseq = m.calendarseq
      and m.startdate >= sa.startdate 
      and m.enddate <= sa.enddate 
      and m.removedate = :c_eot 
      and m.periodtypeseq = cal.minorperiodtypeseq
    where ts.run_key = :i_run_key
      and not exists (select 1 from kyn_tq2com_report_run rr where rr.periodseq = m.periodseq and rr.process_flag = 0);

  end;


  PUBLIC procedure update_sync(in i_run_key bigint) as
  begin
    update ext.kyn_tq2com_sync set process_flag = 3 where process_flag = 0 and run_key = :i_run_key;
  end;


  public procedure load_ipl_trace() AS
  begin

    v_procName = 'load_ipl_trace';
    
    insert into ext.kyn_tq2com_ipl_trace (
      documentprocessseq, generatedate, name, batchname, startdate, enddate,
      positionseq, position, status, acceptdate,
      semiannual_periodseq,
      semiannual_name
    )
    (
    select
    dp.documentprocessseq, dp.generatedate, dp.name, dp.batchname, dp.startdate, dp.enddate,
    dp.positionseq, pos.name as position, dp.status, dp.acceptdate,
    per.periodseq as semiannual_periodseq,
    per.name as semiannual_name
    from csp_documentprocess dp
    join cs_position pos on 
      dp.positionseq = pos.ruleelementownerseq 
      and pos.removedate = :c_eot 
      and pos.effectivestartdate < dp.enddate 
      and pos.effectiveenddate >= dp.enddate
    join cs_period per on
      per.startdate >= dp.startdate
      and per.enddate <= dp.enddate
      and per.removedate = :c_eot
    join cs_periodtype pt on
      per.periodtypeseq = pt.periodtypeseq 
      and pt.removedate = :c_eot
      and pt.name = 'semiannual' 
    where exists (
      -- must be for a plan document
      select 1
      from csp_documentassignment da
      join csp_documenttemplate dt on da.documenttemplateseq = dt.documenttemplateseq and dt.removedate = :c_eot
      join csp_documenttype dty on dt.documenttypeseq = dty.datatypeseq and dty.removedate = :c_eot
      where dp.documentprocesstemplateseq = da.documentprocesstemplateseq 
      and da.removedate = :c_eot
      and dty.datatype = 'Plan'
    )
    and not exists (select 1 from ext.kyn_tq2com_ipl_trace x where x.documentprocessseq = dp.documentprocessseq)
    );

    log(:v_procName||'=> kyn_tq2com_ipl_trace table load with ipl trace info complete', ::ROWCOUNT);	

    -- set run_key
    update ext.kyn_tq2com_ipl_trace ipl
    set ipl.run_key = (
      select max(s.run_key)
      from
      ext.kyn_tq2com_sync s
      join ext.kyn_tq2com_tq_quota q on s.run_key = q.run_key
      where s.semiannual_periodseq = ipl.semiannual_periodseq
      and q.positionseq = ipl.positionseq
      and s.run_date <= ipl.generatedate
    )
    where ipl.run_key is null
      and ipl.process_flag = 0;

    log(:v_procName||'=> kyn_tq2com_ipl_trace table updated with run_key', ::ROWCOUNT);
    
    -- set year_run_key - use greater value
    update ext.kyn_tq2com_ipl_trace x
    set x.year_run_key = (
      select max(y.run_key)
      from ext.kyn_tq2com_ipl_trace y
      where x.documentprocessseq = y.documentprocessseq
    )
    where x.year_run_key is null
      and x.process_flag = 0;    
    
    -- fail any records that do not have a run_key
    update ext.kyn_tq2com_ipl_trace ipl
    set process_flag = 1, message = 'No run_key'
    where process_flag = 0 
    and run_key is null;

    -- refresh status
    update ext.kyn_tq2com_ipl_trace x
    set (x.status, x.acceptdate) = (select y.status, y.acceptdate from csp_documentprocess y where x.documentprocessseq = y.documentprocessseq)
    where x.process_flag = 0
    and exists (select 1 from csp_documentprocess y where x.documentprocessseq = y.documentprocessseq);

    log(:v_procName||'=> kyn_tq2com_ipl_trace table updated with status and accept date', ::ROWCOUNT);	

    COMMIT;

  end;
  
  PUBLIC procedure set_active_status_quota(i_batchname varchar(90)) AS
  begin

    v_procName := 'set_active_status_quota';

    -- semiannual
    update ext.kyn_tq2com_prestage_quota q
    set
    q.active_flag = 1, 
    q.active_start = (
      select min(ipl.acceptdate) 
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.run_key 
      and q.positionseq = ipl.positionseq
    ),
    q.active_end = :c_eot,
    q.batchname = :i_batchname
    where exists (
      select 1 
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.run_key 
      and q.positionseq = ipl.positionseq
    )
    and q.periodtypename = :c_pt_semiannual;

    log(:v_procName||'=> kyn_tq2com_prestage_quota table active - semiannual', ::ROWCOUNT);	    

    -- deactivate semiannual
    update ext.kyn_tq2com_prestage_quota q1
    set
      q1.active_flag = 0,
      q1.active_end = (
        select min(q2.active_start)
        from ext.kyn_tq2com_prestage_quota q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.semiannual_periodseq = q1.semiannual_periodseq
          and q2.positionseq = q1.positionseq
        )
    where q1.active_flag = 1
      and exists (
        select 1
        from ext.kyn_tq2com_prestage_quota q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.semiannual_periodseq = q1.semiannual_periodseq
          and q2.positionseq = q1.positionseq      
      )
    and q1.periodtypename = :c_pt_semiannual;

    log(:v_procName||'=> kyn_tq2com_prestage_quota table deactivate - semiannual', ::ROWCOUNT);

  end;
  

  PUBLIC procedure set_active_status_quota_year(i_batchname varchar(90)) AS
  begin

    v_procName := 'set_active_status_quota_year';
  
    -- year
    update ext.kyn_tq2com_prestage_quota q
    set
    q.active_flag = 1, 
    q.active_start = (
      select min(ipl.acceptdate) 
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.year_run_key 
      and q.positionseq = ipl.positionseq
    ),
    q.active_end = :c_eot,
    q.batchname = :i_batchname
    where exists (
      select 1 
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.year_run_key 
      and q.positionseq = ipl.positionseq
    )
    and q.periodtypename = :c_pt_year;

    log(:v_procName||'=> kyn_tq2com_prestage_quota table active - year', ::ROWCOUNT);	    

    -- deactivate year
    update ext.kyn_tq2com_prestage_quota q1
    set
      q1.active_flag = 0,
      q1.active_end = (
        select min(q2.active_start)
        from ext.kyn_tq2com_prestage_quota q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.territoryprogram_periodseq = q1.territoryprogram_periodseq
          and q2.positionseq = q1.positionseq
        )
    where q1.active_flag = 1
      and exists (
        select 1
        from ext.kyn_tq2com_prestage_quota q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.territoryprogram_periodseq = q1.territoryprogram_periodseq
          and q2.positionseq = q1.positionseq      
      )
    and q1.periodtypename = :c_pt_year;    

    log(:v_procName||'=> kyn_tq2com_prestage_quota table deactivate - year', ::ROWCOUNT);

  end;

 
  PUBLIC procedure load_stagequota(i_batchname varchar(90))
  AS
  begin
    declare v_seq bigint;
    declare v_tenantId varchar2(4);

    v_procName = 'load_stagequota';

    select tenantid into v_tenantId from cs_tenant;
    
    select ifnull(max(stagequotaseq),0) into v_seq from cs_stagequota;

    insert into cs_stagequota
    (tenantid, stagequotaseq, effectivestartdate, effectiveenddate, quotaname, value, 
    unittypeforvalue, periodtypename, positionname, batchname, stageprocessflag, description)
    select :v_tenantId as tenantid,
    :v_seq + row_number() over (order by quotaname, positionname) as stagequotaseq,
    q.effectivestartdate, q.effectiveenddate, q.quotaname, q.value, 
    q.unittypeforvalue, q.periodtypename, q.positionname, 
    :i_batchname as batchname,
    0 as stageprocessflag, 
    null as description
    from ext.kyn_tq2com_prestage_quota q
    where q.batchname = :i_batchname
    and q.active_flag = 1
    and q.quotaname in (select cs.name from cs_quota cs where cs.removedate = :c_eot);

    log(:v_procName||'=> cs_stagequota table load with accepted IPL complete', ::ROWCOUNT);

  end;  
  
  PUBLIC procedure set_active_status_account()  AS
  begin

    v_procName := 'set_active_status_account';

    update ext.kyn_tq2com_account q
    set
    active_flag = 1, 
    active_start = (
      select min(ipl.acceptdate)
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.run_key 
      and q.positionseq = ipl.positionseq
    ),
    active_end = :c_eot
    where exists (
      select 1 
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.run_key 
      and q.positionseq = ipl.positionseq
    );

    log(:v_procName||'=> kyn_tq2com_account table updated with accepted status', ::ROWCOUNT);	

    update ext.kyn_tq2com_account q1
    set
      q1.active_flag = 0,
      q1.active_end = (
        select min(q2.active_start)
        from ext.kyn_tq2com_account q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.semiannual_periodseq = q1.semiannual_periodseq
          and q2.positionseq = q1.positionseq
        )
    where q1.active_flag = 1
      and exists (
        select 1
        from ext.kyn_tq2com_account q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.semiannual_periodseq = q1.semiannual_periodseq
          and q2.positionseq = q1.positionseq      
      );

    log(:v_procName||'=> kyn_tq2com_account table - Older version of accounts end dated', ::ROWCOUNT);	

  end;


  PUBLIC procedure set_active_status_product() AS
  begin

    v_procName := 'set_active_status_product';

    update ext.kyn_tq2com_product q
    set
    active_flag = 1, 
    active_start = (
      select min(ipl.acceptdate)
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.run_key 
      and q.positionseq = ipl.positionseq
    ),
    active_end = :c_eot
    where exists (
      select 1 
      from ext.kyn_tq2com_ipl_trace ipl
      where ipl.process_flag = 0
      and ipl.status = 'status_Accepted'
      and q.run_key = ipl.run_key 
      and q.positionseq = ipl.positionseq
    );

    log(:v_procName||'=> kyn_tq2com_product table updated with accepted status', ::ROWCOUNT);	

    update ext.kyn_tq2com_product q1
    set
      q1.active_flag = 0,
      q1.active_end = (
        select min(q2.active_start)
        from ext.kyn_tq2com_product q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.semiannual_periodseq = q1.semiannual_periodseq
          and q2.positionseq = q1.positionseq
        )
    where q1.active_flag = 1
      and exists (
        select 1
        from ext.kyn_tq2com_product q2
        where q2.active_flag = 1
          and q2.active_start > q1.active_start
          and q2.semiannual_periodseq = q1.semiannual_periodseq
          and q2.positionseq = q1.positionseq      
      );

    log(:v_procName||'=> kyn_tq2com_product table - Older version of products end dated', ::ROWCOUNT);	

  end;
  
  public procedure update_ipl_trace as
  begin
    update ext.kyn_tq2com_ipl_trace ipl 
    set ipl.process_flag = 3 
    where ipl.process_flag = 0
      and ipl.status = 'status_Accepted';
  end;

  PUBLIC procedure trigger_quota_import(i_batchname varchar(90)) AS
  begin
    declare v_pipelinerunseq BIGINT;
    declare v_count integer;

    select count(*) into v_count from cs_stagequota where batchname = :i_batchname;
    if :v_count > 0 then
      ext.kyn_lib_pipeline:v_and_t_quota(i_batchname => :i_batchname, o_pipelinerunseq => v_pipelinerunseq);
      log('Quota import triggered for batchname => '|| :i_batchname ||' with Pipelinerunseq => '|| :v_pipelinerunseq);
    end if;
  end;
  
  public procedure run_report_procs as
  begin
    declare v_row_report_run row like kyn_tq2com_report_run;
    declare cursor c_periods for
    select *
    from kyn_tq2com_report_run
    where process_flag = 0
    order by periodseq;
    
    log('run_report_procs: start');
    
    for x as c_periods
    do
      v_row_report_run.start_time := current_timestamp;
      v_row_report_run.stop_time := null;
      v_row_report_run.message := null;
      v_row_report_run.process_flag := 0;    
      begin
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        begin
          rollback;
          ext.kyn_prc_debug_error (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
          v_row_report_run.message := '['||::SQL_ERROR_CODE || ']' || ::SQL_ERROR_MESSAGE;
          v_row_report_run.process_flag := 1;
        END;

        log('run_report_procs: Run report procs for "'||x.period_name||'"');
        execute immediate 'call ext.Rep_Proc_IPL_Status_Detail_Report(:1)' using :x.periodseq;
        execute immediate 'call ext.Rep_Proc_IPL_Sales_Team_Report(:1)' using :x.periodseq;
        v_row_report_run.process_flag := 3;
      end;
      
      v_row_report_run.stop_time := current_timestamp;
      
      update kyn_tq2com_report_run
      set
        start_time = :v_row_report_run.start_time,
        stop_time = :v_row_report_run.stop_time,
        process_flag = :v_row_report_run.process_flag,
        message = :v_row_report_run.message
      where run_key = :x.run_key
        and periodseq = :x.periodseq
        and process_flag = 0;
      
      commit;
      
    end for;

    log('run_report_procs: end');

  end;


  PUBLIC procedure run() as
  begin
    declare v_lock_success boolean;
    declare v_batchname varchar(90) := 'TQ2COM_'||TO_CHAR(current_timestamp,'YYYYMMDD_HH24MISS');
    declare cursor c_key for select run_key from ext.kyn_tq2com_sync where process_flag = 0 order by run_key;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      rollback;
      log('Failed');
      kyn_prc_debug_error(::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :v_uuid);
      resignal;
    END;
    v_uuid := SYSUUID;

    log('Start');

    KYN_Lib_Schedule:create_lock(:v_process_name, v_lock_success);
    
    if :v_lock_success = false then
      log('Lock already exists - exit');
      return;
    end if;

    for x as c_key
    do
      begin
      
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        begin
          rollback;
          kyn_prc_debug_error (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :v_uuid);
          log_error_to_sync (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :x.run_key);
        END;
        
        log('run_key='||to_char(:x.run_key));
        reset(:x.run_key);
        update_territoryprogram_data(:x.run_key);
        get_tq_from_program(:x.run_key);
        update_tq_quota(:x.run_key);
        load_quota_to_prestage(:x.run_key);
        load_quota_to_prestage_year(:x.run_key);
        get_product_from_tq(:x.run_key);
        get_account_from_tq(:x.run_key);
        load_report_run(:x.run_key);
        update_sync(:x.run_key);
        commit;
      end;
    end for;

    load_ipl_trace();
    
    if accepted_ipl() = true then    
      set_active_status_quota(:v_batchname);
      set_active_status_quota_year(:v_batchname);
      load_stagequota(:v_batchname);    
      set_active_status_account();
      set_active_status_product();
      update_ipl_trace();
      commit;
      trigger_quota_import(:v_batchname);
    end if;
    
    commit;
    
    --run_report_procs();
    
    KYN_Lib_Schedule:remove_lock(:v_process_name);
    KYN_Lib_Schedule:delete_lock();
    
    log('End');
  end;


  public procedure run_test(IN i_territoryProgram nvarchar(500)) as
  begin
    get_territoryprogram_data( pTerritoryProgram => :i_territoryProgram);
    run();  
  end;
  
  -- procedure for report 25
  PUBLIC procedure run_jc_test() as
  begin
    declare v_lock_success boolean;
    declare v_batchname varchar(90) := 'TQ2COM_'||TO_CHAR(current_timestamp,'YYYYMMDD_HH24MISS');
    declare cursor c_key for  
    --select -999 as run_key from dummy;
    select run_key from ext.kyn_tq2com_sync where run_key < 0 order by run_key;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      rollback;
      log('Failed');
      kyn_prc_debug_error(::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :v_uuid);
      resignal;
    END;
    v_uuid := SYSUUID;

    log('Start');

    KYN_Lib_Schedule:create_lock(:v_process_name, v_lock_success);
    
    if :v_lock_success = false then
      log('Lock already exists - exit');
      return;
    end if;
    
    -- add rows for a TP
    /*
    insert into ext.kyn_tq2com_sync (run_key, territoryprogramseq, territoryprogram_name)
    select -999, tp.territoryprogramseq, tp.name
    from csq_territoryprogram tp
    where tp.removedate = :c_eot
    and tp.name = 'FY24H1_Seller_AG';
    */
    /*
    insert into ext.kyn_tq2com_sync (run_key, territoryprogramseq, territoryprogram_name)
    select -998, tp.territoryprogramseq, tp.name
    from csq_territoryprogram tp
    where tp.removedate = :c_eot
    and tp.name = 'FY24H2_Seller_AG';    
*/
    for x as c_key
    do
      begin     
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        begin
          rollback;
          kyn_prc_debug_error (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :v_uuid);
          log_error_to_sync (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE, :x.run_key);
        END;
        update kyn_tq2com_sync set process_Flag = 0 where run_key = :x.run_key;        
        
        log('run_key='||to_char(:x.run_key));
        reset(:x.run_key);
        update_territoryprogram_data(:x.run_key);
        get_tq_from_program(:x.run_key);
        
        -- use the undistributed values
        update kyn_tq2com_tq_quota
        set finalquotavalue = quotavalue
        where run_key = :x.run_key;
        
        update_tq_quota(:x.run_key);
        update_sync(:x.run_key);
        commit;
      end;
    end for;
    
    KYN_Lib_Schedule:remove_lock(:v_process_name);
    KYN_Lib_Schedule:delete_lock();
    
    log('End');
  end;

END