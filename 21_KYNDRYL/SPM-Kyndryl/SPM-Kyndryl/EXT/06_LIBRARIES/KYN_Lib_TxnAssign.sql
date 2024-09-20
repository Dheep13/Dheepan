--liquibase formatted sql

--changeset jcadby:KYN_Lib_TxnAssign splitStatements:false stripComments:false
--comment: Create library
--ignoreLines:1
set schema ext;

create or replace library EXT.KYN_Lib_TxnAssign default schema ext as
begin
  public variable c_eot constant date := to_date('2200-01-01','yyyy-mm-dd');
  private variable c_log_prefix constant varchar(100) := '['||::CURRENT_OBJECT_NAME||'] ';
  private variable v_uuid varchar(100);
  
  private procedure log(
    IN i_text      varchar(4000) default null,
    IN i_value     decimal(25,10) default null
  ) as
  begin
    kyn_prc_debug(:c_log_prefix||:i_text, :i_value, :v_uuid);
  end;

  public procedure reset(in i_processingunitseq bigint) as
  begin
    delete from kyn_txnassign_txn_values where processingunitseq = :i_processingunitseq;
    delete from kyn_txnassign_variable where processingunitseq = :i_processingunitseq;
    delete from kyn_txnassign_territory where processingunitseq = :i_processingunitseq;
    delete from kyn_txnassign_lkp where processingunitseq = :i_processingunitseq;
    log('reset done');
  end;
  
  public procedure load_txn_values(in i_processingunitseq bigint, in i_periodSeq bigint, out o_rowcount integer) as
  begin
    declare v_periodRow row like cs_period;
    select * into v_periodRow from cs_period where periodseq = :i_periodSeq and removedate = :c_eot;  

    insert into kyn_txnassign_txn_values
    select distinct st.processingunitseq, :v_periodRow.periodseq, st.eventtypeseq, et.eventtypeid, st.productid, st.genericattribute3
    from cs_salestransaction st
    join cs_eventtype et on st.eventtypeseq = et.datatypeseq and et.removedate = :c_eot
    where st.isrunnable = 1
    and st.compensationdate >= :v_periodRow.startdate
    and st.compensationdate < :v_periodRow.enddate
    and st.productid is not null
    and st.genericattribute3 is not null
    and st.processingunitseq = :i_processingunitseq;
    
    o_rowcount := ::ROWCOUNT;
    
    log('load_txn_values insert', ::ROWCOUNT);
  
  end;

  public procedure load_variable(in i_processingunitseq bigint, in i_periodSeq bigint) as
  begin
    declare v_periodRow row like cs_period;
    select * into v_periodRow from cs_period where periodseq = :i_periodSeq and removedate = :c_eot;
  
    -- territory directly assigned to a position or title
    insert into kyn_txnassign_variable (
      processingunitseq, variableseq, variable_name, territoryseq, territory_name, titleseq, title_name,
      positionseq,position_name, default_flag   
    )
    select
      :i_processingunitseq,
      var.ruleelementseq as variableseq,
      var.name as variable_name,
      t.ruleelementseq as territoryseq,
      t.name as territory_name, 
      ttl.ruleelementownerseq as titleseq,
      ttl.name as title_name,
      pos.ruleelementownerseq as positionseq,
      pos.name as position_name,
      0 as default_flag
    from cs_territory t
    join cs_variableassignment vas on
      t.ruleelementseq = vas.assignmentseq
      and vas.removedate = :c_eot
      and vas.effectivestartdate < :v_periodRow.enddate
      and vas.effectiveenddate >= :v_periodRow.enddate
    join cs_variable var on
      vas.variableseq = var.ruleelementseq
      and var.removedate = :c_eot
      and var.effectivestartdate < :v_periodRow.enddate
      and var.effectiveenddate >= :v_periodRow.enddate
    left outer join cs_title ttl on
      ttl.ruleelementownerseq = vas.ruleelementownerseq
      and ttl.removedate = :c_eot
      and ttl.effectivestartdate < :v_periodRow.enddate
      and ttl.effectiveenddate >= :v_periodRow.enddate
    left outer join cs_position pos on
      pos.ruleelementownerseq = vas.ruleelementownerseq
      and pos.removedate = :c_eot
      and pos.effectivestartdate < :v_periodRow.enddate
      and pos.effectiveenddate >= :v_periodRow.enddate
    where t.removedate = :c_eot
      and t.effectivestartdate < :v_periodRow.enddate
      and t.effectiveenddate >= :v_periodRow.enddate
    order by t.name, ttl.name, pos.name;
    
    log('kyn_txnassign_variable insert #1', ::ROWCOUNT);
    
    -- default territory on plan directly assigned to title/position
    insert into kyn_txnassign_variable (
      processingunitseq, variableseq, variable_name, territoryseq, territory_name, titleseq, title_name,
      positionseq,position_name, default_flag   
    )
    select
      :i_processingunitseq,
      var.ruleelementseq as variableseq,
      var.name as variable_name,
      var.defaultelementseq as default_territoryseq,
      t.name as default_territory_name,
      ttl.ruleelementownerseq as titleseq, 
      ttl.name as title_name, 
      pos.ruleelementownerseq as positionseq, 
      pos.name as position_name,
      1 as default_flag
    from cs_variable var
    join cs_territory t on
      var.defaultelementseq = t.ruleelementseq 
      and t.removedate = :c_eot
      and t.effectivestartdate < :v_periodRow.enddate
      and t.effectiveenddate >= :v_periodRow.enddate
    join cs_rule_elements rel on 
      rel.ruleelementseq = var.ruleelementseq 
      and rel.removedate = :c_eot
      and rel.effectivestartdate < :v_periodRow.enddate
      and rel.effectiveenddate >= :v_periodRow.enddate      
    join cs_plan_rules pr on 
      rel.ruleseq = pr.ruleseq 
      and pr.removedate = :c_eot
      and pr.effectivestartdate < :v_periodRow.enddate
      and pr.effectiveenddate >= :v_periodRow.enddate          
    join cs_planassignable pas on 
      pr.planseq = pas.planseq
      and pas.removedate = :c_eot
      and pas.effectivestartdate < :v_periodRow.enddate
      and pas.effectiveenddate >= :v_periodRow.enddate
    left outer join cs_title ttl on
      ttl.ruleelementownerseq = pas.ruleelementownerseq
      and ttl.removedate = :c_eot
      and ttl.effectivestartdate < :v_periodRow.enddate
      and ttl.effectiveenddate >= :v_periodRow.enddate
    left outer join cs_position pos on
      pos.ruleelementownerseq = pas.ruleelementownerseq
      and pos.removedate = :c_eot
      and pos.effectivestartdate < :v_periodRow.enddate
      and pos.effectiveenddate >= :v_periodRow.enddate
    where var.removedate = :c_eot 
      and var.referenceclasstype = 'com.callidus.territory.Territory'
      and var.effectivestartdate < :v_periodRow.enddate
      and var.effectiveenddate >= :v_periodRow.enddate;
      
    log('kyn_txnassign_variable insert #2', ::ROWCOUNT);
    
    -- default territory on plan component directly assigned to title/position
    insert into kyn_txnassign_variable (
      processingunitseq, variableseq, variable_name, territoryseq, territory_name, titleseq, title_name,
      positionseq,position_name, default_flag   
    )
    select
      :i_processingunitseq,
      var.ruleelementseq as variableseq,
      var.name as variable_name,
      var.defaultelementseq as default_territoryseq,
      t.name as default_territory_name,
      ttl.ruleelementownerseq as titleseq, 
      ttl.name as title_name, 
      pos.ruleelementownerseq as positionseq, 
      pos.name as position_name,
      1 as default_flag
    from cs_variable var
    join cs_territory t on 
      var.defaultelementseq = t.ruleelementseq 
      and t.removedate = :c_eot
      and t.effectivestartdate < :v_periodRow.enddate
      and t.effectiveenddate >= :v_periodRow.enddate
    join cs_rule_elements rel on 
      rel.ruleelementseq = var.ruleelementseq 
      and rel.removedate = :c_eot
      and rel.effectivestartdate < :v_periodRow.enddate
      and rel.effectiveenddate >= :v_periodRow.enddate    
    join cs_plancomponent_rules pcr on 
      rel.ruleseq = pcr.ruleseq 
      and pcr.removedate = :c_eot
      and pcr.effectivestartdate < :v_periodRow.enddate
      and pcr.effectiveenddate >= :v_periodRow.enddate  
    join cs_plan_plancomponents ppc on
      pcr.plancomponentseq = ppc.plancomponentseq
      and ppc.removedate = :c_eot
      and ppc.effectivestartdate < :v_periodRow.enddate
      and ppc.effectiveenddate >= :v_periodRow.enddate  
    join cs_planassignable pas on 
      ppc.planseq = pas.planseq
      and pas.removedate = :c_eot
      and pas.effectivestartdate < :v_periodRow.enddate
      and pas.effectiveenddate >= :v_periodRow.enddate
    left outer join cs_title ttl on
      ttl.ruleelementownerseq = pas.ruleelementownerseq
      and ttl.removedate = :c_eot
      and ttl.effectivestartdate < :v_periodRow.enddate
      and ttl.effectiveenddate >= :v_periodRow.enddate
    left outer join cs_position pos on
      pos.ruleelementownerseq = pas.ruleelementownerseq
      and pos.removedate = :c_eot
      and pos.effectivestartdate < :v_periodRow.enddate
      and pos.effectiveenddate >= :v_periodRow.enddate
    where var.removedate = :c_eot
      and var.referenceclasstype = 'com.callidus.territory.Territory'
      and var.effectivestartdate < :v_periodRow.enddate
      and var.effectiveenddate >= :v_periodRow.enddate;
      
    log('kyn_txnassign_variable insert #3', ::ROWCOUNT);
    
    -- remove defaults when a non-default exists
    delete from kyn_txnassign_variable x
    where x.processingunitseq = :i_processingunitseq
    and x.default_flag = 1
    and exists (
      select 1
      from ext.kyn_txnassign_variable y
      where x.variableseq = y.variableseq
      and x.titleseq = y.titleseq
      and y.default_flag = 0
      and y.processingunitseq = :i_processingunitseq
    );
    
    log('kyn_txnassign_variable delete #1', ::ROWCOUNT);
    
    delete from kyn_txnassign_variable x
    where x.processingunitseq = :i_processingunitseq
    and x.default_flag = 1
    and exists (
      select 1
      from ext.kyn_txnassign_variable y
      where x.variableseq = y.variableseq
      and x.positionseq = y.positionseq
      and y.default_flag = 0
      and y.processingunitseq = :i_processingunitseq
    );
    
    log('kyn_txnassign_variable delete #2', ::ROWCOUNT);
    
    update kyn_txnassign_variable
    set eventtypeid = 
      case 
        when variable_name like '%Signings%' then 'Signings'
        when variable_name like '%Profit%' then 'Profit'
        when variable_name like '%Revenue%' then 'Revenue'
      end
    where processingunitseq = :i_processingunitseq;
      
    log('kyn_txnassign_variable update #1', ::ROWCOUNT);

    update kyn_txnassign_variable x
    set x.eventtypeseq = (
      select et.datatypeseq
      from cs_eventtype et
      where et.removedate = :c_eot
      and upper(x.eventtypeid) = upper(et.eventtypeid)
    )
    where processingunitseq = :i_processingunitseq;

    log('kyn_txnassign_variable update #2', ::ROWCOUNT);    
    
  end;
  
  public procedure load_territory(in i_processingunitseq bigint, in i_periodSeq bigint) as
  begin
    declare v_periodRow row like cs_period;
    select * into v_periodRow from cs_period where periodseq = :i_periodSeq and removedate = :c_eot;
   
    -- category
    insert into kyn_txnassign_territory
    select 
      :i_processingunitseq,
      t.ruleelementseq as territoryseq, 
      t.name as territory_name, 
      cat.ruleelementseq as categoryseq, 
      cat.name as category_name,
      cl.classifierseq,
      cl.classifierid
    from cs_territory t
    join cs_ruleelement_references rr on 
      rr.referringelementsseq = t.ruleelementseq 
      and rr.removedate = :c_eot
      and rr.effectivestartdate < :v_periodRow.enddate 
      and rr.effectiveenddate >= :v_periodRow.enddate 
    join cs_category cat on
      cat.ruleelementseq = rr.referencesseq 
      and cat.removedate = :c_eot
      and cat.effectivestartdate < :v_periodRow.enddate
      and cat.effectiveenddate >= :v_periodRow.enddate
    join cs_category_classifiers cc on
      cat.ruleelementseq = cc.categoryseq
      and cc.removedate = :c_eot
      and cc.effectivestartdate < :v_periodRow.enddate
      and cc.effectiveenddate >= :v_periodRow.enddate
    join cs_classifier cl on
      cl.classifierseq = cc.classifierseq
      and cl.removedate = :c_eot
      and cl.effectivestartdate < :v_periodRow.enddate
      and cl.effectiveenddate >= :v_periodRow.enddate
    where t.removedate = :c_eot
    and t.effectivestartdate < :v_periodRow.enddate
    and t.effectiveenddate >= :v_periodRow.enddate;
    
    log('kyn_txnassign_territory insert #1', ::ROWCOUNT);
    
    -- classifiers
    insert into kyn_txnassign_territory (processingunitseq, territoryseq, territory_name, classifierseq, classifierid)
    select
      :i_processingunitseq,
      t.ruleelementseq as territoryseq, 
      t.name as territory_name,
      cl.classifierseq,
      cl.classifierid
    from cs_territory t
    join cs_ruleelement_references rr on 
      rr.referringelementsseq = t.ruleelementseq 
      and rr.removedate = :c_eot
      and rr.effectivestartdate < :v_periodRow.enddate 
      and rr.effectiveenddate >= :v_periodRow.enddate
    join cs_classifier cl on
      cl.classifierseq = rr.referencesseq 
      and cl.removedate = :c_eot
      and cl.effectivestartdate < :v_periodRow.enddate
      and cl.effectiveenddate >= :v_periodRow.enddate
    where t.removedate = :c_eot
    and t.effectivestartdate < :v_periodRow.enddate
    and t.effectiveenddate >= :v_periodRow.enddate;
    
    log('kyn_txnassign_territory insert #2', ::ROWCOUNT);    

  end;
  
  public procedure reduce_dataset(in i_processingunitseq bigint) as
  begin
  
    delete from kyn_txnassign_variable var
    where not exists (
      select 1
      from kyn_txnassign_txn_values txn
      where var.eventtypeseq = txn.eventtypeseq
        and txn.processingunitseq = :i_processingunitseq
    )
    and var.processingunitseq = :i_processingunitseq;
    
    log('kyn_txnassign_variable reduce_dataset', ::ROWCOUNT);
  
    delete from kyn_txnassign_territory ter
    where not exists (
      select 1
      from kyn_txnassign_txn_values txn
      where ter.classifierid = txn.productid
        and txn.processingunitseq = :i_processingunitseq
    )
    and ter.processingunitseq = :i_processingunitseq;
    
    log('kyn_txnassign_territory reduce_dataset', ::ROWCOUNT);
  
  end;
  
  public procedure load_lkp(in i_processingunitseq bigint, in i_periodSeq bigint, in i_full_lkp boolean default false) as
  begin
    declare v_periodRow row like cs_period;
    select * into v_periodRow from cs_period where periodseq = :i_periodSeq and removedate = :c_eot;
    
    -- load assignments to positions
    insert into KYN_TxnAssign_LKP (
      processingunitseq, periodseq, period_name, variableseq, variable_name, 
      eventtypeseq, eventtypeid, territoryseq, territory_name,
      categoryseq, category_name, classifierseq, classifierid,
      planning_accountseq, planning_accountid, bp_accountseq, bp_accountid,
      positionseq, position_name
    )
    select
      :i_processingunitseq,
      :v_periodRow.periodseq,
      :v_periodRow.name,
      v.variableseq,
      v.variable_name,
      v.eventtypeseq,
      v.eventtypeid,
      t.territoryseq,
      t.territory_name,
      t.categoryseq,
      t.category_name,
      t.classifierseq,
      t.classifierid, 
      pln.accountseq as planning_accountseq, 
      pln.accountid as planning_accountid,
      bp.accountseq as bp_accountseq, 
      bp.accountid as bp_accountid,
      pln.positionseq, 
      pln.position
    from kyn_tq2com_account pln
    join csq_account bp on
      bp.parentseq = pln.accountseq
      and bp.removedate = :c_eot
      and bp.effectivestartdate < :v_periodRow.enddate
      and bp.effectiveenddate >= :v_periodRow.enddate
    join cs_period per on
      pln.semiannual_periodseq = per.periodseq
      and per.removedate = :c_eot
      and per.startdate < :v_periodRow.enddate
      and per.enddate >= :v_periodRow.enddate
    join kyn_txnassign_variable v on pln.positionseq = v.positionseq
    join kyn_txnassign_territory t on v.territoryseq = t.territoryseq
    where pln.active_flag = 1
    and pln.tacc_esd < :v_periodRow.enddate
    and pln.tacc_edd >= :v_periodRow.enddate
    and (:i_full_lkp = true 
          or exists (
            select 1
            from ext.kyn_txnassign_txn_values y
            where t.classifierid = y.productid
              and bp.accountid = y.genericattribute3
              and v.eventtypeseq = y.eventtypeseq
              )
           );
    
    log('KYN_TxnAssign_LKP insert #1', ::ROWCOUNT);     
    
    -- load assignments via titles
    insert into KYN_TxnAssign_LKP (
      processingunitseq, periodseq, period_name, variableseq, variable_name, 
      eventtypeseq, eventtypeid, territoryseq, territory_name,
      categoryseq, category_name, classifierseq, classifierid,
      planning_accountseq, planning_accountid, bp_accountseq, bp_accountid,
      positionseq, position_name 
    )
    select
      :i_processingunitseq,
      :v_periodRow.periodseq,
      :v_periodRow.name,    
      v.variableseq,
      v.variable_name,
      v.eventtypeseq,
      v.eventtypeid,
      t.territoryseq,
      t.territory_name,
      t.categoryseq,
      t.category_name,
      t.classifierseq,
      t.classifierid, 
      pln.accountseq as planning_accountseq, 
      pln.accountid as planning_accountid,
      bp.accountseq as bp_accountseq, 
      bp.accountid as bp_accountid,
      pln.positionseq, 
      pln.position
    from kyn_tq2com_account pln
    join csq_account bp on
      bp.parentseq = pln.accountseq
      and bp.removedate = :c_eot
      and bp.effectivestartdate < :v_periodRow.enddate
      and bp.effectiveenddate >= :v_periodRow.enddate    
    join cs_period per on
      pln.semiannual_periodseq = per.periodseq
      and per.removedate = :c_eot
      and per.startdate < :v_periodRow.enddate
      and per.enddate >= :v_periodRow.enddate
    join cs_position pos on
      pln.positionseq = pos.ruleelementownerseq
      and pos.removedate = :c_eot
      and pos.effectivestartdate < :v_periodRow.enddate
      and pos.effectiveenddate >= :v_periodRow.enddate
    join kyn_txnassign_variable v on pos.titleseq = v.titleseq
    join kyn_txnassign_territory t on v.territoryseq = t.territoryseq
    where pln.active_flag = 1
    and pln.tacc_esd < :v_periodRow.enddate
    and pln.tacc_edd >= :v_periodRow.enddate
    and not exists (
      select 1 
      from KYN_TxnAssign_LKP lkp
      where v.variableseq = lkp.variableseq
      and pln.positionseq = lkp.positionseq
    )
    and (:i_full_lkp = true 
          or exists (
            select 1
            from ext.kyn_txnassign_txn_values y
            where t.classifierid = y.productid
              and bp.accountid = y.genericattribute3
              and v.eventtypeseq = y.eventtypeseq
              )
    );
    
    log('KYN_TxnAssign_LKP insert #2', ::ROWCOUNT);

  end;   
    
  public procedure run( 
    in i_periodSeq bigint default null,
    in i_calendarSeq bigint default null,
    in i_processingUnitSeq bigint default 38280596832649217,
    in i_tentantId varchar(4) default null,
    in i_pipelineRunSeq bigint default null, 
    in i_runMode varchar(255) default null,
    in i_full_lkp boolean default false
  ) as
  begin
    declare v_count integer;
    declare v_periodRow row like cs_period;

    v_uuid := SYSUUID;    
    
    select * into v_periodRow
    from cs_period
    where removedate = :c_eot
    and periodseq = :i_periodSeq;

    log('Start - '||:v_periodRow.name);    
    
    reset(:i_processingUnitSeq);
    load_txn_values(:i_processingUnitSeq, :v_periodRow.periodseq, v_count);

    if :v_count > 0 or :i_full_lkp = true then
      load_variable(:i_processingUnitSeq, :v_periodRow.periodseq);
      load_territory(:i_processingUnitSeq, :v_periodRow.periodseq);
      if :i_full_lkp = false then
        reduce_dataset(:i_processingUnitSeq);
      end if;
      load_lkp(:i_processingUnitSeq, :v_periodRow.periodseq, :i_full_lkp);
    else
      log('Skip LKP creation - no relevant runnable txns');      
    end if;

    commit;
    log('End');
  end;
  
  public procedure run2( 
    in i_periodName varchar(50),
    in i_full_lkp boolean default false
  ) as
  begin
    declare v_periodRow row like cs_period;
    
    select per.* into v_periodRow
    from cs_period per
    join cs_calendar cal on
      per.calendarseq = cal.calendarseq
      and cal.removedate = :c_eot
      and cal.name = 'Fiscal Calendar'
    where per.removedate = :c_eot
    and per.name = :i_periodName;
    
    run(i_periodSeq => :v_periodRow.periodSeq, i_full_lkp => :i_full_lkp);
    
    commit;
  end;

end;