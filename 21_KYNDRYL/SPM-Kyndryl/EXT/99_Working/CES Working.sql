SELECT  TO_CHAR(COMPENSATIONDATE, 'yyyy-MM') as period, et.eventtypeid, isrunnable, count(*)
from cs_salestransaction st
join cs_eventtype et on st.eventtypeseq = et.datatypeseq and et.removedate= '2200-01-01'
group by et.eventtypeid, TO_CHAR(COMPENSATIONDATE, 'yyyy-MM'), isrunnable order by 1,2;

select * from cs_transactionassignment where compensationdate >= '2023-04-01' and compensationdate < '2023-05-01';

select * from tables where table_name like 'CSTA%' ORDER BY TABLE_NAME;

select * from csta_runlog order by 2 desc;



do begin
  declare v_eot timestamp := '2200-01-01';
  declare v_calendar row like cs_calendar;
  declare v_period row like cs_period;
  declare v_tenant row like cs_tenant;
  select * into v_calendar from cs_calendar where name = 'Fiscal Calendar' and removedate = :v_eot;
  select * into v_period from cs_period where name = 'April 2023' and calendarseq = :v_calendar.calendarseq and removedate = :v_eot;
  select * into v_tenant from cs_tenant;
  
  update cs_salestransaction set isrunnable = 1 where compensationdate >= :v_period.startdate and compensationdate < :v_period.enddate;
  commit;
  
  tcmp.CSTA_ASSIGNMENTLIB__RUN(
    INPIPELINERUNSEQ => NULL,
    INPERIODSEQ => :v_period.periodseq,
    INCALENDARSEQ => :v_period.calendarseq,
    INPROCESSINGUNITSEQ => 38280596832649217,
    INMASTERRULENAME => 'KYN',
    INTENTANTID => :v_tenant.tenantid
  );
  commit;
end;

do begin
  declare c_sot constant timestamp := '1900-01-01';
  declare c_eot constant timestamp := '2200-01-01';
  declare c_cts constant timestamp := current_timestamp;
  declare v_tenantid varchar(4);
  declare c_tab_txn varchar(127) := 'CS_SalesTransaction';
  declare c_tab_lkp varchar(127) := 'KYN_TxnAssign_LKP';
  declare v_ruleseq bigint := 1;
  declare v_masterruleseq bigint := 50946970784628737;
  declare c_user constant varchar(255) := 'Kyndryl';
  select tenantid into v_tenantid from cs_tenant;
  
  delete from csta_masterrule;
  delete from csta_rule;
  delete from csta_joincondition;
  delete from csta_expression;
  delete from csta_targetfield;
  
  insert into csta_masterrule(
    tenantid, masterruleseq, masterrulename, effectivestartdate, effectiveenddate, removedate, createdate, createdby
  )
  values (:v_tenantid, :v_masterruleseq, 'KYN', :c_sot, :c_eot, :c_eot, :c_cts, :c_user);
  
  insert into csta_rule (
    tenantid, masterruleseq, ruleseq, runorder, rulename, executerule, effectivestartdate, effectiveenddate, 
    removedate, createdate, createdby, prerulesql, postrulesql
  )
  values (
    :v_tenantid, :v_masterruleseq, :v_ruleseq, 1, 'KYN_Rules', 'Y', :c_sot, :c_eot, 
    :c_eot, :c_cts, :c_user, 'CALL EXT.KYN_Lib_TxnAssign:run(:1,:2,:3,:4,:5,:6)', null);
  );
  
  insert into csta_joincondition (
    tenantid, ruleseq, sourcetablename, sourcecolumnname, operator, targettablename, 
    effectivestartdate, effectiveenddate, removedate, createdate
  )
  values (
    :v_tenantid, :v_ruleseq, :c_tab_txn, 'productID', '=', :c_tab_lkp, 'classifierID', 
    :c_sot, :c_eot, :c_eot, :c_cts
  );
  
  insert into csta_joincondition (
    tenantid, ruleseq, sourcetablename, sourcecolumnname, operator, targettablename, 
    effectivestartdate, effectiveenddate, removedate, createdate
  )
  values (
    :v_tenantid, :v_ruleseq, :c_tab_txn, 'eventTypeSeq', '=', :c_tab_lkp, 'eventTypeSeq', 
    :c_sot, :c_eot, :c_eot, :c_cts
  );

  insert into csta_joincondition (
    tenantid, ruleseq, sourcetablename, sourcecolumnname, operator, targettablename, 
    effectivestartdate, effectiveenddate, removedate, createdate
  )
  values (
    :v_tenantid, :v_ruleseq, :c_tab_txn, 'genericAttribute3', '=', :c_tab_lkp, 'BP_accountID', 
    :c_sot, :c_eot, :c_eot, :c_cts
  );
  
  insert into csta_expression (
    tenantID, ruleSeq, sourceTableName, overrideExpression, effectiveStartDate, effectiveEndDate, removeDate, createDate
  )
  values (
    :v_tenantid, :v_ruleSeq, :c_tab_txn, 'genericAttribute3 is not null and productID is not null', :c_sot, :c_eot, :c_eot, :c_cts);
  );
  
  insert into csta_targetfield (tenantid, ruleseq, sourcetablename, sourcecoumnname, recordtype, effectivestartdate, effectiveenddate, removedate, createdate)
  values (
    :v_tenantid, :v_ruleseq, :c_tab_lkp, 'position_name', 'PositionName', :c_sot, :c_eot, :c_eot, :c_cts
  );
  
  commit;
end;