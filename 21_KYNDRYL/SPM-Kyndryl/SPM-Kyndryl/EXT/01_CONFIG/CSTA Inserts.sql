--liquibase formatted sql

--changeset jcadby:CSTA_Inserts splitStatements:false stripComments:false
--comment: Configure the Credit Eligibility rules
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
  delete from csta_preferences;
  
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
    :c_eot, :c_cts, :c_user, 'call EXT.KYN_Prc_TxnAssign(:1,:2,:3,:4,:5,:6)', null
  );
  
  insert into csta_joincondition (
    tenantid, ruleseq, sourcetablename, sourcecolumnname, operator, targettablename, targetColumnName,
    effectivestartdate, effectiveenddate, removedate, createdate
  )
  values (
    :v_tenantid, :v_ruleseq, :c_tab_txn, 'productID', '=', :c_tab_lkp, 'classifierID', 
    :c_sot, :c_eot, :c_eot, :c_cts
  );
  
  insert into csta_joincondition (
    tenantid, ruleseq, sourcetablename, sourcecolumnname, operator, targettablename, targetColumnName, 
    effectivestartdate, effectiveenddate, removedate, createdate
  )
  values (
    :v_tenantid, :v_ruleseq, :c_tab_txn, 'eventTypeSeq', '=', :c_tab_lkp, 'eventTypeSeq', 
    :c_sot, :c_eot, :c_eot, :c_cts
  );

  insert into csta_joincondition (
    tenantid, ruleseq, sourcetablename, sourcecolumnname, operator, targettablename, targetColumnName, 
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
    :v_tenantid, :v_ruleSeq, :c_tab_txn, :c_tab_txn||'.genericAttribute3 is not null and '||:c_tab_txn||'.productID is not null', :c_sot, :c_eot, :c_eot, :c_cts
  );
  
  insert into csta_targetfield (tenantid, ruleseq, sourcetablename, sourcecoumnname, recordtype, effectivestartdate, effectiveenddate, removedate, createdate)
  values (
    :v_tenantid, :v_ruleseq, :c_tab_lkp, 'position_name', 'POSITIONNAME', :c_sot, :c_eot, :c_eot, :c_cts
  );

  insert into csta_preferences values (:v_tenantid, 'UseCompensationDates',      'true');
  insert into csta_preferences values (:v_tenantid, 'PreserveImportedTA',        'true');
  insert into csta_preferences values (:v_tenantid, 'validateECA',               'false');
  insert into csta_preferences values (:v_tenantid, 'AllowAssignmentDuplicates', 'false');
  
  commit;
end;