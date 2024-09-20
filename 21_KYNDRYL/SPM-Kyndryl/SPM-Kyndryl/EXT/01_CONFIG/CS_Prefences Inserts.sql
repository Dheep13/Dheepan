do begin
  declare v_tenantid varchar(4);
  declare v_username varchar(100) := '__^^CallidusSystemUser^^__';
  select tenantid into v_tenantid from cs_tenant;
  insert into cs_preferences values (:v_tenantid, :v_username, 'pipeline.generateAssignment', 'true');
  insert into cs_preferences values (:v_tenantid, :v_username, 'pipeline.masterRuleName',     'KYN');
  commit;
end;