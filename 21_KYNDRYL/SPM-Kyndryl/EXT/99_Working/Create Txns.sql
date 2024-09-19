/*
create table ext.jc_stagesalestransaction (
	    tenantid varchar(4),
    stagesalestransactionseq bigint,
    batchname varchar(90),
    orderid varchar(1000),
    linenumber integer, 
    sublinenumber integer, 
    eventtypeid varchar(255), 
    productid varchar(255), 
    value decimal(25,10),
    unittypeforvalue varchar(255), 
    compensationdate timestamp,
    businessunitname varchar(255),
    genericattribute3 varchar(255)
);
*/

do begin
  declare c_eot constant date := '2200-01-01';
  declare v_seq bigint;
  declare v_tenantid varchar(4);
  declare v_compdate date := '2023-04-15';
  declare v_batchname varchar(90) := 'JC_TEST_TXNASSIGN_'||to_char(v_compdate,'YYYYMMDD');
  declare v_orderId_prefix varchar(40) := 'JC_';
  
  delete from cs_stagesalestransaction where batchname = :v_batchname;
  
  select ifnull(max(stagesalestransactionseq),0) into v_seq from cs_stagesalestransaction;
  select tenantid into v_tenantid from cs_tenant;

  delete from ext.jc_stagesalestransaction;
  insert into ext.jc_stagesalestransaction (
    tenantid,
    stagesalestransactionseq,
    batchname,
    orderid,
    linenumber, 
    sublinenumber, 
    eventtypeid, 
    productid, 
    value,
    unittypeforvalue, 
    compensationdate,
    businessunitname,
    genericattribute3
  )
  (
    select
    :v_tenantid as tenantid, 
    :v_seq + rn as stagesalestransactionseq,
    :v_batchname as batchname,
    :v_orderId_prefix || 
    hash_md5(to_binary(to_char(:v_compdate,'YYYYMMDD')), '00', to_binary(eventtypeid), '00', to_binary(classifierid), '00', to_binary(accountid), '00', to_binary(ifnull(businessunitname,'{null}'))) as orderid,
    1 as linenumber,
    1 as sublinenumber,
    eventtypeid,
    classifierid as productid,
    round(rand()*10000000,2) as value,
    'USD' as unittypeforvalue,
    :v_compdate as compensationdate,
    businessunitname,
    accountid as generiattribute3
    from (
      select
      row_number() over (order by x.eventtypeid, y.classifierid, z.accountid, z.businessunitname) as rn,
      x.eventtypeid, y.classifierid, z.accountid, z.businessunitname
      from
      (select eventtypeid from cs_eventtype where removedate = '2200-01-01' and eventtypeid in ('Signings','Revenue','Profit')) x,
      (
        select cl.classifierid
        from
        cs_categorytree ctr
        join cs_category_classifiers cc on cc.categorytreeseq = ctr.categorytreeseq and cc.removedate = :c_eot
        join cs_classifier cl on cc.classifierseq= cl.classifierseq and cl.removedate = :c_eot
        and cl.effectivestartdate < cc.effectiveenddate and cl.effectiveenddate > cc.effectivestartdate
        where ctr.removedate = :c_eot 
        and ctr.name = 'Sub Practices'
        and cl.effectivestartdate <= :v_compdate
        and cl.effectiveenddate > :v_compdate
      ) y,
      (
        select acc.accountid, bu.name as businessunitname
        from csq_account acc
        left outer join cs_businessunit bu on bitand(acc.businessunitmap, bu.mask) > 1
        where acc.removedate = :c_eot 
        and acc.parentseq is not null 
        and acc.effectivestartdate <= :v_compdate 
        and acc.effectiveenddate > :v_compdate
      ) z
    )
    where mod(rn,10) = 0
  );
  commit;
end;
