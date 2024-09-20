-- generate active account data in tq2com table
do begin
  declare v_run_key bigint;
  declare cursor c_tp for
  select distinct x.documentprocessseq, x.generatedate, x.positionseq, x.position, vtp.territoryprogramseq, vtp.territoryprogram_name
  from ext.kyn_tq2com_ipl_trace x 
  join cs_period per on x.semiannual_periodseq = per.periodseq and per.removedate = '2200-01-01'
  join ext.kyn_v_tq2com_wf_position vp on x.positionseq = vp.positionseq 
  join ext.kyn_v_tq2com_wf_tp vtp on vp.territoryprogramseq = vtp.territoryprogramseq and per.parentseq = vtp.periodseq
  where x.acceptdate is not null
  order by x.documentprocessseq, vtp.territoryprogram_name;
  
  delete from ext.kyn_tq2com_sync;
  delete from ext.kyn_tq2com_filter;
  delete from ext.kyn_tq2com_account;
  delete from ext.kyn_tq2com_product;
  delete from ext.kyn_tq2com_tq_quota;
  delete from ext.kyn_tq2com_prestage_quota;
  
  select ifnull(max(run_key)+1,1) into v_run_key from ext.kyn_tq2com_sync;
  
  for x as c_tp
  do
    insert into ext.kyn_tq2com_sync (run_key, run_date, territoryprogramseq, territoryprogram_name)
    values (:v_run_key, :x.generatedate, :x.territoryprogramseq, :x.territoryprogram_name);
    insert into ext.kyn_tq2com_filter values (:v_run_key, 'POSITION', :x.position);
    v_run_key := v_run_key + 1;
  end for;
  
  delete from ext.kyn_tq2com_ipl_trace;
  
  commit;
end;

-- check assignements
select compensationdate, positionname, count(*)
from cs_transactionassignment 
where genericboolean6 = 1
group by compensationdate, positionname;
  

-- lookup has 1230 records
select count(*) from ext.KYN_TxnAssign_LKP;

-- txn assignments have 1229
select count(*)
from cs_salestransaction st 
join cs_plrun plr on st.pipelinerunseq = plr.pipelinerunseq
join cs_transactionassignment ta on st.salestransactionseq = ta.salestransactionseq and ta.genericboolean6 = 1
where plr.batchname = 'JC_TEST_TXNASSIGN_20230415';


-- check for records that didnt create assignement but have an entry in the lookup - no issue, there were other batches
select plr.batchname, st.productid, et.eventtypeid, st.genericattribute3, ta.positionname, ta.genericboolean6
from cs_salestransaction st 
join cs_plrun plr on st.pipelinerunseq = plr.pipelinerunseq
left outer join cs_transactionassignment ta on st.salestransactionseq = ta.salestransactionseq and ta.genericboolean6 = 1
join cs_eventtype et on st.eventtypeseq = et.datatypeseq and et.removedate = '2200-01-01'
where st.compensationdate >= '2023-04-01' 
and st.compensationdate < '2023-05-01'
and ta.salestransactionseq is null
and (et.eventtypeid, st.productid, st.genericattribute3) in (select eventtypeid, classifierid, bp_accountid from ext.KYN_TxnAssign_LKP);


-- volume test
-- next we mockup active account data for a large lookup
insert into ext.kyn_tq2com_account (run_key, semiannual_periodseq,
semiannual_name, active_flag, active_start, active_end, territory, positionseq, 
position, tacc_esd, tacc_edd, accountseq, accountid, isaddedduetoparent)
select 9999, 2533274790396385, 'HY1 2024', 1, current_timestamp, '2200-01-01', 'DummyDataForTxnAssignTest', 
4785074604095229, '5009345_GermanSchmidt_01', '2023-04-01', '2024-04-01',
acc.accountseq, acc.accountid, 0
from csq_account acc
where acc.removedate = '2200-01-01'
and acc.islast = 1
and not exists (select 1 from ext.kyn_tq2com_account x where x.accountseq = acc.accountseq);

-- switch the title of the position to 'DPE001 - DPE Complex Account (By Exception)'



-- check which territories have most accounts
select plr.batchname, st.productid, et.eventtypeid, st.genericattribute3, ta.positionname, ta.genericboolean6
from cs_salestransaction st 
join cs_plrun plr on st.pipelinerunseq = plr.pipelinerunseq
join cs_eventtype et on st.eventtypeseq = et.datatypeseq and et.removedate = '2200-01-01'
join ext.kyn_txnassign_territory
where st.compensationdate >= '2023-04-01' 
and st.compensationdate < '2023-05-01';





-- find generate date of accepted IPL for this position
select * from ext.kyn_tq2com_ipl_trace 
where status = 'status_Accepted' 
and position = '5009345_GermanSchmidt_01'
order by generatedate desc;

-- find which TPs for position
select * from ext.kyn_v_tq2com_wf_position where position_name = '5009345_GermanSchmidt_01';





-- back date the sync run so that the account records are set to active
do begin
delete from ext.kyn_tq2com_ipl_trace;
delete from ext.kyn_tq2com_sync;
insert into ext.kyn_tq2com_sync (run_key, run_date, territoryprogramseq, territoryprogram_name)
values (1, '2023-04-25 12:14:22', 507217908032603114, 'FY24H1_Seller_WW_DI');
insert into ext.kyn_tq2com_filter values (1, 'POSITION', '5009345_GermanSchmidt_01');
commit;
end;

select * from ext.kyn_tq2com_sync;

select * from ext.kyn_tq2com_account; -- we need active records here


select * from ext.kyn_debug where text like '[KYN_LIB_TXNASSIGN]%' order by 1 desc;





    select count(*) from ext.kyn_txnassign_txn_values;
    
    select * from ext.kyn_txnassign_variable;
    
    select * from ext.kyn_tq2com_account;
    

    
    ;
    delete from kyn_txnassign_variable where processingunitseq = :i_processingunitseq;
    delete from kyn_txnassign_territory where processingunitseq = :i_processingunitseq;
    delete from kyn_txnassign_lkp where processingunitseq = :i_processingunitseq;

select eventtypeid, productid, businessunitname, genericattribute3, compensationdate, orderid,
hash_sha256(to_binary(eventtypeid)) as x,
length(hash_md5(to_binary(eventtypeid))) as z
from ext.jc_stagesalestransaction;

select length('54358A914F51E1AF19DF8520159FE607') from dummy;

select length(orderid), count(*) from ext.jc_stagesalestransaction group by length(orderid);

select 
BATCHNAME, orderid, COUNT(*) 
from cs_stagesalestransaction 
where batchname like 'JC_TEST_TXNASSIGN_%' GROUP BY BATCHNAME, orderid
having count(*) > 1;

select 
BATCHNAME,  COUNT(*) 
from cs_stagesalestransaction 
where batchname like 'JC_TEST_TXNASSIGN_%' GROUP BY BATCHNAME;

select 
BATCHNAME, businessunitname, COUNT(*) 
from cs_stagesalestransaction 
where batchname like 'JC_TEST_TXNASSIGN_%' 
GROUP BY BATCHNAME, businessunitname;

select compensationdate, count(*) from cs_salestransaction st group by compensationdate order by 2 desc;

select * from cs_businessunit;

create table jc_stagesalestransaction as (select * from cs_salestransaction where 1=0);

alter table jc_stagesalestransaction alter (salesorderid varchar(10000));


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
	
	stagesalestransactionseq bigint,
	
)

select length('JC_TEST_20230415:6981CADCA8CD12A619DBDB6CBA3834C1C5BE09043C7C0F8FA5E167610865EB53') from dummy;

        select acc.accountid,  bu.name
        from csq_account acc
        left outer join cs_businessunit bu on bitand(acc.businessunitmap, bu.mask) > 1
        where removedate = '2200-01-01' and parentseq is not null
        and islast = 1;
        
        and effectivestartdate <= :v_compdate and effectiveenddate > :v_compdate

select round(rand()*10000000,2) from dummy;

select * from cs_product;

select * from cs_eventtype where removedate = '2200-01-01' and eventtypeid in ('Signings','Revenue','Profit');

select length(accountid), count(*) from csq_account where removedate = '2200-01-01' and parentseq is not null
group by length(accountid)
order by 1;

select count(*), count(distinct cl.classifierid)
from 
cs_categorytree ctr
join cs_category_classifiers cc on cc.categorytreeseq = ctr.categorytreeseq and cc.removedate = '2200-01-01'
join cs_classifier cl on cc.classifierseq= cl.classifierseq and cl.removedate = '2200-01-01'
and cl.effectivestartdate < cc.effectiveenddate and cl.effectiveenddate > cc.effectivestartdate
where ctr.removedate = '2200-01-01' 
and ctr.name = 'Sub Practices';

select * from table_columns where column_name = 'CATEGORYTREESEQ' order by 2;

select * from cs_classifier;