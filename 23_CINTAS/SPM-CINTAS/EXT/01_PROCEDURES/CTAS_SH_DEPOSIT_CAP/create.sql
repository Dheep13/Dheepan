CREATE PROCEDURE ext.ctas_sh_deposit_cap()
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
DEFAULT SCHEMA EXT
AS
BEGIN

/**************************************************************************************************
	This procedure is for updating deposit cap

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       02-JUL-2023		Rakesh		     Initial creation
    1.1       11-NOV-2023       Deepan           CR - 10 month deposit CAP changes for SR/SS
***************************************************************************************************/
DECLARE v_proc_name varchar2(100) := 'ctas_sh_deposit_cap';
DECLARE v_tenantid varchar(4);
DECLARE in_periodseq bigint;
DECLARE v_parentseq bigint;
DECLARE v_pay_periodseq bigint;
DECLARE v_depositseq bigint;
DECLARE v_yearperiodtypeseq bigint;
DECLARE in_processingunitseq bigint;
DECLARE v_calendar varchar (250):= 'Cintas Hybrid Calendar';
DECLARE v_calendarseq bigint;
DECLARE v_periodstartdate date;
DECLARE v_periodenddate date;
DECLARE v_year_startdate date;
DECLARE v_year_enddate date;
DECLARE v_10months_prev date;
DECLARE v_isr_cap decimal(25,10);
DECLARE v_se_cap decimal(25,10);
DECLARE v_sr_ss_cap decimal(25,10);
DECLARE v_fv_ssr_fst_cap decimal(25,10);
DECLARE v_eot date := to_date('01-JAN-2200', 'DD-MON-YYYY');
DECLARE v_sql varchar2(32000);
DECLARE v_sqlerrm varchar(32000);

declare cursor get_pay_update  with hold
for
select
	distinct 
	payeeseq,
	serviced_custid,
	service_type,
	invoice_date,
	division_code,
	cust_loc
from
	ext.ctas_sh_comm_custidlvl_agg
where periodseq = :in_periodseq
order by division_code,payeeseq;

DECLARE EXIT HANDLER FOR SQLEXCEPTION 
BEGIN v_sqlerrm := ::SQL_ERROR_MESSAGE ;  
ext.ctas_event_log (v_proc_name,::SQL_ERROR_CODE || ' - ' ||ifnull(:v_sqlerrm,''),0);
RESIGNAL ;
END;

select 
	cast(session_context('GLOBVAR_in_periodseq') as bigint) 
into 
	in_periodseq
from 
	sys.dummy;
	
select 
	cast(session_context('GLOBVAR_in_processingunitseq') as bigint) 
into 
	in_processingunitseq 
from 
	sys.dummy;
	
	
call ext.ctas_event_log(:v_proc_name,'Execution Starts for period : '||:in_periodseq,0);	

select
	distinct tenantid
into
	v_tenantid
from
	tcmp.cs_tenant;
	
select 
	calendarseq 
into 
	v_calendarseq 
from 
	tcmp.cs_calendar 
where 
	name = :v_calendar
	and removedate = :v_eot;	
	
	
select
	parentseq
into
	v_parentseq
from
	tcmp.cs_period
where
	periodseq = :in_periodseq
	and removedate = :v_eot;	
	

select
	c.periodseq 
into 
	v_pay_periodseq
from
	tcmp.cs_period c,
	tcmp.cs_period p
where
	c.parentseq = :v_parentseq
	and p.periodseq = :v_parentseq
	and c.parentseq = p.periodseq
	and c.removedate = :v_eot
	and p.removedate = :v_eot
	and c.enddate = p.enddate;
	

select
	startdate,enddate
into
	v_periodstartdate,v_periodenddate
from
	tcmp.cs_period
where
	periodseq = :in_periodseq
	and removedate = :v_eot;
	
select 
	periodtypeseq 
into 
	v_yearperiodtypeseq 
from 
	cs_periodtype 
where 
	name='year';


select
	startdate,enddate
into
	v_year_startdate, v_year_enddate
from
	tcmp.cs_period
where
	periodtypeseq = :v_yearperiodtypeseq
	and calendarseq = :v_calendarseq
	and startdate <= :v_periodstartdate 
	and enddate > :v_periodstartdate		-- AD 9/20 Removed equal to sign for 6/1/2024, which falls in two calendar years
	and removedate = :v_eot;
	

select
	add_months((startdate), -10)
into
	v_10months_prev
from
	tcmp.cs_period
where
	periodseq = :in_periodseq
	and removedate = :v_eot;
	
	

select 
	value
into 
	v_isr_cap
from 
	tcmp.cs_fixedvalue
where     
	name = 'FV_ISR_Cap'
    and effectiveenddate = :v_eot
    and removedate = :v_eot;
       
select 
	value
into 
	v_se_cap
from 
	tcmp.cs_fixedvalue
where     
	name = 'FV_SE_Cap'
    and effectiveenddate = :v_eot
    and removedate = :v_eot;  

select 
	value
into 
	v_sr_ss_cap
from 
	tcmp.cs_fixedvalue
where     
	name = 'FV_SR-SS_Cap'
    and effectiveenddate = :v_eot
    and removedate = :v_eot;  
	
	
select 
	value
into 
	v_fv_ssr_fst_cap
from 
	tcmp.cs_fixedvalue
where     
	name = 'FV_SSR-FST_Cap'
    and effectiveenddate = :v_eot
    and removedate = :v_eot; 	
	
select 
	max(depositseq)
into
	v_depositseq
from
	tcmp.cs_deposit;
	
	
call ext.ctas_event_log(:v_proc_name,'Period Startdate :'||:v_periodstartdate||' and Period Enddate :'||:v_periodenddate,0);

call ext.ctas_event_log(:v_proc_name,'Period Year Startdate :'||:v_year_startdate||' and Period Year Enddate :'||:v_year_enddate,0);

delete from ext.ctas_sh_comm_orderlvl where periodseq = in_periodseq;
commit;

insert into ext.ctas_sh_comm_orderlvl 
	(payeeid,
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	orderid,
	salesorderseq,
	periodseq,
	pay_periodseq,
	processingunitseq,
	businessunitmap,
	periodstartdate,
	comm_value)
select pa.payeeid,
	pa.payeeseq,
	po.ruleelementownerseq,
	ti.name,
	ti.ruleelementownerseq,
	so.orderid,
	so.salesorderseq,
	cr.periodseq,
	:v_pay_periodseq,
	po.processingunitseq,
	pa.businessunitmap,
	v_periodstartdate,
	sum (cm.value)
from 
	tcmp.cs_salestransaction tx,
	cs_salesorder so,
	cs_credit cr,
	cs_commission cm,
	cs_payee pa,
	cs_position po,
	cs_title ti
where     
	tx.salesorderseq = so.salesorderseq
	and tx.genericattribute22 = 'Invoiced'
	and tx.salestransactionseq = cr.salestransactionseq
	and so.salesorderseq = cr.salesorderseq
	and cr.creditseq = cm.creditseq
	and cr.payeeseq = cm.payeeseq
	and cr.positionseq = cm.positionseq
	and cr.periodseq = :in_periodseq
	and cm.periodseq = :in_periodseq
	and cr.periodseq = cm.periodseq
	and cr.payeeseq = pa.payeeseq
	and cm.payeeseq = pa.payeeseq
	and pa.payeeseq = po.payeeseq
	and po.titleseq = ti.ruleelementownerseq
	and ti.name in ('Sales Engineer','Inside Sales Representative')
	and tx.processingunitseq = so.processingunitseq
	and cr.processingunitseq = cm.processingunitseq
	and tx.processingunitseq = cm.processingunitseq
	and tx.processingunitseq = :in_processingunitseq
	and so.processingunitseq = :in_processingunitseq
	and cr.processingunitseq = :in_processingunitseq
	and cm.processingunitseq = :in_processingunitseq
	and so.removedate = :v_eot
	and pa.effectiveenddate = :v_eot
	and pa.removedate = :v_eot
	and po.effectiveenddate = :v_eot
	and po.removedate = :v_eot
	and ti.effectiveenddate = :v_eot
	and ti.removedate = :v_eot
group by 
	pa.payeeid,
	pa.payeeseq,
	po.ruleelementownerseq,
	ti.name,
	ti.ruleelementownerseq,
	so.orderid,
	so.salesorderseq,
	cr.periodseq,
	po.processingunitseq,
	pa.businessunitmap;

call ext.ctas_event_log(:v_proc_name,'ctas_sh_comm_orderlvl: Number of rows Inserted',::ROWCOUNT );

--updating cap_value

commit;	   

update 
	ext.ctas_sh_comm_orderlvl
set 
	cap_value = :v_isr_cap, 
	bal_value = :v_isr_cap
where 
	title_name = 'Inside Sales Representative'
	and periodseq = :in_periodseq;

commit;

call ext.ctas_event_log(:v_proc_name,'Cap Value for Inside Sales Representative: Number of rows Updated ',::ROWCOUNT);

update 
	ext.ctas_sh_comm_orderlvl
set 
	cap_value = :v_se_cap, 
	bal_value = :v_se_cap
where 
	title_name = 'Sales Engineer'
	and periodseq = :in_periodseq;

commit; 

call ext.ctas_event_log(:v_proc_name,'Cap Value for Sales Engineer: Number of rows Updated ',::ROWCOUNT);

--updating bal_value

merge into ext.ctas_sh_comm_orderlvl tgt
using (select 
		payeeseq,
		salesorderseq,
		sum (pay_value) pay_value
     from
    	ext.ctas_sh_comm_orderlvl
     where 
		title_name in ('Sales Engineer','Inside Sales Representative')
		and periodstartdate < :v_periodstartdate
		--and periodstartdate >= :v_year_startdate
 	group by
		payeeseq,salesorderseq
) src
on (tgt.payeeseq = src.payeeseq
	and tgt.salesorderseq = src.salesorderseq
	and tgt.periodseq = :in_periodseq
)
when matched
then
   update set tgt.bal_value = tgt.cap_value - ifnull(src.pay_value,0);

commit;

call ext.ctas_event_log(:v_proc_name,'BAL_VALUE Field Sales Engineer and Inside Sales Representative : Number of rows Updated ',::ROWCOUNT);


--updating pay_value

update ext.ctas_sh_comm_orderlvl
   set pay_value = least (bal_value, comm_value)
where 
	periodseq = :in_periodseq;
	
commit;	

call ext.ctas_event_log(:v_proc_name,'PAY_VALUE Field in ctas_sh_comm_orderlvl : Number of rows Updated ',::ROWCOUNT);

truncate table ext.ctas_sh_comm_orderlvl_stg;

insert into ext.ctas_sh_comm_orderlvl_stg
select 
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	periodseq,
	pay_periodseq,
	processingunitseq,
	businessunitmap,
	sum(pay_value)
from 
	ext.ctas_sh_comm_orderlvl 
where 
	pay_periodseq = :v_pay_periodseq
group by 
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	periodseq,
	pay_periodseq,
	processingunitseq,
	businessunitmap;
commit;

truncate table ext.ctas_deposit_stg;

-- deriving adjustments for ISR and SE deposits

insert into ext.ctas_deposit_stg 
with dep
as
(select 
	payeeseq,
	positionseq,
	periodseq,
	sum(value) as preadjustedvalue,     
	unittypeforpreadjustedvalue,
	sum(value) as value,
	unittypeforvalue,
	earninggroupid,                              
	earningcodeid
from 
	cs_deposit deposit
where 
	deposit.name in 
		('DO_Revenue_Commission_ISR',
		'DO_Revenue_Commission_OSR_Assist_ISR',
		'DO_Revenue_Commission_SE',
		'DO_Revenue_Commission_ISR_Cap',
		'DO_Revenue_Commission_SE_Cap')
	and deposit.earninggroupid = 'Earnings'
	and deposit.earningcodeid = 'Non Sales Commission'
group by
	deposit.payeeseq,
	deposit.positionseq,
	deposit.periodseq,
	deposit.unittypeforpreadjustedvalue,
	deposit.unittypeforvalue,
	deposit.earninggroupid,
	deposit.earningcodeid	
) 
select :v_tenantid,
	v_depositseq,
	case
		when stg.title_name = 'Inside Sales Representative'
		then
		   'DO_Revenue_Commission_ISR_Cap'
		when stg.title_name = 'Sales Engineer'
		then
		   'DO_Revenue_Commission_SE_Cap'
	end as deposit_name,
	stg.payeeseq,
	stg.positionseq,
	stg.pay_periodseq,
	(select pipelinerunseq
	   from cs_pipelinerun
	  where status = 'Running')
	   as pipelinerunseq,
	'manual' as origintypeid,
	(select starttime
	   from cs_pipelinerun
	  where status = 'Running')
	   as pipelinerundate,
	stg.businessunitmap as businessunitmap,
	case when dep.value = 0 then 0 else sum (stg.pay_value) - dep.value end as preadjustedvalue, 
	dep.unittypeforpreadjustedvalue,
	case when dep.value = 0 then 0 else sum (stg.pay_value) - dep.value end as value,
	dep.unittypeforvalue,
	dep.earninggroupid,                              
	dep.earningcodeid,                               
	0 as isheld,
	null as comments,
	stg.processingunitseq
from 
	ext.ctas_sh_comm_orderlvl_stg stg, dep
where     
	dep.periodseq = stg.pay_periodseq
	and dep.payeeseq = stg.payeeseq
	and dep.positionseq = stg.positionseq
	and dep.periodseq = :in_periodseq
group by 
	stg.title_name,
	dep.payeeseq,
	dep.positionseq,
	dep.periodseq,
	stg.payeeseq,
	stg.positionseq,
	--stg.periodseq,
	stg.pay_periodseq,
	stg.businessunitmap,
	dep.value,
	dep.unittypeforpreadjustedvalue,
	dep.unittypeforvalue,
	dep.earninggroupid,
	dep.earningcodeid,
	stg.processingunitseq;
	
commit;	

call ext.ctas_event_log(:v_proc_name,'ctas_deposit_stg from ctas_sh_comm_orderlvl: Number of rows Inserted ',::ROWCOUNT);


delete from ext.ctas_sh_comm_contractlvl where periodseq = in_periodseq;
commit;

insert into ext.ctas_sh_comm_contractlvl 
	(payeeid,
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	periodseq,
	pay_periodseq,
	genericattribute17,
	processingunitseq,
	businessunitmap,
	compensationdate,
	monitorstartdate,
	monitorenddate,
	comm_value

	)
select pa.payeeid,
	pa.payeeseq,
	po.ruleelementownerseq,
	ti.name,
	ti.ruleelementownerseq,
	cr.periodseq,
	:v_pay_periodseq,
	tx.genericattribute17,
	po.processingunitseq,
	pa.businessunitmap,
	-- v_periodstartdate,
	tx.compensationdate,--CR4-use compdate instead of period startdate
	tx.genericdate5,--CR4-Monitor start Date
	tx.genericdate6,--CR4-Monitor end Date
	sum (cm.value)
	
from 
	tcmp.cs_salestransaction tx,
	cs_salesorder so,
	cs_credit cr,
	cs_commission cm,
	cs_payee pa,
	cs_position po,
	cs_title ti
where     
	tx.salesorderseq = so.salesorderseq
	and tx.genericattribute22 = 'Invoiced'
	and tx.salestransactionseq = cr.salestransactionseq
	and so.salesorderseq = cr.salesorderseq
	and cr.creditseq = cm.creditseq
	and cr.payeeseq = cm.payeeseq
	and cr.positionseq = cm.positionseq
	and cr.periodseq = :in_periodseq
	and cm.periodseq = :in_periodseq
	and cr.periodseq = cm.periodseq
	and cr.payeeseq = pa.payeeseq
	and cm.payeeseq = pa.payeeseq
	and pa.payeeseq = po.payeeseq
	and po.titleseq = ti.ruleelementownerseq
	and ti.name in ('Sales Representative','Sales Specialist')
	and tx.processingunitseq = so.processingunitseq
	and cr.processingunitseq = cm.processingunitseq
	and tx.processingunitseq = cm.processingunitseq
	and tx.processingunitseq = :in_processingunitseq
	and so.processingunitseq = :in_processingunitseq
	and cr.processingunitseq = :in_processingunitseq
	and cm.processingunitseq = :in_processingunitseq
	and so.removedate = :v_eot
	and pa.effectiveenddate = :v_eot
	and pa.removedate = :v_eot
	and po.effectiveenddate = :v_eot
	and po.removedate = :v_eot
	and ti.effectiveenddate = :v_eot
	and ti.removedate = :v_eot
group by 
	pa.payeeid,
	pa.payeeseq,
	po.ruleelementownerseq,
	ti.name,
	ti.ruleelementownerseq,
	cr.periodseq,
	tx.genericattribute17,
	tx.genericdate5,
	tx.genericdate6,
	tx.compensationdate,
	po.processingunitseq,
	pa.businessunitmap;
commit;	   

call ext.ctas_event_log(:v_proc_name,'ctas_sh_comm_contractlvl: Number of rows Inserted ',::ROWCOUNT);

-- updating cap_value, bal_value
 
update 
	ext.ctas_sh_comm_contractlvl
set 
	cap_value = :v_sr_ss_cap, 
	bal_value = :v_sr_ss_cap
where 
	title_name in ('Sales Representative','Sales Specialist')
	and periodseq = :in_periodseq;

commit; 

call ext.ctas_event_log(:v_proc_name,'Cap Value in ctas_sh_comm_contractlvl : Number of rows Updated ',::ROWCOUNT);

-- re-updating bal_value

merge into ext.ctas_sh_comm_contractlvl tgt
using (select 
		payeeseq,
		genericattribute17,
		sum (pay_value) pay_value
     from
    	ext.ctas_sh_comm_contractlvl
     where 
		title_name in ('Sales Representative','Sales Specialist')
		-- and periodstartdate < :v_periodstartdate--CR4 Changes
		-- and periodstartdate >= :v_10months_prev --CR4 Changes
		and compensationdate <= add_months(monitorstartdate,10)--CR4 Changes
		and compensationdate >= monitorstartdate--CR4 Changes
 	group by
		payeeseq,genericattribute17
) src
on (tgt.periodseq = :in_periodseq
	and tgt.payeeseq = src.payeeseq
	and tgt.genericattribute17 = src.genericattribute17)
when matched
then
   update set tgt.bal_value = tgt.cap_value - ifnull(src.pay_value,0);

commit;


call ext.ctas_event_log(:v_proc_name,'BAL_VALUE Field in ctas_sh_comm_contractlvl : Number of rows Updated ',::ROWCOUNT);

-- updating pay_value

update ext.ctas_sh_comm_contractlvl
   set pay_value = case when compensationdate <= add_months(monitorstartdate,10) and compensationdate >= monitorstartdate then least (bal_value, comm_value)
                   else 0.0 end --CR4 changes
where 
	periodseq = :in_periodseq;

commit;	
	
commit;	

call ext.ctas_event_log(:v_proc_name,'PAY_VALUE Field in ctas_sh_comm_contractlvl : Number of rows Updated ',::ROWCOUNT);

-- deriving adjustments for Sales Representative and Sales Specialist deposits

insert into ext.ctas_sh_comm_contractlvl_stg
select 
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	periodseq,
	pay_periodseq,
	genericattribute17,
	processingunitseq,
	businessunitmap,
	sum(pay_value)
from
	ext.ctas_sh_comm_contractlvl
where
	periodseq = :v_pay_periodseq
group by
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	periodseq,
	pay_periodseq,
	genericattribute17,
	processingunitseq,
	businessunitmap;	

commit;

insert into ext.ctas_deposit_stg 
with dep
as
(select 
	payeeseq,
	positionseq,
	periodseq,
	sum(value) as preadjustedvalue,     
	unittypeforpreadjustedvalue,
	sum(value) as value,
	unittypeforvalue,
	earninggroupid,                              
	earningcodeid
from 
	cs_deposit deposit
where 
	deposit.name in ('DO_SRSS_Commissions','DO_SRSS_Commissions_Cap')
	and deposit.earninggroupid = 'Earnings'
	and deposit.earningcodeid = 'Non Sales Commission'
group by
	deposit.payeeseq,
	deposit.positionseq,
	deposit.periodseq,
	deposit.unittypeforpreadjustedvalue,
	deposit.unittypeforvalue,
	deposit.earninggroupid,
	deposit.earningcodeid	
) 
select :v_tenantid,
	v_depositseq,
	'DO_SRSS_Commissions_Cap' as deposit_name,
	stg.payeeseq,
	stg.positionseq,
	stg.pay_periodseq,
	(select pipelinerunseq
	   from cs_pipelinerun
	  where status = 'Running')
	   as pipelinerunseq,
	'manual' as origintypeid,
	(select starttime
	   from cs_pipelinerun
	  where status = 'Running')
	   as pipelinerundate,
	stg.businessunitmap as businessunitmap,
	case when dep.value = 0 then 0 else sum (stg.pay_value) - dep.value end as preadjustedvalue, 
	dep.unittypeforpreadjustedvalue,
	case when dep.value = 0 then 0 else sum (stg.pay_value) - dep.value end as value,
	dep.unittypeforvalue,
	dep.earninggroupid,                              
	dep.earningcodeid,                               
	0 as isheld,
	null as comments,
	stg.processingunitseq
from 
	ext.ctas_sh_comm_contractlvl_stg stg, dep
where     
	dep.periodseq = stg.pay_periodseq
	and dep.payeeseq = stg.payeeseq
	and dep.positionseq = stg.positionseq
	and dep.periodseq = :in_periodseq
group by 
	stg.title_name,
	dep.payeeseq,
	dep.positionseq,
	dep.periodseq,
	stg.payeeseq,
	stg.positionseq,
	--stg.periodseq,
	stg.pay_periodseq,
	stg.businessunitmap,
	dep.value,
	dep.unittypeforpreadjustedvalue,
	dep.unittypeforvalue,
	dep.earninggroupid,
	dep.earningcodeid,
	stg.processingunitseq;
	
commit;	


call ext.ctas_event_log(:v_proc_name,'ctas_deposit_stg from ctas_sh_comm_contractlvl: Number of rows Inserted ',::ROWCOUNT);


delete from ext.ctas_sh_comm_custidlvl where periodseq = in_periodseq;
commit;

-- Considering New business invoices

insert into ext.ctas_sh_comm_custidlvl
	(payeeid,
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	orderid,
	salesorderseq,
	serviced_custid,
	service_type,
	invoice_date,
	start_date,
	end_date,
	division_code,
	cust_loc,
	periodseq,
	processingunitseq,
	businessunitmap,
	periodstartdate,
	comm_value)
select pa.payeeid,
	pa.payeeseq,
	po.ruleelementownerseq,
	ti.name,
	ti.ruleelementownerseq,
	so.orderid,
	so.salesorderseq,
	tadd.custid,
	tx.genericattribute16,
	tx.compensationdate,
	tx.genericdate1,
	tx.genericdate2,
	tx.genericattribute5,
	tx.genericattribute19,
	cr.periodseq,
	po.processingunitseq,
	pa.businessunitmap,
	v_periodstartdate,
	cm.value
from 
	tcmp.cs_salestransaction tx,
	tcmp.cs_transactionaddress tadd,
	tcmp.cs_addresstype addt,
	tcmp.cs_salesorder so,
	tcmp.cs_credit cr,
	tcmp.cs_commission cm,
	tcmp.cs_payee pa,
	tcmp.cs_position po,
	tcmp.cs_title ti
where     
	tx.salesorderseq = so.salesorderseq
	and tx.genericattribute22 = 'Invoiced'
	and tx.genericboolean1 = 1 -- new business
	and tadd.addresstypeseq = addt.addresstypeseq
	and addt.addresstypeid = 'SHIPTO'
	and tx.salestransactionseq = tadd.salestransactionseq
	and tx.salestransactionseq = cr.salestransactionseq
	and so.salesorderseq = cr.salesorderseq
	and cr.creditseq = cm.creditseq
	and cr.payeeseq = cm.payeeseq
	and cr.positionseq = cm.positionseq
	and cr.periodseq = :in_periodseq
	and cm.periodseq = :in_periodseq
	and cr.periodseq = cm.periodseq
	and cr.payeeseq = pa.payeeseq
	and cm.payeeseq = pa.payeeseq
	and pa.payeeseq = po.payeeseq
	and po.titleseq = ti.ruleelementownerseq
	and ti.name in ('SSR','SSRIT - Bench','SSRIT - Assigned','FST','FST In Training')
	and tx.processingunitseq = so.processingunitseq
	and cr.processingunitseq = cm.processingunitseq
	and tx.processingunitseq = cm.processingunitseq
	and tx.processingunitseq = :in_processingunitseq
	and so.processingunitseq = :in_processingunitseq
	and cr.processingunitseq = :in_processingunitseq
	and cm.processingunitseq = :in_processingunitseq
	and so.removedate = :v_eot
	and pa.effectiveenddate = :v_eot
	and pa.removedate = :v_eot
	and po.effectiveenddate = :v_eot
	and po.removedate = :v_eot
	and ti.effectiveenddate = :v_eot
	and ti.removedate = :v_eot;	

commit;	   

call ext.ctas_event_log(:v_proc_name,'ctas_sh_comm_custidlvl: Number of rows Inserted ',::ROWCOUNT);

delete from ext.ctas_sh_comm_custidlvl_agg where periodseq = :in_periodseq;

commit;

--Aggregating transactions with same invoicedate,offering,customerid and location

insert into ext.ctas_sh_comm_custidlvl_agg
	(payeeid,
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	serviced_custid,
	service_type,
	invoice_date,
	division_code,
	cust_loc,
	periodseq,
	processingunitseq,
	businessunitmap,
	periodstartdate,
	comm_value)
select 
	payeeid,
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	serviced_custid,
	service_type,
	invoice_date,
	division_code,
	cust_loc,
	periodseq,
	processingunitseq,
	businessunitmap,
	periodstartdate,
	sum(comm_value)
from 
	ext.ctas_sh_comm_custidlvl	
where periodseq = :in_periodseq
group by 
	payeeid,
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	serviced_custid,
	service_type,
	invoice_date,
	division_code,
	cust_loc,
	periodseq,
	processingunitseq,
	businessunitmap,
	periodstartdate;

update 
	ext.ctas_sh_comm_custidlvl_agg
set 
	cap_value = :v_fv_ssr_fst_cap, 
	bal_value = :v_fv_ssr_fst_cap
where 
	title_name in ('SSR','SSRIT - Bench','SSRIT - Assigned','FST','FST In Training')
	and periodseq = :in_periodseq;

commit; 

update ext.ctas_sh_comm_custidlvl_agg
   set pay_value = least (bal_value, comm_value)
where 
	periodseq = :in_periodseq;
	
commit;	

-- for loop to handle multiple use cases
	
for i as get_pay_update
do
		call ext.ctas_event_log(':v_proc_name',i.invoice_date,0);
		
		--handling invoices falling on samedate
		
		update 
			ext.ctas_sh_comm_custidlvl_agg 
		set division_flg = (select min(division_code) from ext.ctas_sh_comm_custidlvl_agg	
							where periodseq = :in_periodseq
							and invoice_date = i.invoice_date
							and payeeseq = i.payeeseq
							and serviced_custid = i.serviced_custid)
		where periodseq = :in_periodseq
			and invoice_date = i.invoice_date
			and payeeseq = i.payeeseq
			and serviced_custid = i.serviced_custid;	
		
		commit;
		
		merge into ext.ctas_sh_comm_custidlvl_agg tgt
		using (select 
				payeeseq,
				serviced_custid,
				service_type,
				--division_code,
				division_flg,
				sum (pay_value) pay_value
		     from
		    	ext.ctas_sh_comm_custidlvl_agg
		     where 
				title_name in ('SSR','SSRIT - Bench','SSRIT - Assigned','FST','FST In Training')
				and periodstartdate < :v_periodenddate
				and days_between(i.invoice_date,invoice_date) = 0
				and division_code < i.division_code
				and i.cust_loc = cust_loc
		 	group by
				payeeseq,
				serviced_custid,
				service_type,
				--division_code
				division_flg) src
		on (tgt.payeeseq = src.payeeseq
			and tgt.serviced_custid = src.serviced_custid
			and tgt.service_type = src.service_type
			and tgt.periodseq = :in_periodseq
			and i.payeeseq = tgt.payeeseq
			and i.serviced_custid = tgt.serviced_custid
			and i.service_type = tgt.service_type
			and i.invoice_date = tgt.invoice_date
			and i.division_code  = tgt.division_code
			and i.cust_loc = tgt.cust_loc)
		when matched	
		then
		   update set tgt.bal_value = tgt.cap_value - ifnull(src.pay_value,0),
		   tgt.pay_value = least(tgt.cap_value - ifnull(src.pay_value,0),tgt.comm_value);
		
		commit;	
		
		
		call ext.ctas_event_log(:v_proc_name,'PAY_VALUE Field Update for invoices with same date in ctas_sh_comm_custidlvl : Number of rows Updated ',::ROWCOUNT);
		
		--handling invoices falling in 30 days window
		
		merge into ext.ctas_sh_comm_custidlvl_agg tgt
		using (select 
				payeeseq,
				serviced_custid,
				service_type,
				sum (pay_value) pay_value
		     from
		    	ext.ctas_sh_comm_custidlvl_agg
		     where 
				title_name in ('SSR','SSRIT - Bench','SSRIT - Assigned','FST','FST In Training')
				and periodstartdate < :v_periodenddate
				and abs(days_between(i.invoice_date,invoice_date)) <= 30
				and invoice_date < i.invoice_date
				and division_code = i.division_code
				and i.cust_loc = cust_loc
		 	group by
				payeeseq,
				serviced_custid,
				service_type) src
		on (tgt.payeeseq = src.payeeseq
			and tgt.serviced_custid = src.serviced_custid
			and tgt.service_type = src.service_type
			and tgt.periodseq = :in_periodseq
			and i.payeeseq = tgt.payeeseq
			and i.serviced_custid = tgt.serviced_custid
			and i.service_type = tgt.service_type
			and i.invoice_date = tgt.invoice_date
			and i.division_code  = tgt.division_code)
		when matched
		then
		   update set tgt.bal_value = tgt.cap_value - ifnull(src.pay_value,0),
		   tgt.pay_value = least(tgt.cap_value - ifnull(src.pay_value,0),tgt.comm_value);
		
		commit;

end for;	

call ext.ctas_event_log(:v_proc_name,'PAY_VALUE Field Update for invoices in 30 day window in ctas_sh_comm_custidlvl : Number of rows Updated ',::ROWCOUNT);

truncate table ext.ctas_sh_comm_custidlvl_stg;

insert into ext.ctas_sh_comm_custidlvl_stg
select 
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	serviced_custid,
	periodseq,
	processingunitseq,
	businessunitmap,
	sum(pay_value)
from
	ext.ctas_sh_comm_custidlvl_agg
where
	periodseq = :in_periodseq
group by
	payeeseq,
	positionseq,
	title_name,
	titleseq,
	serviced_custid,
	periodseq,
	processingunitseq,
	businessunitmap;

commit;

--deriving the adjustments for ssrfst desposits

insert into ext.ctas_deposit_stg 
with dep
as
(select 
	payeeseq,
	positionseq,
	periodseq,
	sum(value) as preadjustedvalue,     
	unittypeforpreadjustedvalue,
	sum(value) as value,
	unittypeforvalue,
	earninggroupid,                              
	earningcodeid
from 
	cs_deposit deposit
where 
	deposit.name in 
		('DO_Commission_Total',
		'DO_National_Commission_Total',
		'DO_National_Commission_Total_Cap',
		'DO_Commission_Total_Cap')
	and deposit.earninggroupid = 'Earnings'
	and deposit.earningcodeid in ('Fire National Accounts','Fire Service Pay')
group by
	deposit.payeeseq,
	deposit.positionseq,
	deposit.periodseq,
	deposit.unittypeforpreadjustedvalue,
	deposit.unittypeforvalue,
	deposit.earninggroupid,
	deposit.earningcodeid	
) 
select :v_tenantid,
	v_depositseq,
	case 
		 when 
			dep.earningcodeid = 'Fire National Accounts' 
		 then 
			'DO_National_Commission_Total_Cap' 
		 when 
			dep.earningcodeid = 'Fire Service Pay' 
		 then
			'DO_Commission_Total_Cap' 
	end as deposit_name,
	stg.payeeseq,
	stg.positionseq,
	stg.periodseq,
	(select pipelinerunseq
	   from cs_pipelinerun
	  where status = 'Running')
	   as pipelinerunseq,
	'manual' as origintypeid,
	(select starttime
	   from cs_pipelinerun
	  where status = 'Running')
	   as pipelinerundate,
	stg.businessunitmap as businessunitmap,
	sum (stg.pay_value) - dep.value as preadjustedvalue, 
	dep.unittypeforpreadjustedvalue,
	sum (stg.pay_value) - dep.value as value, 
	dep.unittypeforvalue,
	dep.earninggroupid,                              
	dep.earningcodeid,                               
	0 as isheld,
	null as comments,
	stg.processingunitseq
from 
	ext.ctas_sh_comm_custidlvl_stg stg, dep
where     
	dep.periodseq = stg.periodseq
	and dep.payeeseq = stg.payeeseq
	and dep.positionseq = stg.positionseq
	and dep.periodseq = :in_periodseq
group by 
	stg.title_name,
	dep.payeeseq,
	dep.positionseq,
	dep.periodseq,
	stg.payeeseq,
	stg.positionseq,
	stg.periodseq,
	stg.businessunitmap,
	dep.value,
	dep.unittypeforpreadjustedvalue,
	dep.unittypeforvalue,
	dep.earninggroupid,
	dep.earningcodeid,
	stg.processingunitseq;
	
commit;	

call ext.ctas_event_log(:v_proc_name,'ctas_deposit_stg from ctas_sh_comm_custidlvl_stg: Number of rows Inserted ',::ROWCOUNT);


update 
	ext.ctas_deposit_stg 
set 
	depositseq = v_depositseq + "$rowid$";

commit;

insert into tcmp.cs_deposit_vw
	(tenantid,
	 depositseq,
	 name,
	 payeeseq,
	 positionseq,
	 periodseq,
	 pipelinerunseq,
	 origintypeid,
	 pipelinerundate,
	 businessunitmap,
	 preadjustedvalue,
	 unittypeforpreadjustedvalue,
	 value,
	 unittypeforvalue,
	 earninggroupid,
	 earningcodeid,
	 isheld,
	 depositdate,
	 comments,
	 processingunitseq)
select tenantid,
	max(depositseq),
	name,
	payeeseq,
	positionseq,
	periodseq,
	pipelinerunseq,
	origintypeid,
	pipelinerundate,
	businessunitmap,
	sum(preadjustedvalue),
	unittypeforpreadjustedvalue,
	sum(value),
	unittypeforvalue,
	earninggroupid,
	earningcodeid,
	isheld,
	current_date,
	comments,
	processingunitseq
from 
	ext.ctas_deposit_stg
where value <> 0
group by 
	tenantid,
	name,
	payeeseq,
	positionseq,
	periodseq,
	pipelinerunseq,
	origintypeid,
	pipelinerundate,
	businessunitmap,
	unittypeforpreadjustedvalue,
	unittypeforvalue,
	earninggroupid,
	earningcodeid,
	isheld,
	comments,
	processingunitseq;

commit;

call ext.ctas_event_log(:v_proc_name,'cs_deposit : Number of rows Inserted ',::ROWCOUNT);


call ext.ctas_event_log(:v_proc_name,'Execution ends for period : '||:in_periodseq,0);	

END