CREATE OR REPLACE PROCEDURE EXT.TRYG_SP_NOTES_EXTRACT(OUT FILENAME varchar(120), IN pPlRunSeq BIGINT)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
AS
BEGIN


DECLARE DBMTK_TRUE TINYINT := 1; DECLARE DBMTK_FALSE TINYINT := 0; /* boolean constants */
DECLARE vSeq BIGINT;
declare vPeriodSeq bigint;
declare vPUSeq bigint;
declare vStartDate timestamp;
declare vEndDate timestamp;
declare vYear varchar(4);
DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
DECLARE v_procName varchar(100);
DECLARE v_removeDate date;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
		CALL EXT.TRYG_LOG(v_procName,'ERROR = '||IFNULL( ::SQL_ERROR_MESSAGE,'') ,NULL);
      resignal;
    END;
--------------------------------------------------------------------------------------------

v_procName='EXT.TRYG_SP_NOTES_EXTRACT';
v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');
---------------------------------------------------------------------------------------------


select periodSeq into vPeriodSeq from CS_PlRun where pipelineRunSeq = :pPlRunSeq;
select * into v_periodRow from CS_Period where periodseq = :vPeriodSeq and removedate=:v_removeDate;
select processingUnitSeq into vPUSeq from CS_PlRun where pipelineRunSeq = :pPlRunSeq;

CALL EXT.TRYG_LOG(v_procName,'Begin '||:v_procName || ' for period: '|| :v_periodRow.name,NULL);

select SUBSTR_AFTER(per.name, ' ' ) into vYear from cs_period per
join cs_periodtype pt on
pt.periodtypeseq=per.periodtypeseq
where per.periodseq=:vPeriodSeq 
and pt.name='month'
and per.removedate=:v_removeDate
and per.removedate=:v_removeDate;

delete from ext.TRYG_NOTES_EXTRACT_INCENTIVE;
delete from ext.TRYG_NOTES_EXTRACT_MEASUREMENT;
/* Load Redemption Grade incentives into incentive prestage*/
insert into  ext.TRYG_NOTES_EXTRACT_INCENTIVE
(participantName,
	payeeid ,
	positionname ,
	title ,
	period,
	Yearly_redemption,
	realized_redemption,
	Redemption_target_ytd,
	Realized_Redemption_ytd
	-- redemption_target_pension_agents
	) 
(select distinct par.firstname||' '||par.lastname, 
pay.payeeid, 
pos.name,
ti.name, 
per.name,
inc.genericnumber2 as Yearly_redemption,
inc.genericnumber4 as realized_redemption,
case when ti.name in ('PKL','AKF-PKL','PKM','AKF-PKM','CRP','AKF-CRP') then to_decimal(REPLACE(SUBSTR_BEFORE(inc.genericattribute2,' ') ,',', '')) 
     when ti.name in ('EKC','AKF-EKC','PROD','AKF-PROD','EA','AKF-EA') then inc.genericnumber6
     end as Redemption_target_ytd, 
to_decimal(REPLACE(SUBSTR_BEFORE(inc.genericattribute5,' ') ,',', '')) as Realized_Redemption_ytd

from 
cs_incentive inc
inner join cs_period per
on inc.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=inc.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=inc.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=inc.payeeseq
where per.removedate=:v_removeDate
and per.name like  '%'||:vYear||'%'
-- and per.periodseq=:vPeriodSeq
and pt.name='month'
and inc.genericattribute1='Redemption Grade' 
and ti.name not like 'FD%'
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1);

commit;

/*Load 'New Customers Grade' Incentive*/
merge into ext.TRYG_NOTES_EXTRACT_INCENTIVE p using
(select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
-- sum(inc.genericnumber4) as new_customers_target,sum(inc.genericnumber5) as realized_customers
inc.genericnumber4 as new_customers_target,
inc.genericnumber5 as realized_customers

from 
cs_incentive inc
inner join cs_period per
on inc.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=inc.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=inc.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=inc.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||:vYear||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and inc.genericattribute1='New Customers Grade' 
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
and ti.name not like 'FD%'
-- group by par.firstname||' '||par.lastname, pay.payeeid, pos.name, ti.name,  per.name
) sub on
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
and sub.period_name=p.period
when matched then 
update set new_customers_target = sub.new_customers_target,realized_customers = sub.realized_customers;

/*
merge into ext.TRYG_NOTES_EXTRACT_INCENTIVE p using
(select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
-- sum(inc.genericnumber4) as new_customers_target,sum(inc.genericnumber5) as realized_customers
inc.genericnumber4 as new_customers_target,
inc.genericnumber5 as realized_customers
from 
cs_incentive inc
inner join cs_period per
on inc.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=inc.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=inc.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=inc.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||:vYear||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and inc.genericattribute1='New Customers Grade' 
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
-- group by par.firstname||' '||par.lastname, pay.payeeid, pos.name, ti.name,  per.name
) sub on
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
and sub.period_name=p.period
when NOT MATCHED then 
insert 
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    new_customers_target,
    realized_customers
) 
values
(sub.participant_name,
sub.payeeid,
sub.position_name,
sub.title_name,
sub.period_name,
sub.new_customers_target,
sub.realized_customers
);
*/
insert into ext.TRYG_NOTES_EXTRACT_INCENTIVE (participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    new_customers_target,
    realized_customers)
(select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
-- sum(inc.genericnumber4) as new_customers_target,sum(inc.genericnumber5) as realized_customers
inc.genericnumber4 as new_customers_target,
inc.genericnumber5 as realized_customers
from 
cs_incentive inc
inner join cs_period per
on inc.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=inc.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=inc.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=inc.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||:vYear||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and inc.genericattribute1='New Customers Grade'
and ti.name not like 'FD%'
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
and not exists (select * from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
where par.firstname||' '||par.lastname = ip.participantName 
and pay.payeeid=ip.payeeid
and pos.name=ip.positionname
and ti.name=ip.title
and per.name=ip.period ));

commit;

/*Load 'New Customers Grade' Incentive*/

/*Load IO_TBVS13_Service_Visits_Grade incentive*/
merge into ext.TRYG_NOTES_EXTRACT_INCENTIVE p using
(select par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
-- sum(inc.genericnumber2) as realized_service ,sum(inc.genericnumber4) as service_target
inc.genericnumber2 as realized_service ,
inc.genericnumber4 as service_target
from 
cs_incentive inc
inner join cs_period per
on inc.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=inc.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=inc.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=inc.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||:vYear||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and inc.name='IO_TBVS13_Service_Visits_Grade' 
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
-- group by par.firstname||' '||par.lastname, pay.payeeid, pos.name, ti.name,  per.name
) sub on
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
and sub.period_name=p.period
when matched then 
update set realized_service = sub.realized_service,service_target = sub.service_target;


/*merge into ext.TRYG_NOTES_EXTRACT_INCENTIVE p using
(select par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
-- sum(inc.genericnumber2) as realized_service ,sum(inc.genericnumber4) as service_target
inc.genericnumber2 as realized_service ,
inc.genericnumber4 as service_target
from 
cs_incentive inc
inner join cs_period per
on inc.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=inc.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=inc.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=inc.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||:vYear||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and inc.genericattribute1='New Customers Grade' 
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
-- group by par.firstname||' '||par.lastname, pay.payeeid, pos.name, ti.name,  per.name
) sub on
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
and sub.period_name=p.period
when NOT MATCHED then 
insert 
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    realized_service,
    service_target
) 
values
(sub.participant_name,
sub.payeeid,
sub.position_name,
sub.title_name,
sub.period_name,
sub.realized_service,
sub.service_target
);
*/

insert into ext.TRYG_NOTES_EXTRACT_INCENTIVE 
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    realized_service,
    service_target
)
(select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
-- sum(inc.genericnumber2) as realized_service ,sum(inc.genericnumber4) as service_target
inc.genericnumber2 as realized_service ,
inc.genericnumber4 as service_target
from 
cs_incentive inc
inner join cs_period per
on inc.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=inc.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=inc.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=inc.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||:vYear||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and inc.name='IO_TBVS13_Service_Visits_Grade'
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and ti.name not like 'FD%'
and pay.islast=1
and pos.islast=1
and par.islast=1
and not exists(select * from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
where par.firstname||' '||par.lastname = ip.participantName 
and pay.payeeid=ip.payeeid
and pos.name=ip.positionname
and ti.name=ip.title
and per.name=ip.period));


commit;

/*Load IO_TBVS13_Service_Visits_Grade incentive*/

/*Load SMO_TBVS10_Portfolio_Development_Target_Achievement measurements into measurement prestage*/
insert into  ext.TRYG_NOTES_EXTRACT_MEASUREMENT
(participantName,
	payeeid ,
	positionname ,
	title ,
	period,
	Portfolio_target,
	Realized_portfolio_development,
	Portfolio_primo
) 
select distinct par.firstname||' '||par.lastname, pay.payeeid, pos.name, ti.name, per.name,
ms.genericnumber4 as Portfolio_target,ms.genericnumber3 as Realized_portfolio_development,ms.genericnumber1 as Portfolio_primo
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('SMO_TBVS10_Portfolio_Development_Target_Achievement')
-- and ti.name not like 'FD%'
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1;

commit;
/*Load SMO_TBVS10_Portfolio_Development_Target_Achievement measurements into measurement prestage*/

/*update ext.TRYG_NOTES_EXTRACT_MEASUREMENT p set Profitability =(
select 
ms.genericnumber1
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('SMO_TBVS11_Profitability_+/-Benchmark')
and par.firstname||' '||par.lastname = p.participantName 
and pay.payeeid=p.payeeid
and pos.name=p.positionname
and ti.name=p.title
and per.name=p.period
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
);
*/

/*Load SMO_TBVS11_Profitability_+/-Benchmark into measurement prestage*/
merge into ext.TRYG_NOTES_EXTRACT_MEASUREMENT p using (
select 
distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
(ms.genericnumber1*100) as profitability
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('SMO_TBVS11_Profitability_+/-Benchmark')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1	
) sub on
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
and sub.period_name=p.period
when matched then update 
set p.Profitability = sub.profitability;
/*

merge into ext.TRYG_NOTES_EXTRACT_MEASUREMENT p using (
select 
par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
ms.genericnumber1 as profitability
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('SMO_TBVS11_Profitability_+/-Benchmark')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1	
) sub on
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
and sub.period_name=p.period
when not matched then
insert 
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    profitability
) 
values
(sub.participant_name,
sub.payeeid,
sub.position_name,
sub.title_name,
sub.period_name,
sub.profitability
);
*/

insert into ext.TRYG_NOTES_EXTRACT_MEASUREMENT
(  participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    profitability) 
(select distinct
par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
(ms.genericnumber1*100) as profitability
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('SMO_TBVS11_Profitability_+/-Benchmark')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
and not exists(select 1 from ext.TRYG_NOTES_EXTRACT_MEASUREMENT p
	where par.firstname||' '||par.lastname = p.participantName 
	and pay.payeeid=p.payeeid
	and pos.name=p.positionname
	and ti.name=p.title
	and per.name=p.period
));

commit;
/*Load SMO_TBVS11_Profitability_+/-Benchmark into measurement prestage*/

/*Load PMO_New_Contracts_YTD into measurement prestage*/
merge into ext.TRYG_NOTES_EXTRACT_MEASUREMENT p 
using(
select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
ms.value as New_Contracts_YTD
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||2023||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('PMO_New_Contracts_YTD')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1) sub 
on 
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
when matched then 
update set p.new_contracts = sub.New_Contracts_YTD;

/*
merge into ext.TRYG_NOTES_EXTRACT_MEASUREMENT p 
using(
select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
ms.value as New_Contracts_YTD
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||2023||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('PMO_New_Contracts_YTD')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1) sub 
on 
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
when NOT MATCHED then 
insert 
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    new_contracts
) 
values
(sub.participant_name,
sub.payeeid,
sub.position_name,
sub.title_name,
sub.period_name,
sub.New_Contracts_YTD
);
*/

insert into ext.TRYG_NOTES_EXTRACT_MEASUREMENT  
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    new_contracts
)
( select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
ms.value as New_Contracts_YTD
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||2023||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('PMO_New_Contracts_YTD')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
and not exists (select 1 from ext.TRYG_NOTES_EXTRACT_MEASUREMENT p
where par.firstname||' '||par.lastname = p.participantName 
and pay.payeeid=p.payeeid
and pos.name=p.positionname
and ti.name=p.title
));

/*Load PMO_New_Contracts_YTD into measurement prestage completed */

/*Load PMO_Own_Generated_Contracts_YTD into measurement prestage*/
merge into ext.TRYG_NOTES_EXTRACT_MEASUREMENT p 
using(
select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
ms.value as NEW_CONTRACTS_TARGET,
ms.value as REALIZED_CONTRACTS
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||2023||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('PMO_Own_Generated_Contracts_YTD')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1) sub 
on 
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
when matched then 
update set p.NEW_CONTRACTS_TARGET = sub.NEW_CONTRACTS_TARGET,
p.REALIZED_CONTRACTS=p.REALIZED_CONTRACTS;

/*
merge into ext.TRYG_NOTES_EXTRACT_MEASUREMENT p 
using(
select par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
ms.value as NEW_CONTRACTS_TARGET,
ms.value as REALIZED_CONTRACTS
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||2023||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('PMO_Own_Generated_Contracts_YTD')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1) sub 
on 
sub.participant_name = p.participantName 
and sub.payeeid=p.payeeid
and sub.position_name=p.positionname
and sub.title_name=p.title
when NOT MATCHED then 
insert 
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    NEW_CONTRACTS_TARGET,
    REALIZED_CONTRACTS
) 
values
(sub.participant_name,
sub.payeeid,
sub.position_name,
sub.title_name,
sub.period_name,
sub.NEW_CONTRACTS_TARGET,
sub.REALIZED_CONTRACTS
);
*/

insert into ext.TRYG_NOTES_EXTRACT_MEASUREMENT
(   participantName,
	payeeid ,
	positionname ,
	title ,
	period,
    NEW_CONTRACTS_TARGET,
    REALIZED_CONTRACTS
) 
(select distinct par.firstname||' '||par.lastname as participant_name, 
pay.payeeid,
pos.name as position_name, 
ti.name as title_name,
per.name as period_name,
ms.value as NEW_CONTRACTS_TARGET,
ms.value as REALIZED_CONTRACTS
from 
cs_measurement ms
inner join cs_period per
on ms.periodseq = per.periodseq
inner join cs_periodtype pt
on pt.periodtypeseq=per.periodtypeseq
inner join cs_position pos on
pos.ruleelementownerseq=ms.positionseq
inner join cs_title ti on
pos.titleseq=ti.ruleelementownerseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=ms.payeeseq
inner join cs_payee pay on
pay.payeeseq=par.payeeseq
and pay.payeeseq=ms.payeeseq
where per.removedate=:v_removeDate
-- and per.name like  '%'||2023||'%'
and per.periodseq=:vPeriodSeq
and pt.name='month'
and ms.name = ('PMO_Own_Generated_Contracts_YTD')
and pos.removedate=:v_removeDate
and par.removedate=:v_removeDate
and ti.removedate=:v_removeDate
and pay.removedate=:v_removeDate
and pay.islast=1
and pos.islast=1
and par.islast=1
and not exists (select 1 from ext.TRYG_NOTES_EXTRACT_MEASUREMENT p
where par.firstname||' '||par.lastname = p.participantName 
and pay.payeeid=p.payeeid
and pos.name=p.positionname
and ti.name=p.title ));

/*Load PMO_Own_Generated_Contracts_YTD into measurement prestage complete */



delete from ext.TRYG_NOTES_EXTRACT_MEASUREMENT /*delete duplicate rows if any*/
where "$rowid$" in
(SELECT LEAD("$rowid$") over (partition by  participantName,payeeid ,positionname ,title ,period
order by period) from ext.TRYG_NOTES_EXTRACT_MEASUREMENT);


delete from EXT.TRYG_NOTES_EXTRACT; --truncate and load
insert into EXT.TRYG_NOTES_EXTRACT(
Navn ,      
MA_NR,
S_NR,
PORTEFOLJEUDV_MAL,
PORTEFOLJEUDV_REAL,
PORTEFOLJE_PRIMO,
LONSOMHED_REAL,
NYE_KONTRAKT_REAL,
EGNE_KONTRAKT_MAL,
EGNE_KONTRAKT_REAL
-- SKADE_INDL_MAL,
-- SKADE_INDL_REAL,
-- NYEKUNDER_MAL,
-- NYEKUNDER_REAL,
-- SERVICE_MAL,
-- SERVICE_REAL
)
select
mp.PARTICIPANTNAME,
mp.PAYEEID,
mp.POSITIONNAME,
ifnull(mp.PORTFOLIO_TARGET,0),
ifnull(mp.REALIZED_PORTFOLIO_DEVELOPMENT,0),
ifnull(mp.PORTFOLIO_PRIMO,0),
ifnull(mp.PROFITABILITY,0),
ifnull(mp.NEW_CONTRACTS,0),
ifnull(mp.NEW_CONTRACTS_TARGET,0), 
ifnull(mp.REALIZED_CONTRACTS,0)
-- 0,
-- 0,
-- 0,
-- 0,
-- 0,
-- 0
from ext.TRYG_NOTES_EXTRACT_MEASUREMENT mp
where mp.period=:v_periodRow.name;

CALL EXT.TRYG_LOG(:v_procName,'Count of measurements loaded ',::ROWCOUNT);
COMMIT;	

update EXT.TRYG_NOTES_EXTRACT ne set (
SKADE_INDL_MAL,
SKADE_INDL_REAL,
NYEKUNDER_MAL,
NYEKUNDER_REAL,
SERVICE_MAL,
SERVICE_REAL)=(select
ifnull(mp.REDEMPTION_TARGET_YTD,0),
ifnull(mp.REALIZED_REDEMPTION_YTD,0),
ifnull(mp.NEW_CUSTOMERS_TARGET,0),
ifnull(mp.REALIZED_CUSTOMERS,0),
ifnull(mp.SERVICE_TARGET,0),
ifnull(mp.REALIZED_SERVICE,0)
from ext.TRYG_NOTES_EXTRACT_INCENTIVE mp
where mp.payeeid= ne.ma_nr
and mp.POSITIONNAME=ne.S_NR
and mp.period=:v_periodRow.name)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE mp
where mp.payeeid= ne.ma_nr
and mp.POSITIONNAME=ne.S_NR
and mp.period=:v_periodRow.name);

CALL EXT.TRYG_LOG(:v_procName,'Count of incentives with matching measurement positions in notes extract ',::ROWCOUNT);


insert into EXT.TRYG_NOTES_EXTRACT (
Navn ,      
MA_NR,
S_NR,
PORTEFOLJEUDV_MAL,
PORTEFOLJEUDV_REAL,
PORTEFOLJE_PRIMO,
LONSOMHED_REAL,
NYE_KONTRAKT_REAL,
EGNE_KONTRAKT_MAL,
EGNE_KONTRAKT_REAL,
SKADE_INDL_MAL,
SKADE_INDL_REAL,
NYEKUNDER_MAL,
NYEKUNDER_REAL,
SERVICE_MAL,
SERVICE_REAL) 

(select
mp.PARTICIPANTNAME,
mp.PAYEEID,
mp.POSITIONNAME,
0,
0,
0,
0,
0,
0,
0,
ifnull(mp.REDEMPTION_TARGET_YTD,0), 
ifnull(mp.REALIZED_REDEMPTION_YTD,0),
ifnull(mp.NEW_CUSTOMERS_TARGET,0),
ifnull(mp.REALIZED_CUSTOMERS,0),
ifnull(mp.SERVICE_TARGET,0),
ifnull(mp.REALIZED_SERVICE,0)
from ext.TRYG_NOTES_EXTRACT_INCENTIVE mp
where not exists (select * from EXT.TRYG_NOTES_EXTRACT ne
where mp.payeeid= ne.ma_nr
and mp.POSITIONNAME=ne.S_NR)
and mp.period=:v_periodRow.name);

CALL EXT.TRYG_LOG(:v_procName,'Count of incentives not matching with measurement positions in notes extract ',::ROWCOUNT);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JANUAR=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'January%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'January%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JANUAR=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'January%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'January%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_FEBRUAR=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'February%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
) where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'February%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_FEBRUAR=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'February%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'February%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_MARTS=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'March%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'March%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_MARTS=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'March%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'March%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_APRIL=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'April%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'April%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_APRIL=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'April%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'April%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_MAJ=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'May%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
) where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'May%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_MAJ=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'May%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'May%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JUNI=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'June%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'June%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JUNI=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'June%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'June%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JULI=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'July%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'July%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JULI=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'July%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'July%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_AUGUST=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'August%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'August%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_AUGUST=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'August%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'August%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_SEPTEMB=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'September%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'September%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_SEPTEMB=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'September%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'September%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_OKTOBER=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'October%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'October%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_OKTOBER=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'October%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'October%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_NOV=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'November%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
) where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'November%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);

update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_NOV=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'November%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'November%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_DEC=(select ifnull(yearly_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'December%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'December%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_DEC=(select ifnull(realized_redemption,0)
	from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'December%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
)
where exists (select 1 from ext.TRYG_NOTES_EXTRACT_INCENTIVE ip
	where ip.period like 'December%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr 
);
/*
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JANUAR=0 where INDL_LON_JANUAR is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_FEBRUAR=0 where INDL_LON_FEBRUAR is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_MARTS=0 where INDL_LON_MARTS is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_APRIL=0 where INDL_LON_APRIL is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_MAJ=0 where INDL_LON_MAJ is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JUNI=0 where INDL_LON_JUNI is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JULI=0 where INDL_LON_JULI is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_AUGUST=0 where INDL_LON_AUGUST is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_SEPTEMB=0 where INDL_LON_SEPTEMB is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_OKTOBER=0 where INDL_LON_OKTOBER is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_NOV=0 where INDL_LON_NOV is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_DEC=0 where INDL_LON_DEC is NULL;

update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JANUAR=0 where INDL_REAL_JANUAR is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_FEBRUAR=0 where INDL_REAL_FEBRUAR is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_MARTS=0 where INDL_REAL_MARTS is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_APRIL=0 where INDL_REAL_APRIL is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_MAJ=0 where INDL_REAL_MAJ is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JUNI=0 where INDL_REAL_JUNI is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JULI=0 where INDL_REAL_JULI is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_AUGUST=0 where INDL_REAL_AUGUST is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_SEPTEMB=0 where INDL_REAL_SEPTEMB is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_OKTOBER=0 where INDL_REAL_OKTOBER is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_NOV=0 where INDL_REAL_NOV is NULL;
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_DEC=0 where INDL_REAL_DEC is NULL;
*/
CALL EXT.TRYG_LOG(v_procName,'End '||:v_procName ,NULL);

commit;
FILENAME := 'NOTESEXTRACT.txt';
END