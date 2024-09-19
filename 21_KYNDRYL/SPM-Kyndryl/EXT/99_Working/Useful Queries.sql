-- data extracts
select 
plr.pipelinerunseq, 
st.name stage, 
per.name period, 
plr.starttime, 
plr.stoptime, 
plr.status, 
REPLACE_REGEXPR( '(.*)(\[dataExtractsFileType\])(([a-zA-Z0-9]*))(\[.*)' in runparameters with '\3') as dataExtractsFileType,
plr.runparameters
from cs_plrun plr
join cs_stagesummary ss on plr.pipelinerunseq = ss.pipelinerunseq
join cs_stagetype st on ss.stagetypeseq = st.stagetypeseq
join cs_period per on plr.periodseq = per.periodseq and per.removedate = '2200-01-01'
where st.name = 'DataExtracts'
order by plr.pipelinerunseq desc;

-- check the debug entries for anything long running
select * from (
select datetime, seconds_between(lag(datetime) over (order by datetime), datetime) as secs, text, value 
from ext.kyn_debug
)
where 1=1
--and datetime >= add_days(current_timestamp,-1)
and text != '[KYN_LIB_TQ2COM] Start'
and secs > 5
order by 1;

-- Quota query
select 
ts.run_key, 
ts.territoryprogram_name as tp_name, 
ts.territoryprogram_period as tp_year, 
ts.semiannual_name as tp_h,
tq.territory_name, 
tq.element,
tq.position, tq.plan, tq.title_gn3,
--
cast(tq.finalquotavalue as decimal(25,2))     as h_orig,
cast(tq.lt_min_quota as decimal(25,2))        as h_country_min,
cast(tq.subordinate_count as decimal(25,2))   as h_sub_count,
cast(tq.lt_min_quota_override as decimal(25,2)) as h_min_override,
cast(tq.min_quota as decimal(25,2))           as h_min,
cast(tq.revenue_percent as decimal(25,2))     as h_rev_perc, 
cast(tq.final_quota as decimal(25,2))         as h_final,
---
cast(tq.year_finalquotavalue as decimal(25,2))    as y_orig,
cast(tq.year_lt_min_quota as decimal(25,2))       as y_country_min,
cast(tq.year_subordinate_count as decimal(25,2))  as y_sub_count,
cast(tq.year_lt_min_quota_override as decimal(25,2)) as y_min_override,
cast(tq.year_min_quota as decimal(25,2))          as y_min,
cast(tq.year_revenue_percent as decimal(25,2))    as y_rev_perc,
cast(tq.year_final_quota as decimal(25,2))        as y_final
from ext.kyn_tq2com_tq_quota tq
join ext.kyn_tq2com_sync ts on tq.run_key = ts.run_key
where tq.position like '5084057%'
and tq.run_key = (
  select max(tq2.run_key) 
  from ext.kyn_tq2com_tq_quota tq2 
  where tq.semiannual_periodseq = tq2.semiannual_periodseq
  and tq.positionseq = tq2.positionseq
)
order by ts.territoryprogram_period, tq.element, ts.semiannual_name;

-- IPL calls these functions to get the values from prestage quota table
select  pos.name as position, ttl.name as title, ttl.genericnumber3 as title_gn3,
per_sa.name as period, pt.name as periodtype, per_m.name as month,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Signings_Minimum',  pt.name) as sign_min,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Signings_Original', pt.name) as sign_orig,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Signings_Final',    pt.name) as sign_final,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Revenue_Minimum',   pt.name) as rev_min,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Revenue_Original',  pt.name) as rev_orig,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Revenue_Final',     pt.name) as rev_final,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Profit_Minimum',    pt.name) as profit_min,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Profit_Original',   pt.name) as profit_orig,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Profit_Revenue%',   pt.name) as profit_rev_perc,
ext.kyn_fnc_tq2com_ipl_quota(pos.ruleelementownerseq,  per_m.periodseq, 'Q_Profit_Final',      pt.name) as profit_final
from cs_period per_sa
join cs_periodtype pt on per_sa.periodtypeseq = pt.periodtypeseq and pt.removedate= '2200-01-01'
join cs_calendar cal on
  per_sa.calendarseq = cal.calendarseq
  and cal.removedate= '2200-01-01'
join cs_period per_m on 
  per_sa.enddate = per_m.enddate 
  and per_m.removedate = '2200-01-01' 
  and per_m.calendarseq = cal.calendarseq
  and per_m.periodtypeseq = cal.minorperiodtypeseq
join cs_position pos on
  pos.removedate= '2200-01-01'
  and pos.effectivestartdate < per_m.enddate
  and pos.effectiveenddate >= per_m.enddate
join cs_title ttl on 
ttl.removedate = '2200-01-01'
and pos.titleseq = ttl.ruleelementownerseq
  and ttl.effectivestartdate < per_m.enddate
  and ttl.effectiveenddate >= per_m.enddate
where per_sa.removedate=  '2200-01-01'
and per_sa.name in ('HY1 2024', 'HY2 2024', 'Fiscal Year 2024')
--and pos.name like '5084057%'
and ((pt.name = 'year' and ttl.genericnumber3 = 12) or (pt.name = 'semiannual' and ttl.genericnumber3 = 6))
and exists (select 1 from cs_planassignable pas where ttl.ruleelementownerseq = pas.ruleelementownerseq and pas.removedate = '2200-01-01' and pas.planseq is not null)
and exists (select 1 from ext.kyn_tq2com_tq_quota tq where pos.ruleelementownerseq = tq.positionseq)
order by 1, 2, 3;

-- add dummy entries instead of generating documents
insert into ext.kyn_tq2com_ipl_trace
select 
row_number() over (order by q.positionname, yr.name) as documentprocessseq, 
current_timestamp as generatedate,
'Dummy '||to_char(current_timestamp, 'YYYYMMDD_HH24MISS') as name,
yr.name as batchname,
yr.startdate,
yr.enddate,
q.positionseq,
q.positionname as position,
'status_Accepted' as status,
current_timestamp as acceptdate,
q.semiannual_periodseq,
q.semiannual_name, 
max(q.run_key) as run_key,
null as year_run_key,
0 as process_flag,
null as message
from ext.kyn_tq2com_prestage_quota q
join cs_period per on q.semiannual_periodseq  = per.periodseq and per.removedate = '2200-01-01'
join cs_period yr on per.parentseq = yr.periodseq and yr.removedate = '2200-01-01'
group by q.positionseq, q.positionname, yr.name, q.semiannual_periodseq, q.semiannual_name, yr.startdate, yr.enddate;


-- get positions and sales role for IAS
select
bu.name as bu,
pay.payeeid,
par.firstname,
par.lastname,
par.userid, 
par.participantemail as email,
ttl.name as sales_role,
pos.name as position,
cast(pos.effectivestartdate as date) as position_esd, 
cast(pos.effectiveenddate as date) as position_eed 
from cs_position pos
join cs_ruleelementowner reo on
  pos.ruleelementownerseq = reo.ruleelementownerseq
  and reo.removedate = '2200-01-01'
  and reo.effectivestartdate = pos.effectivestartdate
left outer join cs_businessunit bu on
  reo.businessunitmap = bu.mask
join cs_title ttl on
pos.titleseq = ttl.ruleelementownerseq 
and ttl.removedate = '2200-01-01' 
and ttl.effectivestartdate <= current_timestamp 
and ttl.effectiveenddate > current_timestamp
join cs_participant par on
par.payeeseq = pos.payeeseq
and par.removedate = '2200-01-01' 
and par.effectivestartdate <= current_timestamp 
and par.effectiveenddate > current_timestamp
join cs_payee pay on
par.payeeseq = pay.payeeseq
and pay.removedate = '2200-01-01' 
and pay.effectivestartdate = par.effectivestartdate
where pos.removedate = '2200-01-01' 
and pos.effectivestartdate <= current_timestamp 
and pos.effectiveenddate > current_timestamp
order by bu.name, par.userid, pos.name, ttl.name;


-- sellers for import into IAS
select distinct
par.userid as "userName", 
par.participantemail as "mail",
par.firstname as "firstName",
par.lastname as "lastName",
'active' as "status",
'"APP_SCAN,AUTHENTICATED_COMM-SCAN"' as "groups",
'21000401120000Z' as "validTo",
'20230401120000Z' as "validFrom"
from cs_position pos
join cs_ruleelementowner reo on
  pos.ruleelementownerseq = reo.ruleelementownerseq
  and reo.removedate = '2200-01-01'
  and reo.effectivestartdate = pos.effectivestartdate
left outer join cs_businessunit bu on
  reo.businessunitmap = bu.mask
join cs_title ttl on
pos.titleseq = ttl.ruleelementownerseq 
and ttl.removedate = '2200-01-01' 
and ttl.effectivestartdate <= current_timestamp 
and ttl.effectiveenddate > current_timestamp
join cs_participant par on
par.payeeseq = pos.payeeseq
and par.removedate = '2200-01-01' 
and par.effectivestartdate <= current_timestamp 
and par.effectiveenddate > current_timestamp
join cs_payee pay on
par.payeeseq = pay.payeeseq
and pay.removedate = '2200-01-01' 
and pay.effectivestartdate = par.effectivestartdate
where pos.removedate = '2200-01-01' 
and pos.effectivestartdate <= current_timestamp 
and pos.effectiveenddate > current_timestamp
and par.userid  is not null
and par.participantemail is not null
order by par.userid;