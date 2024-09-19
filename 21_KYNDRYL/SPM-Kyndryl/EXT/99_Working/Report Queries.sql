select pos.name as position, ttl.name as title, ttl.genericnumber3 as plan_length,
trc.positionseq, trc.position, trc.documentprocessseq, trc.generatedate, trc.status, 
trc.startdate, trc.enddate, trc.acceptdate,
trc.run_key as semiannual_run_key, trc.year_run_key,
pq.quotaname, pq.value
from ext.kyn_tq2com_ipl_trace trc
join cs_period per on trc.semiannual_periodseq = per.periodseq and per.removedate = '2200-01-01' and per.startdate < trc.enddate and per.enddate >= trc.enddate
join cs_position pos on trc.positionseq = pos.ruleelementownerseq and pos.removedate ='2200-01-01'
and pos.effectivestartdate < trc.enddate
and pos.effectiveenddate >= trc.enddate
join cs_title ttl on pos.titleseq = ttl.ruleelementownerseq
and ttl.removedate = '2200-01-01'
and ttl.effectivestartdate < trc.enddate
and ttl.effectiveenddate >= trc.enddate
join ext.kyn_tq2com_prestage_quota pq on 
(case when ttl.genericnumber3 = 12 then trc.year_run_key else trc.run_key end) = pq.run_key
and (case when ttl.genericnumber3 = 12 then 'year' else 'semiannual' end) = pq.periodtypename
and pq.quotaname like '%Final'
where 1=1
--and trc.positionseq= 4785074604087607
and trc.position like '5044734%'
order by trc.generatedate desc, pq.quotaname
;