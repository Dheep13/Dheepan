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