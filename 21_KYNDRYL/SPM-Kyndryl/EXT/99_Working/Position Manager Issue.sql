select 
pay.payeeid, 
cast(pay.effectivestartdate as date) as par_esd,
cast(pay.effectiveenddate as date) as par_eed,
par.genericattribute9 as line_mgr, 
par.genericattribute10 as second_mgr, 
par.genericattribute11 as func_mgr,
par.genericboolean1 as eligible,
pos.name as pos_name,
cast(pos.effectivestartdate as date) as pos_esd,
cast(pos.effectiveenddate as date) as pos_eed,
mgr.name as mgr_name 
from cs_participant par
join cs_payee pay on par.payeeseq = pay.payeeseq and pay.effectivestartdate = par.effectivestartdate and pay.removedate = '2200-01-01'
join cs_position pos on 
pos.payeeseq = par.payeeseq
and pos.removedate = '2200-01-01'
and pos.effectivestartdate < par.effectiveenddate
and pos.effectiveenddate > par.effectivestartdate
left outer join cs_position mgr on
pos.managerseq = mgr.ruleelementownerseq
and mgr.removedate = '2200-01-01'
and mgr.islast = 1
where par.removedate = '2200-01-01'
and pay.payeeid = '5000836'
--and mgr.name not like par.genericattribute11||'%'
order by pay.payeeid, pay.effectivestartdate, pos.effectivestartdate;


select pay.payeeid, pos.payeeseq, count(distinct pos.ruleelementownerseq), count(distinct pos.min_esd), count(distinct pos.max_eed)
from (
select payeeseq, ruleelementownerseq, min(effectivestartdate) as min_esd, max(effectiveenddate) as max_eed
from cs_position 
where removedate = '2200-01-01'
group by payeeseq, ruleelementownerseq
) pos
join cs_payee pay on pos.payeeseq = pay.payeeseq and pay.removedate = '2200-01-01' and pay.islast = 1
group by pos.payeeseq, pay.payeeid
having count(distinct pos.ruleelementownerseq) > 1
and (count(distinct pos.min_esd) > 1 or count(distinct pos.max_eed) > 1)
order by 3 desc;

select pos.name, min(pos.effectivestartdate) as min_esd, max(pos.effectiveenddate) as max_eed 
from cs_position pos
where pos.removedate= '2200-01-01'
and exists (select 1 from cs_position pos2 where pos2.managerseq = pos.ruleelementownerseq and pos2.removedate= '2200-01-01')
and pos.name like '%$_01' escape '$'
group by pos.name
having max(pos.effectiveenddate) != '2200-01-01'
order by 1;