--liquibase formatted sql

--changeset jcadby:KYN_V_TQ2COM_WF_Position splitStatements:false stripComments:false
--comment: Create view

create or replace view EXT.KYN_V_TQ2COM_WF_Position as
select
tp.territoryprogramseq,
tp.name as territoryprogram_name,
t.territoryseq,
t.name as territory_name,
tpos.positionseq,
pos.name as position_name,
pos.titleseq,
ttl.name as title_name,
pay.payeeseq,
pay.payeeid,
ltrim(ifnull(par.firstname, ' ') || ' ') || par.lastname as payee_name,
pos.managerseq as manager_positionseq,
mgr.name as manager_position_name,
mgr_pay.payeeseq as manager_payeeseq,
mgr_pay.payeeid as manager_payeeid,
ltrim(mgr_par.firstname || ' ') || mgr_par.lastname as manager_payee_name,
tpos.split
from 
csq_territory t
join csq_territoryprogram tp on 
  t.territoryprogramseq = tp.territoryprogramseq 
  and tp.removedate = '2200-01-01'
join csq_territoryposition tpos on 
  t.territoryseq = tpos.territoryseq 
  and tpos.removedate = '2200-01-01'
join cs_position pos on 
  tpos.positionseq = pos.ruleelementownerseq 
  and pos.removedate = '2200-01-01'
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate >=  tpos.effectiveenddate
join cs_title ttl on 
  pos.titleseq = ttl.ruleelementownerseq 
  and ttl.removedate = '2200-01-01'
  and ttl.effectivestartdate < tpos.effectiveenddate
  and ttl.effectiveenddate >=  tpos.effectiveenddate
join cs_payee pay on 
  pos.payeeseq = pay.payeeseq 
  and pay.removedate = '2200-01-01'
  and pay.effectivestartdate < tpos.effectiveenddate
  and pay.effectiveenddate >=  tpos.effectiveenddate
join cs_participant par on 
  pay.payeeseq = par.payeeseq
  and par.removedate = '2200-01-01'
  and par.effectivestartdate = pay.effectivestartdate
left outer join cs_position mgr on 
  pos.managerseq = mgr.ruleelementownerseq 
  and mgr.removedate = '2200-01-01'
  and mgr.effectivestartdate < tpos.effectiveenddate
  and mgr.effectiveenddate >=  tpos.effectiveenddate
left outer join cs_payee mgr_pay on 
  mgr.payeeseq = mgr_pay.payeeseq 
  and mgr_pay.removedate = '2200-01-01'
  and mgr_pay.effectivestartdate < tpos.effectiveenddate
  and mgr_pay.effectiveenddate >=  tpos.effectiveenddate
left outer join cs_participant mgr_par on 
  mgr_pay.payeeseq = mgr_par.payeeseq
  and mgr_par.removedate = '2200-01-01'
  and mgr_par.effectivestartdate = mgr_pay.effectivestartdate
where t.removedate = '2200-01-01'
  and tpos.split > 0;
