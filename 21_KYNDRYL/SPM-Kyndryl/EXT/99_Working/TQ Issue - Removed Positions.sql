select * from (
select tpos.territorypositionseq, tpos.territoryseq, ter.name as territory,
cast(tpos.effectivestartdate as date) as tpos_esd,
cast(tpos.effectiveenddate as date) as tpos_eed,
tpos.positionseq, pos.name as position,
cast(pos.effectivestartdate as date) as pos_esd, 
cast(pos.effectiveenddate as date) as pos_eed, 
pos.createdate as pos_createdate, 
pos.removedate as pos_removedate,
row_number() over (partition by tpos.territorypositionseq order by pos.removedate desc) as rn
from csq_territoryposition tpos
join csq_territory ter on tpos.territoryseq = ter.territoryseq and ter.removedate = '2200-01-01'
join cs_position pos on tpos.positionseq = pos.ruleelementownerseq
and pos.removedate > tpos.createdate
and tpos.effectivestartdate < pos.effectiveenddate
and tpos.effectiveenddate > pos.effectivestartdate
where tpos.removedate='2200-01-01'
) 
where rn = 1
and pos_removedate != '2200-01-01'
order by 1, 2, 3, 4, 5, 6, 7;