select
case when dim0.name = 'Position Name' then ind0.minstring else ind1.minstring end as position_name,
case when dim0.name = 'Element' then ind0.minstring else ind1.minstring end as element,
cell.effectivestartdate, cell.effectiveenddate,
cell.value
from cs_relationalmdlt mdlt
join cs_mdltdimension dim0 on 
  mdlt.ruleelementseq = dim0.ruleelementseq 
  and dim0.removedate = '2200-01-01' 
  and dim0.dimensionslot = 0
join cs_mdltindex ind0 on
  ind0.ruleelementseq = dim0.ruleelementseq 
  and ind0.removedate = '2200-01-01' 
  and ind0.dimensionseq = dim0.dimensionseq
join cs_mdltdimension dim1 on 
  mdlt.ruleelementseq = dim1.ruleelementseq 
  and dim1.removedate = '2200-01-01' 
  and dim1.dimensionslot = 1
join cs_mdltindex ind1 on
  ind1.ruleelementseq = dim1.ruleelementseq 
  and ind1.removedate = '2200-01-01' 
  and ind1.dimensionseq = dim1.dimensionseq
join cs_mdltcell cell on
  cell.mdltseq = mdlt.ruleelementseq
  and cell.removedate = '2200-01-01'
  and cell.dim0index = ind0.ordinal
  and cell.dim1index = ind1.ordinal
where mdlt.name = 'LT_Minimum_Quota_Override'
and mdlt.removedate = '2200-01-01';

do begin
declare cursor c_runs for select distinct run_key from ext.kyn_tq2com_tq_quota order by run_key;
for x as c_runs
do
  ext.kyn_lib_tq2com:override_min_quota(:x.run_key);
  commit;
end for;
end;


select position, count(*) from ext.kyn_tq2com_tq_quota group by position order by 1 desc;

select distinct semiannual_periodseq from ext.kyn_tq2com_tq_quota where position = '5009345_GermanSchmidt_01';

select * from ext.kyn_tq2com_tq_quota where position = '5009345_GermanSchmidt_01';

select * from cs_period where periodseq in (2533274790396386, 2533274790396385) and removedate = '2200-01-01';