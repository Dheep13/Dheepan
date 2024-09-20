
select ind0.minstring as Policy_Type, ind1.minstring as Title,cell.stringvalue
    from cs_relationalmdlt mdlt
    join cs_mdltdimension dim0 on 
      mdlt.ruleelementseq = dim0.ruleelementseq 
      and dim0.dimensionslot = 0
      and dim0.effectivestartdate <= current_timestamp
      and dim0.effectiveenddate > current_timestamp
      and dim0.removedate = '2200-01-01' 
    join cs_mdltindex ind0 on 
      mdlt.ruleelementseq = ind0.ruleelementseq 
      and ind0.dimensionseq = dim0.dimensionseq
      and ind0.effectivestartdate <= current_timestamp
      and ind0.effectiveenddate > current_timestamp
      and ind0.removedate = '2200-01-01'
    join cs_mdltdimension dim1 on 
      mdlt.ruleelementseq = dim1.ruleelementseq
      and dim1.dimensionslot = 1
      and dim1.effectivestartdate <= current_timestamp
      and dim1.effectiveenddate > current_timestamp
      and dim1.removedate = '2200-01-01'
    join cs_mdltindex ind1 on 
      mdlt.ruleelementseq = ind1.ruleelementseq
      and ind1.effectivestartdate <= current_timestamp
      and ind1.effectiveenddate > current_timestamp
      and ind1.dimensionseq = dim1.dimensionseq
      and ind1.removedate = '2200-01-01' 
    -- join cs_mdltdimension dim2 on 
    --   mdlt.ruleelementseq = dim2.ruleelementseq 
    --   and dim2.dimensionslot = 2
    --   and dim2.effectivestartdate <= current_timestamp
    --   and dim2.effectiveenddate > current_timestamp
    --   and dim2.removedate = '2200-01-01'
    -- join cs_mdltindex ind2 on 
    --   mdlt.ruleelementseq = ind2.ruleelementseq 
    --   and ind2.dimensionseq = dim2.dimensionseq
    --   and ind2.effectivestartdate <= current_timestamp
    --   and ind2.effectiveenddate > current_timestamp
    --   and ind2.removedate = '2200-01-01' 
    join cs_mdltcell cell on 
      mdlt.ruleelementseq = cell.mdltseq 
      and ind0.ordinal = cell.dim0index
      and ind1.ordinal = cell.dim1index
      --and ind2.ordinal = cell.dim2index
      and cell.effectivestartdate <= current_timestamp
      and cell.effectiveenddate > current_timestamp
      and cell.removedate = '2200-01-01'
    where mdlt.removedate= '2200-01-01' 
      and mdlt.name = 'LT_Policy_Type_Insurance_Grp_Mapping'
      and mdlt.effectivestartdate <= current_timestamp
      and mdlt.effectiveenddate > current_timestamp   
      
;