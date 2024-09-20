DROP FUNCTION EXT.TRYG_FNC_GET_INSURANCE_GROUP;
CREATE function EXT.TRYG_FNC_GET_INSURANCE_GROUP(
  in i_policyType varchar(255) default null,
  in i_title varchar(255) default null,
  in i_date timestamp default null
) returns o_grpname varchar(255),o_fgr varchar(255) as
begin
  declare v_eot timestamp := '2200-01-01';
  declare v_date timestamp = ifnull(i_date, current_timestamp);
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  begin
    o_grpname := 0;
  END;
  select stringvalue, 
  CASE WHEN stringvalue='Auto' then 1 
  WHEN stringvalue='Transport' then 2 
  WHEN stringvalue='Firmapension' then 3 
  WHEN stringvalue='Erhvervsskade' then 4 
  WHEN stringvalue='E-rejse, sundhed og gruppeliv' then 5
  WHEN stringvalue='Privatskade' then 6
  WHEN stringvalue='Landbrugskade' then 7
  WHEN stringvalue='Individuel liv og pension' then 8
  WHEN stringvalue='Kollektiv ulykke' then 9
  ELSE NULL
  END AS fgr
  into o_grpname, o_fgr from (
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
    join cs_mdltcell cell on 
      mdlt.ruleelementseq = cell.mdltseq 
      and ind0.ordinal = cell.dim0index
      and ind1.ordinal = cell.dim1index
      and cell.effectivestartdate <= current_timestamp
      and cell.effectiveenddate > current_timestamp
      and cell.removedate = '2200-01-01'
    where mdlt.removedate= '2200-01-01' 
      and mdlt.name = 'LT_Policy_Type_Insurance_Grp_Mapping'
      and mdlt.effectivestartdate <= current_timestamp
      and mdlt.effectiveenddate > current_timestamp )
      where Policy_Type=:i_policyType
      and Title=:i_title;
      

end
