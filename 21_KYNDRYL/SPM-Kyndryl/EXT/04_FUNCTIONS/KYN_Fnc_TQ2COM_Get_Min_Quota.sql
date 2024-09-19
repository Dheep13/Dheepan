--liquibase formatted sql

--changeset jcadby:KYN_FNC_TQ2COM_Get_Min_Quota splitStatements:false stripComments:false
--comment: Create function
create or replace function EXT.KYN_FNC_TQ2COM_Get_Min_Quota(
  in i_element varchar(255) default null,
  in i_country varchar(255) default null,
  in i_period_cycle integer default null,
  in i_date timestamp default null
) returns o_minvalue integer as
begin
  declare v_eot timestamp := '2200-01-01';
  declare v_date timestamp = ifnull(i_date, current_timestamp);
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  begin
    o_minvalue := 0;
  END;
  select value into o_minvalue from (
    select 
    case 'Country'
    when dim0.name then ind0.minstring
    when dim1.name then ind1.minstring
    when dim2.name then ind2.minstring
    end as country,
    case 'Element'
    when dim0.name then ind0.minstring
    when dim1.name then ind1.minstring
    when dim2.name then ind2.minstring
    end as element,
    case 'Period Cycle'
    when dim0.name then ind0.minvalue
    when dim1.name then ind1.minvalue
    when dim2.name then ind2.minvalue
    end as period_cycle,
    cell.value
    from cs_relationalmdlt mdlt
    join cs_mdltdimension dim0 on 
      mdlt.ruleelementseq = dim0.ruleelementseq 
      and dim0.dimensionslot = 0
      and dim0.effectivestartdate <= :v_date
      and dim0.effectiveenddate > :v_date
      and dim0.removedate = :v_eot 
    join cs_mdltindex ind0 on 
      mdlt.ruleelementseq = ind0.ruleelementseq 
      and ind0.dimensionseq = dim0.dimensionseq
      and ind0.effectivestartdate <= :v_date
      and ind0.effectiveenddate > :v_date
      and ind0.removedate = :v_eot
    join cs_mdltdimension dim1 on 
      mdlt.ruleelementseq = dim1.ruleelementseq
      and dim1.dimensionslot = 1
      and dim1.effectivestartdate <= :v_date
      and dim1.effectiveenddate > :v_date
      and dim1.removedate = :v_eot
    join cs_mdltindex ind1 on 
      mdlt.ruleelementseq = ind1.ruleelementseq
      and ind1.effectivestartdate <= :v_date
      and ind1.effectiveenddate > :v_date
      and ind1.dimensionseq = dim1.dimensionseq
      and ind1.removedate = :v_eot 
    join cs_mdltdimension dim2 on 
      mdlt.ruleelementseq = dim2.ruleelementseq 
      and dim2.dimensionslot = 2
      and dim2.effectivestartdate <= :v_date
      and dim2.effectiveenddate > :v_date
      and dim2.removedate = :v_eot
    join cs_mdltindex ind2 on 
      mdlt.ruleelementseq = ind2.ruleelementseq 
      and ind2.dimensionseq = dim2.dimensionseq
      and ind2.effectivestartdate <= :v_date
      and ind2.effectiveenddate > :v_date
      and ind2.removedate = :v_eot 
    join cs_mdltcell cell on 
      mdlt.ruleelementseq = cell.mdltseq 
      and ind0.ordinal = cell.dim0index
      and ind1.ordinal = cell.dim1index
      and ind2.ordinal = cell.dim2index
      and cell.effectivestartdate <= :v_date
      and cell.effectiveenddate > :v_date
      and cell.removedate = :v_eot
    where mdlt.removedate= :v_eot 
      and mdlt.name = 'LT_Minimum_Quota'
      and mdlt.effectivestartdate <= :v_date
      and mdlt.effectiveenddate > :v_date   
  )
  where country = :i_country
    and element = :i_element
    and period_cycle = :i_period_cycle;
end;