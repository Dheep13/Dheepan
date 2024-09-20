-- we load a table for the year

    select
      per.periodseq,
      per.name as period,
      per.startdate,
      per.enddate,
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
    from cs_calendar cal
    join cs_period per on 
      cal.calendarseq = per.calendarseq
      and per.removedate = :v_eot
    join cs_periodtype pt on
      per.periodtypeseq = pt.periodtypeseq
      and pt.removedate = :v_eot
    join cs_relationalmdlt mdlt on
      mdlt.removedate= :v_eot 
      and mdlt.name = 'LT_Minimum_Quota'
      and mdlt.effectivestartdate < per.enddate
      and mdlt.effectiveenddate >= per.enddate
    join cs_mdltdimension dim0 on 
      mdlt.ruleelementseq = dim0.ruleelementseq 
      and dim0.dimensionslot = 0
      and dim0.effectivestartdate < per.enddate
      and dim0.effectiveenddate >= per.enddate
      and dim0.removedate = :v_eot 
    join cs_mdltindex ind0 on 
      mdlt.ruleelementseq = ind0.ruleelementseq 
      and ind0.dimensionseq = dim0.dimensionseq
      and ind0.effectivestartdate < per.enddate
      and ind0.effectiveenddate >= per.enddate
      and ind0.removedate = :v_eot
    join cs_mdltdimension dim1 on 
      mdlt.ruleelementseq = dim1.ruleelementseq
      and dim1.dimensionslot = 1
      and dim1.effectivestartdate < per.enddate
      and dim1.effectiveenddate >= per.enddate
      and dim1.removedate = :v_eot
    join cs_mdltindex ind1 on 
      mdlt.ruleelementseq = ind1.ruleelementseq
      and ind1.effectivestartdate < per.enddate
      and ind1.effectiveenddate >= per.enddate
      and ind1.dimensionseq = dim1.dimensionseq
      and ind1.removedate = :v_eot 
    join cs_mdltdimension dim2 on 
      mdlt.ruleelementseq = dim2.ruleelementseq 
      and dim2.dimensionslot = 2
      and dim2.effectivestartdate < per.enddate
      and dim2.effectiveenddate >= per.enddate
      and dim2.removedate = :v_eot
    join cs_mdltindex ind2 on 
      mdlt.ruleelementseq = ind2.ruleelementseq 
      and ind2.dimensionseq = dim2.dimensionseq
      and ind2.effectivestartdate < per.enddate
      and ind2.effectiveenddate >= per.enddate
      and ind2.removedate = :v_eot 
    join cs_mdltcell cell on 
      mdlt.ruleelementseq = cell.mdltseq 
      and ind0.ordinal = cell.dim0index
      and ind1.ordinal = cell.dim1index
      and ind2.ordinal = cell.dim2index
      and cell.effectivestartdate < per.enddate
      and cell.effectiveenddate >= per.enddate
      and cell.removedate = :v_eot
    where
      cal.removedate = :v_eot
      and cal.name = 'Fiscal Calendar'
      and pt.name in ('year','semiannual')
      and per.startdate >= '2023-04-01' and per.enddate <= '2024-04-01';
 