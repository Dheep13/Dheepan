--liquibase formatted sql

--changeset jcadby:KYN_V_TQ2COM_WF_Territory splitStatements:false stripComments:false
--comment: Create view
create or replace view EXT.KYN_V_TQ2COM_WF_Territory as
SELECT
  x.hierarchy_rank AS rank,
  x.hierarchy_level AS level,
  x.parent_id,
  x.node_id,
  x.territoryprogramseq,
  x.territoryprogram_name,
  x.territoryseq,
  x.territory_name,
  x.territory_path,
  x.territory_path_display,
  x.territory_esd,
  x.territory_eed,
  count(pos.ruleelementownerseq) as position_count
FROM
  HIERARCHY_ANCESTORS_AGGREGATE(
    SOURCE HIERARCHY ( SOURCE (
      select
        tp.territoryprogramseq,
        tp.name as territoryprogram_name,
        t.territoryseq as node_id,
        t.parentseq as parent_id,
        t.territoryseq,
        t.name territory_name,
        t.effectivestartdate as territory_esd,
        t.effectiveenddate as territory_eed
      from csq_territory t
      join csq_territoryprogram tp on t.territoryprogramseq = tp.territoryprogramseq and tp.removedate = '2200-01-01'
      where t.removedate = '2200-01-01'
    ))
  MEASURES (
    STRING_AGG('['||territory_name||']', '') as territory_path,
    STRING_AGG(territory_name, ' / ') AS territory_path_display
  )
) x
left outer join csq_territoryposition tpos on
  tpos.territoryseq = x.territoryseq
  and tpos.removedate = '2200-01-01'
  -- include this filter for when a position has been removed
left outer join cs_position pos on
  tpos.positionseq = pos.ruleelementownerseq
  and pos.removedate = '2200-01-01'
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate >=  tpos.effectiveenddate
group by   
  x.hierarchy_rank,
  x.hierarchy_level,
  x.parent_id,
  x.node_id,
  x.territoryprogramseq,
  x.territoryprogram_name,
  x.territoryseq,
  x.territory_name,
  x.territory_path,
  x.territory_path_display,
  x.territory_esd,
  x.territory_eed
order by x.territoryprogram_name, x.territory_path;
