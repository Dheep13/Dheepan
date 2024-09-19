with h as (SELECT *
from HIERARCHY (
			 		Source (SELECT
    t.name || '|' || p.name as node_id,
    tm.name || '|' || m.name as parent_id
    -- p.name AS Position,
    -- m.name AS MgrPosition,
    -- t.name AS Title,
    -- tm.name AS MgrTitle,
    -- p.effectivestartdate AS PosStartDate,
    -- p.effectiveenddate AS PosEndDate
FROM
    cs_position p
    JOIN cs_position m 
        ON m.ruleelementownerseq = p.managerseq
        AND m.tenantid = p.tenantid
        AND p.effectiveenddate BETWEEN m.effectivestartdate AND ADD_DAYS(m.effectiveenddate,-1)
    JOIN cs_title t 
        ON t.ruleelementownerseq = p.titleseq
        AND t.tenantid = p.tenantid
        AND p.effectiveenddate  BETWEEN t.effectivestartdate AND ADD_DAYS(t.effectiveenddate,-1)
    JOIN cs_title tm 
        ON tm.ruleelementownerseq = m.titleseq
        AND tm.tenantid = p.tenantid
        AND m.effectiveenddate  BETWEEN tm.effectivestartdate AND ADD_DAYS(tm.effectiveenddate,-1)
        
      join ext.inbound_cfg_Rolldown icr 
        on icr.current_parenttitle=tm.name
		and icr.current_childtitle=t.name
	    
WHERE
    p.removedate > CURRENT_TIMESTAMP
    AND m.removedate > CURRENT_TIMESTAMP
    AND t.removedate > CURRENT_TIMESTAMP
    AND tm.removedate > CURRENT_TIMESTAMP
    and icr.type = 'REVERSE'
			 		)
					-- start where ruleelementownerseq = 4785074604152399 -- DSC : ruleelementownerseq will not be the same for all environments  
					start where tm.name in (select distinct current_parenttitle from ext.inbound_cfg_Rolldown
					where type = 'REVERSE'
					) -- DSC : replace ruleelementownerseq with position name
				
				)
				
				-- inner join
				-- ext.inbound_cfg_Rolldown icr on 
			 --   icr.current_parenttitle=mgrtitle
				-- and icr.current_childtitle=title
			 --  where icr.type = 'REVERSE'
			   order by hierarchy_root_rank, hierarchy_rank)
SELECT
    node_id,
    parent_id,
    path,
    SUBSTR_AFTER (node_id,'|') as CHILDNAME,
    SUBSTR_AFTER (parent_id,'|') as PARENTNAME,
    SUBSTR_BEFORE (node_id,'|') as CHILDTITLE,
    SUBSTR_BEFORE (parent_id,'|') as PARENTTITLE,
    SUBSTR_AFTER(SUBSTR_BEFORE (PATH,'/'),'|') as HIERARCHYMGRPOSITION,
    SUBSTR_BEFORE(SUBSTR_BEFORE (PATH,'/'),'|') as HIERARCHYMGRTITLE,
    HIERARCHY_LEVEL
FROM HIERARCHY_ANCESTORS_AGGREGATE (
    SOURCE h
    MEASURES ( string_agg(node_id, '/') AS path ) )
    where node_id <> path
ORDER BY
    node_id;