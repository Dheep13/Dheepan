
SELECT *
from HIERARCHY (
			 		Source (SELECT
    p.ruleelementownerseq as node_id,
    p.managerseq as parent_id,
    p.name AS Position,
    m.name AS MgrPosition,
    t.name AS Title,
    tm.name AS MgrTitle,
    p.effectivestartdate AS PosStartDate,
    p.effectiveenddate AS PosEndDate
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
WHERE
    p.removedate > CURRENT_TIMESTAMP
    AND m.removedate > CURRENT_TIMESTAMP
    AND t.removedate > CURRENT_TIMESTAMP
    AND tm.removedate > CURRENT_TIMESTAMP)
					-- start where ruleelementownerseq = 4785074604152399 -- DSC : ruleelementownerseq will not be the same for all environments  
					start where tm.name in (select distinct current_parenttitle from ext.inbound_cfg_Rolldown
						where type = 'REVERSE'
					) -- DSC : replace ruleelementownerseq with position name
				
				)
				
				inner join
				ext.inbound_cfg_Rolldown icr on 
			    icr.current_parenttitle=mgrtitle
				and icr.current_childtitle=title
			   where icr.type = 'REVERSE'
				;
			
				-- where hierarchy_rank=1;
				
				
				select * from cs_position where name='1258921' and removedate > current_timestamp;
				
				-- select * from ext.inbound_cfg_Rolldown