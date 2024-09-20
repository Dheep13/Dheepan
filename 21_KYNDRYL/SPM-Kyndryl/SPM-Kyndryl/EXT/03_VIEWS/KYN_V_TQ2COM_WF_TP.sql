--liquibase formatted sql

--changeset jcadby:KYN_V_TQ2COM_WF_TP splitStatements:false stripComments:false
--comment: Create view
create or replace view EXT.KYN_V_TQ2COM_WF_TP as
select 
per.periodseq,
per.startdate as period_startdate,
per.name      as period_name,
--
tp.territoryprogramseq, 
tp.name               as territoryprogram_name,
tp.casestatus         as territoryprogram_casestatus,
tp.effectivestartdate as territoryprogram_esd, 
tp.effectiveenddate   as territoryprogram_eed,
--
tp2.territoryprogramseq as linked_territoryprogramseq,
tp2.name                as linked_territoryprogram_name,
tp2.casestatus          as linked_territoryprogram_casestatus,
tp2.effectivestartdate  as linked_territoryprogram_esd, 
tp2.effectiveenddate    as linked_territoryprogram_eed
from csq_territoryprogram tp
join cs_period per on tp.periodseq = per.periodseq and per.removedate = '2200-01-01'
left outer join csq_territoryprogram tp2 on 
  tp.periodseq = tp2.periodseq 
  and tp2.removedate = '2200-01-01' 
  and tp2.name != tp.name
  and tp2.name like 'FY__H2$_Seller$_%' escape '$'
  and REPLACE_REGEXPR('(FY[0-9][0-9])(H[1-2])(.*)' FLAG 'i' IN tp2.name WITH '\1H$\3' OCCURRENCE 1) = 
  REPLACE_REGEXPR('(FY[0-9][0-9])(H[1-2])(.*)' FLAG 'i' IN tp.name WITH '\1H$\3' OCCURRENCE 1)
where tp.removedate= '2200-01-01' and tp.name like 'FY__H_$_Seller$_%' escape '$'
order by per.startdate, tp.name;