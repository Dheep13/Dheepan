--liquibase formatted sql

--changeset jcadby:KYN_FNC_TQ2COM_Get_Subordinates_Count splitStatements:false stripComments:false
--comment: Create function
create or replace function EXT.KYN_FNC_TQ2COM_Get_Subordinates_Count(
    IN i_positionSeq bigint,
    IN i_effectivestartdate timestamp,
    IN i_effectiveenddate timestamp
  )
  returns o_subordinates_count number as
begin
  declare v_eot date := to_date('01/01/2200','mm/dd/yyyy');

  select count(distinct ruleelementownerseq) into o_subordinates_count
  from cs_position pos
  where pos.managerseq = :i_positionSeq
  and pos.removedate = :v_eot 
  and pos.effectivestartdate < :i_effectiveenddate
  and pos.effectiveenddate > :i_effectivestartdate
  and exists (
      select 1
      from cs_planassignable pas
      join cs_title ttl on 
        pas.ruleelementownerseq = ttl.ruleelementownerseq
        and ttl.removedate= :v_eot
        and ttl.effectiveenddate > pas.effectivestartdate 
        and ttl.effectivestartdate < pas.effectiveenddate
      where pas.ruleelementownerseq = pos.titleseq 
      and pas.removedate= :v_eot 
      and pas.effectiveenddate > :i_effectivestartdate 
      and pas.effectivestartdate < :i_effectiveenddate
      and pas.planseq is not null
      and upper(ttl.genericattribute1) = 'QUOTA'
  );
   
end;