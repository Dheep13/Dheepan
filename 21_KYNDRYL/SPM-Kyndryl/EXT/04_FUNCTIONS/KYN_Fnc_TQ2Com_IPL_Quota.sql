--liquibase formatted sql

--changeset jcadby:KYN_Fnc_TQ2Com_IPL_Quota splitStatements:false stripComments:false
--comment: Create function
create or replace function ext.KYN_Fnc_TQ2Com_IPL_Quota(
  i_positionSeq bigint default null,
  i_periodSeq bigint default null,
  i_quotaName varchar(127) default null,
  i_periodType varchar(50) default null
) returns v_ret decimal(25,10) as
begin
  declare v_eot date := '2200-01-01';
  declare v_semiannual_periodseq bigint;
  declare v_year_periodseq bigint;
  declare exit handler for sqlexception
  begin
    v_ret := null;
  end;
  
  select sa.periodseq, y.periodseq into v_semiannual_periodseq, v_year_periodseq
  from cs_period y
  join cs_period sa on y.periodseq = sa.parentseq and sa.removedate = :v_eot
  join cs_period q on sa.periodseq = q.parentseq and q.removedate = :v_eot
  join cs_period m on q.periodseq = m.parentseq and m.removedate = :v_eot
  where y.removedate = :v_eot
  and m.periodseq = :i_periodSeq;
  
  if lower(:i_periodType) in ('semiannual','semi-annual') then
    select value into v_ret
    from ext.kyn_tq2com_prestage_quota q
    where q.run_key = (
      select max(q1.run_key)
      from ext.kyn_tq2com_prestage_quota q1 
      where q.positionseq = q1.positionseq
      and q.semiannual_periodseq = q1.semiannual_periodseq
    )
    and q.positionseq = :i_positionseq
    and q.semiannual_periodseq = :v_semiannual_periodseq
    and q.quotaname = :i_quotaName
    and q.periodtypename = 'semiannual';
    
  elseif lower(:i_periodType) = 'year' then
  
    select value into v_ret
    from ext.kyn_tq2com_prestage_quota q
    where q.run_key = (
      select max(q1.run_key)
      from ext.kyn_tq2com_prestage_quota q1 
      where q.positionseq = q1.positionseq
      and q.territoryprogram_periodseq = q1.territoryprogram_periodseq
    )
    and q.positionseq = :i_positionseq
    and q.territoryprogram_periodseq = :v_year_periodseq
    and q.quotaname = :i_quotaName
    and q.periodtypename = 'year';  

  end if;
end;