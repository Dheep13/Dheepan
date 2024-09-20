--liquibase formatted sql

--changeset jcadby:KYN_Fnc_TQ2Com_IPL_Account_Summary splitStatements:false stripComments:false
--comment: Create function

CREATE or replace function ext.KYN_Fnc_TQ2Com_IPL_Account_Summary(
    i_positionSeq bigint default null,
    i_periodSeq bigint default null,
    i_periodType varchar(50) default null,
    i_startRow bigint default 1,
    i_endRow bigint default 1000000
  ) returns v_ret varchar(2147483647) as 
  
begin
  declare v_eot date := '2200-01-01';
  declare v_semiannual_periodType bigint;
  declare v_periodRow row like cs_period;
  declare v_null varchar(255) := '{null}';
  declare v_header varchar(1000) := '<table class="ruleElementTable table table-condensed"><thead><tr><th>Planning Level</th><th>Count</th></tr></thead>';

  declare cursor c_account_summary for
  select rn, PlanningLevel, planninglevel_count from (
  select row_number() over (order by sub.planninglevel_count, sub.planninglevel ) AS rn,
  sub.* 
  from (
      select
      case when grouping(gacc.genericAttribute1) = 1 then 'TOTAL' else ifnull(gacc.genericAttribute1, :v_null) end as PlanningLevel,
      count(distinct q.accountseq) as planninglevel_count
      from ext.kyn_tq2com_account q
        left outer join csq_gaaccount gacc on
          gacc.accountseq = q.accountseq
          and q.account_esd = gacc.effectivestartdate
          and q.account_createdate = gacc.createdate
       where q.run_key = (
          select max(q1.run_key)
          from ext.kyn_tq2com_prestage_quota q1
          where q1.positionseq = :i_positionseq
          and q.semiannual_periodseq = q1.semiannual_periodseq
        )
        and q.positionseq = :i_positionseq
        and q.semiannual_periodseq in (
          select periodseq
          from cs_period x
          where x.periodtypeseq = :v_semiannual_periodType
          and x.removedate = :v_eot
          and x.startdate >= :v_periodRow.startdate and x.enddate <= :v_periodRow.enddate
          and x.calendarseq = :v_periodRow.calendarseq
        )
      group by rollup(gacc.genericAttribute1)
  ) sub) A
  where A.rn between :i_startRow and :i_endRow
  order by A.rn;


  declare exit handler for sqlexception
    begin v_ret := null;
  end;
  
  select periodtypeseq into v_semiannual_periodType
  from cs_periodtype
  where removedate = :v_eot
  and lower(name) = 'semiannual';


  if lower(:i_periodType) in ('semiannual','semi-annual') then
    -- get semiannual parent period
    select sa.* into v_periodRow
    from cs_period sa
    join cs_period q on sa.periodseq = q.parentseq and q.removedate = :v_eot
    join cs_period m on q.periodseq = m.parentseq and m.removedate = :v_eot
    where sa.removedate = :v_eot
      and m.periodseq = :i_periodSeq;

  elseif lower(:i_periodType) = 'year' then
    -- get year parent peiod
    select yr.* into v_periodRow
    from cs_period yr    
    join cs_period sa on yr.periodseq = sa.parentseq and sa.removedate = :v_eot
    join cs_period q on sa.periodseq = q.parentseq and q.removedate = :v_eot
    join cs_period m on q.periodseq = m.parentseq and m.removedate = :v_eot
    where yr.removedate = :v_eot
      and m.periodseq = :i_periodSeq;
  end if;

  v_ret := :v_header;
  for x as c_account_summary
  do
    v_ret := :v_ret || '<tr><td>' || :x.PlanningLevel || '</td><td>' || :x.planninglevel_count||'</td></tr>';
  end for;

  if length(:v_ret) > length(:v_header) then
    v_ret := :v_ret || '</table>';
  else
    v_ret := '';
  end if;

end