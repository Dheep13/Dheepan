--liquibase formatted sql
--changeset jcadby:KYN_Fnc_TQ2Com_IPL_Account splitStatements:false stripComments:false
--comment: Create function

CREATE or replace function ext.KYN_Fnc_TQ2Com_IPL_Account(
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
  declare v_header varchar(1000) := '<table class="ruleElementTable table table-condensed">' || '<thead><tr><th>#</th><th>Cycle</th><th>Account ID</th><th>Country</th><th>Coverage ID</th><th>Coverage Name</th><th>Account Name</th></tr></thead>';
  
  declare cursor c_account_detail for
  select * from (
  select
    row_number() over (order by cycle, accountid) AS rn,
    ifnull(cycle, :v_null) as cycle,
    ifnull(Country, :v_null) as Country,
    Accountseq,
    ifnull(AccountId, :v_null) as AccountId,
    ifnull(PlanningLevel, :v_null) as PlanningLevel,
    ifnull(CoverageID, :v_null) as CoverageID,
    ifnull(CoverageName, :v_null) as CoverageName,
    ifnull(AccountName, :v_null) as AccountName
  from (
    select 
      string_agg(p.shortname, ' & ' order by p.shortname) as cycle,
      gacc.genericAttribute3 as Country,
      q.accountseq as Accountseq,
      q.accountid as AccountId,
      gacc.genericAttribute1 as PlanningLevel,
      gacc.genericAttribute12 as CoverageID,
      gacc.genericAttribute13 as CoverageName,
      case
        when gacc.genericAttribute1 in ('COVERAGE', 'REMAIN') then gacc.genericAttribute13
        when gacc.genericAttribute1 in ('GBG') then gacc.genericAttribute7
        when gacc.genericAttribute1 in ('BG') then gacc.genericAttribute9
      end as AccountName
    from ext.kyn_tq2com_account q
    left outer join csq_gaaccount gacc on
      gacc.accountseq = q.accountseq
      and q.account_esd = gacc.effectivestartdate
      and q.account_createdate = gacc.createdate
    join cs_period p on q.semiannual_periodseq = p.periodseq and p.removedate = :v_eot
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
    group by
      q.accountseq,
      q.accountid,
      gacc.genericAttribute1,
      gacc.genericAttribute3,      
      gacc.genericAttribute7, 
      gacc.genericAttribute9,
      gacc.genericAttribute12,
      gacc.genericAttribute13
  ) A
  )
  where rn between :i_startRow and :i_endRow
  order by rn;

  declare exit handler for sqlexception
  begin
    v_ret := null;
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

  for x as c_account_detail
  do
    v_ret := :v_ret || '<tr><td>' || to_char(x.rn) || '</td><td>' || :x.cycle || '</td><td>'|| :x.accountid || '</td><td>'|| :x.country || '</td><td>'|| :x.CoverageID || '</td><td>'|| :x.CoverageName || '</td><td>'|| :x.accountname || '</td></tr>';
  end for;

  if length(:v_ret) > length(:v_header) then
    v_ret := :v_ret || '</table>';
  else
    v_ret := '';
  end if;

end