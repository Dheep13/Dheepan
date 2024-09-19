-- copy data from one user to another
insert into ext.kyn_tq2com_prestage_quota (
run_key, semiannual_periodseq, semiannual_name, 
effectivestartdate, effectiveenddate, quotaname, value, unittypeforvalue, 
periodtypename, positionseq, positionname)
select 
run_key, semiannual_periodseq, semiannual_name, 
effectivestartdate, effectiveenddate, quotaname, value, unittypeforvalue, 
periodtypename, 4785074604090125 as positionseq, '0123461_DivyaKesineni-001' as positionname
from ext.kyn_tq2com_prestage_quota 
where positionname = '5059419_IGNACIOMARTINEZ_01'
and (run_key, semiannual_periodseq) in (select max(run_key), semiannual_periodseq from ext.kyn_tq2com_prestage_quota group by semiannual_periodseq);

insert into ext.kyn_tq2com_account (
  run_key, semiannual_periodseq, semiannual_name, territory, t_esd, t_eed, 
  positionseq, position, pos_esd, pos_eed, tacc_esd, tacc_edd, 
  accountseq, accountid, account_esd, account_eed, 
  account_createdate, isaddedduetoparent
)
select run_key, semiannual_periodseq, semiannual_name, territory, t_esd, t_eed, 
4785074604090125 as positionseq, '0123461_DivyaKesineni-001' as position, pos_esd, pos_eed, tacc_esd, tacc_edd, accountseq, accountid, account_esd, account_eed, account_createdate, isaddedduetoparent
from ext.kyn_tq2com_account
where position = '5059419_IGNACIOMARTINEZ_01'
and (run_key, semiannual_periodseq) in (select max(run_key), semiannual_periodseq from ext.kyn_tq2com_prestage_quota group by semiannual_periodseq);


select us.userid, gr.groupid
from csi_usergroup ug
join csi_user us on ug.userseq = us.userseq
join csi_group gr on ug.groupseq = gr.groupseq;

select * from csi_principalrole;

select * from ext.kyn_tq2com_sync order by run_key desc;

select * from ext.kyn_tq2com_account where run_key = 1464;

select * from cs_period where name = 'September 2023';

select ext.KYN_Fnc_TQ2Com_IPL_Account_Summary(
    i_positionSeq => 4785074604095229,
    i_periodSeq=> 2533274790396396,
    i_periodType => 'semi-annual',
    i_startRow => 1,
    i_endRow => 10) html
    from dummy;

select ext.KYN_Fnc_TQ2Com_IPL_Account(
    i_positionSeq => 4785074604095229,
    i_periodSeq=> 2533274790396396,
    i_periodType => 'semi-annual',
    i_startRow => 1,
    i_endRow => 10) html
    from dummy;    
    
/* gives a timeout in the doc:
Accounts:

[SQLException when executing query "select EXT.KYN_Fnc_TQ2Com_IPL_Account(positionSeq, periodSeq, periodType, to_number(startRow), to_number(endRow)) from ( select 4785074604095229 as positionSeq, 2533274790396396 as periodSeq, 'semiannual' as periodType, 1.0000000000 as startRow, 1000000.0000000000 as endRow from dummy)"
for IPL_Account
[613]: execution aborted by timeout, Error executing Evaluation Service SystemSvc.queryForString. Caused by: SQLException when executing query "IPL_Account"
[613]: execution aborted by timeout.] 

looks like we have a timeout with the function from the docs:
The query has to finish within 5 seconds, or it will be canceled. (The query will be terminated and the pipeline will continue.)

*/
    
    

  select * from (
  select
    row_number() over (order by cycle, accountid) AS rn,
  ifnull(cycle, '{null}') as cycle,
  ifnull(Country, '{null}') as Country,
  Accountseq,
  ifnull(AccountId, '{null}') as AccountId,
  ifnull(PlanningLevel, '{null}') as PlanningLevel,
  ifnull(CoverageID, '{null}') as CoverageID,
  ifnull(CoverageName, '{null}') as CoverageName,
  ifnull(AccountName, '{null}') as AccountName
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
    join cs_period p on q.semiannual_periodseq = p.periodseq and p.removedate = '2200-01-01'
    where q.run_key = (
      select max(q1.run_key)
      from ext.kyn_tq2com_prestage_quota q1
      where q.positionseq = q1.positionseq 
      and q.semiannual_periodseq = q1.semiannual_periodseq
    )
    and q.positionseq = 4785074604095229 --:i_positionseq
        and q.semiannual_periodseq in (
          select periodseq
          from cs_period x
          where x.periodtypeseq =2814749767106567
          and x.removedate = '2200-01-01'
          and x.startdate >= '2023-04-01' and x.enddate <= '2023-10-01'
          and x.calendarseq = 2251799813685250
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
  where rn between 1 and 10
  order by rn;




    
    
    
    
    select sa.* 
    from cs_period sa
    join cs_period q on sa.periodseq = q.parentseq and q.removedate = '2200-01-01'
    join cs_period m on q.periodseq = m.parentseq and m.removedate = '2200-01-01'
    where sa.removedate = '2200-01-01'
      and m.periodseq = 2533274790396396;    


  select rn, PlanningLevel, planninglevel_count, semiannual_periodseq from (
  select row_number() over (order by sub.planninglevel_count, sub.planninglevel ) AS rn,
  sub.* 
  from (
      select  q.semiannual_periodseq,
      case when grouping(gacc.genericAttribute1) = 1 then 'TOTAL' else gacc.genericAttribute1 end as PlanningLevel,
      count(1) as planninglevel_count
      from ext.kyn_tq2com_account q
        join csq_account acc on
          q.accountseq = acc.accountseq
          and q.account_esd = acc.effectivestartdate
          and q.account_createdate = acc.createdate
        join csq_gaaccount gacc on
          gacc.accountseq = acc.accountseq
          and acc.effectivestartdate = gacc.effectivestartdate
          and acc.createdate = gacc.createdate
       where q.run_key = (
          select max(q1.run_key)
          from ext.kyn_tq2com_prestage_quota q1
          where q.positionseq = q1.positionseq 
          and q.semiannual_periodseq = q1.semiannual_periodseq
        )
        and q.positionseq = 4785074604095229
        and q.semiannual_periodseq in (
          select periodseq
          from cs_period x
          where x.periodtypeseq =2814749767106567
          and x.removedate = '2200-01-01'
          and x.startdate >= '2023-04-01' and x.enddate <= '2023-10-01'
          and x.calendarseq = 2251799813685250
        )
      group by  q.semiannual_periodseq, rollup(gacc.genericAttribute1)
  ) sub) A
  where A.rn between 1 and 100
  order by A.rn;
  
select * from cs_period where periodseq= 2533274790396404;  

select * from cs_period where periodseq= 2533274790396385 and removedate= '2200-01-01';

select * from cs_period where parentseq = 2533274790396385 and removedate= '2200-01-01';

select * from cs_period where parentseq in (2533274790396388, 2533274790396387) and removedate= '2200-01-01';

