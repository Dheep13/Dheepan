procedure SP_CLAWBACK_CALCULATION_FA(i_periodSeq in int) as

     type tRun is record
   (
      pipelineRunSeq     CS_PipelineRun.pipelineRunSeq%type,
      startTime          CS_PipelineRun.startTime%type,
      stopTime           CS_PipelineRun.stopTime%type,
      periodSeq          CS_PipelineRun.periodSeq%type,
      runMode            CS_PipelineRun.runMode%type,
      stageTypeSeq       CS_PipelineRun.stageTypeSeq%type,
      status             CS_PipelineRun.status%type,
      processingUnitSeq  CS_PipelineRun.processingUnitSeq%type
   );
v_tempcount integer;
   v_pipelinerunseq integer;
   v_pipelinerundate date;
   v_batch_no_lumpsum_comm integer;
   v_batch_no_lumpsum_comp integer;
   v_batch_no_ongoing_comm integer;
   v_batch_no_ongoing_comp integer;
   v_processingunitseq integer;
   v_str_cycledate VARCHAR2(20);
   v_period_start date;
   v_period_end date;
   STR_CYCLEDATE_FILE_NAME CONSTANT VARCHAR2(10) := 'GLOBAL';
   STR_CYCLEDATE_KEY       CONSTANT VARCHAR2(20) := 'OPER_CYCLE_DATE';
  STR_LUMPSUM CONSTANT VARCHAR2(20) := 'LUMPSUM';
  STR_ONGOING CONSTANT VARCHAR2(20) := 'ONGOING';
  STR_COMMISSION CONSTANT VARCHAR2(20) := 'COMMISSION';
  STR_COMPENSATION CONSTANT VARCHAR2(20) := 'COMPENSATION';
  STR_STATUS_START CONSTANT VARCHAR2(20) := 'start';
  STR_STATUS_PROCESSING CONSTANT VARCHAR2(20) := 'processing';
  STR_STATUS_FAIL CONSTANT VARCHAR2(20) := 'fail';
  STR_STATUS_COMPLETED_SP CONSTANT VARCHAR2(20) := 'completed_sp';
  STR_STATUS_COMPLETED_SH CONSTANT VARCHAR2(20) := 'completed_sh';
  V_MESSAGE        VARCHAR2(2000);
  v_batch_no_special integer; --added by Win for version 6
   --version 12 add by Amanda begin
   v_periodStartDate  date;
   v_previous_qtr varchar2(10);
   --version 12 end
    begin

    --get cycle date
   SELECT TXT_KEY_VALUE
    INTO v_str_cycledate
    FROM IN_ETL_CONTROL
   WHERE TXT_FILE_NAME = STR_CYCLEDATE_FILE_NAME
     AND TXT_KEY_STRING = STR_CYCLEDATE_KEY;

Log('[CB] Oper cycle date is: ' || v_str_cycledate);

   --get period start date and period end date
   select cp.startdate, (cp.enddate - 1)
     into v_period_start, v_period_end
     from cs_period cp
    where cp.periodseq = Gv_Periodseq;

--if current cycle date not between pipeline period, then exit.
if to_date(v_str_cycledate,'yyyy-mm-dd') between v_period_start and v_period_end then

    Log('[CB] SP_EXEC_COMMISSION_ONGOING_FA starting '|| v_str_cycledate);
    PK_AIA_CB_CALCULATION_FA.SP_EXEC_COMMISSION_ONGOING_FA(v_str_cycledate);
    Log('[CB] SP_EXEC_COMMISSION_ONGOING_FA ending');

    Log('[CB] SP_EXEC_COMP_ONGOING_FA starting');
    PK_AIA_CB_CALCULATION_FA.SP_EXEC_COMP_ONGOING_FA(v_str_cycledate);
    Log('[CB] SP_EXEC_COMP_ONGOING_FA ended');

    --get pipeline run sequence number
  --  v_pipelinerunseq := tcmp.pipelinerun.GetLatestActiveRunSeq(i_periodSeq, gv_processingUnitSeq);

    --get pipeline run date
  --  v_pipelinerundate := tcmp.pipelinerun.GetRunDate(v_pipelinerunseq, gv_processingUnitSeq);

/*Arjun 0607 - to get around the grants. temporary*/
select max(y.pipelinerunseq) , max(y.starttime)
into v_pipelinerunseq, v_pipelinerundate
from cs_pipelinerun y
where y.command='PipelineRun' and y.stoptime is null and y.starttime =
(Select max(x.starttime) from cs_pipelinerun  x where x.command='PipelineRun' and x.stoptime is null  )
;

if v_pipelinerundate is null then
select max(pipelinerunseq), sysdate
into v_pipelinerunseq, v_pipelinerundate
from cs_pipelinerun y
where y.command='PipelineRun'
;
end if;

   --added by Win for version 6
   --begin
   --get the batch number for special handling
   select nvl(max(batchnum), 0)
     into v_batch_no_special
     from aia_cb_batch_special
    where ISACTIVE = 'Y'
      and SPEC_RUN_DATE = to_date(v_str_cycledate, 'yyyy-mm-dd')
      and BUNAME = 'SGPAFA'; --v19 add BUNAME

    Log('[CB] Special run batch number is: ' || v_batch_no_special);
    --end

    --get active batch number for lumpsum commission
    v_batch_no_lumpsum_comm := PK_AIA_CB_CALCULATION_FA.fn_get_batch_no_fa(v_str_cycledate, STR_COMMISSION, STR_LUMPSUM,  STR_STATUS_COMPLETED_SP);

    if v_batch_no_lumpsum_comm = 0 then
      v_batch_no_lumpsum_comm := PK_AIA_CB_CALCULATION_FA.fn_get_batch_no_fa(v_str_cycledate, STR_COMMISSION, STR_LUMPSUM,  STR_STATUS_COMPLETED_SH);
    end if;

    Log('[CB] Clawback batch for commission LUMPSUM: ' || v_batch_no_lumpsum_comm);
    PK_AIA_CB_CALCULATION_FA.sp_revert_by_batch(v_batch_no_lumpsum_comm);

    --get active batch number for on-going commission
    v_batch_no_ongoing_comm := PK_AIA_CB_CALCULATION_FA.fn_get_batch_no_fa(v_str_cycledate, STR_COMMISSION, STR_ONGOING,  STR_STATUS_COMPLETED_SP);
    Log('[CB] Clawback batch for commission ONGOING: ' || v_batch_no_ongoing_comm);
    PK_AIA_CB_CALCULATION_FA.sp_revert_by_batch(v_batch_no_ongoing_comm);

    --get active batch number for lumpsum compensation
    v_batch_no_lumpsum_comp := PK_AIA_CB_CALCULATION_FA.fn_get_batch_no_fa(v_str_cycledate, STR_COMPENSATION, STR_LUMPSUM,  STR_STATUS_COMPLETED_SP);
    if v_batch_no_lumpsum_comp = 0 then
      v_batch_no_lumpsum_comp := PK_AIA_CB_CALCULATION_FA.fn_get_batch_no_fa(v_str_cycledate, STR_COMPENSATION, STR_LUMPSUM,  STR_STATUS_COMPLETED_SH);
    end if;
    Log('[CB] Clawback batch for compensation LUMPSUM: ' || v_batch_no_lumpsum_comp);
    PK_AIA_CB_CALCULATION_FA.sp_revert_by_batch(v_batch_no_lumpsum_comp);

    --get active batch number for on-going compensation
    v_batch_no_ongoing_comp := PK_AIA_CB_CALCULATION_FA.fn_get_batch_no_fa(v_str_cycledate, STR_COMPENSATION, STR_ONGOING,  STR_STATUS_COMPLETED_SP);
    Log('[CB] Clawback batch for compensation ONGOING: ' || v_batch_no_ongoing_comp);
    PK_AIA_CB_CALCULATION_FA.sp_revert_by_batch(v_batch_no_ongoing_comp);


    --prepare the commission clawback data for insert to TrueComp build in tables
    --credit
    PK_AIA_CB_CALCULATION_FA.SP_CREDIT_COMMISSION_FA (v_str_cycledate, v_batch_no_lumpsum_comm, v_batch_no_ongoing_comm);
    --primary measurement
    PK_AIA_CB_CALCULATION_FA.SP_PM_COMMISSION_FA (v_str_cycledate, v_batch_no_lumpsum_comm, v_batch_no_ongoing_comm);
    --pm credit trace
    PK_AIA_CB_CALCULATION_FA.SP_PMCRDTRACE_COMMISSION_FA (v_str_cycledate, v_batch_no_lumpsum_comm, v_batch_no_ongoing_comm);

    --Added by Win Tan for version 4 **Begin**
    if to_date(v_str_cycledate,'yyyy-mm-dd') = v_period_end then

    --prepare the compensation clawback data for insert to TrueComp build in tables
    --credit
    PK_AIA_CB_CALCULATION_FA.SP_CREDIT_COMP_FA (v_str_cycledate, v_batch_no_lumpsum_comp, v_batch_no_ongoing_comp);
    --primary measurement
    PK_AIA_CB_CALCULATION_FA.SP_PM_COMP_FA (v_str_cycledate, v_batch_no_lumpsum_comp, v_batch_no_ongoing_comp);
    --pm credit trace
    PK_AIA_CB_CALCULATION_FA.SP_PMCRDTRACE_COMP_FA (v_str_cycledate, v_batch_no_lumpsum_comp, v_batch_no_ongoing_comp);

    end if;
    --Added by Win Tan for version 4 **End**

--return; --arjun 0531 to stop proc
--insert records into cs_credit

Log('[CB] Insert COMMISSION records into cs_credit for batch-' || v_batch_no_lumpsum_comm || ' and batch-' || v_batch_no_ongoing_comm);
Log('[CB] Insert COMPENSATION records into cs_credit for batch-' || v_batch_no_lumpsum_comp || ' and batch-' || v_batch_no_ongoing_comp);

insert into cs_credit
( TENANTID,
  CREDITSEQ,
  PAYEESEQ,
  POSITIONSEQ,
  SALESORDERSEQ,
  SALESTRANSACTIONSEQ,
  PERIODSEQ,
  CREDITTYPESEQ,
  NAME,
  PIPELINERUNSEQ,
  ORIGINTYPEID,
  COMPENSATIONDATE,
  PIPELINERUNDATE,
  BUSINESSUNITMAP,
  PREADJUSTEDVALUE,
  UNITTYPEFORPREADJUSTEDVALUE,
  VALUE,
  UNITTYPEFORVALUE,
  RELEASEDATE,
  RULESEQ,
  ISHELD,
  ISROLLABLE,
  ROLLDATE,
  REASONSEQ,
  COMMENTS,
  GENERICATTRIBUTE1,
  GENERICATTRIBUTE2,
  GENERICATTRIBUTE3,
  GENERICATTRIBUTE4,
  GENERICATTRIBUTE5,
  GENERICATTRIBUTE6,
  GENERICATTRIBUTE7,
  GENERICATTRIBUTE8,
  GENERICATTRIBUTE9,
  GENERICATTRIBUTE10,
  GENERICATTRIBUTE11,
  GENERICATTRIBUTE12,
  GENERICATTRIBUTE13,
  GENERICATTRIBUTE14,
  GENERICATTRIBUTE15,
  GENERICATTRIBUTE16,
  GENERICNUMBER1,
  UNITTYPEFORGENERICNUMBER1,
  GENERICNUMBER2,
  UNITTYPEFORGENERICNUMBER2,
  GENERICNUMBER3,
  UNITTYPEFORGENERICNUMBER3,
  GENERICNUMBER4,
  UNITTYPEFORGENERICNUMBER4,
  GENERICNUMBER5,
  UNITTYPEFORGENERICNUMBER5,
  GENERICNUMBER6,
  UNITTYPEFORGENERICNUMBER6,
  GENERICDATE1,
  GENERICDATE2,
  GENERICDATE3,
  GENERICDATE4,
  GENERICDATE5,
  GENERICDATE6,
  GENERICBOOLEAN1,
  GENERICBOOLEAN2,
  GENERICBOOLEAN3,
  GENERICBOOLEAN4,
  GENERICBOOLEAN5,
  GENERICBOOLEAN6,
  PROCESSINGUNITSEQ)
  select 'AIAS' as tenantid,
  new_creditseq,
         payeeseq,
         positionseq,
         salesorderseq,
         salestransactionseq,
         periodseq,
         credittypeseq,
         name,
         v_pipelinerunseq            as pipelinerunseq,
         origintypeid,
         compensationdate,
         v_pipelinerundate           as pipelinerundate,
         businessunitmap,
         preadjustedvalue,
         unittypeforpreadjustedvalue,
         value,
         unittypeforvalue,
         releasedate,
         ruleseq,
         isheld,
         isrollable,
         rolldate,
         reasonseq,
         comments,
         genericattribute1,
         genericattribute2,
         genericattribute3,
         genericattribute4,
         genericattribute5,
         genericattribute6,
         genericattribute7,
         genericattribute8,
         genericattribute9,
         genericattribute10,
         genericattribute11,
         genericattribute12,
         genericattribute13,
         genericattribute14,
         genericattribute15,
         genericattribute16,
         genericnumber1,
         unittypeforgenericnumber1,
         genericnumber2,
         unittypeforgenericnumber2,
         genericnumber3,
         unittypeforgenericnumber3,
         genericnumber4,
         unittypeforgenericnumber4,
         genericnumber5,
         unittypeforgenericnumber5,
         genericnumber6,
         unittypeforgenericnumber6,
         genericdate1,
         genericdate2,
         genericdate3,
         genericdate4,
         genericdate5,
         genericdate6,
         genericboolean1,
         genericboolean2,
         genericboolean3,
         genericboolean4,
         genericboolean5,
         genericboolean6,
         processingunitseq
    from AIA_CB_CREDIT_STG crd
   where crd.batch_no in (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          v_batch_no_ongoing_comp,
                          v_batch_no_special --revised by Win Tan for version 6
                          )
  /*    inner join AIA_CB_BATCH_LIST cbl
  on crd.batch_no = cbl.batchnum*/
  ;

Log('[CB] Insert records into cs_credit' || '; row count: ' || to_char(sql%rowcount));

commit;

--delete the obsolete PM records belongs to last batchs
-- Un Supported feature in Callidus - Commented out - Balaji Mar 17 2017
--Log('[CB] Delete obsolete records in cs_measurement');
--delete from cs_measurement pm
-- where periodseq = Gv_Periodseq
--   and pm.value = 0
--   and pm.numberofcredits = 0
--   and pm.genericnumber1 is null
--   and substr(pm.name, -3) = '_CB';
--
--Log('[CB] Delete obsolete records in cs_measurement' || '; row count: ' || to_char(sql%rowcount));
--
--commit;


--insert records into cs_measurement
Log('[CB] Insert COMMISSION records into cs_measurement for batch-' || v_batch_no_lumpsum_comm || ' and batch-' || v_batch_no_ongoing_comm);
Log('[CB] Insert COMPENSATION records into cs_measurement for batch-' || v_batch_no_lumpsum_comp || ' and batch-' || v_batch_no_ongoing_comp);
Log(v_processingunitseq);
Log(v_batch_no_lumpsum_comm||' '|| v_batch_no_ongoing_comm||' '|| v_batch_no_lumpsum_comp||' '|| v_batch_no_ongoing_comp);


select count(*) into v_tempcount
    from AIA_CB_PM_STG pms
    join cs_period pd
on pd.periodseq=pms.periodseq and pd.removedate='1-jan-2200'
join  cs_position p
on p.ruleelementownerseq=pms.positionseq and p.removedate='1-jan-2200'
and pd.startdate between p.effectivestartdate and p.effectiveenddate-1
left join  cs_planassignable  pa
on (pa.ruleelementownerseq=p.titleseq) and pa.removedate='1-jan-2200'
and pd.startdate between pa.effectivestartdate and pa.effectiveenddate-1
    where pms.batch_no in (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          v_batch_no_ongoing_comp,
                          v_batch_no_special --revised by Win Tan for version 6
                          );
   -- group by new_measurementseq, pms.name, pms.payeeseq, pms.positionseq, pms.periodseq;

    Log('v_tempcount '||v_tempcount);


--/*No permission in DEV
merge into cs_measurement tgt
using (
select   --revised by Win for version 6
         --new_measurementseq as measurementseq,
         max(new_measurementseq) as measurementseq,
         max(pa.planseq) planseq,
         'AIAS' tenantid,
         pms.name,
         pms.payeeseq,
         pms.positionseq,
         pms.periodseq,
         --v_pipelinerunseq as pipelinerunseq,
         --v_pipelinerundate as pipelinerundate,
         max(pms.ruleseq) as ruleseq,
         sum(pms.value) as value,
         max(pms.unittypeforvalue) as unittypeforvalue,
         sum(pms.numberofcredits) as numberofcredits,
         max(pms.businessunitmap) as businessunitmap,
         max(pms.genericnumber1) as genericnumber1,
         max(pms.unittypeforgenericnumber1) as unittypeforgenericnumber1,
         max(pms.processingunitseq) processingunitseq,
         max(pms.unittypefornumberofcredits) unittypefornumberofcredits
    from AIA_CB_PM_STG pms
    join cs_period pd
on pd.periodseq=pms.periodseq and pd.removedate='1-jan-2200'
join  cs_position p
on p.ruleelementownerseq=pms.positionseq and p.removedate='1-jan-2200'
and pd.startdate between p.effectivestartdate and p.effectiveenddate-1
left join  cs_planassignable  pa
on (pa.ruleelementownerseq=p.titleseq) and pa.removedate='1-jan-2200'
and pd.startdate between pa.effectivestartdate and pa.effectiveenddate-1
    where pms.batch_no in (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          v_batch_no_ongoing_comp,
                          v_batch_no_special --revised by Win Tan for version 6
                          )
    group by --new_measurementseq,  --revised by Win for version 6
    pms.name, pms.payeeseq, pms.positionseq, pms.periodseq) src
on ( src.positionseq=tgt.positionseq and src.payeeseq=tgt.payeeseq
and src.periodseq=tgt.periodseq and src.tenantid=tgt.tenantid
 and src.processingunitseq=tgt.processingunitseq
 and src.name=tgt.name)
when matched then update
set tgt.planseq=src.planseq,
   -- tgt.pipelinerunseq=src.pipelinerunseq,
   --tgt.pipelinerundate=src.pipelinerundate,
   tgt.ruleseq=src.ruleseq,
   tgt.value=tgt.value+src.value,  --Modified by Gopi-28112019-to get total sum of measurement
   tgt.unittypeforvalue=src.unittypeforvalue,
   tgt.numberofcredits=src.numberofcredits,
   tgt.businessunitmap=src.businessunitmap,
   tgt.genericnumber1=src.genericnumber1,
   tgt.unittypeforgenericnumber1=src.unittypeforgenericnumber1,
   tgt.unittypefornumberofcredits=src.unittypefornumberofcredits
   where tgt.tenantid='AIAS'; --and tgt.processingunitseq=v_processingunitseq;

--*/


/*This query only works once  - since the orig measurement seq is not backed up*/
update  AIA_CB_PMCRDTRACE_STG tgt
set measurementseq= (select
--Version 12 add by Amanda begin
--max(m.measurementseq)
case when max(m.measurementseq) is null then tgt.measurementseq --for SPI CB, no need to update credit trace stage
     else max(m.measurementseq) end
--Version 12 add by Amanda end
from cs_measurement m
join AIA_CB_PM_STG pms
on m.positionseq=pms.positionseq and  m.payeeseq=pms.payeeseq
and m.periodseq=pms.periodseq --and src.tenantid=pms.tenantid
 and m.processingunitseq=pms.processingunitseq
 and m.name=pms.name and m.tenantid='AIAS'
where pms.batch_no in (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          v_batch_no_ongoing_comp,
                          v_batch_no_special --revised by Win Tan for version 6
                          )
and  pms.new_measurementseq =tgt.measurementseq
and pms.batch_no=tgt.batch_no
)
where tgt.batch_no in  (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          v_batch_no_ongoing_comp,
                          v_batch_no_special --revised by Win Tan for version 6
                          )
;
/*

insert into cs_measurement
  (measurementseq, planseq,
   name,
   payeeseq,
   positionseq,
   periodseq,
   pipelinerunseq,
   pipelinerundate,
   ruleseq,
   value,
   unittypeforvalue,
   numberofcredits,
   businessunitmap,
   genericnumber1,
   unittypeforgenericnumber1,
   processingunitseq,
   unittypefornumberofcredits)
  select new_measurementseq as measurementseq, max(pa.planseq),
         pms.name,
         pms.payeeseq,
         pms.positionseq,
         pms.periodseq,
         v_pipelinerunseq as pipelinerunseq,
         v_pipelinerundate as pipelinerundate,
         max(pms.ruleseq) as ruleseq,
         sum(pms.value) as value,
         max(pms.unittypeforvalue) as unittypeforvalue,
         sum(pms.numberofcredits) as numberofcredits,
         max(pms.businessunitmap) as businessunitmap,
         max(pms.genericnumber1) as genericnumber1,
         max(pms.unittypeforgenericnumber1) as unittypeforgenericnumber1,
         max(pms.processingunitseq),
         max(pms.unittypefornumberofcredits)
    from AIA_CB_PM_STG pms
    join cs_period pd
on pd.periodseq=pms.periodseq and pd.removedate='1-jan-2200'
join  cs_position p
on p.ruleelementownerseq=pms.positionseq and p.removedate='1-jan-2200'
and pd.startdate between p.effectivestartdate and p.effectiveenddate-1
left join  cs_planassignable  pa
on (pa.ruleelementownerseq=p.titleseq) and pa.removedate='1-jan-2200'
and pd.startdate between pa.effectivestartdate and pa.effectiveenddate-1
    where pms.batch_no in (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          v_batch_no_ongoing_comp)
    group by new_measurementseq, pms.name, pms.payeeseq, pms.positionseq, pms.periodseq
; */



Log('[CB] Merge records into cs_measurement' || '; row count: ' || to_char(sql%rowcount));
/*
select ruleelementownerseq, count(*)
from cs_planassignable
where effectiveenddate>sysdate and removedate>sysdate
group by ruleelementownerseq
having count(*)>1 returns  nothing


merge into cs_measurement tgt
using(
select m.measurementseq, pa.planseq from cs_measurement m
join cs_period pd
on pd.periodseq=m.periodseq and pd.removedate='1-jan-2200'
join  cs_position p
on p.ruleelementownerseq=m.positionseq and p.removedate='1-jan-2200'
and pd.startdate between pos.effectivestartdate and pos.effectiveenddate-1
join cs_planassignable pa
on (pa.ruleelementownerseq=p.ruleelementownerseq or pa.ruleelementownerseq=p.titleseq) and pa.removedate='1-jan-2200'
and pd.startdate between pa.effectivestartdate and pa.effectiveenddate-1
where  pa.planseq is not null and m.tenantid = 'AIAS' and m.processingunitseq=gv_processingUnitSeq
and m.name in (select name from AIA_CB_PM_STG pms where pms.batch_no in (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp, v_batch_no_ongoing_comp))
) src
on (src.measurementseq=tgt.measurementseq)
when matched then update set tgt.planseq=src.planseq
where    tgt.planseq is null
         and tgt.tenantid='AIAS' and tgt.processingunitseq=gv_processingUnitSeq ;
  */
    commit;

    --insert records into cs_pmcredittrace
Log('[CB] Insert COMMISSION records into cs_pmcredittrace for batch-' || v_batch_no_lumpsum_comm || ' and batch-' || v_batch_no_ongoing_comm);
Log('[CB] Insert COMPENSATION records into cs_pmcredittrace for batch-' || v_batch_no_lumpsum_comp || ' and batch-' || v_batch_no_ongoing_comp);

insert into cs_pmcredittrace
    (TENANTID,
    creditSeq,
       measurementSeq,
       ruleSeq,
       pipelineRunSeq,
       sourcePeriodSeq,
       targetPeriodSeq,
       sourceorigintypeid,
       contributionValue,
       unittypeforContributionValue,
       businessunitMap,
       processingUnitSeq)
  select 'AIAS' as Tenantid,
          creditseq,
         measurementseq,
         ruleseq,
         v_pipelinerunseq as pipelinerunseq,
         sourceperiodseq,
         targetperiodseq,
         sourceorigintypeid,
         contributionvalue,
         unittypeforcontributionvalue,
         businessunitmap,
         processingunitseq
    from AIA_CB_PMCRDTRACE_STG pcs
  /*    inner join AIA_CB_BATCH_LIST cbl
  on pcs.batch_no = cbl.batchnum */
   where pcs.batch_no in (v_batch_no_lumpsum_comm,
                          v_batch_no_ongoing_comm,
                          v_batch_no_lumpsum_comp,
                          v_batch_no_ongoing_comp,
                          v_batch_no_special --revised by Win Tan for version 6
                          )
;

Log('[CB] Insert records into cs_pmcredittrace' || '; row count: ' || to_char(sql%rowcount));

    commit;

--Version 12 add by Amanda begin
select startDate
      into v_periodStartDate
      from cs_period
     where periodSeq = i_periodSeq;

--get previous quarter for YTD clawback
select qtr.name
  into v_previous_qtr
  from cs_period csp
 inner join cs_period qtr
    on csp.parentseq = qtr.periodseq
   and qtr.removedate = '1-jan-2200'
   and qtr.calendarseq = gv_calendarSeq
   and qtr.periodtypeseq = 2814749767106563 --quarter
 where csp.startdate = add_months(v_periodStartDate, -3)
   and csp.removedate = '1-jan-2200'
   and csp.periodtypeseq = 2814749767106561 --month
   and csp.calendarseq = gv_calendarSeq;

Log('[CB] Get previous quarter for SPI FA CB' || '; row count: ' || to_char(sql%rowcount));

merge into cs_measurement tgt
using (
select 'AIAS' as tenantid,
       cb.buname,
       cb.year,
       cb.quarter,
       cb.WRI_AGT_CODE,
       pos.ruleelementownerseq,
       pos.payeeseq,
       sum(case when cb.year = extract (year from v_periodStartDate ) then YTD_SPI_CB --ONGONING YTD FYC CB
         else 0
        end) YTD_SPI_CB
    from AIA_CB_SPI_CLAWBACK cb
    inner join cs_position pos
           on  pos.name = 'SGT'||cb.WRI_AGT_CODE
           and pos.tenantid='AIAS'
           and pos.removedate='1-jan-2200'
           and v_periodStartDate between pos.effectivestartdate and pos.effectiveenddate-1
     where (cb.quarter || ' ' || cb.year) = v_previous_qtr
        and cb.buname = 'SGPAFA'
       group by
       cb.buname,
       cb.year,
       cb.quarter,
       cb.WRI_AGT_CODE,
       pos.ruleelementownerseq,
       pos.payeeseq
) src
on (   src.ruleelementownerseq=tgt.positionseq
   and src.payeeseq=tgt.payeeseq
   and i_periodSeq=tgt.periodseq
   and src.tenantid=tgt.tenantid
   )
when matched then update
set tgt.value = YTD_SPI_CB
   where tgt.tenantid = 'AIAS'
     and tgt.processingunitseq = Gv_Processingunitseq
     and tgt.name = 'PM_SPI_ONG_YTD_CB';

Log('[CB] Merge records into cs_measurement for SPI FA ONGOING YTD CB' || '; row count: ' || to_char(sql%rowcount));
commit;
--Version 12 end

--update the batch status to complete
PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_lumpsum_comm, STR_STATUS_COMPLETED_SH);
PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_ongoing_comm, STR_STATUS_COMPLETED_SH);
--Commented by Win Tan for version 4
--PK_AIA_CB_CALCULATION.sp_update_batch_status(v_batch_no_lumpsum_comp, STR_STATUS_COMPLETED_SH);
--PK_AIA_CB_CALCULATION.sp_update_batch_status(v_batch_no_ongoing_comp, STR_STATUS_COMPLETED_SH);

    --Added by Win Tan for version 4 **Begin**
    if to_date(v_str_cycledate,'yyyy-mm-dd') = v_period_end then
    PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_lumpsum_comp, STR_STATUS_COMPLETED_SH);
    PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_ongoing_comp, STR_STATUS_COMPLETED_SH);
    end if;
    --Added by Win Tan for version 4 **End**

   --added by Win for version 6
   --update the batch status for special handling
    update aia_cb_batch_special
       set ISACTIVE = 'N'
     where batchnum = v_batch_no_special
       and ISACTIVE = 'Y'
       and SPEC_RUN_DATE = to_date(v_str_cycledate, 'yyyy-mm-dd')
       and BUNAME = 'SGPAFA'; --v19 add BUNAME

else

Log('[CB] ' || v_str_cycledate || ' not exit in pipeline run period: ' || Gv_Periodseq);

end if;

      ---catch exception
      EXCEPTION WHEN OTHERS
        THEN    v_message := SUBSTR(SQLERRM,1,2000);
                Log('[CB] ' || v_message);
                --update the batch status to fail
                PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_lumpsum_comm, STR_STATUS_FAIL);
                PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_ongoing_comm, STR_STATUS_FAIL);
                PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_lumpsum_comp, STR_STATUS_FAIL);
                PK_AIA_CB_CALCULATION_FA.sp_update_batch_status(v_batch_no_ongoing_comp, STR_STATUS_FAIL);


end SP_CLAWBACK_CALCULATION_FA;
