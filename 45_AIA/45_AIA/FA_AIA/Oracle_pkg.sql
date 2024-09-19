CREATE OR REPLACE EDITIONABLE PACKAGE BODY "AIASEXT"."PK_AIA_CB_CALCULATION_FA" AS
  /*
  this pakage is created for Fair BSC clawback calculation
  ************************************************
  Version     Create By       Create Date   Change
  ************************************************
  1           Gopi Callidus   20181203      Added procedures for FA Agents
  2           Amanda Wei      20190531      For BSC SPI clawback enhancement
  3           Endi            20190921      Fix bug in condition SP_TRACE_FORWARD_COMMISSION_FA Ongoing
  4           Endi            20190924      Fix bug in SP_TRACE_FORWARD_COMMISSION_FA (Ongoing) and fine tuning
  5           Endi            20200430      Fine tune SP_TRACE_FORWARD_COMMISSION_FA for ongoing after "'insert 1 started'"
                                           - add AIA_CB_TRACE_FORWARD_TMP.PM_TARGETPERIODSEQ
                                           - join AIA_CB_IDENTIFY_POLICY at first query to limit result
  6           Amanda Wei      20191025      For BSC day2 project enhancement
  7           Sammi Chen      20191206      For BSC day2 project enhancement
  8           Endi            20200928      Performance tuning
  9           Endi            20201030      Performance tuning for aias_tx_temp2
  10          Sammi           20201104      To support forget agent share case clawback
  11          Duncan          20210604      fix bug clawback name, 'RYO_FA_ONG' instead of 'RYO_ONG_FA'
  12          Endi            20211231      CS0589748 add ORDERED hints to SP_TRACE_FORWARD_COMMISSION_FA
  13          King            20220920      Harmonization Phase 4 FYO/RYO/AI/NADOR ClawBack
  14          Endi            20230211      GST8 LT_GST_Rate change
  15          Endi            20230330      ensure cs_rule has effectivestart or end date condition
  16          Zero            20230331      Harm BSC SPI commision issue
  17          Zero            20230331      Harm BSC SPI clawback
  */
procedure init as

begin

--setup processing unit seq number
select processingunitseq into V_PROCESSINGUNITSEQ from cs_processingunit where name = STR_PU;

--setup calendar seq number
select CALENDARSEQ into V_CALENDARSEQ from cs_calendar where name = STR_CALENDARNAME;

--get weekend date
SELECT CTL.TXT_KEY_VALUE INTO V_WEEKEND_DATE FROM IN_ETL_CONTROL CTL
WHERE CTL.TXT_KEY_STRING='PAYMENT_END_DATE_WEEKLY' AND CTL.TXT_FILE_NAME= STR_PU;

--get week sequence number
SELECT CTL.TXT_KEY_VALUE INTO V_WEEKEND_SEQ FROM IN_ETL_CONTROL CTL
WHERE CTL.TXT_KEY_STRING='PAYMENT_SEQ_WEEKLY' AND CTL.TXT_FILE_NAME= STR_PU;

--get current cycle date
SELECT CTL.TXT_KEY_VALUE INTO V_CYCLE_DATE FROM IN_ETL_CONTROL CTL WHERE CTL.TXT_KEY_STRING='OPER_CYCLE_DATE';

--Version 2 Amanda add begin
--Get quarter period type
select periodtypeseq into V_periodtype_quarter_seq from cs_periodtype where name=STR_CALENDAR_TYPE_QTR;
select periodtypeseq into V_periodtype_month_seq from cs_periodtype where name=STR_CALENDAR_TYPE;
--Version 2 end

LOG('END INT');
end;

procedure Log(inText varchar2) is
    pragma autonomous_transaction;
    vText AIA_CB_DEBUG.text%type;
    vBatch_No integer;
  begin
    vText := substr(inText, 1, 4000);
    vBatch_No := fn_get_batch_no();
    insert into AIA_CB_DEBUG (datetime, text, batch_no) values (systimestamp, 'CB_LOG: ' || vText, vBatch_No);
    commit;
    dbms_output.put_line( to_char(systimestamp,'yyyy-mm-dd hh24:mi:ssxff') || ' CB: ' || vText);
  exception
    when others then
      rollback;
      raise;
  end Log;

    /* this procedure is to get the periodseq by cycle date*/
  function fn_get_periodseq(P_STR_CB_CYCLEDATE IN VARCHAR2) return number
  is
  v_periodseq integer;
  begin

    select cbp.periodseq
    into v_periodseq
    from cs_period cbp
   inner join cs_calendar cd
      on cbp.calendarseq = cd.calendarseq
   inner join cs_periodtype pt
      on cbp.periodtypeseq = pt.periodtypeseq
   where cd.name = STR_CALENDARNAME
     and cbp.removedate = to_date('2200-01-01','yyyy-mm-dd') --for Cosimo
     and to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE) between cbp.startdate and
         cbp.enddate - 1
     and pt.name = STR_CALENDAR_TYPE;

     return v_periodseq;

  end;

  /* this procedure is to create the batch no*/
--  PROCEDURE sp_create_batch_no(P_STR_CB_CYCLEDATE IN VARCHAR2, P_STR_CB_TYPE IN VARCHAR2, P_STR_CB_NAME IN VARCHAR2)
--  is
--  V_BATCH_NO INTEGER;
--  V_CB_QUARTER_NAME varchar2(50);
--  V_CB_CYCLE_TYPE varchar2(50);
--  V_PREVIOUS_BATCH_NO INTEGER;
--  STR_WEEKLY_CYCLE_DATE varchar2(50);
--  V_MONTHEND_FLAG INTEGER;
--  NUM_OF_CYCLE_IND integer;
--  NUM_LAST_BATCH integer;
--  begin
--
--if P_STR_CB_TYPE = STR_LUMPSUM then
----get measurement quarter name for lumpsum clawback
--select --cbp.cb_quarter_name
--       substr(cbp.cb_quarter_name, instr(cbp.cb_quarter_name, ' ') + 1) || ' ' ||
--       substr(cbp.cb_quarter_name, 1, instr(cbp.cb_quarter_name, ' ') - 1)
--  into V_CB_QUARTER_NAME
--  from AIA_CB_PERIOD cbp
-- where CB_CYCLEDATE = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE)
-- and cbp.buname = STR_BUNAME
-- and cbp.cb_name = P_STR_CB_NAME
-- ;
--
-- --need to be revised
-- --get current quarter by P_STR_CB_CYCLEDATE
--ELSIF P_STR_CB_TYPE = STR_ONGOING and P_STR_CB_NAME = STR_COMMISSION then
--
--    select to_char(to_date(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE),'yyyymm')
--    into V_CB_QUARTER_NAME
--    from dual;
--
--ELSIF P_STR_CB_TYPE = STR_ONGOING and P_STR_CB_NAME = STR_COMPENSATION then
--
-- select to_char(to_date(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE),'yyyymm')
--    into V_CB_QUARTER_NAME
--    from dual;
--
--end if;
--
----get last batch number of batch
--  select nvl(max(t.batchnum),0)
--    into V_PREVIOUS_BATCH_NO
--    from AIA_CB_BATCH_STATUS t
--   where t.buname = STR_BUNAME
--     and t.clawbacktype = P_STR_CB_TYPE
--     and t.clawbackname = P_STR_CB_NAME
--     and t.cb_quarter_name = V_CB_QUARTER_NAME
--     ;
--       --and t.status in (STR_STATUS_FAIL, STR_STATUS_COMPLETED_SP, STR_STATUS_COMPLETED_SH);
--
----Log('V_PREVIOUS_BATCH_NO: ' || V_PREVIOUS_BATCH_NO);
--
----get batch number by max(batch number) + 1
--  select nvl(max(batchnum),0) + 1 into V_BATCH_NO from AIA_CB_BATCH_STATUS;
--
----Log('V_BATCH_NO: ' || V_BATCH_NO);
--
----update the column islatest for previous cycle
--if V_PREVIOUS_BATCH_NO > 0 then
--  update AIA_CB_BATCH_STATUS cbs
--     set islatest = 'N'
--   where cbs.batchnum = V_PREVIOUS_BATCH_NO
--   and BUNAME=STR_BUNAME;
--
--   commit;
--
--end if;
--
----insert new cycle record
--insert into AIA_CB_BATCH_STATUS
--  (batchnum,
--   BUNAME,
--   cb_quarter_name,
--   status,
--   isactive,
--   islatest,
--   ispopulated,
--   cycledate,
--   clawbackname,
--   clawbacktype,
--   createdate,
--   updatedate)
--values
--  (V_BATCH_NO,
--   STR_BUNAME,
--   V_CB_QUARTER_NAME,
--   STR_STATUS_START,
--   'Y',
--   'Y',
--   'N',
--   to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE),
--   P_STR_CB_NAME,
--   P_STR_CB_TYPE,
--   sysdate,
--   '');
--
--  commit;
--
--  Log('V_BATCH_NO: ' || V_BATCH_NO);
--  Log('V_CB_QUARTER_NAME: ' || V_CB_QUARTER_NAME);
--  end sp_create_batch_no
--    ;

  /* this procedure is to get the batch no which is avaiable*/
function fn_get_batch_no(P_STR_CYCLEDATE IN VARCHAR2, P_CB_NAME IN VARCHAR2, P_CB_TYPE IN VARCHAR2, P_STATUS IN VARCHAR2) return number
  is
  v_batch_no integer;
  begin

  select nvl(max(cbs.batchnum), 0)
    into v_batch_no
    from AIA_CB_BATCH_STATUS cbs
   where to_char(cbs.cycledate, 'yyyymm') =
         to_char(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE), 'yyyymm')
     and cbs.status = P_STATUS
     and cbs.clawbacktype = P_CB_TYPE
     and cbs.clawbackname = P_CB_NAME
     and cbs.islatest = 'Y'
     and cbs.buname = STR_BUNAME_FA;

  return v_batch_no;

  end fn_get_batch_no
  ;

  /* this procedure is to get the latest avtive batch no*/
function fn_get_batch_no return number
    is
    v_batch_no integer;
    begin
    select nvl(max(cbs.batchnum),0)
    into v_batch_no
    from AIA_CB_BATCH_STATUS cbs
    where cbs.buname = STR_BUNAME_FA;

    return v_batch_no;
end fn_get_batch_no;

  /* this procedure is to get the clawback type of input batch no*/
  function fn_get_cb_type (P_BATCH_NO IN INTEGER) return varchar2
    is
    v_cb_type varchar2(50);
    begin
    select cbs.clawbacktype
    into v_cb_type
    from AIA_CB_BATCH_STATUS cbs
    where cbs.batchnum = P_BATCH_NO;

    return v_cb_type;
    end;

  /* this procedure is to get the clawback name of input batch no*/
  function fn_get_cb_name (P_BATCH_NO IN INTEGER) return varchar2
    is
    v_cb_name varchar2(50);
    begin
    select cbs.clawbackname
    into v_cb_name
    from AIA_CB_BATCH_STATUS cbs
    where cbs.batchnum = P_BATCH_NO;

    return v_cb_name;
    end;

  /* this procedure is to get the clawback name of input batch no*/
  function fn_get_cb_quarter (P_BATCH_NO IN INTEGER) return varchar2
    is
    v_cb_quarter varchar2(50);
    begin
    select cbs.cb_quarter_name
    into v_cb_quarter
    from AIA_CB_BATCH_STATUS cbs
    where cbs.batchnum = P_BATCH_NO;

    return v_cb_quarter;
    end;

  /* this procedure is to get the avaiable batch number*/
procedure sp_get_batch_no_pre(P_CB_NAME IN VARCHAR2, P_CB_TYPE IN VARCHAR2)
    is
    v_cb_type varchar2(50);
    v_cb_name varchar2(50);
    v_cb_month VARCHAR2(20);
    v_pre_cb_batch_no INTEGER;
    v_rec_count INTEGER;
begin

v_cb_month := to_char(to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE),'yyyymm');

delete from AIA_CB_BATCH_LIST cbl
 where cbl.clawbackname = P_CB_NAME
   and cbl.clawbacktype = P_CB_TYPE
;

--for commission lumpsum
if P_CB_NAME = STR_COMMISSION and P_CB_TYPE = STR_LUMPSUM then

insert into AIA_CB_BATCH_LIST
  select batchnum,
         cb_quarter_name,
         status,
         isactive,
         islatest,
         ispopulated,
         cycledate,
         clawbackname,
         clawbacktype
    from AIA_CB_BATCH_STATUS cbs
   where cbs.islatest = 'Y'
     and cbs.clawbackname = P_CB_NAME
     and cbs.clawbacktype = P_CB_TYPE
     and ((cbs.status = STR_STATUS_COMPLETED_SP and cbs.isactive = 'Y') or
         (cbs.status = STR_STATUS_COMPLETED_SH and cbs.isactive = 'N'))
     and to_char(cbs.cycledate, 'yyyymm') = v_cb_month;
   commit;

--for commission on-going
elsif P_CB_NAME = STR_COMMISSION and P_CB_TYPE = STR_ONGOING then
insert into AIA_CB_BATCH_LIST
  select batchnum,
         cb_quarter_name,
         status,
         isactive,
         islatest,
         ispopulated,
         cycledate,
         clawbackname,
         clawbacktype
    from AIA_CB_BATCH_STATUS cbs
   where cbs.islatest = 'Y'
     and cbs.clawbackname = P_CB_NAME
     and cbs.clawbacktype = P_CB_TYPE
     and to_char(cbs.cycledate, 'yyyymm') = v_cb_month
     and cbs.status = STR_STATUS_COMPLETED_SP
     and cbs.isactive = 'Y';

   commit;

--for compensation lumpsum
elsif P_CB_NAME = STR_COMPENSATION and P_CB_TYPE = STR_LUMPSUM then
insert into AIA_CB_BATCH_LIST
  select batchnum,
         cb_quarter_name,
         status,
         isactive,
         islatest,
         ispopulated,
         cycledate,
         clawbackname,
         clawbacktype
    from AIA_CB_BATCH_STATUS cbs
   where cbs.islatest = 'Y'
     and cbs.clawbackname = P_CB_NAME
     and cbs.clawbacktype = P_CB_TYPE
     and ((cbs.status = STR_STATUS_COMPLETED_SP and cbs.isactive = 'Y') or
         (cbs.status = STR_STATUS_COMPLETED_SH and cbs.isactive = 'N'))
     and to_char(cbs.cycledate, 'yyyymm') = v_cb_month;

   commit;

--for compensation on-going
elsif P_CB_NAME = STR_COMPENSATION and P_CB_TYPE = STR_ONGOING then
insert into AIA_CB_BATCH_LIST
  select batchnum,
         cb_quarter_name,
         status,
         isactive,
         islatest,
         ispopulated,
         cycledate,
         clawbackname,
         clawbacktype
    from AIA_CB_BATCH_STATUS cbs
   where cbs.islatest = 'Y'
     and cbs.clawbackname = P_CB_NAME
     and cbs.clawbacktype = P_CB_TYPE
     and to_char(cbs.cycledate, 'yyyymm') = v_cb_month
     and cbs.status = STR_STATUS_COMPLETED_SP
     and cbs.isactive = 'Y';

   commit;

end if;

--Log('previous clawback quarter is: ' || v_pre_cb_qtr);

end sp_get_batch_no_pre;

PROCEDURE sp_update_batch_status (P_BATCH_NO IN INTEGER, P_STR_STATUS IN VARCHAR2)
  IS
    BEGIN
       --status: completed_sp
       IF P_STR_STATUS = STR_STATUS_COMPLETED_SP THEN
          UPDATE AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,ISACTIVE='Y',UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO;
       --status: completed_sh
       ELSIF P_STR_STATUS = STR_STATUS_COMPLETED_SH THEN
          UPDATE AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,ISACTIVE='N',UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO;
       --status: fail
       ELSIF P_STR_STATUS = STR_STATUS_FAIL THEN
          UPDATE AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,ISACTIVE='N',UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO;
       --status: processing
       ELSIF P_STR_STATUS = STR_STATUS_PROCESSING THEN
          UPDATE AIA_CB_BATCH_STATUS ST SET ST.STATUS=P_STR_STATUS,UPDATEDATE=SYSDATE WHERE ST.BATCHNUM=P_BATCH_NO;
      END IF;

      commit;

  END sp_update_batch_status;

  /* this procedure is revert the records in staging table and TrueComp bulid in tables*/
procedure sp_revert_by_batch(P_BATCH_NO IN INTEGER) AS
V_REC_COUNT INTEGER;
Begin

Log('Revert clawback related tables for batch: ' || P_BATCH_NO);

----------------------------------------------------------------------------------
--delete Credit records by batch number
----------------------------------------------------------------------------------
--get records count from AIA_CB_CREDIT_STG
select count(1)
  into V_REC_COUNT
  from AIA_CB_CREDIT_STG
 where batch_no = P_BATCH_NO;

--delete the records in AIA_CB_CREDIT_STG if batch number is being reused.
if V_REC_COUNT > 0 then

 -- insert into aia_cb_credit_Stg_reset   select sysdate, a.* from AIA_CB_CREDIT_STG a where batch_no = P_BATCH_NO;
  delete from AIA_CB_CREDIT_STG where batch_no = P_BATCH_NO;

  Log('delete from AIA_CB_CREDIT_STG' || '; row count: ' || to_char(sql%rowcount));

  commit;

END IF;

----------------------------------------------------------------------------------
--delete PM records by batch number
----------------------------------------------------------------------------------
--get records count from AIA_CB_PM_STG
select count(1)
  into V_REC_COUNT
  from AIA_CB_PM_STG
 where batch_no = P_BATCH_NO;

--delete the records in AIA_CB_CREDIT_STG if batch number is being reused.
if V_REC_COUNT > 0 then

--delete related records in AIA_CB_PM_STG
delete from AIA_CB_PM_STG where batch_no = P_BATCH_NO;

Log('delete from AIA_CB_PM_STG' || '; row count: ' || to_char(sql%rowcount));

commit;
END IF;

----------------------------------------------------------------------------------
--delete PM Credit trace records by batch number
----------------------------------------------------------------------------------
--get records count from AIA_CB_PM_STG
select count(1)
  into V_REC_COUNT
  from AIA_CB_PMCRDTRACE_STG
 where batch_no = P_BATCH_NO;

--delete the records in AIA_CB_PMCRDTRACE_STG if batch number is being reused.
if V_REC_COUNT > 0 then

--delete related records in AIA_CB_PMCRDTRACE_STG
delete from AIA_CB_PMCRDTRACE_STG where batch_no = P_BATCH_NO;

Log('delete from AIA_CB_PMCRDTRACE_STG' || '; row count: ' || to_char(sql%rowcount));

commit;
END IF;

end sp_revert_by_batch
;

--this procedure is to manaually delete the records in identify policy list
procedure sp_delete_policy(P_POLICYIDSEQ IN INTEGER, P_DELETEBY IN VARCHAR2) AS

begin

insert into aia_cb_identify_policy_log
select  BUNAME,
  YEAR,
  QUARTER,
  WRI_DIST_CODE,
  WRI_DIST_NAME,
  WRI_DM_CODE,
  WRI_DM_NAME,
  WRI_AGY_CODE,
  WRI_AGY_NAME,
  WRI_AGY_LDR_CODE,
  WRI_AGY_LDR_NAME,
  WRI_AGT_CODE,
  WRI_AGT_NAME,
  FSC_TYPE,
  RANK,
  CLASS,
  FSC_BSC_GRADE,
  FSC_BSC_PERCENTAGE,
  PONUMBER,
  INSURED_NAME,
  CONTRACT_CAT ,
  LIFE_NUMBER,
  COVERAGE_NUMBER,
  RIDER_NUMBER,
  COMPONENT_CODE,
  COMPONENT_NAME,
  ISSUE_DATE,
  INCEPTION_DATE,
  RISK_COMMENCEMENT_DATE,
  FHR_DATE,
  BASE_RIDER_IND,
  TRANSACTION_DATE,
  PAYMENT_MODE,
  POLICY_CURRENCY,
  PROCESSING_PERIOD,
  CREATED_DATE,
  POLICYIDSEQ,
  SUBMITDATE,
   P_DELETEBY,
     FAOB_AGT_CODE from aia_cb_identify_policy ip
where ip.policyidseq = P_POLICYIDSEQ;

delete from aia_cb_identify_policy ip where ip.policyidseq = P_POLICYIDSEQ;

commit;

end sp_delete_policy
;

--PROCEDURE SP_POLICY_EXCL_INIT AS
--
--BEGIN
--
---- delete AIA_CB_POLICY_EXCL_HIST
--EXECUTE IMMEDIATE 'Truncate table AIA_CB_POLICY_EXCL_HIST drop storage';
--
--insert into AIA_CB_POLICY_EXCL_HIST
--  select /*+ PARALLEL */ distinct STR_BUNAME, st.ponumber, sysdate as create_date
--    from cs_salestransaction   st,
--         AIA_CB_COMPONENT_EXCL ex,
--         cs_businessunit       bu
--   where st.tenantid='AIAS' and st.processingUnitseq=V_PROCESSINGUNITSEQ and st.genericattribute19 in ('LF', 'LN', 'UL')
--     and st.productid = ex.component_name
--     and ex.removedate = to_date('22000101', 'yyyymmdd')
--     and st.genericattribute6 = '1'
--     and st.ponumber is not null
--     and st.businessunitmap = bu.mask
--     and bu.name = STR_BUNAME
--     and st.compensationdate < to_date('20160101', 'yyyymmdd');
--
--  commit;
--
--END SP_POLICY_EXCL_INIT;

--PROCEDURE SP_POLICY_EXCL(P_STR_CYCLEDATE IN VARCHAR2, P_CB_NAME IN VARCHAR2) AS
--
--V_CYCLEDATE             DATE;
--V_CB_YEAR               VARCHAR2(20);
--V_CB_QUARTER            VARCHAR2(20);
--V_INCEPTION_START_DT    DATE;
--V_INCEPTION_END_DT      DATE;
--
--BEGIN
--
--Log('SP_POLICY_EXCL start');
--
--  ------------------------get cycle date  'yyyy-mm-dd'--------------------------
--  SELECT TO_DATE(NVL(P_STR_CYCLEDATE, TXT_KEY_VALUE), STR_DATE_FORMAT_TYPE)
--    INTO V_CYCLEDATE
--    FROM IN_ETL_CONTROL
--   WHERE TXT_FILE_NAME = STR_CYCLEDATE_FILE_NAME
--     AND TXT_KEY_STRING = STR_CYCLEDATE_KEY;
--
--  ------------------------get clawback year and quarter, inception period--------------------------
--  SELECT CBP.YEAR,
--         CBP.Quarter,
--         CBP.Inception_Startdate,
--         CBP.Inception_Enddate
--    INTO V_CB_YEAR, V_CB_QUARTER, V_INCEPTION_START_DT, V_INCEPTION_END_DT
--    FROM AIA_CB_PERIOD CBP
--   WHERE CBP.CB_CYCLEDATE = TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
--     and CBP.BUNAME = STR_BUNAME
--     and CBP.Removedate = DT_REMOVEDATE
--     and cbp.cb_name = P_CB_NAME;
--
--
--    --for trace indirect credit rule records
--merge into AIA_CB_POLICY_EXCL pol_ex
--using (select hist.BUNAME, hist.PONUMBER
--         from AIA_CB_POLICY_EXCL_HIST hist
--       union
--       select STR_BUNAME, st.ponumber
--         from cs_salestransaction   st,
--              AIA_CB_COMPONENT_EXCL ex,
--              cs_businessunit       bu
--        where st.tenantid='AIAS' and st.processingUnitseq=V_PROCESSINGUNITSEQ and st.genericattribute19 in ('LF', 'LN', 'UL')
--          and st.productid = ex.component_name
--          and ex.removedate = to_date('22000101', 'yyyymmdd')
--          and st.genericattribute6 = '1'
--          and st.ponumber is not null
--          and st.businessunitmap = bu.mask
--          and bu.name = STR_BUNAME
--          and st.compensationdate between V_INCEPTION_START_DT and V_INCEPTION_END_DT
--          ) t
--on (pol_ex.buname = t.buname and pol_ex.ponumber = t.ponumber)
--when not matched then
--  insert
--    (BUNAME, PONUMBER, CYCLE_DATE, CREATE_DATE)
--  values
--    (t.BUNAME, t.PONUMBER, TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE), sysdate);
--
--Log('updated AIA_CB_POLICY_EXCL; row count: ' || to_char(sql%rowcount));
--
--  commit;
--
--Log('SP_POLICY_EXCL end');
--
--END SP_POLICY_EXCL;

--procedure SP_IDENTIFY_POLICY (P_STR_CB_CYCLEDATE IN VARCHAR2, P_CB_NAME IN VARCHAR2) is
--  /*
--  Clawback Policy/Component list is just for Lumpsum calculation,
--  which for user to review the policy/component that will be used as base table.
--  After the BSC agent list and policy list for exclusion are ready in system.
--  System will base on the credit and transaction to build up the policy/component list.
--
--  // @input P_STR_CB_CYCLEDATE: cycle date with format yyyymmdd
--
--  ************************************************
--  Version     Create By       Create Date   Change
--  ************************************************
--  1           Zeno Zhao        20160510    Initial
--  */
--
--  /* TODO enter package declarations (types, exceptions, methods etc) here */
--
--  -- define period seq of each month
--  TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
--  t_periodseq periodseq_type;
--
--  dt_cb_cycledate date;   -- date format for input cb cycle date
--   v_cb_period  aia_cb_period%rowtype;    --row variable of the CS period. For each procedure call, there is one cb_period_record
--
--  int_sg_calendar_seq cs_calendar.calendarseq%type;  -- SG calendar seq
--  int_periodtype_month_seq cs_periodtype.periodtypeseq%type; -- month period type seq
--
--  int_bu_unit_map_sgp int;
--begin
--
--init;
--
--  ---------------- initial variables
--  --get the batch number from batch status table
--  --select 1 into V_BATCH_NO from AIA_CB_BATCH_STATUS bs;
--  --V_BATCH_NO := 1;
--
--  -- get calendar seq and period type seq
--  select calendarseq into int_sg_calendar_seq from cs_calendar where name=STR_CALENDARNAME;
--  select periodtypeseq into int_periodtype_month_seq from cs_periodtype where name=STR_CALENDAR_TYPE;
--  select mask into int_bu_unit_map_sgp from cs_businessunit where name=STR_BUNAME;
--  Log('calendar seq: ' || to_char(int_sg_calendar_seq) || '; month seq: ' || to_char(int_periodtype_month_seq) || '; BU mask: ' || to_char(int_bu_unit_map_sgp) );
--
--  -- get cs_period record
--  dt_cb_cycledate :=TO_DATE(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);
--
--  select *
--    into v_cb_period
--    from aia_cb_period
--   where cb_cycledate = dt_cb_cycledate
--     and BUNAME = STR_BUNAME
--     and CB_NAME = P_CB_NAME
--     and removedate = DT_REMOVEDATE
--     and rownum = 1;
--  Log('quarter ' || v_cb_period.cb_quarter_name);
--
--  ---------------- empty temp table
--  Log('Empty temp tables');
--  execute immediate 'truncate table AIA_CB_CREDITFILTER_TMP';
--  execute immediate 'truncate table AIA_CB_POLICY_INC_TMP';
--  execute immediate 'truncate table AIA_CB_CREDITFILTER';
--  execute immediate 'truncate table AIA_CB_SALESTRANSACTION';
--
--  -- delete old records for rerun
--  Log('Remove old record for rerun');
--  delete from AIA_CB_IDENTIFY_POLICY where buname=STR_BUNAME and year=v_cb_period.year and quarter=v_cb_period.quarter;
--  Log('Delete from  AIA_CB_IDENTIFY_POLICY' || '; row count: ' || to_char(sql%rowcount));
--  ------------------
--
--
--  -- get all month period seq
--  select p.periodseq BULK COLLECT into t_periodseq
--  from cs_period a, cs_period p
--  where  a.calendarseq=int_sg_calendar_seq
--    and p.calendarseq=int_sg_calendar_seq
--    --Revised by Win Tan for version 12 begin
--    --and a.name=v_cb_period.cb_quarter_name
--    and a.name in (v_cb_period.cb_quarter_name,
--        decode(v_cb_period.cb_quarter_name,'Q4 2017','Q1 2017S',''))
--    --version 12 end
--    and p.periodtypeseq= int_periodtype_month_seq
--    and p.startdate>=a.startdate
--    and p.enddate <=a.enddate
--    and p.startdate >= v_cb_period.inception_startdate
--    and p.enddate <= v_cb_period.inception_enddate + 1
--    and a.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
--    and p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
--    ;
--
--
--  --------- For each month (partition), filter the select credit into the temp table
--  for i in 1..t_periodseq.count loop
--    Log('peroid seq: [' || to_char(i) || ']= ' || to_char(t_periodseq(i)) );      -- log
--
--    -- Get credit record by month with cretia: agent is in date scope
--   insert into AIA_CB_CREDITFILTER_TMP
--   select /*+ parallel */
--    cr.CREDITSEQ,cr.PAYEESEQ,cr.POSITIONSEQ,cr.SALESORDERSEQ,cr.SALESTRANSACTIONSEQ,cr.PERIODSEQ,cr.CREDITTYPESEQ,cr.NAME,cr.PIPELINERUNSEQ,cr.ORIGINTYPEID,cr.COMPENSATIONDATE
--,cr.PIPELINERUNDATE,cr.BUSINESSUNITMAP,cr.PREADJUSTEDVALUE,cr.UNITTYPEFORPREADJUSTEDVALUE,cr.VALUE,cr.UNITTYPEFORVALUE,cr.RELEASEDATE,cr.RULESEQ,cr.ISHELD,cr.ISROLLABLE
--,cr.ROLLDATE,cr.REASONSEQ,cr.COMMENTS,cr.GENERICATTRIBUTE1,cr.GENERICATTRIBUTE2,cr.GENERICATTRIBUTE3,cr.GENERICATTRIBUTE4,cr.GENERICATTRIBUTE5,cr.GENERICATTRIBUTE6
--,cr.GENERICATTRIBUTE7,cr.GENERICATTRIBUTE8,cr.GENERICATTRIBUTE9,cr.GENERICATTRIBUTE10,cr.GENERICATTRIBUTE11,cr.GENERICATTRIBUTE12,cr.GENERICATTRIBUTE13,cr.GENERICATTRIBUTE14
--,cr.GENERICATTRIBUTE15,cr.GENERICATTRIBUTE16,cr.GENERICNUMBER1,cr.UNITTYPEFORGENERICNUMBER1,cr.GENERICNUMBER2,cr.UNITTYPEFORGENERICNUMBER2,cr.GENERICNUMBER3,cr.UNITTYPEFORGENERICNUMBER3
--,cr.GENERICNUMBER4,cr.UNITTYPEFORGENERICNUMBER4,cr.GENERICNUMBER5,cr.UNITTYPEFORGENERICNUMBER5,cr.GENERICNUMBER6,cr.UNITTYPEFORGENERICNUMBER6,cr.GENERICDATE1,
--cr.GENERICDATE2,cr.GENERICDATE3,cr.GENERICDATE4,cr.GENERICDATE5,cr.GENERICDATE6,cr.GENERICBOOLEAN1,cr.GENERICBOOLEAN2,cr.GENERICBOOLEAN3,
--cr.GENERICBOOLEAN4,cr.GENERICBOOLEAN5,cr.GENERICBOOLEAN6,cr.PROCESSINGUNITSEQ
--     from cs_credit cr
--    inner join AIA_CB_BSC_AGENT agt
--    on cr.GENERICATTRIBUTE12 = agt.AGENTCODE
--    inner join (select distinct SOURCE_RULE_OUTPUT
--                  from AIA_CB_RULES_LOOKUP
--                 where buname = STR_BUNAME
--                   and rule_type = 'CREDIT'
--                  and SOURCE_RULE_OUTPUT like '%\_DIRECT\_%' ESCAPE '\') rl
--    on cr.name = rl.SOURCE_RULE_OUTPUT
----    inner join cs_position POS on 'SGT'||AGT.AGENTCODE=POS.NAME
--    where cr.tenantid='AIAS' and cr.processingUnitseq=V_PROCESSINGUNITSEQ and cr.periodseq = t_periodseq(i)
--      and agt.ENTITLEMENTPERCENT <> 1 -- not equal 100%
--      and agt.year = v_cb_period.year --change to period year and quator instead of startdate/enddate
--      and agt.quarter = v_cb_period.quarter
--      and cr.compensationdate between v_cb_period.inception_startdate and v_cb_period.inception_enddate
--      AND CR.businessunitmap = int_bu_unit_map_sgp
----      and POS.GENERICATTRIBUTE6='AGY'
----       AND POS.removedate = DT_REMOVEDATE
----       AND POS.effectivestartdate <= AGT.ENDDATE
----       AND POS.effectiveenddate   > AGT.ENDDATE
--    ;
--
--    Log('moth peroid seq: [' || to_char(i) || '] ' || '; row count: ' || to_char(sql%rowcount));
--    commit;
--
--  end loop;
--
--    ----------- get target policy list
--    insert into AIA_CB_POLICY_INC_TMP
--      (ponumber, create_date, fhr_date)
--      select /*+ parallel */ distinct st.ponumber, sysdate, fhr.fhr_date
--        from cs_salestransaction st
--       inner join AIA_CB_CREDITFILTER_TMP cr
--          on st.salestransactionseq = cr.salestransactionseq
--        left join AIA_CB_POLICY_EXCL ex
--          on st.ponumber = ex.ponumber
--        left join AIA_CB_POLICY_FHR_DATE fhr
--          on st.ponumber = fhr.ponumber
--       where st.tenantid='AIAS' and st.processingUnitseq=V_PROCESSINGUNITSEQ and st.GENERICATTRIBUTE19 in ('LF', 'LN', 'UL')
--         and ex.ponumber is null
--         and EX.BUNAME=STR_BUNAME
--         and FHR.BUNAME=STR_BUNAME
--         ;
--    Log('CB policy include records; row count: ' || to_char(sql%rowcount));
----  end if;
--
--  commit;
--  --execute immediate 'analyze table AIA_CB_POLICY_INC_TMP compute statistic';
--
--
--------------- insert into CB credit filter
--  insert into AIA_CB_CREDITFILTER
--    select cr.genericattribute12,
--           cr.genericattribute14,
--           cr.genericattribute1,
--           cr.compensationdate,
--           cr.positionseq,
--           cr.genericdate2,
--           cr.salestransactionseq,
--           inc.fhr_date
--      from AIA_CB_CREDITFILTER_TMP cr
--     inner join AIA_CB_POLICY_INC_TMP inc
--        on cr.genericattribute6 = inc.ponumber;
--
-- Log('CB credit filter; row count: ' || to_char(sql%rowcount));
--
-- commit;
--
--  ------------- insert into CB transaction
--insert into AIA_CB_SALESTRANSACTION
--  select cr.genericattribute12 as WRI_AGT_CODE,
--         cr.genericattribute14 as CLASS,
--         st.ponumber as PONUMBER,
--         st.genericattribute23 as INSURED_NAME,
--         st.genericattribute19 as CONTRACT_CAT,
--         st.GENERICATTRIBUTE29 as LIFE_NUMBER,
--         st.GENERICATTRIBUTE30 as COVERAGE_NUMBER,
--         st.GENERICATTRIBUTE31 as RIDER_NUMBER,
--         cr.genericattribute1 as COMPONENT_CODE,
--         st.genericattribute3 as COMPONENT_NAME,
--         st.genericdate3 as ISSUE_DATE,
--         st.genericdate6 as INCEPTION_DATE,
--         st.genericdate2 as RISK_COMMENCEMENT_DATE,
--         cr.fhr_date as FHR_DATE,
--         decode(st.GENERICATTRIBUTE6, '1', 'Y', 'N') as BASE_RIDER_IND,
--         cr.compensationdate as TRANSACTION_DATE,
--         st.genericattribute1 as PAYMENT_MODE,
--         st.genericattribute5 as POLICY_CURRENCY,
--         st.salestransactionseq,
--         cr.positionseq,
--         cr.genericdate2 as POLICY_ISSUE_DATE,
--         gast.genericdate8 submitdate
--    from cs_salestransaction st
--   inner join AIA_CB_CREDITFILTER cr
--      on st.salestransactionseq = cr.salestransactionseq
--   inner join cs_gasalestransaction gast
--      on st.salestransactionseq = gast.salestransactionseq
--      and gast.pagenumber =0
--   where st.tenantid = 'AIAS'
--     and st.processingunitseq = V_PROCESSINGUNITSEQ
--     and greatest(nvl(st.genericdate3, to_date('19000101', 'yyyymmdd')),
--                  nvl(st.genericdate6, to_date('19000101', 'yyyymmdd')),
--                  nvl(st.genericdate2, to_date('19000101', 'yyyymmdd'))) between
--         v_cb_period.inception_startdate and v_cb_period.inception_enddate;
--
-- Log('CB transaction; row count: ' || to_char(sql%rowcount));
--
--     commit;
--
--  ------------- final insert into target
--  insert into AIA_CB_IDENTIFY_POLICY(
--      BUNAME
--      ,YEAR
--      ,QUARTER
--      ,WRI_DIST_CODE
--      ,WRI_DIST_NAME
--      ,WRI_DM_CODE
--      ,WRI_DM_NAME
--      ,WRI_AGY_CODE
--      ,WRI_AGY_NAME
--      ,WRI_AGY_LDR_CODE
--      ,WRI_AGY_LDR_NAME
--      ,WRI_AGT_CODE
--      ,WRI_AGT_NAME
--      ,FSC_TYPE
--      ,RANK
--      ,CLASS
--      ,FSC_BSC_GRADE
--      ,FSC_BSC_PERCENTAGE
--      ,PONUMBER
--      ,INSURED_NAME
--      ,CONTRACT_CAT
--      ,LIFE_NUMBER
--      ,COVERAGE_NUMBER
--      ,RIDER_NUMBER
--      ,COMPONENT_CODE
--      ,COMPONENT_NAME
--      ,ISSUE_DATE
--      ,INCEPTION_DATE
--      ,RISK_COMMENCEMENT_DATE
--      ,FHR_DATE
--      ,BASE_RIDER_IND
--      ,TRANSACTION_DATE
--      ,PAYMENT_MODE
--      ,POLICY_CURRENCY
--      ,PROCESSING_PERIOD
--      --,BATCH_NO
--      ,CREATED_DATE
--      ,POLICYIDSEQ
--      ,SUBMITDATE
--        )
--  select
--        curr_ip.BUNAME
--      ,curr_ip.YEAR
--      ,curr_ip.QUARTER
--      ,curr_ip.WRI_DIST_CODE
--      ,curr_ip.WRI_DIST_NAME
--      ,curr_ip.WRI_DM_CODE
--      ,curr_ip.WRI_DM_NAME
--      ,curr_ip.WRI_AGY_CODE
--      ,curr_ip.WRI_AGY_NAME
--      ,curr_ip.WRI_AGY_LDR_CODE
--      ,curr_ip.WRI_AGY_LDR_NAME
--      ,curr_ip.WRI_AGT_CODE
--      ,curr_ip.WRI_AGT_NAME
--      ,curr_ip.FSC_TYPE
--      ,curr_ip.RANK
--      ,curr_ip.CLASS
--      ,curr_ip.FSC_BSC_GRADE
--      ,curr_ip.FSC_BSC_PERCENTAGE
--      ,curr_ip.PONUMBER
--      ,curr_ip.INSURED_NAME
--      ,curr_ip.CONTRACT_CAT
--      ,curr_ip.LIFE_NUMBER
--      ,curr_ip.COVERAGE_NUMBER
--      ,curr_ip.RIDER_NUMBER
--      ,curr_ip.COMPONENT_CODE
--      ,curr_ip.COMPONENT_NAME
--      ,curr_ip.ISSUE_DATE
--      ,curr_ip.INCEPTION_DATE
--      ,curr_ip.RISK_COMMENCEMENT_DATE
--      ,curr_ip.FHR_DATE
--      ,curr_ip.BASE_RIDER_IND
--      ,curr_ip.TRANSACTION_DATE
--      ,curr_ip.PAYMENT_MODE
--      ,curr_ip.POLICY_CURRENCY
--      ,curr_ip.PROCESSING_PERIOD
--      --,BATCH_NO
--      ,curr_ip.CREATED_DATE
--      -- add sequence for id
--      ,SEQ_CB_IDENTIFY_POLICY.NEXTVAL as POLICYIDSEQ
--      ,curr_ip.submitdate
--  from (
--    select /*+ INDEX(cr IDX_CB_CREDITFILTER_TMP_1)*/
--        STR_BUNAME       as BUNAME
--        ,v_cb_period.year     as YEAR
--        ,v_cb_period.quarter   as QUARTER
--        --writing district info.
--        ,pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE
--        ,trim(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME
--        --writing district leader info.
--        ,pos_dis.genericattribute2 as WRI_DM_CODE
--        ,pos_dis.genericattribute7 as WRI_DM_NAME
--        --writing agency info.
--        ,substr(pos_agy.name, 4) as WRI_AGY_CODE
--        ,trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME
--        --writing agency leader info.
--        ,pos_agt.GENERICATTRIBUTE2 as WRI_AGY_LDR_CODE
--        ,pos_agt.genericattribute7 as WRI_AGY_LDR_NAME
--        --writing agent info.
--        ,st.WRI_AGT_CODE
--        ,trim(par_agt.firstname||' '||par_agt.lastname) as WRI_AGT_NAME
--        --,'Normal FSC' as FSC_TYPE
--        ,decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE
--        , title_agt.name as RANK
--        ,st.CLASS
--        ,agt.bsc_grade as FSC_BSC_GRADE
--        ,agt.entitlementpercent as FSC_BSC_PERCENTAGE
--        ,st.ponumber as PONUMBER            ---???
--        ,st.INSURED_NAME
--        ,st.CONTRACT_CAT
--        ,st.LIFE_NUMBER
--        ,st.COVERAGE_NUMBER
--        ,st.RIDER_NUMBER
--        ,st.COMPONENT_CODE
--        ,st.COMPONENT_NAME
--        ,st.ISSUE_DATE
--        ,st.INCEPTION_DATE
--        ,st.RISK_COMMENCEMENT_DATE
--        ,st.fhr_date          as FHR_DATE
--        ,st.BASE_RIDER_IND
--        ,st.TRANSACTION_DATE
--        ,st.PAYMENT_MODE
--        ,st.POLICY_CURRENCY
--        ,dt_cb_cycledate      as PROCESSING_PERIOD      --:22:23:24
--        --,V_BATCH_NO           as BATCH_NO   ---AIA_CB_BATCH_STATUS, :25:26:27:28:29 when to insert data into this table:30:31:32:33
--        ,sysdate              as CREATED_DATE
--        -- Rank by key: policy number, comonent code, writing agent
--        ,row_number() over(partition by st.ponumber, st.COMPONENT_CODE, st.WRI_AGT_CODE,
--                 st.LIFE_NUMBER, st.COVERAGE_NUMBER, st.RIDER_NUMBER order by st.TRANSACTION_DATE desc) rk
--        --,row_number() over(partition by st.ponumber, cr.genericattribute1,cr.genericattribute12  order by cr.compensationdate desc) rk
--        --,1 as rk
--        ,st.submitdate
--    from   AIA_CB_SALESTRANSACTION  st
--     inner join AIA_CB_BSC_AGENT agt
--        on st.WRI_AGT_CODE = agt.AGENTCODE
--       and agt.year = v_cb_period.year
--       and agt.quarter = v_cb_period.quarter
--     inner join cs_position pos_agy
--        on pos_agy.tenantid = 'AIAS'
--       AND pos_agy.ruleelementownerseq = st.positionseq
--       AND pos_agy.removedate = DT_REMOVEDATE
--       AND pos_agy.effectivestartdate <= st.POLICY_ISSUE_DATE
--       AND pos_agy.effectiveenddate   > st.POLICY_ISSUE_DATE
----       and pos_agy.GENERICATTRIBUTE6='AGY'
--     inner join cs_participant par_agy
--        on par_agy.tenantid = 'AIAS'
--        AND par_agy.PAYEESEQ = pos_agy.PAYEESEQ
--        AND par_agy.effectivestartdate <= st.POLICY_ISSUE_DATE
--        AND par_agy.effectiveenddate   >  st.POLICY_ISSUE_DATE
--        AND par_agy.removedate = DT_REMOVEDATE
--     inner join cs_position pos_dis
--        on pos_dis.tenantid = 'AIAS'
--        AND pos_dis.name= 'SGY' || pos_agy.genericattribute3
--        AND pos_dis.effectivestartdate <= st.POLICY_ISSUE_DATE
--        AND pos_dis.effectiveenddate   > st.POLICY_ISSUE_DATE
--        AND pos_dis.removedate = DT_REMOVEDATE
--     inner join cs_participant par_dis
--        on par_dis.tenantid = 'AIAS'
--        AND par_dis.PAYEESEQ = pos_dis.PAYEESEQ
--        AND par_dis.effectivestartdate <= st.POLICY_ISSUE_DATE
--        AND par_dis.effectiveenddate  > st.POLICY_ISSUE_DATE
--        AND par_dis.removedate = DT_REMOVEDATE
--      inner join cs_position pos_agt
--        on pos_agt.tenantid = 'AIAS'
--        AND 'SGT'||st.WRI_AGT_CODE=pos_agt.name
--        and pos_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
--        AND pos_agt.effectiveenddate   > st.POLICY_ISSUE_DATE
--        and pos_agt.removedate = DT_REMOVEDATE
--     inner join cs_participant par_agt
--     on par_agt.tenantid = 'AIAS'
--     AND par_agt.payeeseq= pos_agt.PAYEESEQ
--     AND par_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
--     AND par_agt.effectiveenddate  > st.POLICY_ISSUE_DATE
--     AND par_agt.removedate = DT_REMOVEDATE
--     inner join cs_title title_agt
--     on title_agt.tenantid = 'AIAS'
--     AND title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
--     AND title_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
--     AND title_agt.effectiveenddate   > st.POLICY_ISSUE_DATE
--     AND title_agt.REMOVEDATE = DT_REMOVEDATE
--   ) curr_ip
--   --if the component is being capture in previous quarters, then ignore to capture in current quarter
--   left join AIA_CB_IDENTIFY_POLICY pre_ip
--   on (pre_ip.year || ' ' || pre_ip.quarter) < (curr_ip.year || ' ' || curr_ip.quarter)
--   and pre_ip.ponumber = curr_ip.ponumber
--   and pre_ip.wri_agt_code = curr_ip.wri_agt_code
--   and pre_ip.life_number = curr_ip.life_number
--   and pre_ip.coverage_number = curr_ip.coverage_number
--   and pre_ip.rider_number = curr_ip.rider_number
--   and pre_ip.component_code = curr_ip.component_code
--   where curr_ip.rk=1
--   and pre_ip.BUNAME is null
--   ;
--
--   Log('Final AIA_CB_IDENTIFY_POLICY; row count: ' || to_char(sql%rowcount));
--
--   commit;
--
--end SP_IDENTIFY_POLICY;

/* this procedure is to trace forward for the commission*/
--PROCEDURE SP_TRACE_FORWARD_COMMISSION (P_STR_CYCLEDATE IN VARCHAR2, P_STR_TYPE IN VARCHAR2, P_BATCH_NO IN INTEGER) as
--
--V_CAL_PERIOD VARCHAR2(30); --measurement quarter
--DT_CB_START_DATE DATE;
--DT_CB_END_DATE DATE;
--DT_INCEPTION_START_DATE DATE;
--DT_INCEPTION_END_DATE DATE;
--DT_WEEKLY_START_DATE DATE;
--DT_WEEKLY_END_DATE DATE;
--DT_ONGOING_START_DATE DATE;
--DT_ONGOING_END_DATE DATE;
----NUM_OF_CYCLE_IND integer;
--v_cb_period  aia_cb_period%rowtype;
--vSQL varchar2(4000);
--vCalendarseq integer;
--vPertypeSeq integer;
--TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
--t_periodseq periodseq_type;
--vOngoingperiod number;
--vOngoingendperiod number;
--begin
--
--init;
--
----update status
--sp_update_batch_status (P_BATCH_NO,'processing');
--
--select calendarseq into vCalendarseq from cs_calendar where removedate =DT_REMOVEDATE and name='AIA Singapore Calendar';
--
--select periodtypeseq into vPertypeSeq from cs_periodtype where removedate =DT_REMOVEDATE  and name='month';
--
--/*
----if the input parameter for cycledate is not exist in AIA_CB_PERIOD, the program will end.
--select count(1)
--  into NUM_OF_CYCLE_IND
--  from AIA_CB_PERIOD cbp
-- where CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE);
--
--if NUM_OF_CYCLE_IND = 0 then
--  Log(P_STR_CYCLEDATE || ' is not the eligible cycle date.');
--  return;
--END IF; */
--
--Log('SP_TRACE_FORWARD_COMMISSION start');
--
--/*  select p.periodseq --BULK COLLECT into t_periodseq
--  from cs_period a, cs_period p
--  where a.calendarseq=2251799813685250
--    and p.calendarseq=2251799813685250
--    and a.name=V_CAL_PERIOD--'Q3 2016'
--    and p.periodtypeseq = 2814749767106561
--    and p.startdate>=a.startdate
--    and p.enddate <=a.enddate;
--*/
--
----get cycle date for weekly payment
----weekly payment start date
--select to_date(TXT_KEY_VALUE , STR_DATE_FORMAT_TYPE)
--  into DT_WEEKLY_START_DATE
--  from IN_ETL_CONTROL
-- where txt_key_string = 'PAYMENT_START_DATE_WEEKLY';
--
----weekly payment end date
--select to_date(TXT_KEY_VALUE , STR_DATE_FORMAT_TYPE)
--  into DT_WEEKLY_END_DATE
--  from IN_ETL_CONTROL
-- where txt_key_string = 'PAYMENT_END_DATE_WEEKLY';
--
--if P_STR_TYPE = STR_LUMPSUM then
--
--select * into v_cb_period from aia_cb_period where cb_cycledate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_COMMISSION and BUNAME=STR_BUNAME;
--
--select cbp.quarter || ' ' || cbp.year,
--       cbp.cb_startdate,
--       cbp.cb_enddate,
--       cbp.inception_startdate,
--       cbp.inception_enddate
--  into V_CAL_PERIOD,
--       DT_CB_START_DATE,
--       DT_CB_END_DATE,
--       DT_INCEPTION_START_DATE,
--       DT_INCEPTION_END_DATE
--  from AIA_CB_PERIOD cbp
-- where CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_COMMISSION AND BUNAME=STR_BUNAME;
--
--Log('DT_LUMPSUM_START_DATE = ' || DT_CB_START_DATE);
--Log('DT_LUMPSUM_END_DATE = ' || DT_CB_END_DATE);
--
---- Get the periodseqs for lumpsum period
-- select
--periodseq BULK COLLECT into t_periodseq
--from cs_period csp
-- inner join cs_periodtype pt
--    on csp.periodtypeseq = pt.periodtypeseq
-- where csp.startdate >= DT_CB_START_DATE
--and csp.enddate <= DT_CB_END_DATE + 1
--   and csp.removedate = DT_REMOVEDATE
--   and csp.calendarseq = V_CALENDARSEQ
--   and pt.name = STR_CALENDAR_TYPE;
--
--   execute immediate 'truncate table aia_tmp_comls_period';
--insert into aia_tmp_comls_period
--select periodseq  from cs_period csp
-- inner join cs_periodtype pt
--    on csp.periodtypeseq = pt.periodtypeseq
-- where csp.startdate >= DT_CB_START_DATE
--and csp.enddate <= DT_CB_END_DATE + 1
--   and csp.removedate = DT_REMOVEDATE
--   and csp.calendarseq = V_CALENDARSEQ
--   and pt.name = STR_CALENDAR_TYPE;
--   commit;
--
--Log('insert into AIA_CB_TRACE_FORWARD, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--/*
--for i in 1..t_periodseq.count loop
--
----for lumpsum commission trace forward
--insert /*+ APPEND   into AIA_CB_TRACE_FORWARD
--select STR_BUNAME as BUNAME,
--       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--       pm.positionseq as PAYEE_SEQ,
--       substr(dep_pos.name, 4) as PAYEE_CODE,
--       crd.genericattribute12 as PAYOR_CODE,
--       ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--       crd.compensationdate as TRANSACTION_DATE,
--       --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as TRANSACTION_DATE,
--       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
--       --clawback type = 'Lumpsum'
--       STR_LUMPSUM as CLAWBACK_TYPE,
--       --clawback name = 'Commission'
--       --STR_CB_NAME            as CLAWBACK_NAME,
--       rl.CLAWBACK_NAME       as CLAWBACK_NAME,
--       ct.credittypeid        as CREDITTYPE,
--       crd.creditseq          as CREDITSEQ,
--       crd.name               as CREDIT_NAME,
--       crd.value              as CREDIT_VALUE,
--       pm.measurementseq      as PM_SEQ,
--       pm.name                as PM_NAME,
--       pct.contributionvalue  as PM_CONTRIBUTE_VALUE,
--       1                      as PM_RATE,
--       dep.depositseq         as DEPOSITSEQ,
--       dep.name               as DEPOSIT_NAME,
--       dep.value              as DEPOSIT_VALUE,
--       crd.periodseq          as PERIODSEQ,
--       st.salestransactionseq as SALESTRANSACTIONSEQ,
--       crd.genericattribute2  as PRODUCT_NAME,
--       crd.genericnumber1     as POLICY_YEAR,
--       st.genericnumber2      as COMMISSION_RATE,
--       st.genericdate4        as PAID_TO_DATE,
--       P_BATCH_NO             as BATCH_NUMBER,
--       sysdate                as CREATED_DATE
--  FROM CS_SALESTRANSACTION st
-- inner join cs_period p
--    on      st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate    and p.calendarseq=2251799813685250
-- inner join CS_CREDIT crd
--    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
--        and crd.periodseq=p.periodseq
-- inner join CS_PMCREDITTRACE pct
--    on crd.CREDITSEQ = pct.CREDITSEQ
-- inner join CS_MEASUREMENT pm
--    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
-- inner join cs_depositpmtrace dpt
--    on pm.measurementseq = dpt.measurementseq
-- inner join cs_deposit dep
--    on dep.depositseq = dpt.depositseq
-- inner join cs_position dep_pos
--    on dep.positionseq = dep_pos.ruleelementownerseq
--   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= crd.genericdate2
--   and dep_pos.effectiveenddate > crd.genericdate2
-- inner join CS_CREDITTYPE ct
--    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--   and ct.Removedate = DT_REMOVEDATE
-- inner join AIA_CB_IDENTIFY_POLICY ip
--    on ip.BUNAME = STR_BUNAME
--   AND st.PONUMBER = ip.PONUMBER
--   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--   AND st.PRODUCTID = ip.COMPONENT_CODE
--   and crd.genericattribute12 = ip.wri_agt_code
--   and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
--   --check if the deposit position is same as writing agent
--   and dep_pos.name = 'SGT' || ip.wri_agt_code
-- inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'PM'
--                AND CLAWBACK_NAME in (STR_COMMISSION,STR_GST_COMMISSION)) rl
--    on pm.NAME = rl.SOURCE_RULE_OUTPUT
-- WHERE st.tenantid='AIAS' and crd.tenantid='AIAS' and pm.tenantid='AIAS'
-- and pct.tenantid='AIAS' and dpt.tenantid='AIAS'
-- and pct.PROCESSINGUNITSEQ= V_PROCESSINGUNITSEQ
-- and pct.TARGETPERIODSEQ=pm.periodseq
-- and dpt.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--      and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--      and crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--      and pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--      and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
--      AND st.BUSINESSUNITMAP = 1
--   and crd.genericattribute16 not in ('RO', 'RNO')
--   and  dep.periodseq =  t_periodseq(i)
--     ;
--*/
--
--
--
-- execute immediate 'truncate table aia_tmp_Comls_Step0';
-- insert into aia_tmp_Comls_Step0
--
-- select /*+ leading(ip,st) */ st.salestransactionseq,  ip.wri_agt_code as wri_agt_code_ORIG
-- , IP.QUARTER
--  || ' '
--  || IP.YEAR             AS CALCULATION_PERIOD,
--  IP.PONUMBER            AS POLICY_NUMBER,
--  IP.POLICYIDSEQ         AS POLICYIDSEQ,
--  IP.LIFE_NUMBER         AS LIFE_NUMBER,
--  IP.COVERAGE_NUMBER     AS COVERAGE_NUMBER,
--  IP.RIDER_NUMBER        AS RIDER_NUMBER,
--  IP.COMPONENT_CODE      AS COMPONENT_CODE,
--  IP.COMPONENT_NAME      AS COMPONENT_NAME,
--  IP.BASE_RIDER_IND      AS BASE_RIDER_IND,
--  ST.GENERICNUMBER2      AS COMMISSION_RATE,
--  ST.GENERICDATE4        AS PAID_TO_DATE ,
--  'SGT'
--  ||IP.WRI_AGT_CODE WRI_AGT_CODE ,
--  IP.QUARTER
--  || ' '
--  || IP.YEAR QTRYR
-- from cs_Salestransaction st
-- INNER JOIN AIA_CB_IDENTIFY_POLICY IP
--ON 1                              =1
--AND IP.BUNAME                     = STR_BUNAME
--AND ST.PONUMBER                   = IP.PONUMBER
--AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
--AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
--AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER
-- where st.tenantid='AIAS'
--and st.processingUnitseq=V_PROCESSINGUNITSEQ
----and st.compensationdate between '1-mar-2017' and '31-may-2017'
--and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
--;
--
--
--
--
--execute immediate 'TRUNCATE table aia_tmp_comls_step1';
--
--insert into AIA_TMP_COMLS_step1
--
--SELECT /*+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ ) */  CRD.CREDITSEQ,
--  CRD.SALESTRANSACTIONSEQ ,
--  ip.CALCULATION_PERIOD,
--  ip.POLICY_NUMBER,
--  ip.POLICYIDSEQ,
--  ip.LIFE_NUMBER,
--  ip.COVERAGE_NUMBER,
--  ip.RIDER_NUMBER,
--  IP.COMPONENT_CODE      ,
--  IP.COMPONENT_NAME     ,
--  IP.BASE_RIDER_IND     ,
--  CRD.COMPENSATIONDATE   AS TRANSACTION_DATE,
--  CRD.GENERICATTRIBUTE12 AS PAYOR_CODE,
--  CT.CREDITTYPEID        AS CREDITTYPE,
--  CRD.NAME               AS CREDIT_NAME,
--  CRD.VALUE              AS CREDIT_VALUE,
--  CRD.PERIODSEQ          AS PERIODSEQ,
--  CRD.GENERICATTRIBUTE2  AS PRODUCT_NAME,
--  CRD.GENERICNUMBER1     AS POLICY_YEAR,
--  ip.COMMISSION_RATE      AS COMMISSION_RATE,
--  ip.PAID_TO_DATE        AS PAID_TO_DATE ,
--  ip.WRI_AGT_CODE ,
--  ip.QTRYR,
--  CRD.GENERICDATE2
--FROM CS_CREDIT CRD
--JOIN AIA_TMP_COMLS_PERIOD P
--ON CRD.PERIODSEQ=P.PERIODSEQ
--INNER JOIN CS_CREDITTYPE CT
--ON CRD.CREDITTYPESEQ = CT.DATATYPESEQ
--AND CT.REMOVEDATE    >SYSDATE
--INNER JOIN aia_tmp_Comls_Step0 IP
--ON 1                              =1
----AND IP.BUNAME                     = STR_BUNAME
--and crd.salestransactionseq= ip.salestransactionseq
--AND CRD.GENERICATTRIBUTE12        = IP.WRI_AGT_CODE_ORIG
--WHERE 1=1
--and CRD.GENERICATTRIBUTE16 NOT IN ('RO', 'RNO')
--AND CRD.TENANTID                  = 'AIAS'
--AND CRD.PROCESSINGUNITSEQ         = 38280596832649218
--
--;
--
--/* 170807
--insert into aia_tmp_comls_step1
--
----drop table aia_tmp_comls_step1;
----create table aia_tmp_comls_step1 as
--select crd.creditseq,
--       crd.salestransactionseq ,
--        ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--        ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--        crd.compensationdate as TRANSACTION_DATE,
--         crd.genericattribute12 as PAYOR_CODE,
--         ct.credittypeid        as CREDITTYPE,
--       crd.name               as CREDIT_NAME,
--       crd.value              as CREDIT_VALUE,
--        crd.periodseq          as PERIODSEQ,
--         crd.genericattribute2  as PRODUCT_NAME,
--       crd.genericnumber1     as POLICY_YEAR,
--       st.genericnumber2      as COMMISSION_RATE,
--       st.genericdate4        as PAID_TO_DATE
--       ,'SGT'||ip.wri_agt_code wri_agt_code
--       ,ip.quarter || ' ' || ip.year qtrYr, crd.genericdate2
--
--  from cs_Credit crd
--  join aia_tmp_comls_period p
--  on crd.periodseq=p.periodseq
--  join cs_Salestransaction st
--  on st.salestransactionseq=crd.salestransactionseq
--  and st.tenantid='AIAS' and st.processingunitseq=crd.processingunitseq
-- -- and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
--   inner join CS_CREDITTYPE ct
--    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--   and ct.Removedate >sysdate
--  inner join AIA_CB_IDENTIFY_POLICY ip
--    on 1=1
--    and ip.BUNAME = STR_BUNAME
--   AND st.PONUMBER = ip.PONUMBER
--   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--   AND st.PRODUCTID = ip.COMPONENT_CODE
--   and crd.genericattribute12 = ip.wri_agt_code
--   where crd.genericattribute16 not in ('RO', 'RNO')
--   and crd.tenantid = 'AIAS'
--   and crd.processingunitseq = V_PROCESSINGUNITSEQ
--  --and st.compensationdate>='1-mar-2016' and st.compensationdate<='30-nov-2016'
----   and periodseq = 2533274790398934
----105 seconds. 9 mill rows for nov
----9 secs, 1221 rows
---- 240 secs 5000 rows
----select count(*) from xtmp
--;*/
--
--
--
--Log('insert 1 done '||SQL%ROWCOUNT);
--commit;
--
--delete from AIA_TMP_COMLS_STEP1 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;
--
--
--Log('delete 1 done '||SQL%ROWCOUNT);
--
--commit;
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP1"',
--          estimate_percent => 1
--          );
--execute immediate 'truncate table aia_tmp_comls_step2';
--insert into aia_tmp_comls_step2
----drop table aia_tmp_comls_step2;
----create table aia_tmp_comls_step2  as
--
--select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , x.clawback_name
--from cs_measurement m
--join aia_tmp_comls_period p
--  on m.periodseq=p.periodseq
--  join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'PM'
--                AND CLAWBACK_NAME in (STR_COMMISSION,STR_GST_COMMISSION)
--                AND BUNAME=STR_BUNAME)
--                x
--                on x.SOURCE_RULE_OUTPUT=m.name
--  where  m.processingunitseq = V_PROCESSINGUNITSEQ
--  and m.tenantid='AIAS'
--   ;
--
--
--
--Log('insert 2 done  '||SQL%ROWCOUNT);
--commit;
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP2"',
--          estimate_percent => 1
--          );
--
--  execute immediate 'truncate table aia_tmp_comls_step3';
--  insert into aia_tmp_comls_step3
--
--  -- drop table aia_tmp_comls_step3
--  -- create table aia_tmp_comls_step3 as
--   select   pct.creditseq pctCreditSeq,
--   pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq
--   , s1.CREDITSEQ
--,SALESTRANSACTIONSEQ
--,CALCULATION_PERIOD
--,POLICY_NUMBER
--,POLICYIDSEQ
--,LIFE_NUMBER
--,COVERAGE_NUMBER
--,RIDER_NUMBER
--,COMPONENT_CODE
--,COMPONENT_NAME
--,BASE_RIDER_IND
--,TRANSACTION_DATE
--,PAYOR_CODE
--,CREDITTYPE
--,CREDIT_NAME
--,CREDIT_VALUE
--,s1.PERIODSEQ
--,PRODUCT_NAME
--,POLICY_YEAR
--,COMMISSION_RATE
--,PAID_TO_DATE
--
--   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
--   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
--   ,WRI_AGT_CODE
--,QTRYR
--,GD2
--   from cs_pmcredittrace pct
--   join aia_tmp_comls_step1 s1
--   on pct.creditseq=s1.creditseq
--   join aia_tmp_comls_step2 s2
--   on s2.measurementseq=pct.measurementseq and s2.ruleseq=pct.ruleseq
--   --and pct.targetperiodseq=s2.periodseq
--   join cs_depositpmtrace dct
--   on 1=1
--   and dct.measurementseq=pct.measurementseq
--   --and dct.targetperiodseq=s2.periodseq
--   and dct.tenantid='AIAS' and pct.tenantid='AIAS'
--   and dct.processingUnitseq=V_PROCESSINGUNITSEQ and pct.processingUnitseq=V_PROCESSINGUNITSEQ
--
--;
--commit;
--Log('insert 3 done');
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP3"',
--          estimate_percent => 1
--          );
--
--insert into AIA_CB_TRACE_FORWARD
--
--select
--STR_BUNAME as BUNAME,
--       QtrYr as CALCULATION_PERIOD,
--        POLICY_NUMBER,
--        POLICYIDSEQ,
--       mPositionseq PAYEE_SEQ,
--       substr(dep_pos.name, 4) as PAYEE_CODE,
--         PAYOR_CODE,
--        LIFE_NUMBER,
--       COVERAGE_NUMBER,
--        RIDER_NUMBER,
--      COMPONENT_CODE,
--         COMPONENT_NAME,
--         BASE_RIDER_IND,
--       TRANSACTION_DATE,
--       --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as TRANSACTION_DATE,
--       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
--       --clawback type = 'Lumpsum'
--       STR_LUMPSUM as CLAWBACK_TYPE,
--       --clawback name = 'Commission'
--       --STR_CB_NAME            as CLAWBACK_NAME,
--       CLAWBACK_NAME       as CLAWBACK_NAME,
--        CREDITTYPE,
--        CREDITSEQ,
--        CREDIT_NAME,
--         CREDIT_VALUE,
--       measurementseq      as PM_SEQ,
--       mname                as PM_NAME,
--       pctcontribvalue  as PM_CONTRIBUTE_VALUE,
--       1                      as PM_RATE,
--       dep.depositseq         as DEPOSITSEQ,
--       dep.name               as DEPOSIT_NAME,
--       dep.value              as DEPOSIT_VALUE,
--       x.periodseq          as PERIODSEQ,
--       salestransactionseq as SALESTRANSACTIONSEQ,
--        PRODUCT_NAME,
--         POLICY_YEAR,
--         COMMISSION_RATE,
--         PAID_TO_DATE,
--       P_BATCH_NO             as BATCH_NUMBER,
--       sysdate                as CREATED_DATE
--       from aia_tmp_comls_step3 x
--       join cs_deposit dep
--       on dep.depositseq=x.depositseq
--       join cs_position dep_pos
--       on dep.positionseq = dep_pos.ruleelementownerseq
--   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= x.GD2
--   and dep_pos.effectiveenddate > x.GD2
--       and dep_pos.name = x.wri_agt_code
--   where x.qtrYr = V_CAL_PERIOD
--   ;
--
--
--Log('insert into AIA_CB_TRACE_FORWARD' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
----end loop;
--
--elsif P_STR_TYPE = STR_ONGOING then
--    --setup the start date and end date for on-going period
--    if to_date(P_STR_CYCLEDATE , STR_DATE_FORMAT_TYPE) = DT_WEEKLY_END_DATE then
--      DT_ONGOING_START_DATE := trunc(to_date(P_STR_CYCLEDATE , STR_DATE_FORMAT_TYPE),'MONTH');
--      DT_ONGOING_END_DATE := DT_WEEKLY_END_DATE;
--    else
--      DT_ONGOING_START_DATE := trunc(to_date(P_STR_CYCLEDATE , STR_DATE_FORMAT_TYPE),'MONTH');
--    select csp.enddate - 1
--      into DT_ONGOING_END_DATE
--      from cs_period csp
--     inner join cs_periodtype pt
--        on csp.periodtypeseq = pt.periodtypeseq
--     where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
--       and csp.removedate = DT_REMOVEDATE
--       and calendarseq = V_CALENDARSEQ
--       and pt.name = 'month';
--    end if;
--
--Log('DT_ONGOING_START_DATE = ' || DT_ONGOING_START_DATE);
--Log('DT_ONGOING_END_DATE = ' || DT_ONGOING_END_DATE);
--
--select min(periodseq) into vOngoingperiod
--from CS_period where removedate>sysdate and startdate=add_months(last_day(trunc(DT_ONGOING_START_DATE))+1,-1)
--and periodtypeseq=2814749767106561 and calendarseq=2251799813685250
--and removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
--;
--
--select min(periodseq) into vOngoingendperiod
--from CS_period where removedate>sysdate and startdate=add_months(last_day(trunc(DT_ONGOING_END_DATE))+1,-1)
--and periodtypeseq=2814749767106561 and calendarseq=2251799813685250
--and removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
--;
--
--Log('insert into AIA_CB_TRACE_FORWARD, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--
--execute immediate 'truncate table AIA_CB_TRACE_FORWARD_TMP';
--Log('insert 1 started');
-----temp table insert  AIA_CB_TRACE_FORWARD_TMP
--insert /*+ APPEND */  into AIA_CB_TRACE_FORWARD_TMP
--select  /*+ PARALLEL leading(crd) */  null as PAYEE_SEQ,
--null as PAYEE_CODE,
--crd.genericattribute12 as PAYOR_CODE,
--crd.compensationdate as TRANSACTION_DATE,
--ct.credittypeid as CREDITTYPE,
--crd.creditseq as CREDITSEQ,
--crd.name as CREDIT_NAME,
--crd.value as CREDIT_VALUE,
--null as PM_SEQ,
--null as PM_NAME,
--pct.CONTRIBUTIONVALUE as PM_CONTRIBUTE_VALUE,
--crd.periodseq as PERIODSEQ,
--st.salestransactionseq as SALESTRANSACTIONSEQ,
--crd.genericattribute2 as PRODUCT_NAME,
--crd.genericnumber1 as POLICY_YEAR,
--st.genericnumber2 as COMMISSION_RATE,
--st.genericdate4 as PAID_TO_DATE,
--st.GENERICATTRIBUTE29,
--st.PONUMBER,
--st.GENERICATTRIBUTE30,
--st.GENERICATTRIBUTE31,
--st.PRODUCTID,
--crd.genericattribute12,
--null name,
--pct.measurementseq,
--null,
--crd.genericdate2
-- FROM CS_SALESTRANSACTION st
-- inner join CS_CREDIT crd
--on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
----and crd.genericdate2
-- inner join CS_PMCREDITTRACE pct
--on crd.CREDITSEQ = pct.CREDITSEQ
-- inner join CS_CREDITTYPE ct
--on crd.CREDITTYPESEQ = ct.DATATYPESEQ
-- and ct.Removedate = DT_REMOVEDATE
-- inner join cs_businessunit  bu on st.businessunitmap = bu.mask
-- WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
-- and bu.name = STR_BUNAME
-- and st.compensationdate between DT_ONGOING_START_DATE and DT_ONGOING_END_DATE
-- and crd.genericattribute16 not in ('RO', 'RNO')
-- and crd.periodseq between vOngoingperiod and vOngoingendperiod
-- and crd.tenantid='AIAS' and crd.processingunitseq=V_PROCESSINGUNITSEQ
-- and st.tenantid='AIAS' and st.processingunitseq=V_PROCESSINGUNITSEQ
-- and pct.tenantid='AIAS' and pct.processingunitseq=V_PROCESSINGUNITSEQ;
--
-- commit;
--Log('insert 1 ended');
--
--Log('insert 2 started');
--------Main table insert  AIA_CB_TRACE_FORWARD
--insert /*+ APPEND */  into AIA_CB_TRACE_FORWARD
--  select    STR_BUNAME as BUNAME,
--         ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--         ip.ponumber as POLICY_NUMBER,
--         ip.policyidseq as POLICYIDSEQ,
--        pm.positionseq as PAYEE_SEQ,
--         substr(pm_pos.name, 4) as PAYEE_CODE,
--         tmp.PAYOR_CODE,
--         ip.life_number as LIFE_NUMBER,
--         ip.coverage_number as COVERAGE_NUMBER,
--         ip.rider_number as RIDER_NUMBER,
--         ip.component_code as COMPONENT_CODE,
--         ip.component_name as COMPONENT_NAME,
--         ip.base_rider_ind as BASE_RIDER_IND,
--         tmp.TRANSACTION_DATE,
--         --TO_CHAR(DT_CB_START_DATE, 'MON-YYYY') as PROCESSING_PERIOD,
--         TO_CHAR(to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE), 'MON-YYYY') as PROCESSING_PERIOD,
--         --clawback type = 'Lumpsum'
--         STR_ONGOING as CLAWBACK_TYPE,
--         --clawback name = 'Commission'
--         --STR_CB_NAME            as CLAWBACK_NAME,
--         rl.CLAWBACK_NAME as CLAWBACK_NAME,
--         tmp.CREDIT_TYPE,
--         tmp.CREDITSEQ,
--         tmp.CREDIT_NAME,
--         tmp.CREDIT_VALUE,
--        pm.measurementseq as PM_SEQ,
--         pm.name as PM_NAME,
--         tmp.PM_CONTRIBUTION_VALUE,
--         1 as PM_RATE,
--         '' as DEPOSITSEQ,
--         '' as DEPOSIT_NAME,
--         '' as DEPOSIT_VALUE,
--         tmp.PERIODSEQ,
--         tmp.SALESTRANSACTIONSEQ,
--         tmp.PRODUCT_NAME,
--         tmp.POLICY_YEAR,
--         tmp.COMMISSION_RATE,
--         tmp.PAID_TO_DATE,
--         P_BATCH_NO as BATCH_NUMBER,
--         sysdate as CREATED_DATE
--    FROM AIA_CB_TRACE_FORWARD_TMP tmp
--     inner join CS_MEASUREMENT pm
--      on tmp.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
-- inner join CS_POSITION pm_pos
--on pm_pos.ruleelementownerseq =pm.positionseq
--and pm_pos.removedate = DT_REMOVEDATE
-- and pm_pos.effectivestartdate <= tmp.genericdate2
-- and pm_pos.effectiveenddate > tmp.genericdate2
--   inner join AIA_CB_IDENTIFY_POLICY ip
--      on ip.BUNAME = STR_BUNAME
--     AND tmp.PONUMBER = ip.PONUMBER
--     AND tmp.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--     AND tmp.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--     AND tmp.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--     AND tmp.PRODUCTID = ip.COMPONENT_CODE
--     and tmp.genericattribute12 = ip.wri_agt_code
--   inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--                 from AIA_CB_RULES_LOOKUP
--                where RULE_TYPE = 'PM'
--                  AND CLAWBACK_NAME in (STR_COMMISSION, STR_GST_COMMISSION)
--                  and BUNAME=STR_BUNAME ) rl
--      on pm.NAME = rl.SOURCE_RULE_OUTPUT
--   inner join (select distinct
--                      cb_quarter_name,
--                      cb_startdate,
--                      cb_enddate
--                 from aia_cb_period
--                where cb_name = STR_COMMISSION
--                and BUNAME=STR_BUNAME) cbp
--      on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
--   WHERE
--     to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
--     ;
--
--
--Log('insert into AIA_CB_TRACE_FORWARD' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--Log('SP_TRACE_FORWARD_COMMISSION end');
--
--end if;
--
--end  SP_TRACE_FORWARD_COMMISSION;



/* this procedure is for commission clawback calculation*/
--PROCEDURE SP_CLAWBACK_COMMISSION (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO IN INTEGER) as
--
--V_REC_COUNT INTEGER;
--V_BATCH_NO_PRE_QTR INTEGER;
--V_CB_TYPE VARCHAR2(50);
--V_CB_NAME VARCHAR2(50);
--V_CB_QTR VARCHAR2(50);
--begin
--
--Log('SP_CLAWBACK_COMMISSION start');
--
--init;
--
----get records count from AIA_CB_CLAWBACK_COMMISSION
--select count(1)
--  into V_REC_COUNT
--  from AIA_CB_CLAWBACK_COMMISSION
-- where batch_no = P_BATCH_NO;
--
----delete the records in AIA_CB_CLAWBACK_COMMISSION if batch number is being reused.
--if V_REC_COUNT > 0 then
--
--delete from AIA_CB_CLAWBACK_COMMISSION where batch_no = P_BATCH_NO;
--delete from AIA_CB_CLAWBACK_SVI_TMP where batch_no = P_BATCH_NO;
--
--commit;
--
--END IF;
--
--Log('insert into AIA_CB_CLAWBACK_COMMISSION,' ||' batch_no = ' || P_BATCH_NO);
--
----insert data into AIA_CB_CLAWBACK_COMMISSION for commission
--insert into AIA_CB_CLAWBACK_COMMISSION
--  select -- RULE*/
--  /*+  leading(tf,ip,ba,st,cr) use_nl(tf,ip,ba,st,cr) NO_PARALLEL index(ST AIA_CS_SALESTRANSACTION_SEQ) index(CR OD_CREDIT_CREDITSEQ) */
--  tf.calculation_period as MEASUREMENT_QUARTER,
--         tf.clawback_type as CLAWBACK_TYPE,
--         tf.clawback_name as CLAWBACK_NAME,
--         --tf.processing_period as CALCULATION_DATE,
--         to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
--         pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE,
--         trim(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME,
--         pos_dis.genericattribute2 as WRI_DM_CODE,
--         substr(pos_agy.name, 4) as WRI_AGY_CODE,
--         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME,
--         pos_agt.GENERICATTRIBUTE2 as WRI_AGY_LDR_CODE,
--         pos_agt.genericattribute7 as WRI_AGY_LDR_NAME,
--         tf.payor_code as WRI_AGT_CODE,
--         trim(par_agt.firstname||' '||par_agt.lastname) as WRI_AGT_NAME,
--         --'Normal FSC' as FSC_TYPE,
--         decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
--         title_agt.name as RANK,
--         cr.genericattribute14 as CLASS,
--         pos_agt.genericattribute4 as UM_CLASS,
--         ba.bsc_grade as FSC_BSC_GRADE,
--         ba.entitlementpercent as FSC_BSC_PERCENTAGE,
--         tf.policy_number as PONUMBER,
--         tf.LIFE_NUMBER as LIFE_NUMBER,
--         tf.COVERAGE_NUMBER as COVERAGE_NUMBER,
--         tf.RIDER_NUMBER as RIDER_NUMBER,
--         tf.component_code as COMPONENT_CODE,
--         tf.product_name as PRODUCT_NAME,
--         tf.transaction_date as TRANSACTION_DATE,
--         tf.policy_year as POLICY_YEAR,
--         case
--           when tf.credit_type = 'FYC' then
--            tf.credit_value
--           else
--            0
--         end as FYC,
--         case
--           when tf.credit_type = 'API' then
--            tf.credit_value
--           else
--            0
--         end as API,
--         case
--           when tf.credit_type = 'SSCP' then
--            tf.credit_value
--           else
--            0
--         end as SSC,
--         case
--           when tf.credit_type = 'RYC' then
--            tf.credit_value
--           else
--            0
--         end as RYC,
--         /**
--         --for Commission only
--         --if SVI is a negative value, then check if this component exist in last quarter clawback result,
--         --if exist and clawback value is negative, then continue, else skip(set figure=0).
--         **/
--         (tf.pm_contribution_value * INT_SVI_RATE) as SVI,
--         (tf.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent as ENTITLEMENT,
--         /** SVI - ENTITLEMENT */
--         --fix the rounding issue
--         round(
--         ((tf.pm_contribution_value * INT_SVI_RATE) -
--         (tf.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent) * (-1)
--         ,2)
--         as CLAWBACK_VALUE,
--         0 as PROCESSED_CLAWBACK,
--         --0 as BASIC_RIDER_IND,
--         tf.base_rider_ind as BASE_RIDER_IND,
--         tf.salestransactionseq,
--         tf.creditseq,
--         tf.pm_seq,
--         P_BATCH_NO,
--         0 as OFFSET_CLAWBACK
--    from AIA_CB_TRACE_FORWARD tf
--   inner join aia_cb_identify_policy ip
--   on tf.policyidseq = ip.policyidseq
--   inner join aia_cb_bsc_agent ba
--      on tf.calculation_period = (ba.quarter || ' ' || ba.year)
--     and tf.payor_code = ba.agentcode
--   inner join cs_salestransaction st
--      on tf.salestransactionseq = st.salestransactionseq
--   inner join CS_CREDIT cr
--      on tf.creditseq = cr.creditseq and cr.processingUnitseq=V_PROCESSINGUNITSEQ
--   --for writing Agency postion info
--   inner join cs_position pos_agy
--        on pos_agy.name = 'SGY' || ip.wri_agy_code
--        AND pos_agy.removedate = DT_REMOVEDATE
--        AND pos_agy.effectivestartdate <= cr.genericdate2
--        AND pos_agy.effectiveenddate   >  cr.genericdate2
--     --for writing Agency participant info
--     inner join cs_participant par_agy
--        on par_agy.PAYEESEQ = pos_agy.PAYEESEQ
--        AND par_agy.effectivestartdate <= cr.genericdate2
--        AND par_agy.effectiveenddate   >  cr.genericdate2
--        AND par_agy.removedate = DT_REMOVEDATE
--      --for writing District postion info
--     inner join cs_position pos_dis
--        on pos_dis.name= 'SGY' || pos_agy.genericattribute3
--        AND pos_dis.effectivestartdate <= cr.genericdate2
--        AND pos_dis.effectiveenddate   > cr.genericdate2
--        AND pos_dis.removedate = DT_REMOVEDATE
--     --for writing District participant info
--     inner join cs_participant par_dis
--        on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
--        AND par_dis.effectivestartdate <= cr.genericdate2
--        AND par_dis.effectiveenddate  > cr.genericdate2
--        AND par_dis.removedate = DT_REMOVEDATE
--     --for writing Agent postion info
--      inner join cs_position pos_agt
--        on 'SGT'||cr.genericattribute12=pos_agt.name
--        and pos_agt.effectivestartdate <= cr.genericdate2
--        AND pos_agt.effectiveenddate   > cr.genericdate2
--        and pos_agt.removedate = DT_REMOVEDATE
--        and POS_AGY.GENERICATTRIBUTE6='AGY'
--     --for writing Agent participant info
--     inner join cs_participant par_agt
--     on par_agt.payeeseq= pos_agt.PAYEESEQ
--     AND par_agt.effectivestartdate <= cr.genericdate2
--     AND par_agt.effectiveenddate  > cr.genericdate2
--     AND par_agt.removedate = DT_REMOVEDATE
--     --for payor agent title info
--     inner join cs_title title_agt
--     on title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
--     AND title_agt.effectivestartdate <= cr.genericdate2
--     AND title_agt.effectiveenddate   > cr.genericdate2
--     AND title_agt.REMOVEDATE = DT_REMOVEDATE
--   where tf.clawback_name in (STR_COMMISSION, STR_GST_COMMISSION)
--   and tf.batch_number = P_BATCH_NO
--           --     and st.tenantid = 'AIAS'
--           --  and cr.tenantid = 'AIAS'
--             and pos_agy.tenantid  = 'AIAS'
--             and pos_dis.tenantid  = 'AIAS'
--             and pos_agt.tenantid  = 'AIAS'
--             and par_agt.tenantid  = 'AIAS'
--          and title_agt.tenantid  = 'AIAS'
--          and  par_agy.tenantid  = 'AIAS'
--             and par_dis.tenantid  = 'AIAS'
--             and tf.BUNAME=STR_BUNAME
--             and ip.BUNAME=STR_BUNAME
--
--   ;
--
--Log('insert into AIA_CB_CLAWBACK_COMMISSION' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--/**
--the below logic is to check the clawback policy has the negative SVI value in current measurement quarter.
--if yes, need to trace the same policy's clawback value of last quarter,
--  if figure < 0, continue
--  else if figure > 0, set current month clawback value = 0
--end
--**/
--
----get clawback type and clawback name, only LUMPSUM case will apply this logic
--V_CB_TYPE := fn_get_cb_type(P_BATCH_NO);
----V_CB_NAME := fn_get_cb_name(P_BATCH_NO);
--V_CB_QTR := fn_get_cb_quarter(P_BATCH_NO);
--
--
--if V_CB_TYPE = STR_LUMPSUM then
--   --get previous quarter batch number
--    --V_BATCH_NO_PRE_QTR := fn_get_batch_no_pre_qtr(P_BATCH_NO);
--
--insert into AIA_CB_CLAWBACK_SVI_TMP
--select curr_cc.*, P_BATCH_NO from
--(select wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name,
--  sum(clawback) as clawback
--  from
--AIA_CB_CLAWBACK_COMMISSION
--where clawback_type = STR_LUMPSUM
-- and clawback_name = STR_COMMISSION
-- and batch_no = P_BATCH_NO
--group by wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name
--  having sum(clawback) > 0
--) curr_cc
--left join
--(select cc.wri_dist_code,
--       cc.wri_agy_code,
--       cc.wri_agt_code,
--       cc.ponumber,
--       cc.life_number,
--       cc.coverage_number,
--       cc.rider_number,
--       cc.component_code,
--       cc.product_name,
--       --processed_clawback value should be updated after pipeline compeleted
--       sum(cc.processed_clawback) as processed_clawback
--  from AIA_CB_CLAWBACK_COMMISSION cc
-- inner join (select nvl(max(t.batchnum), 0) as batch_no
--               from aia_cb_batch_status t
--               inner join (select distinct quarter, year, cb_startdate, cb_enddate
--               from aia_cb_period
--              where cb_name = STR_COMMISSION
--              and BUNAME=STR_BUNAME
--              ) cbp
--              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
--              where t.islatest = 'Y'
--                and t.status = STR_STATUS_COMPLETED_SH
--                and t.clawbackname = STR_COMMISSION
--                and t.clawbacktype = STR_LUMPSUM
--                and t.cb_quarter_name <> V_CB_QTR
--                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
--              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype) pre_batch
-- on cc.batch_no = pre_batch.batch_no
-- where cc.clawback_type = STR_LUMPSUM
--   and cc.clawback_name = STR_COMMISSION
-- group by cc.wri_dist_code,
--          cc.wri_agy_code,
--          cc.wri_agt_code,
--          cc.ponumber,
--          cc.life_number,
--          cc.coverage_number,
--          cc.rider_number,
--          cc.component_code,
--          cc.product_name
--having sum(cc.processed_clawback) < 0) pre_cc
-- on curr_cc.wri_dist_code = pre_cc.wri_dist_code
-- and curr_cc.wri_agy_code = pre_cc.wri_agy_code
-- and curr_cc.wri_agt_code = pre_cc.wri_agt_code
-- and curr_cc.ponumber = pre_cc.ponumber
-- and curr_cc.life_number = pre_cc.life_number
-- and curr_cc.coverage_number = pre_cc.coverage_number
-- and curr_cc.rider_number = pre_cc.rider_number
-- and curr_cc.component_code = pre_cc.component_code
-- and curr_cc.product_name = pre_cc.product_name
-- where pre_cc.ponumber is null;
--
--Log('insert into AIA_CB_CLAWBACK_SVI_TMP' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--elsif V_CB_TYPE = STR_ONGOING then
--
--insert into AIA_CB_CLAWBACK_SVI_TMP
--select curr_cc.*, P_BATCH_NO from
--(select wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name,
--  sum(clawback) as clawback
--  from
--AIA_CB_CLAWBACK_COMMISSION
--where clawback_type = STR_ONGOING
-- and clawback_name = STR_COMMISSION
-- and batch_no = P_BATCH_NO
--group by wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name
--  having sum(clawback) > 0
--) curr_cc
--left join
--(select cc.wri_dist_code,
--       cc.wri_agy_code,
--       cc.wri_agt_code,
--       cc.ponumber,
--       cc.life_number,
--       cc.coverage_number,
--       cc.rider_number,
--       cc.component_code,
--       cc.product_name,
--       --processed_clawback value should be updated after pipeline compeleted
--       sum(cc.processed_clawback) as processed_clawback
--  from AIA_CB_CLAWBACK_COMMISSION cc
-- inner join (
-- --lumpsum batch number
-- select nvl(max(t.batchnum), 0) as batch_no
--               from aia_cb_batch_status t
--               inner join (select distinct quarter, year, cb_startdate, cb_enddate
--               from aia_cb_period
--              where cb_name = STR_COMMISSION
--              and BUNAME=STR_BUNAME
--              ) cbp
--              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
--              where t.islatest = 'Y'
--              and t.BUNAME=STR_BUNAME
--                and t.status = STR_STATUS_COMPLETED_SH
--                and t.clawbackname = STR_COMMISSION
--                and t.clawbacktype = STR_LUMPSUM
--                and t.cb_quarter_name <> V_CB_QTR
--                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
--                and T.BUNAME=STR_BUNAME
--              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype
--              union
--  --on-going batch number
--              select nvl(max(t.batchnum), 0) as batch_no
--               from aia_cb_batch_status t
--              where t.islatest = 'Y'
--                and t.status = STR_STATUS_COMPLETED_SH --'completed_sh'
--                and t.clawbackname = STR_COMMISSION--'COMMISSION'
--                and t.clawbacktype = STR_ONGOING --'ONGOING'
--                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) > t.cycledate
--                and t.BUNAME=STR_BUNAME
--              ) pre_batch
-- on cc.batch_no = pre_batch.batch_no
-- where cc.clawback_name = STR_COMMISSION
-- group by cc.wri_dist_code,
--          cc.wri_agy_code,
--          cc.wri_agt_code,
--          cc.ponumber,
--          cc.life_number,
--          cc.coverage_number,
--          cc.rider_number,
--          cc.component_code,
--          cc.product_name
--having sum(cc.processed_clawback) < 0) pre_cc
-- on curr_cc.wri_dist_code = pre_cc.wri_dist_code
-- and curr_cc.wri_agy_code = pre_cc.wri_agy_code
-- and curr_cc.wri_agt_code = pre_cc.wri_agt_code
-- and curr_cc.ponumber = pre_cc.ponumber
-- and curr_cc.life_number = pre_cc.life_number
-- and curr_cc.coverage_number = pre_cc.coverage_number
-- and curr_cc.rider_number = pre_cc.rider_number
-- and curr_cc.component_code = pre_cc.component_code
-- and curr_cc.product_name = pre_cc.product_name
-- where pre_cc.ponumber is null;
--
--Log('insert into AIA_CB_CLAWBACK_SVI_TMP' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--end if;
--
----update the table AIA_CB_CLAWBACK_COMMISSION for special handling for positive clawback
--merge into AIA_CB_CLAWBACK_COMMISSION cc
--using AIA_CB_CLAWBACK_SVI_TMP st
--on (cc.wri_dist_code = st.wri_dist_code
-- and cc.wri_agy_code = st.wri_agy_code
-- and cc.wri_agt_code = st.wri_agt_code
-- and cc.ponumber = st.ponumber
-- and cc.life_number = st.life_number
-- and cc.coverage_number = st.coverage_number
-- and cc.rider_number = st.rider_number
-- and cc.component_code = st.component_code
-- and cc.product_name = st.product_name
-- and cc.batch_no = st.batch_no
-- and cc.batch_no = P_BATCH_NO
--)
--when matched then update set cc.clawback = 0;
--
--Log('merge into AIA_CB_CLAWBACK_COMMISSION' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--Log('SP_CLAWBACK_COMMISSION end');
--
--end SP_CLAWBACK_COMMISSION;




--PROCEDURE SP_TRACE_FORWARD_COMP(P_STR_CYCLEDATE IN VARCHAR2, P_STR_TYPE IN VARCHAR2, P_BATCH_NO IN INTEGER) AS
--STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION';
--V_CAL_PERIOD VARCHAR2(30); --measurement quarter
--DT_CB_START_DATE DATE;
--DT_CB_END_DATE DATE;
--DT_INCEPTION_START_DATE DATE;
--DT_INCEPTION_END_DATE DATE;
--NUM_OF_CYCLE_IND integer;
--RECORD_CNT_ONGOING integer;
--ts_periodseq integer;
--V_NADOR_RATE NUMBER(10,2);
--V_NLPI_RATE NUMBER(10,2);
---- define period seq of each month
--TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
--t_periodseq periodseq_type;
--ONGOING_ST_DT DATE;
--ONGOING_END_DT DATE;
--ONGOING_PERIOD VARCHAR2(50);
--
--begin
--
--init;
--
----update status
--sp_update_batch_status (P_BATCH_NO,'processing');
--
--Log('SP_TRACE_FORWARD_COMP start');
--
----Get the periodseq for Ongoing period
--if P_STR_TYPE = STR_ONGOING then
--
--select count(1)
--into RECORD_CNT_ONGOING
--from cs_period csp
-- inner join cs_periodtype pt
--    on csp.periodtypeseq = pt.periodtypeseq
-- where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
--   and csp.removedate = DT_REMOVEDATE
--   and csp.calendarseq = V_CALENDARSEQ
--   and pt.name = STR_CALENDAR_TYPE;
--
--    if RECORD_CNT_ONGOING = 0 then
--   goto ProcDone;
--   END IF;
--
--select csp.periodseq,
--csp.startdate,
--csp.enddate-1,
--csp.name
--into ts_periodseq,
--ONGOING_ST_DT,
--ONGOING_END_DT,
--ONGOING_PERIOD
--from cs_period csp
-- inner join cs_periodtype pt
--    on csp.periodtypeseq = pt.periodtypeseq
-- where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
--   and csp.removedate = DT_REMOVEDATE
--   and csp.calendarseq = V_CALENDARSEQ
--   and pt.name = STR_CALENDAR_TYPE;
--
--Log('DT_ONGOING_START_DATE = ' || ONGOING_ST_DT);
--Log('DT_ONGOING_END_DATE = ' || ONGOING_END_DT);
--
----delete from AIA_CB_TRACE_FORWARD_COMP where CLAWBACK_TYPE= P_STR_TYPE and  transaction_date between ONGOING_ST_DT and ONGOING_END_DT;
--   --and CLAWBACK_NAME not in (STR_COMMISSION, STR_GST_COMMISSION);
--
----commit;
--
--else
--
--select count(1)
--  into NUM_OF_CYCLE_IND
--  from AIA_CB_PERIOD cbp
-- where cbp.CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_CB_NAME
-- and CBP.BUNAME=STR_BUNAME; -- add cb_name here
--
--   if NUM_OF_CYCLE_IND = 0 then
--   goto ProcDone;
--   END IF;
--
-- --get calculation period name, clawback start date and end date for lumpsum compensation
--select cbp.quarter || ' ' || cbp.year,
--       cbp.cb_startdate,
--       cbp.cb_enddate,
--       cbp.inception_startdate,
--       cbp.inception_enddate
--  into V_CAL_PERIOD,
--       DT_CB_START_DATE,
--       DT_CB_END_DATE,
--       DT_INCEPTION_START_DATE,
--       DT_INCEPTION_END_DATE
--  from AIA_CB_PERIOD cbp
-- where CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_CB_NAME
-- and CBP.BUNAME=STR_BUNAME;
--
--Log('DT_LUMPSUM_START_DATE = ' || DT_CB_START_DATE);
--Log('DT_LUMPSUM_END_DATE = ' || DT_CB_END_DATE);
--
--
--
--
---- Get the periodseqs for lumpsum period
-- select
--periodseq BULK COLLECT into t_periodseq
--from cs_period csp
-- inner join cs_periodtype pt
--    on csp.periodtypeseq = pt.periodtypeseq
-- where csp.startdate >= DT_CB_START_DATE
--and csp.enddate <= DT_CB_END_DATE + 1
--   and csp.removedate = DT_REMOVEDATE
--   and csp.calendarseq = V_CALENDARSEQ
--   and pt.name = STR_CALENDAR_TYPE;
--
--
--execute immediate 'truncate table aia_tmp_comls_period';
--insert into aia_tmp_comls_period
--select periodseq
--from cs_period csp
-- inner join cs_periodtype pt
--    on csp.periodtypeseq = pt.periodtypeseq
-- where csp.startdate >= DT_CB_START_DATE
--and csp.enddate <= DT_CB_END_DATE + 1
--   and csp.removedate = DT_REMOVEDATE
--   and csp.calendarseq = V_CALENDARSEQ
--   and pt.name = STR_CALENDAR_TYPE;
--commit;
----delete from AIA_CB_TRACE_FORWARD_COMP where CLAWBACK_TYPE= P_STR_TYPE and CALCULATION_PERIOD=V_CAL_PERIOD; --and CLAWBACK_NAME not in (STR_COMMISSION, STR_GST_COMMISSION);
----commit;
--
--end if;
--
--   select value into V_NADOR_RATE
--from CS_FIXEDVALUE fv where
--name='FV_NADOR_Payout_Rate'
--and Removedate = DT_REMOVEDATE;
--
-- select value into V_NLPI_RATE
--from CS_FIXEDVALUE fv where
--name='FV_NLPI_RATE'
--and Removedate = DT_REMOVEDATE;
--
--if P_STR_TYPE = STR_LUMPSUM  then
--
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for FYO,RYO,FSM_RYO,NLPI' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--
----for i in 1..t_periodseq.count loop
--
--Log('AIA_CB_TRACE_FORWARD_COMP '|| ' '||V_CAL_PERIOD);
----for lumpsum compensation trace forward for 'FYO','RYO','FSM_RYO','NLPI'
--
--
--
-- execute immediate 'truncate table aia_tmp_Comls_Step0_1';
-- insert into  aia_tmp_Comls_Step0_1
--
-- select /*+ leading(ip,st) */ st.salestransactionseq,  ip.wri_agt_code as wri_agt_code_ORIG,
--  ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--        ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--       st.genericnumber2      as COMMISSION_RATE,
--       st.genericdate4        as PAID_TO_DATE
--       ,'SGT'||ip.wri_agt_code wri_agt_code
--       ,ip.quarter || ' ' || ip.year qtrYr
--
-- from cs_Salestransaction st
-- INNER JOIN AIA_CB_IDENTIFY_POLICY IP
--ON 1                              =1
--AND IP.BUNAME                     = STR_BUNAME
--AND ST.PONUMBER                   = IP.PONUMBER
--AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
--AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
--AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER
--and st.productid=ip.component_CODE
-- where st.tenantid='AIAS'
--and st.processingUnitseq=V_PROCESSINGUNITSEQ
--and st.eventtypeseq <> 16607023625933358
----and st.compensationdate between '1-mar-2017' and '31-may-2017'
----and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
--
----AND ST.PRODUCTID                  = IP.COMPONENT_CODE
--;
--
----Add for AI transaction NL20180308
-- insert into  aia_tmp_Comls_Step0_1
--with AMR as (select row_number() over(partition by t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE ,t1.POLICY_INCEPTION_DATE order by t1.component_CODE) as rn,
--             t1.* from AI_MONTHLY_REPORT t1 where t1.AI_PAYMENT<> 0),
--     st as (select row_number() over(partition by t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11 ,t2.GENERICDATE2 order by t2.PRODUCTID) as rn,
--            t2.* from cs_Salestransaction t2,cs_businessunit bu  where t2.tenantid='AIAS' and t2.businessunitmap = bu.mask
--          and bu.name = STR_BUNAME and t2.processingUnitseq=V_PROCESSINGUNITSEQ and t2.eventtypeseq = 16607023625933358 ),
--     IP as (select row_number() over(partition by t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date order by t3.coverage_number ) as rn,
--            t3.* from AIA_CB_IDENTIFY_POLICY t3 where t3.BUNAME  = STR_BUNAME)
-- select /*+ PARALLEL */ st.salestransactionseq,  ip.wri_agt_code as wri_agt_code_ORIG,
--  ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--        ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--       st.genericnumber2      as COMMISSION_RATE,
--       st.genericdate4        as PAID_TO_DATE
--       ,'SGT'||ip.wri_agt_code wri_agt_code
--       ,ip.quarter || ' ' || ip.year qtrYr
-- from  st
-- INNER JOIN  AMR
--ON  st.PONUMBER = AMR.PONUMBER
--AND st.VALUE = AMR.AI_PAYMENT
--AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
--AND st.GENERICATTRIBUTE11 = AMR.PAYEE_CODE
--AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
--AND st.rn = AMR.rn
-- INNER JOIN IP
--ON 1                              =1
--AND IP.BUNAME                     = STR_BUNAME
--AND AMR.PONUMBER                   = IP.PONUMBER
--and AMR.PAYEE_CODE = IP.WRI_AGT_CODE
--and AMR.component_CODE=ip.component_CODE
--and AMR.policy_inception_date = ip.inception_date
--and AMR.risk_commencement_date = ip.risk_commencement_date
--and AMR.rn = IP.rn
--;
--
--Log('insert 0_1 done '||SQL%ROWCOUNT);
--commit;
--
--execute immediate 'TRUNCATE table aia_tmp_comls_step1_1';
--insert into aia_tmp_comls_step1_1
--select /*+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ ) */  crd.creditseq,
--       crd.salestransactionseq ,
--        ip.CALCULATION_PERIOD,
--      ip.POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--        ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--        crd.compensationdate as TRANSACTION_DATE,
--         crd.genericattribute12 as PAYOR_CODE,
--         ct.credittypeid        as CREDITTYPE,
--       crd.name               as CREDIT_NAME,
--       crd.value              as CREDIT_VALUE,
--        crd.periodseq          as PERIODSEQ,
--         crd.genericattribute2  as PRODUCT_NAME,
--       crd.genericnumber1     as POLICY_YEAR,
--       ip.COMMISSION_RATE,
--       ip.PAID_TO_DATE
--       ,ip.wri_agt_code
--       ,ip.qtrYr, crd.genericdate2
--   ,crd.genericattribute13  ,crd.genericattribute14, crd.positionseq, crd.ruleseq
--  from cs_Credit crd
--  join aia_tmp_comls_period p
--  on crd.periodseq=p.periodseq
--  inner join CS_CREDITTYPE ct
--    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--   and ct.Removedate >sysdate
--  inner join aia_tmp_comls_step0_1 ip
--    on 1=1
--    and ip.salestransactionseq = crd.salestransactionseq
--     and crd.genericattribute12 = ip.wri_agt_code_orig
--   and ip.CALCULATION_PERIOD = V_CAL_PERIOD
--   --where crd.genericattribute16 not in ('RO', 'RNO')
--   where crd.tenantid = 'AIAS'
--   and crd.processingunitseq = V_PROCESSINGUNITSEQ
--
--;
--
--
--
--Log('insert 1_1 done '||SQL%ROWCOUNT);
--
----delete from AIA_TMP_COMLS_STEP1_1 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;
--
--
--Log('delete 1_1 done '||SQL%ROWCOUNT);
--commit;
--
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP1_1"',
--          estimate_percent => 1
--          );
--
--
--execute immediate 'truncate table aia_tmp_comls_step2_1';
--insert into aia_tmp_comls_step2_1
--
--select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , null as clawback_name
--from cs_measurement m
--join aia_tmp_comls_period p
--  on m.periodseq=p.periodseq
--   inner join (select distinct SOURCE_RULE_OUTPUT
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'PM'
--              --added by suresh
--              --add AI NL20180308
--                AND CLAWBACK_NAME  IN ('FYO','NEW_FYO','RYO','NEW_RYO','FSM_RYO','NLPI','AI'))pmr
--                --end by Suresh
--    on pmr.SOURCE_RULE_OUTPUT = m.name
--
--  where  m.processingunitseq = V_PROCESSINGUNITSEQ
--  and m.tenantid='AIAS'
--   ;
--
--
--
--Log('insert 2_1 done '||SQL%ROWCOUNT);
--commit;
--
--
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP2_1"',
--          estimate_percent => 1
--          );
--
--  execute immediate 'truncate table aia_tmp_comls_step3_1';
--  insert into aia_tmp_comls_step3_1
--   select   pct.creditseq pctCreditSeq,
--   pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq
--   , s1.CREDITSEQ
--,SALESTRANSACTIONSEQ
--,CALCULATION_PERIOD
--,POLICY_NUMBER
--,POLICYIDSEQ
--,LIFE_NUMBER
--,COVERAGE_NUMBER
--,RIDER_NUMBER
--,COMPONENT_CODE
--,COMPONENT_NAME
--,BASE_RIDER_IND
--,TRANSACTION_DATE
--,PAYOR_CODE
--,CREDITTYPE
--,CREDIT_NAME
--,CREDIT_VALUE
--,s1.PERIODSEQ
--,PRODUCT_NAME
--,POLICY_YEAR
--,COMMISSION_RATE
--,PAID_TO_DATE
--
--   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
--   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
--   ,WRI_AGT_CODE
--,QTRYR
--,GD2
--,         pct.contributionvalue  as CONTRIBUTIONVALUE , s1.genericattribute13
-- ,s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
--   from cs_pmcredittrace pct
--   join aia_tmp_comls_step1_1 s1
--   on pct.creditseq=s1.creditseq
--   join aia_tmp_comls_step2_1 s2
--   on s2.measurementseq=pct.measurementseq and s2.ruleseq=pct.ruleseq
--   --and pct.targetperiodseq=s2.periodseq
--
--   inner join  CS_PMSELFTRACE pmslf
--   on  s2.measurementseq = pmslf.sourcemeasurementseq
--    --     and pmslf.targetperiodseq=s2.periodseq
--inner join CS_INCENTIVEPMTRACE inpm
--   on pmslf.TARGETMEASUREMENTSEQ = inpm.MEASUREMENTSEQ
--   --and pmslf.targetperiodseq=s2.periodseq
--
--inner join /*CS_DEPOSITINCENTIVETRACE*/ (select * from cs_depositincentivetrace union select * from aias_depositincentivetrace) dct
--   on inpm.incentiveseq = dct.incentiveseq
--   --and dct.targetperiodseq=s2.periodseq
--
--   and dct.targetperiodseq=s2.periodseq
--   and dct.tenantid='AIAS' and pct.tenantid='AIAS' and pmslf.tenantid='AIAS'  and inpm.tenantid='AIAS'
--   and dct.processingUnitseq=V_PROCESSINGUNITSEQ and pct.processingUnitseq=V_PROCESSINGUNITSEQ
--   and pmslf.processingUnitseq=V_PROCESSINGUNITSEQ and inpm.processingUnitseq=V_PROCESSINGUNITSEQ
--
--;
----add AI NL20180308
-- insert into aia_tmp_comls_step3_1
--   select   pct.creditseq pctCreditSeq,
--   pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq
--   , s1.CREDITSEQ
--,SALESTRANSACTIONSEQ
--,CALCULATION_PERIOD
--,POLICY_NUMBER
--,POLICYIDSEQ
--,LIFE_NUMBER
--,COVERAGE_NUMBER
--,RIDER_NUMBER
--,COMPONENT_CODE
--,COMPONENT_NAME
--,BASE_RIDER_IND
--,TRANSACTION_DATE
--,PAYOR_CODE
--,CREDITTYPE
--,CREDIT_NAME
--,CREDIT_VALUE
--,s1.PERIODSEQ
--,PRODUCT_NAME
--,POLICY_YEAR
--,COMMISSION_RATE
--,PAID_TO_DATE
--
--   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
--   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
--   ,WRI_AGT_CODE
--,QTRYR
--,GD2
--,         pct.contributionvalue  as CONTRIBUTIONVALUE , s1.genericattribute13
-- ,s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
--   from cs_pmcredittrace pct
--   join aia_tmp_comls_step1_1 s1
--   on pct.creditseq=s1.creditseq
--   join aia_tmp_comls_step2_1 s2
--   on s2.measurementseq=pct.measurementseq and s2.ruleseq=pct.ruleseq
--   --and pct.targetperiodseq=s2.periodseq
--    --     and pmslf.targetperiodseq=s2.periodseq
--inner join CS_INCENTIVEPMTRACE inpm
--   on pct.measurementseq = inpm.MEASUREMENTSEQ
--   --and pmslf.targetperiodseq=s2.periodseq
--
--inner join /*CS_DEPOSITINCENTIVETRACE*/ (select * from cs_depositincentivetrace union select * from aias_depositincentivetrace) dct
--   on inpm.incentiveseq = dct.incentiveseq
--   --and dct.targetperiodseq=s2.periodseq
--
--   and dct.targetperiodseq=s2.periodseq
--   and dct.tenantid='AIAS' and pct.tenantid='AIAS'  and inpm.tenantid='AIAS'
--   and dct.processingUnitseq=V_PROCESSINGUNITSEQ and pct.processingUnitseq=V_PROCESSINGUNITSEQ
--   and inpm.processingUnitseq=V_PROCESSINGUNITSEQ
--
--;
--
--Log('insert 3 done '||SQL%ROWCOUNT);
--commit;
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP3_1"',
--          estimate_percent => 1
--          );
--
--
--
--insert into AIA_CB_TRACE_FORWARD_COMP
--select STR_BUNAME as BUNAME,
--       QtrYr as CALCULATION_PERIOD,
--       POLICY_NUMBER,
--        POLICYIDSEQ,
--       mPositionSeq PAYEE_SEQ,
--       substr(dep_pos.name, 4) as PAYEE_CODE,
--     PAYOR_CODE,
--     LIFE_NUMBER,
--      COVERAGE_NUMBER,
--       RIDER_NUMBER,
--        COMPONENT_CODE,
--         COMPONENT_NAME,
--         BASE_RIDER_IND,
--         TRANSACTION_DATE,
--       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
--       STR_LUMPSUM as CLAWBACK_TYPE,
--          rl.CLAWBACK_NAME,
--        STR_CB_NAME as CLAWBACK_METHOD,
--        CREDITTYPE,
--      CREDITSEQ,
--        CREDIT_NAME,
--        CREDIT_VALUE,
--       crd_positionseq as crd_positionseq,
--       GD2 as crd_genericdate2,
--       crd_ruleseq as crd_ruleseq,
--       measurementseq      as PM_SEQ,
--       mname                as PM_NAME,
--        case rl.CLAWBACK_NAME
--       when 'NLPI' then x.contributionvalue*V_NLPI_RATE
--       else
--       x.contributionvalue
--       end as PM_CONTRIBUTION_VALUE,
--       case rl.CLAWBACK_NAME
--         when 'FYO' then fyo_rate.value
--         when 'NEW_FYO' then new_fyo_rate.value
--         when 'RYO' then ryo_rate.value
--         when 'NEW_RYO' then new_ryo_rate.value
--         when 'FSM_RYO' then ryo_rate.value
--         when 'NLPI' then V_NLPI_RATE
--       else 1
--         end as PM_RATE,
--       dep.depositseq         as DEPOSITSEQ,
--       /*dep.name*/ replace(dep.name,'_MANUAL','')            as DEPOSIT_NAME,
--       dep.value              as DEPOSIT_VALUE,
--       x.periodseq          as PERIODSEQ,
--       x.salestransactionseq as SALESTRANSACTIONSEQ,
--        PRODUCT_NAME,
--         POLICY_YEAR,
--         COMMISSION_RATE,
--         PAID_TO_DATE,
--       P_BATCH_NO             as BATCH_NUMBER,
--       sysdate                as CREATED_DATE
--
--
--
--
-- from aia_tmp_comls_step3_1 x
--       join cs_deposit dep
--       on dep.depositseq=x.depositseq
--       join cs_position dep_pos
--       on dep.positionseq = dep_pos.ruleelementownerseq
--   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= x.GD2
--   and dep_pos.effectiveenddate > x.GD2
--       --and dep_pos.name = x.wri_agt_code
--        inner join cs_title dep_title
-- on dep_pos.titleseq = dep_title.ruleelementownerseq
-- and dep_title.removedate = DT_REMOVEDATE
-- and dep_title.effectivestartdate <= GD2
-- and dep_title.effectiveenddate > GD2
--        inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'DR'
----Changed by suresh
----Add for AI NL20180308
--             AND CLAWBACK_NAME  IN ('FYO','NEW_FYO','RYO','NEW_RYO','FSM_RYO','NLPI','AI')) rl
----end by suresh
--    on /*dep.NAME*/ replace(dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT
-- left join vw_lt_fyo_rate fyo_rate
-- on fyo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
--   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
--   and fyo_rate.Receiver_title = dep_title.name
--   and rl.CLAWBACK_NAME = 'FYO'
-- --for lookup PM rate for RYO
-- left join vw_lt_ryo_life_rate ryo_rate
-- on ryo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
-- and ryo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
-- and ryo_rate.Receiver_title = dep_title.name
-- and rl.CLAWBACK_NAME in ( 'RYO','FSM_RYO')
-- --Added by Suresh
--  --for lookup PM rate for New FYO
-- left join vw_lt_new_fyo_rate new_fyo_rate
-- on new_fyo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
-- and new_fyo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
-- and new_fyo_rate.Receiver_title = dep_title.name
-- and rl.CLAWBACK_NAME = 'NEW_FYO'
--  --for lookup PM rate for New RYO
-- left join VW_LT_NEW_RYO_LIFE_RATE new_ryo_rate
-- on new_ryo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
--   and new_ryo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
--   and new_ryo_rate.Receiver_title = dep_title.name
--   and rl.CLAWBACK_NAME = 'NEW_RYO'
----end by Suresh
-- --  where x.qtrYr = V_CAL_PERIOD
--   ;
--
--
--
--
--
--
--
--/*
--
--
--
--insert into AIA_CB_TRACE_FORWARD_COMP
--select STR_BUNAME as BUNAME,
--       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--       pm.positionseq as PAYEE_SEQ,
--       substr(dep_pos.name, 4) as PAYEE_CODE,
--       crd.genericattribute12 as PAYOR_CODE,
--       ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--       crd.compensationdate as TRANSACTION_DATE,
--       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
--       STR_LUMPSUM as CLAWBACK_TYPE,
--        rl.CLAWBACK_NAME       as CLAWBACK_NAME,
--        STR_CB_NAME as CLAWBACK_METHOD,
--       ct.credittypeid        as CREDIT_TYPE,
--       crd.creditseq          as CREDITSEQ,
--       crd.name               as CREDIT_NAME,
--       crd.value              as CREDIT_VALUE,
--       crd.positionseq as crd_positionseq,
--       st.genericdate2 as crd_genericdate2,
--       crd.ruleseq as crd_ruleseq,
--       pm.measurementseq      as PM_SEQ,
--       pm.name                as PM_NAME,
--       case rl.CLAWBACK_NAME
--       when 'NLPI' then pct.contributionvalue*V_NLPI_RATE
--       else
--       pct.contributionvalue
--       end as PM_CONTRIBUTION_VALUE,
--       case rl.CLAWBACK_NAME
--         when 'FYO' then fyo_rate.value
--         when 'RYO' then ryo_rate.value
--         when 'FSM_RYO' then ryo_rate.value
--         when 'NLPI' then V_NLPI_RATE
--       else 1
--         end as PM_RATE,
--       dep.depositseq         as DEPOSITSEQ,
--       dep.name               as DEPOSIT_NAME,
--       dep.value              as DEPOSIT_VALUE,
--       crd.periodseq          as PERIODSEQ,
--       st.salestransactionseq as SALESTRANSACTIONSEQ,
--       crd.genericattribute2  as PRODUCT_NAME,
--       crd.genericnumber1     as POLICY_YEAR,
--       st.genericnumber2      as COMMISSION_RATE,
--       st.genericdate4        as PAID_TO_DATE,
--       P_BATCH_NO             as BATCH_NUMBER,
--       sysdate                as CREATED_DATE
--  FROM CS_SALESTRANSACTION st
-- inner join CS_CREDIT crd
--    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
--    and crd.tenantid=st.tenantid and crd.processingunitseq=st.processingunitseq
----and crd.genericdate2
-- inner join CS_PMCREDITTRACE pct
--    on crd.CREDITSEQ = pct.CREDITSEQ
--    and pct.tenantid=crd.tenantid and pct.processingunitseq=crd.processingunitseq
-- inner join CS_MEASUREMENT pm
--    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
--    and pct.tenantid=pm.tenantid and pct.processingunitseq=pm.processingunitseq
-- inner join (select distinct SOURCE_RULE_OUTPUT
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'PM'
--                AND CLAWBACK_NAME  IN ('FYO','RYO','FSM_RYO','NLPI'))pmr
--    on pmr.SOURCE_RULE_OUTPUT = pm.name
-- inner join  CS_PMSELFTRACE pmslf
--   on  pm.measurementseq = pmslf.sourcemeasurementseq
--   and pm.tenantid=pmslf.tenantid and pm.processingunitseq=pmslf.processingunitseq
--inner join CS_INCENTIVEPMTRACE inpm
--   on pmslf.TARGETMEASUREMENTSEQ = inpm.MEASUREMENTSEQ
--   and inpm.tenantid=pmslf.tenantid and inpm.processingunitseq=pmslf.processingunitseq
--inner join CS_DEPOSITINCENTIVETRACE depin
--   on inpm.incentiveseq = depin.incentiveseq
--   and inpm.tenantid=depin.tenantid and inpm.processingunitseq=depin.processingunitseq
-- inner join cs_deposit dep
--    on depin.depositseq = dep.depositseq
--    and dep.tenantid=depin.tenantid and dep.processingunitseq=depin.processingunitseq
-- inner join cs_position dep_pos
--    on dep.positionseq = dep_pos.ruleelementownerseq
--    and dep_pos.tenantid='AIAS'
--   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= crd.genericdate2
--   and dep_pos.effectiveenddate > crd.genericdate2
-- inner join CS_CREDITTYPE ct
--    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--   and ct.Removedate = DT_REMOVEDATE
-- inner join AIA_CB_IDENTIFY_POLICY ip
--    on st.PONUMBER = ip.PONUMBER
--   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--   AND st.PRODUCTID = ip.COMPONENT_CODE
--   and crd.genericattribute12 = ip.wri_agt_code
--    and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
-- --for lookup the compensation output name
-- inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'DR'
--                AND CLAWBACK_NAME  IN ('FYO','RYO','FSM_RYO','NLPI')) rl
--    on dep.NAME = rl.SOURCE_RULE_OUTPUT
-- --for lookup the receiver info.
-- inner join cs_title dep_title
-- on dep_pos.titleseq = dep_title.ruleelementownerseq
-- and dep_title.removedate = DT_REMOVEDATE
-- and dep_title.effectivestartdate <= crd.genericdate2
-- and dep_title.effectiveenddate > crd.genericdate2
-- --for lookup PM rate for FYO
-- left join vw_lt_fyo_rate fyo_rate
-- on fyo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
--   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
--   and fyo_rate.Receiver_title = dep_title.name
--   and rl.CLAWBACK_NAME = 'FYO'
-- --for lookup PM rate for RYO
-- left join vw_lt_ryo_life_rate ryo_rate
-- on ryo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
-- and ryo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
-- and ryo_rate.Receiver_title = dep_title.name
-- and rl.CLAWBACK_NAME in ( 'RYO','FSM_RYO')
-- WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ and st.tenantid='AIAS'
--   AND st.BUSINESSUNITMAP = 1;
--   --and dep.periodseq =  t_periodseq(i);
--   --and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
-- /*  and greatest(nvl(st.GENERICDATE3, to_date('19000101', 'yyyymmdd')),
--                nvl(st.GENERICDATE2, to_date('19000101', 'yyyymmdd')),
--                nvl(st.GENERICDATE5, to_date('19000101', 'yyyymmdd'))) between
--                --to_date('20150801','yyyymmdd') and to_date('20160531','yyyymmdd');
--       DT_INCEPTION_START_DATE and DT_INCEPTION_END_DATE; */
-- --  and crd.genericattribute16 not in ('RO', 'RNO')
--
--
-- Log('insert into AIA_CB_TRACE_FORWARD_COMP for FYO,RYO,FSM_RYO,NLPI' || '; row count: ' || to_char(sql%rowcount));
--
----end loop;
--
--
--
--commit;
--
--
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for NADOR' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--
----for lumpsum compensation trace forward for NADOR
----for i in 1..t_periodseq.count
----loop
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for NADOR using periodseq V_CAL_PERIOD of ' || V_CAL_PERIOD);
--
--
-- execute immediate 'truncate table aia_tmp_Comls_Step0_2';
-- insert into  aia_tmp_Comls_Step0_2
--
--select /*+ leading(ip,st) */ st.salestransactionseq,  ip.wri_agt_code as wri_agt_code_ORIG,
-- ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--        ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--       st.genericnumber2      as COMMISSION_RATE,
--       st.genericdate4        as PAID_TO_DATE
--       ,'SGT'||ip.wri_agt_code wri_agt_code
--       ,ip.quarter || ' ' || ip.year qtrYr
--
-- from cs_Salestransaction st
-- INNER JOIN AIA_CB_IDENTIFY_POLICY IP
--ON 1                              =1
--AND IP.BUNAME                     = STR_BUNAME
--AND ST.PONUMBER                   = IP.PONUMBER
--AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
--AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
--AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER
--and st.productid=ip.component_CODE
-- where st.tenantid='AIAS'
--and st.processingUnitseq=V_PROCESSINGUNITSEQ
----and st.compensationdate between '1-mar-2017' and '31-may-2017'
----and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
--
----AND ST.PRODUCTID                  = IP.COMPONENT_CODE
--;
--
--Log('insert 0_2 done '||SQL%ROWCOUNT);
--
--
--commit;
--
--execute immediate 'truncate table aia_tmp_comls_step1_2';
--insert into aia_tmp_comls_step1_2
--
--select /*+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ ) */ crd.creditseq,
--       crd.salestransactionseq ,
--        ip.CALCULATION_PERIOD,
--       ip.POLICY_NUMBER as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--        ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--        crd.compensationdate as TRANSACTION_DATE,
--         crd.genericattribute12 as PAYOR_CODE,
--         ct.credittypeid        as CREDITTYPE,
--       crd.name               as CREDIT_NAME,
--       crd.value              as CREDIT_VALUE,
--        crd.periodseq          as PERIODSEQ,
--         crd.genericattribute2  as PRODUCT_NAME,
--       crd.genericnumber1     as POLICY_YEAR,
--       ip.COMMISSION_RATE,
--       ip.PAID_TO_DATE
--       ,ip.wri_agt_code wri_agt_code
--       ,ip.qtrYr, crd.genericdate2
--   ,crd.genericattribute13  ,crd.genericattribute14, crd.positionseq, crd.ruleseq
--  from cs_Credit crd
--  join aia_tmp_comls_period p
--  on crd.periodseq=p.periodseq
--  join cs_Salestransaction st
--  on st.salestransactionseq=crd.salestransactionseq
--  and st.tenantid='AIAS' and st.processingunitseq=crd.processingunitseq
-- -- and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
--   inner join CS_CREDITTYPE ct
--    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--   and ct.Removedate >sysdate
--  inner join aia_tmp_comls_step0_2 ip
--    on 1=1
--    and ip.salestransactionseq = crd.salestransactionseq
--      and crd.genericattribute12 = ip.wri_agt_code_ORIG
--  and  ip.CALCULATION_PERIOD = V_CAL_PERIOD
--  inner join cs_businessunit bu on st.businessunitmap = bu.mask
--   where crd.tenantid = 'AIAS'
--   and crd.processingunitseq = V_PROCESSINGUNITSEQ
-- and bu.name = STR_BUNAME
--;
--
--
--
--
--
--Log('insert 1_2 done '||SQL%ROWCOUNT);
--
----delete from AIA_TMP_COMLS_STEP1_2 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;
--
--
--Log('delete 1_2 done '||SQL%ROWCOUNT);
--commit;
--
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP1_2"',
--          estimate_percent => 1
--          );
--
--
--execute immediate 'truncate table aia_tmp_comls_step2_2';
--insert into aia_tmp_comls_step2_2
--
--select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , null as clawback_name
--from cs_measurement m
--join aia_tmp_comls_period p
--  on m.periodseq=p.periodseq
--  inner join (select distinct SOURCE_RULE_OUTPUT
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'PM'
--                AND CLAWBACK_NAME ='NADOR')pmr
--    on pmr.SOURCE_RULE_OUTPUT = m.name
--
--  where  m.processingunitseq = V_PROCESSINGUNITSEQ
--  and m.tenantid='AIAS'
--   ;
--
--
--
--Log('insert 2_2 done '||SQL%ROWCOUNT);
--commit;
--
--
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP2_2"',
--          estimate_percent => 1
--          );
--
--  execute immediate 'truncate table aia_tmp_comls_step3_2';
--  insert into aia_tmp_comls_step3_2
--   select   pct.creditseq pctCreditSeq,
--   pct.measurementseq, pct.contributionvalue PctContribValue, depin.depositseq
--   , s1.CREDITSEQ
--,SALESTRANSACTIONSEQ
--,CALCULATION_PERIOD
--,POLICY_NUMBER
--,POLICYIDSEQ
--,LIFE_NUMBER
--,COVERAGE_NUMBER
--,RIDER_NUMBER
--,COMPONENT_CODE
--,COMPONENT_NAME
--,BASE_RIDER_IND
--,TRANSACTION_DATE
--,PAYOR_CODE
--,CREDITTYPE
--,CREDIT_NAME
--,CREDIT_VALUE
--,s1.PERIODSEQ
--,PRODUCT_NAME
--,POLICY_YEAR
--,COMMISSION_RATE
--,PAID_TO_DATE
--
--   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
--   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
--   ,WRI_AGT_CODE
--,QTRYR
--,GD2
--,         pct.contributionvalue  as CONTRIBUTIONVALUE , s1.genericattribute13
-- ,s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
--   from cs_pmcredittrace pct
--   join aia_tmp_comls_step1_2 s1
--   on pct.creditseq=s1.creditseq
--   join aia_tmp_comls_step2_2 s2
--   on s2.measurementseq=pct.measurementseq --and s2.ruleseq=pct.ruleseq
--   --and pct.targetperiodseq=s2.periodseq
--   inner join CS_INCENTIVEPMTRACE inpm
--    on s2.MEASUREMENTSEQ = inpm.MEASUREMENTSEQ and inpm.tenantid='AIAS' and inpm.processingunitseq=v_processingunitseq
--     --and inpm.targetperiodseq=s2.periodseq
-- inner join  /*CS_DEPOSITINCENTIVETRACE*/ (select * from cs_depositincentivetrace union select * from aias_depositincentivetrace) depin
--    on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid
--    --and depin.targetperiodseq=s2.periodseq
-- where depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ   and depin.tenantid='AIAS'
--;
--
--Log('insert 3_2 done '||SQL%ROWCOUNT);
--commit;
--DBMS_STATS.GATHER_TABLE_STATS (
--              ownname => '"AIASEXT"',
--          tabname => '"AIA_TMP_COMLS_STEP3_2"',
--          estimate_percent => 1
--          );
--
--
--
--
--insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
--select STR_BUNAME as BUNAME,
--       QtrYr as CALCULATION_PERIOD,
--       POLICY_NUMBER,
--        POLICYIDSEQ,
--       mPositionSeq PAYEE_SEQ,
--              substr(dep_pos.name, 4) as PAYEE_CODE,
--       PAYOR_CODE,
--     LIFE_NUMBER,
--      COVERAGE_NUMBER,
--       RIDER_NUMBER,
--        COMPONENT_CODE,
--         COMPONENT_NAME,
--         BASE_RIDER_IND,
--         TRANSACTION_DATE,
--       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
--       STR_LUMPSUM as CLAWBACK_TYPE,
--        rl.CLAWBACK_NAME       as CLAWBACK_NAME,
--         STR_CB_NAME as CLAWBACK_METHOD,
--     CREDITTYPE,
--      CREDITSEQ,
--        CREDIT_NAME,
--        CREDIT_VALUE,
--       crd_positionseq as crd_positionseq,
--       GD2 as crd_genericdate2,
--       crd_ruleseq as crd_ruleseq,
--       measurementseq      as PM_SEQ,
--       mname                as PM_NAME,
--       x.contributionvalue*V_NADOR_RATE  as PM_CONTRIBUTION_VALUE,
--       V_NADOR_RATE           as PM_RATE,
--       dep.depositseq         as DEPOSITSEQ,
--       /*dep.name*/replace(dep.name,'_MANUAL','')              as DEPOSIT_NAME,
--       dep.value              as DEPOSIT_VALUE,
--       x.periodseq          as PERIODSEQ,
--        x.salestransactionseq as SALESTRANSACTIONSEQ,
--        PRODUCT_NAME,
--         POLICY_YEAR,
--         COMMISSION_RATE,
--         PAID_TO_DATE,
--       P_BATCH_NO             as BATCH_NUMBER,
--       sysdate                as CREATED_DATE
--  FROM
-- aia_tmp_comls_step3_2 x
--       join cs_deposit dep
--       on dep.depositseq=x.depositseq
--       join cs_position dep_pos
--       on dep.positionseq = dep_pos.ruleelementownerseq
--   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= x.GD2
--   and dep_pos.effectiveenddate > x.GD2
--
-- inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'DR'
--                AND CLAWBACK_NAME  IN ('NADOR')) rl
--    on /*dep.NAME*/ replace(Dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT
-- WHERE dep.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--
--   --and dep.periodseq =  x.periodseq 170807
--    ;
--
--
--
--
--
----insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
--/*select STR_BUNAME as BUNAME,
--       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--       pm.positionseq as PAYEE_SEQ,
--       substr(dep_pos.name, 4) as PAYEE_CODE,
--       crd.genericattribute12 as PAYOR_CODE,
--       ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--       crd.compensationdate as TRANSACTION_DATE,
--       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
--       STR_LUMPSUM as CLAWBACK_TYPE,
--        rl.CLAWBACK_NAME       as CLAWBACK_NAME,
--         STR_CB_NAME as CLAWBACK_METHOD,
--       ct.credittypeid        as CREDIT_TYPE,
--       crd.creditseq          as CREDITSEQ,
--       crd.name               as CREDIT_NAME,
--       crd.value              as CREDIT_VALUE,
--        crd.positionseq as crd_positionseq,
--       st.genericdate2 as crd_genericdate2,
--       crd.ruleseq as crd_ruleseq,
--       pm.measurementseq      as PM_SEQ,
--       pm.name                as PM_NAME,
--       pct.contributionvalue*V_NADOR_RATE  as PM_CONTRIBUTION_VALUE,
--       V_NADOR_RATE           as PM_RATE,
--       dep.depositseq         as DEPOSITSEQ,
--       dep.name               as DEPOSIT_NAME,
--       dep.value              as DEPOSIT_VALUE,
--       crd.periodseq          as PERIODSEQ,
--       st.salestransactionseq as SALESTRANSACTIONSEQ,
--       crd.genericattribute2  as PRODUCT_NAME,
--       crd.genericnumber1     as POLICY_YEAR,
--       st.genericnumber2      as COMMISSION_RATE,
--       st.genericdate4        as PAID_TO_DATE,
--       P_BATCH_NO             as BATCH_NUMBER,
--       sysdate                as CREATED_DATE
--  FROM CS_SALESTRANSACTION st
-- inner join CS_CREDIT crd
--    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ and st.tenantid=crd.tenantid and st.processingunitseq=pm.processingunitseq
-- inner join CS_PMCREDITTRACE pct
--    on crd.CREDITSEQ = pct.CREDITSEQ and pct.tenantid=crd.tenantid and crd.processingunitseq=pm.processingunitseq
--    and pct.sourceperiodseq=crd.periodseq
-- inner join CS_MEASUREMENT pm
--    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ and pct.tenantid=pm.tenantid and pct.processingunitseq=pm.processingunitseq
--    and pm.periodseq=pct.targetperiodseq
-- inner join (select distinct SOURCE_RULE_OUTPUT
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'PM'
--                AND CLAWBACK_NAME ='NADOR')pmr
--    on pmr.SOURCE_RULE_OUTPUT = pm.name
-- inner join CS_INCENTIVEPMTRACE inpm
--    on pm.MEASUREMENTSEQ = inpm.MEASUREMENTSEQ and inpm.tenantid=pm.tenantid and inpm.processingunitseq=pm.processingunitseq
-- inner join CS_DEPOSITINCENTIVETRACE depin
--    on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid and inpm.processingunitseq=depin.processingunitseq
-- inner join cs_deposit dep
--    on depin.depositseq = dep.depositseq and dep.tenantid=depin.tenantid and dep.processingunitseq=depin.processingunitseq
-- inner join cs_position dep_pos
--    on dep.positionseq = dep_pos.ruleelementownerseq
--   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= crd.genericdate2
--   and dep_pos.effectiveenddate > crd.genericdate2
-- inner join CS_CREDITTYPE ct
--    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--   and ct.Removedate = DT_REMOVEDATE
-- inner join AIA_CB_IDENTIFY_POLICY ip
--    on st.PONUMBER = ip.PONUMBER
--   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--   AND st.PRODUCTID = ip.COMPONENT_CODE
--   and crd.genericattribute12 = ip.wri_agt_code
--   and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
-- inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'DR'
--                AND CLAWBACK_NAME  IN ('NADOR')) rl
--    on dep.NAME = rl.SOURCE_RULE_OUTPUT
-- WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   AND st.BUSINESSUNITMAP = 1
--   and dep.periodseq =  t_periodseq(i)
--   and st.tenantid='AIAS' and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   and crd.tenantid='AIAS' and crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   and pct.tenantid='AIAS' and pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   and pm.tenantid='AIAS' and pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   and inpm.tenantid='AIAS' and inpm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   and depin.tenantid='AIAS' and depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   and dep.tenantid='AIAS' and dep.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--   and dep_pos.tenantid='AIAS' and dep_pos.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ;
--
----   and crd.genericattribute16 not in ('RO', 'RNO');
--
--
--commit;
----end loop;
--*/
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for NADOR' || '; row count: ' || to_char(sql%rowcount));
--commit;
--
--
--/*Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--
----for lumpsum compensation trace forward for SPI
--for i in 1..t_periodseq.count loop
--insert into AIA_CB_TRACE_FORWARD_COMP
--select STR_BUNAME as BUNAME,
--       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--       ip.ponumber as POLICY_NUMBER,
--       ip.policyidseq as POLICYIDSEQ,
--       pm.positionseq as PAYEE_SEQ,
--       substr(dep_pos.name, 4) as PAYEE_CODE,
--       crd.genericattribute12 as PAYOR_CODE,
--       ip.life_number as LIFE_NUMBER,
--       ip.coverage_number as COVERAGE_NUMBER,
--       ip.rider_number as RIDER_NUMBER,
--       ip.component_code as COMPONENT_CODE,
--       ip.component_name as COMPONENT_NAME,
--       ip.base_rider_ind as BASE_RIDER_IND,
--       crd.compensationdate as TRANSACTION_DATE,
--       TO_CHAR(DT_CB_START_DATE, 'MON-YYYY') as PROCESSING_PERIOD,
--       STR_LUMPSUM as CLAWBACK_TYPE,
--       rl.CLAWBACK_NAME as CLAWBACK_NAME,
--       STR_COMPENSATION as CLAWBACK_METHOD,
--       ct.credittypeid as CREDIT_TYPE,
--       crd.creditseq as CREDITSEQ,
--       crd.name as CREDIT_NAME,
--       crd.value as CREDIT_VALUE,
--       crd.positionseq as crd_positionseq,
--       st.genericdate2 as crd_genericdate2,
--       crd.ruleseq as crd_ruleseq,
--       pm.measurementseq as PM_SEQ,
--       pm.name as PM_NAME,
--       pct.contributionvalue as PM_CONTRIBUTION_VALUE,
--       case
--         when sm3.name like 'SM_SPI_RATE%' then
--          sm3.value
--         else
--          0
--       end as PM_RATE,
--       dep.depositseq as DEPOSITSEQ,
--       dep.name as DEPOSIT_NAME,
--       dep.value as DEPOSIT_VALUE,
--       crd.periodseq as PERIODSEQ,
--       st.salestransactionseq as SALESTRANSACTIONSEQ,
--       crd.genericattribute2 as PRODUCT_NAME,
--       crd.genericnumber1 as POLICY_YEAR,
--       st.genericnumber2 as COMMISSION_RATE,
--       st.genericdate4 as PAID_TO_DATE,
--       P_BATCH_NO as BATCH_NUMBER,
--       sysdate as CREATED_DATE
--  FROM CS_SALESTRANSACTION st
-- inner join CS_CREDIT crd
--    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
----and crd.genericdate2
-- inner join CS_PMCREDITTRACE pct
--    on crd.CREDITSEQ = pct.CREDITSEQ
-- inner join CS_MEASUREMENT pm
--    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
-- inner join (select distinct SOURCE_RULE_OUTPUT
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'PM'
--                AND CLAWBACK_NAME = 'SPI') pmr
--    on pmr.SOURCE_RULE_OUTPUT = pm.name
----for SM level 1 (SM_PIB_SG_SPI)
-- inner join CS_PMSELFTRACE pm_sm1
--    on pm.measurementseq = pm_sm1.sourcemeasurementseq
-- inner join CS_MEASUREMENT sm1
--    on sm1.measurementseq = pm_sm1.targetmeasurementseq
----for SM level 2 (SM_PIB_YTD_SG_SPI)
-- inner join CS_PMSELFTRACE sm1_sm2
--    on sm1.measurementseq = sm1_sm2.sourcemeasurementseq
-- inner join CS_MEASUREMENT sm2
--    on sm2.measurementseq = sm1_sm2.targetmeasurementseq
----for SM level 3 (SM_SPI_RATE/SM_SPI_RATE_NEW_AGT)
-- inner join CS_PMSELFTRACE sm2_sm3
--    on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
-- inner join CS_MEASUREMENT sm3
--    on sm3.measurementseq = sm2_sm3.targetmeasurementseq
----for SM level 4 (SM_SPI_CALCULATE_YTD)
-- inner join CS_PMSELFTRACE sm3_sm4
--    on sm3.measurementseq = sm3_sm4.sourcemeasurementseq
-- inner join CS_MEASUREMENT sm4
--    on sm4.measurementseq = sm3_sm4.targetmeasurementseq
----for SM level 5 (SM_SPI_PAYMENT_QTR)
-- inner join CS_PMSELFTRACE sm4_sm5
--    on sm4.measurementseq = sm4_sm5.sourcemeasurementseq
-- inner join CS_MEASUREMENT sm5
--    on sm5.measurementseq = sm4_sm5.targetmeasurementseq
----for SM level 6 (SM_SPI_PAYMENT_YTD)
-- inner join CS_PMSELFTRACE sm5_sm6
--    on sm5.measurementseq = sm5_sm6.sourcemeasurementseq
-- inner join CS_MEASUREMENT sm6
--    on sm6.measurementseq = sm5_sm6.targetmeasurementseq
----for Incentive (I_SPI_SG)
-- inner join CS_INCENTIVEPMTRACE inpm
--    on sm5.measurementseq = inpm.MEASUREMENTSEQ
-- inner join cs_incentive inc
--    on inpm.incentiveseq = inc.incentiveseq
----for deposit (D_SPI_SG)
-- inner join CS_DEPOSITINCENTIVETRACE depin
--    on inpm.incentiveseq = depin.incentiveseq
-- inner join cs_deposit dep
--    on depin.depositseq = dep.depositseq
-- inner join cs_position dep_pos
--    on dep.positionseq = dep_pos.ruleelementownerseq
--   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= crd.genericdate2
--   and dep_pos.effectiveenddate > crd.genericdate2
-- inner join CS_CREDITTYPE ct
--    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--   and ct.Removedate = DT_REMOVEDATE
-- inner join AIA_CB_IDENTIFY_POLICY ip
--    on st.PONUMBER = ip.PONUMBER
--   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--   AND st.PRODUCTID = ip.COMPONENT_CODE
--   and crd.genericattribute12 = ip.wri_agt_code
--  inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--               from AIA_CB_RULES_LOOKUP
--              where RULE_TYPE = 'DR'
--                AND CLAWBACK_NAME = 'SPI') rl
--    on dep.NAME = rl.SOURCE_RULE_OUTPUT
-- where dep.periodseq = t_periodseq(i)
-- and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
-- AND st.BUSINESSUNITMAP = 1
-- --and crd.genericattribute16 not in ('RO', 'RNO')
-- ;
--
--end loop;
--
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI' || '; row count: ' || to_char(sql%rowcount));*/
--
--elsif P_STR_TYPE = STR_ONGOING then
--
--  select value into V_NADOR_RATE
--from CS_FIXEDVALUE fv where
--name='FV_NADOR_Payout_Rate'
--and Removedate = DT_REMOVEDATE;
--
-- select value into V_NLPI_RATE
--from CS_FIXEDVALUE fv where
--name='FV_NLPI_RATE'
--and Removedate = DT_REMOVEDATE;
--
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for Ongoing 1, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--
--/*
--
--create table aias_tx_temp tablespace tallydata
--as
--select  *+ PARALLEL leading(ip,st) INDEX(st aia_salestransaction_product) *st*,
--ip.BUNAME
--,ip.YEAR
--,ip.QUARTER
--,ip.WRI_DIST_CODE
--,ip.WRI_DIST_NAME
--,ip.WRI_DM_CODE
--,ip.WRI_DM_NAME
--,ip.WRI_AGY_CODE
--,ip.WRI_AGY_NAME
--,ip.WRI_AGY_LDR_CODE
--,ip.WRI_AGY_LDR_NAME
--,ip.WRI_AGT_CODE
--,ip.WRI_AGT_NAME
--,ip.FSC_TYPE
--,ip.RANK
--,ip.CLASS
--,ip.FSC_BSC_GRADE
--,ip.FSC_BSC_PERCENTAGE
--,ip.INSURED_NAME
--,ip.CONTRACT_CAT
--,ip.LIFE_NUMBER
--,ip.COVERAGE_NUMBER
--,ip.RIDER_NUMBER
--,ip.COMPONENT_CODE
--,ip.COMPONENT_NAME
--,ip.ISSUE_DATE
--,ip.INCEPTION_DATE
--,ip.RISK_COMMENCEMENT_DATE
--,ip.FHR_DATE
--,ip.BASE_RIDER_IND
--,ip.TRANSACTION_DATE
--,ip.PAYMENT_MODE
--,ip.POLICY_CURRENCY
--,ip.PROCESSING_PERIOD
--,ip.CREATED_DATE
--,ip.POLICYIDSEQ
--,ip.SUBMITDATE
--,p.periodseq from CS_SALESTRANSACTION st
--  inner join AIA_CB_IDENTIFY_POLICY ip
--     on st.PONUMBER = ip.PONUMBER
--    AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--    AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--    AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--    AND st.PRODUCTID = ip.COMPONENT_CODE
--  inner join CS_PERIOD p
--     on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
--        and p.calendarseq=2251799813685250
--       where 1=0;
--*/
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for Ongoing 1 part1, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--
--execute immediate 'truncate table aias_tx_temp';
--insert into aias_tx_temp
--select  /*+ PARALLEL leading(ip,st,p) INDEX(st aia_salestransaction_product) */st.*,
--ip.BUNAME
--,ip.YEAR
--,ip.QUARTER
--,ip.WRI_DIST_CODE
--,ip.WRI_DIST_NAME
--,ip.WRI_DM_CODE
--,ip.WRI_DM_NAME
--,ip.WRI_AGY_CODE
--,ip.WRI_AGY_NAME
--,ip.WRI_AGY_LDR_CODE
--,ip.WRI_AGY_LDR_NAME
--,ip.WRI_AGT_CODE
--,ip.WRI_AGT_NAME
--,ip.FSC_TYPE
--,ip.RANK
--,ip.CLASS
--,ip.FSC_BSC_GRADE
--,ip.FSC_BSC_PERCENTAGE
--,ip.INSURED_NAME
--,ip.CONTRACT_CAT
--,ip.LIFE_NUMBER
--,ip.COVERAGE_NUMBER
--,ip.RIDER_NUMBER
--,ip.COMPONENT_CODE
--,ip.COMPONENT_NAME
--,ip.ISSUE_DATE
--,ip.INCEPTION_DATE
--,ip.RISK_COMMENCEMENT_DATE
--,ip.FHR_DATE
--,ip.BASE_RIDER_IND
--,ip.TRANSACTION_DATE
--,ip.PAYMENT_MODE
--,ip.POLICY_CURRENCY
--,ip.PROCESSING_PERIOD
--,ip.CREATED_DATE
--,ip.POLICYIDSEQ
--,ip.SUBMITDATE
--,p.periodseq from CS_SALESTRANSACTION st
--  inner join AIA_CB_IDENTIFY_POLICY ip
--     on st.PONUMBER = ip.PONUMBER
--    AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--    AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--    AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--    AND st.PRODUCTID = ip.COMPONENT_CODE
--  inner join CS_PERIOD p
--     on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
--        and p.calendarseq=2251799813685250 and p.periodtypeseq = 2814749767106561
--   inner join cs_businessunit bu on st.businessunitmap = bu.mask
--    where st.tenantid='AIAS' and   st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--    and bu.name = STR_BUNAME
--    and st.eventtypeseq <> 16607023625933358
--    and p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
--        ;
--
----For AI clawback NL20180308
--insert into aias_tx_temp
--with AMR as (select row_number() over(partition by t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE ,t1.POLICY_INCEPTION_DATE order by t1.component_CODE) as rn,
--             t1.* from AI_MONTHLY_REPORT t1 where t1.AI_PAYMENT<> 0),
--     st as (select row_number() over(partition by t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11 ,t2.GENERICDATE2 order by t2.PRODUCTID) as rn,
--            t2.* from cs_Salestransaction t2,cs_businessunit bu  where t2.tenantid='AIAS' and t2.businessunitmap = bu.mask
--          and bu.name = STR_BUNAME and t2.eventtypeseq = 16607023625933358 and  t2.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ),
--     IP as (select row_number() over(partition by t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date order by t3.coverage_number ) as rn,
--            t3.* from AIA_CB_IDENTIFY_POLICY t3 where t3.BUNAME  = STR_BUNAME)
--select  /*+ PARALLEL */st.TENANTID                        ,
--st.SALESTRANSACTIONSEQ             ,
--st.SALESORDERSEQ                   ,
--st.LINENUMBER                      ,
--st.SUBLINENUMBER                   ,
--st.EVENTTYPESEQ                    ,
--st.PIPELINERUNSEQ                  ,
--st.ORIGINTYPEID                    ,
--st.COMPENSATIONDATE                ,
--st.BILLTOADDRESSSEQ                ,
--st.SHIPTOADDRESSSEQ                ,
--st.OTHERTOADDRESSSEQ               ,
--st.ISRUNNABLE                      ,
--st.BUSINESSUNITMAP                 ,
--st.ACCOUNTINGDATE                  ,
--st.PRODUCTID                       ,
--st.PRODUCTNAME                     ,
--st.PRODUCTDESCRIPTION              ,
--st.NUMBEROFUNITS                   ,
--st.UNITVALUE                       ,
--st.UNITTYPEFORUNITVALUE            ,
--st.PREADJUSTEDVALUE                ,
--st.UNITTYPEFORPREADJUSTEDVALUE     ,
--st.VALUE                           ,
--st.UNITTYPEFORVALUE                ,
--st.NATIVECURRENCY                  ,
--st.NATIVECURRENCYAMOUNT            ,
--st.DISCOUNTPERCENT                 ,
--st.DISCOUNTTYPE                    ,
--st.PAYMENTTERMS                    ,
--st.PONUMBER                        ,
--st.CHANNEL                         ,
--st.ALTERNATEORDERNUMBER            ,
--st.DATASOURCE                      ,
--st.REASONSEQ                       ,
--st.COMMENTS                        ,
--st.GENERICATTRIBUTE1               ,
--st.GENERICATTRIBUTE2               ,
--st.GENERICATTRIBUTE3               ,
--st.GENERICATTRIBUTE4               ,
--st.GENERICATTRIBUTE5               ,
--st.GENERICATTRIBUTE6               ,
--st.GENERICATTRIBUTE7               ,
--st.GENERICATTRIBUTE8               ,
--st.GENERICATTRIBUTE9               ,
--st.GENERICATTRIBUTE10              ,
--st.GENERICATTRIBUTE11              ,
--st.GENERICATTRIBUTE12              ,
--st.GENERICATTRIBUTE13              ,
--st.GENERICATTRIBUTE14              ,
--st.GENERICATTRIBUTE15              ,
--st.GENERICATTRIBUTE16              ,
--st.GENERICATTRIBUTE17              ,
--st.GENERICATTRIBUTE18              ,
--st.GENERICATTRIBUTE19              ,
--st.GENERICATTRIBUTE20              ,
--st.GENERICATTRIBUTE21              ,
--st.GENERICATTRIBUTE22              ,
--st.GENERICATTRIBUTE23              ,
--st.GENERICATTRIBUTE24              ,
--st.GENERICATTRIBUTE25              ,
--st.GENERICATTRIBUTE26              ,
--st.GENERICATTRIBUTE27              ,
--st.GENERICATTRIBUTE28              ,
--st.GENERICATTRIBUTE29              ,
--st.GENERICATTRIBUTE30              ,
--st.GENERICATTRIBUTE31              ,
--st.GENERICATTRIBUTE32              ,
--st.GENERICNUMBER1                  ,
--st.UNITTYPEFORGENERICNUMBER1       ,
--st.GENERICNUMBER2                  ,
--st.UNITTYPEFORGENERICNUMBER2       ,
--st.GENERICNUMBER3                  ,
--st.UNITTYPEFORGENERICNUMBER3       ,
--st.GENERICNUMBER4                  ,
--st.UNITTYPEFORGENERICNUMBER4       ,
--st.GENERICNUMBER5                  ,
--st.UNITTYPEFORGENERICNUMBER5       ,
--st.GENERICNUMBER6                  ,
--st.UNITTYPEFORGENERICNUMBER6       ,
--st.GENERICDATE1                    ,
--st.GENERICDATE2                    ,
--st.GENERICDATE3                    ,
--st.GENERICDATE4                    ,
--st.GENERICDATE5                    ,
--st.GENERICDATE6                    ,
--st.GENERICBOOLEAN1                 ,
--st.GENERICBOOLEAN2                 ,
--st.GENERICBOOLEAN3                 ,
--st.GENERICBOOLEAN4                 ,
--st.GENERICBOOLEAN5                 ,
--st.GENERICBOOLEAN6                 ,
--st.PROCESSINGUNITSEQ               ,
--st.MODIFICATIONDATE                ,
--st.UNITTYPEFORLINENUMBER           ,
--st.UNITTYPEFORSUBLINENUMBER        ,
--st.UNITTYPEFORNUMBEROFUNITS        ,
--st.UNITTYPEFORDISCOUNTPERCENT      ,
--st.UNITTYPEFORNATIVECURRENCYAMT    ,
--st.MODELSEQ                        ,
--ip.BUNAME
--,ip.YEAR
--,ip.QUARTER
--,ip.WRI_DIST_CODE
--,ip.WRI_DIST_NAME
--,ip.WRI_DM_CODE
--,ip.WRI_DM_NAME
--,ip.WRI_AGY_CODE
--,ip.WRI_AGY_NAME
--,ip.WRI_AGY_LDR_CODE
--,ip.WRI_AGY_LDR_NAME
--,ip.WRI_AGT_CODE
--,ip.WRI_AGT_NAME
--,ip.FSC_TYPE
--,ip.RANK
--,ip.CLASS
--,ip.FSC_BSC_GRADE
--,ip.FSC_BSC_PERCENTAGE
--,ip.INSURED_NAME
--,ip.CONTRACT_CAT
--,ip.LIFE_NUMBER
--,ip.COVERAGE_NUMBER
--,ip.RIDER_NUMBER
--,ip.COMPONENT_CODE
--,ip.COMPONENT_NAME
--,ip.ISSUE_DATE
--,ip.INCEPTION_DATE
--,ip.RISK_COMMENCEMENT_DATE
--,ip.FHR_DATE
--,ip.BASE_RIDER_IND
--,ip.TRANSACTION_DATE
--,ip.PAYMENT_MODE
--,ip.POLICY_CURRENCY
--,ip.PROCESSING_PERIOD
--,ip.CREATED_DATE
--,ip.POLICYIDSEQ
--,ip.SUBMITDATE
--,p.periodseq from  st
-- INNER JOIN  AMR
--ON  st.PONUMBER = AMR.PONUMBER
--AND st.VALUE = AMR.AI_PAYMENT
--AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
--AND st.GENERICATTRIBUTE11 = AMR.PAYEE_CODE
--AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
--AND st.rn = AMR.rn
--inner join ip
--     on IP.BUNAME                     = 'SGPAGY'
--     AND AMR.PONUMBER                   = IP.PONUMBER
--/*AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
--AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
--AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER*/
--     and AMR.PAYEE_CODE = IP.WRI_AGT_CODE
--     and AMR.component_CODE=ip.component_CODE
--     and AMR.policy_inception_date = ip.inception_date
--     and AMR.risk_commencement_date = ip.risk_commencement_date
--     and AMR.rn = IP.rn
--  inner join CS_PERIOD p
--     on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
--        and p.calendarseq=2251799813685250 and p.periodtypeseq = 2814749767106561
--    where p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
--        ;
--
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for Ongoing 1 part 1b, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT);
--commit;
--
--execute immediate 'truncate table aias_tx_temp15';
----drop table aias_tx_temp2
--insert into aias_tx_temp15
--select /*+ INDEX(crd CS_CREDIT_TRANSACTIONSEQ) */ ip.*, crd.name, crd.creditseq, crd.genericdate2 crdGd2
-- , crd.genericattribute13 crdga13, crd.genericattribute14 crdga14, crd.name crdName, null as measurementseq
-- , ct.credittypeid, cbp.cb_enddate cbpenddate, crd.genericattribute12 CRDGA12, crd.compensationdate CRDCOMPDate
-- ,crd.value crdvalue, crd.positionseq crdpositionseq, crd.ruleseq crdRuleSeq, crd.periodseq Crdperiodseq
-- , crd.genericattribute2 crdGA2, crd.genericnumber1 crdgn1--, null as contributionvalue
--   FROM aias_tx_temp ip
--  inner join CS_CREDIT crd
--     on ip.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
--    and crd.genericattribute12 = ip.wri_agt_code
--    and crd.periodseq = ip.periodseq
--  --inner join CS_PMCREDITTRACE pct
--    -- on crd.CREDITSEQ = pct.CREDITSEQ
--    -- and pct.sourceperiodseq=2533274790398934
--  inner join CS_CREDITTYPE ct
--     on crd.CREDITTYPESEQ = ct.DATATYPESEQ and ct.tenantid='AIAS'
--    and ct.Removedate = '1-jan-2200'
--    inner join  (select distinct
--                      cb_quarter_name,
--                      cb_startdate,
--                      cb_enddate
--                 from aia_cb_period
--                where cb_name = STR_CB_NAME
--                and BUNAME=STR_BUNAME) cbp
--     on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
--   --  where    crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
----      AND pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--;
--
--
--execute immediate 'truncate table aias_tx_temp2';
----drop table aias_tx_temp2
--insert into aias_tx_temp2
----create table aias_tx_temp2 as
-- select    ip.tenantid,
--    salestransactionseq,
--    salesorderseq,
--    linenumber,
--    sublinenumber,
--    eventtypeseq,
--    ip.pipelinerunseq,
--    origintypeid,
--    compensationdate,
--    billtoaddressseq,
--    shiptoaddressseq,
--    othertoaddressseq,
--    isrunnable,
--   ip.businessunitmap,
--    accountingdate,
--    productid,
--    productname,
--    productdescription,
--    numberofunits,
--    unitvalue,
--    unittypeforunitvalue,
--    preadjustedvalue,
--    unittypeforpreadjustedvalue,
--    value,
--    unittypeforvalue,
--    nativecurrency,
--    nativecurrencyamount,
--    discountpercent,
--    discounttype,
--    paymentterms,
--    ponumber,
--    channel,
--    alternateordernumber,
--    datasource,
--    reasonseq,
--    comments,
--    genericattribute1,
--    genericattribute2,
--    genericattribute3,
--    genericattribute4,
--    genericattribute5,
--    genericattribute6,
--    genericattribute7,
--    genericattribute8,
--    genericattribute9,
--    genericattribute10,
--    genericattribute11,
--    genericattribute12,
--    genericattribute13,
--    genericattribute14,
--    genericattribute15,
--    genericattribute16,
--    genericattribute17,
--    genericattribute18,
--    genericattribute19,
--    genericattribute20,
--    genericattribute21,
--    genericattribute22,
--    genericattribute23,
--    genericattribute24,
--    genericattribute25,
--    genericattribute26,
--    genericattribute27,
--    genericattribute28,
--    genericattribute29,
--    genericattribute30,
--    genericattribute31,
--    genericattribute32,
--    genericnumber1,
--    unittypeforgenericnumber1,
--    genericnumber2,
--    unittypeforgenericnumber2,
--    genericnumber3,
--    unittypeforgenericnumber3,
--    genericnumber4,
--    unittypeforgenericnumber4,
--    genericnumber5,
--    unittypeforgenericnumber5,
--    genericnumber6,
--    unittypeforgenericnumber6,
--    genericdate1,
--    genericdate2,
--    genericdate3,
--    genericdate4,
--    genericdate5,
--    genericdate6,
--    genericboolean1,
--    genericboolean2,
--    genericboolean3,
--    genericboolean4,
--    genericboolean5,
--    genericboolean6,
--    ip.processingunitseq,
--    modificationdate,
--    unittypeforlinenumber,
--    unittypeforsublinenumber,
--    unittypefornumberofunits,
--    unittypefordiscountpercent,
--    unittypefornativecurrencyamt,
--    ip.modelseq,
--    buname,
--    year,
--    quarter,
--    wri_dist_code,
--    wri_dist_name,
--    wri_dm_code,
--    wri_dm_name,
--    wri_agy_code,
--    wri_agy_name,
--    wri_agy_ldr_code,
--    wri_agy_ldr_name,
--    wri_agt_code,
--    wri_agt_name,
--    fsc_type,
--    rank,
--    class,
--    fsc_bsc_grade,
--    fsc_bsc_percentage,
--    insured_name,
--    contract_cat,
--    life_number,
--    coverage_number,
--    rider_number,
--    component_code,
--    component_name,
--    issue_date,
--    inception_date,
--    risk_commencement_date,
--    fhr_date,
--    base_rider_ind,
--    transaction_date,
--    payment_mode,
--    policy_currency,
--    processing_period,
--    created_date,
--    policyidseq,
--    submitdate,
--    periodseq,
--    name,
--    ip.creditseq,
--    crdgd2,
--    crdga13,
--    crdga14,
--    crdname,
--    pct.measurementseq,
--    credittypeid,
--    cbpenddate,
--    crdga12,
--    crdcompdate,
--    crdvalue,
--    crdpositionseq,
--    crdruleseq,
--    crdperiodseq,
--    crdga2,
--    crdgn1, pct.contributionvalue
--   FROM aias_tx_temp15 ip
--  inner join CS_PMCREDITTRACE pct
--     on ip.CREDITSEQ = pct.CREDITSEQ
--    -- and pct.sourceperiodseq=2533274790398934
--
--     where  pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--;
--
----for on-going compensation trace forward
----insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
---- select  /*+ LEADING(aias_tx_temp,crd,pct,pm) PARALLEL */ STR_BUNAME as BUNAME,
--  /*      ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--        ip.ponumber as POLICY_NUMBER,
--        ip.policyidseq as POLICYIDSEQ,
--        pm.positionseq as PAYEE_SEQ,
--        substr(pm_pos.name, 4) as PAYEE_CODE,
--        crd.genericattribute12 as PAYOR_CODE,
--        ip.life_number as LIFE_NUMBER,
--        ip.coverage_number as COVERAGE_NUMBER,
--        ip.rider_number as RIDER_NUMBER,
--        ip.component_code as COMPONENT_CODE,
--        ip.component_name as COMPONENT_NAME,
--        ip.base_rider_ind as BASE_RIDER_IND,
--        crd.compensationdate as TRANSACTION_DATE,
--        substr(ONGOING_PERIOD,1,3)||'-'||substr(ONGOING_PERIOD,-4) as PROCESSING_PERIOD,
--        STR_ONGOING as CLAWBACK_TYPE,
--         rl.CLAWBACK_NAME       as CLAWBACK_NAME,
--          STR_CB_NAME as CLAWBACK_METHOD,
--        ct.credittypeid as CREDITTYPE,
--        crd.creditseq as CREDITSEQ,
--        crd.name as CREDIT_NAME,
--        crd.value as CREDIT_VALUE,
--         crd.positionseq as crd_positionseq,
--        ip.genericdate2 as crd_genericdate2,
--        crd.ruleseq as crd_ruleseq,
--        pm.measurementseq as PM_SEQ,
--        pm.name as PM_NAME,
--        case rl.CLAWBACK_NAME
--         when 'NLPI_ONG' then pct.contributionvalue*V_NLPI_RATE
--         when 'NADOR' then pct.contributionvalue*V_NADOR_RATE
--         else
--          pct.contributionvalue
--        end as PM_CONTRIBUTION_VALUE,
--        case rl.CLAWBACK_NAME
--         when 'FYO_ONG' then fyo_rate.value
--         when 'RYO_ONG' then ryo_rate.value
--         when 'FSM_RYO_ONG' then ryo_rate.value
--         when 'NLPI_ONG' then V_NLPI_RATE
--         when 'NADOR' then V_NADOR_RATE
--       else 1
--         end as PM_RATE,
--      --1 as PM_RATE,
--        '' as DEPOSITSEQ,
--        '' as DEPOSIT_NAME,
--        '' as DEPOSIT_VALUE,
--        crd.periodseq as PERIODSEQ,
--        ip.salestransactionseq as SALESTRANSACTIONSEQ,
--        crd.genericattribute2 as PRODUCT_NAME,
--        crd.genericnumber1 as POLICY_YEAR,
--        ip.genericnumber2      as COMMISSION_RATE,
--        ip.genericdate4 as PAID_TO_DATE,
--        P_BATCH_NO as BATCH_NUMBER,
--        sysdate as CREATED_DATE
--   FROM aias_tx_temp ip
--  inner join CS_CREDIT crd
--     on ip.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
--    and crd.genericattribute12 = ip.wri_agt_code
--    and crd.periodseq = ip.periodseq
--  inner join CS_PMCREDITTRACE pct
--     on crd.CREDITSEQ = pct.CREDITSEQ
--    -- and pct.targetperiodseq= ip.periodseq
--  inner join CS_MEASUREMENT pm
--     on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
--      --    and pct.targetperiodseq= pm.periodseq
--  inner join CS_POSITION pm_pos
--     on pm.positionseq = pm_pos.ruleelementownerseq
--    and pm_pos.removedate = DT_REMOVEDATE
--    and pm_pos.effectivestartdate <= crd.genericdate2
--    and pm_pos.effectiveenddate > crd.genericdate2
--  inner join CS_CREDITTYPE ct
--     on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--    and ct.Removedate = DT_REMOVEDATE
--  inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--                from AIA_CB_RULES_LOOKUP
--               where RULE_TYPE = 'PM'
--                 AND CLAWBACK_NAME  IN ('FYO_ONG','RYO_ONG','FSM_RYO_ONG','NLPI_ONG','NADOR')) rl
--     on pm.NAME = rl.SOURCE_RULE_OUTPUT
--     and pm.periodseq = ts_periodseq
--  inner join  (select distinct
--                      cb_quarter_name,
--                      cb_startdate,
--                      cb_enddate
--                 from aia_cb_period
--                where cb_name = STR_CB_NAME) cbp
--     on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
--  inner join cs_position dep_pos
--     on pm.positionseq = dep_pos.ruleelementownerseq
--    and dep_pos.removedate = DT_REMOVEDATE
--    and dep_pos.effectivestartdate <= crd.genericdate2
--    and dep_pos.effectiveenddate > crd.genericdate2
--      --for lookup the receiver info.
--  inner join cs_title dep_title
--     on dep_pos.titleseq = dep_title.ruleelementownerseq
--    and dep_title.removedate = DT_REMOVEDATE
--    and dep_title.effectivestartdate <= crd.genericdate2
--    and dep_title.effectiveenddate > crd.genericdate2
-- left join vw_lt_fyo_rate fyo_rate
-- on fyo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
--   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
--   and fyo_rate.Receiver_title = dep_title.name
--   and rl.CLAWBACK_NAME = 'FYO'
-- left join vw_lt_ryo_life_rate ryo_rate
-- on ryo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
-- and ryo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
-- and ryo_rate.Receiver_title = dep_title.name
-- and rl.CLAWBACK_NAME in ( 'RYO','FSM_RYO')
--  WHERE to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
--    AND crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--    AND crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--    AND pm.tenantid='AIAS' and   pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--    AND pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ    ;
--*/
--insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
-- select  /*+ LEADING(aias_tx_temp2,pm) PARALLEL */ STR_BUNAME as BUNAME,
--       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--        ip.ponumber as POLICY_NUMBER,
--        ip.policyidseq as POLICYIDSEQ,
--        pm.positionseq as PAYEE_SEQ,
--        substr(pm_pos.name, 4) as PAYEE_CODE,
--        crdga12 as PAYOR_CODE,
--        ip.life_number as LIFE_NUMBER,
--        ip.coverage_number as COVERAGE_NUMBER,
--        ip.rider_number as RIDER_NUMBER,
--        ip.component_code as COMPONENT_CODE,
--        ip.component_name as COMPONENT_NAME,
--        ip.base_rider_ind as BASE_RIDER_IND,
--        crdcompdate as TRANSACTION_DATE,
--        substr(ONGOING_PERIOD,1,3)||'-'||substr(ONGOING_PERIOD,-4) as PROCESSING_PERIOD,
--        STR_ONGOING as CLAWBACK_TYPE,
--         rl.CLAWBACK_NAME       as CLAWBACK_NAME,
--          STR_CB_NAME as CLAWBACK_METHOD,
--        credittypeid as CREDITTYPE,
--        creditseq as CREDITSEQ,
--        crdname as CREDIT_NAME,
--        crdvalue as CREDIT_VALUE,
--         crdpositionseq as crd_positionseq,
--        crdgd2 as crd_genericdate2,
--        crdRuleSeq as crd_ruleseq,
--        pm.measurementseq as PM_SEQ,
--        pm.name as PM_NAME,
--        case rl.CLAWBACK_NAME
--         when 'NLPI_ONG' then ip.contributionvalue*V_NLPI_RATE
--         when 'NADOR' then ip.contributionvalue*V_NADOR_RATE
--         else
--          ip.contributionvalue
--        end as PM_CONTRIBUTION_VALUE,
--        case rl.CLAWBACK_NAME
--         when 'FYO_ONG' then fyo_rate.value
--         --Added by Suresh
--         when 'NEW_FYO_ONG' then new_fyo_rate.value
--         when 'RYO_ONG' then ryo_rate.value
--         when 'NEW_RYO_ONG' then new_ryo_rate.value
--         when 'FSM_RYO_ONG' then ryo_rate.value
--         when 'NLPI_ONG' then V_NLPI_RATE
--         when 'NADOR' then V_NADOR_RATE
--         --added by Suresh
--       else 1
--         end as PM_RATE,
--      --1 as PM_RATE,
--        '' as DEPOSITSEQ,
--        '' as DEPOSIT_NAME,
--        '' as DEPOSIT_VALUE,
--        crdperiodseq as PERIODSEQ,
--        ip.salestransactionseq as SALESTRANSACTIONSEQ,
--        crdga2 as PRODUCT_NAME,
--        crdgn1 as POLICY_YEAR,
--        ip.genericnumber2      as COMMISSION_RATE,
--        ip.genericdate4 as PAID_TO_DATE,
--        P_BATCH_NO as BATCH_NUMBER,
--        sysdate as CREATED_DATE
--from aias_tx_temp2 ip
--   inner join CS_MEASUREMENT pm
--     on pm.MEASUREMENTSEQ = ip.MEASUREMENTSEQ
-- and pm.tenantid='AIAS' and pm.processingunitseq=v_processingunitseq
--  inner join CS_POSITION pm_pos
--     on pm.positionseq = pm_pos.ruleelementownerseq
--    and pm_pos.removedate = '1-jan-2200'
--    and pm_pos.effectivestartdate <= ip.crdGD2
--    and pm_pos.effectiveenddate > ip.crdGD2
--    inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--                from AIA_CB_RULES_LOOKUP
--               where RULE_TYPE = 'PM'
----Changed by Suresh
----Add AI NL20180308
--                AND CLAWBACK_NAME  IN ('FYO_ONG','NEW_FYO_ONG','RYO_ONG','NEW_RYO_ONG','FSM_RYO_ONG','NLPI_ONG','NADOR','AI_ONG')) rl
----end by Suresh
--     on pm.NAME = rl.SOURCE_RULE_OUTPUT
--     and pm.periodseq = ts_periodseq
--     inner join cs_position dep_pos
--     on pm.positionseq = dep_pos.ruleelementownerseq
--    and dep_pos.removedate = '1-jan-2200'
--    and dep_pos.effectivestartdate <= crdGD2
--    and dep_pos.effectiveenddate > crdGD2
--      --for lookup the receiver info.
--  inner join cs_title dep_title
--     on dep_pos.titleseq = dep_title.ruleelementownerseq
--    and dep_title.removedate = '1-jan-2200'
--    and dep_title.effectivestartdate <= crdGD2
--    and dep_title.effectiveenddate > crdGD2
-- left join vw_lt_fyo_rate fyo_rate
-- on fyo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
--   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
--   and fyo_rate.Receiver_title = dep_title.name
--   --Changed by Suresh
--   and rl.CLAWBACK_NAME = 'FYO_ONG'
--   --end by Suresh
-- left join vw_lt_ryo_life_rate ryo_rate
-- on ryo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
-- and ryo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
-- and ryo_rate.Receiver_title = dep_title.name
-- --Added by Suresh
-- and rl.CLAWBACK_NAME in ( 'RYO_ONG','FSM_RYO_ONG')
--  --for lookup PM rate for New FYO
-- left join vw_lt_new_fyo_rate new_fyo_rate
-- on new_fyo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
-- and new_fyo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
-- and new_fyo_rate.Receiver_title = dep_title.name
-- and rl.CLAWBACK_NAME = 'NEW_FYO_ONG'
--  --for lookup PM rate for New RYO
-- left join VW_LT_NEW_RYO_LIFE_RATE new_ryo_rate
-- on new_ryo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
--   and new_ryo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
--   and new_ryo_rate.Receiver_title = dep_title.name
--   and rl.CLAWBACK_NAME = 'NEW_RYO_ONG'
----End by Suresh
--  WHERE to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbpenddate
--       AND pm.tenantid='AIAS' and   pm.PROCESSINGUNITSEQ = v_processingunitseq
--
--;
--
--
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for Ongoing 2' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--/*
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI Ongoing, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
--
----for SPI on-going
----for on-going compensation trace forward
--insert into AIA_CB_TRACE_FORWARD_COMP
--  with cb_period as
--   (select p.periodseq, p.name
--      from cs_period a
--     inner join cs_period p
--        on p.startdate >= a.startdate
--       and p.enddate <= a.enddate
--     inner join cs_periodtype cpt_qtr
--        on a.periodtypeseq = cpt_qtr.periodtypeseq
--       and cpt_qtr.name = 'quarter'
--     inner join cs_periodtype cpt_mon
--        on p.periodtypeseq = cpt_mon.periodtypeseq
--       and cpt_mon.name = 'month'
--     where a.calendarseq = V_CALENDARSEQ
--       and p.calendarseq = V_CALENDARSEQ
--       and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) between a.startdate and
--           (a.enddate - 1)
--       and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= p.startdate)
--  select STR_BUNAME as BUNAME,
--         ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
--         ip.ponumber as POLICY_NUMBER,
--         ip.policyidseq as POLICYIDSEQ,
--         pm.positionseq as PAYEE_SEQ,
--         substr(pm_pos.name, 4) as PAYEE_CODE,
--         crd.genericattribute12 as PAYOR_CODE,
--         ip.life_number as LIFE_NUMBER,
--         ip.coverage_number as COVERAGE_NUMBER,
--         ip.rider_number as RIDER_NUMBER,
--         ip.component_code as COMPONENT_CODE,
--         ip.component_name as COMPONENT_NAME,
--         ip.base_rider_ind as BASE_RIDER_IND,
--         crd.compensationdate as TRANSACTION_DATE,
--         substr(ONGOING_PERIOD, 1, 3) || '-' || substr(ONGOING_PERIOD, -4) as PROCESSING_PERIOD,
--         STR_ONGOING as CLAWBACK_TYPE,
--         rl.CLAWBACK_NAME as CLAWBACK_NAME,
--         STR_CB_NAME as CLAWBACK_METHOD,
--         ct.credittypeid as CREDITTYPE,
--         crd.creditseq as CREDITSEQ,
--         crd.name as CREDIT_NAME,
--         crd.value as CREDIT_VALUE,
--         crd.positionseq as crd_positionseq,
--         st.genericdate2 as crd_genericdate2,
--         crd.ruleseq as crd_ruleseq,
--         pm.measurementseq as PM_SEQ,
--         pm.name as PM_NAME,
--         pct.contributionvalue as PM_CONTRIBUTION_VALUE,
--         1 as PM_RATE,
--         '' as DEPOSITSEQ,
--         '' as DEPOSIT_NAME,
--         '' as DEPOSIT_VALUE,
--         crd.periodseq as PERIODSEQ,
--         st.salestransactionseq as SALESTRANSACTIONSEQ,
--         crd.genericattribute2 as PRODUCT_NAME,
--         crd.genericnumber1 as POLICY_YEAR,
--         st.genericnumber2 as COMMISSION_RATE,
--         st.genericdate4 as PAID_TO_DATE,
--         P_BATCH_NO as BATCH_NUMBER,
--         sysdate as CREATED_DATE
--    FROM CS_SALESTRANSACTION st
--   inner join CS_CREDIT crd
--      on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
--   inner join CS_PMCREDITTRACE pct
--      on crd.CREDITSEQ = pct.CREDITSEQ
--   inner join CS_MEASUREMENT pm
--      on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
--   inner join CS_POSITION pm_pos
--      on pm.positionseq = pm_pos.ruleelementownerseq
--     and pm_pos.removedate = DT_REMOVEDATE
--     and pm_pos.effectivestartdate <= crd.genericdate2
--     and pm_pos.effectiveenddate > crd.genericdate2
--   inner join CS_CREDITTYPE ct
--      on crd.CREDITTYPESEQ = ct.DATATYPESEQ
--     and ct.Removedate = DT_REMOVEDATE
--   inner join cb_period
--      on pm.periodseq = cb_period.periodseq
--     and crd.periodseq = cb_period.periodseq
--   inner join AIA_CB_IDENTIFY_POLICY ip
--      on st.PONUMBER = ip.PONUMBER
--     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
--     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
--     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
--     AND st.PRODUCTID = ip.COMPONENT_CODE
--     and crd.genericattribute12 = ip.wri_agt_code
--   inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
--                 from AIA_CB_RULES_LOOKUP
--                where RULE_TYPE = 'PM'
--                  AND CLAWBACK_NAME = 'SPI_ONG') rl
--      on pm.NAME = rl.SOURCE_RULE_OUTPUT
--   inner join cs_position dep_pos
--      on pm.positionseq = dep_pos.ruleelementownerseq
--     and dep_pos.removedate = DT_REMOVEDATE
--     and dep_pos.effectivestartdate <= crd.genericdate2
--     and dep_pos.effectiveenddate > crd.genericdate2
--  --for lookup the receiver info.
--   inner join cs_title dep_title
--      on dep_pos.titleseq = dep_title.ruleelementownerseq
--     and dep_title.removedate = DT_REMOVEDATE
--     and dep_title.effectivestartdate <= crd.genericdate2
--     and dep_title.effectiveenddate > crd.genericdate2
--   inner join (select distinct
--                      cb_quarter_name,
--                      cb_startdate,
--                      cb_enddate
--                 from aia_cb_period
--                where cb_name = STR_COMPENSATION) cbp
--      on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
--   WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--     AND st.BUSINESSUNITMAP = 1
--     --to avoid fetching the transactions which not being processed by lumpsum procedure
--     and to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
--     ;
--
--Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI Ongoing' || '; row count: ' || to_char(sql%rowcount));
--
--commit;*/
--
--end if;
--
--<<ProcDone>>
--NULL;
--
--end  SP_TRACE_FORWARD_COMP;
--
--
--PROCEDURE SP_CLAWBACK_COMP (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO IN INTEGER) as
--
--STR_LUMPSUM CONSTANT VARCHAR2(20) := 'LUMPSUM';
--STR_ONGOING CONSTANT VARCHAR2(20) := 'ONGOING';
--STR_BUNAME CONSTANT VARCHAR2(20) := 'SGPAGY';
--STR_DATE_FORMAT CONSTANT VARCHAR2(50) := 'yyyymmdd';
--STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION';
--STR_CALENDARNAME  CONSTANT VARCHAR2(50) := 'AIA Singapore Calendar';
--V_CAL_PERIOD VARCHAR2(30); --measurement quarter
--DT_REMOVEDATE CONSTANT DATE := TO_DATE('22000101', 'yyyymmdd');
--DT_CB_START_DATE DATE;
--DT_CB_END_DATE DATE;
--DT_INCEPTION_START_DATE DATE;
--DT_INCEPTION_END_DATE DATE;
--DT_WEEKLY_START_DATE DATE;
--DT_WEEKLY_END_DATE DATE;
--DT_ONGOING_START_DATE DATE;
--DT_ONGOING_END_DATE DATE;
--NUM_OF_CYCLE_IND integer;
--STR_DATE_FORMAT_TYPE    CONSTANT VARCHAR2(50) := 'yyyy-mm-dd';
--V_REC_COUNT INTEGER;
--V_NLPI_RATE NUMBER(10,2);
--INT_SVI_RATE NUMBER(10,2) := 0.60;
--V_BATCH_NO_PRE_QTR INTEGER;
--V_CB_TYPE VARCHAR2(50);
--V_CB_NAME VARCHAR2(50);
--STR_STATUS_COMPLETED_SH CONSTANT VARCHAR2(20) := 'completed_sh';
--V_CB_QTR VARCHAR2(50);
--
--begin
--
--
--Log('SP_CLAWBACK_COMP start');
--
--init;
--
----get records count from AIA_CB_CLAWBACK_COMP
--select count(1)
--  into V_REC_COUNT
--  from AIA_CB_CLAWBACK_COMP
-- where batch_no = P_BATCH_NO;
--
----delete the records in AIA_CB_CLAWBACK_COMP if batch number is being reused.
--if V_REC_COUNT > 0 then
--
--delete from AIA_CB_CLAWBACK_COMP where batch_no = P_BATCH_NO;
--delete from AIA_CB_CLAWBACK_SVI_COMP_TMP where batch_no = P_BATCH_NO;
--
--commit;
--
--END IF;
--
--Log('insert into AIA_CB_CLAWBACK_COMP for FYO,RYO, NADOR' ||' batch_no = ' || P_BATCH_NO);
--
----insert data into AIA_CB_CLAWBACK_COMP for compensation for FYO, RYO and NADOR
--insert into AIA_CB_CLAWBACK_COMP
--  select tf.calculation_period as MEASUREMENT_QUARTER,
--         tf.clawback_type as CLAWBACK_TYPE,
--         tf.clawback_name as CLAWBACK_NAME,
--         STR_CB_NAME as CLAWBACK_METHOD,
--           to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
--         Agency_code.genericattribute3 as WRI_DIST_CODE,
--         trim(District_name.firstname||' '||District_name.lastname) as WRI_DIST_NAME,
--         DM_code.genericattribute2 as  WRI_DM_CODE,
--           --substr(pos_agy.name, 4) as WRI_AGY_CODE,
--         --pos_agy.genericattribute1 as WRI_AGY_CODE,
--         agent.genericattribute1 as WRI_AGY_CODE,
----         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME,
--trim(Agency_name.firstname||' '||Agency_name.lastname)  as  WRI_AGY_NAME,
--         agent.genericattribute2 as wri_agy_ldr_code,
--         agent.genericattribute7 as wri_agy_ldr_name,
--         tf.payor_code as WRI_AGT_CODE,
--         trim(Agent_name.firstname||' '||Agent_name.lastname)  as wri_agt_name  ,
--         decode(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
--         title_agt.name as RANK,
--          agent.genericattribute4 as UM_CLASS,
--         agent.genericattribute11 as UM_RANK, -- Check cr.genericattribute14 as CLASS,
--         ba.bsc_grade as FSC_BSC_GRADE,
--         ba.entitlementpercent as FSC_BSC_PERCENTAGE,
--         tf.policy_number as PONUMBER,
--         tf.life_number as LIFE_NUMBER,
--         tf.coverage_number as COVERAGE_NUMBER,
--         tf.RIDER_NUMBER as RIDER_NUMBER,
--         tf.COMPONENT_NAME as COMPONENT_NAME,
--         tf.component_code as COMPONENT_CODE,
--         tf.PRODUCT_NAME as PRODUCT_NAME,
--         tf.transaction_date as TRANSACTION_DATE,
--         tf.policy_year as POLICY_YEAR,
--          case
--           when tf.credit_type  in ('FYC','FYC_W','FYC_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as FYC,
--         case
--           when tf.credit_type in ('API','API_W','API_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as API,
--         case
--           when tf.credit_type  in ('SSCP','SSCP_W','SSCP_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as SSC,
--         case
--           when tf.credit_type  in ('RYC','RYC_W','RYC_W_DUPLICATE','ORYC_W','ORYC_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as RYC,
--         case
--           when tf.clawback_name  in ('FYO','FYO_ONG') then
--            tf.pm_contribution_value
--           else
--            0
--         end as FYO,
--         case
--           when tf.clawback_name  in ('RYO','RYO_ONG') then
--            tf.pm_contribution_value
--           else
--            0
--         end as RYO,
--          case when tf.clawback_name   in ('FSM_RYO','FSM_RYO_ONG') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
--   when tf.clawback_name   in ('FSM_RYO','FSM_RYO_ONG')  then tf.pm_contribution_value  else 0 end  as FSM_RYO,
--          case
--           when tf.clawback_name  ='NADOR' then
--            tf.pm_contribution_value
--           else
--            0
--         end as NADOR,
--   case when tf.clawback_name in ('NLPI','NLPI_ONG')and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
--when  tf.clawback_name in ('NLPI','NLPI_ONG') then   tf.pm_contribution_value else 0 end as NLPI,
--         0 as SPI,
--      case
--       when tf.clawback_name  in ('NLPI','NLPI_ONG') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
--       when tf.clawback_name   in ('FSM_RYO','FSM_RYO_ONG') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
--       else  tf.pm_contribution_value end *0.60 as SVI,
--       case
--             when tf.clawback_name  in ('NLPI','NLPI_ONG') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
--       when tf.clawback_name   in ('FSM_RYO','FSM_RYO_ONG') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
--       else  tf.pm_contribution_value end *0.60* ba.entitlementpercent as ENTITLEMENT,
--         round((( case when tf.clawback_name  in ('NLPI','NLPI_ONG') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
--       when tf.clawback_name   in ('FSM_RYO','FSM_RYO_ONG') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
--       else  tf.pm_contribution_value end *0.60) -
--         (case when tf.clawback_name  in ('NLPI','NLPI_ONG') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
--       when tf.clawback_name   in ('FSM_RYO','FSM_RYO_ONG') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
--       else  tf.pm_contribution_value end *0.60* ba.entitlementpercent ))* (-1),2) as CLAWBACK_VALUE,
--         0 as PROCESSED_CLAWBACK,
--          tf.base_rider_ind as BASIC_RIDER_IND,
--         tf.salestransactionseq,
--         tf.creditseq,
--         tf.pm_seq,
--         P_BATCH_NO,
--         pos_agy_rcr.GENERICATTRIBUTE2 as RCVR_AGY_LDR_CODE ,
--         pos_agy_rcr.genericattribute11 as RCVR_AGY_LDR_RANK,
--         case rul.EXPRESSIONTYPEFORTYPE
--             when 256 then 'DIRECT'
--             when 1024 then 'INDIRECT'
--            else '0'
--         end as REPORT_TYPE,
--        --Added by Suresh
--        0 as OFFSET_CLAWBACK,
--         case
--           when tf.clawback_name  in ('NEW_FYO','NEW_FYO_ONG') then
--            tf.pm_contribution_value
--           else
--            0
--         end as NEW_FYO,
--           case
--           when tf.clawback_name  in ('NEW_RYO','NEW_RYO_ONG') then
--            tf.pm_contribution_value
--           else
--            0
--         end as NEW_RYO,
--         --End by Suresh
--         --add AI NL20180308
--         case
--          when tf.credit_type  in ('AI') then
--            tf.credit_value
--           else
--            0
--         end as AI
--    from AIA_CB_TRACE_FORWARD_COMP tf
--   inner join aia_cb_bsc_agent ba
--      on tf.calculation_period = (ba.quarter || ' ' || ba.year)
--     and tf.payor_code = ba.agentcode
--   inner join CS_CREDIT cr
--      on tf.creditseq = cr.creditseq
--   inner join cs_rule rul
--     on cr.ruleseq=rul.ruleseq
--     and rul.REMOVEDATE=DT_REMOVEDATE
--     and rul.islast = 1
--      inner join cs_position pos_agy_rcr
--        on pos_agy_rcr.ruleelementownerseq = cr.positionseq
--        /*AND pos_agy_rcr.removedate = DT_REMOVEDATE
--        and pos_agy_rcr.islast = 1 */
--            AND pos_agy_rcr.effectivestartdate <= tf.CRD_GENERICDATE2
--        AND pos_agy_rcr.effectiveenddate   >  tf.CRD_GENERICDATE2
--        AND pos_agy_rcr.removedate = DT_REMOVEDATE
-- inner join cs_position Agent
--        on Agent.name = 'SGT'||tf.payor_code
--        AND Agent.effectivestartdate <= tf.CRD_GENERICDATE2
--        AND Agent.effectiveenddate   >  tf.CRD_GENERICDATE2
--        AND Agent.removedate = DT_REMOVEDATE
--inner join cs_participant Agent_name
--        on Agent.payeeseq = Agent_name.payeeseq
--        AND Agent_name.effectivestartdate <= tf.CRD_GENERICDATE2
--        AND Agent_name.effectiveenddate   > tf.CRD_GENERICDATE2
--        AND Agent_name.removedate = DT_REMOVEDATE
--     inner join cs_position Agency_code
--        on 'SGY'||agent.genericattribute1 = Agency_code.name
--        AND Agency_code.effectivestartdate <= tf.CRD_GENERICDATE2
--        AND Agency_code.effectiveenddate   > tf.CRD_GENERICDATE2
--        AND Agency_code.removedate = DT_REMOVEDATE
--             inner join cs_participant Agency_name
--       on Agency_code.payeeseq = Agency_name.payeeseq
--       and Agency_name.effectivestartdate <= tf.CRD_GENERICDATE2
--        AND Agency_name.effectiveenddate   > tf.CRD_GENERICDATE2
--        AND Agency_name.removedate = DT_REMOVEDATE
-- inner join cs_position DM_code
--        --on 'SGY'||agent.genericattribute3 = DM_code.name
--        on 'SGY'||Agency_code.genericattribute3 = DM_code.name
--        AND DM_code.effectivestartdate <= tf.CRD_GENERICDATE2
--        AND DM_code.effectiveenddate   > tf.CRD_GENERICDATE2
--        AND DM_code.removedate = DT_REMOVEDATE
--  inner join cs_participant District_name
--       on dm_code.payeeseq = district_name.payeeseq
--       and District_name.effectivestartdate <= tf.CRD_GENERICDATE2
--        AND District_name.effectiveenddate   > tf.CRD_GENERICDATE2
--        AND District_name.removedate = DT_REMOVEDATE
--         inner join cs_title title_agt
--     on title_agt.RULEELEMENTOWNERSEQ = Agent.TITLESEQ
--     AND title_agt.effectivestartdate <= tf.CRD_GENERICDATE2
--     AND title_agt.effectiveenddate   > tf.CRD_GENERICDATE2
--     AND title_agt.REMOVEDATE = DT_REMOVEDATE
--   --chaned by Suresh
--   --add AI NL20180308
--   where tf.clawback_name in ('FYO','FYO_ONG','NEW_FYO','NEW_FYO_ONG','RYO','RYO_ONG','NEW_RYO','NEW_RYO_ONG','NADOR','FSM_RYO','FSM_RYO_ONG','NLPI','NLPI_ONG','AI','AI_ONG')
--   --End by Suresh
--   and tf.batch_number = P_BATCH_NO
--   ;
--
--Log('insert into AIA_CB_CLAWBACK_COMP for FYO, RYO, NADOR' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--/*
---- Delete records from temp tables for FSM_RYO with the same batch number
--delete from AIA_CB_CLAWBACK_TEMP_FSM where batch_number = P_BATCH_NO;
--commit;
--delete from AIA_CB_CLAWBACK_COMP_FSM where batch_number = P_BATCH_NO;
--commit;
--delete from AIA_CB_CLAWBACK_COMP_FSM_CR where batch_number = P_BATCH_NO;
--commit;
--
--
--
---- Temp table for FSM RYO to get the distinct group by columns
--INSERT INTO AIA_CB_CLAWBACK_TEMP_FSM
--select distinct clawback_name,POLICY_NUMBER,policy_year,PAYEE_CODE,payor_code,crd_positionseq, crd_genericdate2,credit_type,
--LIFE_NUMBER,COVERAGE_NUMBER,RIDER_NUMBER,COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--from AIA_CB_TRACE_FORWARD_COMP where clawback_name in ('FSM_RYO','FSM_RYO_ONG') and batch_number=P_BATCH_NO;
--commit;
--
---- Temp table for FSM_RYO to get the pm_contribution_value for the 3 Primary measurements.
--insert into AIA_CB_CLAWBACK_COMP_FSM
--select clawback_name,policy_number, policy_year,payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_NOT_MANAGER_PERSONAL_SGPAGY',nvl(pm_value,0))) as PM_DIRECT_TEAM_NOT_MANAGER,
--     sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_MANAGER_PERSONAL',nvl(pm_value,0))) as PM_DIRECT_TEAM_MANAGER,
--    sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY',nvl(pm_value,0))) as PM_DIRECT_TEAM_Exclude,
--    sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_NOT_MANAGER_PERSONAL_SGPAGY',nvl(pm_value,0)))+ sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_MANAGER_PERSONAL',nvl(pm_value,0)))
--    - sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY',nvl(pm_value,0))) as pm_contribution_value,
--    sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_NOT_MANAGER_PERSONAL_SGPAGY',nvl(credit_value,0))) as CR_DIRECT_TEAM_NOT_MANAGER,
--    sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_MANAGER_PERSONAL',nvl(credit_value,0))) as CR_DIRECT_TEAM_MANAGER,
--    sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_NOT_MANAGER_PERSONAL_SGPAGY',nvl(credit_value,0)))+ sum(decode(pm_name,'PM_RYO_LIFE_FSM_DIRECT_TEAM_MANAGER_PERSONAL',nvl(credit_value,0)))
--    as RYC_Credit_value
--from (
-- select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code, crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_RYO_LIFE_FSM_DIRECT_TEAM_NOT_MANAGER_PERSONAL_SGPAGY' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value  from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_RYO_LIFE_FSM_DIRECT_TEAM_NOT_MANAGER_PERSONAL_SGPAGY' and clawback_name in ('FSM_RYO','FSM_RYO_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_FSM where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number, policy_year,payee_code,payor_code, crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select  clawback_name,pm_name,policy_number, policy_year,payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_RYO_LIFE_FSM_DIRECT_TEAM_MANAGER_PERSONAL' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_RYO_LIFE_FSM_DIRECT_TEAM_MANAGER_PERSONAL' and clawback_name in ('FSM_RYO','FSM_RYO_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_FSM where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number, policy_year,payee_code, payor_code,crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' and clawback_name in ('FSM_RYO','FSM_RYO_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_FSM where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2, credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number)
--group by clawback_name,policy_number, policy_year,payee_code, payor_code,crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number;
--commit;
--
---- To get the associated creditseq for these records
--insert into AIA_CB_CLAWBACK_COMP_FSM_CR
--select  b.*,a.creditseq as creditseq, a.salestransactionseq as salestransactionseq  from
--(select * from AIA_CB_TRACE_FORWARD_COMP where  clawback_name in ('FSM_RYO','FSM_RYO_ONG') and batch_number = P_BATCH_NO and credit_value <> 0 and pm_name not in (
--'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY'))A
--right outer join
--(Select * from AIA_CB_CLAWBACK_COMP_FSM where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number;
--
--Log('insert into AIA_CB_CLAWBACK_COMP for FSM_RYO' ||' batch_no = ' || P_BATCH_NO);
--
----insert data into AIA_CB_CLAWBACK_COMP for compensation for FSM RYO
--insert into AIA_CB_CLAWBACK_COMP
--select  distinct b.calculation_period as measurement_quarter,
--A.clawback_type as clawback_type,
--b.Clawback_name as Clawback_name,
--STR_CB_NAME as CLAWBACK_METHOD,
--to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
-- pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE,
--         trim(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME,
--         pos_dis.genericattribute2 as WRI_DM_CODE,
--           --substr(pos_agy.name, 4) as WRI_AGY_CODE,
--         pos_agy.genericattribute1 as WRI_AGY_CODE,
----         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME,
--trim(part_agy.firstname||' '||part_agy.lastname) as WRI_AGY_NAME,
--         pos_agt.GENERICATTRIBUTE2 as WRI_AGY_LDR_CODE,
--         pos_agt.genericattribute7 as WRI_AGY_LDR_NAME,
--         a.payor_code as WRI_AGT_CODE,
--         trim(par_agt.firstname||' '||par_agt.lastname) as WRI_AGT_NAME,
--         decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
--         title_agt.name as RANK,
--         pos_agt.genericattribute4 as CLASS,
--         pos_agt.genericattribute11 as UM_RANK, -- Check
--         ba.bsc_grade as FSC_BSC_GRADE,
--         ba.entitlementpercent as FSC_BSC_PERCENTAGE,
--b.policy_number as PONUMBER,
--         b.life_number as LIFE_NUMBER,
--         b.coverage_number as COVERAGE_NUMBER,
--         b.RIDER_NUMBER as RIDER_NUMBER,
--         b.COMPONENT_NAME as COMPONENT_NAME,
--         b.component_code as COMPONENT_CODE,
--         b.PRODUCT_NAME as PRODUCT_NAME,
--a.transaction_date as transaction_date,
--b.policy_year as policy_year,
--0 as FYC,
--0 as API,
--0 as SSC,
--a.ryc_credit_value as RYC,
--0 as FYO,
--0 as RYO,
--a.pm_contribution_value as FSM_RYO,
--0 as NADOR,
--0 as NLPI,
--0 as SPI,
--a.pm_contribution_value * INT_SVI_RATE as SVI, -- * INT_SVI_RATE
--(a.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent as ENTITLEMENT,
--         round((((a.pm_contribution_value * INT_SVI_RATE) -
--         (a.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent)) * (-1),2) as CLAWBACK,
--         0 as PROCESSED_CLAWBACK,
--b.base_rider_ind,
--b.salestransactionseq as salestransactionseq,
--b.creditseq as creditseq,
--b.pm_seq as pmseq,
--P_BATCH_NO as batch_number,
--pos_agy_rcr.GENERICATTRIBUTE2 as RCVR_AGY_LDR_CODE ,
--         pos_agy_rcr.genericattribute11 as RCVR_AGY_LDR_RANK,
--          case rul.EXPRESSIONTYPEFORTYPE
--             when 256 then 'DIRECT'
--             when 1024 then 'INDIRECT'
--            else '0'
--         end as REPORT_TYPE,
--         0 as OFFSET_CLAWBACK
--         from
--(select * from AIA_CB_CLAWBACK_COMP_FSM_CR where batch_number=P_BATCH_NO)A inner join
--(select * from AIA_CB_TRACE_FORWARD_COMP where clawback_name in ('FSM_RYO','FSM_RYO_ONG') and batch_number=P_BATCH_NO and pm_name not in (
--'PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY')) B
--on  A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq= b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name = b.clawback_name
--and a.batch_number=b.batch_number
--   inner join cs_rule rul
--     on b.CRD_RULESEQ=rul.ruleseq
--     and rul.REMOVEDATE=DT_REMOVEDATE
--     and rul.islast = 1
--inner join aia_cb_bsc_agent ba
--on b.calculation_period = (ba.quarter || ' ' || ba.year)
--     and b.payor_code = ba.agentcode
--         --for receiving Agency leader info
--     inner join cs_position pos_agy_rcr
--        on pos_agy_rcr.ruleelementownerseq = a.crd_positionseq
--        AND pos_agy_rcr.removedate = DT_REMOVEDATE
--        and pos_agy_rcr.islast = 1
--          --for writing Agency postion info
--   inner join cs_position pos_agy
--        on pos_agy.name = 'SGT'||b.payor_code
--        AND pos_agy.removedate = DT_REMOVEDATE
--        and pos_agy.islast = 1
--      /* --for writing Agency participant info
--     inner join cs_participant par_agy
--        on par_agy.PAYEESEQ = pos_agy.PAYEESEQ
--        AND par_agy.effectivestartdate <= cr.genericdate2
--        AND par_agy.effectiveenddate   >  cr.genericdate2
--        AND par_agy.removedate = to_date('01012200','ddmmyyyy') */
--        --for writing Agency participant infoto join position
--    /*    inner join cs_position par_agy_pos
--        on par_agy_pos.name =  'SGY'||pos_agy.genericattribute1
--        and par_agy_pos.removedate = DT_REMOVEDATE
--        and par_agy_pos.islast = 1
--             --for writing Agency participant info
--        inner join cs_participant part_agy
--        on par_agy_pos.payeeseq = part_agy.payeeseq
--             AND part_agy.effectivestartdate <= a.crd_genericdate2
--        AND part_agy.effectiveenddate   >  a.crd_genericdate2
--        and part_agy.removedate = DT_REMOVEDATE
--      --for writing District postion info
--     inner join cs_position pos_dis
--        on pos_dis.name= 'SGY' || pos_agy.genericattribute3
--        AND pos_dis.effectivestartdate <= a.crd_genericdate2
--        AND pos_dis.effectiveenddate   > a.crd_genericdate2
--        AND pos_dis.removedate = DT_REMOVEDATE
--     --for writing District participant info
--     inner join cs_participant par_dis
--        on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
--        AND par_dis.effectivestartdate <= a.crd_genericdate2
--        AND par_dis.effectiveenddate  > a.crd_genericdate2
--        AND par_dis.removedate = DT_REMOVEDATE
--     --for writing Agent postion info
--      inner join cs_position pos_agt
--        on 'SGT'||a.payor_code=pos_agt.name
--        and pos_agt.effectivestartdate <= a.crd_genericdate2
--        AND pos_agt.effectiveenddate   > a.crd_genericdate2
--        and pos_agt.removedate = DT_REMOVEDATE
--     --for writing Agent participant info
--     inner join cs_participant par_agt
--     on par_agt.payeeseq= pos_agt.PAYEESEQ
--     AND par_agt.effectivestartdate <= a.crd_genericdate2
--     AND par_agt.effectiveenddate  > a.crd_genericdate2
--     AND par_agt.removedate = DT_REMOVEDATE
--     --for payor agent title info
--     inner join cs_title title_agt
--     on title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
--     AND title_agt.effectivestartdate <= a.crd_genericdate2
--     AND title_agt.effectiveenddate   > a.crd_genericdate2
--     AND title_agt.REMOVEDATE = DT_REMOVEDATE;
--
--Log('insert into AIA_CB_CLAWBACK_COMP for FSM_RYO' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
---- Delete records from temp tables for NLPI with the same batch number
--delete from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number = P_BATCH_NO;
--commit;
--delete from AIA_CB_CLAWBACK_COMP_NLPI where batch_number = P_BATCH_NO;
--commit;
--delete from AIA_CB_CLAWBACK_COMP_NLPI_CR where batch_number = P_BATCH_NO;
--commit;
--
---- Temp table for NLPI to get the distinct group by columns
--insert into AIA_CB_CLAWBACK_TEMP_NLPI
--select distinct clawback_name,POLICY_NUMBER,policy_year,PAYEE_CODE,payor_code,crd_positionseq, crd_genericdate2,credit_type,
--LIFE_NUMBER,COVERAGE_NUMBER,RIDER_NUMBER,COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--from AIA_CB_TRACE_FORWARD_COMP where clawback_name in ('NLPI','NLPI_ONG') and batch_number=P_BATCH_NO;--P_BATCH_NO;
--
--commit;
--
---- Temp table for NLPI to get the pm_contribution_value for the 8 Primary measurements.
--insert into AIA_CB_CLAWBACK_COMP_NLPI
--select clawback_name,policy_number, policy_year,payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal',nvl(pm_value,0))) as Manager_Personal,
--     sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned',nvl(pm_value,0))) as Not_Assigned,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned',nvl(pm_value,0))) as Assigned,
--     sum(decode(pm_name,'PM_NLPI_PIB_Exclusion',nvl(pm_value,0))) as PIB_Exclusion,
--     sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal_NEW',nvl(pm_value,0))) as Manager_Personal_NEW,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned_NEW',nvl(pm_value,0))) as Not_Assigned_NEW,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned_NEW',nvl(pm_value,0))) as Assigned_NEW,
--    sum(decode(pm_name,'PM_NLPI_PIB_Exclusion_NEW',nvl(pm_value,0))) as PIB_Exclusion_NEW,
--    ((sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal',nvl(pm_value,0)))+ sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned',nvl(pm_value,0)))
--  + sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned',nvl(pm_value,0)))- sum(decode(pm_name,'PM_NLPI_PIB_Exclusion',nvl(pm_value,0))))
--  +
--  (sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal_NEW',nvl(pm_value,0)))+ sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned_NEW',nvl(pm_value,0)))
--  + sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned_NEW',nvl(pm_value,0)))- sum(decode(pm_name,'PM_NLPI_PIB_Exclusion_NEW',nvl(pm_value,0)))))
--  as pm_contribution_value,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal',nvl(credit_value,0))) as CR_Manager_Personal,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned',nvl(credit_value,0))) as CR_Not_Assigned,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned',nvl(credit_value,0))) as CR_Assigned,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal_NEW',nvl(credit_value,0))) as CR_Manager_Personal_NEW,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned_NEW',nvl(credit_value,0))) as CR_Not_Assigned_new,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned_NEW',nvl(credit_value,0))) as CR_Assigned_new,
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal',nvl(credit_value,0)))+ sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned',nvl(credit_value,0))) +
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned',nvl(credit_value,0)))+ sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Manager_Personal_NEW',nvl(credit_value,0))) +
--    sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Not_Assigned_NEW',nvl(credit_value,0)))+ sum(decode(pm_name,'PM_PIB_DIRECT_TEAM_Assigned_NEW',nvl(credit_value,0)))
--    as FYC_RYC_Credit_value
--from (
-- select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code, crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_PIB_DIRECT_TEAM_Manager_Personal' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value  from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_PIB_DIRECT_TEAM_Manager_Personal' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number, policy_year,payee_code,payor_code, crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number, policy_year,payee_code,payor_code,crd_positionseq, crd_genericdate2, credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_PIB_DIRECT_TEAM_Not_Assigned' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_PIB_DIRECT_TEAM_Not_Assigned' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number, policy_year,payee_code, payor_code,crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_PIB_DIRECT_TEAM_Assigned' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_PIB_DIRECT_TEAM_Assigned' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_NLPI_PIB_Exclusion' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_NLPI_PIB_Exclusion' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2, credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_PIB_DIRECT_TEAM_Manager_Personal_NEW' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_PIB_DIRECT_TEAM_Manager_Personal_NEW' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2, credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_PIB_DIRECT_TEAM_Not_Assigned_NEW' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_PIB_DIRECT_TEAM_Not_Assigned_NEW' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2, credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_PIB_DIRECT_TEAM_Assigned_NEW' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_PIB_DIRECT_TEAM_Assigned_NEW' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--union all
--select clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2, credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number,
-- sum(pm_value) as pm_value, sum(credit_value) as credit_value from (
--select * from (
--select b.*,'PM_NLPI_PIB_Exclusion_NEW' as pm_name, nvl(A.pm_contribution_value,0) as pm_value, nvl(a.credit_value,0) as credit_value from
--(select * from AIA_CB_TRACE_FORWARD_COMP where pm_name ='PM_NLPI_PIB_Exclusion_NEW' and clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0)A
--right outer join
--(Select * from AIA_CB_CLAWBACK_TEMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number)AA)
--group by clawback_name,pm_name,policy_number,policy_year, payee_code,payor_code,crd_positionseq, crd_genericdate2,credit_type, LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number
--)
--group by clawback_name,policy_number, policy_year,payee_code, payor_code,crd_positionseq, crd_genericdate2,credit_type,LIFE_NUMBER,COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE,COMPONENT_NAME,TRANSACTION_DATE,clawback_type,batch_number;
--
--commit;
--
---- To get the associated creditseq for these records
--insert into AIA_CB_CLAWBACK_COMP_NLPI_CR
--select  distinct b.*,a.creditseq as creditseq, a.salestransactionseq as salestransactionseq  from
--(select * from AIA_CB_TRACE_FORWARD_COMP where  clawback_name in ('NLPI','NLPI_ONG') and batch_number = P_BATCH_NO and credit_value <> 0 and pm_name not in (
--'PM_NLPI_PIB_Exclusion_NEW',
--'PM_NLPI_PIB_Exclusion'))A
--right outer join
--(Select * from AIA_CB_CLAWBACK_COMP_NLPI where batch_number=P_BATCH_NO)B
--on A.policy_number = b.policy_number
--and a.policy_year = b.policy_year
--and a.payee_code=b.payee_code
--and a.payor_code=b.payor_code
--and a.crd_positionseq = b.crd_positionseq
--and a.crd_genericdate2 = b.crd_genericdate2
--and a.credit_type=b.credit_type
--and a.LIFE_NUMBER=b.LIFE_NUMBER
--and a.COVERAGE_NUMBER=b.COVERAGE_NUMBER
--and a.RIDER_NUMBER=b.RIDER_NUMBER
--and a.COMPONENT_CODE = b.COMPONENT_CODE
--and a.COMPONENT_NAME=b.COMPONENT_NAME
--and a.TRANSACTION_DATE=b.TRANSACTION_DATE
--and a.clawback_type=b.clawback_type
--and a.clawback_name=b.clawback_name
--and a.batch_number=b.batch_number;
--
--commit;
--
--Log('insert into AIA_CB_CLAWBACK_COMP for NLPI' ||' batch_no = ' || P_BATCH_NO);
--
--insert into aia_cb_clawback_comp
--select  distinct b.calculation_period as measurement_quarter,
--A.clawback_type as clawback_type,
--b.Clawback_name as Clawback_name,
--STR_CB_NAME as CLAWBACK_METHOD,
--to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
-- pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE,
--         trim(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME,
--         pos_dis.genericattribute2 as WRI_DM_CODE,
--           --substr(pos_agy.name, 4) as WRI_AGY_CODE,
--         pos_agy.genericattribute1 as WRI_AGY_CODE,
----         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME,
--trim(part_agy.firstname||' '||part_agy.lastname) as WRI_AGY_NAME,
--         pos_agt.GENERICATTRIBUTE2 as WRI_AGY_LDR_CODE,
--         pos_agt.genericattribute7 as WRI_AGY_LDR_NAME,
--         a.payor_code as WRI_AGT_CODE,
--         trim(par_agt.firstname||' '||par_agt.lastname) as WRI_AGT_NAME,
--         decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
--         title_agt.name as RANK,
--         pos_agt.genericattribute4 as CLASS,
--         pos_agt.genericattribute11 as UM_RANK, -- Check
--         ba.bsc_grade as FSC_BSC_GRADE,
--         ba.entitlementpercent as FSC_BSC_PERCENTAGE,
--b.policy_number as PONUMBER,
--         b.life_number as LIFE_NUMBER,
--         b.coverage_number as COVERAGE_NUMBER,
--         b.RIDER_NUMBER as RIDER_NUMBER,
--         b.COMPONENT_NAME as COMPONENT_NAME,
--         b.component_code as COMPONENT_CODE,
--         b.PRODUCT_NAME as PRODUCT_NAME,
--a.transaction_date as transaction_date,
--b.policy_year as policy_year,
-- case
--           when a.credit_type  in ('FYC','FYC_W','FYC_W_DUPLICATE') then
--           a.FYC_RYC_Credit_value
--           else
--            0
--         end as FYC,
--         case
--           when a.credit_type in ('API','API_W','API_W_DUPLICATE') then
--           a.FYC_RYC_Credit_value
--           else
--            0
--         end as API,
--         case
--           when a.credit_type  in ('SSCP','SSCP_W','SSCP_W_DUPLICATE') then
--           a.FYC_RYC_Credit_value
--           else
--            0
--         end as SSC,
--         case
--           when a.credit_type  in ('RYC','RYC_W','RYC_W_DUPLICATE','ORYC_W','ORYC_W_DUPLICATE') then
--            a.FYC_RYC_Credit_value
--           else
--            0
--         end as RYC,
--0 as FYO,
--0 as RYO,
--0 as FSM_RYO,
--0 as NADOR,
--a.pm_contribution_value as NLPI,
--0 as SPI,
--a.pm_contribution_value * INT_SVI_RATE as SVI, -- * INT_SVI_RATE
--(a.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent as ENTITLEMENT,
--         round((((a.pm_contribution_value * INT_SVI_RATE) -
--         (a.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent)) * (-1),2) as CLAWBACK,
--         0 as PROCESSED_CLAWBACK,
--b.base_rider_ind,
--b.salestransactionseq as salestransactionseq,
--b.creditseq as creditseq,
--b.pm_seq as pmseq,
--P_BATCH_NO as batch_number,
--pos_agy_rcr.GENERICATTRIBUTE2 as RCVR_AGY_LDR_CODE ,
--         pos_agy_rcr.genericattribute11 as RCVR_AGY_LDR_RANK,
--         case rul.EXPRESSIONTYPEFORTYPE
--             when 256 then 'DIRECT'
--             when 1024 then 'INDIRECT'
--            else '0'
--         end as REPORT_TYPE,
--         0 as OFFSET_CLAWBACK
--         from
--(select * from AIA_CB_CLAWBACK_COMP_NLPI_CR where batch_number=P_BATCH_NO)A inner join
--(select * from AIA_CB_TRACE_FORWARD_COMP where clawback_name in ('NLPI','NLPI_ONG') and batch_number=P_BATCH_NO and pm_name not in (
--'PM_NLPI_PIB_Exclusion_NEW',
--'PM_NLPI_PIB_Exclusion')) B
--on
--a.creditseq=b.creditseq
--and a.salestransactionseq = b.salestransactionseq
--   inner join cs_rule rul
--     on b.crd_ruleseq=rul.ruleseq
--     and rul.REMOVEDATE=DT_REMOVEDATE
--     and rul.islast = 1
--inner join aia_cb_bsc_agent ba
--on b.calculation_period = (ba.quarter || ' ' || ba.year)
--     and b.payor_code = ba.agentcode
--       --for receiving Agency leader info
--     inner join cs_position pos_agy_rcr
--        on pos_agy_rcr.ruleelementownerseq = a.crd_positionseq
--        AND pos_agy_rcr.removedate = DT_REMOVEDATE
--        and pos_agy_rcr.islast = 1
--          --for writing Agency postion info
--   inner join cs_position pos_agy
--        on pos_agy.name = 'SGT'||b.payor_code
--        AND pos_agy.removedate = DT_REMOVEDATE
--        and pos_agy.islast = 1
--      /* --for writing Agency participant info
--     inner join cs_participant par_agy
--        on par_agy.PAYEESEQ = pos_agy.PAYEESEQ
--        AND par_agy.effectivestartdate <= cr.genericdate2
--        AND par_agy.effectiveenddate   >  cr.genericdate2
--        AND par_agy.removedate = to_date('01012200','ddmmyyyy') */
--        --for writing Agency participant infoto join position
--  /*     inner join cs_position par_agy_pos
--        on par_agy_pos.name =  'SGY'||pos_agy.genericattribute1
--        and par_agy_pos.removedate = DT_REMOVEDATE
--        and par_agy_pos.islast = 1
--             --for writing Agency participant info
--        inner join cs_participant part_agy
--        on par_agy_pos.payeeseq = part_agy.payeeseq
--             AND part_agy.effectivestartdate <= a.crd_genericdate2
--        AND part_agy.effectiveenddate   >  a.crd_genericdate2
--        and part_agy.removedate = DT_REMOVEDATE
--      --for writing District postion info
--     inner join cs_position pos_dis
--        on pos_dis.name= 'SGY' || pos_agy.genericattribute3
--        AND pos_dis.effectivestartdate <= a.crd_genericdate2
--        AND pos_dis.effectiveenddate   > a.crd_genericdate2
--        AND pos_dis.removedate = DT_REMOVEDATE
--     --for writing District participant info
--     inner join cs_participant par_dis
--        on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
--        AND par_dis.effectivestartdate <= a.crd_genericdate2
--        AND par_dis.effectiveenddate  > a.crd_genericdate2
--        AND par_dis.removedate = DT_REMOVEDATE
--     --for writing Agent postion info
--      inner join cs_position pos_agt
--        on 'SGT'||a.payor_code=pos_agt.name
--        and pos_agt.effectivestartdate <= a.crd_genericdate2
--        AND pos_agt.effectiveenddate   > a.crd_genericdate2
--        and pos_agt.removedate = DT_REMOVEDATE
--     --for writing Agent participant info
--     inner join cs_participant par_agt
--     on par_agt.payeeseq= pos_agt.PAYEESEQ
--     AND par_agt.effectivestartdate <= a.crd_genericdate2
--     AND par_agt.effectiveenddate  > a.crd_genericdate2
--     AND par_agt.removedate = DT_REMOVEDATE
--     --for payor agent title info
--     inner join cs_title title_agt
--     on title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
--     AND title_agt.effectivestartdate <= a.crd_genericdate2
--     AND title_agt.effectiveenddate   > a.crd_genericdate2
--     AND title_agt.REMOVEDATE = DT_REMOVEDATE;
--
--     Log('insert into AIA_CB_CLAWBACK_COMP for NLPI' || '; row count: ' || to_char(sql%rowcount));
--
--     commit;
--     */
--
--/*
----Added by Win Tan for SPI calculation
--Log('insert into AIA_CB_CLAWBACK_COMP for SPI' ||' batch_no = ' || P_BATCH_NO);
--
--insert into AIA_CB_CLAWBACK_COMP
--  select tf.calculation_period as MEASUREMENT_QUARTER,
--         tf.clawback_type as CLAWBACK_TYPE,
--         tf.clawback_name as CLAWBACK_NAME,
--         STR_COMPENSATION as CLAWBACK_METHOD,
--         to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
--         pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE,
--         trim(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME,
--         pos_dis.genericattribute2 as WRI_DM_CODE,
--         substr(pos_agy.name, 4) as WRI_AGY_CODE,
--         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME,
--         pos_agt.GENERICATTRIBUTE2 as WRI_AGY_LDR_CODE,
--         pos_agt.genericattribute7 as WRI_AGY_LDR_NAME,
--         tf.payor_code as WRI_AGT_CODE,
--         trim(par_agt.firstname||' '||par_agt.lastname) as WRI_AGT_NAME,
--         decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
--         title_agt.name as RANK,
--          pos_agt.genericattribute4 as UM_CLASS,
--         pos_agt.genericattribute11 as UM_RANK, -- Check cr.genericattribute14 as CLASS,
--         ba.bsc_grade as FSC_BSC_GRADE,
--         ba.entitlementpercent as FSC_BSC_PERCENTAGE,
--         tf.policy_number as PONUMBER,
--         tf.life_number as LIFE_NUMBER,
--         tf.coverage_number as COVERAGE_NUMBER,
--         tf.RIDER_NUMBER as RIDER_NUMBER,
--         tf.COMPONENT_NAME as COMPONENT_NAME,
--         tf.component_code as COMPONENT_CODE,
--         tf.PRODUCT_NAME as PRODUCT_NAME,
--         tf.transaction_date as TRANSACTION_DATE,
--         tf.policy_year as POLICY_YEAR,
--          case
--           when tf.credit_type  in ('FYC','FYC_W','FYC_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as FYC,
--         case
--           when tf.credit_type in ('API','API_W','API_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as API,
--         case
--           when tf.credit_type  in ('SSCP','SSCP_W','SSCP_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as SSC,
--         case
--           when tf.credit_type  in ('RYC','RYC_W','RYC_W_DUPLICATE','ORYC_W','ORYC_W_DUPLICATE') then
--            tf.credit_value
--           else
--            0
--         end as RYC,
--         0 as FYO,
--         0 as RYO,
--         0 as FSM_RYO,
--         0 as NADOR,
--         0 as NLPI,
--         case
--           when tf.clawback_name  ='SPI' then
--            tf.pm_contribution_value
--           else
--            0
--         end as SPI,
--         tf.pm_contribution_value * tf.pm_rate * INT_SVI_RATE as SVI,
--         tf.pm_contribution_value * tf.pm_rate * INT_SVI_RATE * ba.entitlementpercent as ENTITLEMENT,
--         (tf.pm_contribution_value * tf.pm_rate * INT_SVI_RATE -
--         tf.pm_contribution_value * tf.pm_rate * INT_SVI_RATE * ba.entitlementpercent) * (-1) as CLAWBACK_VALUE,
--         0 as PROCESSED_CLAWBACK,
--          tf.base_rider_ind as BASIC_RIDER_IND,
--         tf.salestransactionseq,
--         tf.creditseq,
--         tf.pm_seq,
--         P_BATCH_NO,
--         pos_agy.GENERICATTRIBUTE2 as RCVR_AGY_LDR_CODE ,
--         pos_agy.genericattribute11 as RCVR_AGY_LDR_RANK,
--         case rul.EXPRESSIONTYPEFORTYPE
--             when 256 then 'DIRECT'
--             when 1024 then 'INDIRECT'
--            else '0'
--         end as REPORT_TYPE
--    from AIA_CB_TRACE_FORWARD_COMP tf
--   inner join aia_cb_identify_policy ip
--   on ip.policyidseq = tf.policyidseq
--   inner join aia_cb_bsc_agent ba
--      on tf.calculation_period = (ba.quarter || ' ' || ba.year)
--     and tf.payor_code = ba.agentcode
--   inner join CS_CREDIT cr
--      on tf.creditseq = cr.creditseq
--   inner join cs_rule rul
--     on cr.ruleseq=rul.ruleseq
--     and rul.REMOVEDATE = DT_REMOVEDATE
--     and rul.islast = 1
--   --for writing Agency postion info
--   inner join cs_position pos_agy
--        on pos_agy.name = 'SGY' || ip.wri_agy_code
--        AND pos_agy.effectivestartdate <= cr.genericdate2
--        AND pos_agy.effectiveenddate   >  cr.genericdate2
--        AND pos_agy.removedate = DT_REMOVEDATE
--     --for writing Agency participant info
--     inner join cs_participant par_agy
--        on par_agy.PAYEESEQ = pos_agy.PAYEESEQ
--        AND par_agy.effectivestartdate <= cr.genericdate2
--        AND par_agy.effectiveenddate   >  cr.genericdate2
--        AND par_agy.removedate = DT_REMOVEDATE
--      --for writing District postion info
--     inner join cs_position pos_dis
--        on pos_dis.name= 'SGY' || pos_agy.genericattribute3
--        AND pos_dis.effectivestartdate <= cr.genericdate2
--        AND pos_dis.effectiveenddate   > cr.genericdate2
--        AND pos_dis.removedate = DT_REMOVEDATE
--     --for writing District participant info
--     inner join cs_participant par_dis
--        on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
--        AND par_dis.effectivestartdate <= cr.genericdate2
--        AND par_dis.effectiveenddate  > cr.genericdate2
--        AND par_dis.removedate = DT_REMOVEDATE
--     --for writing Agent postion info
--      inner join cs_position pos_agt
--        on 'SGT'||cr.genericattribute12=pos_agt.name
--        and pos_agt.effectivestartdate <= cr.genericdate2
--        AND pos_agt.effectiveenddate   > cr.genericdate2
--        and pos_agt.removedate = DT_REMOVEDATE
--     --for writing Agent participant info
--     inner join cs_participant par_agt
--     on par_agt.payeeseq= pos_agt.PAYEESEQ
--     AND par_agt.effectivestartdate <= cr.genericdate2
--     AND par_agt.effectiveenddate  > cr.genericdate2
--     AND par_agt.removedate = DT_REMOVEDATE
--     --for payor agent title info
--     inner join cs_title title_agt
--     on title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
--     AND title_agt.effectivestartdate <= cr.genericdate2
--     AND title_agt.effectiveenddate   > cr.genericdate2
--     AND title_agt.REMOVEDATE = DT_REMOVEDATE
--   where tf.clawback_name in ('SPI', 'SPI_ONG')
--   and tf.batch_number = P_BATCH_NO
--   ;
--
--     Log('insert into AIA_CB_CLAWBACK_COMP for SPI' || '; row count: ' || to_char(sql%rowcount));
--
--     commit;*/
--
--  /**
--the below logic is to check the clawback policy has the negative SVI value in current measurement quarter.
--if yes, need to trace the same policy's clawback value of last quarter,
--  if figure < 0, continue
--  else if figure > 0, set current month clawback value = 0
--end
--**/
--
----get clawback type and clawback name, only LUMPSUM case will apply this logic
--V_CB_TYPE := fn_get_cb_type(P_BATCH_NO);
----V_CB_NAME := fn_get_cb_name(P_BATCH_NO);
--V_CB_QTR := fn_get_cb_quarter(P_BATCH_NO);
--
--
--if V_CB_TYPE = STR_LUMPSUM then
--   --get previous quarter batch number
--    --V_BATCH_NO_PRE_QTR := fn_get_batch_no_pre_qtr(P_BATCH_NO);
--
--insert into AIA_CB_CLAWBACK_SVI_COMP_TMP
--select curr_cc.*, P_BATCH_NO from
--(select wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name,
--  clawback_name,
--  sum(clawback) as clawback
--  from
--AIA_CB_CLAWBACK_COMP
--where clawback_type = STR_LUMPSUM
-- and clawback_method = STR_CB_NAME
-- and batch_no = P_BATCH_NO
--group by wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name,
--  clawback_name
--  having sum(clawback) > 0
--) curr_cc
--left join
--(select cc.wri_dist_code,
--       cc.wri_agy_code,
--       cc.wri_agt_code,
--       cc.ponumber,
--       cc.life_number,
--       cc.coverage_number,
--       cc.rider_number,
--       cc.component_code,
--       cc.product_name,
--       cc.clawback_name,
--       --processed_clawback value should be updated after pipeline compeleted
--       sum(cc.processed_clawback) as processed_clawback
--  from AIA_CB_CLAWBACK_COMP cc
-- inner join (select nvl(max(t.batchnum), 0) as batch_no
--               from aia_cb_batch_status t
--               inner join (select distinct quarter, year, cb_startdate, cb_enddate
--               from aia_cb_period
--              where cb_name =  STR_CB_NAME
--              and BUNAME=STR_BUNAME
--              ) cbp
--              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
--              where t.islatest = 'Y'
--                and t.status = STR_STATUS_COMPLETED_SH
--                and t.clawbackname = STR_CB_NAME
--                and t.clawbacktype = STR_LUMPSUM
--                and t.cb_quarter_name <> V_CB_QTR
--               and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
--              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype) pre_batch
-- on cc.batch_no = pre_batch.batch_no
-- where cc.clawback_type =  STR_LUMPSUM
--   and cc.clawback_method =STR_CB_NAME
-- group by cc.wri_dist_code,
--          cc.wri_agy_code,
--          cc.wri_agt_code,
--          cc.ponumber,
--          cc.life_number,
--          cc.coverage_number,
--          cc.rider_number,
--          cc.component_code,
--          cc.product_name,
--            cc.clawback_name
--having sum(cc.processed_clawback) < 0) pre_cc
-- on curr_cc.wri_dist_code = pre_cc.wri_dist_code
-- and curr_cc.wri_agy_code = pre_cc.wri_agy_code
-- and curr_cc.wri_agt_code = pre_cc.wri_agt_code
-- and curr_cc.ponumber = pre_cc.ponumber
-- and curr_cc.life_number = pre_cc.life_number
-- and curr_cc.coverage_number = pre_cc.coverage_number
-- and curr_cc.rider_number = pre_cc.rider_number
-- and curr_cc.component_code = pre_cc.component_code
-- and curr_cc.product_name = pre_cc.product_name
-- and curr_cc.clawback_name = pre_cc.clawback_name
-- where pre_cc.ponumber is null;
--
--Log('insert into AIA_CB_CLAWBACK_SVI_COMP_TMP for Compensation Lumpsum' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--elsif V_CB_TYPE = STR_ONGOING then
--
--insert into AIA_CB_CLAWBACK_SVI_COMP_TMP
--select curr_cc.*, P_BATCH_NO from
--(select wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name,
--  clawback_name,
--  sum(clawback) as clawback
--  from
--AIA_CB_CLAWBACK_COMP
--where clawback_type = STR_ONGOING
-- and clawback_method = STR_CB_NAME
-- and batch_no = P_BATCH_NO
--group by wri_dist_code ,
--  wri_agy_code,
--  wri_agt_code,
--  ponumber,
--  life_number,
--  coverage_number,
--  rider_number,
--  component_code,
--  product_name,
--  clawback_name
--  having sum(clawback) > 0
--) curr_cc
--left join
--(select cc.wri_dist_code,
--       cc.wri_agy_code,
--       cc.wri_agt_code,
--       cc.ponumber,
--       cc.life_number,
--       cc.coverage_number,
--       cc.rider_number,
--       cc.component_code,
--       cc.product_name,
--       cc.clawback_name,
--       --processed_clawback value should be updated after pipeline compeleted
--       sum(cc.processed_clawback) as processed_clawback
--  from AIA_CB_CLAWBACK_COMP cc
-- inner join (
-- --lumpsum batch number
-- select nvl(max(t.batchnum), 0) as batch_no
--               from aia_cb_batch_status t
--               inner join (select distinct quarter, year, cb_startdate, cb_enddate
--               from aia_cb_period
--              where cb_name = STR_CB_NAME
--              and buname=STR_BUNAME
--              ) cbp
--              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
--              where t.islatest = 'Y'
--                and t.status = STR_STATUS_COMPLETED_SH
--                and t.clawbackname = STR_CB_NAME
--                and t.clawbacktype = STR_LUMPSUM
--                and t.cb_quarter_name <> V_CB_QTR
--                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
--              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype
--              union
--  --on-going batch number
--              select nvl(max(t.batchnum), 0) as batch_no
--               from aia_cb_batch_status t
--              where t.islatest = 'Y'
--                and t.status = STR_STATUS_COMPLETED_SH --'completed_sh'
--                and t.clawbackname = STR_CB_NAME--'COMMISSION'
--                and t.clawbacktype = STR_ONGOING --'ONGOING'
--                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) > t.cycledate
--              ) pre_batch
-- on cc.batch_no = pre_batch.batch_no
-- where cc.clawback_method = STR_CB_NAME
-- group by cc.wri_dist_code,
--          cc.wri_agy_code,
--          cc.wri_agt_code,
--          cc.ponumber,
--          cc.life_number,
--          cc.coverage_number,
--          cc.rider_number,
--          cc.component_code,
--          cc.product_name,
--          cc.clawback_name
--having sum(cc.processed_clawback) < 0) pre_cc
-- on curr_cc.wri_dist_code = pre_cc.wri_dist_code
-- and curr_cc.wri_agy_code = pre_cc.wri_agy_code
-- and curr_cc.wri_agt_code = pre_cc.wri_agt_code
-- and curr_cc.ponumber = pre_cc.ponumber
-- and curr_cc.life_number = pre_cc.life_number
-- and curr_cc.coverage_number = pre_cc.coverage_number
-- and curr_cc.rider_number = pre_cc.rider_number
-- and curr_cc.component_code = pre_cc.component_code
-- and curr_cc.product_name = pre_cc.product_name
-- and curr_cc.clawback_name = pre_cc.clawback_name
-- where pre_cc.ponumber is null;
--
--Log('insert into AIA_CB_CLAWBACK_SVI_COMP_TMP for Compensation Ongoing' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--end if;
--
----update the table AIA_CB_CLAWBACK_COMMISSION for special handling for positive clawback
--merge into AIA_CB_CLAWBACK_COMP cc
--using AIA_CB_CLAWBACK_SVI_COMP_TMP st
--on (cc.wri_dist_code = st.wri_dist_code
-- and cc.wri_agy_code = st.wri_agy_code
-- and cc.wri_agt_code = st.wri_agt_code
-- and cc.ponumber = st.ponumber
-- and cc.life_number = st.life_number
-- and cc.coverage_number = st.coverage_number
-- and cc.rider_number = st.rider_number
-- and cc.component_code = st.component_code
-- and cc.product_name = st.product_name
-- and cc.clawback_name = st. clawback_name
-- and cc.batch_no = st.batch_no
-- and cc.batch_no = P_BATCH_NO
--)
--when matched then update set cc.clawback = 0;
--
--Log('merge into AIA_CB_CLAWBACK_COMP to handle positive clawback' || '; row count: ' || to_char(sql%rowcount));
--
--commit;
--
--end SP_CLAWBACK_COMP;
--

--
--
--PROCEDURE SP_EXEC_COMMISSION_LUMPSUM(P_STR_CB_CYCLEDATE IN VARCHAR2)
--  is
--  V_LUMPSUM_FLAG   NUMBER;
--  V_BATCH_NO       NUMBER;
--  v_message        VARCHAR2(2000);
--  V_CB_YEAR        VARCHAR2(20);
--  V_CB_QUARTER     VARCHAR2(20);
--  begin
--
--  init;
--
--    SELECT COUNT(1) INTO V_LUMPSUM_FLAG FROM AIA_CB_PERIOD WHERE CB_CYCLEDATE = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE) and cb_name=STR_COMMISSION and BUNAME=STR_BUNAME;
--    IF V_LUMPSUM_FLAG >0 THEN
--      --LUMPSUM
--         sp_create_batch_no(P_STR_CB_CYCLEDATE,STR_LUMPSUM,STR_COMMISSION);
--         V_BATCH_NO := fn_get_batch_no(P_STR_CB_CYCLEDATE, STR_COMMISSION, STR_LUMPSUM, STR_STATUS_START);
--         --SP_POLICY_EXCL(P_STR_CB_CYCLEDATE, STR_COMMISSION);
--         --SP_IDENTIFY_POLICY (P_STR_CB_CYCLEDATE, STR_COMMISSION);
--         sp_revert_by_batch(V_BATCH_NO);
--/*         --get clawback year and quarter from clawback period table
--         select cbp.year, cbp.quarter
--         into V_CB_YEAR, V_CB_QUARTER
--         from aia_cb_period cbp where cbp.cb_cycledate = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);
--         --run report for identify policy result
--         PK_AIA_CB_REPORTS.SP_RPT_CB_MASTER_POLICY (V_CB_YEAR , V_CB_QUARTER);  */
--
--         SP_TRACE_FORWARD_COMMISSION (P_STR_CB_CYCLEDATE,STR_LUMPSUM, V_BATCH_NO);
--         SP_CLAWBACK_COMMISSION (P_STR_CB_CYCLEDATE, V_BATCH_NO);
--         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) ;
--     ELSE Log(P_STR_CB_CYCLEDATE || ' is not the avaiable clawback cycle date');
--    END IF;
--        ---catch exception
--        EXCEPTION WHEN OTHERS
--        THEN    v_message := SUBSTR(SQLERRM,1,2000);
--                Log(v_message);
--                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
--  END SP_EXEC_COMMISSION_LUMPSUM;
--
--PROCEDURE SP_EXEC_COMMISSION_ONGOING(P_STR_CB_CYCLEDATE IN VARCHAR2)
--  is
--  V_STR_CB_TYPE    VARCHAR2(20);
--  V_BATCH_NO       NUMBER;
--  V_WEEKEND_FLAG   NUMBER;
--  V_MONTHEND_FLAG  NUMBER;
--  V_MESSAGE        VARCHAR2(2000);
--  begin
--
--  init;
--
--  ---to define the run type
--  SELECT COUNT(1) INTO V_WEEKEND_FLAG FROM IN_ETL_CONTROL CTL
--  WHERE CTL.TXT_KEY_STRING='PAYMENT_END_DATE_WEEKLY' AND CTL.TXT_FILE_NAME= STR_PU AND CTL.TXT_KEY_VALUE=P_STR_CB_CYCLEDATE;
--
--  SELECT COUNT(1) INTO V_MONTHEND_FLAG
--  FROM CS_PERIOD CSP
--  where CSP.ENDDATE - 1 = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE)
--  and CSP.CALENDARSEQ = V_CALENDARSEQ and CSP.PERIODTYPESEQ=(select periodtypeseq from  cs_periodtype where name = STR_CALENDAR_TYPE)
--  and CSP.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
--  ;
--
--  IF V_WEEKEND_FLAG+V_MONTHEND_FLAG>0
--  THEN
--         --ONGOING
--         sp_create_batch_no(P_STR_CB_CYCLEDATE,STR_ONGOING,STR_COMMISSION);
--         V_BATCH_NO := fn_get_batch_no(P_STR_CB_CYCLEDATE, STR_COMMISSION,STR_ONGOING, STR_STATUS_START);
--         sp_revert_by_batch(V_BATCH_NO);
--         SP_TRACE_FORWARD_COMMISSION (P_STR_CB_CYCLEDATE,STR_ONGOING, V_BATCH_NO);
--         SP_CLAWBACK_COMMISSION (P_STR_CB_CYCLEDATE, V_BATCH_NO);
--         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) ;
--  END IF;
-----catch exception
--        EXCEPTION WHEN OTHERS
--        THEN    v_message := SUBSTR(SQLERRM,1,2000);
--                Log(v_message);
--                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
--  END SP_EXEC_COMMISSION_ONGOING;
--
--PROCEDURE SP_EXEC_COMPENSATION_LUMPSUM(P_STR_CB_CYCLEDATE IN VARCHAR2)
--  is
--  V_LUMPSUM_FLAG   NUMBER;
--  V_BATCH_NO       NUMBER;
--  v_message        VARCHAR2(2000);
--  V_CB_YEAR        VARCHAR2(20);
--  V_CB_QUARTER     VARCHAR2(20);
--  begin
--
--  init;
--
--    SELECT COUNT(1) INTO V_LUMPSUM_FLAG FROM AIA_CB_PERIOD WHERE CB_CYCLEDATE = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE) and cb_name=STR_COMPENSATION and buname=STR_BUNAME;
--    IF V_LUMPSUM_FLAG >0 THEN
--      --LUMPSUM
--         sp_create_batch_no(P_STR_CB_CYCLEDATE,STR_LUMPSUM,STR_COMPENSATION);
--         V_BATCH_NO := fn_get_batch_no(P_STR_CB_CYCLEDATE, STR_COMPENSATION, STR_LUMPSUM, STR_STATUS_START);
--
--         SP_TRACE_FORWARD_COMP (P_STR_CB_CYCLEDATE,STR_LUMPSUM, V_BATCH_NO);
--         SP_CLAWBACK_COMP (P_STR_CB_CYCLEDATE, V_BATCH_NO);
--         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) ;
--     ELSE Log(P_STR_CB_CYCLEDATE || ' is not the avaiable clawback cycle date');
--    END IF;
--        ---catch exception
--        EXCEPTION WHEN OTHERS
--        THEN    v_message := SUBSTR(SQLERRM,1,2000);
--                Log(v_message);
--                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
--END SP_EXEC_COMPENSATION_LUMPSUM;
--
--
--PROCEDURE SP_EXEC_COMPENSATION_ONGOING(P_STR_CB_CYCLEDATE IN VARCHAR2)
--  is
--  V_STR_CB_TYPE    VARCHAR2(20);
--  V_BATCH_NO       NUMBER;
--  V_WEEKEND_FLAG   NUMBER;
--  V_MONTHEND_FLAG  NUMBER;
--  V_MESSAGE        VARCHAR2(2000);
--  begin
--
--  init;
--
--  ---to define the run type
--  /*SELECT COUNT(1) INTO V_WEEKEND_FLAG FROM IN_ETL_CONTROL CTL
--  WHERE CTL.TXT_KEY_STRING='PAYMENT_END_DATE_WEEKLY' AND CTL.TXT_FILE_NAME= STR_PU AND CTL.TXT_KEY_VALUE=P_STR_CB_CYCLEDATE;*/
--  SELECT COUNT(1) INTO V_MONTHEND_FLAG FROM CS_PERIOD CSP where CSP.ENDDATE - 1 = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE)
--  and CSP.CALENDARSEQ = V_CALENDARSEQ and CSP.PERIODTYPESEQ=(select periodtypeseq from  cs_periodtype where name = STR_CALENDAR_TYPE)
--  and CSP.removedate = to_date('2200-01-01','yyyy-mm-dd')  --Cosimo
--  ;
--
--  IF V_MONTHEND_FLAG>0
--         THEN
--         --ONGOING
--         sp_create_batch_no(P_STR_CB_CYCLEDATE,STR_ONGOING,STR_COMPENSATION);
--         V_BATCH_NO := fn_get_batch_no(P_STR_CB_CYCLEDATE, STR_COMPENSATION,STR_ONGOING, STR_STATUS_START);
--         SP_TRACE_FORWARD_COMP (P_STR_CB_CYCLEDATE,STR_ONGOING, V_BATCH_NO);
--         SP_CLAWBACK_COMP (P_STR_CB_CYCLEDATE, V_BATCH_NO);
--         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP);
--  END IF;
-----catch exception
--        EXCEPTION WHEN OTHERS
--        THEN    v_message := SUBSTR(SQLERRM,1,2000);
--                Log(v_message);
--                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
--  END SP_EXEC_COMPENSATION_ONGOING;
--
--PROCEDURE SP_EXEC_IDENTIFY_POLICY(P_STR_CB_CYCLEDATE IN VARCHAR2)
--  is
--  V_STR_CB_TYPE    VARCHAR2(20);
--  V_BATCH_NO       NUMBER;
--  V_ID_FLAG        NUMBER;
--  V_MESSAGE        VARCHAR2(2000);
--  V_CB_YEAR        VARCHAR2(20);
--  V_CB_QUARTER     VARCHAR2(20);
--  begin
--
--  init;
--
--  --check if the cycle date is the date for run identify policy and report
--    SELECT COUNT(1)
--      INTO V_ID_FLAG
--      FROM AIA_CB_PERIOD
--     WHERE CB_CYCLEDATE = TO_DATE(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE)
--       and cb_name = STR_IDENTIFY
--       and buname = STR_BUNAME;
--
--    IF V_ID_FLAG >0 THEN
--         SP_POLICY_EXCL(P_STR_CB_CYCLEDATE, STR_IDENTIFY);
--         SP_IDENTIFY_POLICY (P_STR_CB_CYCLEDATE, STR_IDENTIFY);
--/*         --get clawback year and quarter from clawback period table
--         select cbp.year, cbp.quarter
--         into V_CB_YEAR, V_CB_QUARTER
--         from aia_cb_period cbp where cbp.cb_cycledate = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);
--
--         --run report for identify policy result
--         PK_AIA_CB_REPORTS.SP_RPT_CB_MASTER_POLICY (V_CB_YEAR , V_CB_QUARTER);*/
--     ELSE Log(P_STR_CB_CYCLEDATE || ' is not the avaiable identify policy cycle date');
--    END IF;
--        ---catch exception
--        EXCEPTION WHEN OTHERS
--        THEN    v_message := SUBSTR(SQLERRM,1,2000);
--                Log(v_message);
--  END SP_EXEC_IDENTIFY_POLICY;

PROCEDURE SP_EXEC_IDENTIFY_POLICY_FA(P_STR_CB_CYCLEDATE IN VARCHAR2)
  is
  V_STR_CB_TYPE    VARCHAR2(20);
  V_BATCH_NO       NUMBER;
  V_ID_FLAG        NUMBER;
  V_MESSAGE        VARCHAR2(2000);
  V_CB_YEAR        VARCHAR2(20);
  V_CB_QUARTER     VARCHAR2(20);
  begin

  init;

  --check if the cycle date is the date for run identify policy and report
    SELECT COUNT(1)
      INTO V_ID_FLAG
      FROM AIA_CB_PERIOD
     WHERE CB_CYCLEDATE = TO_DATE(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE)
       and cb_name = STR_IDENTIFY
       AND buname = STR_BUNAME_FA;

    IF V_ID_FLAG >0 THEN
         SP_POLICY_EXCL_FA(P_STR_CB_CYCLEDATE, STR_IDENTIFY);
         SP_IDENTIFY_POLICY_FA (P_STR_CB_CYCLEDATE, STR_IDENTIFY);
/*         --get clawback year and quarter from clawback period table
         select cbp.year, cbp.quarter
         into V_CB_YEAR, V_CB_QUARTER
         from aia_cb_period cbp where cbp.cb_cycledate = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);

         --run report for identify policy result
         PK_AIA_CB_REPORTS.SP_RPT_CB_MASTER_POLICY (V_CB_YEAR , V_CB_QUARTER);*/
     ELSE Log(P_STR_CB_CYCLEDATE || ' is not the avaiable identify policy cycle date for FA');
    END IF;
        ---catch exception
        EXCEPTION WHEN OTHERS
        THEN    v_message := SUBSTR(SQLERRM,1,2000);
                Log(v_message);
  END SP_EXEC_IDENTIFY_POLICY_FA;

PROCEDURE SP_POLICY_EXCL_FA(P_STR_CYCLEDATE IN VARCHAR2, P_CB_NAME IN VARCHAR2) AS

V_CYCLEDATE             DATE;
V_CB_YEAR               VARCHAR2(20);
V_CB_QUARTER            VARCHAR2(20);
V_INCEPTION_START_DT    DATE;
V_INCEPTION_END_DT      DATE;

BEGIN

Log('SP_POLICY_EXCL_FA start');

  ------------------------get cycle date  'yyyy-mm-dd'--------------------------
  SELECT TO_DATE(NVL(P_STR_CYCLEDATE, TXT_KEY_VALUE), STR_DATE_FORMAT_TYPE)
    INTO V_CYCLEDATE
    FROM IN_ETL_CONTROL
   WHERE TXT_FILE_NAME = STR_CYCLEDATE_FILE_NAME
     AND TXT_KEY_STRING = STR_CYCLEDATE_KEY;

  ------------------------get clawback year and quarter, inception period--------------------------
  SELECT CBP.YEAR,
         CBP.Quarter,
         CBP.Inception_Startdate,
         CBP.Inception_Enddate
    INTO V_CB_YEAR, V_CB_QUARTER, V_INCEPTION_START_DT, V_INCEPTION_END_DT
    FROM AIA_CB_PERIOD CBP
   WHERE CBP.CB_CYCLEDATE = TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
     and CBP.BUNAME = STR_BUNAME_FA
     and CBP.Removedate = DT_REMOVEDATE
     and cbp.cb_name = P_CB_NAME;


    --for trace indirect credit rule records
merge into AIA_CB_POLICY_EXCL pol_ex
using (select hist.BUNAME, hist.PONUMBER
         from AIA_CB_POLICY_EXCL_HIST hist
       union
       select STR_BUNAME_FA, st.ponumber
         from cs_salestransaction   st,
              AIA_CB_COMPONENT_EXCL ex,
              cs_businessunit       bu
        where st.tenantid='AIAS' and st.processingUnitseq=V_PROCESSINGUNITSEQ and st.genericattribute19 in ('LF', 'LN', 'UL')
          and st.productid = ex.component_name
          and ex.removedate = to_date('22000101', 'yyyymmdd')
          and st.genericattribute6 = '1'
          and st.ponumber is not null
          and st.businessunitmap = bu.mask
          and bu.name = STR_BUNAME_FA
          and st.compensationdate between V_INCEPTION_START_DT and V_INCEPTION_END_DT
          ) t
on (pol_ex.buname = t.buname and pol_ex.ponumber = t.ponumber)
when not matched then
  insert
    (BUNAME, PONUMBER, CYCLE_DATE, CREATE_DATE)
  values
    (t.BUNAME, t.PONUMBER, TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE), sysdate);

Log('updated AIA_CB_POLICY_EXCL for FA Agents; row count: ' || to_char(sql%rowcount));

  commit;

--version 6 add by Amanda begin
merge into AIA_CB_POLICY_EXCL pol_ex
using (select pol.BUNAME, pol.PONUMBER, pol.component_code
         from VW_CB_PROJECTED_POLICY_MASTER pol
        where pol.CB_EXCLUDE_FLG= 1
          and pol.buname = STR_BUNAME_FA
          and pol.year = V_CB_YEAR
          and pol.quarter = V_CB_QUARTER
          ) t
on (pol_ex.buname = t.buname and pol_ex.ponumber = t.ponumber and pol_ex.component_cd = t.component_code )
when not matched then
  insert
    (BUNAME, PONUMBER, CYCLE_DATE, CREATE_DATE, component_cd)
  values
    (t.BUNAME, t.PONUMBER, TO_DATE(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE), sysdate, t.component_code);

Log('updated AIA_CB_POLICY_EXCL from policy master; row count: ' || to_char(sql%rowcount));

  commit;
--version 6 add by Amanda end

Log('SP_POLICY_EXCL_FA end');

END SP_POLICY_EXCL_FA;

procedure SP_IDENTIFY_POLICY_FA (P_STR_CB_CYCLEDATE IN VARCHAR2, P_CB_NAME IN VARCHAR2) is
  /*
  Clawback Policy/Component list is just for Lumpsum calculation,
  which for user to review the policy/component that will be used as base table.
  After the BSC agent list and policy list for exclusion are ready in system.
  System will base on the credit and transaction to build up the policy/component list.

  // @input P_STR_CB_CYCLEDATE: cycle date with format yyyymmdd

  ************************************************
  Version     Create By       Create Date   Change
  ************************************************
  1           Zeno Zhao        20160510    Initial
  */

  /* TODO enter package declarations (types, exceptions, methods etc) here */

  -- define period seq of each month
  TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
  t_periodseq periodseq_type;

  dt_cb_cycledate date;   -- date format for input cb cycle date
   v_cb_period  aia_cb_period%rowtype;    --row variable of the CS period. For each procedure call, there is one cb_period_record

  int_sg_calendar_seq cs_calendar.calendarseq%type;  -- SG calendar seq
  int_periodtype_month_seq cs_periodtype.periodtypeseq%type; -- month period type seq

  int_bu_unit_map_sgp int;
begin

init;

  ---------------- initial variables
  --get the batch number from batch status table
  --select 1 into V_BATCH_NO from AIA_CB_BATCH_STATUS bs;
  --V_BATCH_NO := 1;

  -- get calendar seq and period type seq
  select calendarseq into int_sg_calendar_seq from cs_calendar where name=STR_CALENDARNAME;
  select periodtypeseq into int_periodtype_month_seq from cs_periodtype where name=STR_CALENDAR_TYPE;
  select mask into int_bu_unit_map_sgp from cs_businessunit where name=STR_BUNAME_FA;
  Log('calendar seq: ' || to_char(int_sg_calendar_seq) || '; month seq: ' || to_char(int_periodtype_month_seq) || '; BU mask: ' || to_char(int_bu_unit_map_sgp) );

  -- get cs_period record
  dt_cb_cycledate :=TO_DATE(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);

  select *
    into v_cb_period
    from aia_cb_period
   where cb_cycledate = dt_cb_cycledate
     and BUNAME = STR_BUNAME_FA
     and CB_NAME = P_CB_NAME
     and removedate = DT_REMOVEDATE
     and rownum = 1;
  Log('quarter ' || v_cb_period.cb_quarter_name);

  ---------------- empty temp table
  Log('Empty temp tables for FA');
  execute immediate 'truncate table AIA_CB_CREDITFILTER_TMP';
  execute immediate 'truncate table AIA_CB_POLICY_INC_TMP';
  execute immediate 'truncate table AIA_CB_CREDITFILTER';
  execute immediate 'truncate table AIA_CB_SALESTRANSACTION';

  -- delete old records for rerun
  Log('Remove old record for rerun for FA Agents');
  delete from AIA_CB_IDENTIFY_POLICY where buname=STR_BUNAME_FA and year=v_cb_period.year and quarter=v_cb_period.quarter;
  Log('Delete from  AIA_CB_IDENTIFY_POLICY for FA Agents' || '; row count: ' || to_char(sql%rowcount));
  ------------------


  -- get all month period seq
  select p.periodseq BULK COLLECT into t_periodseq
  from cs_period a, cs_period p
  where  a.calendarseq=int_sg_calendar_seq
    and p.calendarseq=int_sg_calendar_seq
    --Revised by Win Tan for version 12 begin
    --and a.name=v_cb_period.cb_quarter_name
    and a.name in (v_cb_period.cb_quarter_name,
        decode(v_cb_period.cb_quarter_name,'Q4 2017','Q1 2017S',''))
    --version 12 end
    and p.periodtypeseq= int_periodtype_month_seq
    and p.startdate>=a.startdate
    and p.enddate <=a.enddate
    and p.startdate >= v_cb_period.inception_startdate
    and p.enddate <= v_cb_period.inception_enddate + 1
    and a.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
    and p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
    ;


  --------- For each month (partition), filter the select credit into the temp table
  for i in 1..t_periodseq.count loop
    Log('peroid seq for FA: [' || to_char(i) || ']= ' || to_char(t_periodseq(i)) );      -- log

    -- Get credit record by month with cretia: agent is in date scope
   insert into AIA_CB_CREDITFILTER_TMP
   select /*+ parallel */
    cr.CREDITSEQ,cr.PAYEESEQ,cr.POSITIONSEQ,cr.SALESORDERSEQ,cr.SALESTRANSACTIONSEQ,cr.PERIODSEQ,cr.CREDITTYPESEQ,cr.NAME,cr.PIPELINERUNSEQ,cr.ORIGINTYPEID,cr.COMPENSATIONDATE
,cr.PIPELINERUNDATE,cr.BUSINESSUNITMAP,cr.PREADJUSTEDVALUE,cr.UNITTYPEFORPREADJUSTEDVALUE,cr.VALUE,cr.UNITTYPEFORVALUE,cr.RELEASEDATE,cr.RULESEQ,cr.ISHELD,cr.ISROLLABLE
,cr.ROLLDATE,cr.REASONSEQ,cr.COMMENTS,cr.GENERICATTRIBUTE1,cr.GENERICATTRIBUTE2,cr.GENERICATTRIBUTE3,cr.GENERICATTRIBUTE4,cr.GENERICATTRIBUTE5,cr.GENERICATTRIBUTE6
,cr.GENERICATTRIBUTE7,cr.GENERICATTRIBUTE8,cr.GENERICATTRIBUTE9,cr.GENERICATTRIBUTE10,cr.GENERICATTRIBUTE11,cr.GENERICATTRIBUTE12,cr.GENERICATTRIBUTE13,cr.GENERICATTRIBUTE14
,cr.GENERICATTRIBUTE15,cr.GENERICATTRIBUTE16,cr.GENERICNUMBER1,cr.UNITTYPEFORGENERICNUMBER1,cr.GENERICNUMBER2,cr.UNITTYPEFORGENERICNUMBER2,cr.GENERICNUMBER3,cr.UNITTYPEFORGENERICNUMBER3
,cr.GENERICNUMBER4,cr.UNITTYPEFORGENERICNUMBER4,cr.GENERICNUMBER5,cr.UNITTYPEFORGENERICNUMBER5,cr.GENERICNUMBER6,cr.UNITTYPEFORGENERICNUMBER6,cr.GENERICDATE1,
cr.GENERICDATE2,cr.GENERICDATE3,cr.GENERICDATE4,cr.GENERICDATE5,cr.GENERICDATE6,cr.GENERICBOOLEAN1,cr.GENERICBOOLEAN2,cr.GENERICBOOLEAN3,
cr.GENERICBOOLEAN4,cr.GENERICBOOLEAN5,cr.GENERICBOOLEAN6,cr.PROCESSINGUNITSEQ
     from cs_credit cr
    inner join AIA_CB_BSC_AGENT agt
    on cr.GENERICATTRIBUTE12 = agt.AGENTCODE
    inner join (select distinct SOURCE_RULE_OUTPUT
                  from AIA_CB_RULES_LOOKUP
                 where buname = STR_BUNAME_FA
                   and rule_type = 'CREDIT'
                  and SOURCE_RULE_OUTPUT like '%\_DIRECT\_%' ESCAPE '\') rl
    on cr.name = rl.SOURCE_RULE_OUTPUT
--    inner join cs_position POS on 'SGT'||AGT.AGENTCODE=POS.NAME
    where cr.tenantid='AIAS' and cr.processingUnitseq=V_PROCESSINGUNITSEQ and cr.periodseq = t_periodseq(i)
      and agt.ENTITLEMENTPERCENT <> 1 -- not equal 100%
      and agt.year = v_cb_period.year --change to period year and quator instead of startdate/enddate
      and agt.quarter = v_cb_period.quarter
      and cr.compensationdate between v_cb_period.inception_startdate and v_cb_period.inception_enddate
      AND CR.businessunitmap = int_bu_unit_map_sgp
--      and POS.GENERICATTRIBUTE6='AFA'
--       AND POS.removedate = DT_REMOVEDATE
--       AND POS.effectivestartdate <= AGT.ENDDATE
--       AND POS.effectiveenddate   >  AGT.ENDDATE
    ;

    Log('moth peroid seq for FA: [' || to_char(i) || '] ' || '; row count: ' || to_char(sql%rowcount));
    commit;

  end loop;

    ----------- get target policy list
    insert into AIA_CB_POLICY_INC_TMP
      (ponumber, create_date, fhr_date)
      select /*+ parallel */ distinct st.ponumber, sysdate, fhr.fhr_date
        from cs_salestransaction st
       inner join AIA_CB_CREDITFILTER_TMP cr
          on st.salestransactionseq = cr.salestransactionseq
        left join AIA_CB_POLICY_EXCL ex
          on st.ponumber = ex.ponumber        --and ex.ponumber is null
          and nvl(ex.component_cd, st.productid) = st.productid --version 6 add by Amanda Wei
         and EX.BUNAME=STR_BUNAME_FA
        left join AIA_CB_POLICY_FHR_DATE fhr
          on st.ponumber = fhr.ponumber and FHR.BUNAME=STR_BUNAME_FA
       where st.tenantid='AIAS' and st.processingUnitseq=V_PROCESSINGUNITSEQ and st.GENERICATTRIBUTE19 in ('LF', 'LN', 'UL')
         and ex.ponumber is null;
--         and EX.BUNAME=STR_BUNAME_FA -- Modified by Gopi for unittesting
 --        and FHR.BUNAME=STR_BUNAME_FA;

    Log('CB policy include records for FA Agents; row count: ' || to_char(sql%rowcount));
--  end if;

  commit;
  --execute immediate 'analyze table AIA_CB_POLICY_INC_TMP compute statistic';


------------- insert into CB credit filter
  insert into AIA_CB_CREDITFILTER
    select cr.genericattribute12,
           cr.genericattribute14,
           cr.genericattribute1,
           cr.compensationdate,
           cr.positionseq,
           cr.genericdate2,
           cr.salestransactionseq,
           inc.fhr_date
      from AIA_CB_CREDITFILTER_TMP cr
     inner join AIA_CB_POLICY_INC_TMP inc
        on cr.genericattribute6 = inc.ponumber;

 Log('CB credit filter for FA; row count: ' || to_char(sql%rowcount));

 commit;

  ------------- insert into CB transaction
insert into AIA_CB_SALESTRANSACTION
  select cr.genericattribute12 as WRI_AGT_CODE,
         cr.genericattribute14 as CLASS,
         st.ponumber as PONUMBER,
         st.genericattribute23 as INSURED_NAME,
         st.genericattribute19 as CONTRACT_CAT,
         st.GENERICATTRIBUTE29 as LIFE_NUMBER,
         st.GENERICATTRIBUTE30 as COVERAGE_NUMBER,
         st.GENERICATTRIBUTE31 as RIDER_NUMBER,
         cr.genericattribute1 as COMPONENT_CODE,
         st.genericattribute3 as COMPONENT_NAME,
         st.genericdate3 as ISSUE_DATE,
         st.genericdate6 as INCEPTION_DATE,
         st.genericdate2 as RISK_COMMENCEMENT_DATE,
         cr.fhr_date as FHR_DATE,
         decode(st.GENERICATTRIBUTE6, '1', 'Y', 'N') as BASE_RIDER_IND,
         cr.compensationdate as TRANSACTION_DATE,
         st.genericattribute1 as PAYMENT_MODE,
         st.genericattribute5 as POLICY_CURRENCY,
         st.salestransactionseq,
         cr.positionseq,
         cr.genericdate2 as POLICY_ISSUE_DATE,
         gast.genericdate8 submitdate
    from cs_salestransaction st
   inner join AIA_CB_CREDITFILTER cr
      on st.salestransactionseq = cr.salestransactionseq
   inner join cs_gasalestransaction gast
      on st.salestransactionseq = gast.salestransactionseq
      and gast.pagenumber =0
   where st.tenantid = 'AIAS'
     and st.processingunitseq = V_PROCESSINGUNITSEQ
     and greatest(nvl(st.genericdate3, to_date('19000101', 'yyyymmdd')),
                  nvl(st.genericdate6, to_date('19000101', 'yyyymmdd')),
                  nvl(st.genericdate2, to_date('19000101', 'yyyymmdd'))) between
         v_cb_period.inception_startdate and v_cb_period.inception_enddate;

 Log('CB transaction for FA; row count: ' || to_char(sql%rowcount));
     commit;

 Log('CB transaction for FA; v_cb_period.inception_startdate: ' || v_cb_period.inception_startdate);
 Log('CB transaction for FA; v_cb_period.inception_enddate: ' || v_cb_period.inception_enddate);
 Log('CB transaction for FA; V_PROCESSINGUNITSEQ: ' || V_PROCESSINGUNITSEQ);
 Log('CB transaction for FA; v_cb_period.year: ' || to_char(v_cb_period.year));
 Log('CB transaction for FA; v_cb_period.quarter: ' || to_char(v_cb_period.quarter));
 Log('CB transaction for FA; DT_REMOVEDATE: ' || to_char(DT_REMOVEDATE));
 Log('CB transaction for FA; STR_BUNAME_FA: ' || to_char(STR_BUNAME_FA));

  ------------- final insert into target
  insert into AIA_CB_IDENTIFY_POLICY(
      BUNAME
      ,YEAR
      ,QUARTER
      ,WRI_DIST_CODE
      ,WRI_DIST_NAME
      ,WRI_DM_CODE
      ,WRI_DM_NAME
      ,WRI_AGY_CODE
      ,WRI_AGY_NAME
      ,WRI_AGY_LDR_CODE
      ,WRI_AGY_LDR_NAME
      ,WRI_AGT_CODE
      ,WRI_AGT_NAME
      ,FSC_TYPE
      ,RANK
      ,CLASS
      ,FSC_BSC_GRADE
      ,FSC_BSC_PERCENTAGE
      ,PONUMBER
      ,INSURED_NAME
      ,CONTRACT_CAT
      ,LIFE_NUMBER
      ,COVERAGE_NUMBER
      ,RIDER_NUMBER
      ,COMPONENT_CODE
      ,COMPONENT_NAME
      ,ISSUE_DATE
      ,INCEPTION_DATE
      ,RISK_COMMENCEMENT_DATE
      ,FHR_DATE
      ,BASE_RIDER_IND
      ,TRANSACTION_DATE
      ,PAYMENT_MODE
      ,POLICY_CURRENCY
      ,PROCESSING_PERIOD
      --,BATCH_NO
      ,CREATED_DATE
      ,POLICYIDSEQ
      ,SUBMITDATE
      ,FAOB_AGT_CODE
        )
  select
        curr_ip.BUNAME
      ,curr_ip.YEAR
      ,curr_ip.QUARTER
      ,curr_ip.WRI_DIST_CODE
      ,curr_ip.WRI_DIST_NAME
      ,curr_ip.WRI_DM_CODE
      ,curr_ip.WRI_DM_NAME
      ,curr_ip.WRI_AGY_CODE
      ,curr_ip.WRI_AGY_NAME
      ,curr_ip.WRI_AGY_LDR_CODE
      ,curr_ip.WRI_AGY_LDR_NAME
      ,curr_ip.WRI_AGT_CODE
      ,curr_ip.WRI_AGT_NAME
      ,curr_ip.FSC_TYPE
      ,curr_ip.RANK
      ,curr_ip.CLASS
      ,curr_ip.FSC_BSC_GRADE
      ,curr_ip.FSC_BSC_PERCENTAGE
      ,curr_ip.PONUMBER
      ,curr_ip.INSURED_NAME
      ,curr_ip.CONTRACT_CAT
      ,curr_ip.LIFE_NUMBER
      ,curr_ip.COVERAGE_NUMBER
      ,curr_ip.RIDER_NUMBER
      ,curr_ip.COMPONENT_CODE
      ,curr_ip.COMPONENT_NAME
      ,curr_ip.ISSUE_DATE
      ,curr_ip.INCEPTION_DATE
      ,curr_ip.RISK_COMMENCEMENT_DATE
      ,curr_ip.FHR_DATE
      ,curr_ip.BASE_RIDER_IND
      ,curr_ip.TRANSACTION_DATE
      ,curr_ip.PAYMENT_MODE
      ,curr_ip.POLICY_CURRENCY
      ,curr_ip.PROCESSING_PERIOD
      --,BATCH_NO
      ,curr_ip.CREATED_DATE
      -- add sequence for id
      ,SEQ_CB_IDENTIFY_POLICY.NEXTVAL as POLICYIDSEQ
      ,curr_ip.submitdate
      ,curr_ip.FAOB_AGT_CODE
  from (
    select /*+ INDEX(cr IDX_CB_CREDITFILTER_TMP_1)*/
        STR_BUNAME_FA       as BUNAME
        ,v_cb_period.year     as YEAR
        ,v_cb_period.quarter   as QUARTER
        --writing district info.
        ,pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE
        ,trim(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME
        --writing district leader info.
        ,pos_dis.genericattribute2 as WRI_DM_CODE
        ,pos_dis.genericattribute7 as WRI_DM_NAME
        --writing agency info.
        ,substr(pos_agy.name, 4) as WRI_AGY_CODE
        ,trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME
        --writing agency leader info.
        ,pos_agt.GENERICATTRIBUTE2 as WRI_AGY_LDR_CODE
        ,pos_agt.genericattribute7 as WRI_AGY_LDR_NAME
        --writing agent info.
        ,st.WRI_AGT_CODE
        ,trim(par_agt.firstname||' '||par_agt.lastname) as WRI_AGT_NAME
        --,'Normal FSC' as FSC_TYPE
        ,decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE
        , title_agt.name as RANK
        ,st.CLASS
        ,agt.bsc_grade as FSC_BSC_GRADE
        ,agt.entitlementpercent as FSC_BSC_PERCENTAGE
        ,st.ponumber as PONUMBER            ---???
        ,st.INSURED_NAME
        ,st.CONTRACT_CAT
        ,st.LIFE_NUMBER
        ,st.COVERAGE_NUMBER
        ,st.RIDER_NUMBER
        ,st.COMPONENT_CODE
        ,st.COMPONENT_NAME
        ,st.ISSUE_DATE
        ,st.INCEPTION_DATE
        ,st.RISK_COMMENCEMENT_DATE
        ,st.fhr_date          as FHR_DATE
        ,st.BASE_RIDER_IND
        ,st.TRANSACTION_DATE
        ,st.PAYMENT_MODE
        ,st.POLICY_CURRENCY
        ,dt_cb_cycledate      as PROCESSING_PERIOD      --:22:23:24
        --,V_BATCH_NO           as BATCH_NO   ---AIA_CB_BATCH_STATUS, :25:26:27:28:29 when to insert data into this table:30:31:32:33
        ,sysdate              as CREATED_DATE
        -- Rank by key: policy number, comonent code, writing agent
        ,row_number() over(partition by st.ponumber, st.COMPONENT_CODE, st.WRI_AGT_CODE,
                 st.LIFE_NUMBER, st.COVERAGE_NUMBER, st.RIDER_NUMBER order by st.TRANSACTION_DATE desc) rk
        --,row_number() over(partition by st.ponumber, cr.genericattribute1,cr.genericattribute12  order by cr.compensationdate desc) rk
        --,1 as rk
        ,st.submitdate
        ,GA_PARTICIPANT.GENERICATTRIBUTE4 AS FAOB_AGT_CODE
    from   AIA_CB_SALESTRANSACTION  st
     inner join AIA_CB_BSC_AGENT agt
        on st.WRI_AGT_CODE = agt.AGENTCODE
       and agt.year = v_cb_period.year
       and agt.quarter = v_cb_period.quarter
     inner join cs_position pos_agy
        on pos_agy.tenantid = 'AIAS'
       AND pos_agy.ruleelementownerseq = st.positionseq
       AND pos_agy.removedate = DT_REMOVEDATE
       AND pos_agy.effectivestartdate <= st.POLICY_ISSUE_DATE
       AND pos_agy.effectiveenddate   > st.POLICY_ISSUE_DATE
--              and pos_agy.GENERICATTRIBUTE6='AFA'
     inner join cs_participant par_agy
        on par_agy.tenantid = 'AIAS'
        AND par_agy.PAYEESEQ = pos_agy.PAYEESEQ
        AND par_agy.effectivestartdate <= st.POLICY_ISSUE_DATE
        AND par_agy.effectiveenddate   >  st.POLICY_ISSUE_DATE
        AND par_agy.removedate = DT_REMOVEDATE
     inner join cs_position pos_dis
        on pos_dis.tenantid = 'AIAS'
        AND pos_dis.name= 'SGY' || pos_agy.genericattribute3
        AND pos_dis.effectivestartdate <= st.POLICY_ISSUE_DATE
        AND pos_dis.effectiveenddate   > st.POLICY_ISSUE_DATE
        AND pos_dis.removedate = DT_REMOVEDATE
     inner join cs_participant par_dis
        on par_dis.tenantid = 'AIAS'
        AND par_dis.PAYEESEQ = pos_dis.PAYEESEQ
        AND par_dis.effectivestartdate <= st.POLICY_ISSUE_DATE
        AND par_dis.effectiveenddate  > st.POLICY_ISSUE_DATE
        AND par_dis.removedate = DT_REMOVEDATE
      inner join cs_position pos_agt
        on pos_agt.tenantid = 'AIAS'
        AND 'SGT'||st.WRI_AGT_CODE=pos_agt.name
        and pos_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
        AND pos_agt.effectiveenddate   > st.POLICY_ISSUE_DATE
        and pos_agt.removedate = DT_REMOVEDATE
        AND pos_agt.GENERICATTRIBUTE6='AFA'
     inner join cs_participant par_agt
     on par_agt.tenantid = 'AIAS'
     AND par_agt.payeeseq= pos_agt.PAYEESEQ
     AND par_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
     AND par_agt.effectiveenddate  > st.POLICY_ISSUE_DATE
     AND par_agt.removedate = DT_REMOVEDATE
     inner join cs_title title_agt
     on title_agt.tenantid = 'AIAS'
     AND title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
     AND title_agt.effectivestartdate <= st.POLICY_ISSUE_DATE
     AND title_agt.effectiveenddate   > st.POLICY_ISSUE_DATE
     AND title_agt.REMOVEDATE = DT_REMOVEDATE
       inner join cs_gaparticipant ga_participant
        on ga_participant.tenantid = 'AIAS'
        AND ga_participant.PAYEESEQ = pos_agy.PAYEESEQ
        AND ga_participant.effectivestartdate <= st.POLICY_ISSUE_DATE
        AND ga_participant.effectiveenddate   >  st.POLICY_ISSUE_DATE
        AND ga_participant.removedate = DT_REMOVEDATE
   ) curr_ip
   --if the component is being capture in previous quarters, then ignore to capture in current quarter
   left join AIA_CB_IDENTIFY_POLICY pre_ip
   on (pre_ip.year || ' ' || pre_ip.quarter) < (curr_ip.year || ' ' || curr_ip.quarter)
   and pre_ip.ponumber = curr_ip.ponumber
   and pre_ip.wri_agt_code = curr_ip.wri_agt_code
   and pre_ip.life_number = curr_ip.life_number
   and pre_ip.coverage_number = curr_ip.coverage_number
   and pre_ip.rider_number = curr_ip.rider_number
   and pre_ip.component_code = curr_ip.component_code
   where curr_ip.rk=1
   and pre_ip.BUNAME is null
   ;

   Log('Final AIA_CB_IDENTIFY_POLICY for FA; row count: ' || to_char(sql%rowcount));

   commit;

end SP_IDENTIFY_POLICY_FA;

PROCEDURE SP_EXEC_COMMISSION_LUMPSUM_FA(P_STR_CB_CYCLEDATE IN VARCHAR2)
  is
  V_LUMPSUM_FLAG   NUMBER;
  V_BATCH_NO       NUMBER;
  v_message        VARCHAR2(2000);
  V_CB_YEAR        VARCHAR2(20);
  V_CB_QUARTER     VARCHAR2(20);
  begin

  init;

    SELECT COUNT(1) INTO V_LUMPSUM_FLAG FROM AIA_CB_PERIOD WHERE CB_CYCLEDATE = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE)and cb_name=STR_COMMISSION and BUNAME=STR_BUNAME_FA;
    IF V_LUMPSUM_FLAG >0 THEN
      --LUMPSUM
         sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_LUMPSUM,STR_COMMISSION);
         V_BATCH_NO := fn_get_batch_no_fa(P_STR_CB_CYCLEDATE, STR_COMMISSION, STR_LUMPSUM, STR_STATUS_START);
         --SP_POLICY_EXCL(P_STR_CB_CYCLEDATE, STR_COMMISSION);
         --SP_IDENTIFY_POLICY (P_STR_CB_CYCLEDATE, STR_COMMISSION);
         sp_revert_by_batch(V_BATCH_NO);
/*         --get clawback year and quarter from clawback period table
         select cbp.year, cbp.quarter
         into V_CB_YEAR, V_CB_QUARTER
         from aia_cb_period cbp where cbp.cb_cycledate = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE);
         --run report for identify policy result
         PK_AIA_CB_REPORTS.SP_RPT_CB_MASTER_POLICY (V_CB_YEAR , V_CB_QUARTER);  */

         SP_TRACE_FORWARD_COMMISSION_FA (P_STR_CB_CYCLEDATE,STR_LUMPSUM, V_BATCH_NO);
         SP_CLAWBACK_COMMISSION_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO);
         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) ;
     ELSE Log(P_STR_CB_CYCLEDATE || ' is not the avaiable clawback cycle date for FA');
    END IF;
        ---catch exception
        EXCEPTION WHEN OTHERS
        THEN    v_message := SUBSTR(SQLERRM,1,2000);
                Log(v_message);
                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
  END SP_EXEC_COMMISSION_LUMPSUM_FA;


 PROCEDURE sp_create_batch_no_FA(P_STR_CB_CYCLEDATE IN VARCHAR2, P_STR_CB_TYPE IN VARCHAR2, P_STR_CB_NAME IN VARCHAR2)
  is
  V_BATCH_NO INTEGER;
  V_CB_QUARTER_NAME varchar2(50);
  V_CB_CYCLE_TYPE varchar2(50);
  V_PREVIOUS_BATCH_NO INTEGER;
  STR_WEEKLY_CYCLE_DATE varchar2(50);
  V_MONTHEND_FLAG INTEGER;
  NUM_OF_CYCLE_IND integer;
  NUM_LAST_BATCH integer;
  begin

if P_STR_CB_TYPE = STR_LUMPSUM then
--get measurement quarter name for lumpsum clawback
select --cbp.cb_quarter_name
       substr(cbp.cb_quarter_name, instr(cbp.cb_quarter_name, ' ') + 1) || ' ' ||
       substr(cbp.cb_quarter_name, 1, instr(cbp.cb_quarter_name, ' ') - 1)
  into V_CB_QUARTER_NAME
  from AIA_CB_PERIOD cbp
 where CB_CYCLEDATE = to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE)
 and cbp.buname = STR_BUNAME_FA
 and cbp.cb_name = P_STR_CB_NAME
 ;

 --need to be revised
 --get current quarter by P_STR_CB_CYCLEDATE
ELSIF P_STR_CB_TYPE = STR_ONGOING and P_STR_CB_NAME = STR_COMMISSION then

    select to_char(to_date(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE),'yyyymm')
    into V_CB_QUARTER_NAME
    from dual;

ELSIF P_STR_CB_TYPE = STR_ONGOING and P_STR_CB_NAME = STR_COMPENSATION then

 select to_char(to_date(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE),'yyyymm')
    into V_CB_QUARTER_NAME
    from dual;

end if;

--get last batch number of batch
  select nvl(max(t.batchnum),0)
    into V_PREVIOUS_BATCH_NO
    from AIA_CB_BATCH_STATUS t
   where t.buname = STR_BUNAME_FA
     and t.clawbacktype = P_STR_CB_TYPE
     and t.clawbackname = P_STR_CB_NAME
     and t.cb_quarter_name = V_CB_QUARTER_NAME
     ;
       --and t.status in (STR_STATUS_FAIL, STR_STATUS_COMPLETED_SP, STR_STATUS_COMPLETED_SH);

--Log('V_PREVIOUS_BATCH_NO: ' || V_PREVIOUS_BATCH_NO);

--get batch number by max(batch number) + 1
  select nvl(max(batchnum),0) + 1 into V_BATCH_NO from AIA_CB_BATCH_STATUS;

--Log('V_BATCH_NO: ' || V_BATCH_NO);

--update the column islatest for previous cycle
if V_PREVIOUS_BATCH_NO > 0 then
  update AIA_CB_BATCH_STATUS cbs
     set islatest = 'N'
   where cbs.batchnum = V_PREVIOUS_BATCH_NO;

   commit;

end if;

--insert new cycle record
insert into AIA_CB_BATCH_STATUS
  (batchnum,
   BUNAME,
   cb_quarter_name,
   status,
   isactive,
   islatest,
   ispopulated,
   cycledate,
   clawbackname,
   clawbacktype,
   createdate,
   updatedate)
values
  (V_BATCH_NO,
   STR_BUNAME_FA,
   V_CB_QUARTER_NAME,
   STR_STATUS_START,
   'Y',
   'Y',
   'N',
   to_date(P_STR_CB_CYCLEDATE, STR_DATE_FORMAT_TYPE),
   P_STR_CB_NAME,
   P_STR_CB_TYPE,
   sysdate,
   '');

  commit;

  Log('V_BATCH_NO for FA: ' || V_BATCH_NO);
  Log('V_CB_QUARTER_NAME for FA: ' || V_CB_QUARTER_NAME);
  end sp_create_batch_no_fa
    ;

function fn_get_batch_no_fa(P_STR_CYCLEDATE IN VARCHAR2, P_CB_NAME IN VARCHAR2, P_CB_TYPE IN VARCHAR2, P_STATUS IN VARCHAR2) return number
  is
  v_batch_no integer;
  begin

  select nvl(max(cbs.batchnum), 0)
    into v_batch_no
    from AIA_CB_BATCH_STATUS cbs
   where to_char(cbs.cycledate, 'yyyymm') =
         to_char(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE), 'yyyymm')
     and cbs.status = P_STATUS
     and cbs.clawbacktype = P_CB_TYPE
     and cbs.clawbackname = P_CB_NAME
     and cbs.islatest = 'Y'
     and cbs.buname = STR_BUNAME_FA;

  return v_batch_no;

  end fn_get_batch_no_fa
  ;

PROCEDURE SP_TRACE_FORWARD_COMMISSION_FA (P_STR_CYCLEDATE IN VARCHAR2, P_STR_TYPE IN VARCHAR2, P_BATCH_NO IN INTEGER) as

V_CAL_PERIOD VARCHAR2(30); --measurement quarter
DT_CB_START_DATE DATE;
DT_CB_END_DATE DATE;
DT_INCEPTION_START_DATE DATE;
DT_INCEPTION_END_DATE DATE;
DT_WEEKLY_START_DATE DATE;
DT_WEEKLY_END_DATE DATE;
DT_ONGOING_START_DATE DATE;
DT_ONGOING_END_DATE DATE;
--NUM_OF_CYCLE_IND integer;
v_cb_period  aia_cb_period%rowtype;
vSQL varchar2(4000);
vCalendarseq integer;
vPertypeSeq integer;
TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
t_periodseq periodseq_type;
vOngoingperiod number;
vOngoingendperiod number;
begin

init;

--update status
sp_update_batch_status (P_BATCH_NO,'processing');

select calendarseq into vCalendarseq from cs_calendar where removedate =DT_REMOVEDATE and name='AIA Singapore Calendar';

select periodtypeseq into vPertypeSeq from cs_periodtype where removedate =DT_REMOVEDATE  and name='month';

/*
--if the input parameter for cycledate is not exist in AIA_CB_PERIOD, the program will end.
select count(1)
  into NUM_OF_CYCLE_IND
  from AIA_CB_PERIOD cbp
 where CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE);

if NUM_OF_CYCLE_IND = 0 then
  Log(P_STR_CYCLEDATE || ' is not the eligible cycle date.');
  return;
END IF; */

Log('SP_TRACE_FORWARD_COMMISSION start for FA');

/*  select p.periodseq --BULK COLLECT into t_periodseq
  from cs_period a, cs_period p
  where a.calendarseq=2251799813685250
    and p.calendarseq=2251799813685250
    and a.name=V_CAL_PERIOD--'Q3 2016'
    and p.periodtypeseq = 2814749767106561
    and p.startdate>=a.startdate
    and p.enddate <=a.enddate;
*/

--get cycle date for weekly payment
--weekly payment start date
select to_date(TXT_KEY_VALUE , STR_DATE_FORMAT_TYPE)
  into DT_WEEKLY_START_DATE
  from IN_ETL_CONTROL
 where txt_key_string = 'PAYMENT_START_DATE_WEEKLY';

--weekly payment end date
select to_date(TXT_KEY_VALUE , STR_DATE_FORMAT_TYPE)
  into DT_WEEKLY_END_DATE
  from IN_ETL_CONTROL
 where txt_key_string = 'PAYMENT_END_DATE_WEEKLY';

 Log('For FA DT_WEEKLY_START_DATE = ' || DT_WEEKLY_START_DATE);
 Log('For FA DT_WEEKLY_END_DATE = ' || DT_WEEKLY_END_DATE);

if P_STR_TYPE = STR_LUMPSUM then

select * into v_cb_period from aia_cb_period where cb_cycledate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_COMMISSION and BUNAME=STR_BUNAME_FA;

select cbp.quarter || ' ' || cbp.year,
       cbp.cb_startdate,
       cbp.cb_enddate,
       cbp.inception_startdate,
       cbp.inception_enddate
  into V_CAL_PERIOD,
       DT_CB_START_DATE,
       DT_CB_END_DATE,
       DT_INCEPTION_START_DATE,
       DT_INCEPTION_END_DATE
  from AIA_CB_PERIOD cbp
 where CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_COMMISSION and cbp.BUNAME=STR_BUNAME_FA;

Log('For FA DT_LUMPSUM_START_DATE = ' || DT_CB_START_DATE);
Log('For FA DT_LUMPSUM_END_DATE = ' || DT_CB_END_DATE);

-- Get the periodseqs for lumpsum period
 select
periodseq BULK COLLECT into t_periodseq
from cs_period csp
 inner join cs_periodtype pt
    on csp.periodtypeseq = pt.periodtypeseq
 where csp.startdate >= DT_CB_START_DATE
and csp.enddate <= DT_CB_END_DATE + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and pt.name = STR_CALENDAR_TYPE;

   execute immediate 'truncate table aia_tmp_comls_period';
insert into aia_tmp_comls_period
select periodseq  from cs_period csp
 inner join cs_periodtype pt
    on csp.periodtypeseq = pt.periodtypeseq
 where csp.startdate >= DT_CB_START_DATE
and csp.enddate <= DT_CB_END_DATE + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and pt.name = STR_CALENDAR_TYPE;
   commit;

Log('insert into AIA_CB_TRACE_FORWARD for FA, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);
/*
for i in 1..t_periodseq.count loop

--for lumpsum commission trace forward
insert /*+ APPEND   into AIA_CB_TRACE_FORWARD
select STR_BUNAME as BUNAME,
       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
       pm.positionseq as PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
       crd.genericattribute12 as PAYOR_CODE,
       ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
       crd.compensationdate as TRANSACTION_DATE,
       --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       --clawback type = 'Lumpsum'
       STR_LUMPSUM as CLAWBACK_TYPE,
       --clawback name = 'Commission'
       --STR_CB_NAME            as CLAWBACK_NAME,
       rl.CLAWBACK_NAME       as CLAWBACK_NAME,
       ct.credittypeid        as CREDITTYPE,
       crd.creditseq          as CREDITSEQ,
       crd.name               as CREDIT_NAME,
       crd.value              as CREDIT_VALUE,
       pm.measurementseq      as PM_SEQ,
       pm.name                as PM_NAME,
       pct.contributionvalue  as PM_CONTRIBUTE_VALUE,
       1                      as PM_RATE,
       dep.depositseq         as DEPOSITSEQ,
       dep.name               as DEPOSIT_NAME,
       dep.value              as DEPOSIT_VALUE,
       crd.periodseq          as PERIODSEQ,
       st.salestransactionseq as SALESTRANSACTIONSEQ,
       crd.genericattribute2  as PRODUCT_NAME,
       crd.genericnumber1     as POLICY_YEAR,
       st.genericnumber2      as COMMISSION_RATE,
       st.genericdate4        as PAID_TO_DATE,
       P_BATCH_NO             as BATCH_NUMBER,
       sysdate                as CREATED_DATE
  FROM CS_SALESTRANSACTION st
 inner join cs_period p
    on      st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate    and p.calendarseq=2251799813685250
 inner join CS_CREDIT crd
    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
        and crd.periodseq=p.periodseq
 inner join CS_PMCREDITTRACE pct
    on crd.CREDITSEQ = pct.CREDITSEQ
 inner join CS_MEASUREMENT pm
    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
 inner join cs_depositpmtrace dpt
    on pm.measurementseq = dpt.measurementseq
 inner join cs_deposit dep
    on dep.depositseq = dpt.depositseq
 inner join cs_position dep_pos
    on dep.positionseq = dep_pos.ruleelementownerseq
   and dep_pos.removedate = DT_REMOVEDATE
   and dep_pos.effectivestartdate <= crd.genericdate2
   and dep_pos.effectiveenddate > crd.genericdate2
 inner join CS_CREDITTYPE ct
    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
   and ct.Removedate = DT_REMOVEDATE
 inner join AIA_CB_IDENTIFY_POLICY ip
    on ip.BUNAME = STR_BUNAME
   AND st.PONUMBER = ip.PONUMBER
   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
   AND st.PRODUCTID = ip.COMPONENT_CODE
   and crd.genericattribute12 = ip.wri_agt_code
   and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
   --check if the deposit position is same as writing agent
   and dep_pos.name = 'SGT' || ip.wri_agt_code
 inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME in (STR_COMMISSION,STR_GST_COMMISSION)) rl
    on pm.NAME = rl.SOURCE_RULE_OUTPUT
 WHERE st.tenantid='AIAS' and crd.tenantid='AIAS' and pm.tenantid='AIAS'
 and pct.tenantid='AIAS' and dpt.tenantid='AIAS'
 and pct.PROCESSINGUNITSEQ= V_PROCESSINGUNITSEQ
 and pct.TARGETPERIODSEQ=pm.periodseq
 and dpt.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
      and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
      and crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
      and pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
      and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
      AND st.BUSINESSUNITMAP = 1
   and crd.genericattribute16 not in ('RO', 'RNO')
   and  dep.periodseq =  t_periodseq(i)
     ;
*/



 execute immediate 'truncate table aia_tmp_Comls_Step0';
 insert into aia_tmp_Comls_Step0
 (SALESTRANSACTIONSEQ,
WRI_AGT_CODE_ORIG,
CALCULATION_PERIOD,
POLICY_NUMBER,
POLICYIDSEQ,
LIFE_NUMBER,
COVERAGE_NUMBER,
RIDER_NUMBER,
COMPONENT_CODE,
COMPONENT_NAME,
BASE_RIDER_IND,
COMMISSION_RATE,
PAID_TO_DATE,
WRI_AGT_CODE,
GENERICATTRIBUTE10, --version 6 update by Amanda Wei
GA26_WRI_AGT2, --version 10 for share agent2
QTRYR,
EXTENDEDDATE9,
FAOB_AGT_CODE) --Added by Gopi, to consider the migrated policies
 select /*+ leading(ip,st) */ st.salestransactionseq,  ip.wri_agt_code as wri_agt_code_ORIG
 , IP.QUARTER
  || ' '
  || IP.YEAR             AS CALCULATION_PERIOD,
  IP.PONUMBER            AS POLICY_NUMBER,
  IP.POLICYIDSEQ         AS POLICYIDSEQ,
  IP.LIFE_NUMBER         AS LIFE_NUMBER,
  IP.COVERAGE_NUMBER     AS COVERAGE_NUMBER,
  IP.RIDER_NUMBER        AS RIDER_NUMBER,
  IP.COMPONENT_CODE      AS COMPONENT_CODE,
  IP.COMPONENT_NAME      AS COMPONENT_NAME,
  IP.BASE_RIDER_IND      AS BASE_RIDER_IND,
  ST.GENERICNUMBER2      AS COMMISSION_RATE,
  ST.GENERICDATE4        AS PAID_TO_DATE ,
  'SGT' ||IP.WRI_AGT_CODE WRI_AGT_CODE ,
  'SGT' || st.GENERICATTRIBUTE10, --version 6 update by Amanda Wei
  'SGT' || st.GENERICATTRIBUTE26, --version 10 for share agent2
  IP.QUARTER
  || ' '
  || IP.YEAR QTRYR,
  GST.GENERICDATE9,
  IP.FAOB_AGT_CODE
 from cs_Salestransaction st
 INNER JOIN AIA_CB_IDENTIFY_POLICY IP
ON 1                              =1
AND IP.BUNAME                     = STR_BUNAME_FA
AND ST.PONUMBER                   = IP.PONUMBER
AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER
AND ST.PRODUCTID = IP.COMPONENT_CODE  --Added based on Endi request-Gopi -27062019
INNER JOIN CS_GASALESTRANSACTION GST ---Added by Gopi, to consider the migrated policies --12092019
ON gst.pagenumber = 0
AND st.salestransactionseq=GST.salestransactionseq
 where st.tenantid='AIAS'
and st.processingUnitseq=V_PROCESSINGUNITSEQ
--and st.compensationdate between '1-mar-2017' and '31-may-2017'
and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
and GST.tenantid='AIAS'
and gst.processingUnitseq=V_PROCESSINGUNITSEQ
and gst.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE

;



execute immediate 'TRUNCATE table aia_tmp_comls_step1';

insert into AIA_TMP_COMLS_step1

SELECT /*+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ ) */
  DISTINCT CRD.CREDITSEQ,
  CRD.SALESTRANSACTIONSEQ ,
  ip.CALCULATION_PERIOD,
  ip.POLICY_NUMBER,
  ip.POLICYIDSEQ,
  ip.LIFE_NUMBER,
  ip.COVERAGE_NUMBER,
  ip.RIDER_NUMBER,
  IP.COMPONENT_CODE      ,
  IP.COMPONENT_NAME     ,
  IP.BASE_RIDER_IND     ,
  CRD.COMPENSATIONDATE   AS TRANSACTION_DATE,
  CRD.GENERICATTRIBUTE12 AS PAYOR_CODE,
  CT.CREDITTYPEID        AS CREDITTYPE,
  CRD.NAME               AS CREDIT_NAME,
  CRD.VALUE              AS CREDIT_VALUE,
  CRD.PERIODSEQ          AS PERIODSEQ,
  CRD.GENERICATTRIBUTE2  AS PRODUCT_NAME,
  CRD.GENERICNUMBER1     AS POLICY_YEAR,
  ip.COMMISSION_RATE      AS COMMISSION_RATE,
  ip.PAID_TO_DATE        AS PAID_TO_DATE ,
  ip.WRI_AGT_CODE ,
  ip.QTRYR,
  CRD.GENERICDATE2
FROM CS_CREDIT CRD
JOIN AIA_TMP_COMLS_PERIOD P
ON CRD.PERIODSEQ=P.PERIODSEQ
INNER JOIN CS_CREDITTYPE CT
ON CRD.CREDITTYPESEQ = CT.DATATYPESEQ
AND CT.REMOVEDATE    >SYSDATE
INNER JOIN aia_tmp_Comls_Step0 IP
ON 1                              =1
--AND IP.BUNAME                     = 'SGPAGY'
and crd.salestransactionseq= ip.salestransactionseq
--version 6 update by Amanda Wei begin
--AND (CRD.GENERICATTRIBUTE12 = IP.WRI_AGT_CODE_ORIG or CRD.GENERICATTRIBUTE12 = IP.FAOB_AGT_CODE)
--AND (CRD.GENERICATTRIBUTE12 = IP.WRI_AGT_CODE_ORIG or CRD.GENERICATTRIBUTE12 = IP.FAOB_AGT_CODE or IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE
AND ((CRD.GENERICATTRIBUTE16 IN ('O') and CRD.GENERICATTRIBUTE12= IP.WRI_AGT_CODE_ORIG) or (CRD.GENERICATTRIBUTE16 IN ('RO','RNO')and IP.GENERICATTRIBUTE10=IP.WRI_AGT_CODE)
or (CRD.GENERICATTRIBUTE16 IN ('RO','RNO')and IP.GA26_WRI_AGT2=IP.WRI_AGT_CODE) ) --version 10 add for share case
INNER JOIN cs_participant PAR ON PAR.USERID = IP.WRI_AGT_CODE
AND PAR.REMOVEDATE = DT_REMOVEDATE
INNER JOIN cs_gaparticipant GA_PAR ON PAR.PAYEESEQ = GA_PAR.PAYEESEQ
AND GA_PAR.REMOVEDATE = DT_REMOVEDATE
--version 6 update by Amanda Wei end
WHERE 1=1
--and CRD.GENERICATTRIBUTE16 NOT IN ('RO', 'RNO')
--and (CRD.GENERICATTRIBUTE16 NOT IN ('RO', 'RNO') OR (CRD.GENERICATTRIBUTE16 IN ('RO', 'RNO') AND ip.Extendeddate9 is not null)) ---Added by Gopi, to consider the migrated policies --12092019
--version 10 start
--and (CRD.GENERICATTRIBUTE16 NOT IN ('RO', 'RNO') OR (CRD.GENERICATTRIBUTE16 IN ('RO', 'RNO') AND ip.Extendeddate9 is not null) or (IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE AND CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4)) --version 6 update by Amanda Wei
--Harm_Phase4 start
--AND (CRD.GENERICATTRIBUTE16 IN ('O') or ((IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or IP.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)  AND CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4))
--AND (CRD.GENERICATTRIBUTE16 IN ('O') or ((IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or IP.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)  AND CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11))
AND (CRD.GENERICATTRIBUTE16 IN ('O') or ((IP.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or IP.GA26_WRI_AGT2 = IP.WRI_AGT_CODE) AND (CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4 OR CRD.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11))) --version 14 Harm BSC SPI
--Harm_Phase4 end
--version 10 end
AND CRD.TENANTID                  = 'AIAS'
AND CRD.PROCESSINGUNITSEQ         = 38280596832649218

;

/* 170807
insert into aia_tmp_comls_step1

--drop table aia_tmp_comls_step1;
--create table aia_tmp_comls_step1 as
select crd.creditseq,
       crd.salestransactionseq ,
        ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
        ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
        crd.compensationdate as TRANSACTION_DATE,
         crd.genericattribute12 as PAYOR_CODE,
         ct.credittypeid        as CREDITTYPE,
       crd.name               as CREDIT_NAME,
       crd.value              as CREDIT_VALUE,
        crd.periodseq          as PERIODSEQ,
         crd.genericattribute2  as PRODUCT_NAME,
       crd.genericnumber1     as POLICY_YEAR,
       st.genericnumber2      as COMMISSION_RATE,
       st.genericdate4        as PAID_TO_DATE
       ,'SGT'||ip.wri_agt_code wri_agt_code
       ,ip.quarter || ' ' || ip.year qtrYr, crd.genericdate2

  from cs_Credit crd
  join aia_tmp_comls_period p
  on crd.periodseq=p.periodseq
  join cs_Salestransaction st
  on st.salestransactionseq=crd.salestransactionseq
  and st.tenantid='AIAS' and st.processingunitseq=crd.processingunitseq
 -- and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
   inner join CS_CREDITTYPE ct
    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
   and ct.Removedate >sysdate
  inner join AIA_CB_IDENTIFY_POLICY ip
    on 1=1
    and ip.BUNAME = STR_BUNAME
   AND st.PONUMBER = ip.PONUMBER
   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
   AND st.PRODUCTID = ip.COMPONENT_CODE
   and crd.genericattribute12 = ip.wri_agt_code
   where crd.genericattribute16 not in ('RO', 'RNO')
   and crd.tenantid = 'AIAS'
   and crd.processingunitseq = V_PROCESSINGUNITSEQ
  --and st.compensationdate>='1-mar-2016' and st.compensationdate<='30-nov-2016'
--   and periodseq = 2533274790398934
--105 seconds. 9 mill rows for nov
--9 secs, 1221 rows
-- 240 secs 5000 rows
--select count(*) from xtmp
;*/



Log('insert 1 done for FA'||SQL%ROWCOUNT);
commit;

delete from AIA_TMP_COMLS_STEP1 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;


Log('delete 1 done for FA '||SQL%ROWCOUNT);

commit;
DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP1"',
          estimate_percent => 1
          );
execute immediate 'truncate table aia_tmp_comls_step2';
insert into aia_tmp_comls_step2
--drop table aia_tmp_comls_step2;
--create table aia_tmp_comls_step2  as

select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , x.clawback_name
from cs_measurement m
join aia_tmp_comls_period p
  on m.periodseq=p.periodseq
  join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME in (STR_COMMISSION,STR_GST_COMMISSION)
                and BUNAME=STR_BUNAME_FA)
                x
                on x.SOURCE_RULE_OUTPUT=m.name
  where  m.processingunitseq = V_PROCESSINGUNITSEQ
  and m.tenantid='AIAS'
   ;



Log('insert 2 done  for FA '||SQL%ROWCOUNT);
commit;
DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP2"',
          estimate_percent => 1
          );

  execute immediate 'truncate table aia_tmp_comls_step3';
  insert into aia_tmp_comls_step3

  -- drop table aia_tmp_comls_step3
  -- create table aia_tmp_comls_step3 as
   select   pct.creditseq pctCreditSeq,
   pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq
   , s1.CREDITSEQ
,SALESTRANSACTIONSEQ
,CALCULATION_PERIOD
,POLICY_NUMBER
,POLICYIDSEQ
,LIFE_NUMBER
,COVERAGE_NUMBER
,RIDER_NUMBER
,COMPONENT_CODE
,COMPONENT_NAME
,BASE_RIDER_IND
,TRANSACTION_DATE
,PAYOR_CODE
,CREDITTYPE
,CREDIT_NAME
,CREDIT_VALUE
,s1.PERIODSEQ
,PRODUCT_NAME
,POLICY_YEAR
,COMMISSION_RATE
,PAID_TO_DATE

   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
   ,WRI_AGT_CODE
,QTRYR
,GD2

   from cs_pmcredittrace pct
   join aia_tmp_comls_step1 s1
   on pct.creditseq=s1.creditseq
   join aia_tmp_comls_step2 s2
   on s2.measurementseq=pct.measurementseq and ((s2.ruleseq=pct.ruleseq and s2.name!='PM_NADOR_CM') OR (s2.name='PM_NADOR_CM')) -- Added condiftion to not check ruleseq for NADOR measurements-Gopi-25102019
   --and pct.targetperiodseq=s2.periodseq
   join cs_depositpmtrace dct
   on 1=1
   and dct.measurementseq=pct.measurementseq
   --and dct.targetperiodseq=s2.periodseq
   and dct.tenantid='AIAS' and pct.tenantid='AIAS'
   and dct.processingUnitseq=V_PROCESSINGUNITSEQ and pct.processingUnitseq=V_PROCESSINGUNITSEQ

;
commit;
Log('insert 3 done for FA');
DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP3"',
          estimate_percent => 1
          );

insert into AIA_CB_TRACE_FORWARD

select DISTINCT
STR_BUNAME_FA as BUNAME,
       QtrYr as CALCULATION_PERIOD,
        POLICY_NUMBER,
        POLICYIDSEQ,
       mPositionseq PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
         PAYOR_CODE,
        LIFE_NUMBER,
       COVERAGE_NUMBER,
        RIDER_NUMBER,
      COMPONENT_CODE,
         COMPONENT_NAME,
         BASE_RIDER_IND,
       TRANSACTION_DATE,
       --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       --clawback type = 'Lumpsum'
       STR_LUMPSUM as CLAWBACK_TYPE,
       --clawback name = 'Commission'
       --STR_CB_NAME            as CLAWBACK_NAME,
       CLAWBACK_NAME       as CLAWBACK_NAME,
        CREDITTYPE,
        CREDITSEQ,
        CREDIT_NAME,
         CREDIT_VALUE,
       measurementseq      as PM_SEQ,
       mname                as PM_NAME,
       pctcontribvalue  as PM_CONTRIBUTE_VALUE,
       1                      as PM_RATE,
       dep.depositseq         as DEPOSITSEQ,
       dep.name               as DEPOSIT_NAME,
       dep.value              as DEPOSIT_VALUE,
       x.periodseq          as PERIODSEQ,
       salestransactionseq as SALESTRANSACTIONSEQ,
        PRODUCT_NAME,
         POLICY_YEAR,
         COMMISSION_RATE,
         PAID_TO_DATE,
       P_BATCH_NO             as BATCH_NUMBER,
       sysdate                as CREATED_DATE
       ,substr(x.WRI_AGT_CODE,4) --version 10
       from aia_tmp_comls_step3 x
       join cs_deposit dep
       on dep.depositseq=x.depositseq
       join cs_position dep_pos
       on dep.positionseq = dep_pos.ruleelementownerseq
   and dep_pos.removedate = DT_REMOVEDATE
--   and dep_pos.effectivestartdate <= x.GD2
--   and dep_pos.effectiveenddate > x.GD2
       and dep_pos.name = 'SGT'||x.payor_code          -- x.wri_agt_code  --Modified by to Get New agent code also
   where x.qtrYr = V_CAL_PERIOD
   ;


Log('insert into AIA_CB_TRACE_FORWARD for FA' || '; row count: ' || to_char(sql%rowcount));

commit;

--end loop;

elsif P_STR_TYPE = STR_ONGOING then
    --setup the start date and end date for on-going period
    if to_date(P_STR_CYCLEDATE , STR_DATE_FORMAT_TYPE) = DT_WEEKLY_END_DATE then
      DT_ONGOING_START_DATE := trunc(to_date(P_STR_CYCLEDATE , STR_DATE_FORMAT_TYPE),'MONTH');
      DT_ONGOING_END_DATE := DT_WEEKLY_END_DATE;
    else
      DT_ONGOING_START_DATE := trunc(to_date(P_STR_CYCLEDATE , STR_DATE_FORMAT_TYPE),'MONTH');
    select csp.enddate - 1
      into DT_ONGOING_END_DATE
      from cs_period csp
     inner join cs_periodtype pt
        on csp.periodtypeseq = pt.periodtypeseq
     where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
       and csp.removedate = DT_REMOVEDATE
       and calendarseq = V_CALENDARSEQ
       and pt.name = 'month';
    end if;

Log('FA DT_ONGOING_START_DATE = ' || DT_ONGOING_START_DATE);
Log('FA DT_ONGOING_END_DATE = ' || DT_ONGOING_END_DATE);

select min(periodseq) into vOngoingperiod
from CS_period where removedate>sysdate and startdate=add_months(last_day(trunc(DT_ONGOING_START_DATE))+1,-1)
and periodtypeseq=2814749767106561 and calendarseq=V_CALENDARSEQ
and removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
;

select min(periodseq) into vOngoingendperiod
from CS_period where removedate>sysdate and startdate=add_months(last_day(trunc(DT_ONGOING_END_DATE))+1,-1)
and periodtypeseq=2814749767106561 and calendarseq=V_CALENDARSEQ
and removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
;

Log('insert into AIA_CB_TRACE_FORWARD for FA, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);

execute immediate 'truncate table AIA_CB_TRACE_FORWARD_TMP';
Log('insert 1 started for FA');
---temp table insert  AIA_CB_TRACE_FORWARD_TMP
insert /*+ APPEND */  into AIA_CB_TRACE_FORWARD_TMP
select  /*+ PARALLEL leading(crd) */  null as PAYEE_SEQ,
null as PAYEE_CODE,
crd.genericattribute12 as PAYOR_CODE,
crd.compensationdate as TRANSACTION_DATE,
ct.credittypeid as CREDITTYPE,
crd.creditseq as CREDITSEQ,
crd.name as CREDIT_NAME,
crd.value as CREDIT_VALUE,
null as PM_SEQ,
null as PM_NAME,
pct.CONTRIBUTIONVALUE as PM_CONTRIBUTE_VALUE,
crd.periodseq as PERIODSEQ,
st.salestransactionseq as SALESTRANSACTIONSEQ,
crd.genericattribute2 as PRODUCT_NAME,
crd.genericnumber1 as POLICY_YEAR,
st.genericnumber2 as COMMISSION_RATE,
st.genericdate4 as PAID_TO_DATE,
st.GENERICATTRIBUTE29,
st.PONUMBER,
st.GENERICATTRIBUTE30,
st.GENERICATTRIBUTE31,
st.PRODUCTID,
crd.genericattribute12,
null name,
pct.measurementseq,
null,
crd.genericdate2,
pct.targetperiodseq -- v5
,st.genericattribute10, crd.genericattribute16, GST.GENERICDATE9 --version 6 add by Amanda Wei
,st.genericattribute26 --version 10
  FROM CS_SALESTRANSACTION st
 inner join CS_CREDIT crd
on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
--and crd.genericdate2
 inner join CS_PMCREDITTRACE pct
 -- v4 added sourceperiodseq to hit index
on crd.CREDITSEQ = pct.CREDITSEQ
and crd.periodseq= pct.sourceperiodseq  --- Modified by Sundeep
and crd.pipelinerunseq=pct.pipelinerunseq  --Added by Sundeep
 inner join CS_CREDITTYPE ct
on crd.CREDITTYPESEQ = ct.DATATYPESEQ
 and ct.Removedate = DT_REMOVEDATE
-- v5 start
 inner join AIA_CB_IDENTIFY_POLICY ip
      on ip.BUNAME = STR_BUNAME_FA
     AND st.PONUMBER = ip.PONUMBER
     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND st.PRODUCTID = ip.COMPONENT_CODE
     and (crd.genericattribute12 = ip.wri_agt_code or crd.genericattribute12=IP.FAOB_AGT_CODE)
-- v5 end
 inner join cs_businessunit bu on st.businessunitmap = bu.mask
 -- v4 added pagenumber
 inner join cs_gasalestransaction GST on GST.SALESTRANSACTIONSEQ=st.SALESTRANSACTIONSEQ and gst.pagenumber=0 ---Added by Gopi, to consider the migrated policies --12092019
 WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
-- AND bu.name = STR_BUNAME_FA    --Added by Gopi, to consider the migrated policies --16012019
 and st.compensationdate between DT_ONGOING_START_DATE and DT_ONGOING_END_DATE
 ---Added by Gopi, to consider the migrated policies --12092019
 -- v3 changed AND condition to OR
-- and (crd.genericattribute16 not in ('RO', 'RNO') OR (crd.genericattribute16 in ('RO', 'RNO') and GST.GENERICDATE9 is not null))  --version 6 remove by Amanda Wei
 and crd.periodseq between vOngoingperiod and vOngoingendperiod
 and crd.tenantid='AIAS' and crd.processingunitseq=V_PROCESSINGUNITSEQ
 and st.tenantid='AIAS' and st.processingunitseq=V_PROCESSINGUNITSEQ
 and pct.tenantid='AIAS' and pct.processingunitseq=V_PROCESSINGUNITSEQ
 --v4 added to hit partition
 and gst.tenantid='AIAS' and pct.processingunitseq=V_PROCESSINGUNITSEQ
 and gst.compensationdate between DT_ONGOING_START_DATE and DT_ONGOING_END_DATE
 -- v5
 and st.compensationdate = crd.compensationdate
 ;

 commit;
Log('insert 1 ended for FA');

Log('insert 2 started for FA');
------Main table insert  AIA_CB_TRACE_FORWARD
-- v12 add ORDERED hints
insert /*+ APPEND */  into AIA_CB_TRACE_FORWARD
  select /*+ ORDERED */ DISTINCT  STR_BUNAME_FA as BUNAME,
         ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
         ip.ponumber as POLICY_NUMBER,
         ip.policyidseq as POLICYIDSEQ,
        pm.positionseq as PAYEE_SEQ,
         substr(pm_pos.name, 4) as PAYEE_CODE,
         tmp.PAYOR_CODE,
         ip.life_number as LIFE_NUMBER,
         ip.coverage_number as COVERAGE_NUMBER,
         ip.rider_number as RIDER_NUMBER,
         ip.component_code as COMPONENT_CODE,
         ip.component_name as COMPONENT_NAME,
         ip.base_rider_ind as BASE_RIDER_IND,
         tmp.TRANSACTION_DATE,
         --TO_CHAR(DT_CB_START_DATE, 'MON-YYYY') as PROCESSING_PERIOD,
         TO_CHAR(to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE), 'MON-YYYY') as PROCESSING_PERIOD,
         --clawback type = 'Lumpsum'
         STR_ONGOING as CLAWBACK_TYPE,
         --clawback name = 'Commission'
         --STR_CB_NAME            as CLAWBACK_NAME,
         rl.CLAWBACK_NAME as CLAWBACK_NAME,
         tmp.CREDIT_TYPE,
         tmp.CREDITSEQ,
         tmp.CREDIT_NAME,
         tmp.CREDIT_VALUE,
        pm.measurementseq as PM_SEQ,
         pm.name as PM_NAME,
         tmp.PM_CONTRIBUTION_VALUE,
         1 as PM_RATE,
         '' as DEPOSITSEQ,
         '' as DEPOSIT_NAME,
         '' as DEPOSIT_VALUE,
         tmp.PERIODSEQ,
         tmp.SALESTRANSACTIONSEQ,
         tmp.PRODUCT_NAME,
         tmp.POLICY_YEAR,
         tmp.COMMISSION_RATE,
         tmp.PAID_TO_DATE,
         P_BATCH_NO as BATCH_NUMBER,
         sysdate as CREATED_DATE
         ,ip.wri_agt_code --version 10
    FROM AIA_CB_TRACE_FORWARD_TMP tmp
     inner join CS_MEASUREMENT pm
      on tmp.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
 inner join CS_POSITION pm_pos
on pm_pos.ruleelementownerseq =pm.positionseq
and pm_pos.removedate = DT_REMOVEDATE
and pm_pos.islast=1
-- and pm_pos.effectivestartdate <= tmp.genericdate2
-- and pm_pos.effectiveenddate > tmp.genericdate2   --Added by Gopi, to consider the migrated policies --16012019
   inner join AIA_CB_IDENTIFY_POLICY ip
      on ip.BUNAME = STR_BUNAME_FA
     AND tmp.PONUMBER = ip.PONUMBER
     AND tmp.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND tmp.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND tmp.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND tmp.PRODUCTID = ip.COMPONENT_CODE
--     and tmp.genericattribute12 = ip.wri_agt_code
--     and (tmp.genericattribute12 = ip.wri_agt_code or tmp.genericattribute12=IP.FAOB_AGT_CODE) ---Added by Gopi, to consider the migrated policies --12092019
     --and (tmp.genericattribute12 = ip.wri_agt_code or tmp.genericattribute12=IP.FAOB_AGT_CODE or tmp.genericattribute10 = ip.wri_agt_code) --version 6 update by Amanda Wei
     AND( (tmp.GENERICATTRIBUTE16 IN ('O') and tmp.genericattribute12 = ip.wri_agt_code ) or (tmp.GENERICATTRIBUTE16 IN ('RO','RNO')and tmp.genericattribute10 = ip.wri_agt_code)
          or (tmp.GENERICATTRIBUTE16 IN ('RO','RNO')and tmp.GA26_WRI_AGT2  = ip.wri_agt_code) )  --version 10 add
   inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                 from AIA_CB_RULES_LOOKUP
                where RULE_TYPE = 'PM'
                  AND CLAWBACK_NAME in (STR_COMMISSION, STR_GST_COMMISSION)
                  and BUNAME=STR_BUNAME_FA ) rl
      on pm.NAME = rl.SOURCE_RULE_OUTPUT
   inner join (select distinct
                      cb_quarter_name,
                      cb_startdate,
                      cb_enddate
                 from aia_cb_period
                where cb_name = STR_COMMISSION
                and BUNAME=STR_BUNAME_FA) cbp
      on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
   --version 6 update by Amanda Wei begin
   INNER JOIN cs_participant PAR ON PAR.USERID = 'SGT' || IP.WRI_AGT_CODE
     AND PAR.REMOVEDATE = DT_REMOVEDATE
   INNER JOIN cs_gaparticipant GA_PAR ON PAR.PAYEESEQ = GA_PAR.PAYEESEQ
     AND GA_PAR.REMOVEDATE = DT_REMOVEDATE
   WHERE
     to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
     -- v5 start
     and pm.tenantid = 'AIAS' and pm.processingunitseq = V_PROCESSINGUNITSEQ
     and tmp.PM_TARGETPERIODSEQ = pm.periodseq
     -- v5 end
     AND PAR.TENANTID                  = 'AIAS'
     AND GA_PAR.TENANTID               = 'AIAS'
     --AND (tmp.GENERICATTRIBUTE16 in ('O') or tmp.GENERICDATE9 is not null or (tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE AND tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4))
     --Harm_Phase4 start
     --AND (tmp.GENERICATTRIBUTE16 IN ('O') or ((tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or tmp.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)  AND tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4))
     --AND (tmp.GENERICATTRIBUTE16 IN ('O') or ((tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or tmp.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)  AND tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11))
	 AND (tmp.GENERICATTRIBUTE16 IN ('O') or ((tmp.GENERICATTRIBUTE10 = IP.WRI_AGT_CODE or tmp.GA26_WRI_AGT2 = IP.WRI_AGT_CODE)   AND (tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE4 OR tmp.GENERICATTRIBUTE12 = GA_PAR.GENERICATTRIBUTE11))) --version 14 Harm BSC SPI
     --Harm_Phase4 end
     --version 6 update by Amanda Wei end
     ;


Log('insert into AIA_CB_TRACE_FORWARD for FA' || '; row count: ' || to_char(sql%rowcount));

commit;

Log('SP_TRACE_FORWARD_COMMISSION_FA end');

end if;

end  SP_TRACE_FORWARD_COMMISSION_FA;

/* this procedure is for commission clawback calculation*/
PROCEDURE SP_CLAWBACK_COMMISSION_FA (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO IN INTEGER) as

V_REC_COUNT INTEGER;
V_BATCH_NO_PRE_QTR INTEGER;
V_CB_TYPE VARCHAR2(50);
V_CB_NAME VARCHAR2(50);
V_CB_QTR VARCHAR2(50);
begin

Log('SP_CLAWBACK_COMMISSION_FA start');

init;

--get records count from AIA_CB_CLAWBACK_COMMISSION
select count(1)
  into V_REC_COUNT
  from AIA_CB_CLAWBACK_COMMISSION
 where batch_no = P_BATCH_NO;

--delete the records in AIA_CB_CLAWBACK_COMMISSION if batch number is being reused.
if V_REC_COUNT > 0 then

delete from AIA_CB_CLAWBACK_COMMISSION where batch_no = P_BATCH_NO;
delete from AIA_CB_CLAWBACK_SVI_TMP where batch_no = P_BATCH_NO;

commit;

END IF;

Log('insert into AIA_CB_CLAWBACK_COMMISSION_FA,' ||' batch_no = ' || P_BATCH_NO);

--insert data into AIA_CB_CLAWBACK_COMMISSION for commission
insert into AIA_CB_CLAWBACK_COMMISSION
  select -- RULE*/
  /*+  leading(tf,ip,ba,st,cr) use_nl(tf,ip,ba,st,cr) NO_PARALLEL index(ST AIA_CS_SALESTRANSACTION_SEQ) index(CR OD_CREDIT_CREDITSEQ) */
DISTINCT  tf.calculation_period as MEASUREMENT_QUARTER,
         tf.clawback_type as CLAWBACK_TYPE,
         tf.clawback_name as CLAWBACK_NAME,
         --tf.processing_period as CALCULATION_DATE,
         to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
         pos_dis.GENERICATTRIBUTE3 as WRI_DIST_CODE,
         trim(par_dis.firstname||' '||par_dis.lastname) as WRI_DIST_NAME,
         pos_dis.genericattribute2 as WRI_DM_CODE,
         substr(pos_agy.name, 4) as WRI_AGY_CODE,
         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME,
         pos_agt.GENERICATTRIBUTE2 as WRI_AGY_LDR_CODE,
         pos_agt.genericattribute7 as WRI_AGY_LDR_NAME,
         tf.payor_code as WRI_AGT_CODE,
         trim(par_agt.firstname||' '||par_agt.lastname) as WRI_AGT_NAME,
         --'Normal FSC' as FSC_TYPE,
         decode(par_agt.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
         title_agt.name as RANK,
         cr.genericattribute14 as CLASS,
         pos_agt.genericattribute4 as UM_CLASS,
         ba.bsc_grade as FSC_BSC_GRADE,
         ba.entitlementpercent as FSC_BSC_PERCENTAGE,
         tf.policy_number as PONUMBER,
         tf.LIFE_NUMBER as LIFE_NUMBER,
         tf.COVERAGE_NUMBER as COVERAGE_NUMBER,
         tf.RIDER_NUMBER as RIDER_NUMBER,
         tf.component_code as COMPONENT_CODE,
         tf.product_name as PRODUCT_NAME,
         tf.transaction_date as TRANSACTION_DATE,
         tf.policy_year as POLICY_YEAR,
         case
           when tf.credit_type = 'FYC' then
            tf.credit_value
           else
            0
         end as FYC,
         case
           when tf.credit_type = 'API' then
            tf.credit_value
           else
            0
         end as API,
         case
           when tf.credit_type = 'SSCP' then
            tf.credit_value
           else
            0
         end as SSC,
         case
           when tf.credit_type = 'RYC' then
            tf.credit_value
           else
            0
         end as RYC,
         /**
         --for Commission only
         --if SVI is a negative value, then check if this component exist in last quarter clawback result,
         --if exist and clawback value is negative, then continue, else skip(set figure=0).
         **/
         (tf.pm_contribution_value * INT_SVI_RATE) as SVI,
         (tf.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent as ENTITLEMENT,
         /** SVI - ENTITLEMENT */
         --fix the rounding issue
         round(
         ((tf.pm_contribution_value * INT_SVI_RATE) -
         (tf.pm_contribution_value * INT_SVI_RATE) * ba.entitlementpercent) * (-1)
         ,2)
         as CLAWBACK_VALUE,
         0 as PROCESSED_CLAWBACK,
         --0 as BASIC_RIDER_IND,
         tf.base_rider_ind as BASE_RIDER_IND,
         tf.salestransactionseq,
         tf.creditseq,
         tf.pm_seq,
         P_BATCH_NO,
         0 as OFFSET_CLAWBACK,
         tf.wri_agt_code wri_agt_code_org --version 10
    from AIA_CB_TRACE_FORWARD tf
   inner join aia_cb_identify_policy ip
   on tf.policyidseq = ip.policyidseq
   inner join aia_cb_bsc_agent ba
      on tf.calculation_period = (ba.quarter || ' ' || ba.year)
     and tf.wri_agt_code = ba.agentcode --version 10
--     and tf.payor_code = ba.agentcode
   inner join cs_salestransaction st
      on tf.salestransactionseq = st.salestransactionseq
    --and (ip.wri_agt_code=ba.agentcode or ip.FAOB_AGT_CODE=ba.agentcode)-- Modified condition to get FA Agent data also-Gopi-11112019
    --and (ip.wri_agt_code=ba.agentcode or ip.FAOB_AGT_CODE=ba.agentcode or (tf.payor_code = st.GENERICATTRIBUTE12 and st.GENERICATTRIBUTE10 = ba.agentcode)) --Modified condition to get Forts Agent data also--version 6 by Amanda
/*    and (ip.wri_agt_code=ba.agentcode or ip.FAOB_AGT_CODE=ba.agentcode or (tf.payor_code = st.GENERICATTRIBUTE12 and st.GENERICATTRIBUTE10 = ba.agentcode)
          or (tf.payor_code = st.GENERICATTRIBUTE12 and st.GENERICATTRIBUTE26 = ba.agentcode)) */--version 10 comment
   inner join CS_CREDIT cr
      on tf.creditseq = cr.creditseq and cr.processingUnitseq=V_PROCESSINGUNITSEQ
   --for writing Agency postion info
   inner join cs_position pos_agy
        on pos_agy.name = 'SGY' || ip.wri_agy_code
        AND pos_agy.removedate = DT_REMOVEDATE
        AND pos_agy.effectivestartdate <= cr.genericdate2
        AND pos_agy.effectiveenddate   >  cr.genericdate2
     --for writing Agency participant info
     inner join cs_participant par_agy
        on par_agy.PAYEESEQ = pos_agy.PAYEESEQ
        AND par_agy.effectivestartdate <= cr.genericdate2
        AND par_agy.effectiveenddate   >  cr.genericdate2
        AND par_agy.removedate = DT_REMOVEDATE
      --for writing District postion info
     inner join cs_position pos_dis
        on pos_dis.name= 'SGY' || pos_agy.genericattribute3
        AND pos_dis.effectivestartdate <= cr.genericdate2
        AND pos_dis.effectiveenddate   > cr.genericdate2
        AND pos_dis.removedate = DT_REMOVEDATE
     --for writing District participant info
     inner join cs_participant par_dis
        on par_dis.PAYEESEQ = pos_dis.PAYEESEQ
        AND par_dis.effectivestartdate <= cr.genericdate2
        AND par_dis.effectiveenddate  > cr.genericdate2
        AND par_dis.removedate = DT_REMOVEDATE
     --for writing Agent postion info
      inner join cs_position pos_agt
        on 'SGT'||ip.wri_agt_code=pos_agt.name
        and pos_agt.effectivestartdate <= cr.genericdate2
        AND pos_agt.effectiveenddate   > cr.genericdate2
        and pos_agt.removedate = DT_REMOVEDATE
--        and POS_AGT.GENERICATTRIBUTE6='AFA'
     --for writing Agent participant info
     inner join cs_participant par_agt
     on par_agt.payeeseq= pos_agt.PAYEESEQ
     AND par_agt.effectivestartdate <= cr.genericdate2
     AND par_agt.effectiveenddate  > cr.genericdate2
     AND par_agt.removedate = DT_REMOVEDATE
     --for payor agent title info
     inner join cs_title title_agt
     on title_agt.RULEELEMENTOWNERSEQ = pos_agt.TITLESEQ
     AND title_agt.effectivestartdate <= cr.genericdate2
     AND title_agt.effectiveenddate   > cr.genericdate2
     AND title_agt.REMOVEDATE = DT_REMOVEDATE
   where tf.clawback_name in (STR_COMMISSION, STR_GST_COMMISSION)
   and tf.batch_number = P_BATCH_NO
           --     and st.tenantid = 'AIAS'
           --  and cr.tenantid = 'AIAS'
             and pos_agy.tenantid  = 'AIAS'
             and pos_dis.tenantid  = 'AIAS'
             and pos_agt.tenantid  = 'AIAS'
             and par_agt.tenantid  = 'AIAS'
          and title_agt.tenantid  = 'AIAS'
          and  par_agy.tenantid  = 'AIAS'
             and par_dis.tenantid  = 'AIAS'
             and tf.BUNAME=STR_BUNAME_FA
             and IP.BUNAME=STR_BUNAME_FA

   ;

Log('insert into AIA_CB_CLAWBACK_COMMISSION_FA' || '; row count: ' || to_char(sql%rowcount));

commit;

/**
the below logic is to check the clawback policy has the negative SVI value in current measurement quarter.
if yes, need to trace the same policy's clawback value of last quarter,
  if figure < 0, continue
  else if figure > 0, set current month clawback value = 0
end
**/

--get clawback type and clawback name, only LUMPSUM case will apply this logic
V_CB_TYPE := fn_get_cb_type(P_BATCH_NO);
--V_CB_NAME := fn_get_cb_name(P_BATCH_NO);
V_CB_QTR := fn_get_cb_quarter(P_BATCH_NO);


if V_CB_TYPE = STR_LUMPSUM then
   --get previous quarter batch number
    --V_BATCH_NO_PRE_QTR := fn_get_batch_no_pre_qtr(P_BATCH_NO);

insert into AIA_CB_CLAWBACK_SVI_TMP
select curr_cc.*, P_BATCH_NO from
(select wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name,
  sum(clawback) as clawback
  from
AIA_CB_CLAWBACK_COMMISSION
where clawback_type = STR_LUMPSUM
 and clawback_name = STR_COMMISSION
 and batch_no = P_BATCH_NO
group by wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name
  having sum(clawback) > 0
) curr_cc
left join
(select cc.wri_dist_code,
       cc.wri_agy_code,
       cc.wri_agt_code,
       cc.ponumber,
       cc.life_number,
       cc.coverage_number,
       cc.rider_number,
       cc.component_code,
       cc.product_name,
       --processed_clawback value should be updated after pipeline compeleted
       sum(cc.processed_clawback) as processed_clawback
  from AIA_CB_CLAWBACK_COMMISSION cc
 inner join (select nvl(max(t.batchnum), 0) as batch_no
               from aia_cb_batch_status t
               inner join (select distinct quarter, year, cb_startdate, cb_enddate
               from aia_cb_period
              where cb_name = STR_COMMISSION
              and BUNAME=STR_BUNAME_FA
              ) cbp
              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
              where t.islatest = 'Y'
              and t.BUNAME=STR_BUNAME_FA
                and t.status = STR_STATUS_COMPLETED_SH
                and t.clawbackname = STR_COMMISSION
                and t.clawbacktype = STR_LUMPSUM
                and t.cb_quarter_name <> V_CB_QTR
                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype) pre_batch
 on cc.batch_no = pre_batch.batch_no
 where cc.clawback_type = STR_LUMPSUM
   and cc.clawback_name = STR_COMMISSION
 group by cc.wri_dist_code,
          cc.wri_agy_code,
          cc.wri_agt_code,
          cc.ponumber,
          cc.life_number,
          cc.coverage_number,
          cc.rider_number,
          cc.component_code,
          cc.product_name
having sum(cc.processed_clawback) < 0) pre_cc
 on curr_cc.wri_dist_code = pre_cc.wri_dist_code
 and curr_cc.wri_agy_code = pre_cc.wri_agy_code
 and curr_cc.wri_agt_code = pre_cc.wri_agt_code
 and curr_cc.ponumber = pre_cc.ponumber
 and curr_cc.life_number = pre_cc.life_number
 and curr_cc.coverage_number = pre_cc.coverage_number
 and curr_cc.rider_number = pre_cc.rider_number
 and curr_cc.component_code = pre_cc.component_code
 and curr_cc.product_name = pre_cc.product_name
 where pre_cc.ponumber is null;

Log('insert into AIA_CB_CLAWBACK_SVI_TMP for FA' || '; row count: ' || to_char(sql%rowcount));

commit;

elsif V_CB_TYPE = STR_ONGOING then

insert into AIA_CB_CLAWBACK_SVI_TMP
select curr_cc.*, P_BATCH_NO from
(select wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name,
  sum(clawback) as clawback
  from
AIA_CB_CLAWBACK_COMMISSION
where clawback_type = STR_ONGOING
 and clawback_name = STR_COMMISSION
 and batch_no = P_BATCH_NO
group by wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name
  having sum(clawback) > 0
) curr_cc
left join
(select cc.wri_dist_code,
       cc.wri_agy_code,
       cc.wri_agt_code,
       cc.ponumber,
       cc.life_number,
       cc.coverage_number,
       cc.rider_number,
       cc.component_code,
       cc.product_name,
       --processed_clawback value should be updated after pipeline compeleted
       sum(cc.processed_clawback) as processed_clawback
  from AIA_CB_CLAWBACK_COMMISSION cc
 inner join (
 --lumpsum batch number
 select nvl(max(t.batchnum), 0) as batch_no
               from aia_cb_batch_status t
               inner join (select distinct quarter, year, cb_startdate, cb_enddate
               from aia_cb_period
              where cb_name = STR_COMMISSION
              and BUNAME=STR_BUNAME_FA
              ) cbp
              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
              where t.islatest = 'Y'
              and T.BUNAME=STR_BUNAME_FA
                and t.status = STR_STATUS_COMPLETED_SH
                and t.clawbackname = STR_COMMISSION
                and t.clawbacktype = STR_LUMPSUM
                and t.cb_quarter_name <> V_CB_QTR
                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype
              union
  --on-going batch number
              select nvl(max(t.batchnum), 0) as batch_no
               from aia_cb_batch_status t
              where t.islatest = 'Y'
              and T.BUNAME=STR_BUNAME_FA
                and t.status = STR_STATUS_COMPLETED_SH --'completed_sh'
                and t.clawbackname = STR_COMMISSION--'COMMISSION'
                and t.clawbacktype = STR_ONGOING --'ONGOING'
                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) > t.cycledate
              ) pre_batch
 on cc.batch_no = pre_batch.batch_no
 where cc.clawback_name = STR_COMMISSION
 group by cc.wri_dist_code,
          cc.wri_agy_code,
          cc.wri_agt_code,
          cc.ponumber,
          cc.life_number,
          cc.coverage_number,
          cc.rider_number,
          cc.component_code,
          cc.product_name
having sum(cc.processed_clawback) < 0) pre_cc
 on curr_cc.wri_dist_code = pre_cc.wri_dist_code
 and curr_cc.wri_agy_code = pre_cc.wri_agy_code
 and curr_cc.wri_agt_code = pre_cc.wri_agt_code
 and curr_cc.ponumber = pre_cc.ponumber
 and curr_cc.life_number = pre_cc.life_number
 and curr_cc.coverage_number = pre_cc.coverage_number
 and curr_cc.rider_number = pre_cc.rider_number
 and curr_cc.component_code = pre_cc.component_code
 and curr_cc.product_name = pre_cc.product_name
 where pre_cc.ponumber is null;

Log('insert into AIA_CB_CLAWBACK_SVI_TMP for FA' || '; row count: ' || to_char(sql%rowcount));

commit;

end if;

--update the table AIA_CB_CLAWBACK_COMMISSION for special handling for positive clawback
merge into AIA_CB_CLAWBACK_COMMISSION cc
using AIA_CB_CLAWBACK_SVI_TMP st
on (cc.wri_dist_code = st.wri_dist_code
 and cc.wri_agy_code = st.wri_agy_code
 and cc.wri_agt_code = st.wri_agt_code
 and cc.ponumber = st.ponumber
 and cc.life_number = st.life_number
 and cc.coverage_number = st.coverage_number
 and cc.rider_number = st.rider_number
 and cc.component_code = st.component_code
 and cc.product_name = st.product_name
 and cc.batch_no = st.batch_no
 and cc.batch_no = P_BATCH_NO
)
when matched then update set cc.clawback = 0;

Log('merge into AIA_CB_CLAWBACK_COMMISSION_FA' || '; row count: ' || to_char(sql%rowcount));

commit;

Log('SP_CLAWBACK_COMMISSION_FA end');

end SP_CLAWBACK_COMMISSION_FA;

PROCEDURE SP_EXEC_COMMISSION_ONGOING_FA(P_STR_CB_CYCLEDATE IN VARCHAR2)
  is
  V_STR_CB_TYPE    VARCHAR2(20);
  V_BATCH_NO       NUMBER;
  V_WEEKEND_FLAG   NUMBER;
  V_MONTHEND_FLAG  NUMBER;
  V_MESSAGE        VARCHAR2(2000);
  begin

  init;
LOG('Start');
  ---to define the run type
  SELECT COUNT(1) INTO V_WEEKEND_FLAG FROM IN_ETL_CONTROL CTL
  WHERE CTL.TXT_KEY_STRING='PAYMENT_END_DATE_WEEKLY' AND CTL.TXT_FILE_NAME= STR_PU AND CTL.TXT_KEY_VALUE=P_STR_CB_CYCLEDATE;

  SELECT COUNT(1) INTO V_MONTHEND_FLAG
  FROM CS_PERIOD CSP
  where CSP.ENDDATE - 1 = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE)
  and CSP.CALENDARSEQ = V_CALENDARSEQ and CSP.PERIODTYPESEQ=(select periodtypeseq from  cs_periodtype where name = STR_CALENDAR_TYPE)
  and CSP.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
  ;
Log(V_WEEKEND_FLAG+V_MONTHEND_FLAG || ' Flag');
  IF V_WEEKEND_FLAG+V_MONTHEND_FLAG>0
  THEN
  Log(V_WEEKEND_FLAG+V_MONTHEND_FLAG || ' Flag');
         --ONGOING
         sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_ONGOING,STR_COMMISSION);
         V_BATCH_NO := fn_get_batch_no_fa(P_STR_CB_CYCLEDATE, STR_COMMISSION,STR_ONGOING, STR_STATUS_START);
         sp_revert_by_batch(V_BATCH_NO);
         Log(V_BATCH_NO || ' Bacth No');
         SP_TRACE_FORWARD_COMMISSION_FA (P_STR_CB_CYCLEDATE,STR_ONGOING, V_BATCH_NO);
         SP_CLAWBACK_COMMISSION_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO);
         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) ;
  END IF;
---catch exception
        EXCEPTION WHEN OTHERS
        THEN    v_message := SUBSTR(SQLERRM,1,2000);
                Log(v_message);
                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
  END SP_EXEC_COMMISSION_ONGOING_FA;

PROCEDURE SP_EXEC_COMP_LUMPSUM_FA(P_STR_CB_CYCLEDATE IN VARCHAR2)
  is
  V_LUMPSUM_FLAG   NUMBER;
  V_BATCH_NO       NUMBER;
  v_message        VARCHAR2(2000);
  V_CB_YEAR        VARCHAR2(20);
  V_CB_QUARTER     VARCHAR2(20);
  begin

  init;

    SELECT COUNT(1) INTO V_LUMPSUM_FLAG FROM AIA_CB_PERIOD WHERE CB_CYCLEDATE = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE) and cb_name=STR_COMPENSATION AND buname = STR_BUNAME_FA;
    LOG('V_LUMPSUM_FLAG'||V_LUMPSUM_FLAG);
    IF V_LUMPSUM_FLAG >0 THEN
      --LUMPSUM
         sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_LUMPSUM,STR_COMPENSATION);
         V_BATCH_NO := fn_get_batch_no_fa(P_STR_CB_CYCLEDATE, STR_COMPENSATION, STR_LUMPSUM, STR_STATUS_START);
LOG('V_BATCH_NO'||V_BATCH_NO);
         SP_TRACE_FORWARD_COMP_FA (P_STR_CB_CYCLEDATE,STR_LUMPSUM, V_BATCH_NO);
         SP_CLAWBACK_COMP_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO);
         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP) ;
     ELSE Log(P_STR_CB_CYCLEDATE || ' is not the avaiable clawback cycle date for FA');
    END IF;
        ---catch exception
        EXCEPTION WHEN OTHERS
        THEN    v_message := SUBSTR(SQLERRM,1,2000);
                Log(v_message);
                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
END SP_EXEC_COMP_LUMPSUM_FA;

PROCEDURE SP_TRACE_FORWARD_COMP_FA(P_STR_CYCLEDATE IN VARCHAR2, P_STR_TYPE IN VARCHAR2, P_BATCH_NO IN INTEGER) AS
STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION';
V_CAL_PERIOD VARCHAR2(30); --measurement quarter
DT_CB_START_DATE DATE;
DT_CB_END_DATE DATE;
DT_INCEPTION_START_DATE DATE;
DT_INCEPTION_END_DATE DATE;
NUM_OF_CYCLE_IND integer;
RECORD_CNT_ONGOING integer;
ts_periodseq integer;
V_NADOR_RATE NUMBER(10,2);
V_NLPI_RATE NUMBER(10,2);
-- define period seq of each month
TYPE periodseq_type IS TABLE OF cs_period.periodseq%TYPE;
t_periodseq periodseq_type;
ONGOING_ST_DT DATE;
ONGOING_END_DT DATE;
ONGOING_PERIOD VARCHAR2(50);

begin

init;

--update status
sp_update_batch_status (P_BATCH_NO,'processing');

Log('SP_TRACE_FORWARD_COMP_FA start');

--Get the periodseq for Ongoing period
if P_STR_TYPE = STR_ONGOING then

select count(1)
into RECORD_CNT_ONGOING
from cs_period csp
 inner join cs_periodtype pt
    on csp.periodtypeseq = pt.periodtypeseq
 where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and pt.name = STR_CALENDAR_TYPE;

    if RECORD_CNT_ONGOING = 0 then
   goto ProcDone;
   END IF;

select csp.periodseq,
csp.startdate,
csp.enddate-1,
csp.name
into ts_periodseq,
ONGOING_ST_DT,
ONGOING_END_DT,
ONGOING_PERIOD
from cs_period csp
 inner join cs_periodtype pt
    on csp.periodtypeseq = pt.periodtypeseq
 where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and pt.name = STR_CALENDAR_TYPE;

Log('FA DT_ONGOING_START_DATE = ' || ONGOING_ST_DT);
Log('FA DT_ONGOING_END_DATE = ' || ONGOING_END_DT);

--delete from AIA_CB_TRACE_FORWARD_COMP where CLAWBACK_TYPE= P_STR_TYPE and  transaction_date between ONGOING_ST_DT and ONGOING_END_DT;
   --and CLAWBACK_NAME not in (STR_COMMISSION, STR_GST_COMMISSION);

--commit;

else

select count(1)
  into NUM_OF_CYCLE_IND
  from AIA_CB_PERIOD cbp
 where cbp.CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_CB_NAME AND cbp.buname = STR_BUNAME_FA; -- add cb_name here

   if NUM_OF_CYCLE_IND = 0 then
   goto ProcDone;
   END IF;

 --get calculation period name, clawback start date and end date for lumpsum compensation
select cbp.quarter || ' ' || cbp.year,
       cbp.cb_startdate,
       cbp.cb_enddate,
       cbp.inception_startdate,
       cbp.inception_enddate
  into V_CAL_PERIOD,
       DT_CB_START_DATE,
       DT_CB_END_DATE,
       DT_INCEPTION_START_DATE,
       DT_INCEPTION_END_DATE
  from AIA_CB_PERIOD cbp
 where CB_CYCLEDATE = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and cb_name=STR_CB_NAME AND cbp.buname = STR_BUNAME_FA;

Log('FA DT_LUMPSUM_START_DATE = ' || DT_CB_START_DATE);
Log('FA DT_LUMPSUM_END_DATE = ' || DT_CB_END_DATE);




-- Get the periodseqs for lumpsum period
 select
periodseq BULK COLLECT into t_periodseq
from cs_period csp
 inner join cs_periodtype pt
    on csp.periodtypeseq = pt.periodtypeseq
 where csp.startdate >= DT_CB_START_DATE
and csp.enddate <= DT_CB_END_DATE + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and pt.name = STR_CALENDAR_TYPE;


execute immediate 'truncate table aia_tmp_comls_period';
insert into aia_tmp_comls_period
select periodseq
from cs_period csp
 inner join cs_periodtype pt
    on csp.periodtypeseq = pt.periodtypeseq
 where csp.startdate >= DT_CB_START_DATE
and csp.enddate <= DT_CB_END_DATE + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and pt.name = STR_CALENDAR_TYPE;
commit;
--delete from AIA_CB_TRACE_FORWARD_COMP where CLAWBACK_TYPE= P_STR_TYPE and CALCULATION_PERIOD=V_CAL_PERIOD; --and CLAWBACK_NAME not in (STR_COMMISSION, STR_GST_COMMISSION);
--commit;

--Version 2 add by Amanda to get quarter end period begin
execute immediate 'truncate table AIA_TMP_COMLS_PERIOD_SPI';

--get 6 lumpsum months for SPI
insert into AIA_TMP_COMLS_PERIOD_SPI
select
  csp.periodseq,
  case when csp_qtr.name is not null then extract (year from csp.startdate ) || '0' || substr(csp_qtr.name,2,1)
       else null
  end,csp.parentseq,'',0
from cs_period csp
 left join cs_period csp_qtr on csp_qtr.periodtypeseq = V_periodtype_quarter_seq
   and csp_qtr.enddate = csp.enddate
   and csp_qtr.removedate = DT_REMOVEDATE
where csp.startdate >= DT_CB_START_DATE
and csp.enddate <= add_months(DT_CB_END_DATE,-2) + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and csp.periodtypeseq = V_periodtype_month_seq;
commit;

--update quarter end month periodseq for traceforward, can't delete!
merge into AIA_TMP_COMLS_PERIOD_SPI tmp1
using( select periodseq,year_qtr,qtr_periodseq,qtr_end_periodseq,row_number() over(order by year_qtr asc) as qtr_order
      from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null
)tmp
on ( tmp1.qtr_periodseq = tmp.qtr_periodseq )
when matched then update
    set tmp1.qtr_end_periodseq = tmp.periodseq,tmp1.qtr_order= tmp.qtr_order;
commit;

select qtr_end_periodseq into V_period_seq2 from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null and qtr_order = 2;
--Version 2 end
end if;

   select value into V_NADOR_RATE
from CS_FIXEDVALUE fv where
name='FV_NADOR_Payout_Rate'
and Removedate = DT_REMOVEDATE;

 select value into V_NLPI_RATE
from CS_FIXEDVALUE fv where
name='FV_NLPI_RATE'
and Removedate = DT_REMOVEDATE;

if P_STR_TYPE = STR_LUMPSUM  then

Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for FYO,RYO,FSM_RYO,NLPI' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);

--for i in 1..t_periodseq.count loop

Log('AIA_CB_TRACE_FORWARD_COMP  for FA'|| ' '||V_CAL_PERIOD);
--for lumpsum compensation trace forward for 'FYO','RYO','FSM_RYO','NLPI'



 execute immediate 'truncate table aia_tmp_Comls_Step0_1';
 insert into  aia_tmp_Comls_Step0_1

 select /*+ leading(ip,st) */ st.salestransactionseq,  ip.wri_agt_code as wri_agt_code_ORIG,
  ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
        ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
       st.genericnumber2      as COMMISSION_RATE,
       st.genericdate4        as PAID_TO_DATE
       ,'SGT'||ip.wri_agt_code wri_agt_code
       ,ip.quarter || ' ' || ip.year qtrYr
,''
,''
 from cs_Salestransaction st
 INNER JOIN AIA_CB_IDENTIFY_POLICY IP
ON 1                              =1
AND IP.BUNAME                     =STR_BUNAME_FA
AND ST.PONUMBER                   = IP.PONUMBER
AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER
and st.productid=ip.component_CODE
 where st.tenantid='AIAS'
and st.processingUnitseq=V_PROCESSINGUNITSEQ
and st.eventtypeseq <> 16607023625933358
--and st.compensationdate between '1-mar-2017' and '31-may-2017'
--and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE

--AND ST.PRODUCTID                  = IP.COMPONENT_CODE
;

commit;

--Add for AI transaction NL20180308

 execute immediate 'truncate table aia_tmp_Comls_Step0_AMR';

--version 11 define column name
insert into aia_tmp_Comls_Step0_AMR
(    RN
   ,QUARTER
   ,COMP_MONTH
   ,COMP_YEAR
   ,COMP_PERIOD
   ,PAYEE_DISTRICT_UNIT
   ,PAYEE_CURRENT_UNIT
   ,PAYEE_CODE
   ,PAYEE_NAME
   ,AGENCY_NAME
   ,PAYEE_CURRENT_CLASS_CODE
   ,PAYEE_RANK
   ,CONTRACT_DATE
   ,CONTRACT_QUARTER
   ,TERMINATIONDATE
   ,PONUMBER
   ,POLICY_SUBMISSION_DATE
   ,RISK_COMMENCEMENT_DATE
   ,POLICY_INCEPTION_DATE
   ,COMPENSATIONDATE
   ,CASE_COUNT
   ,FYC
   ,TOTAL_CASE_COUNT
   ,CURR_QTR_VALIDATION_MET
   ,POL_PREV_QTR_VALIDATION
   ,AI_PAID
   ,AI_CLAWBACK
   ,AI_PAYMENT
   ,FORTS
   ,DEPOSIT_VALUE
   ,AI_RATE
   ,YTD
   ,HELD_DEPOSIT_VALUE
   ,COMPONENT_CODE
   ,FREELOOK_INCEPTED_CURR_QTR
   ,OLD_AGENT_CD
   ,FA_ON_BRIDGING_FLG
   ,NEW_AGENT_CD
   ,OLD_DISTRICT_CODE
   ,OLD_UNIT_CODE
   ,NEW_HIRE_DATE
   ,TRANSFERED_AGENT
)
select row_number() over(partition by t1.PONUMBER, t1.AI_PAYMENT, t1.COMPENSATIONDATE, t1.PAYEE_CODE, t1.POLICY_INCEPTION_DATE order by t1.component_CODE) as rn
   ,t1.QUARTER
   ,t1.COMP_MONTH
   ,t1.COMP_YEAR
   ,t1.COMP_PERIOD
   ,t1.PAYEE_DISTRICT_UNIT
   ,t1.PAYEE_CURRENT_UNIT
   ,t1.PAYEE_CODE
   ,t1.PAYEE_NAME
   ,t1.AGENCY_NAME
   ,t1.PAYEE_CURRENT_CLASS_CODE
   ,t1.PAYEE_RANK
   ,t1.CONTRACT_DATE
   ,t1.CONTRACT_QUARTER
   ,t1.TERMINATIONDATE
   ,t1.PONUMBER
   ,t1.POLICY_SUBMISSION_DATE
   ,t1.RISK_COMMENCEMENT_DATE
   ,t1.POLICY_INCEPTION_DATE
   ,t1.COMPENSATIONDATE
   ,t1.CASE_COUNT
   ,t1.FYC
   ,t1.TOTAL_CASE_COUNT
   ,t1.CURR_QTR_VALIDATION_MET
   ,t1.POL_PREV_QTR_VALIDATION
   ,t1.AI_PAID
   ,t1.AI_CLAWBACK
   ,t1.AI_PAYMENT
   ,t1.FORTS
   ,t1.DEPOSIT_VALUE
   ,t1.AI_RATE
   ,t1.YTD
   ,t1.HELD_DEPOSIT_VALUE
   ,t1.COMPONENT_CODE
   ,t1.FREELOOK_INCEPTED_CURR_QTR
   ,t1.OLD_AGENT_CD
   ,t1.FA_ON_BRIDGING_FLG
   ,t1.NEW_AGENT_CD
   ,t1.OLD_DISTRICT_CODE
   ,t1.OLD_UNIT_CODE
   ,t1.NEW_HIRE_DATE
   ,t1.TRANSFERED_AGENT
  from AI_MONTHLY_REPORT t1
 where t1.AI_PAYMENT <> 0;

commit;

 execute immediate 'truncate table aia_tmp_Comls_Step0_TXN';

 insert into aia_tmp_Comls_Step0_TXN
   select row_number() over(partition by t2.PONUMBER, t2.VALUE, t2.ACCOUNTINGDATE, t2.GENERICATTRIBUTE11, t2.GENERICDATE2 order by t2.PRODUCTID) as rn,
          t2.salestransactionseq,
          t2.PONUMBER,
          t2.VALUE,
          t2.ACCOUNTINGDATE,
          t2.GENERICATTRIBUTE11,
          t2.GENERICDATE2,
          t2.genericnumber2,
          t2.genericdate4
     from cs_Salestransaction t2
    where t2.tenantid = 'AIAS'
      and t2.processingUnitseq = V_PROCESSINGUNITSEQ
      and t2.eventtypeseq = 16607023625933358;

 commit;

 insert into  aia_tmp_Comls_Step0_1
/*with AMR as (select row_number() over(partition by t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE ,t1.POLICY_INCEPTION_DATE order by t1.component_CODE) as rn,
             t1.* from AI_MONTHLY_REPORT t1 where t1.AI_PAYMENT<> 0),
     st as (select row_number() over(partition by t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11 ,t2.GENERICDATE2 order by t2.PRODUCTID) as rn,
            t2.* from cs_Salestransaction t2,cs_businessunit bu  where t2.tenantid='AIAS' and T2.BUSINESSUNITMAP=BU.MASK
--            and BU.NAME=STR_BUNAME_FA  --Changes done to fix not getting AGY AI records --Gopi-04072019
            and t2.processingUnitseq=V_PROCESSINGUNITSEQ and t2.eventtypeseq = 16607023625933358 ),*/
with     IP as (select row_number() over(partition by t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date order by t3.coverage_number ) as rn,
            t3.* from AIA_CB_IDENTIFY_POLICY t3 where t3.BUNAME  = STR_BUNAME_FA)
 select /*+ PARALLEL */ st.salestransactionseq,  ip.wri_agt_code as wri_agt_code_ORIG,
-- (case when AMR.OLD_AGENT_CD IS NULL THEN ip.wri_agt_code ELSE AMR.OLD_AGENT_CD END) as wri_agt_code_ORIG, --Changes done to fix not getting AGY AI records --Gopi-04072019
  ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
        ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
       st.genericnumber2      as COMMISSION_RATE,
       st.genericdate4        as PAID_TO_DATE
       ,'SGT'||ip.wri_agt_code wri_agt_code
--  ,'SGT'||(case when AMR.OLD_AGENT_CD IS NULL THEN ip.wri_agt_code ELSE AMR.OLD_AGENT_CD END)  wri_agt_code --Changes done to fix not getting AGY AI records --Gopi-04072019
       ,ip.quarter || ' ' || ip.year qtrYr
       ,AMR.OLD_AGENT_CD
       ,AMR.NEW_AGENT_CD
 from aia_tmp_Comls_Step0_TXN st
 INNER JOIN  aia_tmp_Comls_Step0_AMR AMR
ON  st.PONUMBER = AMR.PONUMBER
AND st.VALUE = AMR.AI_PAYMENT
AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
AND (st.GENERICATTRIBUTE11 = AMR.NEW_AGENT_CD OR st.GENERICATTRIBUTE11=AMR.OLD_AGENT_CD) ----Changes done to fix not getting AGY AI records --Gopi-04072019
AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
--AND st.rn = AMR.rn
 INNER JOIN IP
ON 1                              =1
AND IP.BUNAME                     = STR_BUNAME_FA
AND AMR.PONUMBER                   = IP.PONUMBER
and (AMR.NEW_AGENT_CD = IP.WRI_AGT_CODE OR AMR.OLD_AGENT_CD=IP.WRI_AGT_CODE) --Changes done to fix not getting AGY AI records --Gopi-04072019
and AMR.component_CODE=ip.component_CODE
and AMR.policy_inception_date = ip.inception_date
and AMR.risk_commencement_date = ip.risk_commencement_date
and AMR.rn = IP.rn
;

Log('insert 0_1 done for FA '||SQL%ROWCOUNT);
commit;

execute immediate 'TRUNCATE table aia_tmp_comls_step1_1';
insert into aia_tmp_comls_step1_1
select /*+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ ) */  crd.creditseq,
       crd.salestransactionseq ,
        ip.CALCULATION_PERIOD,
      ip.POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
        ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
        crd.compensationdate as TRANSACTION_DATE,
         crd.genericattribute12 as PAYOR_CODE,
         ct.credittypeid        as CREDITTYPE,
       crd.name               as CREDIT_NAME,
       crd.value              as CREDIT_VALUE,
        crd.periodseq          as PERIODSEQ,
         crd.genericattribute2  as PRODUCT_NAME,
       crd.genericnumber1     as POLICY_YEAR,
       ip.COMMISSION_RATE,
       ip.PAID_TO_DATE
       ,ip.wri_agt_code
       ,ip.qtrYr, crd.genericdate2
   ,crd.genericattribute13  ,crd.genericattribute14, crd.positionseq, crd.ruleseq,
   ip.old_agent_cd,
   ip.new_agent_cd
  from cs_Credit crd
  join aia_tmp_comls_period p
  on crd.periodseq=p.periodseq
  inner join CS_CREDITTYPE ct
    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
   and ct.Removedate >sysdate
  inner join aia_tmp_comls_step0_1 ip
    on 1=1
    and ip.salestransactionseq = crd.salestransactionseq
     and (crd.genericattribute12 = ip.wri_agt_code_orig or crd.genericattribute12 = ip.old_agent_cd or crd.genericattribute12 = ip.new_agent_cd)
   and ip.CALCULATION_PERIOD = V_CAL_PERIOD
   --where crd.genericattribute16 not in ('RO', 'RNO')
   where crd.tenantid = 'AIAS'
   and crd.processingunitseq = V_PROCESSINGUNITSEQ

;



Log('insert 1_1 done for FA'||SQL%ROWCOUNT);

--delete from AIA_TMP_COMLS_STEP1_1 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;


--Log('delete 1_1 done for FA'||SQL%ROWCOUNT);
commit;

DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP1_1"',
          estimate_percent => 1
          );


execute immediate 'truncate table aia_tmp_comls_step2_1';
insert into aia_tmp_comls_step2_1

select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , null as clawback_name
from cs_measurement m
join aia_tmp_comls_period p
  on m.periodseq=p.periodseq
   inner join (select distinct SOURCE_RULE_OUTPUT
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
              --added by suresh
              --add AI NL20180308
                AND CLAWBACK_NAME  IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION'
                --verstion 13 Harm_Phase4 Start
                ,'FA_FYO_2.1','FA_RYO_2.1','FA_FYO_ONG_2.1','FA_RYO_ONG_2.1','FA_AI_2.1'
                --verstion 13 Harm_Phase4 End
                ))pmr  --Added as part of UnitTesting-Gopi
                --end by Suresh
    on pmr.SOURCE_RULE_OUTPUT = m.name

  where  m.processingunitseq = V_PROCESSINGUNITSEQ
  and m.tenantid='AIAS'
   ;

DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP2_1"',
          estimate_percent => 1
          );


Log('insert 2_1 done for FA'||SQL%ROWCOUNT);
commit;

--  execute immediate 'truncate table aia_tmp_comls_step3_1';
delete from aia_tmp_comls_step3_1; --Update by Amanda here for the issue object on longer exists
commit;

  insert into aia_tmp_comls_step3_1
   select   pct.creditseq pctCreditSeq,
   pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq
   , s1.CREDITSEQ
,SALESTRANSACTIONSEQ
,CALCULATION_PERIOD
,POLICY_NUMBER
,POLICYIDSEQ
,LIFE_NUMBER
,COVERAGE_NUMBER
,RIDER_NUMBER
,COMPONENT_CODE
,COMPONENT_NAME
,BASE_RIDER_IND
,TRANSACTION_DATE
,PAYOR_CODE
,CREDITTYPE
,CREDIT_NAME
,CREDIT_VALUE
,s1.PERIODSEQ
,PRODUCT_NAME
,POLICY_YEAR
,COMMISSION_RATE
,PAID_TO_DATE

   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
   ,WRI_AGT_CODE
,QTRYR
,GD2
,         pct.contributionvalue  as CONTRIBUTIONVALUE , s1.genericattribute13
 ,s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq,s1.old_agent_cd,s1.new_agent_cd
   from cs_pmcredittrace pct
   join aia_tmp_comls_step1_1 s1
   on pct.creditseq=s1.creditseq
   join aia_tmp_comls_step2_1 s2
   on s2.measurementseq=pct.measurementseq and s2.ruleseq=pct.ruleseq
   --and pct.targetperiodseq=s2.periodseq

   inner join  CS_PMSELFTRACE pmslf
   on  s2.measurementseq = pmslf.sourcemeasurementseq
    --     and pmslf.targetperiodseq=s2.periodseq
inner join CS_INCENTIVEPMTRACE inpm
   on pmslf.TARGETMEASUREMENTSEQ = inpm.MEASUREMENTSEQ
   --and pmslf.targetperiodseq=s2.periodseq

inner join (select * from cs_depositincentivetrace union select * from aias_depositincentivetrace) dct
   on inpm.incentiveseq = dct.incentiveseq
   --and dct.targetperiodseq=s2.periodseq

   and dct.targetperiodseq=s2.periodseq
   and dct.tenantid='AIAS' and pct.tenantid='AIAS' and pmslf.tenantid='AIAS'  and inpm.tenantid='AIAS'
   and dct.processingUnitseq=V_PROCESSINGUNITSEQ and pct.processingUnitseq=V_PROCESSINGUNITSEQ
   and pmslf.processingUnitseq=V_PROCESSINGUNITSEQ and inpm.processingUnitseq=V_PROCESSINGUNITSEQ

;
--add AI NL20180308
 insert into aia_tmp_comls_step3_1
   select   pct.creditseq pctCreditSeq,
   pct.measurementseq, pct.contributionvalue PctContribValue, dct.depositseq
   , s1.CREDITSEQ
,SALESTRANSACTIONSEQ
,CALCULATION_PERIOD
,POLICY_NUMBER
,POLICYIDSEQ
,LIFE_NUMBER
,COVERAGE_NUMBER
,RIDER_NUMBER
,COMPONENT_CODE
,COMPONENT_NAME
,BASE_RIDER_IND
,TRANSACTION_DATE
,PAYOR_CODE
,CREDITTYPE
,CREDIT_NAME
,CREDIT_VALUE
,s1.PERIODSEQ
,PRODUCT_NAME
,POLICY_YEAR
,COMMISSION_RATE
,PAID_TO_DATE

   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
   ,WRI_AGT_CODE
,QTRYR
,GD2
,         pct.contributionvalue  as CONTRIBUTIONVALUE , s1.genericattribute13
 ,s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
 ,''
 --verstion 13 start comment the old_agent_cd, otherwise would produce duplicated data for AI
 --,s1.old_agent_cd
 --verstion 13 end
 ,s1.new_agent_cd
   from cs_pmcredittrace pct
   join aia_tmp_comls_step1_1 s1
   on pct.creditseq=s1.creditseq
   join aia_tmp_comls_step2_1 s2
   on s2.measurementseq=pct.measurementseq and s2.ruleseq=pct.ruleseq
   --and pct.targetperiodseq=s2.periodseq
    --     and pmslf.targetperiodseq=s2.periodseq
inner join CS_INCENTIVEPMTRACE inpm
   on pct.measurementseq = inpm.MEASUREMENTSEQ
   --and pmslf.targetperiodseq=s2.periodseq

inner join /*CS_DEPOSITINCENTIVETRACE*/ (select * from cs_depositincentivetrace union select * from aias_depositincentivetrace) dct
   on inpm.incentiveseq = dct.incentiveseq
   --and dct.targetperiodseq=s2.periodseq

   and dct.targetperiodseq=s2.periodseq
   and dct.tenantid='AIAS' and pct.tenantid='AIAS'  and inpm.tenantid='AIAS'
   and dct.processingUnitseq=V_PROCESSINGUNITSEQ and pct.processingUnitseq=V_PROCESSINGUNITSEQ
   and inpm.processingUnitseq=V_PROCESSINGUNITSEQ

;

Log('insert 3 done for FA'||SQL%ROWCOUNT);
commit;
DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP3_1"',
          estimate_percent => 1
          );



insert into AIA_CB_TRACE_FORWARD_COMP
select STR_BUNAME_FA as BUNAME,
       QtrYr as CALCULATION_PERIOD,
       POLICY_NUMBER,
        POLICYIDSEQ,
       mPositionSeq PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
     PAYOR_CODE,
     LIFE_NUMBER,
      COVERAGE_NUMBER,
       RIDER_NUMBER,
        COMPONENT_CODE,
         COMPONENT_NAME,
         BASE_RIDER_IND,
         TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       STR_LUMPSUM as CLAWBACK_TYPE,
          rl.CLAWBACK_NAME,
        STR_CB_NAME as CLAWBACK_METHOD,
        CREDITTYPE,
      CREDITSEQ,
        CREDIT_NAME,
        CREDIT_VALUE,
       crd_positionseq as crd_positionseq,
       GD2 as crd_genericdate2,
       crd_ruleseq as crd_ruleseq,
       measurementseq      as PM_SEQ,
       mname                as PM_NAME,
        case rl.CLAWBACK_NAME
       when 'NLPI' then x.contributionvalue*V_NLPI_RATE
       else
       x.contributionvalue
       end as PM_CONTRIBUTION_VALUE,
       case rl.CLAWBACK_NAME
         when 'FYO' then fyo_rate.value
         when 'NEW_FYO' then new_fyo_rate.value
         when 'RYO' then ryo_rate.value
         when 'NEW_RYO' then new_ryo_rate.value
         when 'FSM_RYO' then ryo_rate.value
         when 'NLPI' then V_NLPI_RATE
       else 1
         end as PM_RATE,
       dep.depositseq         as DEPOSITSEQ,
       /*dep.name*/ replace(dep.name,'_MANUAL','')            as DEPOSIT_NAME,
       dep.value              as DEPOSIT_VALUE,
       x.periodseq          as PERIODSEQ,
       x.salestransactionseq as SALESTRANSACTIONSEQ,
        PRODUCT_NAME,
         POLICY_YEAR,
         COMMISSION_RATE,
         PAID_TO_DATE,
       P_BATCH_NO             as BATCH_NUMBER,
       sysdate                as CREATED_DATE,
       x.old_agent_cd,
       x.new_agent_cd
       ,null as deposit_period --Version 13 add by Amanda
 from aia_tmp_comls_step3_1 x
       join cs_deposit dep
       on dep.depositseq=x.depositseq
       join cs_position dep_pos
       on dep.positionseq = dep_pos.ruleelementownerseq
   and dep_pos.removedate = DT_REMOVEDATE
   and dep_pos.effectivestartdate <= x.GD2
   and dep_pos.effectiveenddate > x.GD2
       --and dep_pos.name = x.wri_agt_code
        inner join cs_title dep_title
 on dep_pos.titleseq = dep_title.ruleelementownerseq
 and dep_title.removedate = DT_REMOVEDATE
 and dep_title.effectivestartdate <= GD2
 and dep_title.effectiveenddate > GD2
        inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'DR'
--Changed by suresh
--Add for AI NL20180308
             AND CLAWBACK_NAME  IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION'
       --verstion 13 start
       ,'FA_FYO_2.1','FA_FYO_ONG_2.1','FA_RYO_2.1','FA_RYO_ONG_2.1','FA_AI_2.1')) rl
       --verstion 13 end
--end by suresh
    on /*dep.NAME*/ replace(dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT
 left join vw_lt_fyo_rate fyo_rate
 on fyo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
   and fyo_rate.Receiver_title = dep_title.name
   and rl.CLAWBACK_NAME = 'FYO_FA'
 --for lookup PM rate for RYO
 left join vw_lt_ryo_life_rate ryo_rate
 on ryo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
 and ryo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
 and ryo_rate.Receiver_title = dep_title.name
 and rl.CLAWBACK_NAME in ( 'RYO_FA','FSM_RYO_FA')
 --Added by Suresh
  --for lookup PM rate for New FYO
 left join vw_lt_new_fyo_rate new_fyo_rate
 on new_fyo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
 and new_fyo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
 and new_fyo_rate.Receiver_title = dep_title.name
 and rl.CLAWBACK_NAME = 'NEW_FYO_FA'
  --for lookup PM rate for New RYO
 left join VW_LT_NEW_RYO_LIFE_RATE new_ryo_rate
 on new_ryo_rate.Contributor_Leader_title = x.genericattribute13 --payor agency leader title
   and new_ryo_rate.PIB_TYPE = fn_fyo_pib_type(x.genericattribute13, x.genericattribute14, x.credit_name)
   and new_ryo_rate.Receiver_title = dep_title.name
   and rl.CLAWBACK_NAME = 'NEW_RYO_FA'
--end by Suresh
 --  where x.qtrYr = V_CAL_PERIOD
   ;







/*



insert into AIA_CB_TRACE_FORWARD_COMP
select STR_BUNAME as BUNAME,
       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
       pm.positionseq as PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
       crd.genericattribute12 as PAYOR_CODE,
       ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
       crd.compensationdate as TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       STR_LUMPSUM as CLAWBACK_TYPE,
        rl.CLAWBACK_NAME       as CLAWBACK_NAME,
        STR_CB_NAME as CLAWBACK_METHOD,
       ct.credittypeid        as CREDIT_TYPE,
       crd.creditseq          as CREDITSEQ,
       crd.name               as CREDIT_NAME,
       crd.value              as CREDIT_VALUE,
       crd.positionseq as crd_positionseq,
       st.genericdate2 as crd_genericdate2,
       crd.ruleseq as crd_ruleseq,
       pm.measurementseq      as PM_SEQ,
       pm.name                as PM_NAME,
       case rl.CLAWBACK_NAME
       when 'NLPI' then pct.contributionvalue*V_NLPI_RATE
       else
       pct.contributionvalue
       end as PM_CONTRIBUTION_VALUE,
       case rl.CLAWBACK_NAME
         when 'FYO' then fyo_rate.value
         when 'RYO' then ryo_rate.value
         when 'FSM_RYO' then ryo_rate.value
         when 'NLPI' then V_NLPI_RATE
       else 1
         end as PM_RATE,
       dep.depositseq         as DEPOSITSEQ,
       dep.name               as DEPOSIT_NAME,
       dep.value              as DEPOSIT_VALUE,
       crd.periodseq          as PERIODSEQ,
       st.salestransactionseq as SALESTRANSACTIONSEQ,
       crd.genericattribute2  as PRODUCT_NAME,
       crd.genericnumber1     as POLICY_YEAR,
       st.genericnumber2      as COMMISSION_RATE,
       st.genericdate4        as PAID_TO_DATE,
       P_BATCH_NO             as BATCH_NUMBER,
       sysdate                as CREATED_DATE
  FROM CS_SALESTRANSACTION st
 inner join CS_CREDIT crd
    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
    and crd.tenantid=st.tenantid and crd.processingunitseq=st.processingunitseq
--and crd.genericdate2
 inner join CS_PMCREDITTRACE pct
    on crd.CREDITSEQ = pct.CREDITSEQ
    and pct.tenantid=crd.tenantid and pct.processingunitseq=crd.processingunitseq
 inner join CS_MEASUREMENT pm
    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
    and pct.tenantid=pm.tenantid and pct.processingunitseq=pm.processingunitseq
 inner join (select distinct SOURCE_RULE_OUTPUT
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME  IN ('FYO','RYO','FSM_RYO','NLPI'))pmr
    on pmr.SOURCE_RULE_OUTPUT = pm.name
 inner join  CS_PMSELFTRACE pmslf
   on  pm.measurementseq = pmslf.sourcemeasurementseq
   and pm.tenantid=pmslf.tenantid and pm.processingunitseq=pmslf.processingunitseq
inner join CS_INCENTIVEPMTRACE inpm
   on pmslf.TARGETMEASUREMENTSEQ = inpm.MEASUREMENTSEQ
   and inpm.tenantid=pmslf.tenantid and inpm.processingunitseq=pmslf.processingunitseq
inner join CS_DEPOSITINCENTIVETRACE depin
   on inpm.incentiveseq = depin.incentiveseq
   and inpm.tenantid=depin.tenantid and inpm.processingunitseq=depin.processingunitseq
 inner join cs_deposit dep
    on depin.depositseq = dep.depositseq
    and dep.tenantid=depin.tenantid and dep.processingunitseq=depin.processingunitseq
 inner join cs_position dep_pos
    on dep.positionseq = dep_pos.ruleelementownerseq
    and dep_pos.tenantid='AIAS'
   and dep_pos.removedate = DT_REMOVEDATE
   and dep_pos.effectivestartdate <= crd.genericdate2
   and dep_pos.effectiveenddate > crd.genericdate2
 inner join CS_CREDITTYPE ct
    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
   and ct.Removedate = DT_REMOVEDATE
 inner join AIA_CB_IDENTIFY_POLICY ip
    on st.PONUMBER = ip.PONUMBER
   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
   AND st.PRODUCTID = ip.COMPONENT_CODE
   and crd.genericattribute12 = ip.wri_agt_code
    and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
 --for lookup the compensation output name
 inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'DR'
                AND CLAWBACK_NAME  IN ('FYO','RYO','FSM_RYO','NLPI')) rl
    on dep.NAME = rl.SOURCE_RULE_OUTPUT
 --for lookup the receiver info.
 inner join cs_title dep_title
 on dep_pos.titleseq = dep_title.ruleelementownerseq
 and dep_title.removedate = DT_REMOVEDATE
 and dep_title.effectivestartdate <= crd.genericdate2
 and dep_title.effectiveenddate > crd.genericdate2
 --for lookup PM rate for FYO
 left join vw_lt_fyo_rate fyo_rate
 on fyo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
   and fyo_rate.Receiver_title = dep_title.name
   and rl.CLAWBACK_NAME = 'FYO'
 --for lookup PM rate for RYO
 left join vw_lt_ryo_life_rate ryo_rate
 on ryo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
 and ryo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
 and ryo_rate.Receiver_title = dep_title.name
 and rl.CLAWBACK_NAME in ( 'RYO','FSM_RYO')
 WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ and st.tenantid='AIAS'
   AND st.BUSINESSUNITMAP = 1;
   --and dep.periodseq =  t_periodseq(i);
   --and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
 /*  and greatest(nvl(st.GENERICDATE3, to_date('19000101', 'yyyymmdd')),
                nvl(st.GENERICDATE2, to_date('19000101', 'yyyymmdd')),
                nvl(st.GENERICDATE5, to_date('19000101', 'yyyymmdd'))) between
                --to_date('20150801','yyyymmdd') and to_date('20160531','yyyymmdd');
       DT_INCEPTION_START_DATE and DT_INCEPTION_END_DATE; */
 --  and crd.genericattribute16 not in ('RO', 'RNO')


 Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for FYO,RYO,FSM_RYO,NLPI' || '; row count: ' || to_char(sql%rowcount));

--end loop;



commit;


Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for NADOR FA' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);

--for lumpsum compensation trace forward for NADOR
--for i in 1..t_periodseq.count
--loop
Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for NADOR FA using periodseq V_CAL_PERIOD of ' || V_CAL_PERIOD);


 execute immediate 'truncate table aia_tmp_Comls_Step0_2';
 insert into  aia_tmp_Comls_Step0_2

select /*+ leading(ip,st) */ st.salestransactionseq,
--ip.wri_agt_code as wri_agt_code_ORIG,
--version 13 start
 nvl(st.genericattribute12,ip.wri_agt_code) as wri_agt_code_ORIG, --commission agent code
--version 13 end
 ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
        ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
       st.genericnumber2      as COMMISSION_RATE,
       st.genericdate4        as PAID_TO_DATE
       ,'SGT'||ip.wri_agt_code wri_agt_code
       ,ip.quarter || ' ' || ip.year qtrYr
 from cs_Salestransaction st
 INNER JOIN AIA_CB_IDENTIFY_POLICY IP
ON 1                              =1
AND IP.BUNAME                     = STR_BUNAME_FA
AND ST.PONUMBER                   = IP.PONUMBER
AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER
and st.productid=ip.component_CODE
 where st.tenantid='AIAS'
and st.processingUnitseq=V_PROCESSINGUNITSEQ
--and st.compensationdate between '1-mar-2017' and '31-may-2017'
--and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE

--AND ST.PRODUCTID                  = IP.COMPONENT_CODE
;

Log('insert 0_2 done for FA'||SQL%ROWCOUNT);


commit;

execute immediate 'truncate table aia_tmp_comls_step1_2';
insert into aia_tmp_comls_step1_2

select /*+ leading(ip,crd) index(crd CS_CREDIT_TRANSACTIONSEQ ) */ crd.creditseq,
       crd.salestransactionseq ,
        ip.CALCULATION_PERIOD,
       ip.POLICY_NUMBER as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
        ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
        crd.compensationdate as TRANSACTION_DATE,
         crd.genericattribute12 as PAYOR_CODE,
         ct.credittypeid        as CREDITTYPE,
       crd.name               as CREDIT_NAME,
       crd.value              as CREDIT_VALUE,
        crd.periodseq          as PERIODSEQ,
         crd.genericattribute2  as PRODUCT_NAME,
       crd.genericnumber1     as POLICY_YEAR,
       ip.COMMISSION_RATE,
       ip.PAID_TO_DATE
       ,ip.wri_agt_code wri_agt_code
       ,ip.qtrYr, crd.genericdate2
   ,crd.genericattribute13  ,crd.genericattribute14, crd.positionseq, crd.ruleseq
  from cs_Credit crd
  join aia_tmp_comls_period p
  on crd.periodseq=p.periodseq
  join cs_Salestransaction st
  on st.salestransactionseq=crd.salestransactionseq
  and st.tenantid='AIAS' and st.processingunitseq=crd.processingunitseq
 -- and st.compensationdate between DT_CB_START_DATE and DT_CB_END_DATE
   inner join CS_CREDITTYPE ct
    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
   and ct.Removedate >sysdate
  inner join aia_tmp_comls_step0_2 ip
    on 1=1
    and ip.salestransactionseq = crd.salestransactionseq
      and crd.genericattribute12 = ip.wri_agt_code_ORIG
  and  ip.CALCULATION_PERIOD = V_CAL_PERIOD
  inner join cs_businessunit  bu on st.businessunitmap = bu.mask
   where crd.tenantid = 'AIAS'
   and crd.processingunitseq = V_PROCESSINGUNITSEQ
-- and bu.name = STR_BUNAME_FA
;





Log('insert 1_2 done for FA '||SQL%ROWCOUNT);

--delete from AIA_TMP_COMLS_STEP1_2 where transaction_Date <DT_CB_START_DATE or transaction_Date>DT_CB_END_DATE;


--Log('delete 1_2 done for FA '||SQL%ROWCOUNT);
commit;

DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP1_2"',
          estimate_percent => 1
          );


execute immediate 'truncate table aia_tmp_comls_step2_2';
insert into aia_tmp_comls_step2_2

select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , null as clawback_name
from cs_measurement m
join aia_tmp_comls_period p
  on m.periodseq=p.periodseq
  inner join (select distinct SOURCE_RULE_OUTPUT
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME ='NADOR')pmr
    on pmr.SOURCE_RULE_OUTPUT = m.name

  where  m.processingunitseq = V_PROCESSINGUNITSEQ
  and m.tenantid='AIAS'
   ;



Log('insert 2_2 done for FA '||SQL%ROWCOUNT);
commit;


DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP2_2"',
          estimate_percent => 1
          );

  execute immediate 'truncate table aia_tmp_comls_step3_2';
  insert into aia_tmp_comls_step3_2
   select   pct.creditseq pctCreditSeq,
   pct.measurementseq, pct.contributionvalue PctContribValue, depin.depositseq
   , s1.CREDITSEQ
,SALESTRANSACTIONSEQ
,CALCULATION_PERIOD
,POLICY_NUMBER
,POLICYIDSEQ
,LIFE_NUMBER
,COVERAGE_NUMBER
,RIDER_NUMBER
,COMPONENT_CODE
,COMPONENT_NAME
,BASE_RIDER_IND
,TRANSACTION_DATE
,PAYOR_CODE
,CREDITTYPE
,CREDIT_NAME
,CREDIT_VALUE
,s1.PERIODSEQ
,PRODUCT_NAME
,POLICY_YEAR
,COMMISSION_RATE
,PAID_TO_DATE

   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
   ,WRI_AGT_CODE
,QTRYR
,GD2
,         pct.contributionvalue  as CONTRIBUTIONVALUE , s1.genericattribute13
 ,s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
   from cs_pmcredittrace pct
   join aia_tmp_comls_step1_2 s1
   on pct.creditseq=s1.creditseq
   join aia_tmp_comls_step2_2 s2
   on s2.measurementseq=pct.measurementseq --and s2.ruleseq=pct.ruleseq
   --and pct.targetperiodseq=s2.periodseq
   inner join CS_INCENTIVEPMTRACE inpm
    on s2.MEASUREMENTSEQ = inpm.MEASUREMENTSEQ and inpm.tenantid='AIAS' and inpm.processingunitseq=v_processingunitseq
     --and inpm.targetperiodseq=s2.periodseq
 inner join  /*CS_DEPOSITINCENTIVETRACE*/ (select * from cs_depositincentivetrace union select * from aias_depositincentivetrace) depin
    on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid
    --and depin.targetperiodseq=s2.periodseq
 where depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ   and depin.tenantid='AIAS'
;

Log('insert 3_2 part a done for FA'||SQL%ROWCOUNT);
commit;
DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP3_2"',
          estimate_percent => 1
          );

--verstion 13 start
insert into aia_tmp_comls_step3_2
select   pct.creditseq pctCreditSeq,
   pct.measurementseq, pct.contributionvalue PctContribValue, depin.depositseq
   , s1.CREDITSEQ
,SALESTRANSACTIONSEQ
,CALCULATION_PERIOD
,POLICY_NUMBER
,POLICYIDSEQ
,LIFE_NUMBER
,COVERAGE_NUMBER
,RIDER_NUMBER
,COMPONENT_CODE
,COMPONENT_NAME
,BASE_RIDER_IND
,TRANSACTION_DATE
,PAYOR_CODE
,CREDITTYPE
,CREDIT_NAME
,CREDIT_VALUE
,s1.PERIODSEQ
,PRODUCT_NAME
,POLICY_YEAR
,COMMISSION_RATE
,PAID_TO_DATE

   , s2.name mname, s2.periodseq mPeriodSeq, s2.payeeseq mPayeeSeq,
   s2.ruleseq mruleSeq, s2.positionseq mPositionSeq , s2.clawback_name
   ,WRI_AGT_CODE
,QTRYR
,GD2
,         pct.contributionvalue  as CONTRIBUTIONVALUE , s1.genericattribute13
 ,s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
   from cs_pmcredittrace pct
   join aia_tmp_comls_step1_2 s1
   on pct.creditseq=s1.creditseq
   join aia_tmp_comls_step2_2 s2
   on s2.measurementseq=pct.measurementseq
   inner join CS_PMSELFTRACE  CPT
   ON s2.measurementseq= CPT.sourcemeasurementseq
   inner join CS_INCENTIVEPMTRACE inpm
    on CPT.TARGETMEASUREMENTSEQ= inpm.MEASUREMENTSEQ and inpm.tenantid='AIAS' and inpm.processingunitseq=v_processingunitseq
 inner join  (select * from cs_depositincentivetrace union select * from aias_depositincentivetrace) depin
    on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid
 where depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ   and depin.tenantid='AIAS';

Log('insert 3_2 part b for NADOR 2.1 On-Bridge done for FA'||SQL%ROWCOUNT);
commit;
DBMS_STATS.GATHER_TABLE_STATS (
              ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP3_2"',
          estimate_percent => 1
          );
-- verstion 13 end

insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
select STR_BUNAME_FA as BUNAME,
       QtrYr as CALCULATION_PERIOD,
       POLICY_NUMBER,
        POLICYIDSEQ,
       mPositionSeq PAYEE_SEQ,
              substr(dep_pos.name, 4) as PAYEE_CODE,
       PAYOR_CODE,
     LIFE_NUMBER,
      COVERAGE_NUMBER,
       RIDER_NUMBER,
        COMPONENT_CODE,
         COMPONENT_NAME,
         BASE_RIDER_IND,
         TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       STR_LUMPSUM as CLAWBACK_TYPE,
        rl.CLAWBACK_NAME       as CLAWBACK_NAME,
         STR_CB_NAME as CLAWBACK_METHOD,
     CREDITTYPE,
      CREDITSEQ,
        CREDIT_NAME,
        CREDIT_VALUE,
       crd_positionseq as crd_positionseq,
       GD2 as crd_genericdate2,
       crd_ruleseq as crd_ruleseq,
       measurementseq      as PM_SEQ,
       mname                as PM_NAME,
       x.contributionvalue*V_NADOR_RATE  as PM_CONTRIBUTION_VALUE,
       V_NADOR_RATE           as PM_RATE,
       dep.depositseq         as DEPOSITSEQ,
       /*dep.name*/replace(dep.name,'_MANUAL','')              as DEPOSIT_NAME,
       dep.value              as DEPOSIT_VALUE,
       x.periodseq          as PERIODSEQ,
        x.salestransactionseq as SALESTRANSACTIONSEQ,
        PRODUCT_NAME,
         POLICY_YEAR,
         COMMISSION_RATE,
         PAID_TO_DATE,
       P_BATCH_NO             as BATCH_NUMBER,
       sysdate                as CREATED_DATE,'',''
       ,null as deposit_period
  FROM
 aia_tmp_comls_step3_2 x
       join cs_deposit dep
       on dep.depositseq=x.depositseq
       join cs_position dep_pos
       on dep.positionseq = dep_pos.ruleelementownerseq
   and dep_pos.removedate = DT_REMOVEDATE
   and dep_pos.effectivestartdate <= x.GD2
   and dep_pos.effectiveenddate > x.GD2

 inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'DR'
                AND CLAWBACK_NAME  IN ('NADOR_FA_2.1')) rl
    on /*dep.NAME*/ replace(Dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT
 WHERE dep.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ

   --and dep.periodseq =  x.periodseq 170807
    ;





--insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
/*select STR_BUNAME as BUNAME,
       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
       pm.positionseq as PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
       crd.genericattribute12 as PAYOR_CODE,
       ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
       crd.compensationdate as TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       STR_LUMPSUM as CLAWBACK_TYPE,
        rl.CLAWBACK_NAME       as CLAWBACK_NAME,
         STR_CB_NAME as CLAWBACK_METHOD,
       ct.credittypeid        as CREDIT_TYPE,
       crd.creditseq          as CREDITSEQ,
       crd.name               as CREDIT_NAME,
       crd.value              as CREDIT_VALUE,
        crd.positionseq as crd_positionseq,
       st.genericdate2 as crd_genericdate2,
       crd.ruleseq as crd_ruleseq,
       pm.measurementseq      as PM_SEQ,
       pm.name                as PM_NAME,
       pct.contributionvalue*V_NADOR_RATE  as PM_CONTRIBUTION_VALUE,
       V_NADOR_RATE           as PM_RATE,
       dep.depositseq         as DEPOSITSEQ,
       dep.name               as DEPOSIT_NAME,
       dep.value              as DEPOSIT_VALUE,
       crd.periodseq          as PERIODSEQ,
       st.salestransactionseq as SALESTRANSACTIONSEQ,
       crd.genericattribute2  as PRODUCT_NAME,
       crd.genericnumber1     as POLICY_YEAR,
       st.genericnumber2      as COMMISSION_RATE,
       st.genericdate4        as PAID_TO_DATE,
       P_BATCH_NO             as BATCH_NUMBER,
       sysdate                as CREATED_DATE
  FROM CS_SALESTRANSACTION st
 inner join CS_CREDIT crd
    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ and st.tenantid=crd.tenantid and st.processingunitseq=pm.processingunitseq
 inner join CS_PMCREDITTRACE pct
    on crd.CREDITSEQ = pct.CREDITSEQ and pct.tenantid=crd.tenantid and crd.processingunitseq=pm.processingunitseq
    and pct.sourceperiodseq=crd.periodseq
 inner join CS_MEASUREMENT pm
    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ and pct.tenantid=pm.tenantid and pct.processingunitseq=pm.processingunitseq
    and pm.periodseq=pct.targetperiodseq
 inner join (select distinct SOURCE_RULE_OUTPUT
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME ='NADOR')pmr
    on pmr.SOURCE_RULE_OUTPUT = pm.name
 inner join CS_INCENTIVEPMTRACE inpm
    on pm.MEASUREMENTSEQ = inpm.MEASUREMENTSEQ and inpm.tenantid=pm.tenantid and inpm.processingunitseq=pm.processingunitseq
 inner join CS_DEPOSITINCENTIVETRACE depin
    on inpm.incentiveseq = depin.incentiveseq and inpm.tenantid=depin.tenantid and inpm.processingunitseq=depin.processingunitseq
 inner join cs_deposit dep
    on depin.depositseq = dep.depositseq and dep.tenantid=depin.tenantid and dep.processingunitseq=depin.processingunitseq
 inner join cs_position dep_pos
    on dep.positionseq = dep_pos.ruleelementownerseq
   and dep_pos.removedate = DT_REMOVEDATE
   and dep_pos.effectivestartdate <= crd.genericdate2
   and dep_pos.effectiveenddate > crd.genericdate2
 inner join CS_CREDITTYPE ct
    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
   and ct.Removedate = DT_REMOVEDATE
 inner join AIA_CB_IDENTIFY_POLICY ip
    on st.PONUMBER = ip.PONUMBER
   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
   AND st.PRODUCTID = ip.COMPONENT_CODE
   and crd.genericattribute12 = ip.wri_agt_code
   and ip.quarter || ' ' || ip.year = V_CAL_PERIOD
 inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'DR'
                AND CLAWBACK_NAME  IN ('NADOR')) rl
    on dep.NAME = rl.SOURCE_RULE_OUTPUT
 WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   AND st.BUSINESSUNITMAP = 1
   and dep.periodseq =  t_periodseq(i)
   and st.tenantid='AIAS' and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   and crd.tenantid='AIAS' and crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   and pct.tenantid='AIAS' and pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   and pm.tenantid='AIAS' and pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   and inpm.tenantid='AIAS' and inpm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   and depin.tenantid='AIAS' and depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   and dep.tenantid='AIAS' and dep.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
   and dep_pos.tenantid='AIAS' and dep_pos.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ;

--   and crd.genericattribute16 not in ('RO', 'RNO');


commit;
--end loop;
*/
Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for NADOR' || '; row count: ' || to_char(sql%rowcount));
commit;

--Version 2 add by Amanda for BSC SPI clawback begin
execute immediate 'truncate table aia_tmp_comls_step2_2';

select year_qtr into V_First_QTR from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null and qtr_order = 1;
select year_qtr into V_Second_QTR from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null and qtr_order = 2;

insert into aia_tmp_comls_step2_2
select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , null as clawback_name
from cs_measurement m
join AIA_TMP_COMLS_PERIOD_SPI p
  on m.periodseq=p.periodseq
  inner join (select distinct SOURCE_RULE_OUTPUT
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME ='SPI_FA')pmr
    on pmr.SOURCE_RULE_OUTPUT = m.name
  where  m.processingunitseq = V_PROCESSINGUNITSEQ
  and m.value <> 0
  and m.tenantid='AIAS';

Log('insert 2_2 done for SPI FA '||SQL%ROWCOUNT);
commit;

execute immediate 'truncate table aia_tmp_comls_step3_3';

insert into aia_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
                        POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
                        BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, PERIODSEQ,
                        PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, MNAME, MPERIODSEQ,
                        MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME, WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13,
                        GENERICATTRIBUTE14, CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ )
select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
       POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
       BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, s1.PERIODSEQ,
       PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, s2.name mname, sm2.periodseq mPeriodSeq,--s2.periodseq mPeriodSeq,
       s2.payeeseq mPayeeSeq, s2.ruleseq mruleSeq, s2.positionseq mPositionSeq, s2.clawback_name,
       WRI_AGT_CODE, QTRYR,GD2, pct.contributionvalue as CONTRIBUTIONVALUE,--sm1.value as CONTRIBUTIONVALUE,
       s1.genericattribute13, s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
/*       depin.depositseq deposit_seq,
       sm4.genericnumber1 SPI_RATE --SPI_RATE */
       ,0, sm2.measurementseq
from cs_pmcredittrace pct
inner join aia_tmp_comls_step1_2 s1
  on pct.creditseq=s1.creditseq
inner join AIA_TMP_COMLS_PERIOD_SPI p_spi --only get 6 months period for SPI
   on s1.periodseq = p_spi.periodseq
inner join aia_tmp_comls_step2_2 s2
  on pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
--for SM level 1 (SM_PIB_SG_SPI_SGPAFA)
inner join CS_PMSELFTRACE pm_sm1
  on s2.measurementseq = pm_sm1.sourcemeasurementseq
inner join CS_MEASUREMENT sm1
  on sm1.measurementseq = pm_sm1.targetmeasurementseq
  and sm1.name = 'SM_PIB_SG_SPI_SGPAFA'
--for SM level 2 (SM_PIB_YTD_SG_SPI_SGPAFA)
inner join CS_MEASUREMENT sm2
  on sm1.payeeseq = sm2.payeeseq
  and sm1.positionseq = sm2.positionseq
  and sm2.periodseq = p_spi.qtr_end_periodseq
  and sm2.name = 'SM_PIB_YTD_SG_SPI_SGPAFA'
;

if substr(V_First_QTR,1,4) = substr(V_Second_QTR,1,4) then --only contribute to second quarter when same year
  insert into aia_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
                        POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
                        BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, PERIODSEQ,
                        PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, MNAME, MPERIODSEQ,
                        MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME, WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13,
                        GENERICATTRIBUTE14, CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ )
  select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
         POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
         BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, s1.PERIODSEQ,
         PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, s2.name mname, sm2.periodseq mPeriodSeq,--s2.periodseq mPeriodSeq,
         s2.payeeseq mPayeeSeq, s2.ruleseq mruleSeq, s2.positionseq mPositionSeq, s2.clawback_name,
         WRI_AGT_CODE, QTRYR,GD2, pct.contributionvalue as CONTRIBUTIONVALUE,--sm1.value as CONTRIBUTIONVALUE,
         s1.genericattribute13, s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
         ,0, sm2.measurementseq
  from cs_pmcredittrace pct
  inner join aia_tmp_comls_step1_2 s1
    on pct.creditseq=s1.creditseq
  inner join AIA_TMP_COMLS_PERIOD_SPI p_spi --only get 6 months period for SPI
     on s1.periodseq = p_spi.periodseq
  inner join aia_tmp_comls_step2_2 s2
    on pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
  --for SM level 1 (SM_PIB_SG_SPI_SGPAFA)
  inner join CS_PMSELFTRACE pm_sm1
    on s2.measurementseq = pm_sm1.sourcemeasurementseq
  inner join CS_MEASUREMENT sm1
    on sm1.measurementseq = pm_sm1.targetmeasurementseq
    and sm1.name = 'SM_PIB_SG_SPI_SGPAFA'
  --for SM level 2 (SM_PIB_YTD_SG_SPI_SGPAFA)
  inner join CS_MEASUREMENT sm2
    on sm1.payeeseq = sm2.payeeseq
    and sm1.positionseq = sm2.positionseq
    and sm2.periodseq = V_period_seq2
    and sm2.name = 'SM_PIB_YTD_SG_SPI_SGPAFA'
    where p_spi.qtr_order = 1;

end if;

Log('insert 3_3 done for SPI FA'||SQL%ROWCOUNT);
commit;

merge into aia_tmp_comls_step3_3 temp
  using(select sm2_sm3.sourcemeasurementseq, depin.depositseq, sm4.genericnumber1
  from CS_PMSELFTRACE sm2_sm3
  --on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
  inner join CS_MEASUREMENT sm3
  on sm3.measurementseq = sm2_sm3.targetmeasurementseq
  and sm3.name = 'SM_SPI_CALCULATE_YTD_SGPAFA'
  --for SM level 4 (SM_SPI_PAYMENT_QTR_SGPAFA)
  inner join CS_PMSELFTRACE sm3_sm4
    on sm3.measurementseq = sm3_sm4.sourcemeasurementseq
  inner join CS_MEASUREMENT sm4
    on sm4.measurementseq = sm3_sm4.targetmeasurementseq
  --for Incentive (I_SPI_SG_SGPAFA)
  inner join CS_INCENTIVEPMTRACE inpm
    on sm4.measurementseq = inpm.MEASUREMENTSEQ
    and inpm.tenantid='AIAS' and inpm.processingunitseq=V_PROCESSINGUNITSEQ
  inner join cs_incentive inc
    on inpm.incentiveseq = inc.incentiveseq
  --for deposit (D_SPI_SG_SGPAFA)
  inner join CS_DEPOSITINCENTIVETRACE depin
    on  inpm.incentiveseq = depin.incentiveseq
    and depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    and depin.tenantid='AIAS'
           ) temp1
  on (  temp.YTD_MEASUREMENTSEQ = temp1.sourcemeasurementseq )
  when matched then update
  set temp.DEPOSITSEQ = temp1.depositseq,
      temp.SPI_RATE = temp1.genericnumber1;

Log('update 3_3 done for SPI FA'||SQL%ROWCOUNT);
commit;

DBMS_STATS.GATHER_TABLE_STATS (
          ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP3_3"',
          estimate_percent => 1);

insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
select STR_BUNAME_FA as BUNAME,
       QtrYr as CALCULATION_PERIOD,
       POLICY_NUMBER,
       POLICYIDSEQ,
       mPositionSeq PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
       PAYOR_CODE,
       LIFE_NUMBER,
       COVERAGE_NUMBER,
       RIDER_NUMBER,
       COMPONENT_CODE,
       COMPONENT_NAME,
       BASE_RIDER_IND,
       TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       STR_LUMPSUM as CLAWBACK_TYPE,
       nvl(rl.CLAWBACK_NAME,'SPI_FA')       as CLAWBACK_NAME,
       STR_CB_NAME as CLAWBACK_METHOD,
       CREDITTYPE,
       CREDITSEQ,
       CREDIT_NAME,
       CREDIT_VALUE,
       crd_positionseq as crd_positionseq,
       GD2 as crd_genericdate2,
       crd_ruleseq as crd_ruleseq,
       measurementseq      as PM_SEQ,
       mname               as PM_NAME,
       x.contributionvalue  as PM_CONTRIBUTION_VALUE,
       --x.contributionvalue*SPI_RATE  as PM_CONTRIBUTION_VALUE,
       SPI_RATE            as PM_RATE,
       x.depositseq      as DEPOSITSEQ,
       /*dep.name*/replace(dep.name,'_MANUAL','') as DEPOSIT_NAME,
       dep.value           as DEPOSIT_VALUE,
       x.periodseq         as PERIODSEQ,
       x.salestransactionseq as SALESTRANSACTIONSEQ,
       PRODUCT_NAME,
       POLICY_YEAR,
       COMMISSION_RATE,
       PAID_TO_DATE,
       P_BATCH_NO           as BATCH_NUMBER,
       sysdate              as CREATED_DATE,
       '','',
       x.MPERIODSEQ -- Quarter end periodseq
  FROM
 aia_tmp_comls_step3_3 x
  inner join cs_position dep_pos
       on  x.mPositionSeq = dep_pos.ruleelementownerseq
       and dep_pos.removedate = DT_REMOVEDATE
       and dep_pos.effectivestartdate <= x.GD2
       and dep_pos.effectiveenddate > x.GD2
  left join cs_deposit dep
       on dep.depositseq=x.depositseq
      and dep.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
  left join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'DR'
                AND CLAWBACK_NAME = 'SPI_FA') rl
    on /*dep.NAME*/ replace(Dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT;

Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI FA' || '; row count: ' || to_char(sql%rowcount));
commit;

--Version 2 end

--Version 17 add by Zero for BSC SPI FA 2.1 clawback begin
execute immediate 'truncate table aia_tmp_comls_step2_2';

--select year_qtr into V_First_QTR from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null and qtr_order = 1;
--select year_qtr into V_Second_QTR from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null and qtr_order = 2;

insert into aia_tmp_comls_step2_2
select measurementseq, m.name, m.periodseq, payeeseq, ruleseq, positionseq , null as clawback_name
from cs_measurement m
join AIA_TMP_COMLS_PERIOD_SPI p
  on m.periodseq=p.periodseq
  inner join (select distinct SOURCE_RULE_OUTPUT
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME ='SPI_FA_2.1')pmr -- version 14 Harm_BSC_SPI
    on pmr.SOURCE_RULE_OUTPUT = m.name
  where  m.processingunitseq = V_PROCESSINGUNITSEQ
  and m.value <> 0
  and m.tenantid='AIAS';

Log('insert 2_2 done for SPI FA 2.1 '||SQL%ROWCOUNT);
commit;

execute immediate 'truncate table aia_tmp_comls_step3_3';

insert into aia_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
                        POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
                        BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, PERIODSEQ,
                        PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, MNAME, MPERIODSEQ,
                        MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME, WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13,
                        GENERICATTRIBUTE14, CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ )
select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
       POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
       BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, s1.PERIODSEQ,
       PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, s2.name mname, sm2.periodseq mPeriodSeq,--s2.periodseq mPeriodSeq,
       s2.payeeseq mPayeeSeq, s2.ruleseq mruleSeq, s2.positionseq mPositionSeq, s2.clawback_name,
       WRI_AGT_CODE, QTRYR,GD2, pct.contributionvalue as CONTRIBUTIONVALUE,--sm1.value as CONTRIBUTIONVALUE,
       s1.genericattribute13, s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
/*       depin.depositseq deposit_seq,
       sm4.genericnumber1 SPI_RATE --SPI_RATE */
       ,0, sm2.measurementseq
from cs_pmcredittrace pct
inner join aia_tmp_comls_step1_2 s1
  on pct.creditseq=s1.creditseq
inner join AIA_TMP_COMLS_PERIOD_SPI p_spi --only get 6 months period for SPI
   on s1.periodseq = p_spi.periodseq
inner join aia_tmp_comls_step2_2 s2
  on pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
--for SM level 1 (SM_PIB_SG_SPI/SM_PIB_SG_SPI_FAOB)
inner join CS_PMSELFTRACE pm_sm1
  on s2.measurementseq = pm_sm1.sourcemeasurementseq
inner join CS_MEASUREMENT sm1
  on sm1.measurementseq = pm_sm1.targetmeasurementseq
  and sm1.name in ('SM_PIB_SG_SPI','SM_PIB_SG_SPI_FAOB')-- version 17 Harm_BSC_SPI
--for SM level 2 (SM_PIB_YTD_SG_SPI/SM_PIB_YTD_SG_SPI_FAOB)
inner join CS_MEASUREMENT sm2
  on sm1.payeeseq = sm2.payeeseq
  and sm1.positionseq = sm2.positionseq
  and sm2.periodseq = p_spi.qtr_end_periodseq
  and sm2.name in ('SM_PIB_YTD_SG_SPI','SM_PIB_YTD_SG_SPI_FAOB')-- version 17 Harm_BSC_SPI
;

if substr(V_First_QTR,1,4) = substr(V_Second_QTR,1,4) then --only contribute to second quarter when same year
  insert into aia_tmp_comls_step3_3(PCTCREDITSEQ, MEASUREMENTSEQ, PCTCONTRIBVALUE, CREDITSEQ, SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
                        POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
                        BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, PERIODSEQ,
                        PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, MNAME, MPERIODSEQ,
                        MPAYEESEQ, MRULESEQ, MPOSITIONSEQ, CLAWBACK_NAME, WRI_AGT_CODE, QTRYR, GD2, CONTRIBUTIONVALUE, GENERICATTRIBUTE13,
                        GENERICATTRIBUTE14, CRD_POSITIONSEQ, CRD_RULESEQ, SPI_RATE, YTD_MEASUREMENTSEQ )
  select pct.creditseq pctCreditSeq,pct.measurementseq, pct.contributionvalue PctContribValue, s1.CREDITSEQ,SALESTRANSACTIONSEQ, CALCULATION_PERIOD,
         POLICY_NUMBER, POLICYIDSEQ, LIFE_NUMBER, COVERAGE_NUMBER, RIDER_NUMBER, COMPONENT_CODE, COMPONENT_NAME,
         BASE_RIDER_IND, TRANSACTION_DATE, PAYOR_CODE, CREDITTYPE, CREDIT_NAME, CREDIT_VALUE, s1.PERIODSEQ,
         PRODUCT_NAME, POLICY_YEAR, COMMISSION_RATE, PAID_TO_DATE, s2.name mname, sm2.periodseq mPeriodSeq,--s2.periodseq mPeriodSeq,
         s2.payeeseq mPayeeSeq, s2.ruleseq mruleSeq, s2.positionseq mPositionSeq, s2.clawback_name,
         WRI_AGT_CODE, QTRYR,GD2, pct.contributionvalue as CONTRIBUTIONVALUE,--sm1.value as CONTRIBUTIONVALUE,
         s1.genericattribute13, s1.genericattribute14, s1.crd_positionseq, s1.crd_ruleseq
         ,0, sm2.measurementseq
  from cs_pmcredittrace pct
  inner join aia_tmp_comls_step1_2 s1
    on pct.creditseq=s1.creditseq
  inner join AIA_TMP_COMLS_PERIOD_SPI p_spi --only get 6 months period for SPI
     on s1.periodseq = p_spi.periodseq
  inner join aia_tmp_comls_step2_2 s2
    on pct.MEASUREMENTSEQ = s2.MEASUREMENTSEQ
  --for SM level 1 (SM_PIB_SG_SPI/SM_PIB_SG_SPI_FAOB)
  inner join CS_PMSELFTRACE pm_sm1
    on s2.measurementseq = pm_sm1.sourcemeasurementseq
  inner join CS_MEASUREMENT sm1
    on sm1.measurementseq = pm_sm1.targetmeasurementseq
    and sm1.name in ('SM_PIB_SG_SPI','SM_PIB_SG_SPI_FAOB') -- version 17 Harm_BSC_SPI
  --for SM level 2 (SM_PIB_YTD_SG_SPI/SM_PIB_YTD_SG_SPI_FAOB)
  inner join CS_MEASUREMENT sm2
    on sm1.payeeseq = sm2.payeeseq
    and sm1.positionseq = sm2.positionseq
    and sm2.periodseq = V_period_seq2
    and sm2.name in ('SM_PIB_YTD_SG_SPI','SM_PIB_YTD_SG_SPI_FAOB') -- version 17 Harm_BSC_SPI
    where p_spi.qtr_order = 1;

end if;

Log('insert 3_3 done for SPI FA 2.1'||SQL%ROWCOUNT);
commit;

-- old bize pay to old code/ new bize pay to new code
merge into aia_tmp_comls_step3_3 temp
  using(select sm2_sm3.sourcemeasurementseq, depin.depositseq, sm4.genericnumber1
  from CS_PMSELFTRACE sm2_sm3
  --on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
  inner join CS_MEASUREMENT sm3
  on sm3.measurementseq = sm2_sm3.targetmeasurementseq
  and sm3.name IN ('SM_SPI_CALCULATE_YTD_FAOB','SM_SPI_CALCULATE_YTD')-- version 17 Harm_BSC_SPI
  --for SM level 4 (SM_SPI_PAYMENT_QTR_FAOB/SM_SPI_PAYMENT_QTR)
  inner join CS_PMSELFTRACE sm3_sm4
    on sm3.measurementseq = sm3_sm4.sourcemeasurementseq
  inner join CS_MEASUREMENT sm4
    on sm4.measurementseq = sm3_sm4.targetmeasurementseq
  --for Incentive (I_SPI_SG_FAOB/I_SPI_SG)
  inner join CS_INCENTIVEPMTRACE inpm
    on sm4.measurementseq = inpm.MEASUREMENTSEQ
    and inpm.tenantid='AIAS' and inpm.processingunitseq=V_PROCESSINGUNITSEQ
  inner join cs_incentive inc
    on inpm.incentiveseq = inc.incentiveseq
  --for deposit (D_SPI_SG_FAOB/D_SPI_SG)
  inner join CS_DEPOSITINCENTIVETRACE depin
    on  inpm.incentiveseq = depin.incentiveseq
    and depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    and depin.tenantid='AIAS'
           ) temp1
  on (  temp.YTD_MEASUREMENTSEQ = temp1.sourcemeasurementseq )
  when matched then update
  set temp.DEPOSITSEQ = temp1.depositseq,
      temp.SPI_RATE = temp1.genericnumber1;

Log('update 3_3 done for SPI FA 2.1'||SQL%ROWCOUNT);
commit;
-- old bize pay to new code
merge into aia_tmp_comls_step3_3 temp
  using(select sm2_sm3.sourcemeasurementseq, depin.depositseq, sm5.genericnumber1
  from CS_PMSELFTRACE sm2_sm3
  --on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
  inner join CS_MEASUREMENT sm3
  on sm3.measurementseq = sm2_sm3.targetmeasurementseq
  and sm3.name = 'SM_PIB_YTD_SG_SPI_FAOB'
    --for SM level 4 (SM_SPI_CALCULATE_YTD_FAOB)
  inner join CS_PMSELFTRACE sm3_sm4
    on sm3.measurementseq = sm3_sm4.sourcemeasurementseq
  inner join CS_MEASUREMENT sm4
    on sm4.measurementseq = sm3_sm4.targetmeasurementseq
	and sm4.name = 'SM_SPI_CALCULATE_YTD_FAOB'
  --for SM level 5 (SM_SPI_PAYMENT_QTR_FAOB)
  inner join CS_PMSELFTRACE sm4_sm5
    on sm4.measurementseq = sm4_sm5.sourcemeasurementseq
  inner join CS_MEASUREMENT sm5
    on sm5.measurementseq = sm4_sm5.targetmeasurementseq
  --for Incentive (I_SPI_SG_FAOB)
  inner join CS_INCENTIVEPMTRACE inpm
    on sm5.measurementseq = inpm.MEASUREMENTSEQ
    and inpm.tenantid='AIAS' and inpm.processingunitseq=V_PROCESSINGUNITSEQ
  inner join cs_incentive inc
    on inpm.incentiveseq = inc.incentiveseq
  --for deposit (D_SPI_SG_FAOB)
  inner join CS_DEPOSITINCENTIVETRACE depin
    on  inpm.incentiveseq = depin.incentiveseq
    and depin.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    and depin.tenantid='AIAS'
           ) temp1
  on (  temp.YTD_MEASUREMENTSEQ = temp1.sourcemeasurementseq )
  when matched then update
  set temp.DEPOSITSEQ = temp1.depositseq,
      temp.SPI_RATE = temp1.genericnumber1;


Log('update 3_3 done for SPI FA 2.1'||SQL%ROWCOUNT);
commit;

DBMS_STATS.GATHER_TABLE_STATS (
          ownname => '"AIASEXT"',
          tabname => '"AIA_TMP_COMLS_STEP3_3"',
          estimate_percent => 1);

insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
select STR_BUNAME_FA as BUNAME,
       QtrYr as CALCULATION_PERIOD,
       POLICY_NUMBER,
       POLICYIDSEQ,
       mPositionSeq PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
       PAYOR_CODE,
       LIFE_NUMBER,
       COVERAGE_NUMBER,
       RIDER_NUMBER,
       COMPONENT_CODE,
       COMPONENT_NAME,
       BASE_RIDER_IND,
       TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE,'MON-YYYY') as PROCESSING_PERIOD,
       STR_LUMPSUM as CLAWBACK_TYPE,
       nvl(rl.CLAWBACK_NAME,'SPI_FA_2.1')       as CLAWBACK_NAME,-- version 17 Harm_BSC_SPI
       STR_CB_NAME as CLAWBACK_METHOD,
       CREDITTYPE,
       CREDITSEQ,
       CREDIT_NAME,
       CREDIT_VALUE,
       crd_positionseq as crd_positionseq,
       GD2 as crd_genericdate2,
       crd_ruleseq as crd_ruleseq,
       measurementseq      as PM_SEQ,
       mname               as PM_NAME,
       x.contributionvalue  as PM_CONTRIBUTION_VALUE,
       --x.contributionvalue*SPI_RATE  as PM_CONTRIBUTION_VALUE,
       SPI_RATE            as PM_RATE,
       x.depositseq      as DEPOSITSEQ,
       /*dep.name*/replace(dep.name,'_MANUAL','') as DEPOSIT_NAME,
       dep.value           as DEPOSIT_VALUE,
       x.periodseq         as PERIODSEQ,
       x.salestransactionseq as SALESTRANSACTIONSEQ,
       PRODUCT_NAME,
       POLICY_YEAR,
       COMMISSION_RATE,
       PAID_TO_DATE,
       P_BATCH_NO           as BATCH_NUMBER,
       sysdate              as CREATED_DATE,
       '','',
       x.MPERIODSEQ -- Quarter end periodseq
  FROM
  aia_tmp_comls_step3_3 x
  inner join cs_position dep_pos
       on  x.mPositionSeq = dep_pos.ruleelementownerseq
       and dep_pos.removedate = DT_REMOVEDATE
       and dep_pos.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
       and dep_pos.effectiveenddate > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
  left join cs_deposit dep
       on dep.depositseq=x.depositseq
      and dep.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
  left join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'DR'
                AND CLAWBACK_NAME = 'SPI_FA_2.1') rl
    on /*dep.NAME*/ replace(Dep.name,'_MANUAL','') = rl.SOURCE_RULE_OUTPUT;

Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI FA 2.1' || '; row count: ' || to_char(sql%rowcount));
commit;

-- update old agent code for old bize pay to new code
merge into AIA_CB_TRACE_FORWARD_COMP tf
using (SELECT distinct
        ip.WRI_AGT_CODE,
         ip.ponumber ,
         ip.COMPONENT_CODE ,
         ip.FAOB_AGT_CODE
       FROM AIA_CB_IDENTIFY_POLICY  ip
       where ip.FAOB_AGT_CODE is not null
       and ip.YEAR IN (substr(V_First_QTR,1,4),substr(V_Second_QTR,1,4))
       and ip.QUARTER in ('Q' || substr(V_First_QTR,6,1),'Q' || substr(V_Second_QTR,6,1))
       and ip.BUNAME = STR_BUNAME_FA) temp
  on (tf.POLICY_NUMBER = temp.ponumber
      and tf.COMPONENT_CODE = temp.COMPONENT_CODE
      and tf.BUNAME = STR_BUNAME_FA
      and tf.BATCH_NUMBER = P_BATCH_NO)
  when matched then update
  set tf.OLD_AGENT_CD = temp.WRI_AGT_CODE;

Log('update AIA_CB_TRACE_FORWARD_COMP old agent code for SPI FA 2.1' || '; row count: ' || to_char(sql%rowcount));
commit;
--Version 17 end

/*Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);

--for lumpsum compensation trace forward for SPI
for i in 1..t_periodseq.count loop
insert into AIA_CB_TRACE_FORWARD_COMP
select STR_BUNAME as BUNAME,
       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
       ip.ponumber as POLICY_NUMBER,
       ip.policyidseq as POLICYIDSEQ,
       pm.positionseq as PAYEE_SEQ,
       substr(dep_pos.name, 4) as PAYEE_CODE,
       crd.genericattribute12 as PAYOR_CODE,
       ip.life_number as LIFE_NUMBER,
       ip.coverage_number as COVERAGE_NUMBER,
       ip.rider_number as RIDER_NUMBER,
       ip.component_code as COMPONENT_CODE,
       ip.component_name as COMPONENT_NAME,
       ip.base_rider_ind as BASE_RIDER_IND,
       crd.compensationdate as TRANSACTION_DATE,
       TO_CHAR(DT_CB_START_DATE, 'MON-YYYY') as PROCESSING_PERIOD,
       STR_LUMPSUM as CLAWBACK_TYPE,
       rl.CLAWBACK_NAME as CLAWBACK_NAME,
       STR_COMPENSATION as CLAWBACK_METHOD,
       ct.credittypeid as CREDIT_TYPE,
       crd.creditseq as CREDITSEQ,
       crd.name as CREDIT_NAME,
       crd.value as CREDIT_VALUE,
       crd.positionseq as crd_positionseq,
       st.genericdate2 as crd_genericdate2,
       crd.ruleseq as crd_ruleseq,
       pm.measurementseq as PM_SEQ,
       pm.name as PM_NAME,
       pct.contributionvalue as PM_CONTRIBUTION_VALUE,
       case
         when sm3.name like 'SM_SPI_RATE%' then
          sm3.value
         else
          0
       end as PM_RATE,
       dep.depositseq as DEPOSITSEQ,
       dep.name as DEPOSIT_NAME,
       dep.value as DEPOSIT_VALUE,
       crd.periodseq as PERIODSEQ,
       st.salestransactionseq as SALESTRANSACTIONSEQ,
       crd.genericattribute2 as PRODUCT_NAME,
       crd.genericnumber1 as POLICY_YEAR,
       st.genericnumber2 as COMMISSION_RATE,
       st.genericdate4 as PAID_TO_DATE,
       P_BATCH_NO as BATCH_NUMBER,
       sysdate as CREATED_DATE
  FROM CS_SALESTRANSACTION st
 inner join CS_CREDIT crd
    on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
--and crd.genericdate2
 inner join CS_PMCREDITTRACE pct
    on crd.CREDITSEQ = pct.CREDITSEQ
 inner join CS_MEASUREMENT pm
    on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
 inner join (select distinct SOURCE_RULE_OUTPUT
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME = 'SPI') pmr
    on pmr.SOURCE_RULE_OUTPUT = pm.name
--for SM level 1 (SM_PIB_SG_SPI)
 inner join CS_PMSELFTRACE pm_sm1
    on pm.measurementseq = pm_sm1.sourcemeasurementseq
 inner join CS_MEASUREMENT sm1
    on sm1.measurementseq = pm_sm1.targetmeasurementseq
--for SM level 2 (SM_PIB_YTD_SG_SPI)
 inner join CS_PMSELFTRACE sm1_sm2
    on sm1.measurementseq = sm1_sm2.sourcemeasurementseq
 inner join CS_MEASUREMENT sm2
    on sm2.measurementseq = sm1_sm2.targetmeasurementseq
--for SM level 3 (SM_SPI_RATE/SM_SPI_RATE_NEW_AGT)
 inner join CS_PMSELFTRACE sm2_sm3
    on sm2.measurementseq = sm2_sm3.sourcemeasurementseq
 inner join CS_MEASUREMENT sm3
    on sm3.measurementseq = sm2_sm3.targetmeasurementseq
--for SM level 4 (SM_SPI_CALCULATE_YTD)
 inner join CS_PMSELFTRACE sm3_sm4
    on sm3.measurementseq = sm3_sm4.sourcemeasurementseq
 inner join CS_MEASUREMENT sm4
    on sm4.measurementseq = sm3_sm4.targetmeasurementseq
--for SM level 5 (SM_SPI_PAYMENT_QTR)
 inner join CS_PMSELFTRACE sm4_sm5
    on sm4.measurementseq = sm4_sm5.sourcemeasurementseq
 inner join CS_MEASUREMENT sm5
    on sm5.measurementseq = sm4_sm5.targetmeasurementseq
--for SM level 6 (SM_SPI_PAYMENT_YTD)
 inner join CS_PMSELFTRACE sm5_sm6
    on sm5.measurementseq = sm5_sm6.sourcemeasurementseq
 inner join CS_MEASUREMENT sm6
    on sm6.measurementseq = sm5_sm6.targetmeasurementseq
--for Incentive (I_SPI_SG)
 inner join CS_INCENTIVEPMTRACE inpm
    on sm5.measurementseq = inpm.MEASUREMENTSEQ
 inner join cs_incentive inc
    on inpm.incentiveseq = inc.incentiveseq
--for deposit (D_SPI_SG)
 inner join CS_DEPOSITINCENTIVETRACE depin
    on inpm.incentiveseq = depin.incentiveseq
 inner join cs_deposit dep
    on depin.depositseq = dep.depositseq
 inner join cs_position dep_pos
    on dep.positionseq = dep_pos.ruleelementownerseq
   and dep_pos.removedate = DT_REMOVEDATE
   and dep_pos.effectivestartdate <= crd.genericdate2
   and dep_pos.effectiveenddate > crd.genericdate2
 inner join CS_CREDITTYPE ct
    on crd.CREDITTYPESEQ = ct.DATATYPESEQ
   and ct.Removedate = DT_REMOVEDATE
 inner join AIA_CB_IDENTIFY_POLICY ip
    on st.PONUMBER = ip.PONUMBER
   AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
   AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
   AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
   AND st.PRODUCTID = ip.COMPONENT_CODE
   and crd.genericattribute12 = ip.wri_agt_code
  inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
               from AIA_CB_RULES_LOOKUP
              where RULE_TYPE = 'DR'
                AND CLAWBACK_NAME = 'SPI') rl
    on dep.NAME = rl.SOURCE_RULE_OUTPUT
 where dep.periodseq = t_periodseq(i)
 and st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
 AND st.BUSINESSUNITMAP = 1
 --and crd.genericattribute16 not in ('RO', 'RNO')
 ;

end loop;

Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI' || '; row count: ' || to_char(sql%rowcount));*/

elsif P_STR_TYPE = STR_ONGOING then

  select value into V_NADOR_RATE
from CS_FIXEDVALUE fv where
name='FV_NADOR_Payout_Rate'
and Removedate = DT_REMOVEDATE;

 select value into V_NLPI_RATE
from CS_FIXEDVALUE fv where
name='FV_NLPI_RATE'
and Removedate = DT_REMOVEDATE;

Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);

/*

create table aias_tx_temp tablespace tallydata
as
select  *+ PARALLEL leading(ip,st) INDEX(st aia_salestransaction_product) *st*,
ip.BUNAME
,ip.YEAR
,ip.QUARTER
,ip.WRI_DIST_CODE
,ip.WRI_DIST_NAME
,ip.WRI_DM_CODE
,ip.WRI_DM_NAME
,ip.WRI_AGY_CODE
,ip.WRI_AGY_NAME
,ip.WRI_AGY_LDR_CODE
,ip.WRI_AGY_LDR_NAME
,ip.WRI_AGT_CODE
,ip.WRI_AGT_NAME
,ip.FSC_TYPE
,ip.RANK
,ip.CLASS
,ip.FSC_BSC_GRADE
,ip.FSC_BSC_PERCENTAGE
,ip.INSURED_NAME
,ip.CONTRACT_CAT
,ip.LIFE_NUMBER
,ip.COVERAGE_NUMBER
,ip.RIDER_NUMBER
,ip.COMPONENT_CODE
,ip.COMPONENT_NAME
,ip.ISSUE_DATE
,ip.INCEPTION_DATE
,ip.RISK_COMMENCEMENT_DATE
,ip.FHR_DATE
,ip.BASE_RIDER_IND
,ip.TRANSACTION_DATE
,ip.PAYMENT_MODE
,ip.POLICY_CURRENCY
,ip.PROCESSING_PERIOD
,ip.CREATED_DATE
,ip.POLICYIDSEQ
,ip.SUBMITDATE
,p.periodseq from CS_SALESTRANSACTION st
  inner join AIA_CB_IDENTIFY_POLICY ip
     on st.PONUMBER = ip.PONUMBER
    AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
    AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
    AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
    AND st.PRODUCTID = ip.COMPONENT_CODE
  inner join CS_PERIOD p
     on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
        and p.calendarseq=2251799813685250
       where 1=0;
*/
Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1 part1, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);

execute immediate 'truncate table aias_tx_temp';
insert into aias_tx_temp
select  /*+ PARALLEL leading(ip,st,p) INDEX(st aia_salestransaction_product) */
st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, 
st.BILLTOADDRESSSEQ, st.SHIPTOADDRESSSEQ, st.OTHERTOADDRESSSEQ, st.ISRUNNABLE, st.BUSINESSUNITMAP, st.ACCOUNTINGDATE, st.PRODUCTID, st.PRODUCTNAME, st.PRODUCTDESCRIPTION, 
st.NUMBEROFUNITS, st.UNITVALUE, st.UNITTYPEFORUNITVALUE, st.PREADJUSTEDVALUE, st.UNITTYPEFORPREADJUSTEDVALUE, st.VALUE, st.UNITTYPEFORVALUE, st.NATIVECURRENCY, st.NATIVECURRENCYAMOUNT, 
st.DISCOUNTPERCENT, st.DISCOUNTTYPE, st.PAYMENTTERMS, st.PONUMBER, st.CHANNEL, st.ALTERNATEORDERNUMBER, st.DATASOURCE, st.REASONSEQ, st.COMMENTS, st.GENERICATTRIBUTE1, 
st.GENERICATTRIBUTE2, st.GENERICATTRIBUTE3, st.GENERICATTRIBUTE4, st.GENERICATTRIBUTE5, st.GENERICATTRIBUTE6, st.GENERICATTRIBUTE7, st.GENERICATTRIBUTE8, st.GENERICATTRIBUTE9, 
st.GENERICATTRIBUTE10, st.GENERICATTRIBUTE11, st.GENERICATTRIBUTE12, st.GENERICATTRIBUTE13, st.GENERICATTRIBUTE14, st.GENERICATTRIBUTE15, st.GENERICATTRIBUTE16, st.GENERICATTRIBUTE17, 
st.GENERICATTRIBUTE18, st.GENERICATTRIBUTE19, st.GENERICATTRIBUTE20, st.GENERICATTRIBUTE21, st.GENERICATTRIBUTE22, st.GENERICATTRIBUTE23, st.GENERICATTRIBUTE24, st.GENERICATTRIBUTE25, 
st.GENERICATTRIBUTE26, st.GENERICATTRIBUTE27, st.GENERICATTRIBUTE28, st.GENERICATTRIBUTE29, st.GENERICATTRIBUTE30, st.GENERICATTRIBUTE31, st.GENERICATTRIBUTE32, st.GENERICNUMBER1, 
st.UNITTYPEFORGENERICNUMBER1, st.GENERICNUMBER2, st.UNITTYPEFORGENERICNUMBER2, st.GENERICNUMBER3, st.UNITTYPEFORGENERICNUMBER3, st.GENERICNUMBER4, st.UNITTYPEFORGENERICNUMBER4, 
st.GENERICNUMBER5, st.UNITTYPEFORGENERICNUMBER5, st.GENERICNUMBER6, st.UNITTYPEFORGENERICNUMBER6, st.GENERICDATE1, st.GENERICDATE2, st.GENERICDATE3, st.GENERICDATE4, st.GENERICDATE5, 
st.GENERICDATE6, st.GENERICBOOLEAN1, st.GENERICBOOLEAN2, st.GENERICBOOLEAN3, st.GENERICBOOLEAN4, st.GENERICBOOLEAN5, st.GENERICBOOLEAN6, st.PROCESSINGUNITSEQ, st.MODIFICATIONDATE, 
st.UNITTYPEFORLINENUMBER, st.UNITTYPEFORSUBLINENUMBER, st.UNITTYPEFORNUMBEROFUNITS, st.UNITTYPEFORDISCOUNTPERCENT, st.UNITTYPEFORNATIVECURRENCYAMT, st.MODELSEQ,
ip.BUNAME
,ip.YEAR
,ip.QUARTER
,ip.WRI_DIST_CODE
,ip.WRI_DIST_NAME
,ip.WRI_DM_CODE
,ip.WRI_DM_NAME
,ip.WRI_AGY_CODE
,ip.WRI_AGY_NAME
,ip.WRI_AGY_LDR_CODE
,ip.WRI_AGY_LDR_NAME
,ip.WRI_AGT_CODE
,ip.WRI_AGT_NAME
,ip.FSC_TYPE
,ip.RANK
,ip.CLASS
,ip.FSC_BSC_GRADE
,ip.FSC_BSC_PERCENTAGE
,ip.INSURED_NAME
,ip.CONTRACT_CAT
,ip.LIFE_NUMBER
,ip.COVERAGE_NUMBER
,ip.RIDER_NUMBER
,ip.COMPONENT_CODE
,ip.COMPONENT_NAME
,ip.ISSUE_DATE
,ip.INCEPTION_DATE
,ip.RISK_COMMENCEMENT_DATE
,ip.FHR_DATE
,ip.BASE_RIDER_IND
,ip.TRANSACTION_DATE
,ip.PAYMENT_MODE
,ip.POLICY_CURRENCY
,ip.PROCESSING_PERIOD
,ip.CREATED_DATE
,ip.POLICYIDSEQ
,ip.SUBMITDATE
,p.periodseq
,ip.FAOB_AGT_CODE
,''
,''
from CS_SALESTRANSACTION st
  inner join AIA_CB_IDENTIFY_POLICY ip
     on st.PONUMBER = ip.PONUMBER
    AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
    AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
    AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
    AND st.PRODUCTID = ip.COMPONENT_CODE
  inner join CS_PERIOD p
     on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
        and p.calendarseq=V_CALENDARSEQ and p.periodtypeseq = 2814749767106561
   inner join cs_businessunit bu on st.businessunitmap = bu.mask
    where st.tenantid='AIAS' and   st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    and ip.buname = STR_BUNAME_FA
    and st.eventtypeseq <> 16607023625933358
    and p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
    --v8 20200928
    and st.compensationdate between ONGOING_ST_DT and ONGOING_END_DT
        ;

--For AI clawback NL20180308
insert into aias_tx_temp
with AMR as (select row_number() over(partition by t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE ,t1.POLICY_INCEPTION_DATE order by t1.component_CODE) as rn,
             t1.* from AI_MONTHLY_REPORT t1 where t1.AI_PAYMENT<> 0),
     st as (select row_number() over(partition by t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11 ,t2.GENERICDATE2 order by t2.PRODUCTID) as rn,
            t2.* from cs_Salestransaction t2,cs_businessunit  bu  where t2.tenantid='AIAS' and t2.businessunitmap = bu.mask
--          and bu.name = STR_BUNAME_FA   --Changes done to fix not getting AGY AI records --Gopi-04072019
          and t2.eventtypeseq = 16607023625933358 and  t2.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
          --v8 20200928
          and t2.compensationdate between ONGOING_ST_DT and ONGOING_END_DT),
     IP as (select row_number() over(partition by t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date order by t3.coverage_number ) as rn,
            t3.* from AIA_CB_IDENTIFY_POLICY t3 where t3.BUNAME  =STR_BUNAME_FA)
select  /*+ PARALLEL */st.TENANTID                        ,
st.SALESTRANSACTIONSEQ             ,
st.SALESORDERSEQ                   ,
st.LINENUMBER                      ,
st.SUBLINENUMBER                   ,
st.EVENTTYPESEQ                    ,
st.PIPELINERUNSEQ                  ,
st.ORIGINTYPEID                    ,
st.COMPENSATIONDATE                ,
st.BILLTOADDRESSSEQ                ,
st.SHIPTOADDRESSSEQ                ,
st.OTHERTOADDRESSSEQ               ,
st.ISRUNNABLE                      ,
st.BUSINESSUNITMAP                 ,
st.ACCOUNTINGDATE                  ,
st.PRODUCTID                       ,
st.PRODUCTNAME                     ,
st.PRODUCTDESCRIPTION              ,
st.NUMBEROFUNITS                   ,
st.UNITVALUE                       ,
st.UNITTYPEFORUNITVALUE            ,
st.PREADJUSTEDVALUE                ,
st.UNITTYPEFORPREADJUSTEDVALUE     ,
st.VALUE                           ,
st.UNITTYPEFORVALUE                ,
st.NATIVECURRENCY                  ,
st.NATIVECURRENCYAMOUNT            ,
st.DISCOUNTPERCENT                 ,
st.DISCOUNTTYPE                    ,
st.PAYMENTTERMS                    ,
st.PONUMBER                        ,
st.CHANNEL                         ,
st.ALTERNATEORDERNUMBER            ,
st.DATASOURCE                      ,
st.REASONSEQ                       ,
st.COMMENTS                        ,
st.GENERICATTRIBUTE1               ,
st.GENERICATTRIBUTE2               ,
st.GENERICATTRIBUTE3               ,
st.GENERICATTRIBUTE4               ,
st.GENERICATTRIBUTE5               ,
st.GENERICATTRIBUTE6               ,
st.GENERICATTRIBUTE7               ,
st.GENERICATTRIBUTE8               ,
st.GENERICATTRIBUTE9               ,
st.GENERICATTRIBUTE10              ,
st.GENERICATTRIBUTE11              ,
st.GENERICATTRIBUTE12              ,
st.GENERICATTRIBUTE13              ,
st.GENERICATTRIBUTE14              ,
st.GENERICATTRIBUTE15              ,
st.GENERICATTRIBUTE16              ,
st.GENERICATTRIBUTE17              ,
st.GENERICATTRIBUTE18              ,
st.GENERICATTRIBUTE19              ,
st.GENERICATTRIBUTE20              ,
st.GENERICATTRIBUTE21              ,
st.GENERICATTRIBUTE22              ,
st.GENERICATTRIBUTE23              ,
st.GENERICATTRIBUTE24              ,
st.GENERICATTRIBUTE25              ,
st.GENERICATTRIBUTE26              ,
st.GENERICATTRIBUTE27              ,
st.GENERICATTRIBUTE28              ,
st.GENERICATTRIBUTE29              ,
st.GENERICATTRIBUTE30              ,
st.GENERICATTRIBUTE31              ,
st.GENERICATTRIBUTE32              ,
st.GENERICNUMBER1                  ,
st.UNITTYPEFORGENERICNUMBER1       ,
st.GENERICNUMBER2                  ,
st.UNITTYPEFORGENERICNUMBER2       ,
st.GENERICNUMBER3                  ,
st.UNITTYPEFORGENERICNUMBER3       ,
st.GENERICNUMBER4                  ,
st.UNITTYPEFORGENERICNUMBER4       ,
st.GENERICNUMBER5                  ,
st.UNITTYPEFORGENERICNUMBER5       ,
st.GENERICNUMBER6                  ,
st.UNITTYPEFORGENERICNUMBER6       ,
st.GENERICDATE1                    ,
st.GENERICDATE2                    ,
st.GENERICDATE3                    ,
st.GENERICDATE4                    ,
st.GENERICDATE5                    ,
st.GENERICDATE6                    ,
st.GENERICBOOLEAN1                 ,
st.GENERICBOOLEAN2                 ,
st.GENERICBOOLEAN3                 ,
st.GENERICBOOLEAN4                 ,
st.GENERICBOOLEAN5                 ,
st.GENERICBOOLEAN6                 ,
st.PROCESSINGUNITSEQ               ,
st.MODIFICATIONDATE                ,
st.UNITTYPEFORLINENUMBER           ,
st.UNITTYPEFORSUBLINENUMBER        ,
st.UNITTYPEFORNUMBEROFUNITS        ,
st.UNITTYPEFORDISCOUNTPERCENT      ,
st.UNITTYPEFORNATIVECURRENCYAMT    ,
st.MODELSEQ                        ,
ip.BUNAME
,ip.YEAR
,ip.QUARTER
,ip.WRI_DIST_CODE
,ip.WRI_DIST_NAME
,ip.WRI_DM_CODE
,ip.WRI_DM_NAME
,ip.WRI_AGY_CODE
,ip.WRI_AGY_NAME
,ip.WRI_AGY_LDR_CODE
,ip.WRI_AGY_LDR_NAME
,ip.WRI_AGT_CODE
,ip.WRI_AGT_NAME
,ip.FSC_TYPE
,ip.RANK
,ip.CLASS
,ip.FSC_BSC_GRADE
,ip.FSC_BSC_PERCENTAGE
,ip.INSURED_NAME
,ip.CONTRACT_CAT
,ip.LIFE_NUMBER
,ip.COVERAGE_NUMBER
,ip.RIDER_NUMBER
,ip.COMPONENT_CODE
,ip.COMPONENT_NAME
,ip.ISSUE_DATE
,ip.INCEPTION_DATE
,ip.RISK_COMMENCEMENT_DATE
,ip.FHR_DATE
,ip.BASE_RIDER_IND
,ip.TRANSACTION_DATE
,ip.PAYMENT_MODE
,ip.POLICY_CURRENCY
,ip.PROCESSING_PERIOD
,ip.CREATED_DATE
,ip.POLICYIDSEQ
,ip.SUBMITDATE
,p.periodseq
,ip.FAOB_AGT_CODE
,'',''
from  st
 INNER JOIN  AMR
ON  st.PONUMBER = AMR.PONUMBER
AND st.VALUE = AMR.AI_PAYMENT
AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
--AND st.GENERICATTRIBUTE11 = AMR.PAYEE_CODE
AND (st.GENERICATTRIBUTE11 = AMR.NEW_AGENT_CD OR st.GENERICATTRIBUTE11=AMR.OLD_AGENT_CD) ----Changes done to fix not getting AGY AI records --Gopi-04072019
AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
AND st.rn = AMR.rn
inner join ip
     on IP.BUNAME                     = STR_BUNAME_FA
     AND AMR.PONUMBER                   = IP.PONUMBER
/*AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER*/
--     and AMR.PAYEE_CODE = IP.WRI_AGT_CODE
and (AMR.NEW_AGENT_CD = IP.WRI_AGT_CODE OR AMR.OLD_AGENT_CD=IP.WRI_AGT_CODE) --Changes done to fix not getting AGY AI records --Gopi-04072019
     and AMR.component_CODE=ip.component_CODE
     and AMR.policy_inception_date = ip.inception_date
     and AMR.risk_commencement_date = ip.risk_commencement_date
     and AMR.rn = IP.rn
  inner join CS_PERIOD p
     on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
        and p.calendarseq=V_CALENDARSEQ and p.periodtypeseq = 2814749767106561
    where p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
        ;

Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 1 part 1b, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT);
commit;

insert into aias_tx_temp
select * from AIA_CB_COMP_ONG_STGPAST_TX_FA;

Log('added AIA_CB_COMP_ONG_STGPAST_TX_FA into aias_tx_temp for Ongoing 1 part 1b, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT);
commit;

execute immediate 'truncate table aias_tx_temp15';
--drop table aias_tx_temp2
insert into aias_tx_temp15
select /*+ INDEX(crd CS_CREDIT_TRANSACTIONSEQ) */ ip.TENANTID,
ip.SALESTRANSACTIONSEQ,
ip.SALESORDERSEQ,
ip.LINENUMBER,
ip.SUBLINENUMBER,
ip.EVENTTYPESEQ,
ip.PIPELINERUNSEQ,
ip.ORIGINTYPEID,
ip.COMPENSATIONDATE,
ip.BILLTOADDRESSSEQ,
ip.SHIPTOADDRESSSEQ,
ip.OTHERTOADDRESSSEQ,
ip.ISRUNNABLE,
ip.BUSINESSUNITMAP,
ip.ACCOUNTINGDATE,
ip.PRODUCTID,
ip.PRODUCTNAME,
ip.PRODUCTDESCRIPTION,
ip.NUMBEROFUNITS,
ip.UNITVALUE,
ip.UNITTYPEFORUNITVALUE,
ip.PREADJUSTEDVALUE,
ip.UNITTYPEFORPREADJUSTEDVALUE,
ip.VALUE ,
ip.UNITTYPEFORVALUE,
ip.NATIVECURRENCY,
ip.NATIVECURRENCYAMOUNT,
ip.DISCOUNTPERCENT,
ip.DISCOUNTTYPE,
ip.PAYMENTTERMS,
ip.PONUMBER  ,
ip.CHANNEL ,
ip.ALTERNATEORDERNUMBER ,
ip.DATASOURCE,
ip.REASONSEQ,
ip.COMMENTS,
ip.GENERICATTRIBUTE1,
ip.GENERICATTRIBUTE2,
ip.GENERICATTRIBUTE3,
ip.GENERICATTRIBUTE4,
ip.GENERICATTRIBUTE5,
ip.GENERICATTRIBUTE6,
ip.GENERICATTRIBUTE7,
ip.GENERICATTRIBUTE8,
ip.GENERICATTRIBUTE9,
ip.GENERICATTRIBUTE10,
ip.GENERICATTRIBUTE11,
ip.GENERICATTRIBUTE12,
ip.GENERICATTRIBUTE13,
ip.GENERICATTRIBUTE14,
ip.GENERICATTRIBUTE15,
ip.GENERICATTRIBUTE16,
ip.GENERICATTRIBUTE17,
ip.GENERICATTRIBUTE18,
ip.GENERICATTRIBUTE19,
ip.GENERICATTRIBUTE20,
ip.GENERICATTRIBUTE21,
ip.GENERICATTRIBUTE22,
ip.GENERICATTRIBUTE23,
ip.GENERICATTRIBUTE24,
ip.GENERICATTRIBUTE25,
ip.GENERICATTRIBUTE26,
ip.GENERICATTRIBUTE27,
ip.GENERICATTRIBUTE28,
ip.GENERICATTRIBUTE29,
ip.GENERICATTRIBUTE30,
ip.GENERICATTRIBUTE31,
ip.GENERICATTRIBUTE32,
ip.GENERICNUMBER1,
ip.UNITTYPEFORGENERICNUMBER1,
ip.GENERICNUMBER2,
ip.UNITTYPEFORGENERICNUMBER2,
ip.GENERICNUMBER3,
ip.UNITTYPEFORGENERICNUMBER3,
ip.GENERICNUMBER4,
ip.UNITTYPEFORGENERICNUMBER4,
ip.GENERICNUMBER5,
ip.UNITTYPEFORGENERICNUMBER5,
ip.GENERICNUMBER6,
ip.UNITTYPEFORGENERICNUMBER6,
ip.GENERICDATE1,
ip.GENERICDATE2,
ip.GENERICDATE3,
ip.GENERICDATE4,
ip.GENERICDATE5,
ip.GENERICDATE6,
ip.GENERICBOOLEAN1,
ip.GENERICBOOLEAN2,
ip.GENERICBOOLEAN3,
ip.GENERICBOOLEAN4,
ip.GENERICBOOLEAN5,
ip.GENERICBOOLEAN6,
ip.PROCESSINGUNITSEQ,
ip.MODIFICATIONDATE,
ip.UNITTYPEFORLINENUMBER ,
ip.UNITTYPEFORSUBLINENUMBER,
ip.UNITTYPEFORNUMBEROFUNITS,
ip.UNITTYPEFORDISCOUNTPERCENT,
ip.UNITTYPEFORNATIVECURRENCYAMT,
ip.MODELSEQ,
ip.BUNAME,
ip.YEAR,
ip.QUARTER ,
ip.WRI_DIST_CODE,
ip.WRI_DIST_NAME ,
ip.WRI_DM_CODE,
ip.WRI_DM_NAME,
ip.WRI_AGY_CODE,
ip.WRI_AGY_NAME ,
ip.WRI_AGY_LDR_CODE ,
ip.WRI_AGY_LDR_NAME,
ip.WRI_AGT_CODE ,
ip.WRI_AGT_NAME,
ip.FSC_TYPE,
ip.RANK,
ip.CLASS ,
ip.FSC_BSC_GRADE,
ip.FSC_BSC_PERCENTAGE,
ip.INSURED_NAME,
ip.CONTRACT_CAT,
ip.LIFE_NUMBER,
ip.COVERAGE_NUMBER,
ip.RIDER_NUMBER,
ip.COMPONENT_CODE ,
ip.COMPONENT_NAME,
ip.ISSUE_DATE,
ip.INCEPTION_DATE,
ip.RISK_COMMENCEMENT_DATE,
ip.FHR_DATE,
ip.BASE_RIDER_IND,
ip.TRANSACTION_DATE,
ip.PAYMENT_MODE,
ip.POLICY_CURRENCY,
ip.PROCESSING_PERIOD,
ip.CREATED_DATE,
ip.POLICYIDSEQ,
ip.SUBMITDATE,
ip.PERIODSEQ,
crd.name,
crd.creditseq,
crd.genericdate2 crdGd2,
crd.genericattribute13 crdga13,
crd.genericattribute14 crdga14,
crd.name crdName,
null as measurementseq,
ct.credittypeid,
cbp.cb_enddate cbpenddate,
crd.genericattribute12 CRDGA12,
crd.compensationdate CRDCOMPDate,
crd.value crdvalue,
crd.positionseq crdpositionseq,
crd.ruleseq crdRuleSeq,
crd.periodseq Crdperiodseq ,
crd.genericattribute2 crdGA2,
crd.genericnumber1 crdgn1--, null as contributionvalue
,'',''
   FROM aias_tx_temp ip
  inner join CS_CREDIT crd
     on ip.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
    and (crd.genericattribute12 = ip.wri_agt_code or ip.FAOB_AGT_CODE=crd.genericattribute12)
    and crd.periodseq = ip.periodseq
  --inner join CS_PMCREDITTRACE pct
    -- on crd.CREDITSEQ = pct.CREDITSEQ
    -- and pct.sourceperiodseq=2533274790398934
  inner join CS_CREDITTYPE ct
     on crd.CREDITTYPESEQ = ct.DATATYPESEQ and ct.tenantid='AIAS'
    and ct.Removedate = '1-jan-2200'
    inner join  (select distinct
                      cb_quarter_name,
                      cb_startdate,
                      cb_enddate
                 from aia_cb_period
                where cb_name = STR_CB_NAME
                AND buname = STR_BUNAME_FA) cbp
     on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
   --  where    crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
--      AND pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
;

Log('insert into aias_tx_temp15 FA for Ongoing 1 part 1c, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT);
commit;

--v9 tuning
execute immediate 'truncate table aia_tmp_comls_period';
insert into aia_tmp_comls_period
   select periodseq
   from cs_period
   where startdate >= trunc( ONGOING_END_DT,'YYYY')
--   and add_months((ONGOING_END_DT+1),-9)
   and enddate <= ONGOING_END_DT + 1
   and periodtypeseq = V_periodtype_month_seq
   and removedate = DT_REMOVEDATE; --month

Log('insert into aia_tmp_comls_period for Ongoing 1 part 1c - comls_period, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO || ' ' ||SQL%ROWCOUNT);
commit;

execute immediate 'truncate table aias_tx_temp2';

for v_comls_period in (select periodseq from aia_tmp_comls_period)
loop
    insert into aias_tx_temp2
    --create table aias_tx_temp2 as
     select    ip.tenantid,
        salestransactionseq,
        salesorderseq,
        linenumber,
        sublinenumber,
        eventtypeseq,
        ip.pipelinerunseq,
        origintypeid,
        compensationdate,
        billtoaddressseq,
        shiptoaddressseq,
        othertoaddressseq,
        isrunnable,
       ip.businessunitmap,
        accountingdate,
        productid,
        productname,
        productdescription,
        numberofunits,
        unitvalue,
        unittypeforunitvalue,
        preadjustedvalue,
        unittypeforpreadjustedvalue,
        value,
        unittypeforvalue,
        nativecurrency,
        nativecurrencyamount,
        discountpercent,
        discounttype,
        paymentterms,
        ponumber,
        channel,
        alternateordernumber,
        datasource,
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
        genericattribute17,
        genericattribute18,
        genericattribute19,
        genericattribute20,
        genericattribute21,
        genericattribute22,
        genericattribute23,
        genericattribute24,
        genericattribute25,
        genericattribute26,
        genericattribute27,
        genericattribute28,
        genericattribute29,
        genericattribute30,
        genericattribute31,
        genericattribute32,
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
        ip.processingunitseq,
        modificationdate,
        unittypeforlinenumber,
        unittypeforsublinenumber,
        unittypefornumberofunits,
        unittypefordiscountpercent,
        unittypefornativecurrencyamt,
        ip.modelseq,
        buname,
        year,
        quarter,
        wri_dist_code,
        wri_dist_name,
        wri_dm_code,
        wri_dm_name,
        wri_agy_code,
        wri_agy_name,
        wri_agy_ldr_code,
        wri_agy_ldr_name,
        wri_agt_code,
        wri_agt_name,
        fsc_type,
        rank,
        class,
        fsc_bsc_grade,
        fsc_bsc_percentage,
        insured_name,
        contract_cat,
        life_number,
        coverage_number,
        rider_number,
        component_code,
        component_name,
        issue_date,
        inception_date,
        risk_commencement_date,
        fhr_date,
        base_rider_ind,
        transaction_date,
        payment_mode,
        policy_currency,
        processing_period,
        created_date,
        policyidseq,
        submitdate,
        periodseq,
        name,
        ip.creditseq,
        crdgd2,
        crdga13,
        crdga14,
        crdname,
        pct.measurementseq,
        credittypeid,
        cbpenddate,
        crdga12,
        crdcompdate,
        crdvalue,
        crdpositionseq,
        crdruleseq,
        crdperiodseq,
        crdga2,
        crdgn1, pct.contributionvalue
        ,'',''
       FROM aias_tx_temp15 ip
      inner join CS_PMCREDITTRACE pct
         on ip.CREDITSEQ = pct.CREDITSEQ
         where  pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
         and pct.targetperiodseq = v_comls_period.periodseq;

    commit;
end loop;

-- v9 tuning end

--for on-going compensation trace forward
--insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
-- select  /*+ LEADING(aias_tx_temp,crd,pct,pm) PARALLEL */ STR_BUNAME as BUNAME,
  /*      ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
        ip.ponumber as POLICY_NUMBER,
        ip.policyidseq as POLICYIDSEQ,
        pm.positionseq as PAYEE_SEQ,
        substr(pm_pos.name, 4) as PAYEE_CODE,
        crd.genericattribute12 as PAYOR_CODE,
        ip.life_number as LIFE_NUMBER,
        ip.coverage_number as COVERAGE_NUMBER,
        ip.rider_number as RIDER_NUMBER,
        ip.component_code as COMPONENT_CODE,
        ip.component_name as COMPONENT_NAME,
        ip.base_rider_ind as BASE_RIDER_IND,
        crd.compensationdate as TRANSACTION_DATE,
        substr(ONGOING_PERIOD,1,3)||'-'||substr(ONGOING_PERIOD,-4) as PROCESSING_PERIOD,
        STR_ONGOING as CLAWBACK_TYPE,
         rl.CLAWBACK_NAME       as CLAWBACK_NAME,
          STR_CB_NAME as CLAWBACK_METHOD,
        ct.credittypeid as CREDITTYPE,
        crd.creditseq as CREDITSEQ,
        crd.name as CREDIT_NAME,
        crd.value as CREDIT_VALUE,
         crd.positionseq as crd_positionseq,
        ip.genericdate2 as crd_genericdate2,
        crd.ruleseq as crd_ruleseq,
        pm.measurementseq as PM_SEQ,
        pm.name as PM_NAME,
        case rl.CLAWBACK_NAME
         when 'NLPI_ONG' then pct.contributionvalue*V_NLPI_RATE
         when 'NADOR' then pct.contributionvalue*V_NADOR_RATE
         else
          pct.contributionvalue
        end as PM_CONTRIBUTION_VALUE,
        case rl.CLAWBACK_NAME
         when 'FYO_ONG' then fyo_rate.value
         when 'RYO_ONG' then ryo_rate.value
         when 'FSM_RYO_ONG' then ryo_rate.value
         when 'NLPI_ONG' then V_NLPI_RATE
         when 'NADOR' then V_NADOR_RATE
       else 1
         end as PM_RATE,
      --1 as PM_RATE,
        '' as DEPOSITSEQ,
        '' as DEPOSIT_NAME,
        '' as DEPOSIT_VALUE,
        crd.periodseq as PERIODSEQ,
        ip.salestransactionseq as SALESTRANSACTIONSEQ,
        crd.genericattribute2 as PRODUCT_NAME,
        crd.genericnumber1 as POLICY_YEAR,
        ip.genericnumber2      as COMMISSION_RATE,
        ip.genericdate4 as PAID_TO_DATE,
        P_BATCH_NO as BATCH_NUMBER,
        sysdate as CREATED_DATE
   FROM aias_tx_temp ip
  inner join CS_CREDIT crd
     on ip.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
    and crd.genericattribute12 = ip.wri_agt_code
    and crd.periodseq = ip.periodseq
  inner join CS_PMCREDITTRACE pct
     on crd.CREDITSEQ = pct.CREDITSEQ
    -- and pct.targetperiodseq= ip.periodseq
  inner join CS_MEASUREMENT pm
     on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
      --    and pct.targetperiodseq= pm.periodseq
  inner join CS_POSITION pm_pos
     on pm.positionseq = pm_pos.ruleelementownerseq
    and pm_pos.removedate = DT_REMOVEDATE
    and pm_pos.effectivestartdate <= crd.genericdate2
    and pm_pos.effectiveenddate > crd.genericdate2
  inner join CS_CREDITTYPE ct
     on crd.CREDITTYPESEQ = ct.DATATYPESEQ
    and ct.Removedate = DT_REMOVEDATE
  inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                from AIA_CB_RULES_LOOKUP
               where RULE_TYPE = 'PM'
                 AND CLAWBACK_NAME  IN ('FYO_ONG','RYO_ONG','FSM_RYO_ONG','NLPI_ONG','NADOR')) rl
     on pm.NAME = rl.SOURCE_RULE_OUTPUT
     and pm.periodseq = ts_periodseq
  inner join  (select distinct
                      cb_quarter_name,
                      cb_startdate,
                      cb_enddate
                 from aia_cb_period
                where cb_name = STR_CB_NAME) cbp
     on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
  inner join cs_position dep_pos
     on pm.positionseq = dep_pos.ruleelementownerseq
    and dep_pos.removedate = DT_REMOVEDATE
    and dep_pos.effectivestartdate <= crd.genericdate2
    and dep_pos.effectiveenddate > crd.genericdate2
      --for lookup the receiver info.
  inner join cs_title dep_title
     on dep_pos.titleseq = dep_title.ruleelementownerseq
    and dep_title.removedate = DT_REMOVEDATE
    and dep_title.effectivestartdate <= crd.genericdate2
    and dep_title.effectiveenddate > crd.genericdate2
 left join vw_lt_fyo_rate fyo_rate
 on fyo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
   and fyo_rate.Receiver_title = dep_title.name
   and rl.CLAWBACK_NAME = 'FYO'
 left join vw_lt_ryo_life_rate ryo_rate
 on ryo_rate.Contributor_Leader_title = crd.genericattribute13 --payor agency leader title
 and ryo_rate.PIB_TYPE = fn_fyo_pib_type(crd.genericattribute13, crd.genericattribute14, crd.name)
 and ryo_rate.Receiver_title = dep_title.name
 and rl.CLAWBACK_NAME in ( 'RYO','FSM_RYO')
  WHERE to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
    AND crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    AND crd.tenantid='AIAS' and   crd.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    AND pm.tenantid='AIAS' and   pm.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
    AND pct.tenantid='AIAS' and   pct.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ    ;
*/
insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
 select  /*+ LEADING(aias_tx_temp2,pm) PARALLEL */ STR_BUNAME_FA as BUNAME,
       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
        ip.ponumber as POLICY_NUMBER,
        ip.policyidseq as POLICYIDSEQ,
        pm.positionseq as PAYEE_SEQ,
        substr(pm_pos.name, 4) as PAYEE_CODE,
--        crdga12 as PAYOR_CODE, --Moified to get NADOR old agent data -Gopinath 10122019
(Case rl.CLAWBACK_NAME WHEN 'NADOR_FA_2.1' THEN IP.WRI_AGT_CODE ELSE crdga12 END) as PAYOR_CODE,
        ip.life_number as LIFE_NUMBER,
        ip.coverage_number as COVERAGE_NUMBER,
        ip.rider_number as RIDER_NUMBER,
        ip.component_code as COMPONENT_CODE,
        ip.component_name as COMPONENT_NAME,
        ip.base_rider_ind as BASE_RIDER_IND,
        crdcompdate as TRANSACTION_DATE,
        substr(ONGOING_PERIOD,1,3)||'-'||substr(ONGOING_PERIOD,-4) as PROCESSING_PERIOD,
        STR_ONGOING as CLAWBACK_TYPE,
         rl.CLAWBACK_NAME       as CLAWBACK_NAME,
          STR_CB_NAME as CLAWBACK_METHOD,
        credittypeid as CREDITTYPE,
        creditseq as CREDITSEQ,
        crdname as CREDIT_NAME,
        crdvalue as CREDIT_VALUE,
         crdpositionseq as crd_positionseq,
        crdgd2 as crd_genericdate2,
        crdRuleSeq as crd_ruleseq,
        pm.measurementseq as PM_SEQ,
        pm.name as PM_NAME,
        case rl.CLAWBACK_NAME
         when 'NLPI_ONG' then ip.contributionvalue*V_NLPI_RATE
         when 'NADOR_FA_2.1' then ip.contributionvalue*V_NADOR_RATE
         else
          ip.contributionvalue
        end as PM_CONTRIBUTION_VALUE,
        case rl.CLAWBACK_NAME
         when 'FYO_FA' then fyo_rate.value
         --Added by Suresh
         when 'FYO_FA_ONG' then new_fyo_rate.value
         when 'RYO_FA_ONG' then ryo_rate.value
         when 'RYO_FA' then new_ryo_rate.value
--         when 'FSM_RYO_ONG' then ryo_rate.value
--         when 'NLPI_ONG' then V_NLPI_RATE
         when 'NADOR_FA_2.1' then V_NADOR_RATE
         --added by Suresh
       else 1
         end as PM_RATE,
      --1 as PM_RATE,
        '' as DEPOSITSEQ,
        '' as DEPOSIT_NAME,
        '' as DEPOSIT_VALUE,
        crdperiodseq as PERIODSEQ,
        ip.salestransactionseq as SALESTRANSACTIONSEQ,
        crdga2 as PRODUCT_NAME,
        crdgn1 as POLICY_YEAR,
        ip.genericnumber2      as COMMISSION_RATE,
        ip.genericdate4 as PAID_TO_DATE,
        P_BATCH_NO as BATCH_NUMBER,
        sysdate as CREATED_DATE,
        '','',null as deposit_period
from aias_tx_temp2 ip
   inner join CS_MEASUREMENT pm
     on pm.MEASUREMENTSEQ = ip.MEASUREMENTSEQ
 and pm.tenantid='AIAS' and pm.processingunitseq=v_processingunitseq
  inner join CS_POSITION pm_pos
     on pm.positionseq = pm_pos.ruleelementownerseq
    and pm_pos.removedate = '1-jan-2200'
    and pm_pos.effectivestartdate <= ip.crdGD2
    and pm_pos.effectiveenddate > ip.crdGD2
    inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                from AIA_CB_RULES_LOOKUP
               where RULE_TYPE = 'PM'
--Changed by Suresh
--Add AI NL20180308
--                AND CLAWBACK_NAME  IN ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION')) rl
AND CLAWBACK_NAME  IN ('FYO_FA_ONG','RYO_FA_ONG','NLPI_ONG'
--verstion 13 Harm_Phase4 Start
,'FA_FYO_ONG_2.1'
,'FA_RYO_ONG_2.1'
,'NADOR_FA_2.1'
,'FA_AI_ONG_2.1'
--verstion 13 Harm_Phase4 End
)) rl
--('FYO_ONG','NEW_FYO_ONG','RYO_ONG','NEW_RYO_ONG','FSM_RYO_ONG','NLPI_ONG','NADOR','AI_ONG')) rl
--end by Suresh
     on pm.NAME = rl.SOURCE_RULE_OUTPUT
     and pm.periodseq = ts_periodseq
     inner join cs_position dep_pos
     on pm.positionseq = dep_pos.ruleelementownerseq
    and dep_pos.removedate = '1-jan-2200'
    and dep_pos.effectivestartdate <= crdGD2
    and dep_pos.effectiveenddate > crdGD2
      --for lookup the receiver info.
  inner join cs_title dep_title
     on dep_pos.titleseq = dep_title.ruleelementownerseq
    and dep_title.removedate = '1-jan-2200'
    and dep_title.effectivestartdate <= crdGD2
    and dep_title.effectiveenddate > crdGD2
 left join vw_lt_fyo_rate fyo_rate
 on fyo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
   and fyo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
   and fyo_rate.Receiver_title = dep_title.name
   --Changed by Suresh
   and rl.CLAWBACK_NAME = 'FYO_FA_ONG'
   --end by Suresh
 left join vw_lt_ryo_life_rate ryo_rate
 on ryo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
 and ryo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
 and ryo_rate.Receiver_title = dep_title.name
 --Added by Suresh
 and rl.CLAWBACK_NAME in ( 'RYO_FA_ONG','FSM_RYO_ONG_FA')     --version 11
  --for lookup PM rate for New FYO
 left join vw_lt_new_fyo_rate new_fyo_rate
 on new_fyo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
 and new_fyo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
 and new_fyo_rate.Receiver_title = dep_title.name
 and rl.CLAWBACK_NAME = 'NEW_FYO_ONG_FA'
  --for lookup PM rate for New RYO
 left join VW_LT_NEW_RYO_LIFE_RATE new_ryo_rate
 on new_ryo_rate.Contributor_Leader_title = crdGA13 --payor agency leader title
   and new_ryo_rate.PIB_TYPE = fn_fyo_pib_type(crdGA13, crdGA14, crdname)
   and new_ryo_rate.Receiver_title = dep_title.name
   and rl.CLAWBACK_NAME = 'NEW_RYO_ONG_FA'
--End by Suresh
  WHERE to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbpenddate
       AND pm.tenantid='AIAS' and   pm.PROCESSINGUNITSEQ = v_processingunitseq
;

Log('insert into AIA_CB_TRACE_FORWARD_COMP FA for Ongoing 2' || '; row count: ' || to_char(sql%rowcount));

commit;

--Version 2 added by Amanda for SPI FA ONGOING begin
RECORD_CNT_ONGOING := 0;

--Check quarter end
select count(1)
into RECORD_CNT_ONGOING
from cs_period csp
 where csp.enddate = ONGOING_END_DT + 1
   and csp.removedate = DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and csp.periodtypeseq = V_periodtype_quarter_seq;

if RECORD_CNT_ONGOING > 0 then
  execute immediate 'truncate table aia_tmp_comls_period';
  --Get clawback period for ongoing, from Jan-1 to clawback end date
  insert into aia_tmp_comls_period
   select periodseq
   from cs_period
   where startdate >= trunc( ONGOING_END_DT,'YYYY')
--   and add_months((ONGOING_END_DT+1),-9)
   and enddate <= ONGOING_END_DT + 1
   and removedate = DT_REMOVEDATE  -- v9 added removecondition
   and periodtypeseq = V_periodtype_month_seq; --month
  commit;

 insert /*+ APPEND */ into AIA_CB_TRACE_FORWARD_COMP
 select  /*+ LEADING(aias_tx_temp2,pm) PARALLEL */ STR_BUNAME_FA as BUNAME,
       ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
        ip.ponumber as POLICY_NUMBER,
        ip.policyidseq as POLICYIDSEQ,
        pm.positionseq as PAYEE_SEQ,
        substr(pm_pos.name, 4) as PAYEE_CODE,
        crdga12 as PAYOR_CODE,
        ip.life_number as LIFE_NUMBER,
        ip.coverage_number as COVERAGE_NUMBER,
        ip.rider_number as RIDER_NUMBER,
        ip.component_code as COMPONENT_CODE,
        ip.component_name as COMPONENT_NAME,
        ip.base_rider_ind as BASE_RIDER_IND,
        crdcompdate as TRANSACTION_DATE,
        substr(ONGOING_PERIOD,1,3)||'-'||substr(ONGOING_PERIOD,-4) as PROCESSING_PERIOD,
        STR_ONGOING as CLAWBACK_TYPE,
         rl.CLAWBACK_NAME       as CLAWBACK_NAME,
          STR_CB_NAME as CLAWBACK_METHOD,
        credittypeid as CREDITTYPE,
        creditseq as CREDITSEQ,
        crdname as CREDIT_NAME,
        crdvalue as CREDIT_VALUE,
         crdpositionseq as crd_positionseq,
        crdgd2 as crd_genericdate2,
        crdRuleSeq as crd_ruleseq,
        pm.measurementseq as PM_SEQ,
        pm.name as PM_NAME,
        ip.contributionvalue PM_CONTRIBUTION_VALUE,
        1 as PM_RATE,
        '' as DEPOSITSEQ,
        '' as DEPOSIT_NAME,
        '' as DEPOSIT_VALUE,
        crdperiodseq as PERIODSEQ,
        ip.salestransactionseq as SALESTRANSACTIONSEQ,
        crdga2 as PRODUCT_NAME,
        crdgn1 as POLICY_YEAR,
        ip.genericnumber2      as COMMISSION_RATE,
        ip.genericdate4 as PAID_TO_DATE,
        P_BATCH_NO as BATCH_NUMBER,
        sysdate as CREATED_DATE,
        '',''
       ,ts_periodseq
from aias_tx_temp2 ip
   inner join CS_MEASUREMENT pm
     on pm.MEASUREMENTSEQ = ip.MEASUREMENTSEQ
 and pm.tenantid='AIAS' and pm.processingunitseq=v_processingunitseq
  inner join CS_POSITION pm_pos
     on pm.positionseq = pm_pos.ruleelementownerseq
    and pm_pos.removedate = '1-jan-2200'
    and pm_pos.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI
    and pm_pos.effectiveenddate > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
    inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                from AIA_CB_RULES_LOOKUP
               where RULE_TYPE = 'PM'
                AND CLAWBACK_NAME in ('SPI_FA_ONG','SPI_FA_ONG_2.1')) rl -- version 17 Harm_BSC_SPI
     on pm.NAME = rl.SOURCE_RULE_OUTPUT
     inner join aia_tmp_comls_period period
     on pm.periodseq = period.periodseq
     inner join cs_position dep_pos
     on pm.positionseq = dep_pos.ruleelementownerseq
    and dep_pos.removedate = '1-jan-2200'
    and dep_pos.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI
    and dep_pos.effectiveenddate > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI
  inner join cs_title dep_title
     on dep_pos.titleseq = dep_title.ruleelementownerseq
    and dep_title.removedate = '1-jan-2200'
    and dep_title.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI
    and dep_title.effectiveenddate > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) -- version 17 Harm_BSC_SPI
  WHERE to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbpenddate
       AND pm.tenantid='AIAS' and   pm.PROCESSINGUNITSEQ = v_processingunitseq;

Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI FA Ongoing ' || '; row count: ' || to_char(sql%rowcount));
commit;

 -- version 17 start Harm_BSC_SPI
-- delete SPI FA 2.2 Ongoing records but agent is FA 2.1
delete from AIA_CB_TRACE_FORWARD_COMP cb
where exists
(
     select 1
     from AIA_CB_TRACE_FORWARD_COMP tf
     inner join cs_position cp
        on cp.ruleelementownerseq = tf.PAYEE_SEQ
        and cp.tenantid='AIAS' -- Added by Sundeep
        AND cp.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND cp.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND cp.removedate = DT_REMOVEDATE
     inner join cs_gaparticipant pa
        on cp.payeeseq = pa.payeeseq
        and pa.tenantid='AIAS' -- Added by Sundeep
        AND pa.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND pa.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND pa.removedate = DT_REMOVEDATE
        and pa.genericboolean2 = 1
     where tf.BUNAME = cb.BUNAME
     and tf.CLAWBACK_TYPE = cb.CLAWBACK_TYPE
     and tf.PAYOR_CODE = cb.PAYOR_CODE
     and tf.POLICY_NUMBER = cb.POLICY_NUMBER
     and tf.LIFE_NUMBER = cb.LIFE_NUMBER
     and tf.COVERAGE_NUMBER = cb.COVERAGE_NUMBER
     and tf.RIDER_NUMBER = cb.RIDER_NUMBER
     and tf.COMPONENT_CODE = cb.COMPONENT_CODE
     and tf.batch_number = P_BATCH_NO
     and cb.batch_number = P_BATCH_NO
     and tf.CLAWBACK_NAME = 'SPI_FA_ONG'
     and cb.CLAWBACK_NAME = 'SPI_FA_ONG'
);

Log('delete AIA_CB_TRACE_FORWARD_COMP  duplicate records for SPI FA Ongoing 2.1' || '; row count: ' || to_char(sql%rowcount));
commit;

-- update old agent code for old bize pay to new code
merge into AIA_CB_TRACE_FORWARD_COMP tf
using (SELECT distinct
         ip.WRI_AGT_CODE,
         ip.ponumber ,
         ip.COMPONENT_CODE ,
         ip.FAOB_AGT_CODE,
	 ip.POLICYIDSEQ
       FROM AIA_CB_IDENTIFY_POLICY  ip
      inner join AIA_CB_TRACE_FORWARD_COMP ctfc
       on ip.POLICYIDSEQ = ctfc.POLICYIDSEQ
       where ip.FAOB_AGT_CODE is not null
       and ip.BUNAME = STR_BUNAME_FA
       and ctfc.BATCH_NUMBER = P_BATCH_NO
       and ip.quarter || ' ' || ip.year = ctfc.CALCULATION_PERIOD ) temp
  on (tf.POLICY_NUMBER = temp.ponumber
      and tf.COMPONENT_CODE = temp.COMPONENT_CODE
      and tf.BUNAME = STR_BUNAME_FA
      and tf.BATCH_NUMBER = P_BATCH_NO
      and tf.POLICYIDSEQ = temp.POLICYIDSEQ)
  when matched then update
  set tf.OLD_AGENT_CD = temp.WRI_AGT_CODE;

Log('update AIA_CB_TRACE_FORWARD_COMP old agent code for SPI FA Ongoing 2.1' || '; row count: ' || to_char(sql%rowcount));
commit;
 -- version 17 end Harm_BSC_SPI

end if;
--Version 2 end

/*
Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI Ongoing, ' || 'clawback type = ' || P_STR_TYPE ||', batch_no = ' || P_BATCH_NO);

--for SPI on-going
--for on-going compensation trace forward
insert into AIA_CB_TRACE_FORWARD_COMP
  with cb_period as
   (select p.periodseq, p.name
      from cs_period a
     inner join cs_period p
        on p.startdate >= a.startdate
       and p.enddate <= a.enddate
     inner join cs_periodtype cpt_qtr
        on a.periodtypeseq = cpt_qtr.periodtypeseq
       and cpt_qtr.name = 'quarter'
     inner join cs_periodtype cpt_mon
        on p.periodtypeseq = cpt_mon.periodtypeseq
       and cpt_mon.name = 'month'
     where a.calendarseq = V_CALENDARSEQ
       and p.calendarseq = V_CALENDARSEQ
       and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) between a.startdate and
           (a.enddate - 1)
       and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= p.startdate)
  select STR_BUNAME as BUNAME,
         ip.quarter || ' ' || ip.year as CALCULATION_PERIOD,
         ip.ponumber as POLICY_NUMBER,
         ip.policyidseq as POLICYIDSEQ,
         pm.positionseq as PAYEE_SEQ,
         substr(pm_pos.name, 4) as PAYEE_CODE,
         crd.genericattribute12 as PAYOR_CODE,
         ip.life_number as LIFE_NUMBER,
         ip.coverage_number as COVERAGE_NUMBER,
         ip.rider_number as RIDER_NUMBER,
         ip.component_code as COMPONENT_CODE,
         ip.component_name as COMPONENT_NAME,
         ip.base_rider_ind as BASE_RIDER_IND,
         crd.compensationdate as TRANSACTION_DATE,
         substr(ONGOING_PERIOD, 1, 3) || '-' || substr(ONGOING_PERIOD, -4) as PROCESSING_PERIOD,
         STR_ONGOING as CLAWBACK_TYPE,
         rl.CLAWBACK_NAME as CLAWBACK_NAME,
         STR_CB_NAME as CLAWBACK_METHOD,
         ct.credittypeid as CREDITTYPE,
         crd.creditseq as CREDITSEQ,
         crd.name as CREDIT_NAME,
         crd.value as CREDIT_VALUE,
         crd.positionseq as crd_positionseq,
         st.genericdate2 as crd_genericdate2,
         crd.ruleseq as crd_ruleseq,
         pm.measurementseq as PM_SEQ,
         pm.name as PM_NAME,
         pct.contributionvalue as PM_CONTRIBUTION_VALUE,
         1 as PM_RATE,
         '' as DEPOSITSEQ,
         '' as DEPOSIT_NAME,
         '' as DEPOSIT_VALUE,
         crd.periodseq as PERIODSEQ,
         st.salestransactionseq as SALESTRANSACTIONSEQ,
         crd.genericattribute2 as PRODUCT_NAME,
         crd.genericnumber1 as POLICY_YEAR,
         st.genericnumber2 as COMMISSION_RATE,
         st.genericdate4 as PAID_TO_DATE,
         P_BATCH_NO as BATCH_NUMBER,
         sysdate as CREATED_DATE
    FROM CS_SALESTRANSACTION st
   inner join CS_CREDIT crd
      on st.SALESTRANSACTIONSEQ = crd.SALESTRANSACTIONSEQ
   inner join CS_PMCREDITTRACE pct
      on crd.CREDITSEQ = pct.CREDITSEQ
   inner join CS_MEASUREMENT pm
      on pct.MEASUREMENTSEQ = pm.MEASUREMENTSEQ
   inner join CS_POSITION pm_pos
      on pm.positionseq = pm_pos.ruleelementownerseq
     and pm_pos.removedate = DT_REMOVEDATE
     and pm_pos.effectivestartdate <= crd.genericdate2
     and pm_pos.effectiveenddate > crd.genericdate2
   inner join CS_CREDITTYPE ct
      on crd.CREDITTYPESEQ = ct.DATATYPESEQ
     and ct.Removedate = DT_REMOVEDATE
   inner join cb_period
      on pm.periodseq = cb_period.periodseq
     and crd.periodseq = cb_period.periodseq
   inner join AIA_CB_IDENTIFY_POLICY ip
      on st.PONUMBER = ip.PONUMBER
     AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
     AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
     AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
     AND st.PRODUCTID = ip.COMPONENT_CODE
     and crd.genericattribute12 = ip.wri_agt_code
   inner join (select distinct SOURCE_RULE_OUTPUT, CLAWBACK_NAME
                 from AIA_CB_RULES_LOOKUP
                where RULE_TYPE = 'PM'
                  AND CLAWBACK_NAME = 'SPI_ONG') rl
      on pm.NAME = rl.SOURCE_RULE_OUTPUT
   inner join cs_position dep_pos
      on pm.positionseq = dep_pos.ruleelementownerseq
     and dep_pos.removedate = DT_REMOVEDATE
     and dep_pos.effectivestartdate <= crd.genericdate2
     and dep_pos.effectiveenddate > crd.genericdate2
  --for lookup the receiver info.
   inner join cs_title dep_title
      on dep_pos.titleseq = dep_title.ruleelementownerseq
     and dep_title.removedate = DT_REMOVEDATE
     and dep_title.effectivestartdate <= crd.genericdate2
     and dep_title.effectiveenddate > crd.genericdate2
   inner join (select distinct
                      cb_quarter_name,
                      cb_startdate,
                      cb_enddate
                 from aia_cb_period
                where cb_name = STR_COMPENSATION) cbp
      on ip.quarter || ' ' || ip.year = cbp.cb_quarter_name
   WHERE st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
     AND st.BUSINESSUNITMAP = 1
     --to avoid fetching the transactions which not being processed by lumpsum procedure
     and to_date(P_STR_CYCLEDATE,STR_DATE_FORMAT_TYPE) > cbp.cb_enddate
     ;

Log('insert into AIA_CB_TRACE_FORWARD_COMP for SPI Ongoing' || '; row count: ' || to_char(sql%rowcount));

commit;*/

end if;

<<ProcDone>>
NULL;

end  SP_TRACE_FORWARD_COMP_FA;


PROCEDURE SP_CLAWBACK_COMP_FA (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO IN INTEGER) as

STR_LUMPSUM CONSTANT VARCHAR2(20) := 'LUMPSUM';
STR_ONGOING CONSTANT VARCHAR2(20) := 'ONGOING';
STR_BUNAME_FA CONSTANT VARCHAR2(20) := 'SGPAFA';
STR_DATE_FORMAT CONSTANT VARCHAR2(50) := 'yyyymmdd';
STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION';
STR_CALENDARNAME  CONSTANT VARCHAR2(50) := 'AIA Singapore Calendar';
V_CAL_PERIOD VARCHAR2(30); --measurement quarter
DT_REMOVEDATE CONSTANT DATE := TO_DATE('22000101', 'yyyymmdd');
DT_CB_START_DATE DATE;
DT_CB_END_DATE DATE;
DT_INCEPTION_START_DATE DATE;
DT_INCEPTION_END_DATE DATE;
DT_WEEKLY_START_DATE DATE;
DT_WEEKLY_END_DATE DATE;
DT_ONGOING_START_DATE DATE;
DT_ONGOING_END_DATE DATE;
NUM_OF_CYCLE_IND integer;
STR_DATE_FORMAT_TYPE    CONSTANT VARCHAR2(50) := 'yyyy-mm-dd';
V_REC_COUNT INTEGER;
V_NLPI_RATE NUMBER(10,2);
INT_SVI_RATE NUMBER(10,2) := 0.60;
V_BATCH_NO_PRE_QTR INTEGER;
V_CB_TYPE VARCHAR2(50);
V_CB_NAME VARCHAR2(50);
STR_STATUS_COMPLETED_SH CONSTANT VARCHAR2(20) := 'completed_sh';
V_CB_QTR VARCHAR2(50);
--Version 2 add by Amanda begin
V_Curr_QTR VARCHAR2(30);
V_Previous_QTR VARCHAR2(30);
--Version 2 end
begin


Log('SP_CLAWBACK_COMP for FA start');

init;

--version 7 start

execute immediate 'truncate table AIA_CB_BSC_LEADER_TMP';

Log('P_STR_CYCLEDATE:  '||P_STR_CYCLEDATE);
Log('P_BATCH_NO:  '||P_BATCH_NO);

  --update leader agency for ongoing only
  insert into AIA_CB_BSC_LEADER_TMP
  (YEAR,
   QUARTER,
   FSC_CODE,
   LEADER_CODE,
   ENTITLEMENT,
   LEADER_AGENCY
   )
   SELECT  ldr.YEAR,
           ldr.QUARTER,
           ldr.FSC_CODE,
           ldr.LEADER_CODE,
           ldr.ENTITLEMENT,
           pos.GENERICATTRIBUTE1 AS LEADER_AGENCY
   FROM AIA_CB_BSC_LEADER ldr,
        cs_position pos
  where pos.effectivestartdate <= to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
   and pos.effectiveenddate   > to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
   and pos.removedate=DT_REMOVEDATE
   and 'SGT'||ldr.LEADER_CODE = pos.NAME
   ;

  Log('Update leader agency code for special rate ' || '; row count: ' || to_char(sql%rowcount));
commit;


--version 7 end

--get records count from AIA_CB_CLAWBACK_COMP
select count(1)
  into V_REC_COUNT
  from AIA_CB_CLAWBACK_COMP
 where batch_no = P_BATCH_NO;

--delete the records in AIA_CB_CLAWBACK_COMP if batch number is being reused.
if V_REC_COUNT > 0 then

delete from AIA_CB_CLAWBACK_COMP where batch_no = P_BATCH_NO;
delete from AIA_CB_CLAWBACK_SVI_COMP_TMP where batch_no = P_BATCH_NO;

commit;

END IF;

--Version 2 add by Amanda for SPI FA CB begin
delete from AIA_CB_SPI_CLAWBACK cb
where exists
(
     select 1
     from AIA_CB_TRACE_FORWARD_COMP tf
     inner join cs_period csp
       on csp.periodseq = tf.YTD_MPERIOD
      and csp.removedate = DT_REMOVEDATE
      and csp.periodtypeseq = V_periodtype_month_seq --month
      and csp.calendarseq = V_CALENDARSEQ --2251799813685250
     inner join cs_period qtr
       on csp.parentseq = qtr.periodseq
      and qtr.removedate = DT_REMOVEDATE
      and qtr.calendarseq = V_CALENDARSEQ --2251799813685250
      and qtr.periodtypeseq = V_periodtype_quarter_seq --quarter
     Where tf.BUNAME = cb.BUNAME
     and tf.CLAWBACK_TYPE = cb.CLAWBACK_TYPE
     and tf.PAYOR_CODE = cb.WRI_AGT_CODE
     and tf.POLICY_NUMBER = cb.PONUMBER
     and tf.LIFE_NUMBER = cb.LIFE_NUMBER
     and tf.COVERAGE_NUMBER = cb.COVERAGE_NUMBER
     and tf.RIDER_NUMBER = cb.RIDER_NUMBER
     and tf.COMPONENT_CODE = cb.COMPONENT_CODE
     and qtr.name = (cb.quarter || ' ' || cb.YEAR) --get current quarter to delete
     and tf.clawback_name in ('SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1') -- version 17 Harm_BSC_SPI
     and tf.batch_number = P_BATCH_NO
)
and cb.BUNAME = STR_BUNAME_FA;

Log('delete from AIA_CB_SPI_CLAWBACK for FA ' || '; row count: ' || to_char(sql%rowcount));
commit;

select min(year_qtr) into V_First_QTR from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null;
select max(year_qtr) into V_Second_QTR from AIA_TMP_COMLS_PERIOD_SPI where year_qtr is not null;

insert into AIA_CB_SPI_CLAWBACK(
          YEAR, QUARTER, BUNAME, PONUMBER, LIFE_NUMBER, COVERAGE_NUMBER,
          RIDER_NUMBER, COMPONENT_CODE, WRI_AGT_CODE, CLAWBACK_TYPE,
          PROCESSING_PERIOD, SPI_RATE, YTD_PIB, YTD_SPI_CB, SPI_CB)
select substr(qtr.name,4,4) as year,
       substr(qtr.name,1,2) as qtr,
       tf.buname,
       tf.policy_number,
       tf.LIFE_NUMBER,
       tf.COVERAGE_NUMBER,
       tf.RIDER_NUMBER,
       tf.COMPONENT_CODE,
       tf.payor_code WRI_AGT_CODE,
       tf.CLAWBACK_TYPE,
       TO_CHAR(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE),'YYYYMM') PROCESSING_PERIOD,
       max(tf.PM_RATE), --SPI Rate
       sum((case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 -
             case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 * ba.entitlementpercent) * (-1)),--YTD PIB
       sum((case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 -
             case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE), --YTD SPI CB = current QTR YTD SPI CB
       case when tf.clawback_name = 'SPI_FA' then
               sum((case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 -
             case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE)
             when tf.clawback_name = 'SPI_FA_ONG' then sum((case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 -
             case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.60 * ba.entitlementpercent) * (-1))
             end SPI_CB  --SPI CB
       from AIA_CB_TRACE_FORWARD_COMP tf
        inner join aia_cb_bsc_agent ba
          on tf.calculation_period = (ba.quarter || ' ' || ba.year)
         and tf.payor_code = ba.agentcode
        inner join cs_period csp
          on csp.periodseq = tf.YTD_MPERIOD
         and csp.removedate = DT_REMOVEDATE
         and csp.periodtypeseq = V_periodtype_month_seq --month
         and csp.calendarseq = V_CALENDARSEQ
       inner join cs_period qtr
          on csp.parentseq = qtr.periodseq
         and qtr.removedate = DT_REMOVEDATE
         and qtr.calendarseq = V_CALENDARSEQ
         and qtr.periodtypeseq = V_periodtype_quarter_seq --quarter
         where tf.clawback_name in ('SPI_FA','SPI_FA_ONG')
           and tf.batch_number = P_BATCH_NO
           --Version 13 update by Amanda for AGY SPI exclusion begin
           and tf.component_code not in (
           select b.MINSTRING
           from CS_RELATIONALMDLT a
           inner join CS_MDLTIndex b on a.ruleelementseq = b.ruleelementseq
             and a.removedate = DT_REMOVEDATE and b.removedate = DT_REMOVEDATE
             and a.name = 'LT_SPI_Bonus_Component_Excl')
           --Version 13 end
        group by qtr.name,
                 tf.buname,
                 tf.policy_number,
                 tf.LIFE_NUMBER,
                 tf.COVERAGE_NUMBER,
                 tf.RIDER_NUMBER,
                 tf.COMPONENT_CODE,
                 tf.payor_code,
                 tf.CLAWBACK_TYPE,
                 tf.clawback_name;

-- version 17 Harm_BSC_SPI start
insert into AIA_CB_SPI_CLAWBACK(
          YEAR, QUARTER, BUNAME, PONUMBER, LIFE_NUMBER, COVERAGE_NUMBER,
          RIDER_NUMBER, COMPONENT_CODE, WRI_AGT_CODE, CLAWBACK_TYPE,
          PROCESSING_PERIOD, SPI_RATE, YTD_PIB, YTD_SPI_CB, SPI_CB)
select substr(qtr.name,4,4) as year,
       substr(qtr.name,1,2) as qtr,
       tf.buname,
       tf.policy_number,
       tf.LIFE_NUMBER,
       tf.COVERAGE_NUMBER,
       tf.RIDER_NUMBER,
       tf.COMPONENT_CODE,
       tf.payor_code WRI_AGT_CODE,
       tf.CLAWBACK_TYPE,
       TO_CHAR(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE),'YYYYMM') PROCESSING_PERIOD,
       max(tf.PM_RATE),
       sum((case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 -
            case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 * ba.entitlementpercent) * (-1)),--YTD PIB
       sum((case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 -
            case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE), --YTD SPI CB = current QTR YTD SPI CB
       case when tf.clawback_name = 'SPI_FA_2.1' then sum((case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 -
            case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 * ba.entitlementpercent) * (-1) * tf.PM_RATE)
            when tf.clawback_name = 'SPI_FA_ONG_2.1' then sum((case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 -
            case when pm_name = 'PM_FYC_SPI_Bonus_Excl' then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
            end * 0.60 * ba.entitlementpercent) * (-1))
            end SPI_CB  --SPI CB
       --Version 13 end
       from AIA_CB_TRACE_FORWARD_COMP tf
        inner join aia_cb_bsc_agent ba
          on tf.calculation_period = (ba.quarter || ' ' || ba.year)
          and ba.agentcode =  (case when tf.OLD_AGENT_CD is not null then tf.OLD_AGENT_CD else tf.payor_code end)
        inner join cs_period csp
          on csp.periodseq = tf.YTD_MPERIOD
         and csp.removedate = DT_REMOVEDATE
         and csp.periodtypeseq = V_periodtype_month_seq --month
         and csp.calendarseq = V_CALENDARSEQ
       inner join cs_period qtr
          on csp.parentseq = qtr.periodseq
         and qtr.removedate = DT_REMOVEDATE
         and qtr.calendarseq = V_CALENDARSEQ
         and qtr.periodtypeseq = V_periodtype_quarter_seq --quarter
         where tf.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1')
           and tf.batch_number = P_BATCH_NO
           and tf.component_code not in (
           select b.MINSTRING
           from CS_RELATIONALMDLT a
           inner join CS_MDLTIndex b on a.ruleelementseq = b.ruleelementseq
             and a.removedate = DT_REMOVEDATE and b.removedate = DT_REMOVEDATE
             and a.name = 'LT_SPI_Bonus_Component_Excl')
        group by qtr.name,
                 tf.buname,
                 tf.policy_number,
                 tf.LIFE_NUMBER,
                 tf.COVERAGE_NUMBER,
                 tf.RIDER_NUMBER,
                 tf.COMPONENT_CODE,
                 tf.payor_code,
                 tf.CLAWBACK_TYPE,
                 tf.clawback_name;
-- version 17 Harm_BSC_SPI  end

Log('insert into AIA_CB_SPI_CLAWBACK FA 1st QTR' || '; row count: ' || to_char(sql%rowcount));
commit;
--Version 2 end

Log('insert into AIA_CB_CLAWBACK_COMP FA for FYO,RYO, NADOR' ||' batch_no = ' || P_BATCH_NO);

--insert data into AIA_CB_CLAWBACK_COMP for compensation for FYO, RYO and NADOR
insert into AIA_CB_CLAWBACK_COMP
  select tf.calculation_period as MEASUREMENT_QUARTER,
         tf.clawback_type as CLAWBACK_TYPE,
         tf.clawback_name as CLAWBACK_NAME,
         STR_CB_NAME as CLAWBACK_METHOD,
           to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
         Agency_code.genericattribute3 as WRI_DIST_CODE,
         trim(District_name.firstname||' '||District_name.lastname) as WRI_DIST_NAME,
         DM_code.genericattribute2 as  WRI_DM_CODE,
           --substr(pos_agy.name, 4) as WRI_AGY_CODE,
         --pos_agy.genericattribute1 as WRI_AGY_CODE,
         agent.genericattribute1 as WRI_AGY_CODE,
--         trim(par_agy.firstname||' '||par_agy.lastname) as WRI_AGY_NAME,
trim(Agency_name.firstname||' '||Agency_name.lastname)  as  WRI_AGY_NAME,
         agent.genericattribute2 as wri_agy_ldr_code,
         agent.genericattribute7 as wri_agy_ldr_name,
         tf.payor_code as WRI_AGT_CODE,
         trim(Agent_name.firstname||' '||Agent_name.lastname)  as wri_agt_name  ,
         decode(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
         title_agt.name as RANK,
          agent.genericattribute4 as UM_CLASS,
         agent.genericattribute11 as UM_RANK, -- Check cr.genericattribute14 as CLASS,
         ba.bsc_grade as FSC_BSC_GRADE,
         --ba.entitlementpercent as FSC_BSC_PERCENTAGE,
         nvl(ldr.ENTITLEMENT,ba.entitlementpercent) as FSC_BSC_PERCENTAGE, --version 7 add
         tf.policy_number as PONUMBER,
         tf.life_number as LIFE_NUMBER,
         tf.coverage_number as COVERAGE_NUMBER,
         tf.RIDER_NUMBER as RIDER_NUMBER,
         tf.COMPONENT_NAME as COMPONENT_NAME,
         tf.component_code as COMPONENT_CODE,
         tf.PRODUCT_NAME as PRODUCT_NAME,
         tf.transaction_date as TRANSACTION_DATE,
         tf.policy_year as POLICY_YEAR,
          case
           when tf.credit_type  in ('FYC','FYC_W','FYC_W_DUPLICATE') then
            tf.credit_value
           else
            0
         end as FYC,
         case
           when tf.credit_type in ('API','API_W','API_W_DUPLICATE') then
            tf.credit_value
           else
            0
         end as API,
         case
           when tf.credit_type  in ('SSCP','SSCP_W','SSCP_W_DUPLICATE') then
            tf.credit_value
           else
            0
         end as SSC,
         case
           when tf.credit_type  in ('RYC','RYC_W','RYC_W_DUPLICATE','ORYC_W','ORYC_W_DUPLICATE') then
            tf.credit_value
           else
            0
         end as RYC,
         case
           when tf.clawback_name  in ('FYO_FA','FYO_FA_ONG'
             --verstion 13 Harm_Phase4 Strat
             ,'FA_FYO_2.1','FA_FYO_ONG_2.1'
             --verstion 13 Harm_Phase4 End
             ) then
            tf.pm_contribution_value
           else
            0
         end as FYO,
         case
           when tf.clawback_name  in ('RYO_FA','RYO_FA_ONG'
             --verstion 13 Harm_Phase4 Start
             ,'FA_RYO_2.1','FA_RYO_ONG_2.1'
             --verstion 13 Harm_Phase4 End
             ) then    --version 11
            tf.pm_contribution_value
           else
            0
         end as RYO,
          case when tf.clawback_name   in ('FSM_RYO_FA','FSM_RYO_ONG_FA') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
   when tf.clawback_name   in ('FSM_RYO_FA','FSM_RYO_ONG_FA')  then tf.pm_contribution_value  else 0 end  as FSM_RYO,
          case
           when tf.clawback_name  ='NADOR_FA_2.1' then
            tf.pm_contribution_value
           else
            0
         end as NADOR,
   case when tf.clawback_name in ('NLPI_FA','NLPI_ONG_FA')and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
when  tf.clawback_name in ('NLPI','NLPI_ONG') then   tf.pm_contribution_value else 0 end as NLPI,
         0 as SPI,
      case
       when tf.clawback_name  in ('NLPI_FA','NLPI_ONG_FA') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
       when tf.clawback_name   in ('FSM_RYO_FA','FSM_RYO_ONG_FA') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
       else  tf.pm_contribution_value end *0.60 as SVI,
       case
             when tf.clawback_name  in ('NLPI_FA','NLPI_ONG_FA') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
       when tf.clawback_name   in ('FSM_RYO_FA','FSM_RYO_ONG_FA') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
       else  tf.pm_contribution_value end *0.60* /*ba.entitlementpercent version 7*/ nvl(ldr.ENTITLEMENT,ba.entitlementpercent) as ENTITLEMENT,
         round((( case when tf.clawback_name  in ('NLPI_FA','NLPI_ONG_FA') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
       when tf.clawback_name   in ('FSM_RYO_FA','FSM_RYO_ONG_FA') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
       else  tf.pm_contribution_value end *0.60) -
         (case when tf.clawback_name  in ('NLPI_FA','NLPI_ONG_FA') and pm_name in  ('PM_NLPI_PIB_Exclusion','PM_NLPI_PIB_Exclusion_NEW') then -1*tf.pm_contribution_value
       when tf.clawback_name   in ('FSM_RYO_FA','FSM_RYO_ONG_FA') and pm_name='PM_RYO_LIFE_FSM_DIRECT_TEAM_Exclude_SGPAGY' then -1 *tf.pm_contribution_value
       else  tf.pm_contribution_value end *0.60* /*ba.entitlementpercent version 7*/ nvl(ldr.ENTITLEMENT,ba.entitlementpercent) ))* (-1),2) as CLAWBACK_VALUE,
         0 as PROCESSED_CLAWBACK,
          tf.base_rider_ind as BASIC_RIDER_IND,
         tf.salestransactionseq,
         tf.creditseq,
         tf.pm_seq,
         P_BATCH_NO,
         pos_agy_rcr.GENERICATTRIBUTE2 as RCVR_AGY_LDR_CODE ,
         pos_agy_rcr.genericattribute11 as RCVR_AGY_LDR_RANK,
         case rul.EXPRESSIONTYPEFORTYPE
             when 256 then 'DIRECT'
             when 1024 then 'INDIRECT'
            else '0'
         end as REPORT_TYPE,
        --Added by Suresh
        0 as OFFSET_CLAWBACK,
         case
           when tf.clawback_name  in ('NEW_FYO_FA','NEW_FYO_ONG_FA') then
            tf.pm_contribution_value
           else
            0
         end as NEW_FYO,
           case
           when tf.clawback_name  in ('NEW_RYO_FA','NEW_RYO_ONG_FA') then
            tf.pm_contribution_value
           else
            0
         end as NEW_RYO,
         --End by Suresh
         --add AI NL20180308
         case
   --verstion 13 start, changed from AI_ONG to FA_AI_ONG_2.1 as the AI_ONG is useless
          when tf.clawback_name  in ('FA_AI_2.1','FA_AI_ONG_2.1') then
   --verstion 13 end
            tf.credit_value
           else
            0
         end as AI
         ,NULL AS YTD_PERIOD --Vesion 13 add by Amanda
    from AIA_CB_TRACE_FORWARD_COMP tf
   inner join aia_cb_bsc_agent ba
      on tf.calculation_period = (ba.quarter || ' ' || ba.year)
     and (tf.payor_code = ba.agentcode or ba.agentcode=tf.new_agent_cd or ba.agentcode=tf.old_agent_cd)
   inner join CS_CREDIT cr
      on tf.creditseq = cr.creditseq
      and tf.periodseq=cr.periodseq -- Added by Sundeep
   inner join cs_rule rul
     on cr.ruleseq=rul.ruleseq
     and rul.REMOVEDATE=DT_REMOVEDATE
     and rul.islast = 1
      inner join cs_position pos_agy_rcr
        on pos_agy_rcr.ruleelementownerseq = cr.positionseq
        and pos_agy_rcr.tenantid= 'AIAS' -- Added by Sundeep
        /*AND pos_agy_rcr.removedate = DT_REMOVEDATE
        and pos_agy_rcr.islast = 1 */
            AND pos_agy_rcr.effectivestartdate <= tf.CRD_GENERICDATE2
        AND pos_agy_rcr.effectiveenddate   >  tf.CRD_GENERICDATE2
        AND pos_agy_rcr.removedate = DT_REMOVEDATE
 inner join cs_position Agent
        on Agent.name = 'SGT'||tf.payor_code
         and Agent.tenantid= 'AIAS' -- Added by Sundeep
        AND Agent.effectivestartdate <= tf.CRD_GENERICDATE2
        AND Agent.effectiveenddate   >  tf.CRD_GENERICDATE2
        AND Agent.removedate = DT_REMOVEDATE
--        and AGENT.GENERICATTRIBUTE6='AFA'
inner join cs_participant Agent_name
        on Agent.payeeseq = Agent_name.payeeseq
          and Agent_name.tenantid= 'AIAS' -- Added by Sundeep
        AND Agent_name.effectivestartdate <= tf.CRD_GENERICDATE2
        AND Agent_name.effectiveenddate   > tf.CRD_GENERICDATE2
        AND Agent_name.removedate = DT_REMOVEDATE
     inner join cs_position Agency_code
        on 'SGY'||agent.genericattribute1 = Agency_code.name
        and Agency_code.tenantid= 'AIAS' -- Added by Sundeep
        AND Agency_code.effectivestartdate <= tf.CRD_GENERICDATE2
        AND Agency_code.effectiveenddate   > tf.CRD_GENERICDATE2
        AND Agency_code.removedate = DT_REMOVEDATE
             inner join cs_participant Agency_name
       on Agency_code.payeeseq = Agency_name.payeeseq
       and Agency_name.effectivestartdate <= tf.CRD_GENERICDATE2
        AND Agency_name.effectiveenddate   > tf.CRD_GENERICDATE2
        AND Agency_name.removedate = DT_REMOVEDATE
 inner join cs_position DM_code
        --on 'SGY'||agent.genericattribute3 = DM_code.name
        on 'SGY'||Agency_code.genericattribute3 = DM_code.name
           and DM_code.tenantid= 'AIAS' -- Added by Sundeep
        AND DM_code.effectivestartdate <= tf.CRD_GENERICDATE2
        AND DM_code.effectiveenddate   > tf.CRD_GENERICDATE2
        AND DM_code.removedate = DT_REMOVEDATE
  inner join cs_participant District_name
       on dm_code.payeeseq = district_name.payeeseq
          and District_name.tenantid= 'AIAS' -- Added by Sundeep
       and District_name.effectivestartdate <= tf.CRD_GENERICDATE2
        AND District_name.effectiveenddate   > tf.CRD_GENERICDATE2
        AND District_name.removedate = DT_REMOVEDATE
         inner join cs_title title_agt
     on title_agt.RULEELEMENTOWNERSEQ = Agent.TITLESEQ
     AND title_agt.effectivestartdate <= tf.CRD_GENERICDATE2
     AND title_agt.effectiveenddate   > tf.CRD_GENERICDATE2
     AND title_agt.REMOVEDATE = DT_REMOVEDATE
     --version 7 add
left join AIA_CB_BSC_LEADER_TMP ldr
       on tf.calculation_period = (ldr.quarter || ' '  || ldr.year)
      and (tf.payee_code = ldr.LEADER_CODE or tf.payee_code = ldr.LEADER_AGENCY)  --for ongoing, payee code is agency code get from measurement
      and tf.payor_code=ldr.FSC_CODE
     --version 7 end
   --chaned by Suresh
   --add AI NL20180308
   where tf.clawback_name in ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION'
   --version13 Harm_Phase4 Start
   ,'FA_FYO_2.1'
   ,'FA_FYO_ONG_2.1'
   ,'FA_RYO_2.1'
   ,'FA_RYO_ONG_2.1'
   ,'NADOR_FA_2.1'
   ,'FA_AI_2.1'
   ,'FA_AI_ONG_2.1'
   --version13  Harm Phase4 End
   )
   --End by Suresh
   and tf.batch_number = P_BATCH_NO
   ;

Log('insert into AIA_CB_CLAWBACK_COMP FA for FYO, RYO, NADOR' || '; row count: ' || to_char(sql%rowcount));

commit;


--get clawback type and clawback name, only LUMPSUM case will apply this logic
V_CB_TYPE := fn_get_cb_type(P_BATCH_NO);
--V_CB_NAME := fn_get_cb_name(P_BATCH_NO);
V_CB_QTR := fn_get_cb_quarter(P_BATCH_NO);


if V_CB_TYPE = STR_LUMPSUM then
   --get previous quarter batch number
    --V_BATCH_NO_PRE_QTR := fn_get_batch_no_pre_qtr(P_BATCH_NO);

--Version 2 add by Amanda begin
   --only same year need to update SPI CB for second QTR
  merge into AIA_CB_SPI_CLAWBACK cb2
  using(select cb.BUNAME, cb.PONUMBER, cb.WRI_AGT_CODE, cb.YTD_PIB, cb.YTD_SPI_CB, cb.YEAR, cb.quarter, cb.CLAWBACK_TYPE,
            cb.LIFE_NUMBER, cb.COVERAGE_NUMBER, cb.RIDER_NUMBER, cb.COMPONENT_CODE
          from AIA_CB_SPI_CLAWBACK cb
         where cb.BUNAME = STR_BUNAME_FA
           and cb.CLAWBACK_TYPE = 'LUMPSUM'
           and cb.year = substr(V_First_QTR,1,4)
           and cb.quarter = 'Q'||substr(V_First_QTR,6,1)
           ) cb1
  on (  cb2.BUNAME = cb1.BUNAME
    and cb1.WRI_AGT_CODE = cb2.WRI_AGT_CODE
    and cb1.PONUMBER = cb2.PONUMBER
    and cb1.CLAWBACK_TYPE = cb2.CLAWBACK_TYPE
    and cb1.LIFE_NUMBER = cb2.LIFE_NUMBER
    and cb1.COVERAGE_NUMBER = cb2.COVERAGE_NUMBER
    and cb1.RIDER_NUMBER = cb2.RIDER_NUMBER
    and cb1.COMPONENT_CODE = cb2.COMPONENT_CODE
    and cb1.year = cb2.year)
  when matched then update
  set --cb2.YTD_PIB = cb2.YTD_PIB + cb1.YTD_PIB,
      cb2.YTD_SPI_CB = case when cb2.SPI_RATE = 0 then cb1.YTD_SPI_CB else cb2.YTD_SPI_CB end,--special handle for new agent in Q1, not new agent in Q2
      cb2.SPI_CB = (case when cb2.SPI_RATE = 0 then cb1.YTD_SPI_CB else cb2.YTD_SPI_CB end) - cb1.YTD_SPI_CB
  where cb2.BUNAME = STR_BUNAME_FA
    and cb2.CLAWBACK_TYPE = 'LUMPSUM'
   and cb2.year = substr(V_Second_QTR,1,4)
   and cb2.quarter = 'Q'||substr(V_Second_QTR,6,1);

  Log('Merge into AIA_CB_SPI_CLAWBACK FA LUMPSUM 2nd QTR ' || V_Second_QTR || '; row count: ' || to_char(sql%rowcount));
  commit;
--Version 2 end

insert into AIA_CB_CLAWBACK_SVI_COMP_TMP
select curr_cc.*, P_BATCH_NO from
(select wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name,
  clawback_name,
  sum(clawback) as clawback
  from
AIA_CB_CLAWBACK_COMP
where clawback_type = STR_LUMPSUM
 and clawback_method = STR_CB_NAME
 and batch_no = P_BATCH_NO
group by wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name,
  clawback_name
  having sum(clawback) > 0
) curr_cc
left join
(select cc.wri_dist_code,
       cc.wri_agy_code,
       cc.wri_agt_code,
       cc.ponumber,
       cc.life_number,
       cc.coverage_number,
       cc.rider_number,
       cc.component_code,
       cc.product_name,
       cc.clawback_name,
       --processed_clawback value should be updated after pipeline compeleted
       sum(cc.processed_clawback) as processed_clawback
  from AIA_CB_CLAWBACK_COMP cc
 inner join (select nvl(max(t.batchnum), 0) as batch_no
               from aia_cb_batch_status t
               inner join (select distinct quarter, year, cb_startdate, cb_enddate
               from aia_cb_period
              where cb_name =  STR_CB_NAME
              and BUNAME=STR_BUNAME_FA
              ) cbp
              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
              where t.islatest = 'Y'
                and t.status = STR_STATUS_COMPLETED_SH
                and t.clawbackname = STR_CB_NAME
                and t.clawbacktype = STR_LUMPSUM
                and t.cb_quarter_name <> V_CB_QTR
               and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype) pre_batch
 on cc.batch_no = pre_batch.batch_no
 where cc.clawback_type =  STR_LUMPSUM
   and cc.clawback_method =STR_CB_NAME
 group by cc.wri_dist_code,
          cc.wri_agy_code,
          cc.wri_agt_code,
          cc.ponumber,
          cc.life_number,
          cc.coverage_number,
          cc.rider_number,
          cc.component_code,
          cc.product_name,
            cc.clawback_name
having sum(cc.processed_clawback) < 0) pre_cc
 on curr_cc.wri_dist_code = pre_cc.wri_dist_code
 and curr_cc.wri_agy_code = pre_cc.wri_agy_code
 and curr_cc.wri_agt_code = pre_cc.wri_agt_code
 and curr_cc.ponumber = pre_cc.ponumber
 and curr_cc.life_number = pre_cc.life_number
 and curr_cc.coverage_number = pre_cc.coverage_number
 and curr_cc.rider_number = pre_cc.rider_number
 and curr_cc.component_code = pre_cc.component_code
 and curr_cc.product_name = pre_cc.product_name
 and curr_cc.clawback_name = pre_cc.clawback_name
 where pre_cc.ponumber is null;

Log('insert into AIA_CB_CLAWBACK_SVI_COMP_TMP FA for Compensation Lumpsum' || '; row count: ' || to_char(sql%rowcount));

commit;

elsif V_CB_TYPE = STR_ONGOING then

insert into AIA_CB_CLAWBACK_SVI_COMP_TMP
select curr_cc.*, P_BATCH_NO from
(select wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name,
  clawback_name,
  sum(clawback) as clawback
  from
AIA_CB_CLAWBACK_COMP
where clawback_type = STR_ONGOING
 and clawback_method = STR_CB_NAME
 and batch_no = P_BATCH_NO
group by wri_dist_code ,
  wri_agy_code,
  wri_agt_code,
  ponumber,
  life_number,
  coverage_number,
  rider_number,
  component_code,
  product_name,
  clawback_name
  having sum(clawback) > 0
) curr_cc
left join
(select cc.wri_dist_code,
       cc.wri_agy_code,
       cc.wri_agt_code,
       cc.ponumber,
       cc.life_number,
       cc.coverage_number,
       cc.rider_number,
       cc.component_code,
       cc.product_name,
       cc.clawback_name,
       --processed_clawback value should be updated after pipeline compeleted
       sum(cc.processed_clawback) as processed_clawback
  from AIA_CB_CLAWBACK_COMP cc
 inner join (
 --lumpsum batch number
 select nvl(max(t.batchnum), 0) as batch_no
               from aia_cb_batch_status t
               inner join (select distinct quarter, year, cb_startdate, cb_enddate
               from aia_cb_period
              where cb_name = STR_CB_NAME
              and BUNAME=STR_BUNAME_FA
              ) cbp
              on t.cb_quarter_name = cbp.year || ' ' || cbp.quarter
              where t.islatest = 'Y'
              and t.BUNAME=STR_BUNAME_FA
                and t.status = STR_STATUS_COMPLETED_SH
                and t.clawbackname = STR_CB_NAME
                and t.clawbacktype = STR_LUMPSUM
                and t.cb_quarter_name <> V_CB_QTR
                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= cbp.cb_enddate
              group by t.cb_quarter_name, t.clawbackname, t.clawbacktype
              union
  --on-going batch number
              select nvl(max(t.batchnum), 0) as batch_no
               from aia_cb_batch_status t
              where t.islatest = 'Y'
              and t.BUNAME=STR_BUNAME_FA
                and t.status = STR_STATUS_COMPLETED_SH --'completed_sh'
                and t.clawbackname = STR_CB_NAME--'COMMISSION'
                and t.clawbacktype = STR_ONGOING --'ONGOING'
                and to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) > t.cycledate
              ) pre_batch
 on cc.batch_no = pre_batch.batch_no
 where cc.clawback_method = STR_CB_NAME
 group by cc.wri_dist_code,
          cc.wri_agy_code,
          cc.wri_agt_code,
          cc.ponumber,
          cc.life_number,
          cc.coverage_number,
          cc.rider_number,
          cc.component_code,
          cc.product_name,
          cc.clawback_name
having sum(cc.processed_clawback) < 0) pre_cc
 on curr_cc.wri_dist_code = pre_cc.wri_dist_code
 and curr_cc.wri_agy_code = pre_cc.wri_agy_code
 and curr_cc.wri_agt_code = pre_cc.wri_agt_code
 and curr_cc.ponumber = pre_cc.ponumber
 and curr_cc.life_number = pre_cc.life_number
 and curr_cc.coverage_number = pre_cc.coverage_number
 and curr_cc.rider_number = pre_cc.rider_number
 and curr_cc.component_code = pre_cc.component_code
 and curr_cc.product_name = pre_cc.product_name
 and curr_cc.clawback_name = pre_cc.clawback_name
 where pre_cc.ponumber is null;

Log('insert into AIA_CB_CLAWBACK_SVI_COMP_TMP FA for Compensation Ongoing' || '; row count: ' || to_char(sql%rowcount));

commit;

--Version 2 added by Amanda begin
--Fix no data found issue
/*V_REC_COUNT := 0;

--Check quarter end
select count(1)
into V_REC_COUNT
from cs_period csp
 inner join cs_period csp_qtr on csp_qtr.periodtypeseq = V_periodtype_quarter_seq
   and csp_qtr.enddate = csp.enddate
   and csp_qtr.removedate =  DT_REMOVEDATE
 where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
   and csp.removedate =  DT_REMOVEDATE
   and csp.calendarseq = V_CALENDARSEQ
   and csp.periodtypeseq = V_periodtype_month_seq;

if V_REC_COUNT > 0 then
  --get current quarter name
  select csp_qtr.name
  into V_Curr_QTR
  from cs_period csp
   inner join cs_period csp_qtr on csp_qtr.periodtypeseq = V_periodtype_quarter_seq
     and csp_qtr.enddate = csp.enddate
     and csp_qtr.removedate =  DT_REMOVEDATE
   where csp.enddate = to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1
     and csp.removedate =  DT_REMOVEDATE
     and csp.calendarseq = V_CALENDARSEQ
     and csp.periodtypeseq = V_periodtype_month_seq;

  --get previous quarter name
  select csp_qtr.name
  into V_Previous_QTR
  from cs_period csp
   inner join cs_period csp_qtr on csp_qtr.periodtypeseq = V_periodtype_quarter_seq
     and csp_qtr.enddate = csp.enddate
     and csp_qtr.removedate =  DT_REMOVEDATE
   where csp.enddate = add_months(to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) + 1,-3)
     and csp.removedate =  DT_REMOVEDATE
     and csp.calendarseq = V_CALENDARSEQ
     and csp.periodtypeseq = V_periodtype_month_seq;

  Log('Current quarter for FA SPI CB: ' || V_Curr_QTR || 'Previous quarter for FA SPI CB: ' || V_Previous_QTR);

  --update YTD SPI
  merge into AIA_CB_SPI_CLAWBACK cb2
  using(select cb.BUNAME, cb.PONUMBER, cb.WRI_AGT_CODE, cb.YTD_PIB, cb.YEAR, cb.quarter,
            cb.LIFE_NUMBER, cb.COVERAGE_NUMBER, cb.RIDER_NUMBER, cb.COMPONENT_CODE
          from AIA_CB_SPI_CLAWBACK cb
         where cb.BUNAME = STR_BUNAME_FA
           and (cb.quarter || ' ' || cb.year) = V_Previous_QTR
           ) cb1
  on (  cb2.BUNAME = cb1.BUNAME
    and cb1.WRI_AGT_CODE = cb2.WRI_AGT_CODE
    and cb1.PONUMBER = cb2.PONUMBER
    and cb1.LIFE_NUMBER = cb2.LIFE_NUMBER
    and cb1.COVERAGE_NUMBER = cb2.COVERAGE_NUMBER
    and cb1.RIDER_NUMBER = cb2.RIDER_NUMBER
    and cb1.COMPONENT_CODE = cb2.COMPONENT_CODE
    and cb1.year = cb2.year) --only same year to update YTD PIB
  when matched then update
  set cb2.YTD_PIB = cb2.YTD_PIB + cb1.YTD_PIB,
      cb2.SPI_CB = cb2.YTD_PIB + cb1.YTD_PIB --update here for value of PM_SPI_ONG_PIB_CB
  where cb2.BUNAME = STR_BUNAME_FA
    and cb2.CLAWBACK_TYPE = 'ONGOING'
    and (cb2.quarter || ' ' || cb2.year) = V_Curr_QTR;

  Log('Merge into AIA_CB_SPI_CLAWBACK ONGOING 2nd QTR ' || V_Curr_QTR || '; row count: ' || to_char(sql%rowcount));
  commit;

else
  Log('Not quarter end for SPI FA CB:' || P_STR_CYCLEDATE);
end if;*/
--Version 2 end

end if;

--Version 2 update AIA_CB_CLAWBACK_COMP clawback value begin
--insert data into AIA_CB_CLAWBACK_COMP for compensation for SPI FA
insert into AIA_CB_CLAWBACK_COMP
  select tf.calculation_period as MEASUREMENT_QUARTER,
         tf.clawback_type as CLAWBACK_TYPE,
         tf.clawback_name as CLAWBACK_NAME,
         STR_CB_NAME as CLAWBACK_METHOD,
         to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) as CALCULATION_DATE,
         Agency_code.genericattribute3 as WRI_DIST_CODE,
         trim(District_name.firstname||' '||District_name.lastname) as WRI_DIST_NAME,
         DM_code.genericattribute2 as  WRI_DM_CODE,
         agent.genericattribute1 as WRI_AGY_CODE,
         trim(Agency_name.firstname||' '||Agency_name.lastname)  as  WRI_AGY_NAME,
         agent.genericattribute2 as wri_agy_ldr_code,
         agent.genericattribute7 as wri_agy_ldr_name,
         tf.payor_code as WRI_AGT_CODE,
         trim(Agent_name.firstname||' '||Agent_name.lastname)  as wri_agt_name  ,
         decode(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC') as FSC_TYPE,
         title_agt.name as RANK,
         agent.genericattribute4 as UM_CLASS,
         agent.genericattribute11 as UM_RANK,
         ba.bsc_grade as FSC_BSC_GRADE,
         ba.entitlementpercent as FSC_BSC_PERCENTAGE,
         tf.policy_number as PONUMBER,
         tf.life_number as LIFE_NUMBER,
         tf.coverage_number as COVERAGE_NUMBER,
         tf.RIDER_NUMBER as RIDER_NUMBER,
         tf.COMPONENT_NAME as COMPONENT_NAME,
         tf.component_code as COMPONENT_CODE,
         tf.PRODUCT_NAME as PRODUCT_NAME,
         max(tf.transaction_date) as TRANSACTION_DATE,
         max(tf.policy_year) as POLICY_YEAR,
         sum(case when tf.credit_type in ('FYC','FYC_W','FYC_W_DUPLICATE') and pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.credit_value
                  when tf.credit_type in ('FYC','FYC_W','FYC_W_DUPLICATE') then tf.credit_value
                  else 0
             end) as FYC,
         sum(case when tf.credit_type in ('API','API_W','API_W_DUPLICATE') then tf.credit_value
                  else 0
             end) as API,
         sum(case when tf.credit_type in ('SSCP','SSCP_W','SSCP_W_DUPLICATE') then tf.credit_value
                  else 0
             end) as SSC,
         0 as RYC,
         0 as FYO,
         0 as RYO,
         0 as FSM_RYO,
         0 as NADOR,
         0 as NLPI,
         sum(case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end) as SPI,
         sum(case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.6) as SVI,
         sum(case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
                  else tf.pm_contribution_value
             end * 0.6 * ba.entitlementpercent) as ENTITLEMENT,
         round(sum((
         case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
              else tf.pm_contribution_value
              end * 0.6 -
         case when pm_name in ('PM_FYC_LF_PIB_EXCL_ILP-TPU_FA','PM_FYC_SPI_Bonus_Excl') then -1 * tf.pm_contribution_value
              else tf.pm_contribution_value
              end * 0.60 * ba.entitlementpercent) * (-1)),2) as CLAWBACK_VALUE,
         0 as PROCESSED_CLAWBACK,
         tf.base_rider_ind as BASIC_RIDER_IND,
         max(tf.salestransactionseq),
         max(tf.creditseq),
         max(tf.pm_seq),
         P_BATCH_NO,
         pos_agy_rcr.GENERICATTRIBUTE2 as RCVR_AGY_LDR_CODE ,
         pos_agy_rcr.genericattribute11 as RCVR_AGY_LDR_RANK,
         case rul.EXPRESSIONTYPEFORTYPE
             when 256 then 'DIRECT'
             when 1024 then 'INDIRECT'
            else '0'
         end as REPORT_TYPE,
         0 as OFFSET_CLAWBACK,
         0 as NEW_FYO,
         0 as NEW_RYO,
         0 as AI
         ,tf.YTD_MPERIOD --Vesion 13 add by Amanda
    from AIA_CB_TRACE_FORWARD_COMP tf
    inner join aia_cb_bsc_agent ba
      on tf.calculation_period = (ba.quarter || ' ' || ba.year)
      and ba.agentcode =  (case when tf.OLD_AGENT_CD is not null then tf.OLD_AGENT_CD else tf.payor_code end) -- version 17 Harm_BSC_SPI
    inner join cs_period csp
          on csp.periodseq = tf.YTD_MPERIOD
         and csp.removedate = DT_REMOVEDATE
         and csp.periodtypeseq = V_periodtype_month_seq --month
         and csp.calendarseq = V_CALENDARSEQ
    inner join cs_period qtr
          on csp.parentseq = qtr.periodseq
         and qtr.removedate = DT_REMOVEDATE
         and qtr.calendarseq = V_CALENDARSEQ
         and qtr.periodtypeseq = V_periodtype_quarter_seq --quarter
    inner join CS_CREDIT cr
      on tf.creditseq = cr.creditseq
      and tf.periodseq=cr.periodseq --Added by Sundeep
      and cr.tenantid='AIAS'  --Added by Sundeep
      and cr.processingunitseq=38280596832649218  --Added by Sundeep
    inner join cs_rule rul
      on cr.ruleseq=rul.ruleseq
      and rul.REMOVEDATE=DT_REMOVEDATE
      and rul.islast = 1
    inner join cs_position pos_agy_rcr
        on pos_agy_rcr.ruleelementownerseq = cr.positionseq
        and pos_agy_rcr.tenantid='AIAS' -- Added by Sundeep
        AND pos_agy_rcr.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND pos_agy_rcr.effectiveenddate   >  to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND pos_agy_rcr.removedate = DT_REMOVEDATE
	    inner join cs_position Agent
        on Agent.name = 'SGT'||tf.payor_code
        and Agent.tenantid='AIAS' -- Added by Sundeep
        AND Agent.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agent.effectiveenddate   >  to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agent.removedate = DT_REMOVEDATE
    inner join cs_participant Agent_name
        on Agent.payeeseq = Agent_name.payeeseq
        and Agent_name.tenantid='AIAS' -- Added by Sundeep
        AND Agent_name.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agent_name.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agent_name.removedate = DT_REMOVEDATE
     inner join cs_position Agency_code
        on 'SGY'||agent.genericattribute1 = Agency_code.name
        and Agency_code.tenantid='AIAS' -- Added by Sundeep
        AND Agency_code.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agency_code.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agency_code.removedate = DT_REMOVEDATE
     inner join cs_participant Agency_name
       on Agency_code.payeeseq = Agency_name.payeeseq
       and Agency_name.tenantid='AIAS' -- Added by Sundeep
       and Agency_name.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agency_name.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND Agency_name.removedate = DT_REMOVEDATE
 inner join cs_position DM_code
        on 'SGY'||Agency_code.genericattribute3 = DM_code.name
        and DM_code.tenantid='AIAS' -- Added by Sundeep
        AND DM_code.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND DM_code.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND DM_code.removedate = DT_REMOVEDATE
  inner join cs_participant District_name
       on dm_code.payeeseq = district_name.payeeseq
       and District_name.tenantid='AIAS' -- Added by Sundeep
       and District_name.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND District_name.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
        AND District_name.removedate = DT_REMOVEDATE
   inner join cs_title title_agt
     on title_agt.RULEELEMENTOWNERSEQ = Agent.TITLESEQ
     AND title_agt.effectivestartdate <= to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
     AND title_agt.effectiveenddate   > to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE)
     AND title_agt.REMOVEDATE = DT_REMOVEDATE
   where tf.clawback_name in ('SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1')  -- version 17 Harm_BSC_SPI
   and tf.batch_number = P_BATCH_NO
   --Version 13 update by Amanda for AGY SPI exclusion begin
   and tf.component_code not in (
   select b.MINSTRING
   from CS_RELATIONALMDLT a
   inner join CS_MDLTIndex b on a.ruleelementseq = b.ruleelementseq
     and a.removedate = DT_REMOVEDATE and b.removedate = DT_REMOVEDATE
     and a.name = 'LT_SPI_Bonus_Component_Excl')
   --Version 13 end
   group by tf.calculation_period,
         tf.clawback_type,
         tf.clawback_name,
         Agency_code.genericattribute3,
         trim(District_name.firstname||' '||District_name.lastname),
         DM_code.genericattribute2,
         agent.genericattribute1,
         trim(Agency_name.firstname||' '||Agency_name.lastname),
         agent.genericattribute2,
         agent.genericattribute7,
         tf.payor_code,
         trim(Agent_name.firstname||' '||Agent_name.lastname),
         decode(Agent_name.genericboolean6, 0, 'Normal FSC', 1, 'FORTS FSC'),
         title_agt.name,
          agent.genericattribute4,
         agent.genericattribute11,
         ba.bsc_grade,
         ba.entitlementpercent,
         tf.policy_number,
         tf.life_number,
         tf.coverage_number,
         tf.RIDER_NUMBER,
         tf.COMPONENT_NAME,
         tf.component_code,
         qtr.name,
         tf.PRODUCT_NAME,
         tf.base_rider_ind,
         tf.YTD_MPERIOD,
         pos_agy_rcr.GENERICATTRIBUTE2,
         pos_agy_rcr.genericattribute11,
         rul.EXPRESSIONTYPEFORTYPE;

Log('insert into AIA_CB_CLAWBACK_COMP FA for SPI FA' || '; row count: ' || to_char(sql%rowcount));
commit;

merge into AIA_CB_CLAWBACK_COMP cc
  using(select row_number() over(partition by
                 cb.YEAR, cb.quarter, cb.PONUMBER, cb.WRI_AGT_CODE,
                 cb.LIFE_NUMBER, cb.RIDER_NUMBER, cb.COVERAGE_NUMBER,
                 cb.COMPONENT_CODE order by cb.WRI_AGT_CODE desc) rk,
                 cb.PONUMBER,
                 cb.WRI_AGT_CODE,
                 cb.SPI_CB,
                 cb.YEAR,
                 cb.quarter,
                 cb.CLAWBACK_TYPE,
                 cb.LIFE_NUMBER,
                 cb.RIDER_NUMBER,
                 cb.COVERAGE_NUMBER,
                 cb.COMPONENT_CODE,
                 csp.PERIODSEQ
        from AIA_CB_SPI_CLAWBACK cb
        left join cs_period qtr on qtr.name = (cb.quarter || ' ' || cb.year)
        left join cs_period csp on csp.parentseq = qtr.periodseq
         and csp.calendarseq = V_CALENDARSEQ
         and csp.periodtypeseq = V_periodtype_month_seq
         and csp.enddate = qtr.enddate
       where cb.BUNAME = STR_BUNAME_FA
         and qtr.removedate = DT_REMOVEDATE
         and qtr.calendarseq = V_CALENDARSEQ
         and qtr.periodtypeseq = V_periodtype_quarter_seq --quarter
        ) src
  on (  cc.WRI_AGT_CODE = src.WRI_AGT_CODE
    and cc.PONUMBER = src.PONUMBER
    and cc.CLAWBACK_TYPE = src.CLAWBACK_TYPE
    and cc.LIFE_NUMBER = src.LIFE_NUMBER
    and cc.COVERAGE_NUMBER = src.COVERAGE_NUMBER
    and cc.RIDER_NUMBER = src.RIDER_NUMBER
    and cc.COMPONENT_CODE = src.COMPONENT_CODE
    and cc.YTD_PERIOD = src.PERIODSEQ )
  when matched then update
  set cc.CLAWBACK = src.SPI_CB
where src.rk = 1
  and cc.CLAWBACK_NAME in ('SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1') -- version 17 Harm_BSC_SPI
  and cc.BATCH_NO = P_BATCH_NO;

  Log('Merge into AIA_CB_CLAWBACK_COMP FA clawback value ' || V_Second_QTR || '; row count: ' || to_char(sql%rowcount));
  commit;
--Version 2

--update the table AIA_CB_CLAWBACK_COMMISSION for special handling for positive clawback
merge into AIA_CB_CLAWBACK_COMP cc
using AIA_CB_CLAWBACK_SVI_COMP_TMP st
on (cc.wri_dist_code = st.wri_dist_code
 and cc.wri_agy_code = st.wri_agy_code
 and cc.wri_agt_code = st.wri_agt_code
 and cc.ponumber = st.ponumber
 and cc.life_number = st.life_number
 and cc.coverage_number = st.coverage_number
 and cc.rider_number = st.rider_number
 and cc.component_code = st.component_code
 and cc.product_name = st.product_name
 and cc.clawback_name = st. clawback_name
 and cc.batch_no = st.batch_no
 and cc.batch_no = P_BATCH_NO
)
when matched then update set cc.clawback = 0;

Log('merge into AIA_CB_CLAWBACK_COMP FA to handle positive clawback' || '; row count: ' || to_char(sql%rowcount));

 -- version 17 Harm_BSC_SPI start
-- update wri_agt_code when  wri_agt_code and comm_agt_code is old code but need pay to new code in migration quarter
merge into AIA_CB_CLAWBACK_COMP cc
using
(select distinct gapar.genericattribute4,par.userid,cc1.YTD_PERIOD,cc1.PONUMBER,cc1.TRANSACTION_DATE  --,cp.enddate,par2.hiredate
  from cs_participant par
  inner join AIA_CB_CLAWBACK_COMP cc1 on par.userid = 'SGT' || cc1.WRI_AGT_CODE
  inner join cs_gaparticipant gapar on gapar.payeeseq = par.payeeseq
  inner join cs_period cp on cp.periodseq = cc1.YTD_PERIOD
  inner join cs_participant par2 on par2.userid = 'SGT' || gapar.genericattribute4
where
  par.islast = 1
  and par.removedate = DT_REMOVEDATE
  and gapar.effectiveenddate = DT_REMOVEDATE
  and gapar.removedate = DT_REMOVEDATE
  and gapar.genericattribute4 is not null
  and cp.removedate = DT_REMOVEDATE
  and par2.hiredate <= cp.enddate -1
  and cc1.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1')
  and cc1.BATCH_NO = P_BATCH_NO
  and par2.removedate = DT_REMOVEDATE
  and par2.islast = 1
  ) tempcode
on (
  tempcode.YTD_PERIOD = cc.YTD_PERIOD
  and tempcode.PONUMBER = cc.PONUMBER
  and tempcode.TRANSACTION_DATE = cc.TRANSACTION_DATE
  and cc.BATCH_NO = P_BATCH_NO
  )
when matched then update
set cc.WRI_AGT_CODE = tempcode.genericattribute4;


Log('update AIA_CB_CLAWBACK_COMP payor code for SPI FA  2.1' || '; row count: ' || to_char(sql%rowcount));
commit;

Log('SP_CLAWBACK_COMP_FA end');
 -- version 17 Harm_BSC_SPI end
commit;

end SP_CLAWBACK_COMP_FA;

PROCEDURE SP_EXEC_COMP_ONGOING_FA(P_STR_CB_CYCLEDATE IN VARCHAR2)
  is
  V_STR_CB_TYPE    VARCHAR2(20);
  V_BATCH_NO       NUMBER;
  V_WEEKEND_FLAG   NUMBER;
  V_MONTHEND_FLAG  NUMBER;
  V_MESSAGE        VARCHAR2(2000);
  begin

  init;

  ---to define the run type
  /*SELECT COUNT(1) INTO V_WEEKEND_FLAG FROM IN_ETL_CONTROL CTL
  WHERE CTL.TXT_KEY_STRING='PAYMENT_END_DATE_WEEKLY' AND CTL.TXT_FILE_NAME= STR_PU AND CTL.TXT_KEY_VALUE=P_STR_CB_CYCLEDATE;*/
  SELECT COUNT(1) INTO V_MONTHEND_FLAG FROM CS_PERIOD CSP where CSP.ENDDATE - 1 = TO_DATE(P_STR_CB_CYCLEDATE,STR_DATE_FORMAT_TYPE)
  and CSP.CALENDARSEQ = V_CALENDARSEQ and CSP.PERIODTYPESEQ=(select periodtypeseq from  cs_periodtype where name = STR_CALENDAR_TYPE)
  and CSP.removedate = to_date('2200-01-01','yyyy-mm-dd')  --Cosimo
  ;

  IF V_MONTHEND_FLAG>0
         THEN
         --ONGOING
         sp_create_batch_no_fa(P_STR_CB_CYCLEDATE,STR_ONGOING,STR_COMPENSATION);
         V_BATCH_NO := fn_get_batch_no_fa(P_STR_CB_CYCLEDATE, STR_COMPENSATION,STR_ONGOING, STR_STATUS_START);
         SP_TRACE_FORWARD_COMP_FA (P_STR_CB_CYCLEDATE,STR_ONGOING, V_BATCH_NO);
         SP_CLAWBACK_COMP_FA (P_STR_CB_CYCLEDATE, V_BATCH_NO);
         sp_update_batch_status (V_BATCH_NO, STR_STATUS_COMPLETED_SP);
  END IF;
---catch exception
        EXCEPTION WHEN OTHERS
        THEN    v_message := SUBSTR(SQLERRM,1,2000);
                Log(v_message);
                sp_update_batch_status(V_BATCH_NO, STR_STATUS_FAIL);
  END SP_EXEC_COMP_ONGOING_FA;

procedure SP_CREDIT_COMMISSION_FA (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO_1 IN INTEGER, P_BATCH_NO_2 IN INTEGER) as

--V_BATCH_NO INTEGER;
V_REC_COUNT INTEGER;
V_GST_RATE NUMBER(10,2);
vCreditOffset INTEGER;
begin

Log('SP_CREDIT_COMMISSION_FA start');

--get batch number
--V_BATCH_NO := PK_AIA_CB_CALCULATION.fn_get_batch_no(P_STR_CYCLEDATE);

--get the GST rate from TrueComp rate schedule table
/*
--v14

select cell.value
  into V_GST_RATE
  from CS_RELATIONALMDLT RM
 inner join CS_MDLTCell cell
    on cell.mdltseq = RM.ruleelementseq
 where RM.name = 'LT_SG_GST'
   and RM.removedate = DT_REMOVEDATE
   and cell.removedate = DT_REMOVEDATE;
*/
select value into V_GST_RATE from vw_lt_gst_rate where
    to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= effectivestartdate and
    to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) < effectiveenddate
    ;

--insert data into credit stage table

Log('insert into AIA_CB_CREDIT_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);

 insert into AIA_CB_CREDIT_STG
                   (new_creditSeq,        src_creditSeq,
                    payeeSeq,
                    positionSeq,           salesOrderSeq,
                    salesTransactionSeq,   creditTypeSeq,
                    isHeld,                releaseDate,
                    pipelineRunSeq,        originTypeId,
                    periodSeq,
                    compensationDate,
                    value,                 unitTypeForValue,
                    preAdjustedValue,      unitTypeForPreAdjustedValue,
                    isRollable,            rollDate,
                    reasonSeq,             ruleSeq,
                    pipelineRunDate,       businessUnitMap,
                    name,
                    comments,              genericAttribute1,
                    genericAttribute2,     genericAttribute3,
                    genericAttribute4,     genericAttribute5,
                    genericAttribute6,     genericAttribute7,
                    genericAttribute8,     genericAttribute9,
                    genericAttribute10,    genericAttribute11,
                    genericAttribute12,    genericAttribute13,
                    genericAttribute14,    genericAttribute15,
                    genericAttribute16,
                    genericNumber1,        unitTypeForGenericNumber1,
                    genericNumber2,        unitTypeForGenericNumber2,
                    genericNumber3,        unitTypeForGenericNumber3,
                    genericNumber4,        unitTypeForGenericNumber4,
                    genericNumber5,        unitTypeForGenericNumber5,
                    genericNumber6,        unitTypeForGenericNumber6,
                    genericDate1,
                    genericDate2,          genericDate3,
                    genericDate4,          genericDate5,
                    genericDate6,          genericBoolean1,
                    genericBoolean2,       genericBoolean3,
                    genericBoolean4,       genericBoolean5,
                    genericBoolean6,       processingUnitSeq,
                    BATCH_NO
                    )
                select
                    --will get credit seq in stagehook
                    rownum as new_creditSeq,
                    cb.creditseq as src_creditSeq,
                    crd.payeeSeq,
                    crd.positionSeq,
                    st.salesOrderSeq,
                    st.salesTransactionSeq,
                    (select dataTypeSeq  from   CS_CreditType     where  lower(creditTypeId) = lower(rl_type.TARGET_CREDIT_TYPE)
                    and    removeDate = to_date('01012200','mmddyyyy')),
                     crd.isHeld,
                    crd.releaseDate,
                    --will get the pipeline run seq in stagehook
                    0 as pipelinerunseq,
                    'calculated',
                    fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq,
                    case cb.clawback_type
                      when STR_LUMPSUM then cb.calculation_date
                      when STR_ONGOING then crd.compensationdate
                      else to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
                    end as compensationDate,
                    round(nvl(cb.clawback,0),2) as crd_value,--round(cb.clawback,2) as crd_value,
                    crd.unittypeforvalue,
                    round(nvl(cb.clawback,0),2) as preAdjustedValue,--round(cb.clawback,2) as preAdjustedValue,
                    crd.unitTypeForPreAdjustedValue,
                    crd.isRollable,
                    crd.rollDate,
                    crd.reasonSeq,
                    crd_rule.ruleseq as ruleSeq,
                    '' as pipelinerundate,
                    crd.businessUnitMap,
                    rl_out.target_rule_output as name,
                    crd.comments,                       crd.genericAttribute1,
                    crd.genericAttribute2,              crd.genericAttribute3,
                    crd.genericAttribute4,              crd.genericAttribute5,
                    crd.genericAttribute6,
                    --expense account
                    --ac_lk.target_ac_code as genericAttribute7,
                    crd.genericAttribute7 as genericAttribute7,
                    --balance account
                    --crd.genericAttribute8 as genericAttribute8,
                    ac_lk.target_ac_code as genericAttribute8,
                    crd.genericAttribute9,
                    crd.genericAttribute10,             crd.genericAttribute11,
                    crd.genericAttribute12,             crd.genericAttribute13,
                    crd.genericAttribute14,             crd.genericAttribute15,
                    crd.genericAttribute16,
                    crd.genericNumber1,                 crd.unitTypeForGenericNumber1,
                    crd.genericNumber2,                 crd.unitTypeForGenericNumber2,
                    --crd.genericNumber3,
                    case crd.value when 0 then 0
                      else round((crd.genericNumber3 / crd.value) * cb.clawback, 2)
                        end as genericNumber3,
                    crd.unitTypeForGenericNumber3,
                    --crd.genericNumber4,
                    case crd.value when 0 then 0
                      else round(round((crd.genericNumber3 / crd.value) * cb.clawback, 2) * V_GST_RATE, 2)
                        end as genericNumber4,
                    crd.unitTypeForGenericNumber4,
                    crd.genericNumber5,                 crd.unitTypeForGenericNumber5,
                    --GST amount
                    round(round(cb.clawback, 2) * V_GST_RATE,2) as genericNumber6,
                    crd.unitTypeForGenericNumber6,
                    crd.genericDate1,
                    crd.genericDate2,                   crd.genericDate3,
                    crd.genericDate4,                   crd.genericDate5,
                    crd.genericDate6,                   crd.genericBoolean1,
                    crd.genericBoolean2,                crd.genericBoolean3,
                    crd.genericBoolean4,                crd.genericBoolean5,
                    crd.genericBoolean6,
                    crd.processingUnitSeq,
                    cb.batch_no
                from AIA_CB_CLAWBACK_COMMISSION    cb
                inner join CS_SalesTransaction st
                on cb.salestransactionseq = st.salestransactionseq
                inner join cs_credit crd
                on cb.creditseq = crd.creditseq
                   inner join CS_CREDITTYPE ct
                   on crd.CREDITTYPESEQ = ct.DATATYPESEQ
                   and ct.Removedate = DT_REMOVEDATE
                --for lookup new credit type
                inner join (select distinct source_credit_type, target_credit_type
                           from aia_cb_rules_lookup
                           where BUNAME = STR_BUNAME_FA
                           and RULE_TYPE = 'CREDIT'
                           and CLAWBACK_NAME = STR_COMMISSION) rl_type
                on ct.credittypeid = rl_type.source_credit_type
                --for lookup new credit output name
                inner join (select distinct source_rule_name, source_rule_output, target_rule_name,target_rule_output
                           from aia_cb_rules_lookup
                           where BUNAME = STR_BUNAME_FA
                           and RULE_TYPE = 'CREDIT'
                           and CLAWBACK_NAME = STR_COMMISSION) rl_out
                on upper(crd.name) = upper(rl_out.source_rule_output)
                --for lookup new credit sequence number
                inner join cs_rule crd_rule
                on crd_rule.removedate = DT_REMOVEDATE
                and crd_rule.name = rl_out.target_rule_name
                --v15
                and crd_rule.effectivestartdate <= to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and crd_rule.effectiveenddate > to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
                --for lookup new account code
                inner join aia_cb_ac_lookup ac_lk
                on crd.name = ac_lk.source_credit_name
                and crd.genericattribute4 = ac_lk.premium_type
                and crd.genericattribute3 = ac_lk.fund_type
                and crd.genericattribute8 = ac_lk.source_ac_code
                where cb.clawback_name = STR_COMMISSION
                and cb.batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2)
                ;

Log('insert into AIA_CB_CREDIT_STG' || '; row count: ' || to_char(sql%rowcount));

commit;

--get latest records count from AIA_CB_CREDIT_STG
select count(1)
  into V_REC_COUNT
  from AIA_CB_CREDIT_STG
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

--get credit sequence number from TrueComp
if V_REC_COUNT > 0 then
vCreditOffset := SequenceGenPkg.GetNextFullSeq('creditSeq', 56, V_REC_COUNT) - 1;

update AIA_CB_CREDIT_STG
   set new_creditSeq = new_creditSeq + vCreditOffset
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

commit;

END IF;

Log('SP_CREDIT_COMMISSION_FA end');

end SP_CREDIT_COMMISSION_FA;

PROCEDURE SP_PM_COMMISSION_FA (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO_1 IN INTEGER, P_BATCH_NO_2 IN INTEGER) as
--V_BATCH_NO INTEGER;
V_REC_COUNT INTEGER;
vPMOffset INTEGER;
begin

Log('SP_PM_COMMISSION_FA start');

Log('insert into AIA_CB_PM_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);

--insert data into AIA_CB_PM_STG
insert into AIA_CB_PM_STG
  (new_measurementseq,
   src_measurementseq,
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
   unittypefornumberofcredits,
   batch_no)
  select dense_rank() over(order by rs.NAME, rs.PAYEESEQ, rs.POSITIONSEQ, rs.PERIODSEQ, rs.PIPELINERUNSEQ) as new_measurementseq,
         rs.*
    from --for commission clawback records
          (select cb.pmseq as src_measurementseq,
                  rl_out.target_rule_output as name,
                  pm.payeeseq as payeeseq,
                  pm.positionseq as positionseq,
                  fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq,
                  0 as pipelinerunseq,
                  '' as pipelinerundate,
                  pm_rule.ruleseq as ruleseq,
                  cb.clawback as value,
                  pm.unittypeforvalue as unittypeforvalue,
                  cb.numberofcredits as numberofcredits,
                  pm.businessunitmap as businessunitmap,
                  cb.FSC_BSC_PERCENTAGE as genericnumber1,
                  --unit type for percent
                  1970324836974598              as unittypeforgenericnumber1,
                  pm.processingunitseq,
                  pm.unittypefornumberofcredits,
                  cb.batch_no
             from (select batch_no,
                          measurement_quarter,
                          clawback_type,
                          pmseq,
                          FSC_BSC_PERCENTAGE,
                          sum(clawback) as clawback,
                          count(distinct creditseq) as numberofcredits
                     from AIA_CB_CLAWBACK_COMMISSION
                    where CLAWBACK_NAME = STR_COMMISSION
                      and AIA_CB_CLAWBACK_COMMISSION.batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2)
                    group by batch_no,
                             measurement_quarter,
                             clawback_type,
                             pmseq,
                             FSC_BSC_PERCENTAGE) cb
            inner join cs_measurement pm
               on cb.pmseq = pm.measurementseq
           --for lookup new pm output name
            inner join (select distinct source_rule_name,
                                       source_rule_output,
                                       target_rule_name,
                                       target_rule_output
                         from aia_cb_rules_lookup
                        where BUNAME = STR_BUNAME_FA
                          and RULE_TYPE = 'PM'
                          and CLAWBACK_NAME = STR_COMMISSION) rl_out
               on upper(pm.name) = upper(rl_out.source_rule_output)
           --for lookup new pm rules
            inner join cs_rule pm_rule
               on pm_rule.removedate = DT_REMOVEDATE
              and rl_out.target_rule_name = pm_rule.name
                --v15
                and pm_rule.effectivestartdate <= to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and pm_rule.effectiveenddate > to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
           union all
--for GST clawback records
           select cb.pmseq as src_measurementseq,
                  rl_out.target_rule_output as name,
                  pm.payeeseq as payeeseq,
                  pm.positionseq as positionseq,
                  fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq,
                  0 as pipelinerunseq,
                  '' as pipelinerundate,
                  pm_rule.ruleseq as ruleseq,
                  --GST value
                  cb.clawback                   as value,
                  pm.unittypeforvalue           as unittypeforvalue,
                  cb.numberofcredits            as numberofcredits,
                  pm.businessunitmap            as businessunitmap,
                  pm.genericnumber1,
                  pm.unittypeforgenericnumber1,
                  pm.processingunitseq,
                  pm.unittypefornumberofcredits,
                  cb.batch_no
             from (select batch_no,
                          measurement_quarter,
                          clawback_type,
                          pmseq,
                          FSC_BSC_PERCENTAGE,
                          sum(clawback) as clawback,
                          count(distinct creditseq) as numberofcredits
                     from AIA_CB_CLAWBACK_COMMISSION
                    where CLAWBACK_NAME = STR_GST_COMMISSION
                      and AIA_CB_CLAWBACK_COMMISSION.batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2)
                    group by batch_no,
                             measurement_quarter,
                             clawback_type,
                             pmseq,
                             FSC_BSC_PERCENTAGE) cb
            inner join cs_measurement pm
               on cb.pmseq = pm.measurementseq
           --for lookup new pm output name
            inner join (select distinct source_rule_name,
                                        source_rule_output,
                                        target_rule_name,
                                        target_rule_output
                          from aia_cb_rules_lookup
                         where BUNAME = STR_BUNAME_FA
                           and RULE_TYPE = 'PM'
                           and CLAWBACK_NAME = STR_GST_COMMISSION) rl_out
               on upper(pm.name) = upper(rl_out.source_rule_output)
           --for lookup new pm rules
            inner join cs_rule pm_rule
               on pm_rule.removedate = DT_REMOVEDATE
              and rl_out.target_rule_name = pm_rule.name
                --v15
                and pm_rule.effectivestartdate <= to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and pm_rule.effectiveenddate > to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
union all
--for supplement the PM records with figure 0 for deposit rules
             select distinct 0 as src_measurementseq,
                             rl_supl_pm.target_rule_output as name,
                             pm.payeeseq as payeeseq,
                             pm.positionseq as positionseq,
                             fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq,
                             0 as pipelinerunseq,
                             '' as pipelinerundate,
                             pm_rule.ruleseq as ruleseq,
                             0 as value,
                             pm.unittypeforvalue as unittypeforvalue,
                             0 as numberofcredits,
                             pm.businessunitmap as businessunitmap,
                             cb.FSC_BSC_PERCENTAGE as genericnumber1,
                             1970324836974598 as unittypeforgenericnumber1,
                             pm.processingunitseq,
                             pm.unittypefornumberofcredits,
                             cb.batch_no as BATCH_NO
                     from (select batch_no,
                            measurement_quarter,
                            clawback_type,
                            pmseq,
                            FSC_BSC_PERCENTAGE,
                            sum(clawback) as clawback,
                            count(distinct creditseq) as numberofcredits
                       from AIA_CB_CLAWBACK_COMMISSION
                      where CLAWBACK_NAME = STR_COMMISSION
                        and AIA_CB_CLAWBACK_COMMISSION.batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2)
                      group by batch_no,
                               measurement_quarter,
                               clawback_type,
                               pmseq,
                               FSC_BSC_PERCENTAGE) cb
                    inner join cs_measurement pm
                       on cb.pmseq = pm.measurementseq
                    inner join (select distinct source_rule_output,
                                          source_rule_name,
                                          target_rule_output,
                                          target_rule_name
                            from aia_cb_rules_lookup
                           where BUNAME = STR_BUNAME_FA
                             and RULE_TYPE = 'PM'
                             and CLAWBACK_NAME = STR_COMMISSION
                             and source_rule_output in
                                 ('PM_FYC_Initial_LF_RP',
                                  'PM_FYC_Initial_LF_SP',
                                  'PM_FYC_Non_Initial_LF_RP',
                                  'PM_FYC_Non_Initial_LF_SP')) rl_supl_pm
                       on 1 = 1 --pm.name = rl_supl_pm.source_rule_output
                      and pm.name in ('PM_FYC_Initial_LF_RP',
                                      'PM_FYC_Initial_LF_SP',
                                      'PM_FYC_Non_Initial_LF_RP',
                                      'PM_FYC_Non_Initial_LF_SP')
                   --for lookup new pm rules
                    inner join cs_rule pm_rule
                       on pm_rule.removedate = DT_REMOVEDATE
                      and rl_supl_pm.target_rule_name = pm_rule.name
                    --v15
                    and pm_rule.effectivestartdate <= to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and pm_rule.effectiveenddate > to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
           ) rs;

Log('insert into AIA_CB_PM_STG' || '; row count: ' || to_char(sql%rowcount));

commit;


--get latest records count from AIA_CB_CREDIT_STG
select count(distinct New_Measurementseq)
  into V_REC_COUNT
  from AIA_CB_PM_STG
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

--get credit sequence number from TrueComp
if V_REC_COUNT > 0 then
vPMOffset := SequenceGenPkg.GetNextFullSeq('measurementSeq', 64, V_REC_COUNT) - 1;

update AIA_CB_PM_STG t
   set new_measurementseq = new_measurementseq + vPMOffset
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

commit;

END IF;

Log('SP_PM_COMMISSION_FA end');

end SP_PM_COMMISSION_FA
;


/* this procedure is for credit staging table*/
PROCEDURE SP_PMCRDTRACE_COMMISSION_FA (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO_1 IN INTEGER, P_BATCH_NO_2 IN INTEGER) is
 -- V_BATCH_NO INTEGER;
 v_puseq integer;
begin

Log('SP_PMCRDTRACE_COMMISSION_FA start');

Log('insert into AIA_CB_PMCRDTRACE_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);

--insert date into AIA_CB_PMCRDTRACE_STG
insert into AIA_CB_PMCRDTRACE_STG
  select distinct crd_stg.new_creditseq as creditseq,
                  pm_stg.new_measurementseq as measurementseq,
                  pm_stg.ruleseq as ruleseq,
                  0 as pipelinerunseq,
                  crd_stg.periodseq as sourceperiodseq,
                  pm_stg.periodseq as targetperiodseq,
                  'calculated' as sourceoringintypeid,
                  decode(rl.clawback_name,
                         STR_COMMISSION,
                         crd_stg.value,
                         STR_GST_COMMISSION,
                         crd_stg.genericnumber6,
                         0) as contributionvalue,
                  crd_stg.unittypeforvalue as unittypeforcontributionvalue,
                  64 as businessunitmap,
                  V_PROCESSINGUNITSEQ as processingunitseq,
                  cb.batch_no
    from AIA_CB_CLAWBACK_COMMISSION cb
   inner join AIA_CB_CREDIT_STG crd_stg
      on cb.creditseq = crd_stg.src_creditseq
   inner join AIA_CB_PM_STG pm_stg
      on cb.pmseq = pm_stg.src_measurementseq
    left join aia_cb_rules_lookup rl
      on pm_stg.name = rl.target_rule_output
     and rl.clawback_name in (STR_COMMISSION, STR_GST_COMMISSION)
     and rule_type = 'PM'
   where cb.batch_no in (P_BATCH_NO_1, P_BATCH_NO_2)
     and crd_stg.batch_no in (P_BATCH_NO_1, P_BATCH_NO_2)
     and pm_stg.batch_no in (P_BATCH_NO_1, P_BATCH_NO_2);

Log('insert into AIA_CB_PMCRDTRACE_STG' || '; row count: ' || to_char(sql%rowcount));

commit;

Log('SP_PMCRDTRACE_COMMISSION_FA end');

end SP_PMCRDTRACE_COMMISSION_FA;

PROCEDURE SP_CREDIT_COMP_FA(P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO_1 IN INTEGER, P_BATCH_NO_2 IN INTEGER) as

V_REC_COUNT INTEGER;
vCreditOffset INTEGER;
V_GST_RATE NUMBER(10,2);

begin

Log('SP_CREDIT_COMP_FA start');

delete from AIA_CB_CREDIT_STG where batch_no in (P_BATCH_NO_1,P_BATCH_NO_2);
commit;

--get batch number
--V_BATCH_NO := PK_AIA_CB_CALCULATION.fn_get_batch_no(P_STR_CYCLEDATE);

--get the GST rate from TrueComp rate schedule table
--v14
/*
select cell.value
  into V_GST_RATE
  from CS_RELATIONALMDLT RM
 inner join CS_MDLTCell cell
    on cell.mdltseq = RM.ruleelementseq
 where RM.name = 'LT_SG_GST'
   and RM.removedate = DT_REMOVEDATE
   and cell.removedate = DT_REMOVEDATE;
*/
select value into V_GST_RATE from vw_lt_gst_rate where
    to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) >= effectivestartdate and
    to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) < effectiveenddate
;

--insert data into credit stage table
Log('insert into AIA_CB_CREDIT_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);

 insert into AIA_CB_CREDIT_STG
                   (new_creditSeq,        src_creditSeq,
                    payeeSeq,
                    positionSeq,           salesOrderSeq,
                    salesTransactionSeq,   creditTypeSeq,
                    isHeld,                releaseDate,
                    pipelineRunSeq,        originTypeId,
                    periodSeq,
                    compensationDate,
                    value,                 unitTypeForValue,
                    preAdjustedValue,      unitTypeForPreAdjustedValue,
                    isRollable,            rollDate,
                    reasonSeq,             ruleSeq,
                    pipelineRunDate,       businessUnitMap,
                    name,
                    comments,              genericAttribute1,
                    genericAttribute2,     genericAttribute3,
                    genericAttribute4,     genericAttribute5,
                    genericAttribute6,     genericAttribute7,
                    genericAttribute8,     genericAttribute9,
                    genericAttribute10,    genericAttribute11,
                    genericAttribute12,    genericAttribute13,
                    genericAttribute14,    genericAttribute15,
                    genericAttribute16,
                    genericNumber1,        unitTypeForGenericNumber1,
                    genericNumber2,        unitTypeForGenericNumber2,
                    genericNumber3,        unitTypeForGenericNumber3,
                    genericNumber4,        unitTypeForGenericNumber4,
                    genericNumber5,        unitTypeForGenericNumber5,
                    genericNumber6,        unitTypeForGenericNumber6,
                    genericDate1,
                    genericDate2,          genericDate3,
                    genericDate4,          genericDate5,
                    genericDate6,          genericBoolean1,
                    genericBoolean2,       genericBoolean3,
                    genericBoolean4,       genericBoolean5,
                    genericBoolean6,       processingUnitSeq,
                    BATCH_NO
                    )
                select
                    --will get credit seq in stagehook
                    rownum as new_creditSeq,
                    cb.creditseq as src_creditSeq,
                    crd.payeeSeq,
                    crd.positionSeq,
                    crd.salesOrderSeq,
                    crd.salesTransactionSeq,
                    (select dataTypeSeq  from   CS_CreditType     where  lower(creditTypeId) = lower(rl_type.TARGET_CREDIT_TYPE)
                    and    removeDate = to_date('01012200','mmddyyyy')),
                    crd.isHeld,
                    crd.releaseDate,
                    --will get the pipeline run seq in stagehook
                    0 as pipelinerunseq,
                    'calculated',
                     fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, -- crd.periodseq as periodSeq,
                   --crd.compensationDate,
                   --to_date(V_CYCLE_DATE,STR_DATE_FORMAT_TYPE) as compensationDate,
                    case cb.clawback_type
                      when STR_LUMPSUM then cb.calculation_date
                      --when STR_ONGOING then crd.compensationdate
                      else to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
                    end as compensationDate,
                    nvl(cb.clawback,0) as crd_value,--cb.clawback as crd_value,
                    crd.unittypeforvalue,
                    nvl(cb.clawback,0) as preAdjustedValue,--cb.clawback as preAdjustedValue,
                    crd.unitTypeForPreAdjustedValue,
                    crd.isRollable,
                    crd.rollDate,
                    crd.reasonSeq,
                   crd_rule.ruleseq as ruleSeq,-- 1 as ruleseq,-- Change it when merged with package
                    '' as pipelinerundate,
                    crd.businessUnitMap,
                    rl_out.target_rule_output as name,
                    crd.comments,                       crd.genericAttribute1,
                    crd.genericAttribute2,              crd.genericAttribute3,
                    crd.genericAttribute4,              crd.genericAttribute5,
                    crd.genericAttribute6,              cb.clawback_name,
                    --ac code
                    crd. genericAttribute8, --Not required for compensation, Use same code as that of credit -- ac_lk.target_ac_code as genericAttribute8,
                    crd.genericAttribute9,
                    crd.genericAttribute10,             crd.genericAttribute11,
                    crd.genericAttribute12,             crd.genericAttribute13,
                    crd.genericAttribute14,             crd.genericAttribute15,
                    crd.genericAttribute16,
                    crd.genericNumber1,                 crd.unitTypeForGenericNumber1,
                    crd.genericNumber2,                 crd.unitTypeForGenericNumber2,
                   --crd.genericNumber3,
                    case crd.value when 0 then 0
                      else (crd.genericNumber3 / crd.value) * cb.clawback
                        end as genericNumber3,
                    crd.unitTypeForGenericNumber3,
                    --crd.genericNumber4,
                    case crd.value when 0 then 0
                      else (crd.genericNumber3 / crd.value) * cb.clawback * V_GST_RATE
                        end as genericNumber4,
                    crd.unitTypeForGenericNumber4,
                    crd.genericNumber5,                 crd.unitTypeForGenericNumber5,
                    --GST amount
                    cb.clawback * V_GST_RATE as genericNumber6,
                    crd.unitTypeForGenericNumber6,
                    crd.genericDate1,
                    crd.genericDate2,                   crd.genericDate3,
                    crd.genericDate4,                   crd.genericDate5,
                    crd.genericDate6,                   crd.genericBoolean1,
                    crd.genericBoolean2,                crd.genericBoolean3,
                    crd.genericBoolean4,                crd.genericBoolean5,
                    crd.genericBoolean6,
                    crd.processingUnitSeq,
                    cb.batch_no
                from AIA_CB_CLAWBACK_COMP    cb
                inner join cs_credit crd
                on cb.creditseq = crd.creditseq
               and  cb.salestransactionseq = crd.salestransactionseq
                --for lookup new credit output name
                 inner join cs_rule crd_rule
                on crd_rule.removedate =  DT_REMOVEDATE
                and crd.compensationdate between  crd_rule.effectivestartdate and crd_rule.effectiveenddate -1
                and crd_rule.ruleseq = crd.ruleseq--rl_out.source_rule_name  */
               inner join (select distinct clawback_name,source_rule_name, source_rule_output, target_rule_name,target_rule_output
                           from aia_cb_rules_lookup
                           where BUNAME = STR_BUNAME_FA
                           and RULE_TYPE = 'CREDIT'
                          --Added by Suresh
                          --Add AI NL20180308
                          and CLAWBACK_NAME  in ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1'-- version 14 Harm_BSC_SPI
                          --version13  Harm_Phase4 Start
                          ,'FA_FYO_2.1'
        ,'FA_FYO_ONG_2.1'
        ,'FA_RYO_2.1'
                          ,'FA_RYO_ONG_2.1'
        ,'NADOR_FA_2.1'
        ,'FA_AI_2.1'
        ,'FA_AI_ONG_2.1'
                          --version13  Harm_Phase4 End
                          )) rl_out
                on upper(crd_rule.name) = upper(rl_out.source_rule_name)
          inner join CS_CREDITTYPE ct
                   on crd.CREDITTYPESEQ = ct.DATATYPESEQ
                   and ct.Removedate = DT_REMOVEDATE
                --for lookup new credit type
               inner join (select distinct clawback_name,source_rule_name,source_credit_type, target_credit_type
                           from aia_cb_rules_lookup
                           where BUNAME = STR_BUNAME_FA
                           and RULE_TYPE = 'CREDIT'
                           and CLAWBACK_NAME  in('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                           --version13 Harm_Phase4 Start
                          ,'FA_FYO_2.1'
        ,'FA_FYO_ONG_2.1'
        ,'FA_RYO_2.1'
                          ,'FA_RYO_ONG_2.1'
        ,'NADOR_FA_2.1'
        ,'FA_AI_2.1'
        ,'FA_AI_ONG_2.1'
                          --version13 Harm_Phase4 End
                           )) rl_type
                on ct.credittypeid = rl_type.source_credit_type
                              and cb.clawback_name = rl_out.clawback_name
                 and cb.clawback_name = rl_type.clawback_name
                 and upper(rl_type.SOURCE_RULE_NAME )=  upper(crd_rule.name)
               --for lookup new credit sequence number
                where cb.clawback_name in ('FYO_FA','FYO_FA_ONG','RYO_FA','RYO_FA_ONG','COMMISSION','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                --version13 Harm_Phase4 Start
                ,'FA_FYO_2.1'
    ,'FA_FYO_ONG_2.1'
    ,'FA_RYO_2.1'
                ,'FA_RYO_ONG_2.1'
    ,'NADOR_FA_2.1'
    ,'FA_AI_2.1'
    ,'FA_AI_ONG_2.1'
                --version13 Harm_Phase4 End
                )
                --End by Suresh
                and cb.batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2)
                ;

Log('insert into AIA_CB_CREDIT_STG' || '; row count: ' || to_char(sql%rowcount));

commit;



commit;

--get latest records count from AIA_CB_CREDIT_STG
select count(1)
  into V_REC_COUNT
  from AIA_CB_CREDIT_STG
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

--get credit sequence number from TrueComp
if V_REC_COUNT > 0 then
vCreditOffset := SequenceGenPkg.GetNextFullSeq('creditSeq', 56, V_REC_COUNT) - 1;

update AIA_CB_CREDIT_STG
   set new_creditSeq = new_creditSeq + vCreditOffset
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

commit;

END IF;

Log('SP_CREDIT_COMP_FA end');

end SP_CREDIT_COMP_FA;

PROCEDURE SP_PM_COMP_FA (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO_1 IN INTEGER, P_BATCH_NO_2 IN INTEGER) as


STR_CB_NAME CONSTANT VARCHAR2(20) := 'COMPENSATION';
V_REC_COUNT INTEGER;
vPMOffset INTEGER;
v_periodseq_new integer;   -----POST Aggregate Fine Tune  202311-15 Tina

begin

Log('SP_PM_COMP_FA start');

Log('insert into AIA_CB_PM_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);

delete from AIA_CB_PM_STG where batch_no in (P_BATCH_NO_1,P_BATCH_NO_2);
commit;

v_periodseq_new := fn_get_periodseq(P_STR_CYCLEDATE) ;            -----POST Aggregate Fine Tune  202311-15 Tina
execute immediate 'truncate table AIA_CB_MEASUREMENT_TEMP';       -----POST Aggregate Fine Tune  202311-15 Tina
commit;

Log('start insert into  into AIA_CB_MEASUREMENT_TEMP, ' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);

insert into AIA_CB_MEASUREMENT_TEMP
(MEASUREMENTSEQ, NAME,PAYEESEQ,POSITIONSEQ,PERIODSEQ,VALUE,UNITTYPEFORVALUE,NUMBEROFCREDITS,
BUSINESSUNITMAP,PROCESSINGUNITSEQ,UNITTYPEFORNUMBEROFCREDITS) 
select pm.MEASUREMENTSEQ,  pm.NAME, pm.PAYEESEQ, pm.POSITIONSEQ, pm.PERIODSEQ, pm.VALUE,
pm.UNITTYPEFORVALUE, pm.NUMBEROFCREDITS, pm.BUSINESSUNITMAP, pm.PROCESSINGUNITSEQ,
pm.UNITTYPEFORNUMBEROFCREDITS from cs_measurement pm
where pm.tenantid ='AIAS'
and pm.name in (
select distinct source_rule_output
from aia_cb_rules_lookup
where BUNAME = STR_BUNAME_FA
and RULE_TYPE = 'PM'
 )
 and pm.PERIODSEQ in (
select cbp.PERIODSEQ 
   from cs_period cbp
   inner join cs_calendar cd
   on cbp.calendarseq = cd.calendarseq
   inner join cs_periodtype pt
   on cbp.periodtypeseq = pt.periodtypeseq
   where cd.name = 'AIA Singapore Calendar'
    -- and cbp.removedate = to_date('2200-01-01','yyyy-mm-dd') --for Cosimo
   and cbp.removedate = DT_REMOVEDATE
   and  cbp.startdate  between  add_months(  to_date(P_STR_CYCLEDATE, 'yyyy-mm-dd') ,-12 )  and
   to_date(P_STR_CYCLEDATE, 'yyyy-mm-dd')
   and pt.name = 'month'
   );
   
 DBMS_STATS.GATHER_TABLE_STATS (
  ownname => 'AIASEXT',
  tabname => 'AIA_CB_MEASUREMENT_TEMP'
 ,estimate_percent => dbms_stats.AUTO_SAMPLE_SIZE );   
commit;    


Log('end insert into  into AIA_CB_MEASUREMENT_TEMP, ' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);


--insert data into AIA_CB_PM_STG
insert into AIA_CB_PM_STG
  (new_measurementseq,
   src_measurementseq,
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
   unittypefornumberofcredits,
   batch_no,
   clawback_name)
  select dense_rank() over(order by rs.NAME, rs.PAYEESEQ, rs.POSITIONSEQ, rs.PERIODSEQ, rs.PIPELINERUNSEQ) as new_measurementseq,
         rs.*
    from --for comp clawback records
          (select DISTINCT cb.pmseq as src_measurementseq,
                 rl_out.target_rule_output as name,
		  -- version 17 Harm_BSC_SPI start
                  case when rl_out.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1') then cb.payeeseq else pm.payeeseq end as payeeseq,
                  case when rl_out.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1') then cb.positionseq else pm.positionseq end as positionseq,
                   -- version 17 Harm_BSC_SPI end
		         --fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, -- pm.periodSeq as periodSeq, ----POST Aggregate Fine Tune  202311-15 Tina
				 v_periodseq_new as periodSeq,
                  0 as pipelinerunseq,
                  '' as pipelinerundate,
                  pm_rule.ruleseq as ruleseq,
                  nvl(cb.clawback,0) as value,
                  pm.unittypeforvalue as unittypeforvalue,
                  cb.numberofcredits as numberofcredits,
                  pm.businessunitmap as businessunitmap,
                  cb.FSC_BSC_PERCENTAGE as genericnumber1,
                  --unit type for percent
                  1970324836974598              as unittypeforgenericnumber1,
                  pm.processingunitseq,
                  pm.unittypefornumberofcredits,
                  cb.batch_no,
                   rl_out.clawback_name
		    -- version 17 Harm_BSC_SPI start
             from (select comp.clawback_name,
				   comp.batch_no,
				   comp.measurement_quarter,
				   comp.clawback_type,
				   comp.pmseq,
				   comp.FSC_BSC_PERCENTAGE,
				   sum(comp.clawback) as clawback,
				   count(distinct comp.creditseq) as numberofcredits,
				   max(cp.ruleelementownerseq) as positionseq,
				   max(cp.payeeseq) as payeeseq
                  from AIA_CB_CLAWBACK_COMP comp
                  inner join CS_POSITION cp
                    on 'SGT' || comp.wri_agt_code = cp.name
                   and cp.islast = 1
                   and cp.removedate = DT_REMOVEDATE
                  where comp.CLAWBACK_METHOD = STR_CB_NAME
                   and comp.batch_no in (P_BATCH_NO_1, P_BATCH_NO_2)
                  group by comp.clawback_name,
                          comp.batch_no,
                          comp.measurement_quarter,
                          comp.ytd_period,
                          comp.clawback_type,
                          comp.pmseq,
                          comp.FSC_BSC_PERCENTAGE) cb
		   -- version 17 Harm_BSC_SPI end
           -- inner join cs_measurement pm  ----POST Aggregate Fine Tune  202311-15 Tina
                inner join AIA_CB_MEASUREMENT_TEMP pm 
               on cb.pmseq = pm.measurementseq
           --for lookup new pm output name
            inner join (select distinct clawback_name,source_rule_name,
                                       source_rule_output,
                                       target_rule_name,
                                       target_rule_output
                         from aia_cb_rules_lookup
                        where BUNAME = STR_BUNAME_FA
                          and RULE_TYPE = 'PM'
                          --Added by Suresh
                          and CLAWBACK_NAME  in ('FYO_FA_ONG','FYO_FA','RYO_FA','RYO_FA_ONG','NLPI','NLPI_ONG','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                          --version13 Harm_Phase4 Start
                          ,'FA_FYO_2.1'
        ,'FA_FYO_ONG_2.1'
        ,'FA_RYO_2.1'
                          ,'FA_RYO_ONG_2.1'
        ,'NADOR_FA_2.1'
        ,'FA_AI_2.1'
        ,'FA_AI_ONG_2.1'
                          --version13 Harm_Phase4 End
                          )) rl_out
                          --end by Suresh
               on upper(pm.name) = upper(rl_out.source_rule_output)
           --for lookup new pm rules
            inner join cs_rule pm_rule
               on pm_rule.removedate = DT_REMOVEDATE
              and rl_out.target_rule_name = pm_rule.name
              and rl_out.clawback_name = cb.clawback_name
                --v15
                and pm_rule.effectivestartdate <= to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and pm_rule.effectiveenddate > to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
              union all
--for supplement the PM records with figure 0 for deposit rules
            select distinct 0 as src_measurementseq,
                             rl_supl_pm.target_rule_output as name,
			      -- version 17 Harm_BSC_SPI start
                             case when rl_supl_pm.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1') then cb.payeeseq else pm.payeeseq end as payeeseq,
                             case when rl_supl_pm.clawback_name in ('SPI_FA_2.1','SPI_FA_ONG_2.1') then cb.positionseq else pm.positionseq end as positionseq,
                              -- version 17 Harm_BSC_SPI end
			                --fn_get_periodseq(P_STR_CYCLEDATE) as periodSeq, -- pm.periodSeq as periodSeq, ----POST Aggregate Fine Tune  202311-15 Tina
				             v_periodseq_new as periodSeq,
                             0 as pipelinerunseq,
                             '' as pipelinerundate,
                             pm_rule.ruleseq as ruleseq,
                             0 as value,
                             pm.unittypeforvalue as unittypeforvalue,
                             0 as numberofcredits,
                             pm.businessunitmap as businessunitmap,
                             cb.FSC_BSC_PERCENTAGE as genericnumber1,
                             1970324836974598 as unittypeforgenericnumber1,
                             pm.processingunitseq,
                             pm.unittypefornumberofcredits,
                             cb.batch_no as BATCH_NO,
                                rl_supl_pm.clawback_name
                      -- version 17 Harm_BSC_SPI start
		     from (select comp.clawback_name,comp.batch_no,
                          comp.measurement_quarter,
                          comp.clawback_type,
                          comp.pmseq,
                          comp.FSC_BSC_PERCENTAGE,
                          sum(comp.clawback) as clawback,
                          count(distinct comp.creditseq) as numberofcredits,
			  max(cp.ruleelementownerseq) as positionseq,
			  max(cp.payeeseq) as payeeseq
                  from AIA_CB_CLAWBACK_COMP comp
                  inner join CS_POSITION cp
                    on 'SGT' || comp.wri_agt_code = cp.name
                   and cp.islast = 1
                   and cp.removedate = DT_REMOVEDATE
                      where comp.CLAWBACK_METHOD = STR_CB_NAME
                        and comp.batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2)
                    group by comp.clawback_name,comp.batch_no,
                             comp.measurement_quarter,
                             comp.ytd_period,
                             comp.clawback_type,
                             comp.pmseq,
                             comp.FSC_BSC_PERCENTAGE) cb
		     -- version 17 Harm_BSC_SPI end
                    -- inner join cs_measurement pm  ----POST Aggregate Fine Tune  202311-15 Tina
                       inner join AIA_CB_MEASUREMENT_TEMP pm 
                       on cb.pmseq = pm.measurementseq
                    inner join (select distinct clawback_name,source_rule_output,
                                          source_rule_name,
                                          target_rule_output,
                                          target_rule_name
                            from aia_cb_rules_lookup
                           where BUNAME = STR_BUNAME_FA
                             and RULE_TYPE = 'PM'
                             --Added by Suresh
                             and CLAWBACK_NAME in ('FYO_FA_ONG','FYO_FA','RYO_FA','RYO_FA_ONG','NLPI','NLPI_ONG','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                             --version13 Harm_Phase4 Start
                             ,'FA_FYO_2.1'
           ,'FA_FYO_ONG_2.1'
           ,'FA_RYO_2.1'
                             ,'FA_RYO_ONG_2.1'
           ,'NADOR_FA_2.1'
           ,'FA_AI_2.1'
           ,'FA_AI_ONG_2.1'
                             --version13 Harm_Phase4 End
                             )
                             ) rl_supl_pm
                       on --pm.name = rl_supl_pm.source_rule_output
                       1=1
                       and pm.name in (select distinct source_rule_output from aia_cb_rules_lookup
                           where BUNAME = STR_BUNAME_FA
                             and RULE_TYPE = 'PM'
                             and CLAWBACK_NAME  in ('FYO_FA_ONG','FYO_FA','RYO_FA','RYO_FA_ONG','NLPI','NLPI_ONG','SPI_FA','SPI_FA_ONG','SPI_FA_2.1','SPI_FA_ONG_2.1' -- version 17 Harm_BSC_SPI
                             --version13 Harm_Phase4 Start
                             ,'FA_FYO_2.1'
           ,'FA_FYO_ONG_2.1'
           ,'FA_RYO_2.1'
                             ,'FA_RYO_ONG_2.1'
           ,'NADOR_FA_2.1'
           ,'FA_AI_2.1'
           ,'FA_AI_ONG_2.1'
                             --version13 Harm_Phase4 End
                             ) )
                             --End by Suresh
                   --for lookup new pm rules
                    inner join cs_rule pm_rule
                       on pm_rule.removedate = DT_REMOVEDATE
                      and rl_supl_pm.target_rule_name = pm_rule.name
                    --v15
                    and pm_rule.effectivestartdate <= to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE) and pm_rule.effectiveenddate > to_date(P_STR_CYCLEDATE, STR_DATE_FORMAT_TYPE)
           ) rs;



Log('insert into AIA_CB_PM_STG' || '; row count: ' || to_char(sql%rowcount));

commit;

--get latest records count from AIA_CB_CREDIT_STG
select count(distinct New_Measurementseq)
  into V_REC_COUNT
  from AIA_CB_PM_STG
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

--get credit sequence number from TrueComp
if V_REC_COUNT > 0 then
vPMOffset := SequenceGenPkg.GetNextFullSeq('measurementSeq', 64, V_REC_COUNT) - 1;

update AIA_CB_PM_STG t
   set new_measurementseq = new_measurementseq + vPMOffset
 where batch_no in (P_BATCH_NO_1 , P_BATCH_NO_2);

commit;

END IF;

Log('SP_PM_COMP_FA end');
end SP_PM_COMP_FA;

PROCEDURE SP_PMCRDTRACE_COMP_FA (P_STR_CYCLEDATE IN VARCHAR2, P_BATCH_NO_1 IN INTEGER, P_BATCH_NO_2 IN INTEGER) is

 v_puseq integer;

begin

Log('SP_PMCRDTRACE_COMP_FA start');

Log('insert into AIA_CB_PMCRDTRACE_STG,' ||' batch_no = ' || P_BATCH_NO_1 || ' and ' || P_BATCH_NO_2);

delete from AIA_CB_PMCRDTRACE_STG where batch_no in (P_BATCH_NO_1,P_BATCH_NO_2);
commit;

--insert date into AIA_CB_PMCRDTRACE_STG
insert into AIA_CB_PMCRDTRACE_STG
select distinct crd_stg.new_creditseq as creditseq,
       pm_stg.new_measurementseq as measurementseq,
       pm_stg.ruleseq as ruleseq,
       0 as pipelinerunseq,
       crd_stg.periodseq as sourceperiodseq,
       pm_stg.periodseq as targetperiodseq,
       'calculated' as sourceoringintypeid,
       nvl(crd_stg.value,0) as contributionvalue,
       crd_stg.unittypeforvalue as unittypeforcontributionvalue,
       64 as businessunitmap,
      --1 as processingunitseq, --
      V_PROCESSINGUNITSEQ as processingunitseq,
       cb.batch_no
  from AIA_CB_CLAWBACK_COMP cb
  inner join AIA_CB_CREDIT_STG crd_stg
  on cb.creditseq = crd_stg.src_creditseq
  and cb.clawback_name = crd_stg.genericattribute7
  inner join AIA_CB_PM_STG pm_stg
  on cb.pmseq = pm_stg.src_measurementseq
  and pm_stg.clawback_name =cb.clawback_name
  where cb.batch_no in (P_BATCH_NO_1,P_BATCH_NO_2)
  and crd_stg.batch_no in (P_BATCH_NO_1,P_BATCH_NO_2)
  and pm_stg.batch_no in (P_BATCH_NO_1,P_BATCH_NO_2)
  ;

Log('insert into AIA_CB_PMCRDTRACE_STG' || '; row count: ' || to_char(sql%rowcount));

commit;

Log('SP_PMCRDTRACE_COMP_FA end');

end SP_PMCRDTRACE_COMP_FA;

  procedure SP_STAGE_COMP_ONG_PASTTX_FA(P_STR_CB_ONG_STARTDATE in VARCHAR2) as
  begin
    init;
    Log('SP_STAGE_COMP_ONG_PASTTX_FA started with param ' || P_STR_CB_ONG_STARTDATE);
    execute immediate 'truncate table AIA_CB_COMP_ONG_STGPAST_TX_FA';

    insert into AIA_CB_COMP_ONG_STGPAST_TX_FA
        select  /*+ PARALLEL leading(ip,st,p) INDEX(st aia_salestransaction_product) */
        st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, 
        st.BILLTOADDRESSSEQ, st.SHIPTOADDRESSSEQ, st.OTHERTOADDRESSSEQ, st.ISRUNNABLE, st.BUSINESSUNITMAP, st.ACCOUNTINGDATE, st.PRODUCTID, st.PRODUCTNAME, st.PRODUCTDESCRIPTION, 
        st.NUMBEROFUNITS, st.UNITVALUE, st.UNITTYPEFORUNITVALUE, st.PREADJUSTEDVALUE, st.UNITTYPEFORPREADJUSTEDVALUE, st.VALUE, st.UNITTYPEFORVALUE, st.NATIVECURRENCY, st.NATIVECURRENCYAMOUNT, 
        st.DISCOUNTPERCENT, st.DISCOUNTTYPE, st.PAYMENTTERMS, st.PONUMBER, st.CHANNEL, st.ALTERNATEORDERNUMBER, st.DATASOURCE, st.REASONSEQ, st.COMMENTS, st.GENERICATTRIBUTE1, 
        st.GENERICATTRIBUTE2, st.GENERICATTRIBUTE3, st.GENERICATTRIBUTE4, st.GENERICATTRIBUTE5, st.GENERICATTRIBUTE6, st.GENERICATTRIBUTE7, st.GENERICATTRIBUTE8, st.GENERICATTRIBUTE9, 
        st.GENERICATTRIBUTE10, st.GENERICATTRIBUTE11, st.GENERICATTRIBUTE12, st.GENERICATTRIBUTE13, st.GENERICATTRIBUTE14, st.GENERICATTRIBUTE15, st.GENERICATTRIBUTE16, st.GENERICATTRIBUTE17, 
        st.GENERICATTRIBUTE18, st.GENERICATTRIBUTE19, st.GENERICATTRIBUTE20, st.GENERICATTRIBUTE21, st.GENERICATTRIBUTE22, st.GENERICATTRIBUTE23, st.GENERICATTRIBUTE24, st.GENERICATTRIBUTE25, 
        st.GENERICATTRIBUTE26, st.GENERICATTRIBUTE27, st.GENERICATTRIBUTE28, st.GENERICATTRIBUTE29, st.GENERICATTRIBUTE30, st.GENERICATTRIBUTE31, st.GENERICATTRIBUTE32, st.GENERICNUMBER1, 
        st.UNITTYPEFORGENERICNUMBER1, st.GENERICNUMBER2, st.UNITTYPEFORGENERICNUMBER2, st.GENERICNUMBER3, st.UNITTYPEFORGENERICNUMBER3, st.GENERICNUMBER4, st.UNITTYPEFORGENERICNUMBER4, 
        st.GENERICNUMBER5, st.UNITTYPEFORGENERICNUMBER5, st.GENERICNUMBER6, st.UNITTYPEFORGENERICNUMBER6, st.GENERICDATE1, st.GENERICDATE2, st.GENERICDATE3, st.GENERICDATE4, st.GENERICDATE5, 
        st.GENERICDATE6, st.GENERICBOOLEAN1, st.GENERICBOOLEAN2, st.GENERICBOOLEAN3, st.GENERICBOOLEAN4, st.GENERICBOOLEAN5, st.GENERICBOOLEAN6, st.PROCESSINGUNITSEQ, st.MODIFICATIONDATE, 
        st.UNITTYPEFORLINENUMBER, st.UNITTYPEFORSUBLINENUMBER, st.UNITTYPEFORNUMBEROFUNITS, st.UNITTYPEFORDISCOUNTPERCENT, st.UNITTYPEFORNATIVECURRENCYAMT, st.MODELSEQ,
        ip.BUNAME
        ,ip.YEAR
        ,ip.QUARTER
        ,ip.WRI_DIST_CODE
        ,ip.WRI_DIST_NAME
        ,ip.WRI_DM_CODE
        ,ip.WRI_DM_NAME
        ,ip.WRI_AGY_CODE
        ,ip.WRI_AGY_NAME
        ,ip.WRI_AGY_LDR_CODE
        ,ip.WRI_AGY_LDR_NAME
        ,ip.WRI_AGT_CODE
        ,ip.WRI_AGT_NAME
        ,ip.FSC_TYPE
        ,ip.RANK
        ,ip.CLASS
        ,ip.FSC_BSC_GRADE
        ,ip.FSC_BSC_PERCENTAGE
        ,ip.INSURED_NAME
        ,ip.CONTRACT_CAT
        ,ip.LIFE_NUMBER
        ,ip.COVERAGE_NUMBER
        ,ip.RIDER_NUMBER
        ,ip.COMPONENT_CODE
        ,ip.COMPONENT_NAME
        ,ip.ISSUE_DATE
        ,ip.INCEPTION_DATE
        ,ip.RISK_COMMENCEMENT_DATE
        ,ip.FHR_DATE
        ,ip.BASE_RIDER_IND
        ,ip.TRANSACTION_DATE
        ,ip.PAYMENT_MODE
        ,ip.POLICY_CURRENCY
        ,ip.PROCESSING_PERIOD
        ,ip.CREATED_DATE
        ,ip.POLICYIDSEQ
        ,ip.SUBMITDATE
        ,p.periodseq
        ,ip.FAOB_AGT_CODE
        ,''
        ,''
        from CS_SALESTRANSACTION st
          inner join AIA_CB_IDENTIFY_POLICY ip
             on st.PONUMBER = ip.PONUMBER
            AND st.GENERICATTRIBUTE29 = ip.LIFE_NUMBER
            AND st.GENERICATTRIBUTE30 = ip.COVERAGE_NUMBER
            AND st.GENERICATTRIBUTE31 = ip.RIDER_NUMBER
            AND st.PRODUCTID = ip.COMPONENT_CODE
          inner join CS_PERIOD p
             on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
                and p.calendarseq=V_CALENDARSEQ and p.periodtypeseq = 2814749767106561
           inner join cs_businessunit bu on st.businessunitmap = bu.mask
            where st.tenantid='AIAS' and   st.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
            and ip.buname = STR_BUNAME_FA
            and st.eventtypeseq <> 16607023625933358
            and p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
            --v8 20200928
            and st.compensationdate <= (TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE)-1)
                ;

        Log('SP_STAGE_COMP_ONG_PASTTX_FA compensation, tx up to ' || (TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE)-1) || ' with count = ' || SQL%ROWCOUNT);
        commit;

        --For AI clawback NL20180308
        insert into AIA_CB_COMP_ONG_STGPAST_TX_FA
        with AMR as (select row_number() over(partition by t1.PONUMBER,t1.AI_PAYMENT,t1.COMPENSATIONDATE,t1.PAYEE_CODE ,t1.POLICY_INCEPTION_DATE order by t1.component_CODE) as rn,
                     t1.* from AI_MONTHLY_REPORT t1 where t1.AI_PAYMENT<> 0),
             st as (select row_number() over(partition by t2.PONUMBER,t2.VALUE,t2.ACCOUNTINGDATE,t2.GENERICATTRIBUTE11 ,t2.GENERICDATE2 order by t2.PRODUCTID) as rn,
                    t2.* from cs_Salestransaction t2,cs_businessunit  bu  where t2.tenantid='AIAS' and t2.businessunitmap = bu.mask
        --          and bu.name = STR_BUNAME_FA   --Changes done to fix not getting AGY AI records --Gopi-04072019
                  and t2.eventtypeseq = 16607023625933358 and  t2.PROCESSINGUNITSEQ = V_PROCESSINGUNITSEQ
                  --v8 20200928
                  and t2.compensationdate <= (TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE)-1)),
             IP as (select row_number() over(partition by t3.PONUMBER,t3.WRI_AGT_CODE,t3.component_CODE,t3.inception_date,t3.risk_commencement_date order by t3.coverage_number ) as rn,
                    t3.* from AIA_CB_IDENTIFY_POLICY t3 where t3.BUNAME  =STR_BUNAME_FA)
        select  /*+ PARALLEL */st.TENANTID                        ,
        st.SALESTRANSACTIONSEQ             ,
        st.SALESORDERSEQ                   ,
        st.LINENUMBER                      ,
        st.SUBLINENUMBER                   ,
        st.EVENTTYPESEQ                    ,
        st.PIPELINERUNSEQ                  ,
        st.ORIGINTYPEID                    ,
        st.COMPENSATIONDATE                ,
        st.BILLTOADDRESSSEQ                ,
        st.SHIPTOADDRESSSEQ                ,
        st.OTHERTOADDRESSSEQ               ,
        st.ISRUNNABLE                      ,
        st.BUSINESSUNITMAP                 ,
        st.ACCOUNTINGDATE                  ,
        st.PRODUCTID                       ,
        st.PRODUCTNAME                     ,
        st.PRODUCTDESCRIPTION              ,
        st.NUMBEROFUNITS                   ,
        st.UNITVALUE                       ,
        st.UNITTYPEFORUNITVALUE            ,
        st.PREADJUSTEDVALUE                ,
        st.UNITTYPEFORPREADJUSTEDVALUE     ,
        st.VALUE                           ,
        st.UNITTYPEFORVALUE                ,
        st.NATIVECURRENCY                  ,
        st.NATIVECURRENCYAMOUNT            ,
        st.DISCOUNTPERCENT                 ,
        st.DISCOUNTTYPE                    ,
        st.PAYMENTTERMS                    ,
        st.PONUMBER                        ,
        st.CHANNEL                         ,
        st.ALTERNATEORDERNUMBER            ,
        st.DATASOURCE                      ,
        st.REASONSEQ                       ,
        st.COMMENTS                        ,
        st.GENERICATTRIBUTE1               ,
        st.GENERICATTRIBUTE2               ,
        st.GENERICATTRIBUTE3               ,
        st.GENERICATTRIBUTE4               ,
        st.GENERICATTRIBUTE5               ,
        st.GENERICATTRIBUTE6               ,
        st.GENERICATTRIBUTE7               ,
        st.GENERICATTRIBUTE8               ,
        st.GENERICATTRIBUTE9               ,
        st.GENERICATTRIBUTE10              ,
        st.GENERICATTRIBUTE11              ,
        st.GENERICATTRIBUTE12              ,
        st.GENERICATTRIBUTE13              ,
        st.GENERICATTRIBUTE14              ,
        st.GENERICATTRIBUTE15              ,
        st.GENERICATTRIBUTE16              ,
        st.GENERICATTRIBUTE17              ,
        st.GENERICATTRIBUTE18              ,
        st.GENERICATTRIBUTE19              ,
        st.GENERICATTRIBUTE20              ,
        st.GENERICATTRIBUTE21              ,
        st.GENERICATTRIBUTE22              ,
        st.GENERICATTRIBUTE23              ,
        st.GENERICATTRIBUTE24              ,
        st.GENERICATTRIBUTE25              ,
        st.GENERICATTRIBUTE26              ,
        st.GENERICATTRIBUTE27              ,
        st.GENERICATTRIBUTE28              ,
        st.GENERICATTRIBUTE29              ,
        st.GENERICATTRIBUTE30              ,
        st.GENERICATTRIBUTE31              ,
        st.GENERICATTRIBUTE32              ,
        st.GENERICNUMBER1                  ,
        st.UNITTYPEFORGENERICNUMBER1       ,
        st.GENERICNUMBER2                  ,
        st.UNITTYPEFORGENERICNUMBER2       ,
        st.GENERICNUMBER3                  ,
        st.UNITTYPEFORGENERICNUMBER3       ,
        st.GENERICNUMBER4                  ,
        st.UNITTYPEFORGENERICNUMBER4       ,
        st.GENERICNUMBER5                  ,
        st.UNITTYPEFORGENERICNUMBER5       ,
        st.GENERICNUMBER6                  ,
        st.UNITTYPEFORGENERICNUMBER6       ,
        st.GENERICDATE1                    ,
        st.GENERICDATE2                    ,
        st.GENERICDATE3                    ,
        st.GENERICDATE4                    ,
        st.GENERICDATE5                    ,
        st.GENERICDATE6                    ,
        st.GENERICBOOLEAN1                 ,
        st.GENERICBOOLEAN2                 ,
        st.GENERICBOOLEAN3                 ,
        st.GENERICBOOLEAN4                 ,
        st.GENERICBOOLEAN5                 ,
        st.GENERICBOOLEAN6                 ,
        st.PROCESSINGUNITSEQ               ,
        st.MODIFICATIONDATE                ,
        st.UNITTYPEFORLINENUMBER           ,
        st.UNITTYPEFORSUBLINENUMBER        ,
        st.UNITTYPEFORNUMBEROFUNITS        ,
        st.UNITTYPEFORDISCOUNTPERCENT      ,
        st.UNITTYPEFORNATIVECURRENCYAMT    ,
        st.MODELSEQ                        ,
        ip.BUNAME
        ,ip.YEAR
        ,ip.QUARTER
        ,ip.WRI_DIST_CODE
        ,ip.WRI_DIST_NAME
        ,ip.WRI_DM_CODE
        ,ip.WRI_DM_NAME
        ,ip.WRI_AGY_CODE
        ,ip.WRI_AGY_NAME
        ,ip.WRI_AGY_LDR_CODE
        ,ip.WRI_AGY_LDR_NAME
        ,ip.WRI_AGT_CODE
        ,ip.WRI_AGT_NAME
        ,ip.FSC_TYPE
        ,ip.RANK
        ,ip.CLASS
        ,ip.FSC_BSC_GRADE
        ,ip.FSC_BSC_PERCENTAGE
        ,ip.INSURED_NAME
        ,ip.CONTRACT_CAT
        ,ip.LIFE_NUMBER
        ,ip.COVERAGE_NUMBER
        ,ip.RIDER_NUMBER
        ,ip.COMPONENT_CODE
        ,ip.COMPONENT_NAME
        ,ip.ISSUE_DATE
        ,ip.INCEPTION_DATE
        ,ip.RISK_COMMENCEMENT_DATE
        ,ip.FHR_DATE
        ,ip.BASE_RIDER_IND
        ,ip.TRANSACTION_DATE
        ,ip.PAYMENT_MODE
        ,ip.POLICY_CURRENCY
        ,ip.PROCESSING_PERIOD
        ,ip.CREATED_DATE
        ,ip.POLICYIDSEQ
        ,ip.SUBMITDATE
        ,p.periodseq
        ,ip.FAOB_AGT_CODE
        ,'',''
        from  st
         INNER JOIN  AMR
        ON  st.PONUMBER = AMR.PONUMBER
        AND st.VALUE = AMR.AI_PAYMENT
        AND st.ACCOUNTINGDATE = AMR.COMPENSATIONDATE
        --AND st.GENERICATTRIBUTE11 = AMR.PAYEE_CODE
        AND (st.GENERICATTRIBUTE11 = AMR.NEW_AGENT_CD OR st.GENERICATTRIBUTE11=AMR.OLD_AGENT_CD) ----Changes done to fix not getting AGY AI records --Gopi-04072019
        AND st.GENERICDATE2 = AMR.POLICY_INCEPTION_DATE
        AND st.rn = AMR.rn
        inner join ip
             on IP.BUNAME                     = STR_BUNAME_FA
             AND AMR.PONUMBER                   = IP.PONUMBER
        /*AND ST.GENERICATTRIBUTE29         = IP.LIFE_NUMBER
        AND ST.GENERICATTRIBUTE30         = IP.COVERAGE_NUMBER
        AND ST.GENERICATTRIBUTE31         = IP.RIDER_NUMBER*/
        --     and AMR.PAYEE_CODE = IP.WRI_AGT_CODE
        and (AMR.NEW_AGENT_CD = IP.WRI_AGT_CODE OR AMR.OLD_AGENT_CD=IP.WRI_AGT_CODE) --Changes done to fix not getting AGY AI records --Gopi-04072019
             and AMR.component_CODE=ip.component_CODE
             and AMR.policy_inception_date = ip.inception_date
             and AMR.risk_commencement_date = ip.risk_commencement_date
             and AMR.rn = IP.rn
          inner join CS_PERIOD p
             on st.compensationdate>=p.startdate and st.compensationdate<p.enddate and p.removedate>sysdate
                and p.calendarseq=V_CALENDARSEQ and p.periodtypeseq = 2814749767106561
            where p.removedate = to_date('2200-01-01','yyyy-mm-dd') --Cosimo
                ;

    Log('SP_STAGE_COMP_ONG_PASTTX_FA AI ended, tx up to ' || (TO_DATE(P_STR_CB_ONG_STARTDATE, STR_DATE_FORMAT_TYPE)-1) || ' with count = ' || SQL%ROWCOUNT);
    commit;

  END SP_STAGE_COMP_ONG_PASTTX_FA;

END PK_AIA_CB_CALCULATION_FA;
