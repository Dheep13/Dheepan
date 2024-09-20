CREATE OR REPLACE PROCEDURE EXT.TRYG_SH_REDEMPTION_REPORT ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Author: Deepan
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 30-May-2023
    |----------------------------------------------------------------------
    | Procedure Purpose: Load ext table for reports Redemption & Sales Commission Reports
    | Version: 0.1	30-May-2023	Intial Version
    -----------------------------------------------------------------------
    */
BEGIN
	--Row type variables declarations
	DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
	DECLARE v_puRow ROW LIKE TCMP.CS_PROCESSINGUNIT;
	DECLARE v_ctRow ROW LIKE TCMP.CS_UNITTYPE;
	DECLARE v_ctDKKRow ROW LIKE TCMP.CS_UNITTYPE;
	DECLARE v_etSumm ROW LIKE TCMP.CS_EVENTTYPE;
	DECLARE v_etpay ROW LIKE TCMP.CS_EVENTTYPE;
	DECLARE v_calendarRow ROW LIKE TCMP.CS_CALENDAR;


	--Variable declarations
	DECLARE v_tenantid VARCHAR(50);
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	DECLARE v_policyPay_ET VARCHAR(50);
	DECLARE v_policySales_ET VARCHAR(50);
	DECLARE v_policySalesSummary_ET VARCHAR(50);


	DECLARE v_sqlCount INT;

	DECLARE v_removeDate DATE;
	DECLARE v_changeDate DATE;


	-- Exeception Handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_REDEMPTION_REPORT';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');
	v_policySales_ET = 'SC-DK-001-001';
	v_policyPay_ET = 'SC-DK-001-002';
 	v_policySalesSummary_ET = 'SC-DK-001-001-SUMMARY';
 	-- v_changeDate = TO_DATE('01/01/2023','mm/dd/yyyy');


	SELECT * INTO v_puRow FROM TCMP.CS_PROCESSINGUNIT cp WHERE cp.PROCESSINGUNITSEQ = in_ProcessingUnitSeq;
	SELECT * INTO v_periodRow FROM TCMP.CS_PERIOD cp WHERE cp.PERIODSEQ = in_PeriodSeq AND cp.REMOVEDATE = v_removeDate;
    select * into v_ctRow from cs_unittype where name='quantity' and REMOVEDATE = :v_removeDate;
    select * into v_etSumm from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and REMOVEDATE = :v_removeDate;
    select * into v_etpay from cs_eventtype where eventtypeid='SC-DK-001-002' and REMOVEDATE = :v_removeDate;
    select * into v_ctDKKRow from cs_unittype where name='DKK' and REMOVEDATE = :v_removeDate;
    select * into v_calendarRow from cs_calendar where name='Main Monthly Calendar' and REMOVEDATE = :v_removeDate;
	v_tenantid = :v_puRow.TENANTID;

EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### '||:v_periodRow.Name,NULL);

delete from EXT.TRYG_REDEMPTION_REPORT
where periodseq=:v_periodRow.periodseq;
    
insert into EXT.TRYG_REDEMPTION_REPORT(
    TABLE_NAME,
	POLICYNUMBER, 
	INSURANCEGRPNAME, 
	FGR,
	CUSTOMERNAME, 
	COUNT,
	SPLITFLAG, 
	WEIGHT,
	POLICYSTARTDATE, 
	NEWPREMIUM ,
	OLDPREMIUM ,
	BEREGNETPROVISION, 
    PARTICIPANT_NAME,
	AGENT,
	AGENTTITLE, 
	PERIOD,
    PERIODSEQ,
	CAUSE,
	ULTIMOPOINT, 
	TILGANGSTYPE,
	RABATGL, 
	RABATNY,
	REVERSAL, 
	PAYMENTSTATUS, 
	PAYMENTDATE, 
	PARTNERAGREEMENT1, 
	PARTNERAGREEMENT2,
    CREATEDATE, 
    BUSINESSUNIT,
    CALENDARSEQ, 
    CALENDARNAME, 
	COMMISSIONPERCENTAGE, 
    PROCESSINGUNITNAME, 
    SPLIT_TYPE,
    POLICY_COMP_DATE,
	PROCESSINGUNITSEQ,
	POSITIONSEQ
)
(select distinct
 'TRYG_REDEMPTION_REPORT' as TABLE_NAME,
  st_002.alternateordernumber as policynumber,
  ext.TRYG_FNC_GET_INSURANCE_GROUP (substr(st_002.alternateordernumber,1,3),ti.name,null).o_grpname as InsuranceGrpName,
  substr(st_002.alternateordernumber,1,3) as FGR,
  st_002.genericattribute15 as CustomerName,
  cr02.genericnumber3 as Count,
  st_002.genericboolean4 as SplitFlag, 
  cr02.genericnumber2 as Weight ,
  st_002.genericdate1 as PolicyStartDate, 
  case when cr02.genericattribute12 is not null and (to_decimal(replace(replace(cr02.genericattribute11,' DKK',''),',',''),25,0) <> 0 
	and to_decimal(replace(replace(cr02.genericattribute12,' DKK',''),',',''),25,0) <> 0 ) then
		to_decimal(replace(replace(cr02.genericattribute12,' DKK',''),',',''),25,10)
	else 
		st_002.genericnumber2 
	end as NewPremium ,
  case when cr02.genericattribute11 is not null and (to_decimal(replace(replace(cr02.genericattribute11,' DKK',''),',',''),25,0) <> 0 
	and to_decimal(replace(replace(cr02.genericattribute12,' DKK',''),',',''),25,0) <> 0 ) then 
		to_decimal(replace(replace(cr02.genericattribute11,' DKK',''),',',''),25,10)
	else
		st_002.genericnumber1 
	end as OldPremium ,
	--2464 AMR 20231025 
  --cr02.genericnumber1 as BeregnetProvision, 
  (st_002.genericnumber2 - st_002.genericnumber1) as BeregnetProvision, 
  COALESCE(par.firstname,'') ||' '|| COALESCE(par.middlename,'')||' '|| COALESCE(par.lastname,'') as participant_name,
  pos.name as Agent ,
  --ta.positionname as Agent ,
  ti.name as AgentTitle,
  --:v_periodRow.name as Period ,
  pr_002.name,--#Defect 2414, Note: this is the original txn compdate not period of plrun 
  :v_periodRow.periodseq as Periodseq,--#Defect 2414, Note: this is the period of plrun
  cr02.genericattribute7 as Cause ,
  cr02.value as UltimoPoint, 
  st_002.genericattribute1 as Tilgangstype, 
  st_002.genericnumber3 as RabatGl,
  st_002.genericnumber4 as RabatNy, 
  'Reversal', 
  case when c.genericattribute1='RECEIVED' then 'Provision' else 'Afventer provision' end as PaymentStatus,
  c.compensationdate as PaymentDate ,
  st_002.genericattribute19 as PartnerAgreement1, 
  st_002.genericattribute20 as PartnerAgreement2,
  current_timestamp as  createdate,
  'DK' as businessunit,
  :v_calendarRow.calendarseq as calendarseq,
  :v_calendarRow.name as calendarname,
  case when c.unittypeforgenericnumber2=1970324836974598 then
  cr02.genericnumber2 
  else
  0 
  end as CommissionPercentage, 
  :v_puRow.name as ProcessingUnitName,
  st_002.genericattribute18 as SPLIT_TYPE,
  st_002.compensationdate as POLICY_COMP_DATE,
  in_ProcessingUnitSeq,
  POS.RULEELEMENTOWNERSEQ
from cs_salestransaction st 
join cs_credit c on st.salestransactionseq=c.salestransactionseq
join cs_transactionassignment ta on ta.salestransactionseq=st.salestransactionseq --and ta.salestransactionseq=c.salestransactionseq 
join cs_position pos on pos.ruleelementownerseq=c.positionseq and ta.positionname=pos.name and pos.removedate = :v_removeDate---Defect 2414-Join ta.positionname and pos.name
join cs_payee pay on pay.payeeseq=pos.payeeseq and pay.removedate = :v_removeDate
join cs_participant par on par.payeeseq=pos.payeeseq and par.payeeseq=pay.payeeseq and par.removedate = :v_removeDate
join cs_title ti on ti.ruleelementownerseq=pos.titleseq and ti.removedate = :v_removeDate
join cs_salestransaction st_002 on st.alternateordernumber||'-'||st.sublinenumber||'-'||st.linenumber = st_002.genericattribute21 and st.alternateordernumber=st_002.alternateordernumber
		and st_002.compensationdate < :v_periodRow.endDate
join cs_transactionassignment ta_002 on ta_002.salestransactionseq=st_002.salestransactionseq 
join cs_position pos_002 on pos_002.ruleelementownerseq=c.positionseq and pos_002.removedate = :v_removeDate
join cs_payee pay_002 on pay_002.payeeseq=pos_002.payeeseq and pay_002.removedate = :v_removeDate
join cs_credit cr02 on cr02.salestransactionseq=st_002.salestransactionseq and c.positionseq=cr02.positionseq
join cs_period pr_002 on pr_002.periodseq=cr02.periodseq and pr_002.removedate=:v_removeDate
where c.periodseq=in_PeriodSeq
and st.compensationdate >=:v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
and ta.compensationdate >=:v_periodRow.startDate
and ta.compensationdate < :v_periodRow.endDate
and Upper(c.genericattribute1) = 'RECEIVED'
and st.eventtypeseq=:v_etpay.datatypeseq
/* DSC- TBSV2-2452 : Removing the below condition. This doesn't work when participant startdate is earlier than position startdate
and pay.effectivestartdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and pos.effectivestartdate between ti.effectivestartdate and add_days(ti.effectiveenddate,-1)
and pay_002.effectivestartdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and pos_002.effectivestartdate between ti.effectivestartdate and add_days(ti.effectiveenddate,-1)
*/

/* DSC - TBSV2-2452 : Added the below condition to ensure the correct version of position is retrieved */
and :v_periodRow.startDate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and add_days(:v_periodRow.endDate,-1) between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)

-- and ta.positionname=ta_002.positionname
and st_002.eventtypeseq=:v_etSumm.datatypeseq 
and pay_002.payeeseq=pay.payeeseq
-- and c.value=cr02.value
--Temp Fix for position Name change
--and ((pos.name like '%-AKF%' and st.compensationdate>= to_date('01012023','ddmmyyyy')) or (c.compensationdate<to_date('01012023','ddmmyyyy')))
-- and ((ta.positionname like '%-AKF%' and st.compensationdate>= to_date('01012023','ddmmyyyy')) or (c.compensationdate<to_date('01012023','ddmmyyyy')))

union all
select distinct
 'TRYG_REDEMPTION_REPORT' as TABLE_NAME,
  st.alternateordernumber as policynumber,
  ext.TRYG_FNC_GET_INSURANCE_GROUP (substr(st.alternateordernumber,1,3),ti.name,null).o_grpname as InsuranceGrpName,
  substr(st.alternateordernumber,1,3) as FGR,
  st.genericattribute15 as CustomerName,
  c.genericnumber3 as Count,
  st.genericboolean4 as SplitFlag, 
  c.genericnumber2 as Weight ,
  st.genericdate1 as PolicyStartDate, 
  case when c.genericattribute12 is not null and (to_decimal(replace(replace(c.genericattribute11,' DKK',''),',',''),25,0) <> 0 
	and to_decimal(replace(replace(c.genericattribute12,' DKK',''),',',''),25,0) <> 0 ) then
		to_decimal(replace(replace(c.genericattribute12,' DKK',''),',',''),25,10)
	else 
		st.genericnumber2 
	end as NewPremium ,
  case when c.genericattribute11 is not null and (to_decimal(replace(replace(c.genericattribute11,' DKK',''),',',''),25,0) <> 0 
	and to_decimal(replace(replace(c.genericattribute12,' DKK',''),',',''),25,0) <> 0 ) then 
		to_decimal(replace(replace(c.genericattribute11,' DKK',''),',',''),25,10)
	else
		st.genericnumber1 
	end as OldPremium ,
	--2464 AMR 20231025
  --c.genericnumber1 as BeregnetProvision, 
  (st.genericnumber2 - st.genericnumber1) as BeregnetProvision,
  COALESCE(par.firstname,'') ||' '|| COALESCE(par.middlename,'')||' '|| COALESCE(par.lastname,'') as participant_name,
  ta.positionname as Agent ,
  ti.name as AgentTitle,
  :v_periodRow.name as Period ,
  :v_periodRow.periodseq as Periodseq,
  c.genericattribute7 as Cause ,
  c.value as UltimoPoint, 
  st.genericattribute1 as Tilgangstype, 
  st.genericnumber3 as RabatGl,
  st.genericnumber4 as RabatNy, 
  'Reversal', 
  case when c.genericattribute1='RECEIVED' then 'Provision' else 'Afventer provision' end as PaymentStatus,
  c.compensationdate as PaymentDate ,
  st.genericattribute19 as PartnerAgreement1, 
  st.genericattribute20 as PartnerAgreement2,
  current_timestamp as  createdate,
  'DK' as businessunit,
  :v_calendarRow.calendarseq as calendarseq,
  :v_calendarRow.name as calendarname,
  case when c.unittypeforgenericnumber2=1970324836974598 then
  c.genericnumber2 
  else
  0 
  end as CommissionPercentage, 
  :v_puRow.name as ProcessingUnitName,
  st.genericattribute18 as SPLIT_TYPE,
  st.compensationdate as POLICY_COMP_DATE,
  in_ProcessingUnitSeq,
  POS.RULEELEMENTOWNERSEQ
  from cs_salestransaction st 
join cs_credit c on st.salestransactionseq=c.salestransactionseq
join cs_transactionassignment ta on ta.salestransactionseq=st.salestransactionseq --and ta.salestransactionseq=c.salestransactionseq 
join cs_position pos on pos.name=ta.positionname and pos.ruleelementownerseq=c.positionseq and pos.removedate = :v_removeDate
join cs_payee pay on pay.payeeseq=pos.payeeseq and pay.removedate = :v_removeDate
join cs_participant par on par.payeeseq=pos.payeeseq and par.payeeseq=pay.payeeseq and par.removedate = :v_removeDate
join cs_title ti on ti.ruleelementownerseq=pos.titleseq and ti.removedate = :v_removeDate
where c.periodseq=in_PeriodSeq
and st.compensationdate >=:v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
and ta.compensationdate >=:v_periodRow.startDate
and ta.compensationdate < :v_periodRow.endDate
and Upper(c.genericattribute1) = 'RECEIVED'
and st.eventtypeseq=:v_etSumm.datatypeseq
/* DSC- 2452 : This condition is not required
and pay.effectivestartdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and pos.effectivestartdate between ti.effectivestartdate and add_days(ti.effectiveenddate,-1)
*/

/* DSC- 2452 : Added the below condition to ensure the correct version of position is retrieved */
and :v_periodRow.startDate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and add_days(:v_periodRow.endDate,-1) between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)

--Temp Fix for position Name change
--and ((ta.positionname like '%-AKF%' and st.compensationdate>= to_date('01012023','ddmmyyyy')) or (c.compensationdate<to_date('01012023','ddmmyyyy')))

union all

select distinct
  'TRYG_REDEMPTION_REPORT' as TABLE_NAME,
  st.alternateordernumber as policynumber,
  --c.genericattribute8 as InsuranceGrpName,
  ext.TRYG_FNC_GET_INSURANCE_GROUP (substr(st.alternateordernumber,1,3),ti.name,null).o_grpname as InsuranceGrpName,
  substr(st.alternateordernumber,1,3) as FGR,
  st.genericattribute15 as CustomerName,
  c.genericnumber3 as Count,
  st.genericboolean4 as SplitFlag, 
  c.genericnumber2 as Weight ,
  st.genericdate1 as PolicyStartDate, 
  st.genericnumber2 as NewPremium ,
  st.genericnumber1 as OldPremium ,
  --2464 AMR 20251016
  --c.genericnumber1 as BeregnetProvision, 
  (st.genericnumber2 - st.genericnumber1) as BeregnetProvision,
  COALESCE(par.firstname,'') ||' '|| COALESCE(par.middlename,'')||' '|| COALESCE(par.lastname,'') as participant_name,
  ta.positionname as Agent ,
  ti.name as AgentTitle,
  :v_periodRow.name as Period ,
  :v_periodRow.periodseq as Periodseq,
  c.genericattribute7 as Cause ,
  c.value as UltimoPoint, 
  st.genericattribute1 as Tilgangstype, 
  st.genericnumber3 as RabatGl,
  st.genericnumber4 as RabatNy, 
  'Reversal', 
   'Afventer provision' as PaymentStatus,
   NULL PaymentDate ,
  st.genericattribute19 as PartnerAgreement1, 
  st.genericattribute20 as PartnerAgreement2,
  current_timestamp as  createdate,
  'DK' as businessunit,
  :v_calendarRow.calendarseq as calendarseq,
  :v_calendarRow.name as calendarname,
  case when c.unittypeforgenericnumber2=1970324836974598 then
  c.genericnumber2 
  else
  0 
  end as CommissionPercentage, 
  :v_puRow.name as ProcessingUnitName,
  st.genericattribute18 as SPLIT_TYPE,
  st.compensationdate as POLICY_COMP_DATE,
  in_ProcessingUnitSeq,
  POS.RULEELEMENTOWNERSEQ
from cs_salestransaction st 
join cs_credit c on st.salestransactionseq=c.salestransactionseq
join cs_transactionassignment ta on ta.salestransactionseq=st.salestransactionseq and ta.salestransactionseq=c.salestransactionseq 
join cs_position pos on pos.name=ta.positionname and pos.ruleelementownerseq=c.positionseq and pos.removedate =:v_removeDate
join cs_payee pay on pay.payeeseq=pos.payeeseq and pay.removedate =:v_removeDate
join cs_participant par on par.payeeseq=pos.payeeseq and par.payeeseq=pay.payeeseq and par.removedate =:v_removeDate
join cs_title ti on ti.ruleelementownerseq=pos.titleseq and ti.removedate =:v_removeDate
left join cs_salestransaction st_002 on st.genericattribute21 = st_002.alternateordernumber||'-'||st_002.sublinenumber||'-'||st_002.linenumber and st.alternateordernumber=st_002.alternateordernumber and st_002.eventtypeseq=:v_etpay.datatypeseq
left join cs_credit cr02 on cr02.salestransactionseq=st_002.salestransactionseq
where 1=1 --cr02.periodseq=:v_periodRow.periodseq
and st.compensationdate >=:v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
and ta.compensationdate >=:v_periodRow.startDate
and ta.compensationdate < :v_periodRow.endDate
-- and ifnull(st.genericboolean5,0)=0---paid flag
and (c.genericattribute1<>'RECEIVED' or c.genericattribute1 is null)
-- and cr02.creditseq is null
--2429 Duplicates in report exclude where there is a credit match
and (cr02.creditseq is null or cr02.genericattribute1<>'RECEIVED')
-----------------------
and st.eventtypeseq=:v_etSumm.datatypeseq
/* DSC- TBSV2-2452 : Removing the below condition. This doesn't work when participant startdate is earlier than position startdate
and pay.effectivestartdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and pos.effectivestartdate between ti.effectivestartdate and add_days(ti.effectiveenddate,-1)
*/
/* DSC- TBSV2-2452 : Added the below condition to ensure the correct version of position is retrieved */
and :v_periodRow.startDate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and add_days(:v_periodRow.endDate,-1) between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)

--Temp Fix for position Name change
--and ((ta.positionname like '%-AKF%' and st.compensationdate>= to_date('01012023','ddmmyyyy')) or (c.compensationdate<to_date('01012023','ddmmyyyy')))
);


EXT.TRYG_LOG(v_procedureName,'Custom report table TRYG_REDEMPTION_REPORT loaded for period '||:v_periodRow.Name,::ROWCOUNT);
/*
update ext.TRYG_REDEMPTION_REPORT rr
set (rr.FGR, rr.INSURANCEGRPNAME) = (select ext.TRYG_FNC_GET_INSURANCE_GROUP (substr(rr.policynumber,1,3),rr.agenttitle,null).o_fgr,
ext.TRYG_FNC_GET_INSURANCE_GROUP (substr(rr.policynumber,1,3),rr.agenttitle,null).o_grpname from dummy)
where period=:v_periodRow.name;
*/
EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

commit;
	
END