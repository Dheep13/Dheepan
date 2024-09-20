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
where period=:v_periodRow.name;
    
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
	PROCESSINGUNITSEQ)
(select distinct
 'TRYG_REDEMPTION_REPORT' as TABLE_NAME,
  st.alternateordernumber as policynumber,
  ext.TRYG_FNC_GET_INSURANCE_GROUP (substr(st.alternateordernumber,1,3),ti.name,null).o_grpname as InsuranceGrpName,
  substr(st.alternateordernumber,1,3) as FGR,
  st.genericattribute15 as CustomerName,
  c.genericnumber3 as Count,
  st.genericboolean4 as SplitFlag, 
  c.genericnumber2 as Weight ,
  st.genericdate1 as PolicyStartDate, 
  st.genericnumber2 as NewPremium ,
  st.genericnumber1 as OldPremium ,
  c.genericnumber1 as BeregnetProvision, 
  par.firstname||' '||par.middlename||' '||par.lastname as participant_name,
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
  case when st.genericboolean5=1 then 'Provision' else 'Afventer provision' end as PaymentStatus,
  case when st.genericboolean5=1 
  and st_002.alternateordernumber||'-'||st_002.sublinenumber||'-'||st_002.linenumber = st.genericattribute21 then 
  st_002.genericdate6 else null end as PaymentDate ,
  st.genericattribute19 as PartnerAgreement1, 
  st.genericattribute20 as PartnerAgreement2,
  current_timestamp as  createdate,
  'DK' as businessunit,
  :v_calendarRow.calendarseq as calendarseq,
  :v_calendarRow.name as calendarname,
  'CommissionPercentage', 
  :v_puRow.name as ProcessingUnitName,
  in_ProcessingUnitSeq 
from cs_salestransaction st 
left join cs_credit c on
st.salestransactionseq=c.salestransactionseq
left join cs_salestransaction st_002 on
st_002.alternateordernumber||'-'||st_002.sublinenumber||'-'||st_002.linenumber = st.genericattribute21
and st.alternateordernumber=st_002.alternateordernumber
left join cs_transactionassignment ta on
ta.salestransactionseq=st.salestransactionseq
and ta.salestransactionseq=c.salestransactionseq
inner join cs_position pos on
pos.name=ta.positionname
and pos.ruleelementownerseq=c.positionseq
inner join cs_payee pay on
pay.payeeseq=pos.payeeseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=pay.payeeseq
inner join cs_title ti on
ti.ruleelementownerseq=pos.titleseq
where c.periodseq=in_PeriodSeq
and st.compensationdate >=:v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
and ta.compensationdate >=:v_periodRow.startDate
and ta.compensationdate < :v_periodRow.endDate
and st.genericboolean5=1---paid flag
and st.eventtypeseq=:v_etSumm.datatypeseq
and pos.removedate =:v_removeDate
and ti.removedate =:v_removeDate
and pay.removedate =:v_removeDate
and par.removedate =:v_removeDate
and pay.effectivestartdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and pos.effectivestartdate between ti.effectivestartdate and add_days(ti.effectiveenddate,-1)
and st_002.eventtypeseq=:v_etpay.datatypeseq

union

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
  c.genericnumber1 as BeregnetProvision, 
  par.firstname||' '||par.middlename||' '||par.lastname as participant_name,
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
  'CommissionPercentage', 
  :v_puRow.name as ProcessingUnitName,
  in_ProcessingUnitSeq 
from cs_salestransaction st 
left  join cs_credit c on
st.salestransactionseq=c.salestransactionseq
left join cs_transactionassignment ta on
ta.salestransactionseq=st.salestransactionseq
and ta.salestransactionseq=c.salestransactionseq
inner join cs_position pos on
pos.name=ta.positionname
and pos.ruleelementownerseq=c.positionseq
inner join cs_payee pay on
pay.payeeseq=pos.payeeseq
inner join cs_participant par on
par.payeeseq=pos.payeeseq
and par.payeeseq=pay.payeeseq
inner join cs_title ti on
ti.ruleelementownerseq=pos.titleseq
where c.periodseq=:v_periodRow.periodseq
and st.compensationdate >=:v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
and ta.compensationdate >=:v_periodRow.startDate
and ta.compensationdate < :v_periodRow.endDate
and st.genericboolean5=0---paid flag
and st.eventtypeseq=:v_etSumm.datatypeseq
and pos.removedate =:v_removeDate
and ti.removedate =:v_removeDate
and pay.removedate =:v_removeDate
and par.removedate =:v_removeDate
and pay.effectivestartdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and pos.effectivestartdate between ti.effectivestartdate and add_days(ti.effectiveenddate,-1)
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