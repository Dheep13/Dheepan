CREATE OR REPLACE PROCEDURE EXT.TRYG_SP_BROKER_SALE	(OUT FILENAME varchar(120), IN vFname varchar(255) default '', pPeriodName varchar(255) default '') --, IN pPlRunSeq BIGINT)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
AS
BEGIN

declare pPlRunSeq bigint :=20547673299879825; --AMRTEST
declare vPipelinerunseq bigint;
-- declare vFname varchar(255) :='';
DECLARE DBMTK_TRUE TINYINT := 1; DECLARE DBMTK_FALSE TINYINT := 0; /* boolean constants */
DECLARE vSeq BIGINT;
declare vPeriodSeq bigint;
declare vPUSeq bigint;
declare vStartDate timestamp;
declare vEndDate timestamp;
declare vYear varchar(4);
DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
DECLARE v_procName varchar(100);
DECLARE v_removeDate date;

	DECLARE v_policyPay_ET VARCHAR(50);
	DECLARE v_policySales_ET VARCHAR(50);
	DECLARE v_policySalesSummary_ET VARCHAR(50);
	DECLARE v_etSumm ROW LIKE TCMP.CS_EVENTTYPE;
	DECLARE v_etpay ROW LIKE TCMP.CS_EVENTTYPE;

DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
		CALL EXT.TRYG_LOG(v_procName,'ERROR = '||IFNULL( ::SQL_ERROR_MESSAGE,'') ,NULL);
      resignal;
    END;
--------------------------------------------------------------------------------------------

v_procName='EXT.TRYG_SP_BROKER_SALE';
v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');

	v_policySales_ET = 'SC-DK-001-001';
	v_policyPay_ET = 'SC-DK-001-002';
 	v_policySalesSummary_ET = 'SC-DK-001-001-SUMMARY';
 	
 	select * into v_etSumm from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and REMOVEDATE = :v_removeDate;
    select * into v_etpay from cs_eventtype where eventtypeid='SC-DK-001-002' and REMOVEDATE = :v_removeDate;
 	
---------------------------------------------------------------------------------------------

if pPeriodName ='' then
	select max(pipelinerunseq) into vPipelinerunseq from cs_pipelinerun where status='Successful' and stagetypeseq =21673573206720532;
	select periodSeq into vPeriodSeq from CS_PlRun where pipelineRunSeq = :vPipelinerunseq;
else 
	select periodseq into vPeriodSeq default null from cs_period where name=pPeriodName and removedate=to_date('01012200','ddmmyyyy') and calendarseq=2251799813685249;
	
	if vPeriodSeq is null then
		select max(pipelinerunseq) into vPipelinerunseq from cs_pipelinerun where status='Successful' and stagetypeseq =21673573206720532;
		select periodSeq into vPeriodSeq from CS_PlRun where pipelineRunSeq = :vPipelinerunseq;
	end if;
	
end if;

select * into v_periodRow from CS_Period where periodseq = :vPeriodSeq and removedate=:v_removeDate;
select processingUnitSeq into vPUSeq from CS_PlRun where pipelineRunSeq = :pPlRunSeq;

CALL EXT.TRYG_LOG(v_procName,'Begin '||:v_procName || ' for period: '|| :v_periodRow.name,NULL);

select SUBSTR_AFTER(per.name, ' ' ) into vYear from cs_period per
join cs_periodtype pt on
pt.periodtypeseq=per.periodtypeseq
where per.periodseq=:vPeriodSeq 
and pt.name='month'
and per.removedate=:v_removeDate
and per.removedate=:v_removeDate;

delete from ext.TRYG_BROKER_SALE;

/* Load Redemption Grade incentives into incentive prestage*/

insert into ext.TRYG_BROKER_SALE
(
select distinct POLICE, PAID_DATE , 
-- ROW_NUMBER() OVER (partition by police, paid_date ORDER BY subline) row,
FIRST_VALUE(GL_PREAMIE) OVER (PARTITION BY police, paid_date ORDER BY subline) AS GL_PRAEMIE,
-- GL_PREAMIE,
first_VALUE(NY_PREAMIE) OVER (PARTITION BY police, paid_date ORDER BY subline desc) AS NY_PRAEMIE
-- NY_PREAMIE
from
(
select distinct
  st.alternateordernumber as POLICE,
  st.linenumber,to_number(left(st.sublinenumber,14)) SUBLINE,
  case when st.genericboolean5=1 
  and st_002.alternateordernumber||'-'||st_002.sublinenumber||'-'||st_002.linenumber = st.genericattribute21 then 
  to_varchar(st_002.compensationdate,'dd/mm/yyyy') else null end as PAID_DATE,
  round(st.genericnumber1,0) as GL_PREAMIE,
  round(st.genericnumber2,0) as NY_PREAMIE
from cs_salestransaction st 
left join cs_credit c on st.salestransactionseq=c.salestransactionseq
left join cs_salestransaction st_002 on st_002.alternateordernumber||'-'||st_002.sublinenumber||'-'||st_002.linenumber = st.genericattribute21 and st.alternateordernumber=st_002.alternateordernumber
left join cs_transactionassignment ta on ta.salestransactionseq=st.salestransactionseq and ta.salestransactionseq=c.salestransactionseq 
inner join cs_position pos on pos.name=ta.positionname and pos.ruleelementownerseq=c.positionseq and pos.removedate =:v_removeDate
inner join cs_payee pay on pay.payeeseq=pos.payeeseq and pay.removedate =:v_removeDate
inner join cs_participant par on par.payeeseq=pos.payeeseq and par.payeeseq=pay.payeeseq and par.removedate =:v_removeDate
inner join cs_title ti on ti.ruleelementownerseq=pos.titleseq and ti.removedate =:v_removeDate
where c.periodseq=:vPeriodSeq
and st.compensationdate >=:v_periodRow.startDate
and st.compensationdate < :v_periodRow.endDate
and ta.compensationdate >=:v_periodRow.startDate
and ta.compensationdate < :v_periodRow.endDate
and st.genericboolean5=1
---paid flag
and st.eventtypeseq=:v_etSumm.datatypeseq
and st_002.eventtypeseq=:v_etpay.datatypeseq
and pay.effectivestartdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
and pos.effectivestartdate between ti.effectivestartdate and add_days(ti.effectiveenddate,-1)
and ti.name like '%IKC'
order by st.alternateordernumber, st.linenumber,to_number(left(st.sublinenumber,14))
)
);

commit;

if vFname ='' then
	FILENAME := 'MAEGLERSALG_' || replace(:v_periodRow.name,' ','_') || '.txt'; --'MÆGLERSALG.txt';
else
	FILENAME:=vFname;
end if;

commit;

CALL EXT.TRYG_LOG(v_procName,'FILENAME '|| :FILENAME ,NULL);

CALL EXT.TRYG_LOG(v_procName,'End '||:v_procName ,NULL);

END