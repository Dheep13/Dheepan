CREATE OR REPLACE PROCEDURE EXT.CTAS_SP_COPAEXTRACT(OUT FILENAME varchar(120), IN plRunSeq varchar(255) default '', IN pPeriodName varchar(255) default '') 
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for the outbound COPA extract

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       31-JUL-2023		Deepan		     Initial creation
	1.1       22-SEP-2023       Deepan           Added Comments, removed unwanted variables   

***************************************************************************************************/
DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
DECLARE v_parentPeriodRow ROW LIKE TCMP.CS_PERIOD;
DECLARE v_eot date := to_date('01-JAN-2200', 'DD-MON-YYYY');
DECLARE v_proc_name varchar2(100);
DECLARE v_eventtypeid varchar2(10);
DECLARE v_businessUnit varchar2(12);
DECLARE v_currInFile varchar2(100);
DECLARE v_period varchar2(50);
DECLARE v_yyyymmdd varchar2(10);
DECLARE v_businessUnitCode varchar2(10);
DECLARE v_fileType varchar2(50);
DECLARE v_dataTypeseq bigint;
DECLARE vPipelinerunseq bigint;
DECLARE vPeriodSeq bigint;


DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 
    BEGIN 
    rollback;
    ext.ctas_event_log (v_proc_name,::SQL_ERROR_CODE || ' . ' ||::SQL_ERROR_MESSAGE,0);
    commit;
resignal;

END;
DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN 
    rollback;
    ext.ctas_event_log (v_proc_name,::SQL_ERROR_CODE || ' . ' ||::SQL_ERROR_MESSAGE,0);
    commit;
resignal;
END;

v_proc_name :=::CURRENT_OBJECT_NAME;
v_eventtypeid := 'SERVICE';
v_businessUnit := 'FIRE';
v_businessUnitCode := '1020';
v_fileType := 'SPM_Incentive_Upload';
v_currInFile = FILENAME;
-- v_period:=pPeriodName;

ext.ctas_event_log (:v_proc_name,'Start',0);

select case when length(month(current_date))= 1 then year(current_date)||'-'||'0'||month(current_date)||'-'||'27' else 
year(current_date)||'-'||month(current_date)||'-'||'27' end 
into v_yyyymmdd 
from sys.dummy;


--get periodname based on plrunseq
if (:plRunSeq !='') then

select periodseq into vPeriodSeq from cs_plrun where pipelinerunseq=:plRunSeq;
select * into v_periodRow from CS_Period where periodseq = :vPeriodSeq and removedate=:v_eot;
select * into v_parentPeriodRow from CS_Period where periodseq=:v_periodRow.parentseq and removedate=:v_eot;
end if;

--get period information based on input periodname
if (:pPeriodName !='') then

select * into v_periodRow from CS_Period where name = :pPeriodName and removedate=:v_eot;
select * into v_parentPeriodRow from CS_Period where periodseq=:v_periodRow.parentseq and removedate=:v_eot;
end if;

select datatypeseq into v_dataTypeseq from cs_eventtype where eventtypeid='SERVICE' and removedate =:v_eot;
truncate table ext.CTAS_COPA_OUT;
--trace from payments through to salestransaction only for SERVICE eventtype
insert into ext.CTAS_COPA_OUT
(select distinct companycode,
filetype, 
Credit,
CostCenter, 
productid, 
shiptocustid, 
billtocustid,
businessunit,
fiscalperiod
from (
select distinct st.salestransactionseq, 
:v_businessUnitCode as companycode, 
:v_fileType as filetype, 
st.genericattribute18 as CostCenter, 
cast(paytrace.inc_value as varchar) as Credit, 
st.productid, 
case when at.addresstypeid ='SHIPTO' then ta.custid end as shiptocustid, 
case when at.addresstypeid ='BILLTO' then ta.custid end as billtocustid,
:v_businessUnit as businessunit,
:v_yyyymmdd as fiscalperiod
from cs_salestransaction st ,(
select salestransactionseq, inc_value from ext.ctas_vw_trxnpaymenttrace--view
where periodname in (:v_periodRow.name,:v_parentPeriodRow.name)) paytrace,
cs_transactionaddress ta,
cs_addresstype at
where paytrace.salestransactionseq=st.salestransactionseq
and st.eventtypeseq = :v_dataTypeseq
and ta.salestransactionseq=st.salestransactionseq
and ta.salestransactionseq=paytrace.salestransactionseq
and at.addresstypeseq=ta.addresstypeseq
and ta.custid is not null
and at.addresstypeid in ('SHIPTO',
'BILLTO')));

ext.ctas_event_log (:v_proc_name,'COPA extract generated',::ROWCOUNT);
FILENAME := 'COPA_DEV_'||:v_yyyymmdd||'_Draft.txt';

ext.ctas_event_log (:v_proc_name,'End',0);

COMMIT;
END