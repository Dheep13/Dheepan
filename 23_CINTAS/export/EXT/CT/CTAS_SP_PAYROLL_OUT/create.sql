CREATE PROCEDURE EXT.CTAS_SP_PAYROLL_OUT(OUT FILENAME varchar(255), IN plRunSeq varchar(255) default '',IN pPeriodName varchar(255) default '' ) 
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for the inbound Payroll Outbound

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       01-AUG-2023		Deepan		     Initial creation
	1.1		  22-SEP-2023       Deepan           Enddate correction & some comments added

***************************************************************************************************/

DECLARE v_fireServPay_cd varchar2(30);
DECLARE v_fireServPay varchar2(30);
DECLARE v_nonSalesComm_cd varchar2(30);
DECLARE v_nonSalesComm varchar2(30);
DECLARE v_salesComm_cd varchar2(30);
DECLARE v_salesComm varchar2(30);
DECLARE v_eot date := to_date('01-JAN-2200', 'DD-MON-YYYY');
DECLARE v_firePinnacleClub_cd varchar2(30);
DECLARE v_firePinnacleClub varchar2(30);
DECLARE v_fireNatlIncr_cd varchar2(30);
DECLARE v_fireNatlIncr varchar2(30);
DECLARE v_currency varchar2(3);
DECLARE v_qtrStartDate date;
DECLARE v_qtrEndDate date;
DECLARE v_weeksInQtr integer;
DECLARE v_monStartDate date;
DECLARE v_monEndDate date;
DECLARE v_weeksInMonth integer;
DECLARE v_proc_name varchar2(50);
DECLARE v_datemmyyyy VARCHAR(10);
DECLARE v_periodName varchar2(50);
DECLARE vPeriodSeq bigint;
DECLARE dynamic_sql NVARCHAR(1000);
DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
DECLARE v_parentPeriodRow ROW LIKE TCMP.CS_PERIOD;

DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 
    BEGIN 
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

v_fireServPay_cd= '1400';
v_fireServPay= 'Fire Service Pay';
v_nonSalesComm_cd= '1402';
v_nonSalesComm= 'Non Sales Commission';
v_salesComm_cd= '1421';
v_salesComm= 'Sales Commission';
v_firePinnacleClub_cd = '1427';
v_firePinnacleClub = 'Fire Pinnacle Club Commission';
v_fireNatlIncr_cd= '1429';
v_fireNatlIncr= 'Fire National Accounts';
v_currency= 'USD';
v_proc_name :=::CURRENT_OBJECT_NAME;

ext.ctas_event_log (:v_proc_name,'Start',0);

SELECT CASE
        WHEN LENGTH(MONTH(CURRENT_DATE)) = 1 THEN CAST('0' || MONTH(CURRENT_DATE) || YEAR(CURRENT_DATE) AS VARCHAR)
        ELSE CAST(MONTH(CURRENT_DATE) || YEAR(CURRENT_DATE) AS VARCHAR) END
    INTO v_datemmyyyy FROM sys.DUMMY;
    
select :v_datemmyyyy from sys.dummy;
--get period info using plrunseq
if (:plRunSeq !='') then
select periodseq into vPeriodSeq from cs_plrun where pipelinerunseq=:plRunSeq;
select * into v_periodRow from CS_Period where periodseq = :vPeriodSeq and removedate=:v_eot;
select * into v_parentPeriodRow from CS_Period where periodseq=:v_periodRow.parentseq and removedate=:v_eot;
end if;
--get period info using period name
if (:pPeriodName !='') then
select * into v_periodRow from CS_Period where name = :pPeriodName and removedate=:v_eot;
select * into v_parentPeriodRow from CS_Period where periodseq=:v_periodRow.parentseq and removedate=:v_eot;
end if;

truncate table ext.ctas_payroll_out;
--get all payment info for week and month level periods and calculate number of weeks in month and quarter
insert into ext.ctas_payroll_out 
select pos.payeeid as PartnerId ,
REPLACE(CAST(per.enddate AS VARCHAR(10)), '-', '') as PayperiodEndDate ,
case when pay.earningcodeid = :v_fireServPay then :v_fireServPay_cd
	 when pay.earningcodeid = :v_nonSalesComm then :v_nonSalesComm_cd
	 when pay.earningcodeid = :v_salesComm then :v_salesComm_cd
	 when pay.earningcodeid = :v_firePinnacleClub then :v_firePinnacleClub_cd
	 when pay.earningcodeid = :v_fireNatlIncr then :v_fireNatlIncr_cd
	 else '1000' 
	 end as EarningCode,
	 sum(pay.value) as amount,
	 :v_currency as currency,
case when pay.earningcodeid = :v_fireServPay then NULL
	 when pay.earningcodeid = :v_nonSalesComm then vm.totalweeks
	 when pay.earningcodeid = :v_salesComm then vq.totalweeks
	 when pay.earningcodeid = :v_salesComm then vm.totalweeks
	 when pay.earningcodeid = :v_firePinnacleClub then vm.totalweeks
	 when pay.earningcodeid = :v_fireNatlIncr then NULL
	 else '1000' end as numberofweeks,
	 --ifnull(pay.postpipelinerunseq,pay.trialpipelinerunseq) as PayrollFileIdentifier
	 max(pay.paymentseq) as PayrollFileIdentifier
from cs_payment pay
inner join
cs_payee pos on
pos.payeeseq=pay.payeeseq
inner join cs_period per on
per.periodseq=pay.periodseq
inner join ext.ctas_vw_WeeksInmonth vm on
per.enddate between vm.startdate and add_days(vm.enddate,-1)
inner join ext.ctas_vw_WeeksInquarter vq on
per.enddate between vq.startdate and add_days(vq.enddate,-1)
and vm.enddate between vq.startdate and add_days(vq.enddate,-1)
where pos.removedate =:v_eot
and per.removedate = :v_eot
and per.name in (:v_periodRow.name, :v_parentPeriodRow.name)
and per.calendarseq=(select calendarseq from cs_calendar where name='Cintas Hybrid Calendar' and removedate=:v_eot)
and pay.earningcodeid in (
						:v_fireServPay, 
						:v_nonSalesComm, 
						:v_salesComm ,
						:v_firePinnacleClub, 
						:v_fireNatlIncr)
						group by pay.earningcodeid, pos.payeeid,vm.totalweeks,vq.totalweeks,per.enddate,:v_currency;

ext.ctas_event_log (:v_proc_name,'ECPAYROLL extract generated',0);

ext.ctas_event_log (:v_proc_name,'End',0);

FILENAME := 'ECPAYROLL_DEV_'||:v_datemmyyyy||'_Draft.txt';
commit;
end