--------------------------------------------------------
--  DDL for Procedure SP_RECON_VIRTUALPARTNERS_S2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "SP_RECON_VIRTUALPARTNERS_S2" 
AS
   v_inbound_cfg_parameter   INBOUND_CFG_PARAMETER%ROWTYPE;
   p_semimonth varchar2(40);
   p_txnmonth date;
   v_rowcount integer:= null;
v_proc_name varchar2(127):='sp_recon_VirtualPartners_s2';
v_pu number;
v_puname varchar2(50):='Singtel_PU';
BEGIN
   SELECT * INTO v_inbound_cfg_parameter FROM INBOUND_CFG_PARAMETER;


select processingunitseq into v_pu from cs_processingunit@stelext where name=v_puname;

if v_inbound_cfg_parameter.file_type like '%Trigger%' then
select last_day(to_Date(max(field1)||'-01','YYYY-MM-DD'))
into p_txnmonth
from inbound_data_Staging
;
end if;
if v_inbound_cfg_parameter.file_type like '%ITDM%' then
select add_months(last_Day(to_Date(max(field7),'YYYYMMDD'))+1,-1)
into p_txnmonth
from inbound_data_Staging
where field100 is not null
;
end if;

dbms_output.put_line('p_txnmonth'|| p_txnmonth);
select  nvl(max(field2), 'B')
into p_semimonth
from inbound_data_Staging
;




insert into STEL_DATA_VPRECONDETAIL (DATASOURCE, TransactionID, TransactionDate, Payeeid, Amount, Phone, TopUpType, period )
/*select  'ITDM', sourceseq, recondate, 'SHOP-'||payeeid||'-'||dealercode, val, msisdn, card_Group, 'B'
from stel_Data_topupitdm
where last_day(recondate)=last_day(p_txnmonth)  and recordstatus=0 and dealercode in 
        (select payeeid from STEL_DATA_shopRECONSUMMARY 
            where last_day(transactionmonth)=last_day(p_txnmonth)  and diff<>0   
            ) --and period = p_semimonth;
;
*/


select a.datasource, a.transactionid  , a.transactiondate, a.payeeid, a.amount,  phone, card_group, 'B'
from stel_Data_shoprecondetail a

where last_day(transactiondate)=last_day(p_txnmonth)  and a.datasource in ('ITDM','MTPOS');

/*

insert into STEL_DATA_VPRECONDETAIL (DATASOURCE, TransactionID, TransactionDate, Payeeid, Amount, Phone, TopUpType, period )

select a.datasource, b.receiptno  , a.transactiondate, a.payeeid, b.rrp, b.serviceno phone, null , 'B'
from stel_Data_shoprecondetail a
join stel_Data_salesorder b
on b.dealercode=a.payeeid and a.transactiondate=trunc(b.txndate)
where last_day(transactiondate)=last_day(p_txnmonth)  and a.datasource='MTPOS'
and b.recordstatus=0 and b.departmentcode='INSTORETOP-UP'
;
*/


INSERT
INTO STEL_RPT_DATA_VPRECONDETAIL@stelext
  (
    PERIODSEQ,    POSITIONSEQ,    PERIODNAME,
    TRANSACTIONID,    TRANSACTIONDATE,    PAYEEID,    AMOUNT,    PHONE,    TOPUPTYPE,    VENDORDETAILMATCH,
    RECONCILED,    DATASOURCE,    VENDORNAME,    PROCESSINGUNITSEQ,    PROCESSINGUNITNAME,    CALENDARNAME,
    SOURCEPERIODNAME  )


     SELECT distinct
     pd.periodseq, nvl(pos.ruleelementownerseq,pos2.ruleelementownerseq) , pd.name,
     TRANSACTIONID,   TRANSACTIONDATE, replace( PAYEEID,'SHOP-',''),  AMOUNT,  PHONE,  TOPUPTYPE,  VENDORDETAILMATCH,
  RECONCILED,  DATASOURCE, par.lastname, v_pu, v_puname, c.name, pd.name
FROM STEL_DATA_VPRECONDETAIL vp


join cs_period@stelext pd
on pd.removedate>sysdate
and vp.transactiondate between pd.startdate and pd.enddate-1
and pd.tenantid='STEL'
join cs_periodtype@stelext pt
on pt.periodtypeseq=pd.periodtypeseq
and pt.removedate=to_Date('22000101','YYYYMMDD')
and pt.name='month'
and pt.tenantid='STEL'
join cs_calendar@stelext c
on c.calendarseq=pd.calendarseq
and c.removedate=to_Date('22000101','YYYYMMDD')
and c.tenantid='STEL'
and c.name = 'Singtel Monthly Calendar'
left join cs_position@stelext pos
on pos.name=vp.payeeid
and pos.removedate>sysdate
and vp.transactiondate between pos.effectivestartdate and pos.effectiveenddate-1
and pos.tenantid='STEL'
left join cs_position@stelext pos2
on 'SHOP-'||pos2.name=substr(vp.payeeid,1,instr(vp.payeeid,'-',-1)-1)
and pos2.removedate>sysdate
and vp.transactiondate between pos2.effectivestartdate and pos2.effectiveenddate-1
and pos2.tenantid='STEL'
left join cs_participant@stelext par
on par.payeeseq=nvl(pos.payeeseq,pos2.payeeseq) and par.removedate>sysdate
and vp.transactiondate between par.effectivestartdate and par.effectiveenddate-1
and par.tenantid='STEL'
where vp.reconciled is null and
 p_txnmonth between pd.startdate and pd.enddate-1 and vp.period = p_semimonth
;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Insert into STEL_RPT_DATA_VPRECONDETAIL:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Insert into STEL_RPT_DATA_VPRECONDETAIL Execution Completed',
         v_rowcount,
         NULL,
         null);       

   commit;  









--For Reconciled Records  in this txnmonth
-- where reconciled=1  



-- update tables based on inb parameter
update stel_data_topupitdm
set reconciled=2, filedate=v_inbound_cfg_parameter.file_date, filename = v_inbound_cfg_parameter.file_name
where 
 last_Day(recondate) =P_txnmonth
and (reconciled=1 or payeeid in (
    select par.payeeid  from stel_lookup@stelext lkp, stel_participant@stelext par
    where lkp.name like 'LT%Vi%' and lkp.dim1='Skip Recon' and lkp.value=1
    and par.lastname = lkp.dim0
    and p_txnmonth between par.effectivestartdate and par.effectiveenddate-1)

OR payeeid in (
    select p.payeeid from cs_payee@stelext p
    join cs_position@stelext pos
    on pos.payeeseq=p.payeeseq
    join cs_title@stelext t
    on t.ruleelementownerseq=pos.titleseq
    where t.removedate>sysdate and pos.removedate>sysdate and p.removedate>sysdate
    and t.effectiveenddate>p_txnmonth and p_txnmonth>=t.effectivestartdate
    and p.effectiveenddate>p_txnmonth and p_txnmonth>=p.effectivestartdate
    and pos.effectiveenddate>p_txnmonth and p_txnmonth>=pos.effectivestartdate
    and t.name like 'Pick%Go%'
    )
    )

;


    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update stel_data_topupitdm recon 2:' || to_char(p_txnmonth,'YYYYMMDD')
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update stel_data_topupitdm Execution Completed',
         v_rowcount,
         NULL,
         null);   

/*
insert into stel_data_topupitdm
select
FILEDATE
,FILENAME
,RECORDSTATUS
,MSISDN
,SOURCEID
,SOURCESEQ
,VAL
,CARD_GROUP
,TXNDATE
,BIZDATE
,y.genericattribute2 PAYEEID
,RECONDATE
,'2' RECONCILED
,DEALERCODE
,FILETYPE
,PERIOD
from stel_data_topupitdm x
join (Select * from stel_Classifier@stelext where categoryname='OPID-AM' ) y
on x.sourceid=y.genericattribute1 and recondate between effectivestartdate and effectiveenddate-1
where last_Day(recondate) =P_txnmonth

;*/
/*
update stel_data_topupecms
set   filedate=v_inbound_cfg_parameter.file_name, filename = v_inbound_cfg_parameter.file_date
where payeeid is not null;
*/
commit;

--triiger stage 2


   sp_inbound_txn_map ('ITDM-SCII-PrepaidTopup',
                       v_inbound_cfg_parameter.file_name,
                       v_inbound_cfg_parameter.file_date,
                       2);



      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'ITDM S2 done:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'ITDM S2',
         0,
         NULL,
         null);       

dbms_output.put_line('ITDM Done S2');
   sp_inbound_txn_map ('ECMS-SCII-ManualTopup',
                       v_inbound_cfg_parameter.file_name,
                       v_inbound_cfg_parameter.file_date,
                       2);                       



      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'ECMS S2 done:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'ECMS S2',
         0,
         NULL,
         null);    

         dbms_output.put_line('ECMS Done S2');


commit;


update stel_data_topupitdm
set reconciled=3, filedate=v_inbound_cfg_parameter.file_date, filename = v_inbound_cfg_parameter.file_name
where (reconciled=2 or ( 
    payeeid in (
    select par.payeeid  from stel_lookup@stelext lkp, stel_participant@stelext par
    where lkp.name like 'LT%Vi%' and lkp.dim1='Skip Recon' and lkp.value=1
    and par.lastname = lkp.dim0
    and p_txnmonth between par.effectivestartdate and par.effectiveenddate-1
    )
    OR payeeid in (
    select p.payeeid from cs_payee@stelext p
    join cs_position@stelext pos
    on pos.payeeseq=p.payeeseq
    join cs_title@stelext t
    on t.ruleelementownerseq=pos.titleseq
    where t.removedate>sysdate and pos.removedate>sysdate and p.removedate>sysdate
    and t.effectiveenddate>p_txnmonth and p_txnmonth>=t.effectivestartdate
    and p.effectiveenddate>p_txnmonth and p_txnmonth>=p.effectivestartdate
    and pos.effectiveenddate>p_txnmonth and p_txnmonth>=pos.effectivestartdate
    and t.name like 'Pick%Go%'
    )

))  and last_Day(recondate) =P_txnmonth;



    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update stel_data_topupitdm recon 3:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update stel_data_topupitdm Execution Completed',
         v_rowcount,
         NULL,
         null);       


-- ITDM additional assignment
/*
insert into inbound_data_assignment 
(filedate,
filename,
orderid,
linenumber,
sublinenumber,
eventtypeid,
positionname,
recordstatus)
select a.filedate,
a.filename,
a.orderid,
a.linenumber,
a.sublinenumber,
a.eventtypeid,
b.genericattribute2 positionname,
0 recordstatus
from inbound_data_txn a,
stel_classifier@stelext b
where
a.genericattribute4=b.genericattribute1
and b.categoryname='OPID-AM'
--and categorytreename='Internal Prepaid '
and a.compensationdate >= b.effectivestartdate 
and a.compensationdate < b.effectiveenddate 
and a.filename= v_inbound_cfg_parameter.file_name
and a.filedate = v_inbound_cfg_parameter.file_date ;
*/
Commit;


--triiger stage 3

if v_inbound_cfg_parameter.file_type like '%ITDM%' then
   sp_inbound_txn_map ('ITDM-SCII-PrepaidTopup',
                       v_inbound_cfg_parameter.file_name,
                       v_inbound_cfg_parameter.file_date,
                       3);
end if;


commit;
end;
