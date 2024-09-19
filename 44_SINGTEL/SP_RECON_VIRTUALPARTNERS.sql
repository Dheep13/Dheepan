--------------------------------------------------------
--  DDL for Procedure SP_RECON_VIRTUALPARTNERS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "SP_RECON_VIRTUALPARTNERS" 
  AS 

p_txnmonth date;
p_semimonth varchar2(40);
v_periodseq number;
   v_inbound_cfg_parameter   inbound_cfg_parameter%rowtype;
   v_rowcount integer:= null;
v_proc_name varchar2(127):='SP_RECON_VIRTUALPARTNERS';

begin
   select * into v_inbound_cfg_parameter from inbound_cfg_parameter;

select to_Date(max(field1)||'-01','YYYY-MM-DD')
into p_txnmonth
from inbound_data_Staging
;

select  nvl(max(field2), 'B')
into p_semimonth
from inbound_data_Staging
;


 SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Proc Started:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Month, semiMonth ' || to_Char(p_txnmonth,'YYYYMMDD') || ' ' || p_semimonth,
         v_rowcount,
         NULL,
         null);  





commit;



execute immediate 'truncate table stel_temp_midmonthpayees';
insert into stel_temp_midmonthpayees
SELECT  nvl(p.payeeid,dim0) dim0 , value cutoff
                         FROM stel_lookup@stelext s
                         join stel_participant@stelext p
                         on p.lastname=dim0
                         and p_txnmonth between p.effectivestartdate and p.effectiveenddate-1
                        WHERE     s.name = 'LT_VirtualPartners_Rates'
                              AND s.dim1 = 'Mid Month Cut Off'
                              AND s.dim2 LIKE 'Top Up Revenue%' and value<>0
                              and p_txnmonth between s.effectivestartdate and s.effectiveenddate-1
                              ;


    v_rowcount := SQL%ROWCOUNT;

 SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Insert mid month payees:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'MidMonthPAyees' || to_Char(p_txnmonth,'YYYYMMDD') || ' ' || p_semimonth,
         v_rowcount,
         NULL,
         null);  


                              commit;



update STEL_DATA_VPRECONSUMMARY
set period = 'B'
where period is null and last_Day(transactionmonth) =last_Day(p_Txnmonth) ;


update STEL_DATA_VENDORDETAIL
set period = 'B'
where period is null and last_Day(transactiondate) =last_Day(p_Txnmonth) ;

update STEL_DATA_VENDORDETAIL x
set period = 'A'
where payeeid in (Select a.payeeid from stel_temp_midmonthpayees a)
and  to_char(transactiondate,'DD')<= (Select max(cutoff) from  stel_temp_midmonthpayees a where a.payeeid=x.payeeid)
and  last_Day(transactiondate) =last_Day(p_Txnmonth);


update STEL_DATA_TOPUPITDM
set recondate=bizdate, period='B'
where last_Day(bizdate) =last_Day(p_Txnmonth) or last_Day(txndate) =last_Day(p_Txnmonth)
;
update STEL_DATA_TOPUPITDM
set recondate=txndate
where payeeid in (Select value from inbound_Cfg_genericparameter where key='VPRECON_EXCEPTION')
and ( last_Day(bizdate) =last_Day(p_Txnmonth) or last_Day(txndate) =last_Day(p_Txnmonth) );

update STEL_DATA_TOPUPITDM x
set period = 'A'
where payeeid in (Select a.payeeid from stel_temp_midmonthpayees a)
and  to_char(recondate,'DD')<= (Select max(cutoff) from  stel_temp_midmonthpayees a where a.payeeid=x.payeeid)
and  last_Day(recondate) =last_Day(p_Txnmonth);


update STEL_DATA_TOPUPECMS
set period='B'
where last_Day(dateofevent) =last_Day(p_Txnmonth);


update STEL_DATA_TOPUPECMS x
set period = 'A'
where payeeid in (Select a.payeeid from stel_temp_midmonthpayees a)
and  to_char(dateofevent,'DD')<= (Select max(cutoff) from  stel_temp_midmonthpayees a where a.payeeid=x.payeeid)
and  last_Day(dateofevent) =last_Day(p_Txnmonth);




/*****************************
SUMMARY RECON
******************************/

delete from STEL_DATA_VPRECONSUMMARY 
where last_day(transactionmonth) =last_day(p_Txnmonth) and diff<>0  and period = p_semimonth ;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Delete last day transactions in STEL_DATA_VPRECONSUMMARY :'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Delete last day transactions in STEL_DATA_VPRECONSUMMARY Execution Completed txnmonht:'||p_txnmonth,
         v_rowcount,
         NULL,
         null);  

insert into STEL_DATA_VPRECONSUMMARY (period, Transactionmonth, payeeid, itdmamount, ecmsamount, adjamount, vsumamount)
select M.period, M.TransactionMonth, M.payeeid
  , nvl(itdm.topupamount,0) itdmAmount
  , nvl(ecms.topupamount,0) ecmsAmount
  , nvl(adj.topupamount,0) AdjAmount
  , nvl(vsum.topupamount,0) VsummaryAmount
  from
(

select distinct period, transactionmonth, payeeid, topupamount from (
select distinct period, last_day(p_Txnmonth) TransactionMonth, payeeid, 0 topupamount
from stel_data_topupitdm where nvl(reconciled,0)=0  and recordstatus=0
and last_day(recondate) =last_day(p_Txnmonth)  
and sourceid <> '30'
and period=p_semimonth

union
select distinct  period, last_day(p_Txnmonth) TransactionMonth, payeeid, 0 topupamount
from stel_data_vendorsummary where nvl(reconciled,0)=0  and recordstatus=0
and  last_day(transactionmonth) =last_day(p_Txnmonth) 
and period=p_semimonth)

) M
left join
(select period, last_day(recondate) TransactionMonth, payeeid, sum(val) topupamount
from stel_data_topupitdm
where last_day(recondate) =last_day(p_Txnmonth)  and recordstatus=0
group by  last_day(recondate) , payeeid, period

) itdm
on M.transactionmonth=itdm.transactionmonth and m.payeeid=itdm.payeeid and m.period=itdm.period
left join
(select period, last_day(dateofevent) TransactionMonth, payeeid, sum(transactamt) topupamount
from stel_data_topupecms
where last_day(dateofevent) =last_day(p_Txnmonth)  and recordstatus=0
and period=p_semimonth
group by  last_day(dateofevent) , payeeid, period

) ecms
on M.transactionmonth=ecms.transactionmonth and m.payeeid=ecms.payeeid and m.period=ecms.period
left join
(select p_semimonth period, last_day(compensationdate) TransactionMonth, positionname payeeid, sum(val) topupamount
from stel_data_topupadjustments
where last_day(compensationdate) =last_day(p_Txnmonth) 
and positionname not in (select payeeid from stel_Temp_midmonthpayees)
and p_semimonth='B'
group by  last_day(compensationdate) , positionname) adj
on M.transactionmonth=adj.transactionmonth and m.payeeid=adj.payeeid and m.period=adj.period
left join
(select period, last_day(TransactionMonth) TransactionMonth, payeeid, sum(amount) topupamount
from stel_data_vendorsummary
where last_day(TransactionMonth) =last_day(p_Txnmonth)  and recordstatus=0
--exclude previously reconciled ones

group by  last_day(TransactionMonth) , payeeid, period) vsum
on M.transactionmonth=vsum.transactionmonth and m.payeeid=vsum.payeeid and m.period=vsum.period
where   m.payeeid not in 
  (select nvl(payeeid,'x') from stel_Data_vpreconsummary where diff=0 and last_Day(transactionmonth) = last_Day(p_txnmonth) and period=p_semimonth)
;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Insert into STEL_DATA_VPRECONSUMMARY :'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Insert into STEL_DATA_VPRECONSUMMARY Execution Completed',
         v_rowcount,
         NULL,
         null);  

commit;

/*****************************
CALCULATE DIFFERENCE
******************************/

update  STEL_DATA_VPRECONSUMMARY
set diff = nvl(itdmamount+ecmsamount+Adjamount-vsumamount,0)
where transactionmonth=last_Day(p_txnmonth) and period=p_semimonth;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update diff STEL_DATA_VPRECONSUMMARY :'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update diff STEL_DATA_VPRECONSUMMARY Execution Completed',
         v_rowcount,
         NULL,
         null);  

/*****************************
MARK RECORDS AS RECONCILED
******************************/

merge into stel_data_topupitdm tgt
using (Select * from STEL_DATA_VPRECONSUMMARY where diff=0 and period=p_semimonth) src
on (tgt.payeeid=src.payeeid and last_day(src.transactionmonth)=last_Day(tgt.recondate) )
when matched then update set tgt.reconciled=1
where    recordstatus=0 and period = p_semimonth and reconciled=0;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Mark Reconciled reocrds in stel_data_topupitdm :'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Mark Reconciled reocrds in stel_data_topupitdm Execution Completed',
         v_rowcount,
         NULL,
         null);  

merge into stel_data_vendorsummary tgt
using (Select * from STEL_DATA_VPRECONSUMMARY where diff=0 and period=p_semimonth) src
on (tgt.payeeid=src.payeeid and src.transactionmonth=tgt.transactionmonth)
when matched then update set tgt.reconciled=1
where    recordstatus=0
;

   v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'set reconcile to 1 in stel_data_vendorsummary :'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'set reconcile to 1 in stel_data_vendorsummary Execution Completed',
         v_rowcount,
         NULL,
         null);  

commit; 





/*****************************
START DETAIL RECON
******************************/

delete from STEL_DATA_VPRECONDETAIL
where last_Day(transactiondate)=last_day(p_txnmonth) and (period=p_semimonth or period is null);
commit;

   v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Delete last day txn in STEL_DATA_VPRECONDETAIL:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Delete last day txn in STEL_DATA_VPRECONDETAIL Execution Completed',
         v_rowcount,
         NULL,
         null);  

insert into STEL_DATA_VPRECONDETAIL (DATASOURCE, TransactionID, TransactionDate, Payeeid, Amount, Phone, TopUpType, period )
select  'ITDM', sourceseq, recondate, payeeid, val, msisdn, card_Group, p_semimonth
from stel_Data_topupitdm
where last_day(recondate)=last_day(p_txnmonth)  and recordstatus=0 and payeeid in 
        (select payeeid from STEL_DATA_VPRECONSUMMARY 
            where last_day(transactionmonth)=last_day(p_txnmonth)  and diff<>0   
            ) and period = p_semimonth

 union all
select  'ECMS', nvl(origintransactionid,'')||'-'||nvl(voucherserial,'') , dateofevent, payeeid, transactamt, subscriberid, vouchergroup,p_semimonth
from stel_Data_topupecms
where last_day(dateofevent)=last_day(p_txnmonth)  and recordstatus=0 and (payeeid in 
        (select payeeid from STEL_DATA_VPRECONSUMMARY 
            where last_day(transactionmonth)=last_day(p_txnmonth) and diff<>0
            ) or payeeid is null) and period = p_semimonth

 union all
 select 'ADJ', orderid, compensationdate, positionname payeeid, val, contact, genericattribute2 card_Group,p_semimonth
from stel_Data_topupadjustments
where last_day(compensationdate)=last_day(p_txnmonth) and positionname in 
        (select payeeid from STEL_DATA_VPRECONSUMMARY 
            where last_day(transactionmonth)=last_day(p_txnmonth) and diff<>0
            )
  union all

select  'VENDORDTL', transactionid , transactiondate, payeeid, amount, phone, topuptype,p_semimonth
from stel_Data_vendordetail
where last_day(transactiondate)=last_day(p_txnmonth)  and recordstatus=0 and payeeid in 
        (select payeeid from STEL_DATA_VPRECONSUMMARY 
            where last_day(transactionmonth)=last_day(p_txnmonth) and diff<>0
            )    and period = p_semimonth  ;        

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Insert into STEL_DATA_VPRECONDETAIL:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Insert into STEL_DATA_VPRECONDETAIL Execution Completed',
         v_rowcount,
         NULL,
         null);              

commit;


/*****************************
COMPARE AGAINST VENDOR DETAIL
******************************/
--fix for duplicates - aggregate across transactionid 

merge into STEL_DATA_VPRECONDETAIL tgt
using (
select  a.payeeid, a.transactiondate, a.phone, b.transactionid, a.amount
from
  (
  select   payeeid, transactiondate, phone,  amount
  from STEL_DATA_VPRECONDETAIL

  where last_Day(transactiondate)= last_day(p_txnmonth)  and  period = p_semimonth
  and datasource<>'VENDORDTL'
  group by payeeid, transactiondate, phone, amount
  ) a
  join  
  (
  select
  payeeid, transactiondate,  amount, phone
  , listagg(transactionid,',') within group (order by transactionid) transactionid
  from stel_Data_vendordetail
  where last_Day(transactiondate)= last_day(p_txnmonth)  and recordstatus=0 and period = p_semimonth
  group by payeeid, transactiondate, phone, amount
  ) b
  on a.payeeid=b.payeeid and a.transactiondate=b.transactiondate and a.phone=b.phone
  and a.amount=b.amount
) src
on (tgt.payeeid=src.payeeid 
and tgt.transactiondate=src.transactiondate 
and tgt.amount=src.amount 
and src.phone=tgt.phone)
when matched then update set 
  tgt.reconciled=1, tgt.vendordetailmatch = src.transactionid||','
where last_day(tgt.transactiondate) = last_day(p_txnmonth) and datasource<>'VENDORDTL' and period = p_semimonth;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL Execution Completed',
         v_rowcount,
         NULL,
         null);  
/*
 update STEL_DATA_VPRECONDETAIL tgt
 set reconciled=1
 where tgt.datasource='VENDORDTL' and last_day(tgt.transactiondate) = last_day(p_txnmonth) and period = p_semimonth
 and 
 exists 
 (select 1 from STEL_DATA_VPRECONDETAIL a where a.datasource<>'VENDORDTL' and last_day(a.transactiondate) = last_day(p_txnmonth)
 and a.reconciled=1 and period = p_semimonth
 and instr(a.vendordetailmatch,tgt.transactionid||',')>0
 );
*/
/*
merge into  /*+ use_nl(src,tgt) FULL(tgt) *  STEL_DATA_VPRECONDETAIL tgt
using (
select /*+ INDEX(a,STEL_DATA_VPRECONDETAIL_INDEX2)    * vendordetailmatch from STEL_DATA_VPRECONDETAIL a where a.datasource<>'VENDORDTL'  
 and a.reconciled=1 and last_day(a.transactiondate) = last_day(p_txnmonth) and 
 period = p_semimonth and a.vendordetailmatch is not null
) src
on ( instr(src.vendordetailmatch,tgt.transactionid||',')>0)
when matched then update set tgt.reconciled=1
where tgt.datasource='VENDORDTL' and last_day(tgt.transactiondate) = last_day(p_txnmonth) and period = p_semimonth;
*/

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL Execution Completed',
         v_rowcount,
         NULL,
         null);  


/*
merge into STEL_DATA_VPRECONDETAIL tgt
using (select payeeid, transactiondate, sum(amount) amount, phone, transactionid  from stel_Data_vendordetail 
where last_Day(transactiondate)= last_day(p_txnmonth)  and recordstatus=0


group by  payeeid, transactiondate,   phone , transactionid
) src
on (tgt.payeeid=src.payeeid 
and tgt.transactiondate=src.transactiondate 
and tgt.amount=src.amount 
and src.phone=tgt.phone)
when matched then update set 
tgt.vendordetailmatch = src.transactionid, tgt.reconciled=1
where last_day(tgt.transactiondate) = last_day(p_txnmonth);
*/


commit;
-- update VDTL table back as reconciled where there is a match above

merge into STEL_DATA_TOPUPECMS tgt
using (select   transactiondate, amount, phone , count(*), max(payeeid) payeeid
        from stel_Data_vprecondetail where last_Day(transactiondate)= last_day(p_txnmonth)   
    and datasource='VENDORDTL' and nvl(reconciled,0)=0 and period = p_semimonth
    group by  transactiondate, amount, phone
) src
on (tgt.dateofevent=src.transactiondate and tgt.transactamt=src.amount and src.phone=tgt.subscriberid)
when matched then update set 
tgt.payeeid = src.payeeid
where last_day(tgt.dateofevent) = last_day(p_txnmonth)  and recordstatus=0 and period = p_semimonth;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update PayeeId in STEL_DATA_TOPUPECMS:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update PayeeId in STEL_DATA_TOPUPECMS Execution Completed',
         v_rowcount,
         NULL,
         null);  

commit;



/****************Do again after ECMS************/




merge into STEL_DATA_VPRECONDETAIL tgt
using (
select  a.payeeid, a.transactiondate, a.phone, b.transactionid, a.amount
from
  (
  select   payeeid, transactiondate, phone,  amount
  from STEL_DATA_VPRECONDETAIL

  where last_Day(transactiondate)= last_day(p_txnmonth)  
  and datasource<>'VENDORDTL' and period = p_semimonth
  group by payeeid, transactiondate, phone, amount
  ) a
  join  
  (
  select
  payeeid, transactiondate,  amount, phone
  , listagg(transactionid,',') within group (order by transactionid) transactionid
  from stel_Data_vendordetail
  where last_Day(transactiondate)= last_day(p_txnmonth)  and recordstatus=0 and period = p_semimonth
  group by payeeid, transactiondate, phone, amount
  ) b
  on a.payeeid=b.payeeid and a.transactiondate=b.transactiondate and a.phone=b.phone
  and a.amount=b.amount
) src
on (tgt.payeeid=src.payeeid 
and tgt.transactiondate=src.transactiondate 
and tgt.amount=src.amount 
and src.phone=tgt.phone)
when matched then update set 
  tgt.reconciled=1, tgt.vendordetailmatch = src.transactionid||','
where last_day(tgt.transactiondate) = last_day(p_txnmonth) and datasource<>'VENDORDTL' and period = p_semimonth;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL Execution Completed',
         v_rowcount,
         NULL,
         null);  
/*
 update STEL_DATA_VPRECONDETAIL tgt
 set reconciled=1
 where tgt.datasource='VENDORDTL' and last_day(tgt.transactiondate) = last_day(p_txnmonth) and period = p_semimonth
 and 
 exists 
 (select 1 from STEL_DATA_VPRECONDETAIL a where a.datasource<>'VENDORDTL' and last_day(a.transactiondate) = last_day(p_txnmonth)
 and a.reconciled=1 and period = p_semimonth
 and instr(a.vendordetailmatch,tgt.transactionid||',')>0
 );
*/
    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL Execution Completed',
         v_rowcount,
         NULL,
         null);  


/*************************/

select pd.periodseq into v_periodseq 
from cs_period@stelext pd
join cs_periodtype@stelext pt
on pt.periodtypeseq=pd.periodtypeseq
and pt.removedate>sysdate
and pt.name='month'
join cs_calendar@stelext c
on c.calendarseq=pd.calendarseq
and c.removedate>sysdate
and c.name like 'Singtel%Month%'
where
 pd.removedate>sysdate
and p_txnmonth between pd.startdate and pd.enddate-1 ;


  STEL_PROC_RPT_PARTITIONS_PSEQ@stelext(v_periodseq,'stel_rpt_data_vpreconsummary');
  STEL_PROC_RPT_PARTITIONS_PSEQ@stelext(v_periodseq,'stel_rpt_data_vprecondetail');


delete from STEL_RPT_DATA_VPRECONSUMMARY@STELEXT
where periodseq=v_periodseq;

INSERT
INTO STEL_RPT_DATA_VPRECONSUMMARY@STELEXT
  (
    PERIODSEQ,    POSITIONSEQ,
    PERIODNAME,    TRANSACTIONMONTH,    PAYEEID,
    ITDMAMOUNT,    ECMSAMOUNT,    ADJAMOUNT,    VSUMAMOUNT,    DIFF,   
    VENDORNAME,    PROCESSINGUNITSEQ,    PROCESSINGUNITNAME,    CALENDARNAME,
    SOURCEPERIODNAME
  )
   SELECT distinct
   pd.periodseq, pos.ruleelementownerseq, pd.name,
   TRANSACTIONMONTH,  PAYEEID,
  ITDMAMOUNT,  ECMSAMOUNT,  ADJAMOUNT,  VSUMAMOUNT,
  DIFF, par.lastname, pu.processingunitseq, pu.name,  c.name, pd.name
FROM STEL_DATA_VPRECONSUMMARY vp
cross join (select * from cs_processingunit@stelext where name ='Singtel_PU') pu
join cs_period@stelext pd
on pd.removedate>sysdate
and pd.enddate-1=vp.transactionmonth
join cs_periodtype@stelext pt
on pt.periodtypeseq=pd.periodtypeseq
and pt.removedate>sysdate
and pt.name='month'
join cs_calendar@stelext c
on c.calendarseq=pd.calendarseq
and c.removedate>sysdate
and c.name like 'Singtel%Month%'
join cs_position@stelext pos
on pos.name=vp.payeeid
and pos.removedate>sysdate
and vp.transactionmonth between pos.effectivestartdate and pos.effectiveenddate-1
join cs_participant@stelext par
on par.payeeseq=pos.payeeseq and par.removedate>sysdate
and vp.transactionmonth between par.effectivestartdate and par.effectiveenddate-1
where diff<>0
and  p_txnmonth between pd.startdate and pd.enddate-1 and vp.period = p_semimonth
;

delete from STEL_RPT_DATA_VPRECONDETAIL@STELEXT
where periodseq=v_periodseq;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Delete STEL_RPT_DATA_VPRECONDETAIL for curr Period:'
            || v_inbound_cfg_parameter.file_type
            || '-FileName:'
            || v_inbound_cfg_parameter.file_name
            || '-Date:'
            || v_inbound_cfg_parameter.file_date,
            1,
            255),
         'Delete STEL_RPT_DATA_VPRECONDETAIL for curr Period Execution Completed',
         v_rowcount,
         NULL,
         null);  
/*
INSERT
INTO STEL_RPT_DATA_VPRECONDETAIL@stelext
  (
    PERIODSEQ,    POSITIONSEQ,    PERIODNAME,
    TRANSACTIONID,    TRANSACTIONDATE,    PAYEEID,    AMOUNT,    PHONE,    TOPUPTYPE,    VENDORDETAILMATCH,
    RECONCILED,    DATASOURCE,    VENDORNAME,    PROCESSINGUNITSEQ,    PROCESSINGUNITNAME,    CALENDARNAME,
    SOURCEPERIODNAME  )


     SELECT 
     pd.periodseq, pos.ruleelementownerseq, pd.name,
     TRANSACTIONID,   TRANSACTIONDATE,  PAYEEID,  AMOUNT,  PHONE,  TOPUPTYPE,  VENDORDETAILMATCH,
  RECONCILED,  DATASOURCE, par.lastname, pu.processingunitseq, pu.name, c.name, pd.name
FROM STEL_DATA_VPRECONDETAIL vp

cross join (select * from cs_processingunit@stelext where name ='Singtel_PU') pu
join cs_period@stelext pd
on pd.removedate>sysdate
and vp.transactiondate between pd.startdate and pd.enddate-1
join cs_periodtype@stelext pt
on pt.periodtypeseq=pd.periodtypeseq
and pt.removedate>sysdate
and pt.name='month'
join cs_calendar@stelext c
on c.calendarseq=pd.calendarseq
and c.removedate>sysdate
and c.name like 'Singtel%Month%'
left join cs_position@stelext pos
on pos.name=vp.payeeid
and pos.removedate>sysdate
and vp.transactiondate between pos.effectivestartdate and pos.effectiveenddate-1
left join cs_participant@stelext par
on par.payeeseq=pos.payeeseq and par.removedate>sysdate
and vp.transactiondate between par.effectivestartdate and par.effectiveenddate-1
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



*/



















END SP_RECON_VIRTUALPARTNERS;
