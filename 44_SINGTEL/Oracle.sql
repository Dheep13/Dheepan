--------------------------------------------------------
--  DDL for Procedure SP_INBOUND_POST_MOBPROCESS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "SP_INBOUND_POST_MOBPROCESS" AS 
 v_param  INBOUND_CFG_PARAMETER%ROWTYPE;
 v_Sql varchar2(4000);
 v_Singlequote varchar2(1):='''';
 v_rowcount integer:=null;
 v_proc_name varchar2(127):='SP_INBOUND_POST_MOBPROCESS';
BEGIN
    SELECT * INTO v_param FROM INBOUND_CFG_PARAMETER;
 execute immediate '  alter session set nls_Date_Format = ''DD-MON-YYYY''' ; 

 execute immediate 'Truncate table LP_STEL_CLASSIFIER drop storage';
 
     SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'LP_STEL_CLASSIFIER BEFORE :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'LP_STEL_CLASSIFIER BEFORE',
         0,
         NULL,
         null);  
         
INSERT INTO LP_STEL_CLASSIFIER
select * from   STEL_CLASSIFIER@STELEXT where  categorytreename in ('StockCode' ,'Singtel');
COMMIT;
     SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'LP_STEL_CLASSIFIER CREATION COMPLETED :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'LP_STEL_CLASSIFIER CREATION COMPLETED',
         0,
         NULL,
         null);  
dbms_stats.gather_table_stats(ownname =>'STELADMIN',
                               tabname => 'LP_STEL_CLASSIFIER',
                                                              cascade => true );
 SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'LP_STEL_CLASSIFIER STATS COMPLETED :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'LP_STEL_CLASSIFIER STATS COMPLETED',
         0,
         NULL,
         null); 
  --customer Id is null, still load in but set comment
  --20190221: leave this here. no need to move to SH. Does not affect comp.
  update  stel_data_txn_mobile
  set comments='Customer ID Missing'
  where filename = v_param.file_name and filedate=v_param.file_Date and recordstatus=0
  and billtocustid is null
  --and channel='SERS'
    ;

       v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update Comments with Customer Missing :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update Comments with Customer Missing Execution Completed',
         v_rowcount,
         NULL,
         null);  



  --SERS Roadshow identification

  /* List of roadhsow dealer codes:
  select distinct classifierid from stel_classifier@stelext
  where categorytreename='Roadshow Codes' AND CATEOGRYNAME='Roadshow'

  We can replace this with a relnship?
  */
  --Dealer Code from Roadshow
  --20190221: Move to SH

  merge into  stel_data_txn_mobile tgt
  using ( 
    select classifierid RdCode, a.Genericattribute2 dlrcode, a.effectivestartdate, a.effectiveenddate, mgr.name VendorCode
    from stel_Classifier@stelext a
    join cs_position@stelext pos
    on pos.name=a.genericattribute2 and pos.removedate>sysdate
    and a.effectiveenddate-1 between pos.effectivestartdate and pos.effectiveenddate-1
    join cs_position@stelext mgr
    on pos.managerseq=mgr.ruleelementownerseq and mgr.removedate>sysdate
    and a.effectiveenddate-1 between mgr.effectivestartdate and mgr.effectiveenddate-1
    where  categorytreename='Roadshow Codes' AND CATEGoRYNAME='Roadshow'
  )src
  on ( tgt.compensationdate between src.effectivestartdate and src.effectiveenddate-1 
      and tgt.genericattribute4 = src.rdCode)
  when matched then update set tgt.tempfield1 = src.dlrCode, tgt.tempfield2 = src.VendorCode
  where filename = v_param.file_name and filedate=v_param.file_Date and recordstatus=0
  ;

     v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update DealerCode and VenderCode :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update DealerCode and VenderCode Execution Completed',
         v_rowcount,
         NULL,
         null);  

  commit;


   --Dealer Code from SalesPerson (Primary Dealer ID - GA1)

 --if ga1 = tempfield1, AI and MSF go to the same shop. (set assignment to be tempfield1 above)
 -- if ga1<>tempfield1, MSF goes to tempfield1 shop, AI goes to GA1 salesperson (assignment is still to dealer code tempfield1 
 -- another assignment to be created for




 --Chane plan as new line
 -- this whole section can go into the post_bcctxn proc
 --on the same day (in the same file)

 --po number has to run first
 --20190221: This is already in RFC SH. Can be removed here?
 merge into  stel_data_txn_mobile tgt
 using (
 select 
  txn.ponumber, max(txn2.orderid) as NewOrdId, txn.orderid
 from  stel_data_txn_mobile txn
 join   stel_data_txn_mobile txn2
 on txn2.genericattribute11 = 'Change Main Plan'
 and txn.ponumber=txn2.ponumber
  where txn.filename = v_param.file_name and txn.filedate=v_param.file_Date and txn.recordstatus=0
  and txn2.filename = v_param.file_name and txn2.filedate=v_param.file_Date and txn2.recordstatus=0
  and txn.genericattribute11='New'
  and txn.genericattribute9='M' and txn2.genericattribute9='M'
 group by txn.ponumber , txn.orderid) src
 on ((tgt.orderid=src.orderid or tgt.orderid=src.newordid) and tgt.ponumber=src.ponumber)
 when matched then update set
 tempfield3 = case when tgt.orderid=src.orderid then 'Replaced with Change Plan' else 'Treat as New Plan' end
 where tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0;

      v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Change plan as new line Same Day :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Change plan as new line same day Execution Completed',
         v_rowcount,
         NULL,
         null);  

 --20190221: This is already in SH. Can be removed?
 -- on a diff day (previoulsy loaded into TC)
 merge into  stel_data_txn_mobile tgt
 using (
 select txn.ponumber, max(txn2.orderid) as NewOrdId, txn.orderid
 from vw_Salestransaction@stelext txn
 join  stel_data_txn_mobile txn2
 on txn2.genericattribute11 = 'Change Main Plan'
 and txn.ponumber=txn2.ponumber
 where   txn2.filename = v_param.file_name and txn2.filedate=v_param.file_Date and txn2.recordstatus=0
  and txn.genericattribute11='New'
  and txn.genericattribute9='M' and txn2.genericattribute9='M'
 group by txn.ponumber , txn.orderid

 )src
 on ((tgt.orderid=src.orderid or tgt.orderid=src.newordid) and tgt.ponumber=src.ponumber)
  when matched then update set
 tempfield3 = case when tgt.orderid=src.orderid then 'Replaced with Change Plan' else 'Treat as New Plan' end
 where tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0;

       v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Change plan on diff day :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Change plan on diff day Execution Completed',
         v_rowcount,
         NULL,
         null); 

 -- Cancelled/Ceased orders



 --mbb classification
 --20190221: Already in SH






 --mbb rejections
 --20190221: Move to SH for SER Event types
 merge into  stel_data_txn_mobile tgt
 using (
 select txn.orderid, txn.linenumber, txn.sublinenumber
 from  stel_data_txn_mobile txn
 join   (select s.*, s.genericattribute4 brand, s.genericattribute1 dept, s.genericattribute3 mat 
 --from stel_Classifier@stelext  s) stock --COMMENTED THIS LINE and added below for performance sankar
  from LP_STEL_CLASSIFIER  s) stock
 on txn.genericattribute28=stock.classifierid
 and stock.categorytreename = 'StockCode'
 and stock.categoryname='PRODUCTS'
 and txn.compensationdate between stock.effectivestartdate and stock.effectiveenddate-1
 --join (select s.* from stel_Classifier@stelext  s) prod --COMMENTED THIS LINE and added below for performance sankar
 join (select s.* from LP_STEL_CLASSIFIER  s) prod
  on prod.categorytreename = 'Singtel'
 and prod.categoryname='PRODUCTS'
 and txn.productid=prod.classifierid
 where 1=1
 and stock.dept not like '%Dongle%'
 and txn.genericattribute14='MBB'
 and txn.shiptopostalcode=0
 )src
 on (tgt.orderid=src.orderid and tgt.linenumber=src.linenumber and tgt.sublinenumber=src.sublinenumber)
  when matched then update set
  genericattribute15 = 'MBB Rejection - No Tie in except Dongle'
 where  tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0;

    v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update MBB Rejections :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update MBB Rejections Execution Completed',
         v_rowcount,
         NULL,
         null); 

 --20190221: Move to SH if its still needed commented by sankar and uncommented again to process regular files its verified
 
merge into  stel_data_txn_mobile tgt
 using (
 select distinct txn.orderid, txn.linenumber, txn.sublinenumber
 from  stel_data_txn_mobile txn
 join   (select s.*, s.genericattribute4 brand, s.genericattribute1 dept, s.genericattribute3 mat 
-- from stel_Classifier@stelext  s) stock
from LP_STEL_CLASSIFIER  s) stock
 on txn.genericattribute28=stock.classifierid
 and stock.categorytreename = 'StockCode'
 and stock.categoryname='PRODUCTS'
 and txn.compensationdate between stock.effectivestartdate and stock.effectiveenddate-1
 --join (select s.* from stel_Classifier@stelext  s) prod
 join (select s.* from LP_STEL_CLASSIFIER  s) prod
  on prod.categorytreename = 'Singtel'
 and prod.categoryname='PRODUCTS'
 and txn.productid=prod.classifierid
 where 1=1 --and txn.orderid in ( '197847073A197847074A185187412626262719' ,'216502916A216502917A103908512678969569')
 and txn.genericattribute14='MBB'
 and nvl(txn.genericattribute8,'0')='0'
 )src
 on (tgt.orderid=src.orderid and tgt.linenumber=src.linenumber and tgt.sublinenumber=src.sublinenumber)
  when matched then update set genericattribute15 = 'MBB Rejection - No Capacity'
 where  tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0;
 
 
 
     v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update MBB Rejections - No Capacity:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update MBB Rejections - No CapacityExecution Completed',
         v_rowcount,
         NULL,
         null); 


 --20190221: Move to SH from DEV environment
 --smae month dup check
 ----same file check

 --loaded into tc check

 --cross mth dup check


 --cross mth CI check



 --MSF eligibility






 --IMEI check Singtel

 ---IMEI check Usage
 ---VSOP file may come in late, can move this to stagehook?
 --20190221: Move to SH

 update  stel_data_txn_mobile tgt
 set tempfield4 = 'IMEI not Found in VSOP'
 where billtopostalcode not in 
 (select to_char(chimei) from stel_Data_vsop)
 and   tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0;

      v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update IMEI not Found in VSOP:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update IMEI not Found in VSOP Execution Completed',
         v_rowcount,
         NULL,
         null); 


  update  stel_data_txn_mobile tgt
 set tempfield4 = 'IMEI used before'
 where billtopostalcode is not null -- --[Arun - Added on 7th Sep 2019 as Mobile Submitted was failing]
 and billtopostalcode in  
 (select distinct to_char(imei)  from stel_Data_usedimei 
 where (IMEI IS NOT NULL AND SERVICENO IS NOT NULL) --[Arun - Added on 7th Sep 2019 as Mobile Submitted was failing]
 and (customerid||'-'||serviceno <> tgt.billtocustid||'-'||tgt.billtocontact) 
 or customerid='USED')
 and   tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0;

       v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update IMEI used before:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update IMEI used before Execution Completed',
         v_rowcount,
         NULL,
         null); 


 insert into stel_Data_usedimei
 select distinct billtopostalcode, compensationdate, billtocustid, billtocontact
 from  stel_data_txn_mobile txn
  where txn.filename = v_param.file_name and txn.filedate=v_param.file_Date and txn.recordstatus=0
  minus
  select to_char(imei) , compdate, customerid, serviceno from stel_Data_usedimei
    ;

        v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Insert into stel_Data_usedimei table:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Insert into stel_Data_usedimei table Execution Completed',
         v_rowcount,
         NULL,
         null); 

 commit;




 --SAP protection period per model
 /*
 txn model - get from stock
 txn sap customer code - participant ga12


 1.    Join the BCC order to SAP based on
-     BCC Model (txn.ga28). = SAP Model (sap.stockcode)
-    BCC Vendor Code = Callidus Profile Vendor Code (par.ga12) and SAP Customer Code = Callidus Profile Customer Code
-    BCC Order Entry Date between SAP Start Date and End Date

If the BCC Order Entry Date is more than 28 days after the End Date of the Transfer Cost in SAP, then the model is not in the protection period. 
Else, the model is price protected.
*/

--20190221: Move to SH
--[arun commented this block as this is moved to SH SER Mobile - STARTTTTT]
/*
merge into  stel_data_txn_mobile tgt
using (select 
case when txn.accountingdate - sap.startdate <=28 then 'Y' else 'N' end as protectionflag
,orderid, linenumber, sublinenumber
,sap.txnprice, sap.costprice
from  stel_data_txn_mobile txn
join stel_data_transfercost sap
on sap.stockcode=txn.genericattribute28
and txn.accountingdate between sap.startdate and nvl(sap.enddate,to_date('22000101','YYYYMMDD'))-1
join stel_participant@stelext par
on par.payeeid=txn.genericattribute3
and par.genericattribute12 = sap.customer
and txn.accountingdate between par.effectivestartdate and  par.effectiveenddate-1
where txn.filename = v_param.file_name and txn.filedate=v_param.file_Date and txn.recordstatus=0
) src
on (Src.orderid=tgt.orderid and src.linenumber=tgt.linenumber and src.sublinenumber=tgt.sublinenumber)
when matched then update set
tgt.protectionflag=src.protectionflag,
tgt.saptxprice=txnprice,
tgt.sapcostprice=costprice

where  tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0;

        v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update ProtectionFlag SAP taxPrice and SAP CostPrice:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update ProtectionFlag SAP taxPrice and SAP CostPrice Execution Completed',
         v_rowcount,
         NULL,
         null); 

 commit;

*/

--[arun commented this block as this is moved to SH SER Mobile - ENDDD]


/*
If model is price protected:
-    Get the VSOP transfer cost and stamp it on the transaction
If model is not cost protected (any more)
-    Get the latest SAP Price (irrespective of the actual transfer cost that is in VSOP), since the protection has ended, and stamp it on the transaction
*/
merge into  stel_data_txn_mobile tgt
using (
select distinct txn.orderid, txn.linenumber, txn.sublinenumber, vsop.transfercost
 from  stel_data_txn_mobile txn
 join stel_Data_vsop vsop
 on txn.billtopostalcode=to_char(vsop.chimei) --Arun[26th Apr 19] Added to_char to chimei as datatype was number and proc was failing --assuming only this join is needed
  where nvl(txn.imeimatch,'Y') = 'Y'
and  txn.filename = v_param.file_name and txn.filedate=v_param.file_Date and txn.recordstatus=0
and vsop.transfercost>0  --[Arun 5th Sep 2019 - Added this condition as the proc is failing]
) src
on (Src.orderid=tgt.orderid and src.linenumber=tgt.linenumber and src.sublinenumber=tgt.sublinenumber)
when matched then update set
tgt.vsopcost=src.transfercost

where  tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0
;

        v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update vsopcost:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update vsopcost Execution Completed',
         v_rowcount,
         NULL,
         null); 

 commit;


/*
Note that for IMEI replacements, the transfer price will not be in VSOP, as no transaction come in through VSOP
in this case, the SAP price will be used, using the Order Entry Date to look up the price.
*/

update  stel_data_txn_mobile tgt
set finalCost = 
case when protectionflag='Y' then nvl(vsopcost,saptxprice)
when protectionflag = 'N' then saptxprice
else 0 end
where  tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0
and tgt.channel='SER'
;

        v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update FinalCost for SER:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update FinalCost Execution Completed',
         v_rowcount,
         NULL,
         null); 

 commit;

  update  stel_data_txn_mobile tgt
set finalCost =  sapcostprice

where  tgt.filename = v_param.file_name and tgt.filedate=v_param.file_Date and tgt.recordstatus=0
and tgt.channel='TEPL'
;

        v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update FinalCost for TEPL:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update FinalCost TEPL Execution Completed',
         v_rowcount,
         NULL,
         null); 

-------------------[Arun SER INDICATOR Fix done on 11th Mar 2019 from this line till the comment below
merge into STEL_DATA_TXN_MOBILE tgt
using(
        select st.seq seq,lt.channel, lt.dealer
        from STEL_DATA_TXN_MOBILE st
        join (
                select dim0 dealer, stringvalue channel, effectivestartdate, effectiveenddate
                from stel_lookup@STELEXT
                where  name like 'LT_Dealer_Channel Type') lt
        on lt.dealer=st.genericattribute3 and lt.channel='SER'
        and st.compensationdate between lt.effectivestartdate and lt.effectiveenddate-1
        where st.filename = v_param.file_name and st.filedate=v_param.file_Date and st.recordstatus=0
)src
on (src.seq=tgt.seq)
when matched then update set
tgt.SERINDICATOR='Y'
;

        v_rowcount := SQL%ROWCOUNT;

          SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update SER Indicator for SER Mobile'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update ER Indicator for SER Mobile Execution Completed',
         v_rowcount,
         NULL,
         null); 

          commit;
  ---------------------------------------------------[Arun bug fix for SER INDICATOR DONE ON 11th Mar 2019] Ends here

END SP_INBOUND_POST_MOBPROCESS;
