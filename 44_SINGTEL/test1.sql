--------------------------------------------------------
--  DDL for Procedure SP_INBOUND_POST_EPC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "SP_INBOUND_POST_EPC" AS 
 v_param   INBOUND_CFG_PARAMETER%ROWTYPE;
    v_rowcount integer:= null;
v_proc_name varchar2(127):='SP_INBOUND_POST_EPC';
BEGIN
   SELECT * INTO v_param FROM INBOUND_CFG_PARAMETER;




   update inbound_Data_clpr t
    set recordstatus=-9

    where t.filename = v_param.file_name and t.filedate=v_param.file_date and t.recordstatus=0;

    insert into inbound_Data_clpr

    SELECT FILEDATE,
  FILENAME,
  '0' RECORDSTATUS,
  DOWNLOADED,
  b.classfiername ||' - ' || PRODUCTID, --note
  EFFECTIVESTARTDATE,
  EFFECTIVEENDDATE,
  PRODUCTID as NAME, --note
  PRICE,
  UNITTYPEFORPRICE,
  COST,
  UNITTYPEFORCOST,
  b.CATEGORYTREENAME,
  b.CATEGORYNAME,
    case when classfiername ='DS - Internal' then 'ConSales_Internal'
 when classfiername ='DH - External' then 'DigitalHome_External'
  when classfiername ='SER' then 'ConSales_External'
   when classfiername ='TEPL' then 'ConSales_External'
    when classfiername ='Digital Sales - Telesales' then 'ConSales_Internal'
     when classfiername ='Singtel Shop' then 'ConSales_Internal'
      when classfiername ='DS - External' then 'ConSales_External'
       when classfiername ='DH - Internal' then 'DigitalHome_Internal'
        when classfiername ='RCM' then 'ConSales_Internal'
         when classfiername ='CCO - Solution Plus' then 'ConOpsSPlus'
         else 'ConSales_External' end as BUSINESSUNITNAME,
 -- BUSINESSUNITNAME,
  name as DESCRIPTION, --note
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
  GENERICBOOLEAN6

    from inbound_Data_clpr t
       cross join (Select distinct classfiername, genericattribute3 categorytreename, case when genericattribute3 like '%Internal%' then 'PRODUCTS-INT' else 'PRODUCTS' end as categoryname
          from stel_Classifier@stelext where categorytreename='Channel List' and categoryname='Channel List'
          and sysdate between effectivestartdate and effectiveenddate-1
          ) b

    where t.filename = v_param.file_name and t.filedate=v_param.file_date and t.recordstatus=-9;

/* Arjun 20190523 removing this. added ct into classifier.ga3 for channel list
    insert into inbound_Data_clpr
    SELECT FILEDATE,
  FILENAME,
  '0' RECORDSTATUS,
  DOWNLOADED,
  b.classfiername ||' - ' || PRODUCTID, --note
  EFFECTIVESTARTDATE,
  EFFECTIVEENDDATE,
  PRODUCTID as NAME, --note
  PRICE,
  UNITTYPEFORPRICE,
  COST,
  UNITTYPEFORCOST,
  'Singtel-Internal-Products' CATEGORYTREENAME,
  'PRODUCTS-INT' CATEGORYNAME,
  BUSINESSUNITNAME,
  name as DESCRIPTION, --note
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
  GENERICBOOLEAN6

    from inbound_Data_clpr t
    cross join (Select distinct classfiername, genericattribute3, case when genericattribute3 like '%Internal%' then 'PRODUCTS-INT' else 'PRODUCTS' end as genericattribute4
          from stel_Classifier@stelext where categorytreename='Channel List' and categoryname='Channel List'
          and sysdate between effectivestartdate and effectiveenddate-1
          ) b

    where t.filename = v_param.file_name and t.filedate=v_param.file_date and t.recordstatus=-9;
*/
   -- we still want these loaded in- as product master data
   update inbound_Data_clpr t
    set recordstatus=0
    where t.filename = v_param.file_name and t.filedate=v_param.file_date and t.recordstatus=-9;



   -- delete data that hasn;t changed compared to what's in TC


   update inbound_Data_clpr t
   set recordstatus=-2
   where t.filename = v_param.file_name and t.filedate=v_param.file_date and t.recordstatus=0
   and  ( productid, name, cost, genericattribute1
   , genericattribute10, genericattribute11, genericattribute13
   , genericattribute14, genericattribute15)  in

   (Select classifierid, classfiername, cost, genericattribute1 
   , genericattribute10 FVSvc, genericattribute11, genericattribute13
   , genericattribute14, genericattribute15
   from stel_Classifier@stelext
   where categorytreename=t.categorytreename/*'Singtel'*/ and Categoryname=t.categoryname/*'PRODUCTS'*/ and 
   sysdate between effectivestartdate and effectiveenddate)
   ;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update RecordStatus=-2 in inbound_Data_clpr :'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update RecordStatus=-2 in inbound_Data_clpr Execution Completed',
         v_rowcount,
         NULL,
         null);    


   -- for data that has changed, update the effectivedate
   --merge the data with what was uploaded by the user
   merge into inbound_Data_clpr tgt
   using (
   Select *
   from stel_Classifier@stelext
   where (( categorytreename='Singtel' and Categoryname='PRODUCTS')
   or (( categorytreename='Singtel-Internal-Products' and Categoryname='PRODUCTS-INT'))
   or (categorytreename='StockCode' and categoryname='PRODUCTS')        --[Arun 28th Mar 2019 - Adding Stock codes to the list as well - As user don't want the configured MSF to be gone after every reload. 
   )
   and sysdate between effectivestartdate and effectiveenddate
   ) src
   on (tgt.productid=src.classifierid 
   --arjun 20190522 adding the below join due to Mobile VAS being set up without category
   and tgt.categorytreename=src.categorytreename and tgt.categoryname=src.categoryname
   )
   when matched then update
   set 
   tgt.price=src.price, tgt.cost=src.cost
   , tgt.genericattribute1 = src.genericattribute1
   , tgt.genericattribute2 = src.genericattribute2
   , tgt.genericattribute3  = src.genericattribute3
   , tgt.genericattribute4  = src.genericattribute4
   , tgt.genericattribute5  = src.genericattribute5
   , tgt.genericattribute6  = src.genericattribute6
   , tgt.genericattribute7  = src.genericattribute7
   , tgt.genericattribute8  = src.genericattribute8
   , tgt.genericattribute9  = src.genericattribute9
   , tgt.genericattribute12  = src.genericattribute12
   , tgt.genericattribute15  = src.genericattribute15
   , tgt.genericattribute16  = src.genericattribute16
   , tgt.genericnumber1  = src.genericnumber1
   , tgt.genericnumber2 = src.genericnumber2
   , tgt.genericnumber3 = src.genericnumber3
   , tgt.genericnumber4 = src.genericnumber4
   , tgt.genericnumber5 = src.genericnumber5
   , tgt.genericnumber6 = src.genericnumber6
    , tgt.unittypeforgenericnumber1  = case when src.genericnumber1 is null then null else 'SGD' end
   , tgt.unittypeforgenericnumber2 = case when src.genericnumber2  is null then null else 'SGD' end
   , tgt.unittypeforgenericnumber3 = case when src.genericnumber3 is null then null else 'SGD' end
   , tgt.unittypeforgenericnumber4 = case when src.genericnumber4 is null then null else 'SGD' end
   , tgt.unittypeforgenericnumber5 = case when src.genericnumber5 is null then null else 'SGD' end
   , tgt.unittypeforgenericnumber6 = case when src.genericnumber6 is null then null else 'SGD' end

   , tgt.genericdate1 = src.genericdate1
   , tgt.genericdate2= src.genericdate2
   , tgt.genericdate3= src.genericdate3
   , tgt.genericdate4= src.genericdate4
   , tgt.genericdate5= src.genericdate5
   , tgt.genericdate6= src.genericdate6
   , tgt.genericboolean1= src.genericboolean1
   , tgt.genericboolean2= src.genericboolean2
   , tgt.genericboolean3= src.genericboolean3
   , tgt.genericboolean4= src.genericboolean4
   , tgt.genericboolean5= src.genericboolean5
   , tgt.genericboolean6= src.genericboolean6
  where tgt.filename = v_param.file_name and tgt.filedate=v_param.file_date and tgt.recordstatus=0
   ;

    v_rowcount := SQL%ROWCOUNT;

      SP_LOGGER (
         SUBSTR (
               v_proc_name
            || 'Update Users data ininbound_Data_clpr:'
            || v_param.file_type
            || '-FileName:'
            || v_param.file_name
            || '-Date:'
            || v_param.file_date,
            1,
            255),
         'Update Users data ininbound_Data_clpr Execution Completed',
         v_rowcount,
         NULL,
         null);    


  


update  inbound_Data_clpr tgt
 set unittypeforprice=case when price is not null then 'SGD' else null end,
   unittypeforcost=case when cost is not null then 'SGD' else null end,
   cost=case when unittypeforcost='SGD' then nvl(cost,0) else null end
 where tgt.filename = v_param.file_name and tgt.filedate=v_param.file_date and tgt.recordstatus=0;
 
 
commit;

END SP_INBOUND_POST_EPC;
