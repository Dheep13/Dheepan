CREATE PROCEDURE EXT.CTAS_SP_ONTIMEPERCENT(IN FILENAME varchar(120))
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for the inbound On Time Percentage Feed

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       14-JUL-2023		Deepan		     Initial creation

***************************************************************************************************/
DECLARE in_periodseq bigint;
DECLARE in_processingunitseq bigint;
DECLARE v_periodstartdate date;
DECLARE v_periodenddate date;
DECLARE v_eot date := to_date('01-JAN-2200', 'DD-MON-YYYY');
DECLARE v_currency varchar2(4);
DECLARE v_proc_name varchar2(100);
DECLARE v_tenantId varchar2(4);
DECLARE v_eventtypeid varchar2(10);
DECLARE v_businessUnit varchar2(12);
DECLARE v_maxSeq bigint;
DECLARE v_intUnitType varchar2(12);
DECLARE v_txta_batchName varchar2(100);
DECLARE v_txsta_batchName varchar2(100);
DECLARE v_prevBatchName varchar2(100);
DECLARE v_currInBatchName varchar2(100);
DECLARE v_count number;
DECLARE v_prestageCount number;
DECLARE v_qtyUnitType varchar(10);
DECLARE v_currMONYYYY varchar2(10);
DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 
    BEGIN 
        v_prevBatchName = NULL;
END;
DECLARE EXIT HANDLER FOR SQLEXCEPTION 

    BEGIN 
    rollback;
--log('Failed');
    update ext.ctas_otpercent_history
set processflag = 1;
--     update ext.ctas_arbalance_in
-- set processflag = 1;
    ext.ctas_event_log (v_proc_name,::SQL_ERROR_CODE || ' . ' ||::SQL_ERROR_MESSAGE,0);
    commit;
resignal;
END;

v_currency := 'USD';
v_maxSeq := 0;
v_proc_name :=::CURRENT_OBJECT_NAME;
v_tenantId := '';
v_eventtypeid := 'ONTIME';
v_businessUnit := 'FIRE';
v_intUnitType := 'Integer';
v_qtyUnitType := 'Quantity';
v_txsta_batchName := '';
v_txta_batchName := '';
v_prevBatchName := '0';
v_currInBatchName := '';
v_count := 0;
v_prestageCount :=0;
v_currMONYYYY := '';

-- ext.ctas_event_log ('Test','Begin',0);

-- select distinct inboundbatch into v_currInBatchName
-- from ext.ctas_arbalance_in
-- where processflag = 0;

v_currInBatchName = FILENAME;

ext.ctas_event_log (:v_proc_name,'Begin '||:v_currInBatchName,0);
select tenantid  into v_tenantId from cs_tenant;

/*delete historical*/
-- delete from ext.ctas_arbalance_in where processflag <>0;

/*create batchnames, get current month and year*/
select :v_tenantId || '_' || 'TXTA' || '_' || substring(replace(TO_CHAR(current_date), '-', ''), 1, 6),
:v_tenantId || '_' || 'TXSTA' || '_' || substring(replace(TO_CHAR(current_date), '-', ''), 1, 6),
substring(MONTHNAME(current_date),1,3)||year(current_date)
into v_txta_batchName,v_txsta_batchName,v_currMONYYYY
from dummy;

/*Load into history*/
insert into ext.ctas_otpercent_history(
loaddate,
inboundbatch,
processflag)
(select distinct
:v_currInBatchName,
0 from ext.ctas_otpercent_in);


/* get max of stagesalestransactionseq*/
select max(stagesalestransactionseq) into v_maxSeq
from cs_stagesalestransaction;

/*for reprocessing same batch*/
select count(distinct inboundbatch) into v_count
from ext.ctas_otpercent_prestage
where inboundbatch in (
        select distinct inboundbatch
        from ext.ctas_otpercent_history
        where processflag = 0
    );
if :v_count = 1 then
    select distinct batchname into v_prevBatchName
    from ext.ctas_otpercent_prestage
    where inboundbatch = (
            select distinct inboundbatch
            from ext.ctas_otpercent_history
            where processflag = 0
        );

   delete from cs_stagetransactionassign ta/*comment this if needed*/
    where exists(
            select 1
            from cs_stagesalestransaction st
            where batchname = :v_prevBatchName
                and st.stagesalestransactionseq = ta.stagesalestransactionseq
        );

    delete from cs_stagesalestransaction st
    where batchname = :v_prevBatchName;
    

    delete from ext.ctas_otpercent_prestage
    where inboundbatch in (
            select inboundbatch
            from ext.ctas_otpercent_history
            where processflag = 0
        );
        
end if;

/*initial load from inbound table for 30, 60, 60 days balance*/

insert into ext.ctas_otpercent_prestage(
        orderid,
        Linenumber,
        Sublinenumber,
        eventtypeid,
        value,
        unittypeforvalue,
        compensationdate,
        businessunitname,
        PartnerId,
        CustomerId ,
        -- CustomerName,
        genericnumber1,
        unittypefor30,
        inboundbatch
    ) (select
            sub.orderid,
            sub.Linenumber,
            sub.sublinenumber,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate,
            sub.businessunit,
            sub.partnerid,
            sub.customerid,
            -- st.genericattribute2 as customername,
            sum(sub.balancevalue),
            sub.unit_type,
            sub.inboundbatch from (

        select 
        	distinct ar.balanceseq,
        	ar.serviceorderdate,
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.positionname as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            ar.balancevalue,
            :v_currency as unit_type,
            ar.inboundbatch as inboundbatch
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_otpercent_history ar on ar.ServiceOrderNumber = so.orderid
            and st.compensationdate=ar.serviceorderdate
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(ar.duedate, current_date) >= 30
            and days_between(ar.duedate, current_date) < 60
            and ar.inboundbatch = :v_currInBatchName
            and ar.processflag = 0
            and not exists (
                select 1
                from ext.ctas_otpercent_prestage ap
                where ap.orderid = so.orderid
                    and ap.customerid = ar.customerid
                    and ap.partnerid = ta.positionname
                    and ap.inboundbatch = ar.inboundbatch)	
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundbatch,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate );
    

insert into ext.ctas_otpercent_prestage(
        orderid,
        Linenumber,
        Sublinenumber,
        eventtypeid,
        value,
        unittypeforvalue,
        compensationdate,
        businessunitname,
        PartnerId,
        CustomerId ,
        -- CustomerName,
        genericnumber2,
        unittypefor60,
        inboundbatch
    ) (select
            sub.orderid,
            sub.Linenumber,
            sub.sublinenumber,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate,
            sub.businessunit,
            sub.partnerid,
            sub.customerid,
            -- st.genericattribute2 as customername,
            sum(sub.balancevalue),
            sub.unit_type,
            sub.inboundbatch from (

        select 
        	distinct ar.balanceseq,
        	ar.serviceorderdate,
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.positionname as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            ar.balancevalue,
            :v_currency as unit_type,
            ar.inboundbatch as inboundbatch
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_otpercent_history ar on ar.ServiceOrderNumber = so.orderid
            and st.compensationdate=ar.serviceorderdate
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(ar.duedate, current_date) >= 60
            and days_between(ar.duedate, current_date) < 90
            and ar.inboundbatch = :v_currInBatchName
            and ar.processflag = 0
            and not exists (
                select 1
                from ext.ctas_otpercent_prestage ap
                where ap.orderid = so.orderid
                    and ap.customerid = ar.customerid
                    and ap.partnerid = ta.positionname
                    and ap.inboundbatch = ar.inboundbatch)	
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundbatch,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate );
    
insert into ext.ctas_otpercent_prestage(
        orderid,
        Linenumber,
        Sublinenumber,
        eventtypeid,
        value,
        unittypeforvalue,
        compensationdate,
        businessunitname,
        PartnerId,
        CustomerId ,
        -- CustomerName,
        genericnumber3,
        unittypefor90,
        inboundbatch
    ) (select
            sub.orderid,
            sub.Linenumber,
            sub.sublinenumber,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate,
            sub.businessunit,
            sub.partnerid,
            sub.customerid,
            -- st.genericattribute2 as customername,
            sum(sub.balancevalue),
            sub.unit_type,
            sub.inboundbatch from (

        select 
        	distinct ar.balanceseq,
        	ar.serviceorderdate,
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.positionname as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            ar.balancevalue,
            :v_currency as unit_type,
            ar.inboundbatch as inboundbatch
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_otpercent_history ar on ar.ServiceOrderNumber = so.orderid
            and st.compensationdate=ar.serviceorderdate
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(ar.duedate, current_date) >= 90
            and ar.inboundbatch = :v_currInBatchName
            and ar.processflag = 0
            and not exists (
                select 1
                from ext.ctas_otpercent_prestage ap
                where ap.orderid = so.orderid
                    and ap.customerid = ar.customerid
                    and ap.partnerid = ta.positionname
                    and ap.inboundbatch = ar.inboundbatch)	
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundbatch,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate );
    
/*update statement in case there are multiple due dates(30,60,90 days etc.) for the same orderid, customer and partnerid*/
merge into ext.ctas_otpercent_prestage ar using 
(select
            sub.orderid,
            sub.partnerid,
            sub.customerid,
            sum(sub.balancevalue) as thirty_days
            from (

        select 
        	distinct ar.balanceseq,
        	ar.serviceorderdate,
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.positionname as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            ar.balancevalue,
            :v_currency as unit_type,
            ar.inboundbatch as inboundbatch
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_otpercent_history ar on ar.ServiceOrderNumber = so.orderid
            and st.compensationdate=ar.serviceorderdate
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(ar.duedate, current_date) >= 30
            and days_between(ar.duedate, current_date) < 60
            and ar.inboundbatch = :v_currInBatchName
            and ar.processflag = 0) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundbatch,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate) sub on ar.orderid = sub.orderid
and sub.customerid = ar.customerid
and sub.partnerid = ar.partnerid
when matched then
update
set ar.genericnumber1 = sub.thirty_days,
    unittypefor30 = :v_currency;
    
    
merge into ext.ctas_otpercent_prestage ar using 
(select
            sub.orderid,
            sub.partnerid,
            sub.customerid,
            sum(sub.balancevalue) as sixty_days
            from (

        select 
        	distinct ar.balanceseq,
        	ar.serviceorderdate,
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.positionname as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            ar.balancevalue,
            :v_currency as unit_type,
            ar.inboundbatch as inboundbatch
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_otpercent_history ar on ar.ServiceOrderNumber = so.orderid
            and st.compensationdate=ar.serviceorderdate
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(ar.duedate, current_date) >= 60
            and days_between(ar.duedate, current_date) < 90
            and ar.inboundbatch = :v_currInBatchName
            and ar.processflag = 0) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundbatch,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate ) sub on ar.orderid = sub.orderid
and sub.customerid = ar.customerid
and sub.partnerid = ar.partnerid
when matched then
update
set ar.genericnumber2 = sub.sixty_days,
    unittypefor60 = :v_currency;

merge into ext.ctas_otpercent_prestage ar using 
(select
            sub.orderid,
            sub.partnerid,
            sub.customerid,
            sum(sub.balancevalue) as ninety_days
            from (

        select 
        	distinct ar.balanceseq,
        	ar.serviceorderdate,
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.positionname as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            ar.balancevalue,
            :v_currency as unit_type,
            ar.inboundbatch as inboundbatch
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_otpercent_history ar on ar.ServiceOrderNumber = so.orderid
            and st.compensationdate=ar.serviceorderdate
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(ar.duedate, current_date) >= 90
            and ar.inboundbatch = :v_currInBatchName
            and ar.processflag = 0) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundbatch,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate ) sub on ar.orderid = sub.orderid
and sub.customerid = ar.customerid
and sub.partnerid = ar.partnerid
when matched then
update
set ar.genericnumber3 = sub.ninety_days,
    unittypefor90 = :v_currency;    
---do not delete this
--this is to delete positions that are not needed

 delete from ext.ctas_otpercent_prestage ap_out where not exists (
 select 1
 from cs_title ti
 inner join cs_position pos
 on pos.titleseq=ti.ruleelementownerseq
 inner join ext.ctas_otpercent_prestage ap
 on ap.partnerid = pos.name
 where ti.name in ('FST In Training','SSRIT - Assigned','SSRIT - Bench', 'FST', 'SSR'
 ,'Sales Representative'	--remove salesrep, this is only for testing
 )
 and ap.inboundbatch= :v_currInBatchName
 and ti.removedate ='2200-01-01'
 and pos.removedate ='2200-01-01'
 and ap.partnerid=ap_out.partnerid	
 )
 and ap_out.inboundbatch=:v_currInBatchName;
 
select count(1) into v_prestageCount from ext.ctas_otpercent_prestage where inboundbatch= :v_currInBatchName;
ext.ctas_event_log (v_proc_name,'Count of records loaded into prestage for inbound file: '||:v_currInBatchName,:v_prestageCount);
 
update ext.ctas_otpercent_prestage
set genericnumber3 = 0,
    unittypefor90 = 'USD'
where genericnumber3 is null;

update ext.ctas_otpercent_prestage
set genericnumber2 = 0,
    unittypefor60 = 'USD'
where genericnumber2 is null;

update ext.ctas_otpercent_prestage
set genericnumber1 = 0,
    unittypefor30 = 'USD'
where genericnumber1 is null;

update ext.ctas_otpercent_prestage
set batchname = :v_txsta_batchName
where batchname is null;

merge into ext.ctas_otpercent_prestage ap using/*update customer name in prestage*/
(
select cl.classifierid, cl.description from cs_categorytree ct
inner join cs_category cat
on ct.categorytreeseq=cat.categorytreeseq
inner join cs_category_classifiers cc
on cc.categoryseq=cat.ruleelementseq
inner join cs_classifier cl
on cl.classifierseq=cc.classifierseq
where ct.name='Customer_Classification'
and cat.name in ('Enterprise Customer','Portfolio Customer')
and ct.removedate='2200-01-01'
and cat.removedate='2200-01-01'
and cc.removedate='2200-01-01'
and cl.removedate='2200-01-01') sub on
sub.classifierid=ap.customerid
and ap.inboundbatch=:v_currInBatchName
when matched then update
set ap.customername=sub.description;


update ext.ctas_otpercent_prestage
set stagesalestransactionseq = stagesalestransactionseq + :v_maxSeq;

/*delete duplicates from stage to avoid unique constraint issue*/
delete from cs_stagetransactionassign ta where exists(
select stagesalestransactionseq from cs_stagesalestransaction where (linenumber,sublinenumber,orderid) in (select linenumber,sublinenumber,
'ARBALANCE' || '_' || Partnerid || '_'||:v_currMONYYYY as orderid
from ext.ctas_otpercent_prestage
        where inboundbatch = :v_currInBatchName) and stagesalestransactionseq=ta.stagesalestransactionseq);

delete from cs_stagesalestransaction where (linenumber,sublinenumber,orderid) in (select linenumber,sublinenumber,
'ARBALANCE' || '_' || Partnerid || '_'||:v_currMONYYYY as orderid
	from ext.ctas_otpercent_prestage
        where inboundbatch = :v_currInBatchName);
        

/*Load into stagesalestransaction and stagesalestransactionassign*/
insert into cs_stagesalestransaction(
        tenantid,
        stagesalestransactionseq,
        orderid,
        batchname,
        Linenumber,
        Sublinenumber,
        eventtypeid,
        value,
        unittypeforvalue,
        compensationdate,
        businessunitname,
        genericattribute1,
        genericattribute2,
        genericattribute3,
        genericnumber1,
        unittypeforgenericnumber1,
        genericnumber2,
        unittypeforgenericnumber2,
        genericnumber3,
        unittypeforgenericnumber3
    ) (
        select :v_tenantId,
            stagesalestransactionseq,
            'ARBALANCE' || '_' || Partnerid || '_'||:v_currMONYYYY  as orderid,
            :v_txsta_batchName,
            Linenumber,
            Sublinenumber,
            eventtypeid,
            value,
            unittypeforvalue,
            compensationdate,
            businessunitname,
            PartnerId,
            CustomerId ,
            CustomerName,
            genericnumber1,
            unittypefor30,
            genericnumber2,
            unittypefor60,
            genericnumber3,
            unittypefor90
        from ext.ctas_otpercent_prestage
        where inboundbatch = :v_currInBatchName
        order by 1
    );

ext.ctas_event_log (:v_proc_name,'Count of records loaded into stagesalestransaction for inbound file: '||:v_currInBatchName,::ROWCOUNT);
 

insert into cs_stagetransactionassign(
        tenantid,
        stagesalestransactionseq,
        setnumber,
        batchname,
        orderid,
        Linenumber,
        Sublinenumber,
        eventtypeid,
        payeeid,
        payeetype
    ) (
        select :v_tenantId,
            stagesalestransactionseq,
            1,
            :v_txta_batchName,
            'ARBALANCE' || '_' || Partnerid || '_'||:v_currMONYYYY  as orderid,
            Linenumber,
            Sublinenumber,
            eventtypeid,
            PartnerId,
            'Participant'
        from ext.ctas_otpercent_prestage
        where inboundbatch = :v_currInBatchName
        order by 1
    );

ext.ctas_event_log (:v_proc_name,'Count of records loaded into stagesalestransactionassign for : '||:v_currInBatchName,::ROWCOUNT);
 
/*update processflag*/
-- update ext.ctas_arbalance_in
-- set processflag = 3
-- where inboundbatch = :v_currInBatchName;

update ext.ctas_otpercent_history
set processflag = 3
where inboundbatch = :v_currInBatchName;

delete from ext.ctas_arbalance_in;

ext.ctas_event_log (:v_proc_name,'End '||:v_currInBatchName,0);
 
COMMIT;
END