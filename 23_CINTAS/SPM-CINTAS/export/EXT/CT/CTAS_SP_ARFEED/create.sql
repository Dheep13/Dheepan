CREATE PROCEDURE EXT.CTAS_SP_ARFEED(IN FILENAME varchar(120))
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for the inbound AR Balances Feed

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       05-JUL-2023		Deepan		     Initial creation
    1.1       22-SEP-2023       Deepan           Included exception handling for inbound feed

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
DECLARE v_intUnitType varchar2(12);
DECLARE v_currInFile varchar2(100);
DECLARE v_count number;
DECLARE v_status nvarchar(2);
DECLARE v_message nvarchar(100);
DECLARE v_errorCount number;
DECLARE v_prestageCount number;
DECLARE v_currMONYYYY varchar2(10);
DECLARE v_filter nvarchar(100);
DECLARE v_error CONDITION FOR SQL_ERROR_CODE 10000;
DECLARE CURSOR cur_row FOR
SELECT * from ext.ctas_arbalance_error;

DECLARE invalid_input CONDITION FOR SQL_ERROR_CODE 10000;
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

v_currency := 'USD';
v_proc_name :=::CURRENT_OBJECT_NAME;
v_tenantId := '';
v_eventtypeid := 'ARBALANCE';
v_businessUnit := 'FIRE';
v_intUnitType := 'Integer';
v_count := 0;
v_prestageCount :=0;
v_currMONYYYY := '';
v_currInFile = :FILENAME;
v_filter='';

ext.ctas_event_log (:v_proc_name,'Begin '||:v_currInFile,0);
select tenantid  into v_tenantId from cs_tenant;

/*Start : Exception handling*/

/* handled in validate_data proc
delete from ext.ctas_arbalance_error;

insert into ext.ctas_arbalance_error
(select * , '100', 
case when serviceordernumber is null then 'Serviceordernumber is empty.' 
-- when serviceorderdate is null then 'Serviceorderdate is empty.'
when duedate is null then 'Duedate is empty.' 
when customerid is null then 'Customerid is empty.' 
when balancevalue is null then 'Balancevalue is empty.'  end
from ext.ctas_arbalance_in where serviceordernumber is null
or duedate is null or customerid is null or balancevalue is null);

insert into ext.ctas_arbalance_error
(
select * , '101' , serviceordernumber || ' is not valid.'
from ext.ctas_arbalance_in where serviceordernumber is not null 
and not exists (
	select 1 from cs_salesorder where orderid=serviceordernumber
	and removedate ='2200-01-01'
));

insert into ext.ctas_arbalance_error
(
select * , '102' , customerid||' is not valid.'
from ext.ctas_arbalance_in where customerid is not null 
and not exists (
	select 1 from cs_classifier 
    -- where classifierid=substring(CUSTOMERID, 3)
    where classifierid=CUSTOMERID
	and removedate ='2200-01-01'
));

delete from ext.ctas_arbalance_in ar where exists(select 1 from ext.ctas_arbalance_error ae
	where ae.INVOICENUMBER = ar.INVOICENUMBER
);*/


/*End : Exception handling* continued in title validation*/

CALL ext.ctas_validate_data(::CURRENT_OBJECT_NAME,:FILENAME,:v_filter,v_status, v_message);
    -- Check the returned status and raise an exception if invalid
    IF :v_status = 0 THEN
        SIGNAL v_error SET MESSAGE_TEXT = :v_message;
    END IF;

/*delete historical*/
delete from ext.ctas_arbalance_prestage;

/*create batchnames, get current month and year*/
select 
year(current_date)||substring(MONTHNAME(current_date),1,3)
into v_currMONYYYY
from sys.dummy;


/*for reprocessing same batch*/

    delete from cs_stagesalestransaction st
    where batchname = :FILENAME;
    delete from cs_stagetransactionassign st
    where batchname = :FILENAME;
    

ext.ctas_event_log (:v_proc_name,'Same file data reset complete '||:v_currInFile,::ROWCOUNT);

/*initial load from inbound table for 30, 60, 60 days balance*/

insert into ext.ctas_arbalance_prestage(
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
        inboundfile
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
            sub.inboundfile from (

        select 
        	distinct 
        	-- ar.balanceseq,
        	-- to_date(ar.ServiceOrderDate, 'MM/DD/YYYY'),
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.payeeid as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            to_integer(ar.balancevalue) as balancevalue,
            :v_currency as unit_type,
            :v_currInFile as inboundfile
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_arbalance_in ar on ar.ServiceOrderNumber = so.orderid
            -- and st.compensationdate=to_date(ar.serviceorderdate, 'MM/DD/YYYY')
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) >= 30
            and days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) < 60
            -- and ar.inboundfile = :v_currInFile
            --and ar.processflag = 0
            and not exists (
                select 1
                from ext.ctas_arbalance_prestage ap
                where ap.orderid = so.orderid
                    and ap.customerid = ar.customerid
                    and ap.partnerid = ta.payeeid
                    -- and ap.inboundfile = ar.inboundfile
            	)	
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundfile,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate );
    
insert into ext.ctas_arbalance_prestage(
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
        inboundfile
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
            sub.inboundfile from (

        select 
        	distinct
        	-- distinct ar.balanceseq,
        	-- to_date(ar.serviceorderdate, 'MM/DD/YYYY'),
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            -- ROW_NUMBER() OVER (PARTITION BY so.orderid,ta.payeeid) as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.payeeid as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            to_integer(ar.balancevalue) as balancevalue,
            :v_currency as unit_type,
            :v_currInFile as inboundfile
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_arbalance_in ar on ar.ServiceOrderNumber = so.orderid
            -- and st.compensationdate=to_date(ar.serviceorderdate, 'MM/DD/YYYY')
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) >= 60
            and days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) < 90
            -- and ar.inboundfile = :v_currInFile
            --and ar.processflag = 0
            and not exists (
                select 1
                from ext.ctas_arbalance_prestage ap
                where ap.orderid = so.orderid
                    and ap.customerid = ar.customerid
                    and ap.partnerid = ta.payeeid
                    -- and ap.inboundfile = ar.inboundfile
            	
            )	
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundfile,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate );
 
insert into ext.ctas_arbalance_prestage(
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
        inboundfile
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
            sub.inboundfile from (

        select 
        	distinct
        	-- distinct ar.balanceseq,
        	-- to_date(ar.serviceorderdate, 'MM/DD/YYYY'),
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            -- ROW_NUMBER() OVER (PARTITION BY so.orderid,ta.payeeid) as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.payeeid as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            to_integer(ar.balancevalue) as balancevalue,
            :v_currency as unit_type,
            :v_currInFile as inboundfile
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_arbalance_in ar on ar.ServiceOrderNumber = so.orderid
            -- and st.compensationdate=to_date(ar.serviceorderdate, 'MM/DD/YYYY')
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) >= 90
            -- and ar.inboundfile = :v_currInFile
            --and ar.processflag = 0
            and not exists (
                select 1
                from ext.ctas_arbalance_prestage ap
                where ap.orderid = so.orderid
                    and ap.customerid = ar.customerid
                    and ap.partnerid = ta.payeeid
                    -- and ap.inboundfile = ar.inboundfile
            	
            )	
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundfile,
            sub.eventtypeid,
            sub.value,
            sub.unittypeforvalue,
            sub.compensationdate );

/*update statement in case there are multiple due dates(30,60,90 days etc.) for the same orderid, customer and partnerid*/
merge into ext.ctas_arbalance_prestage ar using 
(select
            sub.orderid,
            sub.partnerid,
            sub.customerid,
            sum(sub.balancevalue) as thirty_days
            from (

        select 
        	distinct
        	-- distinct ar.balanceseq,
        	-- to_date(ar.serviceorderdate, 'MM/DD/YYYY'),
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            -- ROW_NUMBER() OVER (PARTITION BY so.orderid,ta.payeeid) as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.payeeid as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            to_integer(ar.balancevalue) as balancevalue,
            :v_currency as unit_type,
            :v_currInFile as inboundfile
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_arbalance_in ar on ar.ServiceOrderNumber = so.orderid
            -- and st.compensationdate=to_date(ar.serviceorderdate, 'MM/DD/YYYY')
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) >= 30
            and days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) < 60
            -- and ar.inboundfile = :v_currInFile
            --and ar.processflag = 0
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundfile,
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
    

merge into ext.ctas_arbalance_prestage ar using 
(select
            sub.orderid,
            sub.partnerid,
            sub.customerid,
            sum(sub.balancevalue) as sixty_days
            from (

        select 
        	distinct
        	-- distinct ar.balanceseq,
        	-- to_date(ar.serviceorderdate, 'MM/DD/YYYY'),
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            -- ROW_NUMBER() OVER (PARTITION BY so.orderid,ta.payeeid) as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.payeeid as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            to_integer(ar.balancevalue) as balancevalue,
            :v_currency as unit_type,
            :v_currInFile as inboundfile
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_arbalance_in ar on ar.ServiceOrderNumber = so.orderid
            -- and st.compensationdate=to_date(ar.serviceorderdate, 'MM/DD/YYYY')
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) >= 60
            and days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) < 90
            -- and ar.inboundfile = :v_currInFile
            --and ar.processflag = 0
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundfile,
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
    
merge into ext.ctas_arbalance_prestage ar using 
(select
            sub.orderid,
            sub.partnerid,
            sub.customerid,
            sum(sub.balancevalue) as ninety_days
            from (

        select 
        	distinct
        	-- distinct ar.balanceseq,
        	-- to_date(ar.serviceorderdate, 'MM/DD/YYYY'),
            so.orderid as orderid,
            1 as Linenumber,
            1 as sublinenumber,
            -- ROW_NUMBER() OVER (PARTITION BY so.orderid,ta.payeeid) as sublinenumber,
            :v_eventtypeid as eventtypeid,
            1 as value,
            :v_intUnitType as unittypeforvalue,
            LAST_DAY(current_date) as compensationdate,
            :v_businessUnit as businessunit,
            ta.payeeid as partnerid,
            ar.customerid,
            -- st.genericattribute2 as customername,
            to_integer(ar.balancevalue) as balancevalue,
            :v_currency as unit_type,
            :v_currInFile as inboundfile
        from cs_salestransaction st
            inner join cs_salesorder so on so.salesorderseq = st.salesorderseq
            inner join ext.ctas_arbalance_in ar on ar.ServiceOrderNumber = so.orderid
            -- and st.compensationdate=to_date(ar.serviceorderdate, 'MM/DD/YYYY')
            inner join cs_transactionassignment ta on ta.salestransactionseq = st.salestransactionseq
            and ta.compensationdate = st.compensationdate
            and ta.salesorderseq = st.salesorderseq
            and ta.salesorderseq = so.salesorderseq
        where days_between(to_date(ar.duedate, 'MM/DD/YYYY'), current_date) >= 90
            -- and ar.inboundfile = :v_currInFile
            --and ar.processflag = 0
            ) sub
            group by sub.orderid,
            sub.customerid,
            sub.partnerid,
            sub.unit_type,
            sub.businessunit,
            sub.Linenumber,
            sub.subLinenumber,
            sub.inboundfile,
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

/* handled in validate_data() proc    
insert into ext.ctas_arbalance_error
(select distinct '', orderid, '',customerid, '', '107' , partnerid||' - title is not valid for ARBALANCE' 
from ext.ctas_arbalance_prestage ap_out where not exists (
 select 1
 from cs_title ti
 inner join cs_position pos
 on pos.titleseq=ti.ruleelementownerseq
 inner join cs_payee pay
 on pay.payeeseq=pos.payeeseq
 inner join ext.ctas_arbalance_prestage ap
 on ap.partnerid = pay.payeeid
 where ti.name in ('FST In Training','SSRIT - Assigned','SSRIT - Bench', 'FST', 'SSR'
 ,'Sales Representative'	--remove salesrep, this is only for testing
 )
 and ap.inboundfile= :v_currInFile
 and ti.removedate ='2200-01-01'
 and pos.removedate ='2200-01-01'
 and pay.removedate ='2200-01-01'
 and ap.partnerid=ap_out.partnerid	
 )
 and ap_out.inboundfile=:v_currInFile);

-- Cursor to loop through error table
FOR cur_error as cur_row DO
ext.ctas_error_log (cur_error.errorcode, cur_error.description, :FILENAME,:v_proc_name,1) ;
END FOR;

---do not delete this
--this is to delete positions that are not needed

 delete from ext.ctas_arbalance_prestage ap_out where exists (
 select 1
 from cs_title ti
 inner join cs_position pos
 on pos.titleseq=ti.ruleelementownerseq
 inner join ext.ctas_arbalance_prestage ap
 on ap.partnerid = pos.name
 where ti.name not in ('FST In Training','SSRIT - Assigned','SSRIT - Bench', 'FST', 'SSR'
 ,'Sales Representative'	--remove salesrep, this is only for testing
 )
 and ap.inboundfile= :v_currInFile
 and ti.removedate ='2200-01-01'
 and pos.removedate ='2200-01-01'
 and ap.partnerid=ap_out.partnerid	
 )
 and ap_out.inboundfile=:v_currInFile;

*/
v_filter = 'PARTNERID VALIDATION';
CALL ext.ctas_validate_data(::CURRENT_OBJECT_NAME,:FILENAME,:v_filter,v_status, v_message);
    -- Check the returned status and raise an exception if invalid
    IF :v_status = 0 THEN
        SIGNAL v_error SET MESSAGE_TEXT = :v_message;
    END IF;
select count(1) into v_prestageCount from ext.ctas_arbalance_prestage where inboundfile= :v_currInFile;
ext.ctas_event_log (v_proc_name,'Count of records loaded into prestage for inbound file: '||:v_currInFile,:v_prestageCount);

merge into ext.ctas_arbalance_prestage ap using/*update customer name in prestage*/
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
and ct.removedate=:v_eot
and cat.removedate=:v_eot
and cc.removedate=:v_eot
and cl.removedate=:v_eot) sub on
sub.classifierid=ap.customerid
and ap.inboundfile=:v_currInFile
when matched then update
set ap.customername=sub.description;


/*update sublinenumber since there could be multiple customerid for same partner.If not done this would result in unque constraint 
when loading into staging because the orderid-subline-line combo would be the same becuase we dont include customerid in orderid field*/
merge into ext.ctas_arbalance_prestage ar 
using
(select new_sublinenumber, stagesalestransactionseq from (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY replace(replace(RIGHT(partnerid, 12),'_',''),'-','') ,customerid ORDER BY stagesalestransactionseq) AS new_sublinenumber,
        stagesalestransactionseq
    FROM ext.ctas_arbalance_prestage 
)) sub on
sub.stagesalestransactionseq=ar.stagesalestransactionseq
when matched then 
update set sublinenumber=sub.new_sublinenumber;
 

/*start : delete duplicates to prevent unique constraint violation*/        
delete from cs_stagesalestransaction st where 
exists( select 1 from ext.ctas_arbalance_prestage ap
where 'ARBALANCE' || '_' ||replace(replace(RIGHT(partnerid, 12),'_',''),'-','') || '_'||ap.customerid||'_'||:v_currMONYYYY=st.orderid and
st.linenumber=ap.linenumber and
st.sublinenumber=ap.sublinenumber and
st.eventtypeid=ap.eventtypeid);

delete from cs_stagetransactionassign st where 
exists( select 1 from ext.ctas_arbalance_prestage ap
where 'ARBALANCE' || '_' || replace(replace(RIGHT(partnerid, 12),'_',''),'-','')|| '_'||ap.customerid||'_'||:v_currMONYYYY=st.orderid and
st.linenumber=ap.linenumber and
st.sublinenumber=ap.sublinenumber and
st.eventtypeid=ap.eventtypeid);
/*end : delete duplicates to prevent unique constraint violation*/     

/*start:check for partnerid length*/
/*select count(1) into v_count from ext.ctas_arbalance_prestage ap
where length(ap.Partnerid)>16; --remove this if required

IF :v_count > 0 THEN
SIGNAL invalid_input SET MESSAGE_TEXT ='PartnerId length greater than 16 characters';
END IF;
*/
/*end :check for partnerid length*/

select count(1) into v_errorCount from ext.ctas_arbalance_error;
IF :v_errorCount > 0 THEN
ext.ctas_event_log (:v_proc_name,'There are errors in the inbound file ' ||:v_currInFile ||' please check ext.ctas_arbalance_errors table for more information: ',::ROWCOUNT);
END IF;


/*Load into stagesalestransaction and stagesalestransactionassign*/
insert into cs_stagesalestransaction(
        tenantid,
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
        unittypeforgenericnumber3,
        genericattribute5
    ) (
        select :v_tenantId,
            'ARBALANCE' || '_' || replace(replace(RIGHT(partnerid, 12),'_',''),'-','') || '_'||customerid||'_'||:v_currMONYYYY  as orderid,
            -- 'ARBAL' || '_' || substring(replace(Partnerid,'_',''),1,12) || '_'||customerid||'_'||year(current_date)||substring(MONTHNAME(current_date),1,3)  as orderid,
            :FILENAME,
            Linenumber,
			sublinenumber,
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
            unittypefor90,
            :v_currInFile
        from ext.ctas_arbalance_prestage
        where inboundfile = :v_currInFile
        and partnerid is not null
        order by 1
    );

ext.ctas_event_log (:v_proc_name,'Count of records loaded into stagesalestransaction for inbound file: '||:v_currInFile,::ROWCOUNT);

insert into cs_stagetransactionassign(
        tenantid,
        setnumber,
        batchname,
        orderid,
        Linenumber,
        sublinenumber,
        eventtypeid,
        payeeid,
        payeetype
    ) (
        select distinct :v_tenantId,
            1,
            :FILENAME,
            'ARBALANCE' || '_' || replace(replace(RIGHT(partnerid, 12),'_',''),'-','')|| '_'||customerid||'_'||:v_currMONYYYY  as orderid,
            -- 'ARBALANCE' || '_' || substring(replace(Partnerid,'_',''),1,12) || '_'||customerid||'_'||year(current_date)||substring(MONTHNAME(current_date),1,3)  as orderid,
            Linenumber,
            Sublinenumber,
            eventtypeid,
            PartnerId,
            'Participant'
        from ext.ctas_arbalance_prestage
        where inboundfile = :v_currInFile
        and partnerid is not null
        order by 1
    );

ext.ctas_event_log (:v_proc_name,'Count of records loaded into stagesalestransactionassign for : '||:v_currInFile,::ROWCOUNT);
 
ext.ctas_event_log (:v_proc_name,'End '||:v_currInFile,0);

commit;
 
END