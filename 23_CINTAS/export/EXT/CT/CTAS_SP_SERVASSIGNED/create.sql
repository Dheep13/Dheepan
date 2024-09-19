CREATE PROCEDURE EXT.CTAS_SP_SERVASSIGNED(IN FILENAME varchar(120))
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for the inbound On Time Percentage Feed

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       20-JUL-2023		Deepan		     Initial creation
    1.1       22-SEP-2023       Deepan           Removed capturing of historical data,added logic to remove duplicates, 
                                                 removed unused variables, exception handling
***************************************************************************************************/
DECLARE v_eot date := to_date('01-JAN-2200', 'DD-MON-YYYY');
DECLARE v_currency varchar2(4);
DECLARE v_proc_name varchar2(100);
DECLARE v_tenantId varchar2(4);
DECLARE v_eventtypeid varchar2(10);
DECLARE v_businessUnit varchar2(12);
DECLARE v_intUnitType varchar2(12);
DECLARE v_currInFile varchar2(100);
DECLARE v_status nvarchar(2);
DECLARE v_message nvarchar(100);
DECLARE v_prestageCount number;
DECLARE v_currMONYYYY varchar2(10);
DECLARE v_filter nvarchar(100);
DECLARE CURSOR cur_row FOR
SELECT * from ext.ctas_servassigned_error;
DECLARE v_error CONDITION FOR SQL_ERROR_CODE 10000;
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
v_tenantId := '';
v_eventtypeid := 'DAILYKPISA';
v_businessUnit := 'FIRE';
v_intUnitType := 'Integer';
v_currInFile := '';
v_prestageCount :=0;
v_currMONYYYY := '';
v_currInFile = FILENAME;
v_filter='';
ext.ctas_event_log (:v_proc_name,'Begin '||:v_currInFile,0);
select tenantid  into v_tenantId from cs_tenant;

/*delete existing records from prestage*/
truncate table ext.ctas_servassigned_prestage;

/*Start : Exception handling*/
/*
truncate table ext.ctas_servassigned_error;

insert into ext.ctas_servassigned_error
(select * , '100', 
case when PARTNERID is null then 'PARTNERID is empty.' 
when COMPENSATIONDATE is null then 'COMPENSATIONDATE is empty.'
end
from ext.ctas_servassigned_in where PARTNERID is null or COMPENSATIONDATE is null);

insert into ext.ctas_servassigned_error
(
select * , '106' , partnerid || ' is not valid.'
from ext.ctas_servassigned_in sa where sa.partnerid is not null 
and not exists (
	select 1 from cs_payee where payeeid=sa.partnerid
	and removedate =:v_eot
));


delete from ext.ctas_servassigned_in ar where exists(select 1 from ext.ctas_servassigned_error ae
	where ifnull(ae.partnerid,'1') = ifnull(ar.partnerid,'1')
	and ifnull(ae.compensationdate,'1') = ifnull(ar.compensationdate,'1')
	and ifnull(ae.servassigned,'1')= ifnull(ar.servassigned,'1')
);

--this is to delete positions that are not needed. Only 'FST In Training','SSRIT - Assigned','SSRIT - Bench', 'FST', 'SSR' titles are valid for service assigned feed

insert into ext.ctas_servassigned_error
(
select * , '108' , partnerid || 'title is not valid for service assigned feed'
from ext.ctas_servassigned_in sa where sa.partnerid is not null 
and not exists (
 select 1
 from cs_title ti
 inner join cs_position pos
 on pos.titleseq=ti.ruleelementownerseq
 inner join cs_payee pay
 on sa.partnerid = pay.payeeid
 and pay.payeeseq=pos.payeeseq
 where ti.name in ('FST In Training','SSRIT - Assigned','SSRIT - Bench', 'FST', 'SSR')
 and ti.removedate =:v_eot
 and pos.removedate =:v_eot
 and pay.removedate =:v_eot
 ));


delete from ext.ctas_servassigned_in sa where sa.partnerid is not null 
and not exists (
 select 1
 from cs_title ti
 inner join cs_position pos
 on pos.titleseq=ti.ruleelementownerseq
 inner join cs_payee pay
 on sa.partnerid = pay.payeeid
 and pay.payeeseq=pos.payeeseq
 where ti.name in ('FST In Training','SSRIT - Assigned','SSRIT - Bench', 'FST', 'SSR')
 and ti.removedate =:v_eot
 and pos.removedate =:v_eot
 and pay.removedate =:v_eot
 );
 
-- Cursor to loop through error table
FOR cur_error as cur_row DO
ext.ctas_error_log (cur_error.errorcode, cur_error.description, :FILENAME,:v_proc_name,1) ;
END FOR;
*/
/*End : Exception handling*/

v_filter = NULL;
CALL ext.ctas_validate_data(::CURRENT_OBJECT_NAME,:FILENAME,:v_filter,v_status, v_message);
    -- Check the returned status and raise an exception if invalid
    IF :v_status = 0 THEN
        SIGNAL v_error SET MESSAGE_TEXT = :v_message;
    END IF;

v_filter = 'PARTNERID VALIDATION-SERVASSIGNED';
CALL ext.ctas_validate_data(::CURRENT_OBJECT_NAME,:FILENAME,:v_filter,v_status, v_message);
    -- Check the returned status and raise an exception if invalid
    IF :v_status = 0 THEN
        SIGNAL v_error SET MESSAGE_TEXT = :v_message;
    END IF;

/*get current month and year*/
select 
year(current_date)||substring(MONTHNAME(current_date),1,3)
into v_currMONYYYY
from sys.dummy;

/*for reprocessing same batch*/
   delete from cs_stagetransactionassign ta
   where batchname= :filename;

    delete from cs_stagesalestransaction st
    where batchname= :filename;
    
    ext.ctas_event_log (:v_proc_name,'Same file data reset complete '||:v_currInFile,::ROWCOUNT);
    commit;  


insert into ext.ctas_servassigned_prestage(
	ORDERID, 
	LINENUMBER, 
	SUBLINENUMBER,
	EVENTTYPEID, 
	VALUE,
	UNITTYPEFORVALUE, 
	COMPENSATIONDATE,
	BUSINESSUNITNAME,
	GENERICNUMBER1,
	UNITTYPEFORGENERICNUMBER1,
    INBOUNDFILE,
    PARTNERID,
    BATCHNAME
)
select 
distinct 
'KPISA' || '_' || partnerid || '_'||:v_currMONYYYY  as orderid,
1 as Linenumber,
1 as sublinenumber,
:v_eventtypeid as eventtypeid,
1 as value,
:v_intUnitType as unittypeforvalue,
TO_DATE(compensationdate, 'MMDDYYYY')  as compensationdate,
:v_businessUnit as businessunit,
SUM(CAST(servassigned AS INTEGER)) as genericnumber1,
:v_intUnitType,
:v_currInFile,
partnerid,
:FILENAME
from 
ext.ctas_servassigned_in
group by partnerid,compensationdate;

ext.ctas_event_log (:v_proc_name,'Service Assigned KPI data loaded into prestage '||:v_currInFile,::ROWCOUNT);
 
 

 /*start : delete duplicates to prevent unique constraint violation*/        
delete from cs_stagesalestransaction st where 
exists( select 1 from ext.ctas_servassigned_prestage ap
where ap.orderid = st.orderid and
st.linenumber=ap.linenumber and
st.sublinenumber=ap.sublinenumber and
st.eventtypeid=ap.eventtypeid);

delete from cs_stagetransactionassign st where 
exists( select 1 from ext.ctas_servassigned_prestage ap
where ap.orderid= st.orderid and
st.linenumber=ap.linenumber and
st.sublinenumber=ap.sublinenumber and
st.eventtypeid=ap.eventtypeid);

/* end: delete duplicates to prevent unique constraint violation*/ 

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
        genericnumber1,
        unittypeforgenericnumber1,
	    genericattribute5)
            
        (select :v_tenantId,
            orderid,
            :FILENAME,
            linenumber,
            sublinenumber,
            eventtypeid,
            value,
            unittypeforvalue,
            compensationdate,
            businessunitname,
            genericnumber1,
            unittypeforgenericnumber1,
            inboundfile
            from ext.ctas_servassigned_prestage
        where inboundfile = :v_currInFile
        order by 1
    );

insert into cs_stagetransactionassign(
        tenantid,
        setnumber,
        batchname,
        orderid,
        linenumber,
        sublinenumber,
        eventtypeid,
        payeeid,
        payeetype
    ) (
        select :v_tenantId,
            1,
            :FILENAME,
            orderid,
            Linenumber,
            Sublinenumber,
            eventtypeid,
            partnerid,
            'Participant'
        from ext.ctas_servassigned_prestage
        where inboundfile = :v_currInFile
        order by 1
    );

ext.ctas_event_log (:v_proc_name,'Service Assigned KPI data loaded into staging '||:v_currInFile,::ROWCOUNT);

ext.ctas_event_log (:v_proc_name,'End '||:v_currInFile,0);
 
COMMIT;
END