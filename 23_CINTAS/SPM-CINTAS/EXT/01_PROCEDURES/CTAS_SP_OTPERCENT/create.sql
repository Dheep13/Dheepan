CREATE OR REPLACE PROCEDURE EXT.CTAS_SP_OTPERCENT(IN FILENAME varchar(120))
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for the inbound On Time Percentage Feed

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       18-JUL-2023		Deepan		     Initial creation
    1.1       22-SEP-2023       Deepan           Removed capturing of historical data,added logic to remove duplicates

***************************************************************************************************/
DECLARE v_periodstartdate date;
DECLARE v_periodenddate date;
DECLARE v_eot date := to_date('01-JAN-2200', 'DD-MON-YYYY');
DECLARE v_proc_name varchar2(100);
DECLARE v_tenantId varchar2(4);
DECLARE v_eventtypeid varchar2(10);
DECLARE v_businessUnit varchar2(12);
DECLARE v_intUnitType varchar2(12);
DECLARE v_qtyUnitType varchar2(12);
DECLARE v_percentUnitType varchar2(12);
DECLARE v_currInFile varchar2(100);
DECLARE v_currMONYYYY varchar(20);

-- DECLARE invalid_input CONDITION FOR SQL_ERROR_CODE 10000;
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
v_eventtypeid := 'ONTIME';
v_businessUnit := 'FIRE';
v_intUnitType := 'Integer';
v_qtyUnitType :='quantity';
v_percentUnitType :='percent';
v_currInFile = :FILENAME;

/*delete existing records from prestage*/
delete from ext.ctas_otpercent_prestage;


ext.ctas_event_log (:v_proc_name,'Begin '||:v_currInFile,0);
select tenantid into v_tenantId from cs_tenant;

/*create batchnames, get current month and year*/
select 
year(current_date)||substring(MONTHNAME(current_date),1,3)
into v_currMONYYYY
from sys.dummy;


/*Start : Exception handling*/
delete from ext.ctas_otpercent_error;
select * from ext.ctas_otpercent_error;
insert into ext.ctas_otpercent_error
(select * , '100', 
case when LOCATION is null then 'LOCATION is empty.' 
when MVGR2 is null then 'MVGR2 is empty.'
when CONFIRMEDSERV is null then 'CONFIRMEDSERV is empty.' 
when TOTALSERV is null then 'TOTALSERV is empty.'  
when COMPENSATIONDATE is null then 'COMPENSATIONDATE is empty.'
end
from ext.ctas_otpercent_in where LOCATION is null or MVGR2 is null
or CONFIRMEDSERV is null or TOTALSERV is null or COMPENSATIONDATE is null);

select * from ext.ctas_otpercent_error;
insert into ext.ctas_error_detail(TenantID,
	filename,
    ErrorCode ,
    ErrorMessage ,
    processflag)
(select :v_tenantId, :FILENAME, ec.errorcode||': '||ec.Description, 
ae.description,0
from ext.ctas_error_codes ec, ext.ctas_otpercent_error ae
where ec.errorcode=ae.ERRORCODE and ae.ERRORCODE='100');

select * from ext.ctas_error_detail;

insert into ext.ctas_otpercent_error
(
select * , '105' , location||' is not valid.'
from ext.ctas_otpercent_in where location is not null 
and not exists (
	select 1 from cs_genericclassifier where genericattribute2=location
	and removedate =:v_eot
));

insert into ext.ctas_error_detail(TenantID,
	filename,
    ErrorCode ,
    ErrorMessage ,
    processflag)
(select :v_tenantId , :FILENAME, ec.errorcode||': '||ec.Description, 
ae.description,0
from ext.ctas_error_codes ec, ext.ctas_otpercent_error ae
where ec.errorcode=ae.ERRORCODE and ae.ERRORCODE='105');

insert into ext.ctas_otpercent_error
(
select * , '104' , MVGR2||' is not valid.'
from ext.ctas_otpercent_in where location is not null 
and not exists (
	select 1 from cs_classifier where classifierid=mvgr2
	and removedate =:v_eot
));

insert into ext.ctas_error_detail(TenantID,
	filename,
    ErrorCode ,
    ErrorMessage ,
    processflag)
(select :v_tenantId , :FILENAME, ec.errorcode||': '||ec.Description, 
ae.description,0
from ext.ctas_error_codes ec, ext.ctas_otpercent_error ae
where ec.errorcode=ae.ERRORCODE and ae.ERRORCODE='104');


delete from ext.ctas_otpercent_in ar where exists(select 1 from ext.ctas_otpercent_error ae
	where ae.location = ar.location
	and ae.mvgr2 = ar.mvgr2
	and ae.confirmedserv = ar.confirmedserv
	and ae.totalserv = ar.totalserv
	and ae.compensationdate = ar.compensationdate
);

/*End : Exception handling*/


/*for reprocessing same batch*/

    delete from cs_stagesalestransaction st
    where batchname = :FILENAME;
    delete from cs_stagetransactionassign st
    where batchname = :FILENAME;

ext.ctas_event_log (:v_proc_name,'Same file data reset complete '||:v_currInFile,::ROWCOUNT);


insert into ext.ctas_otpercent_prestage(
	ORDERID, 
	LINENUMBER, 
	SUBLINENUMBER,
	EVENTTYPEID, 
	VALUE,
	UNITTYPEFORVALUE, 
	COMPENSATIONDATE,
	BUSINESSUNITNAME,
	GENERICATTRIBUTE1,
	GENERICATTRIBUTE2,
	GENERICNUMBER1,
	UNITTYPEFORGENERICNUMBER1, 
	GENERICNUMBER2,
	UNITTYPEFORGENERICNUMBER2,
	GENERICNUMBER3,
	UNITTYPEFORGENERICNUMBER3,
	PAYEEID,
	inboundfile,
	BATCHNAME
)

select 
distinct 
'ONTIME' || '_' || category.location || '_'||sub.mvgr2||'_'||v_currMONYYYY  as orderid,
1 as Linenumber,
1 as sublinenumber,
:v_eventtypeid as eventtypeid,
1 as value,
:v_intUnitType as unittypeforvalue,
LAST_DAY(current_date) as compensationdate,
:v_businessUnit as businessunit,
category.location,
sub.mvgr2,
sub.sum_confirmedserv,
:v_qtyUnitType,
sub.sum_totalserv,
:v_qtyUnitType,
sub.ontimepercent,
:v_percentUnitType,
-- category.payeeid,
NULL,
:v_currInFile,
:FILENAME
from (
select location,mvgr2, sum(to_decimal(confirmedserv)) as sum_confirmedserv, sum(to_decimal(totalserv)) as sum_totalserv,(sum(to_decimal(confirmedserv))/sum(to_decimal(totalserv)))*100 as ontimepercent from ext.CTAS_OTPERCENT_IN
group by location,mvgr2 ) sub 
inner join (
select distinct gc.genericattribute2 as Location, gc.genericattribute1 as PayeeId from cs_category cat
inner join cs_category_classifiers cc
on cat.ruleelementseq=cc.categoryseq
inner join cs_classifier cl
on cl.classifierseq=cc.classifierseq
inner join cs_genericclassifier gc
on gc.classifierseq=cc.classifierseq
and gc.classifierseq=cl.classifierseq
where cat.name ='Location Category'
and cat.removedate =:v_eot
and cc.removedate =:v_eot
and cl.removedate =:v_eot
and gc.removedate =:v_eot) category
on category.Location=sub.location;

ext.ctas_event_log (:v_proc_name,'On time percent data loaded into prestage '||:v_currInFile,::ROWCOUNT);

/*start : delete duplicates to prevent unique constraint violation*/        
delete from cs_stagesalestransaction st where 
exists( select 1 from ext.ctas_otpercent_prestage ap
where ap.orderid = st.orderid and
st.linenumber=ap.linenumber and
st.sublinenumber=ap.sublinenumber and
st.eventtypeid=ap.eventtypeid);

delete from cs_stagetransactionassign st where 
exists( select 1 from ext.ctas_otpercent_prestage ap
where ap.orderid= st.orderid and
st.linenumber=ap.linenumber and
st.sublinenumber=ap.sublinenumber and
st.eventtypeid=ap.eventtypeid);

/* end: delete duplicates to prevent unique constraint violation*/ 

/*Remove this before code migration-deleting this because name is too long*/
-- delete from ext.ctas_otpercent_prestage where payeeid in ('Test_Payee_SR-S_SR_2023','Test_Payee_SR-S_SS_2023');

/*Load into stagesalestransaction and stagesalestransactionassign*/
insert into cs_stagesalestransaction(
        tenantid,
        -- stagesalestransactionseq,
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
        genericnumber1,
        unittypeforgenericnumber1,
        genericnumber2,
        unittypeforgenericnumber2,
        genericnumber3,
        unittypeforgenericnumber3,
	    genericattribute5)
            
        (select :v_tenantId,
            -- stagesalestransactionseq + :v_maxSeq,
            orderid,
            :FILENAME,
            linenumber,
            sublinenumber,
            eventtypeid,
            value,
            unittypeforvalue,
            compensationdate,
            businessunitname,
            genericattribute1,
            genericnumber1,
            unittypeforgenericnumber1,
            genericnumber2,
            unittypeforgenericnumber2,
            genericnumber3,
            unittypeforgenericnumber3,
            inboundfile
            from ext.ctas_otpercent_prestage
        where inboundfile = :v_currInFile
        order by 1
    );


/* commented out - assignment not needed for OTPERCENT
insert into cs_stagetransactionassign(
        tenantid,
        -- stagesalestransactionseq,
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
            -- stagesalestransactionseq+:v_maxSeq,
            1,
            :v_txta_batchName,
            orderid,
            Linenumber,
            Sublinenumber,
            eventtypeid,
            payeeid,
            'Participant'
        from ext.ctas_otpercent_prestage
        where inboundfile = :v_currInFile
        order by 1
    );

*/

ext.ctas_event_log (:v_proc_name,'On time percent data loaded into staging '||:v_currInFile,::ROWCOUNT);
 
ext.ctas_event_log (:v_proc_name,'End '||:v_currInFile,0);

commit;
END