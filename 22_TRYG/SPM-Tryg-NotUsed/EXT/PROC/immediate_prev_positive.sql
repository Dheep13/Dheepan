update 
set tas.genericdate6 = latest_enddate.latest_policyenddate

cs_transactionassignment tas 

inner join cs_salestransaction st on
st.salestransactionseq=tas.salestransactionseq

inner join (select A.row_num, alternateordernumber, prev_salestransactionseq, latest_salestransactionseq, latest_policyenddate, latest_sublinenumber from (
SELECT ROW_NUMBER() OVER ( PARTITION BY st.alternateordernumber ORDER BY st.compensationdate desc, st.genericdate2 DESC,
st.sublinenumber desc ) as row_num, st.alternateordernumber as alternateordernumber, st.salestransactionseq as prev_salestransactionseq, 
st_new.salestransactionseq as latest_salestransactionseq, st.genericdate2 as latest_policyenddate, st.sublinenumber as latest_sublinenumber
from ext.tryg_clawback_credits clc
inner join cs_salestransaction st
on
st.compensationdate <= clc.cantxns_compdate
and st.compensationdate >= (clc.cantxns_policy_sDate)
and st.compensationdate < add_months(clc.cantxns_policy_sDate,12)
and st.genericdate1= clc.cantxns_policy_sDate
and st.alternateordernumber = clc.cantxns_alternateordernumber
and st.eventtypeseq =(select  datatypeseq from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and removedate='2200-01-01')
inner join cs_transactionassignment ta
on ta.salestransactionseq = st.salestransactionseq
inner join cs_salestransaction st_new
on st_new.alternateordernumber=clc.cantxns_alternateordernumber
and st_new.salestransactionseq=clc.cantxns_salestransactionseq

where st.genericdate3 is null
and st.genericattribute1 <> 'AFGA'
and st.genericnumber2 > st.genericnumber1
-- and st_new.salestransactionseq = tas.salestransactionseq
and st.genericattribute1 not in ('GENI', 'GESK')) A
where A.row_num=1) latest_enddate

on 
latest_enddate.latest_salestransactionseq=tas.salestransactionseq
and latest_enddate.latest_salestransactionseq=st.salestransactionseq

where exists(select 1 from ext.tryg_clawback_credits clc_sub
	where clc_sub.cantxns_salestransactionseq=tas.salestransactionseq
    and tas.compensationdate >= :v_periodRow.startDate
    and tas.compensationdate < :v_periodRow.endDate
	and clc_sub.periodseq=2533274790396025
);



2023-01-25 00:00:00.000000000


select tas.*
from
cs_transactionassignment tas 
inner join cs_salestransaction st on
st.salestransactionseq=tas.salestransactionseq

inner join (select A.row_num, alternateordernumber, prev_salestransactionseq, latest_salestransactionseq, latest_policyenddate, latest_sublinenumber from (
SELECT ROW_NUMBER() OVER ( PARTITION BY st.alternateordernumber ORDER BY st.compensationdate desc, st.genericdate2 DESC,
st.sublinenumber desc ) as row_num, st.alternateordernumber as alternateordernumber, st.salestransactionseq as prev_salestransactionseq, 
st_new.salestransactionseq as latest_salestransactionseq, st.genericdate2 as latest_policyenddate, st.sublinenumber as latest_sublinenumber
from ext.tryg_clawback_credits clc
inner join cs_salestransaction st
on
st.compensationdate <= clc.cantxns_compdate
and st.compensationdate >= (clc.cantxns_policy_sDate)
and st.compensationdate < add_months(clc.cantxns_policy_sDate,12)
and st.genericdate1= clc.cantxns_policy_sDate
and st.alternateordernumber = clc.cantxns_alternateordernumber
and st.eventtypeseq =(select  datatypeseq from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and removedate='2200-01-01')
inner join cs_transactionassignment ta
on ta.salestransactionseq = st.salestransactionseq
inner join cs_salestransaction st_new
on st_new.alternateordernumber=clc.cantxns_alternateordernumber
and st_new.salestransactionseq=clc.cantxns_salestransactionseq

where st.genericdate3 is null
and st.genericattribute1 <> 'AFGA'
and st.genericnumber2 > st.genericnumber1
-- and st_new.salestransactionseq = tas.salestransactionseq
and st.genericattribute1 not in ('GENI', 'GESK')) A
where A.row_num=1) latest_enddate

on latest_enddate.latest_salestransactionseq=tas.salestransactionseq
and latest_enddate.latest_salestransactionseq=st.salestransactionseq

where tas.salestransactionseq in (select clc_sub.cantxns_salestransactionseq from ext.tryg_clawback_credits clc_sub
	where  clc_sub.cantxns_compdate between '2023-01-01' and '2023-01-31'
);




merge into cs_transactionassignment ta 
using (
select st.alternateordernumber,st.genericdate2 as policyenddate,tas.salestransactionseq as latest_salestransactionseq, tas.setnumber as latest_setnumber
,tas.compensationdate as latest_compensationdate
from
cs_transactionassignment tas 
inner join cs_salestransaction st on
st.salestransactionseq=tas.salestransactionseq
inner join (select A.row_num, alternateordernumber, prev_salestransactionseq, latest_salestransactionseq, latest_policyenddate, latest_sublinenumber from (
SELECT ROW_NUMBER() OVER ( PARTITION BY st.alternateordernumber ORDER BY st.compensationdate desc, st.genericdate2 DESC,
st.sublinenumber desc ) as row_num, st.alternateordernumber as alternateordernumber, st.salestransactionseq as prev_salestransactionseq, 
st_new.salestransactionseq as latest_salestransactionseq, st.genericdate2 as latest_policyenddate, st.sublinenumber as latest_sublinenumber
from ext.tryg_clawback_credits clc
inner join cs_salestransaction st
on
st.compensationdate <= clc.cantxns_compdate
and st.compensationdate >= (clc.cantxns_policy_sDate)
and st.compensationdate < add_months(clc.cantxns_policy_sDate,12)
and st.genericdate1= clc.cantxns_policy_sDate
and st.alternateordernumber = clc.cantxns_alternateordernumber
and st.eventtypeseq =(select  datatypeseq from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and removedate='2200-01-01')
inner join cs_transactionassignment ta
on ta.salestransactionseq = st.salestransactionseq
inner join cs_salestransaction st_new
on st_new.alternateordernumber=clc.cantxns_alternateordernumber
and st_new.salestransactionseq=clc.cantxns_salestransactionseq

where st.genericdate3 is null
and st.genericattribute1 <> 'AFGA'
and st.genericnumber2 > st.genericnumber1
-- and st_new.salestransactionseq = tas.salestransactionseq
and st.genericattribute1 not in ('GENI', 'GESK')) A
where A.row_num=1) latest_enddate

on latest_enddate.latest_salestransactionseq=tas.salestransactionseq
and latest_enddate.latest_salestransactionseq=st.salestransactionseq

where exists  (select clc_sub.cantxns_salestransactionseq from ext.tryg_clawback_credits clc_sub
	where  clc_sub.cantxns_compdate between '2023-01-01' and '2023-01-31'
	and clc_sub.cantxns_salestransactionseq=tas.salestransactionseq
	and clc_sub.cantxns_alternateordernumber in (6200004199488,
6200004284502,
6000000106302)
	
)) sub 
on ta.salestransactionseq=sub.latest_salestransactionseq
and ta.setnumber=sub.latest_setnumber
and ta.compensationdate = sub.latest_compensationdate

when matched then 
update set ta.genericdate6 = sub.policyenddate
;