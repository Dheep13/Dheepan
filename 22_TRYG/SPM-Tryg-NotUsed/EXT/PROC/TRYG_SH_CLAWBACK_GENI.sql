
do begin

declare v_eot date := '2200-01-01';
declare v_semiannual_periodseq bigint;
declare v_annual_periodseq bigint;
declare v_periodRow row like cs_period;
declare v_eventType varchar(25) := 'SC-DK-001-001-SUMMARY';
declare i_PeriodSeq bigint := 2533274790396049;
DECLARE v_unitTypeRow ROW LIKE TCMP.CS_UNITTYPE;
-- declare cursor c_get_geni_credits for

select * into v_periodRow from cs_period where removedate = :v_eot and 
periodseq=:i_periodSeq;

SELECT * INTO v_unitTypeRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_eot AND cu.name = 'quantity';

merge into cs_transactionassignment ta 
using 
(SELECT rn, geni_salestransactionseq,canc_positionname, alternateordernumber, canc_sublinenumber, canc_compensationdate, cancellationdate, canc_oldpremium, canc_credit from (
SELECT DISTINCT 
ROW_NUMBER() OVER (PARTITION BY st_in.alternateordernumber ORDER by st_in.alternateordernumber,st_in.compensationdate, st_in.sublinenumber,st_in.genericdate3 desc) AS rn,
st_in.alternateordernumber,
st_in.sublinenumber as canc_sublinenumber,
st_in.compensationdate as canc_compensationdate,
geni_txn.salestransactionseq as geni_salestransactionseq,
sta_in.positionname as canc_positionname,
st_in.genericdate3 as cancellationdate,
st_in.genericnumber1 as canc_oldpremium,
--st_in.genericnumber2 as newpremium,
sum(IFNULL(c.value, 0)) as canc_credit
FROM cs_salestransaction st_in
INNER JOIN cs_transactionassignment sta_in ON sta_in.salestransactionseq = st_in.salestransactionseq
AND sta_in.compensationdate = st_in.compensationdate
INNER JOIN cs_position pos
on pos.name=sta_in.positionname
and pos.removedate = :v_eot
LEFT join cs_credit c on 
st_in.salestransactionseq=c.salestransactionseq
and sta_in.compensationdate=c.compensationdate
and st_in.compensationdate=c.compensationdate
and st_in.alternateordernumber=c.genericattribute3
and pos.ruleelementownerseq=c.positionseq
INNER JOIN cs_eventtype et ON et.datatypeseq = st_in.eventtypeseq
AND et.removedate = :v_eot
INNER JOIN (select alternateordernumber, salestransactionseq, compensationdate from cs_salestransaction where compensationdate between :v_periodRow.startDate
and :v_periodRow.endDate and genericattribute1 in ('GENI','GESK')
and eventtypeseq=(select datatypeseq from cs_eventtype where eventtypeid=:v_eventType
and removedate=:v_eot
)) geni_txn
on st_in.alternateordernumber=geni_txn.alternateordernumber
WHERE st_in.genericdate3 IS NOT NULL
-- AND sta_in.genericattribute10='CANC'
AND sta_in.processingunitseq = 38280596832649318
AND st_in.genericnumber1 > st_in.genericnumber2
AND geni_txn.compensationdate > st_in.compensationdate
AND et.eventtypeid = :v_eventType
AND MONTHS_BETWEEN(ifnull(geni_txn.compensationdate,add_months(st_in.genericdate3,13)), st_in.genericdate3) < 12
group by st_in.alternateordernumber,
st_in.sublinenumber,
st_in.compensationdate,
st_in.genericdate3,
geni_txn.compensationdate,
geni_txn.salestransactionseq,
sta_in.positionname,
st_in.genericnumber1
order by st_in.alternateordernumber,st_in.compensationdate, st_in.sublinenumber,st_in.genericdate3 desc) A
where rn=1) subq
on ta.salestransactionseq=subq.geni_salestransactionseq
and ta.positionname=subq.canc_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(subq.canc_credit,0), ta.genericnumber3 = subq.canc_oldpremium,
    ta.unittypeforgenericnumber2 = :v_unitTypeRow.unittypeseq ,ta.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq  ;
end;


1	6200003698174	20230213830463	2023-02-13 00:00:00.000000000	2023-02-01 00:00:00.000000000	5686.0000000000	0.0000000000
1	6200004156488	202301171456108	2023-01-17 00:00:00.000000000	2022-12-20 00:00:00.000000000	8195.0000000000	0.0000000000
1	6200008162423	2023020333307	2023-02-03 00:00:00.000000000	2023-02-01 00:00:00.000000000	20973.0000000000	0.0000000000
1	6200011491688	202302031416072	2023-02-03 00:00:00.000000000	2023-02-01 00:00:00.000000000	5399.0000000000	0.0000000000
1	6200011699875	2023011730565	2023-01-17 00:00:00.000000000	2023-01-13 00:00:00.000000000	19266.0000000000	0.0000000000
1	6200013078324	2023021625119	2023-02-16 00:00:00.000000000	2023-01-12 00:00:00.000000000	11036.0000000000	0.0000000000
1	6200014176670	2022122727290	2022-12-27 00:00:00.000000000	2022-09-01 00:00:00.000000000	7279.0000000000	60.0000000000
1	6200014184690	20230214848110	2023-02-14 00:00:00.000000000	2023-01-17 00:00:00.000000000	29124.0000000000	0.0000000000
1	6200014476620	2023012032031	2023-01-20 00:00:00.000000000	2022-12-16 00:00:00.000000000	5328.0000000000	0.0000000000
1	6550000578694	20230109813527	2023-01-09 00:00:00.000000000	2023-01-01 00:00:00.000000000	3796.0000000000	-1990.0000000000






do begin

declare v_eot date := '2200-01-01';
declare v_semiannual_periodseq bigint;
declare v_annual_periodseq bigint;
declare v_periodRow row like cs_period;
declare v_eventType varchar(25) := 'SC-DK-001-001-SUMMARY';
declare i_PeriodSeq bigint := 2533274790396049;
declare v_processingUnitseq bigint;
DECLARE v_unitTypeRow ROW LIKE TCMP.CS_UNITTYPE;
-- declare cursor c_get_geni_credits for

select * into v_periodRow from cs_period where removedate = :v_eot and 
periodseq=:i_periodSeq;

select processingunitseq into v_processingUnitseq from cs_processingunit where name='DK';

SELECT * INTO v_unitTypeRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_eot AND cu.name = 'quantity';

merge into cs_transactionassignment ta 
using 
(
SELECT rn, geni_salestransactionseq, geni_compensationdate, geni_setnumber, geni_salesorderseq, canc_positionname, alternateordernumber, canc_sublinenumber, 
canc_compensationdate, cancellationdate, canc_oldpremium, canc_credit
from (
SELECT DISTINCT 
ROW_NUMBER() OVER (PARTITION BY st_in.alternateordernumber ORDER by st_in.compensationdate desc , st_in.sublinenumber desc ,st_in.genericdate3 desc) AS rn,
st_in.alternateordernumber,
st_in.sublinenumber as canc_sublinenumber,
st_in.compensationdate as canc_compensationdate,
geni_txn.salestransactionseq as geni_salestransactionseq,
geni_txn.salesorderseq as geni_salesorderseq,
geni_txn.compensationdate as geni_compensationdate,
geni_txn.positionname as geni_positionname,
geni_txn.setnumber as geni_setnumber,
sta_in.positionname as canc_positionname,
st_in.genericdate3 as cancellationdate,
st_in.genericnumber1 as canc_oldpremium,
--st_in.genericnumber2 as newpremium,
sum(IFNULL(c.value, 0)) as canc_credit
FROM cs_salestransaction st_in
INNER JOIN cs_transactionassignment sta_in ON sta_in.salestransactionseq = st_in.salestransactionseq
AND sta_in.compensationdate = st_in.compensationdate
INNER JOIN cs_position pos
on pos.name=sta_in.positionname
and pos.removedate = :v_eot
LEFT join cs_credit c on 
st_in.salestransactionseq=c.salestransactionseq
and sta_in.compensationdate=c.compensationdate
and st_in.compensationdate=c.compensationdate
and st_in.alternateordernumber=c.genericattribute3
and pos.ruleelementownerseq=c.positionseq
INNER JOIN cs_eventtype et ON et.datatypeseq = st_in.eventtypeseq
AND et.removedate = :v_eot
INNER JOIN (select st.alternateordernumber, st.salestransactionseq, st.salesorderseq, ta.positionname,ta.setnumber,st.compensationdate 
from cs_salestransaction st, cs_transactionassignment ta
where st.compensationdate between :v_periodRow.startDate
and :v_periodRow.endDate 
and st.genericattribute1 in ('GENI','GESK')
and ta.salestransactionseq=st.salestransactionseq
and ta.compensationdate = st.compensationdate
and st.eventtypeseq=(select datatypeseq from cs_eventtype where eventtypeid=:v_eventType
and removedate=:v_eot
)) geni_txn
on st_in.alternateordernumber=geni_txn.alternateordernumber
WHERE st_in.genericdate3 IS NOT NULL
-- AND sta_in.genericattribute10='CANC'
AND st_in.genericattribute4='AFGA'
AND sta_in.processingunitseq = :v_processingUnitseq
AND st_in.genericnumber1 > st_in.genericnumber2
AND geni_txn.compensationdate > st_in.compensationdate
AND et.eventtypeid = :v_eventType
-- AND MONTHS_BETWEEN(ifnull(geni_txn.compensationdate,add_months(st_in.genericdate3,13)), st_in.genericdate3) < 12
group by st_in.alternateordernumber,
st_in.sublinenumber,
st_in.compensationdate,
st_in.genericdate3,
geni_txn.compensationdate,
geni_txn.salestransactionseq,
sta_in.positionname,
st_in.genericnumber1,
geni_txn.positionname,
geni_txn.salesorderseq ,
geni_txn.compensationdate,
geni_txn.setnumber
order by st_in.compensationdate desc , st_in.sublinenumber desc ,st_in.genericdate3 desc) A
where rn=1
) subq
on ta.salestransactionseq=subq.geni_salestransactionseq
and ta.positionname=subq.canc_positionname
WHEN MATCHED THEN
    UPDATE SET ta.genericnumber2 = ifnull(subq.canc_credit,0), ta.genericnumber3 = subq.canc_oldpremium,
    ta.unittypeforgenericnumber2 = :v_unitTypeRow.unittypeseq ,ta.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq 
WHEN NOT MATCHED THEN
    INSERT (tenantid,salestransactionseq,setnumber,positionname,compensationdate,salesorderseq,processingunitseq,
    genericnumber2,unittypeforgenericnumber2,genericnumber3,unittypeforgenericnumber3) 
    values ('1951', subq.geni_salestransactionseq , (subq.geni_setnumber)+1,
    -- max( ta.setnumber)+ ROW_NUMBER() OVER (ORDER BY  ta.setnumber) ,
    subq.canc_positionname, subq.geni_compensationdate, subq.geni_salesorderseq, :v_processingUnitseq , 
    ifnull(subq.canc_credit,0),:v_unitTypeRow.unittypeseq, subq.canc_oldpremium, :v_unitTypeRow.unittypeseq   --  salestransactionseq=ta.salestransactionseq
    );


end;