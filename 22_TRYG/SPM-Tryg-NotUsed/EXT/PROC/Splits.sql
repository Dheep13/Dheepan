
do begin

declare v_eot date := '2200-01-01';
declare v_lt varchar(25) := 'LT_Pension_Agent_Check';
declare v_dimName varchar(5) :='Title'
declare v_semiannual_periodseq bigint;
declare v_annual_periodseq bigint;
declare v_periodRow row like cs_period;
declare v_eventtypeid varchar(25) := :v_eventtypeid;
declare i_PeriodSeq bigint := 2533274790396049;
declare v_processingUnitseq bigint;
DECLARE v_unitTypeRow ROW LIKE TCMP.CS_UNITTYPE;
-- declare cursor c_get_geni_credits for

select * into v_periodRow from cs_period where removedate = :v_eot and 
periodseq=:i_periodSeq;

-- select processingunitseq into v_processingUnitseq from cs_processingunit where name='DK';

-- SELECT * INTO v_unitTypeRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_eot AND cu.name = 'quantity';


update cs_stagesalestransaction st set genericboolean3=1 where exists(
select alternateordernumber, sublinenumber, eventtypeid, count(1)
from cs_stagesalestransaction where batchname =:v_batchName
and st.alternateordernumber=alternateordernumber
and st.sublinenumber=sublinenumber
and st.eventtypeid=eventtypeid
and eventtypeid=:v_eventtypeid
group by alternateordernumber, sublinenumber, eventtypeid having count(1)>1 )
and batchname=:v_batchName
and eventtypeid=:v_eventtypeid;


update cs_stagesalestransaction st set st.genericattribute18='Pension Split' where exists (select 
sub.stagesalestransactionseq, sub.positionname,dim0.name, ind0.MINSTRING from cs_relationalmdlt mdlt
inner join cs_mdltdimension dim0 on 
mdlt.ruleelementseq = dim0.ruleelementseq 
and dim0.removedate = :v_eot
inner join cs_mdltindex ind0 on 
mdlt.ruleelementseq = ind0.ruleelementseq 
and ind0.dimensionseq = dim0.dimensionseq
and ind0.removedate = :v_eot
inner join 
(select sta.positionname,
ti.name, st.stagesalestransactionseq
from cs_stagesalestransaction st 
inner join cs_stagetransactionassign sta on
sta.stagesalestransactionseq=st.stagesalestransactionseq
inner join cs_position pos on
sta.positionname=pos.name and
st.compensationdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
inner join cs_title ti on
ti.ruleelementownerseq=pos.titleseq
where st.batchname=:v_batchName
and st.eventtypeid=:v_eventtypeid 
and st.genericboolean3=1
and ti.removedate =:v_eot
and pos.removedate =:v_eot
) sub on  
sub.name=ind0.minstring
where mdlt.name = :v_lt
and dim0.name=:v_dimName
and mdlt.removedate =:v_eot
and dim0.removedate =:v_eot
and st.stagesalestransactionseq = sub.stagesalestransactionseq
)
and batchname=:v_batchName
and eventtypeid=:v_eventtypeid;


end;



merge into cs_stagesalestransaction st 
using 
(select distinct sub.positionname, sub.stagesalestransactionseq, sub.name as titlename, sub.positionname, c.name as splittype
from cs_category c
inner join cs_categorytree ct on 
c.categorytreeseq=ct.categorytreeseq
inner join cs_category_classifiers cc on
cc.categoryseq=c.ruleelementseq
and cc.categorytreeseq=ct.categorytreeseq
and cc.categorytreeseq=c.categorytreeseq
inner join cs_classifier cl on
cc.classifierseq=cl.classifierseq
inner join (select sta.positionname,
ti.name, st.stagesalestransactionseq
from cs_stagesalestransaction st 
inner join cs_stagetransactionassign sta on
sta.stagesalestransactionseq=st.stagesalestransactionseq
inner join cs_position pos on
sta.positionname=pos.name and
st.compensationdate between pos.effectivestartdate and add_days(pos.effectiveenddate,-1)
inner join cs_title ti on
ti.ruleelementownerseq=pos.titleseq
where st.batchname=:v_batchName
and st.eventtypeid=:v_eventtype
and st.genericboolean4=1
and ti.removedate =:v_eot
and pos.removedate =:v_eot
-- and st.stagesalestransactionseq=56320372	
) sub on
sub.name = cl.classifierid
where ct.name='Agent Classification'
and ct.removedate =:v_eot 
and cl.removedate =:v_eot 
and c.removedate =:v_eot 
and cc.removedate =:v_eot) subq
on st.stagesalestransactionseq=subq.stagesalestransactionseq
and batchname = :v_batchName
when matched then
    update set st.genericattribute18 = subq.splittype;
;