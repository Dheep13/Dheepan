do begin
create table ext.bkp_20230619_payee as (select * from cs_payee where removedate = '2200-01-01');
create table ext.bkp_20230619_participant as (select * from cs_participant where removedate = '2200-01-01');
create table ext.bkp_20230619_gaparticipant as (select * from cs_gaparticipant where removedate = '2200-01-01');
create table ext.bkp_20230619_position as (select * from cs_position where removedate = '2200-01-01');
create table ext.bkp_20230619_ruleelementowner as (select * from cs_ruleelementowner where removedate = '2200-01-01');
end;

select * from ext.bkp_20230619_participant
minus
select * from cs_participant where removedate= '2200-01-01';


select * from cs_position where removedate= '2200-01-01'
minus 
select * from ext.bkp_20230619_position where removedate= '2200-01-01';



insert into cs_stageparticipant
select 
pay.tenantid,
(select max(stageparticipantseq) from cs_stageparticipant) + row_number() over (order by pay.payeeid, pay.effectivestartdate) as stageparticipantseq,
'JC_'||lpad(row_number() over (partition by pay.payeeid order by pay.effectivestartdate),3,'0') AS batchname,
pay.payeeid,
pay.effectivestartdate,
pay.effectiveenddate,
null as payeeseq,
par.prefix,
par.firstname,
par.middlename,
par.lastname,
par.suffix,
par.taxid,
par.salary,
uts.name as unittypeforsalary,
par.hiredate,
par.terminationdate,
null as stageprocessdate,
0 as stageprocessflag,
bu.name as businessunitname,
null as businessunitmap,
null as description,
par.genericattribute1, par.genericattribute2, par.genericattribute3, par.genericattribute4,
par.genericattribute5, par.genericattribute6, par.genericattribute7, par.genericattribute8,
par.genericattribute9, par.genericattribute10, par.genericattribute11, par.genericattribute12,
par.genericattribute13, par.genericattribute14, par.genericattribute15, par.genericattribute16,
par.genericnumber1, ut1.name as unittypeforgenericnumber1,
par.genericnumber2, ut2.name as unittypeforgenericnumber2,
par.genericnumber3, ut3.name as unittypeforgenericnumber3,
par.genericnumber4, ut4.name as unittypeforgenericnumber4,
par.genericnumber5, ut5.name as unittypeforgenericnumber5,
par.genericnumber6, ut6.name as unittypeforgenericnumber6,
par.genericdate1,par.genericdate2,par.genericdate3,par.genericdate4,par.genericdate5,par.genericdate6,
par.genericboolean1,par.genericboolean2,par.genericboolean3,par.genericboolean4,par.genericboolean5,par.genericboolean6,
par.userid,
null as stageerrorcode,
par.participantemail,
par.preferredlanguage,
ec.name as eventcalendar
from ext.bkp_20230619_payee pay
join ext.bkp_20230619_participant par on pay.payeeseq = par.payeeseq and pay.effectivestartdate = par.effectivestartdate
left outer join ext.bkp_20230619_gaparticipant gap on pay.payeeseq = gap.payeeseq and pay.effectivestartdate = gap.effectivestartdate
left outer join cs_eventcalendar ec on par.eventcalendarseq = ec.eventcalendarseq and ec.removedate = '2200-01-01'
left outer join cs_unittype uts on par.unittypeforsalary = uts.unittypeseq and uts.removedate = '2200-01-01'
left outer join cs_unittype ut1 on par.unittypeforgenericnumber1 = ut1.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut2 on par.unittypeforgenericnumber2 = ut2.unittypeseq and ut2.removedate = '2200-01-01'
left outer join cs_unittype ut3 on par.unittypeforgenericnumber3 = ut3.unittypeseq and ut3.removedate = '2200-01-01'
left outer join cs_unittype ut4 on par.unittypeforgenericnumber4 = ut4.unittypeseq and ut4.removedate = '2200-01-01'
left outer join cs_unittype ut5 on par.unittypeforgenericnumber5 = ut5.unittypeseq and ut5.removedate = '2200-01-01'
left outer join cs_unittype ut6 on par.unittypeforgenericnumber6 = ut6.unittypeseq and ut6.removedate = '2200-01-01'
left outer join cs_businessunit bu on bitand(pay.businessunitmap, bu.mask) > 0
;

insert into cs_gastageparticipant
select
pay.tenantid,
null as stageparticipantseq,
ifnull(gap.pagenumber,0) as pagenumber,
'JC_'||lpad(row_number() over (partition by pay.payeeseq order by pay.effectivestartdate),3,'0') AS batchname,
pay.payeeid,
null as payeeseq,
gap.genericattribute1,gap.genericattribute2,gap.genericattribute3,gap.genericattribute4,
gap.genericattribute5,gap.genericattribute6,gap.genericattribute7,gap.genericattribute8,
gap.genericattribute9,gap.genericattribute10,gap.genericattribute11,gap.genericattribute12,
gap.genericattribute13,gap.genericattribute14,gap.genericattribute15,gap.genericattribute16,
gap.genericattribute17,gap.genericattribute18,gap.genericattribute19,gap.genericattribute20,
gap.genericdate1,gap.genericdate2,gap.genericdate3,gap.genericdate4,
gap.genericdate5,gap.genericdate6,gap.genericdate7,gap.genericdate8,
gap.genericdate9,gap.genericdate10,gap.genericdate11,gap.genericdate12,
gap.genericdate13,gap.genericdate14,gap.genericdate15,gap.genericdate16,
gap.genericdate17,gap.genericdate18,gap.genericdate19,gap.genericdate20,
gap.genericboolean1,gap.genericboolean2,gap.genericboolean3,gap.genericboolean4,
gap.genericboolean5,gap.genericboolean6,gap.genericboolean7,gap.genericboolean8,
gap.genericboolean9,gap.genericboolean10,gap.genericboolean11,gap.genericboolean12,
gap.genericboolean13,gap.genericboolean14,gap.genericboolean15,gap.genericboolean16,
gap.genericboolean17,gap.genericboolean18,gap.genericboolean19,gap.genericboolean20,
gap.genericnumber1, ut1.name as unittypeforgenericnumber1,
gap.genericnumber2, ut2.name as unittypeforgenericnumber2,
gap.genericnumber3, ut3.name as unittypeforgenericnumber3,
gap.genericnumber4, ut4.name as unittypeforgenericnumber4,
gap.genericnumber5, ut5.name as unittypeforgenericnumber5,
gap.genericnumber6, ut6.name as unittypeforgenericnumber6,
gap.genericnumber7, ut7.name as unittypeforgenericnumber7,
gap.genericnumber8, ut8.name as unittypeforgenericnumber8,
gap.genericnumber9, ut9.name as unittypeforgenericnumber9,
gap.genericnumber10, ut10.name as unittypeforgenericnumber10,
gap.genericnumber11, ut11.name as unittypeforgenericnumber11,
gap.genericnumber12, ut12.name as unittypeforgenericnumber12,
gap.genericnumber13, ut13.name as unittypeforgenericnumber13,
gap.genericnumber14, ut14.name as unittypeforgenericnumber14,
gap.genericnumber15, ut15.name as unittypeforgenericnumber15,
gap.genericnumber16, ut16.name as unittypeforgenericnumber16,
gap.genericnumber17, ut17.name as unittypeforgenericnumber17,
gap.genericnumber18, ut18.name as unittypeforgenericnumber18,
gap.genericnumber19, ut19.name as unittypeforgenericnumber19,
gap.genericnumber20, ut20.name as unittypeforgenericnumber20
from ext.bkp_20230619_payee pay
join ext.bkp_20230619_participant par on pay.payeeseq = par.payeeseq and pay.effectivestartdate = par.effectivestartdate
left outer join ext.bkp_20230619_gaparticipant gap on pay.payeeseq = gap.payeeseq and pay.effectivestartdate = gap.effectivestartdate
left outer join cs_unittype ut1 on gap.unittypeforgenericnumber1 = ut1.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut2 on gap.unittypeforgenericnumber2 = ut2.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut3 on gap.unittypeforgenericnumber3 = ut3.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut4 on gap.unittypeforgenericnumber4 = ut4.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut5 on gap.unittypeforgenericnumber5 = ut5.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut6 on gap.unittypeforgenericnumber6 = ut6.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut7 on gap.unittypeforgenericnumber7 = ut7.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut8 on gap.unittypeforgenericnumber8 = ut8.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut9 on gap.unittypeforgenericnumber9 = ut9.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut10 on gap.unittypeforgenericnumber10 = ut10.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut11 on gap.unittypeforgenericnumber11 = ut11.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut12 on gap.unittypeforgenericnumber12 = ut12.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut13 on gap.unittypeforgenericnumber13 = ut13.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut14 on gap.unittypeforgenericnumber14 = ut14.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut15 on gap.unittypeforgenericnumber15 = ut15.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut16 on gap.unittypeforgenericnumber16 = ut16.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut17 on gap.unittypeforgenericnumber17 = ut17.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut18 on gap.unittypeforgenericnumber18 = ut18.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut19 on gap.unittypeforgenericnumber19 = ut19.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut20 on gap.unittypeforgenericnumber10 = ut20.unittypeseq and ut1.removedate = '2200-01-01';

-- import
call ext.kyn_lib_pipeline:v_and_t_participant('JC_001', null, ?);

call ext.kyn_lib_pipeline:v_and_t_participant('JC_002', null, ?);

call ext.kyn_lib_pipeline:v_and_t_participant('JC_003', null, ?);

select batchname, stageprocessflag, count(*) 
from cs_stageparticipant 
where batchname like 'JC%'
group by batchname, stageprocessflag
order by batchname;



delete from cs_stageposition where batchname like 'JC%';


-- positions
insert into cs_stageposition
select
pos.tenantid,
(select max(stagepositionseq) from cs_stageposition) + row_number() over (order by pos.ruleelementownerseq, pos.effectivestartdate) as stagepositionseq,
'JC_'||lpad(row_number() over (partition by pos.ruleelementownerseq order by pos.effectivestartdate),3,'0') AS batchname,
pos.name as positionname,
pos.effectivestartdate,
pos.effectiveenddate,
null as positionseq,
null as managerseq,
pay.payeeid,
'Participant'  as payeetype,
null as payeeseq,
null as titleseq,
pln.name as planname,
mgr.name as managername,
ttl.name as titlename,
pg.name as positiongroupname,
pos.targetcompensation,
utt.name as unittypefortargetcompensation,
null as stageprocessdate,
0 as stageprocessflag,
bu.name as businessunitname,
null as businessunitmap,
reo.description,
pos.genericattribute1, pos.genericattribute2, pos.genericattribute3, pos.genericattribute4,
pos.genericattribute5, pos.genericattribute6, pos.genericattribute7, pos.genericattribute8,
pos.genericattribute9, pos.genericattribute10, pos.genericattribute11, pos.genericattribute12,
pos.genericattribute13, pos.genericattribute14, pos.genericattribute15, pos.genericattribute16,
pos.genericnumber1, ut1.name as unittypeforgenericnumber1,
pos.genericnumber2, ut2.name as unittypeforgenericnumber2,
pos.genericnumber3, ut3.name as unittypeforgenericnumber3,
pos.genericnumber4, ut4.name as unittypeforgenericnumber4,
pos.genericnumber5, ut5.name as unittypeforgenericnumber5,
pos.genericnumber6, ut6.name as unittypeforgenericnumber6,
pos.genericdate1, pos.genericdate2, pos.genericdate3, pos.genericdate4, pos.genericdate5, pos.genericdate6,
pos.genericboolean1, pos.genericboolean2, pos.genericboolean3, pos.genericboolean4, pos.genericboolean5, pos.genericboolean6,
pos.creditstartdate, pos.creditenddate, pos.processingstartdate, pos.processingenddate, 
null as stageerrorcode, null as programname
from ext.bkp_20230619_position pos
join ext.bkp_20230619_ruleelementowner reo on 
  pos.ruleelementownerseq = reo.ruleelementownerseq 
  and pos.effectivestartdate = reo.effectivestartdate 
  and reo.removedate = '2200-01-01'
left outer join ext.bkp_20230619_payee pay on 
  pos.payeeseq = pay.payeeseq 
  and pay.effectivestartdate < pos.effectiveenddate 
  and pay.effectiveenddate >= pos.effectiveenddate 
  and pay.removedate = '2200-01-01'
join cs_title ttl on 
  pos.titleseq = ttl.ruleelementownerseq 
  and ttl.effectivestartdate < pos.effectiveenddate 
  and ttl.effectiveenddate >= pos.effectiveenddate 
  and ttl.removedate = '2200-01-01'
left outer join cs_planassignable pas on 
  pos.ruleelementownerseq = pas.ruleelementownerseq 
  and pas.effectivestartdate < pos.effectiveenddate 
  and pas.effectiveenddate >= pos.effectiveenddate 
  and pas.removedate = '2200-01-01'  
left outer join cs_plan pln on 
  pas.planseq = pln.ruleelementownerseq 
  and pln.effectivestartdate < pos.effectiveenddate 
  and pln.effectiveenddate >= pos.effectiveenddate 
  and pln.removedate = '2200-01-01'
left outer join cs_positiongroup pg on pos.positiongroupseq = pg.positiongroupseq and pg.removedate = '2200-01-01'
left outer join cs_businessunit bu on bitand(reo.businessunitmap, bu.mask) > 0
left outer join ext.bkp_20230619_position mgr on
  pos.managerseq = mgr.ruleelementownerseq 
  and mgr.effectivestartdate < pos.effectiveenddate 
  and mgr.effectiveenddate >= pos.effectiveenddate 
  and mgr.removedate = '2200-01-01'
left outer join cs_unittype utt on pos.unittypefortargetcompensation = utt.unittypeseq and utt.removedate = '2200-01-01'
left outer join cs_unittype ut1 on pos.unittypeforgenericnumber1 = ut1.unittypeseq and ut1.removedate = '2200-01-01'
left outer join cs_unittype ut2 on pos.unittypeforgenericnumber2 = ut2.unittypeseq and ut2.removedate = '2200-01-01'
left outer join cs_unittype ut3 on pos.unittypeforgenericnumber3 = ut3.unittypeseq and ut3.removedate = '2200-01-01'
left outer join cs_unittype ut4 on pos.unittypeforgenericnumber4 = ut4.unittypeseq and ut4.removedate = '2200-01-01'
left outer join cs_unittype ut5 on pos.unittypeforgenericnumber5 = ut5.unittypeseq and ut5.removedate = '2200-01-01'
left outer join cs_unittype ut6 on pos.unittypeforgenericnumber6 = ut6.unittypeseq and ut6.removedate = '2200-01-01';


-- we will have issues importing positions for this condition:
select name, count(distinct ruleelementownerseq)
from cs_position 
where removedate = '2200-01-01'
group by name
having count(distinct ruleelementownerseq) > 1;

-- just remove them from the stage table for now
delete from cs_stageposition
where positionname in (
select name
from cs_position 
where removedate = '2200-01-01'
group by name
having count(distinct ruleelementownerseq) > 1
)
and batchname like 'JC%';

select batchname, stageprocessflag, count(*) 
from cs_stageposition 
where batchname like 'JC%'
group by batchname, stageprocessflag
order by batchname;


do begin
declare v_plrseq bigint;
declare cursor c_batch for
  select distinct batchname
  from cs_stageposition
  where stageprocessflag != 3
  and batchname like 'JC%'
  order by batchname;
for x as c_batch
do
  ext.kyn_lib_pipeline:v_and_t_position(:x.batchname, null, v_plrseq);
end for;
end;


-- for some reason we get BU errors due to the title possibly being assigned to multiple BUs --
70432 	The Import Position 5009345_GermanSchmidt_01 has a Business Unit which is not matching with the position. 	Validation stage 	Error 	20/06/2023, 14:11
70432 	The Import Position 5056400_KAZUUMIKAWASHIMA_01 has a Business Unit which is not matching with the position. 	Validation stage 	Error 	20/06/2023, 14:11
70432 	The Import Position 5043746_JATINMESWANI_01 has a Business Unit which is not matching with the position. 	Validation stage 	Error 	20/06/2023, 14:11
70432 	The Import Position 5052828_LINGRAJUSAWKAR_01 has a Business Unit which is not matching with the position. 	Validation stage 	Error 	20/06/2023, 14:11

-- titles with multiple BU
select ttl.name, count(distinct bu.name) as bu_count, ttl.effectivestartdate, ttl.effectiveenddate
from cs_title ttl
join cs_ruleelementowner reo on ttl.ruleelementownerseq = reo.ruleelementownerseq and reo.removedate = '2200-01-01'
and reo.effectivestartdate = ttl.effectivestartdate
join cs_businessunit bu on bitand(reo.businessunitmap, bu.mask) > 0
where ttl.removedate = '2200-01-01'
group by ttl.name, ttl.effectivestartdate, ttl.effectiveenddate
having count(distinct bu.name)>1;

-- check the stage records
select *
FROM cs_stageposition 
where titlename in (
select ttl.name
from cs_title ttl
join cs_ruleelementowner reo on ttl.ruleelementownerseq = reo.ruleelementownerseq and reo.removedate = '2200-01-01'
and reo.effectivestartdate = ttl.effectivestartdate
join cs_businessunit bu on bitand(reo.businessunitmap, bu.mask) > 0
where ttl.removedate = '2200-01-01'
group by ttl.name, ttl.effectivestartdate, ttl.effectiveenddate
having count(distinct bu.name)>1
)
and batchname like 'JC%';


