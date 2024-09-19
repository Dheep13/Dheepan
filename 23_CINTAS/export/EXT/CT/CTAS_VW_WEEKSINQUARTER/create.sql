CREATE VIEW "EXT"."CTAS_VW_WEEKSINQUARTER" ( "QUATERNAME", "STARTDATE", "ENDDATE", "TOTALWEEKS" ) AS select qtr.name quatername,qtr.startdate, qtr.enddate, count(wk.periodseq) as totalweeks from cs_period mon ,cs_period wk,
	(select distinct periodseq, name , startdate, enddate from cs_period where periodtypeseq=(select distinct periodtypeseq
from cs_periodtype where name ='quarter' and removedate='2200-01-01'
and calendarseq=(select calendarseq from cs_calendar where name='Cintas Hybrid Calendar' and removedate='2200-01-01')
	)
and removedate='2200-01-01') qtr
where wk.parentseq=mon.periodseq
and mon.parentseq=qtr.periodseq

and wk.removedate ='2200-01-01'
and mon.removedate ='2200-01-01'
group by qtr.name,qtr.startdate, qtr.enddate WITH READ ONLY