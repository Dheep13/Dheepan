CREATE VIEW "EXT"."CTAS_VW_WEEKSINMONTH" ( "MONTHNAME", "STARTDATE", "ENDDATE", "TOTALWEEKS" ) AS select parent.name as MonthName, parent.startdate, parent.enddate, count(1) as totalweeks
from cs_period wk ,(
select distinct periodseq, startdate, enddate, name from cs_period where periodtypeseq=(select distinct periodtypeseq
from cs_periodtype where name ='month' and removedate='2200-01-01')
and removedate='2200-01-01'
and calendarseq=(select calendarseq from cs_calendar where name='Cintas Hybrid Calendar' and removedate='2200-01-01')) parent
where parent.periodseq= wk.parentseq
and wk.removedate ='2200-01-01'
group by parent.name,parent.startdate, parent.enddate WITH READ ONLY