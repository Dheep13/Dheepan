CREATE VIEW "EXT"."STEL_DATA_ROADSHOW" ( "GENERICATTRIBUTE3", "RSCODE", "RSSTARTDATE", "RSENDDATE", "COUNTERS" ) AS (select   genericattribute3, genericattribute4 as RSCode
  , effectivestartdate as rsstartdate, effectiveenddate as rsenddate, max(a.genericnumber1) counters
from stel_Classifier a
where categorytreename like 'Roadshow Codes'
group by genericattribute3, genericattribute4 
  , effectivestartdate  , effectiveenddate) WITH READ ONLY