CREATE VIEW "EXT"."TEMP_RT1" ( "mapkey", "mapvalue", "value", "valueunittype", "start_inclusive", "end_inclusive", "open_started" ) AS (SELECT "valueunittype",
"value" ,
"mapkey",
"mapvalue",
"start_inclusive", 
"end_inclusive", 
"open_started" 
-- "value_range"
FROM TCMP.CS_RATETABLE,
XMLTABLE(
  XMLNAMESPACE('http://www.w3.org/2001/XMLSchema-instance' AS 'xsi'),
  '/serialized-container-impl/expression-object' PASSING TCMP.CS_RATETABLE.EXPRESSION
  COLUMNS 
    "valueunittype" BIGINT PATH './map-value/unit-type-seq',
    "value" NVARCHAR(100) PATH './map-value/value',
    "mapkey" NVARCHAR(100) PATH './map-key',
    -- "map-value" NVARCHAR(100) PATH './map-value'
    "mapvalue" NVARCHAR(100) PATH './map-value/@xsi:type',
    "start_inclusive" NVARCHAR(5) PATH './map-value/value-range/@start-inclusive',
    "end_inclusive" NVARCHAR(5) PATH './map-value/value-range/@end-inclusive',
    "open_started" NVARCHAR(5) PATH './map-value/value-range/@open-started'
    -- "value_range" CLOB(50) PATH './map-value/value-range'
) AS XT) WITH READ ONLY