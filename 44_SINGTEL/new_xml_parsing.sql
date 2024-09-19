SELECT 
    "valueunittype",
    "value",
    "mapkey",
    "mapvalue",
    "start_inclusive", 
    "end_inclusive", 
    "open_started",
    "start_value",
    "start_unit_type_seq",
    "operator",
    "end_value",
    "end_unit_type_seq"
FROM 
    TCMP.CS_RATETABLE,
    XMLTABLE(
        XMLNAMESPACE('http://www.w3.org/2001/XMLSchema-instance' AS 'xsi'),
        '/serialized-container-impl/expression-object/map-value/value-range' PASSING TCMP.CS_RATETABLE.EXPRESSION
        COLUMNS 
            "valueunittype" BIGINT PATH '../unit-type-seq',
            "value" NVARCHAR(255) PATH '../value',
            "mapkey" NVARCHAR(255) PATH '../../map-key',
            "mapvalue" NVARCHAR(255) PATH '@xsi:type',
            "start_inclusive" NVARCHAR(5) PATH '@start-inclusive',
            "end_inclusive" NVARCHAR(5) PATH '@end-inclusive',
            "open_started" NVARCHAR(5) PATH '@open-started',
            "start_value" NVARCHAR(255) PATH 'start/value',
            "start_unit_type_seq" BIGINT PATH 'start/unit-type-seq',
            "operator" NVARCHAR(255) PATH 'operator',
            "end_value" NVARCHAR(255) PATH 'end/value',
            "end_unit_type_seq" BIGINT PATH 'end/unit-type-seq'
    ) AS XT;
