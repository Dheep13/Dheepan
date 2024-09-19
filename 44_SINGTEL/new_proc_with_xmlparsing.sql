

CREATE OR REPLACE PROCEDURE EXT.STEL_RATETABLERANGE
(
    IN p_tablename VARCHAR(75), 
    IN IN_PERIODSEQ BIGINT 
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
 
    DECLARE v_StartDate TIMESTAMP;  
    DECLARE v_EndDate TIMESTAMP;  
    DECLARE v_eot TIMESTAMP = TO_DATE('22000101','YYYYMMDD');  

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT
        startdate,
        enddate
    INTO
        v_StartDate,
        v_EndDate
    FROM
        cs_period
    WHERE
        periodseq = :IN_PERIODSEQ
        AND removedate = :v_eot;


        INSERT INTO
        EXT.STEL_XMLDATA (
            SELECT
                EXPRESSION
            FROM
                TCMP.CS_RATETABLE
            where
                name = :p_tablename
                and removedate >:v_eot
                AND effectiveenddate >= :v_StartDate
                AND effectivestartdate BETWEEN :v_StartDate AND :v_EndDate
                AND islast = 1
        );

EXECUTE IMMEDIATE 'TRUNCATE TABLE STEL_RATETABLE';


INSERT INTO EXT.STEL_RATETABLE
(WITH XT AS (
    SELECT 
        ROW_NUMBER() OVER () AS rownum,
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
        XMLTABLE(
            XMLNAMESPACE('http://www.w3.org/2001/XMLSchema-instance' AS 'xsi'),
            '/serialized-container-impl/expression-object/map-value/value-range' PASSING EXT.STEL_XMLDATA.XML
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
        )
),
CT AS (
    SELECT 
        ROW_NUMBER() OVER () AS rownum,
        "currency_unit_type_seq",
        "currency_value"
    FROM 
        XMLTABLE(
            XMLNAMESPACE('http://www.w3.org/2001/XMLSchema-instance' AS 'xsi'),
            '/serialized-container-impl/expression-object/map-value/currency' PASSING EXT.STEL_XMLDATA.XML
            COLUMNS
                "currency_unit_type_seq" BIGINT PATH 'unit-type-seq',
                "currency_value" NVARCHAR(255) PATH 'value'
        )
)
SELECT 
    :p_tablename,
    XT.rownum,
    XT."start_value",
    XT."end_value",
    CT."currency_value",
    XT."operator"
FROM XT
INNER JOIN CT ON XT.rownum = CT.rownum);

    COMMIT;
END;


