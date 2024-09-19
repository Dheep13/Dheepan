CREATE PROCEDURE EXT.STEL_RATETABLERANGE_TVCONTENT
(
    IN p_tablename VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                  /* ORIGSQL: p_tablename varchar2 */
    IN IN_PERIODSEQ BIGINT     /* ORIGSQL: IN_PERIODSEQ IN INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_xml VARCHAR(4000);  /* ORIGSQL: v_xml varchar2(4000); */
    DECLARE v_StartDate TIMESTAMP;  /* ORIGSQL: v_StartDate date; */
    DECLARE v_EndDate TIMESTAMP;  /* ORIGSQL: v_EndDate date; */
    DECLARE v_eot TIMESTAMP = TO_DATE('22000101','YYYYMMDD');  /* ORIGSQL: v_eot date:=TO_DATE('22000101','YYYYMMDD') ; */

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

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_RATETABLE' not found */

    -- SELECT
    --     expression
    -- INTO
    --     v_xml
    -- FROM
    --     cs_ratetable
    -- WHERE
    --     removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
    --     AND effectivestartdate < TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_enddate-1 */
    --     AND effectiveenddate >= :v_StartDate
    --     AND effectivestartdate BETWEEN :v_StartDate AND :v_EndDate
    --     AND islast = 1
    --     AND name = :p_tablename;



        INSERT INTO
        EXT.STEL_XMLDATA (
            SELECT
                EXPRESSION
            FROM
                TCMP.CS_RATETABLE
            where
                name = :p_tablename
                and removedate >CURRENT_TIMESTAMP
                AND effectiveenddate >= :v_StartDate
                AND effectivestartdate BETWEEN :v_StartDate AND :v_EndDate
                AND islast = 1
        );
        /* ORIGSQL: dbms_output.put_line(v_xml); */
        -- CALL SQLSCRIPT_PRINT :PRINT_LINE(:v_xml);

    /* ORIGSQL: execute immediate 'truncate table STEL_RATETABLE'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RATETABLE' not found */

    /* ORIGSQL: truncate table STEL_RATETABLE ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STEL_RATETABLE';

    /* ORIGSQL: insert into STEL_RATETABLE select p_tablename, xmlt3.rownumID, xmlt3.rangestart,(...) */
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
    -- XT."valueunittype",
    -- XT."value",
    -- XT."mapkey",
    -- XT."mapvalue",
    -- XT."start_inclusive", 
    -- XT."end_inclusive", 
    -- XT."open_started",
    -- XT."start_value",
    -- XT."start_unit_type_seq",
    -- XT."operator",
    -- XT."end_value",
    -- XT."end_unit_type_seq",
    -- CT."currency_unit_type_seq",
    -- CT."currency_value"
FROM XT
INNER JOIN CT ON XT.rownum = CT.rownum);


        -- SELECT   /* ORIGSQL: select p_tablename, xmlt3.rownumID, xmlt3.rangestart, xmlt3.rangeend, xmlt2.rate(...) */
        --     :p_tablename,
        --     xmlt3.rownumID,
        --     xmlt3.rangestart,
        --     xmlt3.rangeend,
        --     xmlt2.rate,
        --     xmlt3.operate
        -- FROM

        --     (
        --         SELECT   /* ORIGSQL: (Select ROW_NUMBER() OVER (ORDER BY 0*0) rownumID, xmlt.* FROM XMLTABLE('/serial(...) */
        --             ROW_NUMBER()OVER (ORDER BY 0*0) AS rownumID,  
        --             xmlt.*
        --         FROM
        --             XMLTABLE('/serialized-container-impl/expression-object/map-value/value-range' 
        --                 PASSING xmltype(:v_xml)
        --                 COLUMNS
        --                 /* --describe columns and path to them:  */

        --                 rangeStart varchar2(20) PATH './start/value',
        --                 rangeEnd varchar2(20) PATH './end/value',
        --                 operate varchar2(20) PATH './operator'
        --             ) xmlt
        --     ) AS xmlt3
        -- INNER JOIN
        --     (
        --         SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (ORDER BY 0*0) rownumID, xmlt.* FROM XMLTABLE('/serial(...) */
        --             ROW_NUMBER() OVER (ORDER BY 0*0) AS rownumID,  
        --             xmlt.*
        --         FROM
        --             XMLTABLE('/serialized-container-impl/expression-object/map-value/currency' 
        --                 PASSING xmltype(
        --                     '<?xml version="1.0" encoding="UTF-8"?>
        --                 <serialized-container-impl><expression-object><map-key xsi:type="java:java.lang.String" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">selectorUnitType</map-key><map-value xsi:type="quantity" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><unit-type-seq>1970324836974600</unit-type-seq><value>0.00</value></map-value></expression-object><expression-object><map-key xsi:type="java:java.lang.String" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">resultUnitType</map-key><map-value xsi:type="currency" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><unit-type-seq>1970324836974601</unit-type-seq><value>0.00</value></map-value></expression-object><expression-object><map-key xsi:type="java:java.lang.String" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">selectors</map-key><map-value xsi:type="java:java.util.ArrayList" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><value-range start-inclusive="true" end-inclusive="true" open-started="true" open-ended="false" xsi:type="java:com.callidus.ratetable.ValueRange"><end xsi:type="quantity"><unit-type-seq>1970324836974600</unit-type-seq><value>3.00</value></end><operator>lt;=</operator><start xsi:type="quantity"><unit-type-seq>1970324836974600</unit-type-seq><value>0.00</value></start></value-range><value-range start-inclusive="false" end-inclusive="true" open-started="false" open-ended="false" xsi:type="java:com.callidus.ratetable.ValueRange"><end xsi:type="quantity"><unit-type-seq>1970324836974600</unit-type-seq><value>8.00</value></end><operator>lt;=</operator><start xsi:type="quantity"><unit-type-seq>1970324836974600</unit-type-seq><value>3.00</value></start></value-range><value-range start-inclusive="false" end-inclusive="true" open-started="false" open-ended="true" xsi:type="java:com.callidus.ratetable.ValueRange"><end xsi:type="quantity"><unit-type-seq>1970324836974600</unit-type-seq><value>0.00</value></end><operator>gt;</operator><start xsi:type="quantity"><unit-type-seq>1970324836974600</unit-type-seq><value>8.00</value></start></value-range></map-value></expression-object><expression-object><map-key xsi:type="java:java.lang.String" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">results</map-key><map-value xsi:type="java:java.util.ArrayList" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><currency xsi:type="currency"><unit-type-seq>1970324836974601</unit-type-seq><value>0.00</value></currency><currency xsi:type="currency"><unit-type-seq>1970324836974601</unit-type-seq><value>9.00</value></currency><currency xsi:type="currency"><unit-type-seq>1970324836974601</unit-type-seq><value>10.00</value></currency></map-value></expression-object></serialized-container-impl>')
        --                 COLUMNS
        --                 /* --describe columns and path to them:  */

        --                 rate varchar2(20) PATH './value'
        --             ) xmlt
        --     ) AS xmlt2
        --     ON xmlt2.rownumid = xmlt3.rownumid;

    /* ORIGSQL: commit; */
    COMMIT;
END