CREATE FUNCTION EXT.STEL_GET_TOTAL_WORKINGDAYS
(
    IN IN_PERIODSEQ BIGINT,   /* ORIGSQL: IN_PERIODSEQ INTEGER */
    IN IN_BIZLINE VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                /* ORIGSQL: IN_BIZLINE VARCHAR2 */
)
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: RETURN NUMBER */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_lkpName VARCHAR(255) = 'LT_Working_Days';  /* ORIGSQL: v_lkpName VARCHAR2(255) := 'LT_Working_Days'; */
    DECLARE v_eot TIMESTAMP = to_date('01/01/2200', 'mm/dd/yyyy');  /* ORIGSQL: v_eot DATE := to_date('01/01/2200', 'mm/dd/yyyy') ; */

    DECLARE v_year VARCHAR(10);  /* ORIGSQL: v_year VARCHAR2(10); */
    DECLARE v_mthLapsed INT;  /* ORIGSQL: v_mthLapsed NUMBER(5); */
    DECLARE v_TotalDays BIGINT = 0;  /* ORIGSQL: v_TotalDays NUMBER(10) := 0; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    SELECT
        yr.name,
        MONTHS_BETWEEN(m.enddate,yr.startdate)
    INTO
        v_year,
        v_mthLapsed
    FROM
        cs_period m,
        cs_period yr
    WHERE
        m.periodseq = IN_PERIODSEQ
        AND m.removedate = v_eot
        AND yr.removedate = v_eot
        AND yr.startdate < m.enddate
        AND yr.enddate > m.startdate
        AND m.calendarseq = yr.calendarseq
        AND yr.periodtypeseq
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODTYPE' not found */
        =
        (
            SELECT   /* ORIGSQL: (SELECT periodtypeseq FROM cs_periodtype WHERE name = 'year' AND removedate = v_(...) */
                periodtypeseq
            FROM
                cs_periodtype
            WHERE
                name = 'year'
                AND removedate = :v_eot
        );

    SELECT
        SUM(value) 
    INTO
        v_TotalDays
    FROM
        stel_lookup
    WHERE
        name = :v_lkpName
        AND dim0 = :IN_BIZLINE
        AND dim1 = :v_year
        AND TO_DECIMAL(dim2,38,18) <= :v_mthLapsed;  /* ORIGSQL: to_number(dim2) */

    dbmtk_function_result = :v_TotalDays;
    RETURN;
END