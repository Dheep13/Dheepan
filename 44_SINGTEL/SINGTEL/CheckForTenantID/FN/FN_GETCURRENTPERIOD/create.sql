CREATE FUNCTION EXT.FN_GETCURRENTPERIOD
()
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: RETURN NUMBER */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_ret DECIMAL(38,10);  /* ORIGSQL: v_ret number; */
    DECLARE v_lastpost DECIMAL(38,10);  /* ORIGSQL: v_lastpost number; */

    --return 2533274790396269; --hardocded tempoaraily for testing
    --return 2533274790396199; --hardocded tempoaraily for testing 
    dbmtk_function_result = 2533274790396267;
    RETURN;
    --apr 2017 for testing
    SELECT
        MAX(pr.periodseq)
    INTO
        v_lastpost
    FROM
        cs_pipelinerun pr
        /* RESOLVE: Oracle Database link: Remote table/view 'EXT.cs_pipelinerun@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_pipelinerun_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
        command = 'PipelineRun'
        AND description LIKE '%Pay%';

    SELECT
        MIN(periodseq)
    INTO
        v_ret
    FROM
        (
            SELECT   /* ORIGSQL: (select pd.periodseq from cs_period@STELEXT pd join cs_period@STELEXT pdprev on (...) */
                pd.periodseq
            FROM
                cs_period pd
            INNER JOIN
                cs_period pdprev
                ON pdprev.periodseq = :v_lastpost
                AND pdprev.periodtypeseq = pd.periodtypeseq
                AND pdprev.calendarseq = pd.calendarseq
                AND pdprev.enddate = pd.startdate
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_period@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_period_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_period@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_period_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                pdprev.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND pd.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        UNION
            SELECT   /* ORIGSQL: select distinct pd.periodseq from cs_period@STELEXT pd join cs_calendar@STELEXT (...) */
                DISTINCT
                pd.periodseq
            FROM
                cs_period pd
            INNER JOIN
                cs_calendar ca
                ON pd.calendarseq = ca.calendarseq
                AND ca.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND ca.name = 'Singtel Monthly Calendar'
            INNER JOIN
                cs_periodtype pt
                ON pt.periodtypeseq = pd.periodtypeseq
                AND pt.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND pt.name = 'month'
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_periodtype@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_periodtype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_period@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_period_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_calendar@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_calendar_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                ADD_MONTHS(CURRENT_TIMESTAMP,0) BETWEEN pd.startdate AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)))   /* ORIGSQL: sysdate */
                                                                                                                       /* ORIGSQL: pd.enddate-1 */
        ) AS a;

    --set to 2015 for SIT only 
    dbmtk_function_result = :v_ret;
    RETURN;
END