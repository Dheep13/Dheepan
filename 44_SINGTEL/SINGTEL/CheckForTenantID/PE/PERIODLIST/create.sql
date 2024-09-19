CREATE VIEW "EXT"."PERIODLIST" ( "NAME", "STARTDATE" ) AS (SELECT   /* ORIGSQL: SELECT pd2.name, pd2.startdate FROM cs_period pd JOIN (SELECT MAX(periodseq) AS (...) */
        pd2.name,
        pd2.startdate
    FROM
        cs_period pd
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PIPELINERUN' not found */
    INNER JOIN
        (
            SELECT   /* ORIGSQL: (SELECT MAX(periodseq) lastp FROM cs_pipelinerun WHERE description LIKE '%stage=(...) */
            MAX(periodseq) AS lastp
        FROM
            cs_pipelinerun
        WHERE
            description LIKE '%stage=%Pay%'
    ) AS lastperiod
    ON pd.periodseq = lastperiod.lastp
INNER JOIN
    cs_period pd2
    ON pd2.periodtypeseq = pd.periodtypeseq
    AND pd2.calendarseq = pd.calendarseq
    AND pd.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
WHERE
    pd.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    AND pd2.startdate <= pd.startdate) WITH READ ONLY