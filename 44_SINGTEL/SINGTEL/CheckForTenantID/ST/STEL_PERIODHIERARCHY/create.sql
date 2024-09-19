CREATE VIEW "EXT"."STEL_PERIODHIERARCHY" ( "CALENDARNAME", "PERIODTYPELEVEL", "PERIODTYPENAME", "PERIODNAME", "PERIODSEQ", "STARTDATE", "ENDDATE", "MONTHPERIODSEQ", "MONTHNAME", "MONTHSTARTDATE", "MONTHENDDATE" ) AS (SELECT   /* ORIGSQL: SELECT cal.name CalendarName, pt.periodtypelevel, pt.name PEriodTypeName, pd.nam(...) */
        cal.name AS CalendarName,
        pt.periodtypelevel,
        pt.name AS PEriodTypeName,
        pd.name AS PEriodName,
        pd.periodseq,
        pd.startdate,
        pd.enddate,
        pd2.periodseq AS MonthPeriodSeq,
        pd2.name AS MonthName,
        pd2.startdate AS MonthStartDate,
        pd2.enddate AS MonthEndDate
        /* --, to_char(pd2.startdate,'YYYY'), to_char(pd2.startdate,'MM') */
    FROM
        cs_period pd
    INNER JOIN
        cs_periodtype pt
        ON pd.periodtypeseq = pt.periodtypeseq
    INNER JOIN
        cs_period pd2
        ON TO_DATE(ADD_SECONDS(pd2.enddate,(86400*-1))) BETWEEN pd.startdate AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)))   /* ORIGSQL: pd2.enddate - 1 */
                                                                                                                               /* ORIGSQL: pd.enddate - 1 */
        AND pd.calendarseq = pd2.calendarseq
    INNER JOIN
        cs_periodtype pt2
        ON pd2.periodtypeseq = pt2.periodtypeseq
        AND pt2.name = 'month'
    INNER JOIN
        cs_calendar cal
        ON cal.calendarseq = pd.calendarseq
    WHERE
        pd.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pd2.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pt.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pt2.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pt.periodtypelevel >= 2
        AND cal.removedate > CURRENT_TIMESTAMP) WITH READ ONLY