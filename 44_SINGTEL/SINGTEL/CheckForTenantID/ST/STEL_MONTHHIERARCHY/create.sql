CREATE VIEW "EXT"."STEL_MONTHHIERARCHY" ( "CALENDARNAME", "CALENDARSEQ", "PERIODTYPELEVEL", "PERIODTYPENAME", "PERIODNAME", "PERIODSEQ", "STARTDATE", "ENDDATE", "MONTHPERIODSEQ1", "MONTHNAME1", "MONTHSTARTDATE1", "MONTHENDDATE1", "MONTHPERIODSEQTD", "MONTHNAMETD", "MONTHSTARTDATETD", "MONTHENDDATETD" ) AS (SELECT   /* ORIGSQL: SELECT cal.name CalendarName, cal.calendarseq, pt.periodtypelevel, pt.name PErio(...) */
        cal.name AS CalendarName,
        cal.calendarseq,
        pt.periodtypelevel,
        pt.name AS PEriodTypeName,
        pd.name AS PEriodName,
        pd.periodseq,
        pd.startdate,
        pd.enddate,
        pd2.periodseq AS MonthPeriodSeq1,
        pd2.name AS MonthName1,
        pd2.startdate AS MonthStartDate1,
        pd2.enddate AS MonthEndDate1,
        pd3.periodseq AS MonthPeriodSeqTD,
        pd3.name AS MonthNameTD,
        pd3.startdate AS MonthStartDateTD,
        pd3.enddate AS MonthEndDateTD
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
        cs_period pd3
        ON TO_DATE(ADD_SECONDS(pd3.enddate,(86400*-1))) BETWEEN pd.startdate AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)))   /* ORIGSQL: pd3.enddate - 1 */
                                                                                                                               /* ORIGSQL: pd.enddate - 1 */
        AND pd3.enddate <= pd2.enddate
        AND pd.calendarseq = pd3.calendarseq
    INNER JOIN
        cs_periodtype pt3
        ON pd3.periodtypeseq = pt3.periodtypeseq
        AND pt3.name = 'month'
    INNER JOIN
        cs_calendar cal
        ON cal.calendarseq = pd.calendarseq
    WHERE
        pd.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pd2.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pt.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pt2.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pt.periodtypelevel >= 2
        AND cal.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND cal.name LIKE 'Singtel%') WITH READ ONLY