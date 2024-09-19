CREATE VIEW "EXT"."STEL_WORKINGDAYS" ( "CHANNEL", "CALDATE", "TABLENAME", "WORKINGDAYSINENDMONTH", "PERIODTYPENAME", "PERIODNAME", "PERIODSEQ" ) AS (SELECT   
        Calmonth.channel,
        caldate,
        tablename,
        MAX(ExpectedWorkDays) - COUNT(DISTINCT x.nonworkdate) AS WorkingdaysInEndMonth,
        periodtypename,
        periodname,
        periodseq
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_TEMP_PERIODHIERARCHY' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_TEMP_LOOKUP' not found */
    FROM
        (
            SELECT   /* ORIGSQL: (select distinct prd.startdate caldate, prd.enddate calenddate, lt.dim0 as chann(...) */
            DISTINCT
            prd.startdate AS caldate,
            prd.enddate AS calenddate,
            lt.dim0 AS channel,
            COUNT(*) AS ExpectedWorkDays,
            lt.name AS Tablename,
            prd.periodtypename,
            prd.periodname,
            prd.periodseq
        FROM
            (
                /* RESOLVE: Review semantics in source vs. target DBMS: LIMIT/OFFSET without ORDER BY: consistent results not guaranteed */
                SELECT   /* ORIGSQL: (Select ROW_NUMBER() OVER (ORDER BY 0*0) rn from all_objects LIMIT 31) rownum ro(...) */
                ROW_NUMBER() OVER () AS rn  
            FROM
                SYS.OBJECTS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_OBJECTS': verify conversion */
                             /* ORIGSQL: all_objects (Oracle catalog) */
            LIMIT 31  /* ORIGSQL: rownum <=31 */
        ) AS rn
    INNER JOIN
        EXT.STEL_temp_PERIODHIERARCHY PRD
        ON add_days(prd.monthstartdate,rn-1) BETWEEN prd.monthstartdate AND add_days(prd.monthenddate,-1)
        AND PRD.calendarname = 'Singtel Monthly Calendar'
    INNER JOIN
        EXT.STEL_temp_lookup LT
        ON lt.value = 1
        AND lt.name = 'LT_Working_Days_Channel'
        AND TO_VARCHAR(add_days(prd.monthstartdate,rn-1)) = lt.dim1  /* ORIGSQL: to_Char(prd.monthstartdate+rn-1,'D') */
    GROUP BY
        prd.startdate, prd.enddate, lt.dim0
        , lt.name, prd.periodtypename, prd.periodname, prd.periodseq
        --and prd.periodtypename = 'month'
    ) AS CalMonth
LEFT OUTER JOIN
    EXT.STEL_NONWORKINGDAYS x
    ON x.channel = CalMonth.Channel
    AND x.nonworkdate BETWEEN caldate AND TO_DATE(ADD_SECONDS(calenddate,(86400*(-1
    ))))   
GROUP BY
    Calmonth.channel,caldate, calenddate, tablename, periodtypename, periodname, periodseq) WITH READ ONLY