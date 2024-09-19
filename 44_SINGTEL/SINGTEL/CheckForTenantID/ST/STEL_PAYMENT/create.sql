CREATE VIEW "EXT"."STEL_PAYMENT" ( "POSITIONNAME", "PAYEENAME", "TITLE", "PERIODNAME", "EARNINGCODEID", "EARNINGGROUPID", "AMOUNT", "TRIALPIPELINERUNDATE", "POSTPIPELINERUNDATE", "CHANNEL", "FIRSTDAYOFPERIOD", "FIRSTDAYOFCALMONTH", "WAGETYPECODE", "WAGETYPEDESCRIPTION" ) AS (SELECT   /* ORIGSQL: SELECT pos.name positionname, par.lastname AS payeename, t.name AS title, pd.nam(...) */
        pos.name AS positionname,
        par.lastname AS payeename,
        t.name AS title,
        pd.name AS periodname,
        p.earningcodeid,
        p.earninggroupid,
        p.VALUE AS amount,
        trialpipelinerundate,
        postpipelinerundate,
        pos.genericattribute3 AS Channel,
        pd.startdate AS FirstDayOFPeriod,
        ext.trunc(current_date,'MONTH') AS FirstDayOfCalMonth,  /* ORIGSQL: TRUNC(SYSDATE, 'MM') */
        IFNULL(wt.genericattribute1, 'Code not found') AS WageTypeCode,  /* ORIGSQL: NVL(wt.genericattribute1, 'Code not found') */
        IFNULL(wt.genericattribute3, p.earningcodeid) AS WageTypeDescription  /* ORIGSQL: NVL(wt.genericattribute3, p.earningcodeid) */
    FROM
        cs_payment p
    INNER JOIN
        cS_period pd
        ON p.periodseq = pd.periodseq
    INNER JOIN
        cs_position pos
        ON p.positionseq = pos.ruleelementownerseq
        AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN pos.effectivestartdate AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1)))   /* ORIGSQL: pos.effectiveenddate - 1 */
                                                                                                                                                   /* ORIGSQL: pd.enddate - 1 */
    INNER JOIN
        cS_title t
        ON pos.titleseq = t.ruleelementownerseq
        AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN t.effectivestartdate AND TO_DATE(ADD_SECONDS(t.effectiveenddate,(86400*-1)))   /* ORIGSQL: t.effectiveenddate - 1 */
                                                                                                                                               /* ORIGSQL: pd.enddate - 1 */
    INNER JOIN
        cs_participant par
        ON p.payeeseq = par.payeeseq
        AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: pd.enddate - 1 */
                                                                                                                                                   /* ORIGSQL: par.effectiveenddate - 1 */
    LEFT OUTER JOIN
        (
            SELECT   /* ORIGSQL: (SELECT * FROM stel_Classifier WHERE categorytreename = 'Payment Output Earning (...) */
            *
        FROM
            stel_Classifier
        WHERE
            categorytreename = 'Payment Output Earning Code'
    ) AS wt
    ON wt.classifierid = p.earningcodeid
WHERE
    pd.removedate = TO_DATE('22000101', 'YYYYMMDD')
    AND pos.removedate = TO_DATE('22000101', 'YYYYMMDD')
    AND par.removedate = TO_DATE('22000101', 'YYYYMMDD')
    AND t.removedate = TO_DATE('22000101', 'YYYYMMDD')) WITH READ ONLY