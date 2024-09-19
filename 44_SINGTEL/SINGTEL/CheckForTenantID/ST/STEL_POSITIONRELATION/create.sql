CREATE VIEW "EXT"."STEL_POSITIONRELATION" ( "RELNNAME", "PARENTPOSITIONNAME", "CHILDPOSITIONNAME", "RELNSTARTDATE", "RELNENDDATE", "PARENTSTARTDATE", "PARENTENDDATE", "CHILDSTARTDATE", "CHILDENDDATE", "PARENTTITLE", "CHILDTITLE" ) AS (SELECT   /* ORIGSQL: SELECT prt.name RelnName, par.name PArentPositionName, ch.name ChildPositionName(...) */
        prt.name AS RelnName,
        par.name AS PArentPositionName,
        ch.name AS ChildPositionName,
        pr.effectivestartdate AS RelnStartDate,
        pr.effectiveenddate AS RelnEndDate,
        par.effectivestartdate AS ParentStartDate,
        par.effectiveenddate AS ParentEndDate,
        ch.effectivestartdate AS ChildStartDate,
        ch.effectiveenddate AS ChildEndDate,
        tpar.name AS ParentTitle,
        tch.name AS ChildTitle
    FROM
        cs_positionrelation pr
    INNER JOIN
        cs_positionrelationtype prt
        ON pr.positionrelationtypeseq = prt.datatypeseq
    INNER JOIN
        cs_position par
        ON par.ruleelementownerseq = pr.parentpositionseq
    INNER JOIN
        cs_title tpar
        ON tpar.ruleelementownerseq = par.titleseq
    INNER JOIN
        cs_position ch
        ON ch.ruleelementownerseq = pr.childpositionseq
        AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1))) BETWEEN tpar.effectivestartdate AND TO_DATE(ADD_SECONDS(tpar.effectiveenddate  /* ORIGSQL: par.effectiveenddate - 1 */
                ,(86400*-1)))   /* ORIGSQL: tpar.effectiveenddate - 1 */
    INNER JOIN
        cs_title tch
        ON tch.ruleelementownerseq = ch.titleseq
        AND TO_DATE(ADD_SECONDS(ch.effectiveenddate,(86400*-1))) BETWEEN tch.effectivestartdate AND TO_DATE(ADD_SECONDS(tch.effectiveenddate,(86400*(-1  /* ORIGSQL: ch.effectiveenddate - 1 */
        ))))   /* ORIGSQL: tch.effectiveenddate - 1 */
    WHERE
        TO_DATE(ADD_SECONDS(pr.effectiveenddate,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: pr.effectiveenddate - 1 */
                                                                                                                                                        /* ORIGSQL: par.effectiveenddate - 1 */
        AND TO_DATE(ADD_SECONDS(pr.effectiveenddate,(86400*-1))) BETWEEN ch.effectivestartdate AND TO_DATE(ADD_SECONDS(ch.effectiveenddate,(86400*-1)))   /* ORIGSQL: pr.effectiveenddate - 1 */
                                                                                                                                                          /* ORIGSQL: ch.effectiveenddate - 1 */
        AND ch.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND par.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pr.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND prt.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND tpar.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND tch.removedate > CURRENT_TIMESTAMP) WITH READ ONLY