CREATE VIEW "EXT"."STEL_LTDIMENSIONINDICES" ( "DIMENSIONSLOT", "DIMNAME", "TBLNAME", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE", "MINSTRING", "MINVALUE", "MAXVALUE" ) AS (SELECT   /* ORIGSQL: SELECT dimensionslot, md.name dimName, tbl.name tblname, md.effectivestartdate, (...) */
        dimensionslot,
        md.name AS dimName,
        tbl.name AS tblname,
        md.effectivestartdate,
        md.effectiveenddate,
        di.minstring,
        di.minvalue,
        di.maxvalue
    FROM
        cs_mdltdimension md
    INNER JOIN
        cs_relationalmdlt tbl
        ON tbl.ruleelementseq = md.ruleelementseq
        AND TO_DATE(ADD_SECONDS(tbl.effectiveenddate,(86400*-1))) BETWEEN md.effectivestartdate AND TO_DATE(ADD_SECONDS(md.effectiveenddate  /* ORIGSQL: tbl.effectiveenddate - 1 */
                ,(86400*-1)))   /* ORIGSQL: md.effectiveenddate - 1 */
        AND tbl.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    INNER JOIN
        cs_mdltindex di
        ON di.ruleelementseq = md.ruleelementseq
        AND di.dimensionseq = md.dimensionseq
        AND di.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        AND TO_DATE(ADD_SECONDS(di.effectiveenddate,(86400*-1))) BETWEEN md.effectivestartdate AND TO_DATE(ADD_SECONDS(md.effectiveenddate,(86400*(-1  /* ORIGSQL: di.effectiveenddate - 1 */
        ))))   /* ORIGSQL: md.effectiveenddate - 1 */
    WHERE
        md.removedate > CURRENT_TIMESTAMP) WITH READ ONLY