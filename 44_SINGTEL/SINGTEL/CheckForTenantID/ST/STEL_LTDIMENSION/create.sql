CREATE VIEW "EXT"."STEL_LTDIMENSION" ( "TBLNAME", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE", "DIM0", "DIM1", "DIM2", "DIM3", "DIM4", "DIM5", "DIM6", "DIM7", "DIM8", "DIM9", "DIM10", "DIM11", "DIM12", "DIM13", "DIM14", "DIM15" ) AS (SELECT   /* ORIGSQL: SELECT TBLNAME, EFFECTIVESTARTDATE, EFFECTIVEENDDATE, "0", "1", "2", "3", "4", "(...) */
        TBLNAME,
        EFFECTIVESTARTDATE,
        EFFECTIVEENDDATE,
        max(case when dimensionslot=0 then dimName else null end) dim0,
        max(case when dimensionslot=1 then dimName else null end) dim1,
        max(case when dimensionslot=2 then dimName else null end) dim2,
        max(case when dimensionslot=3 then dimName else null end) dim3,
        max(case when dimensionslot=4 then dimName else null end) dim4,
        max(case when dimensionslot=5 then dimName else null end) dim5,
        max(case when dimensionslot=6 then dimName else null end) dim6,
        max(case when dimensionslot=7 then dimName else null end) dim7,
        max(case when dimensionslot=8 then dimName else null end) dim8,
        max(case when dimensionslot=9 then dimName else null end) dim9,
        max(case when dimensionslot=10 then dimName else null end) dim10,
        max(case when dimensionslot=11 then dimName else null end) dim11,
        max(case when dimensionslot=12 then dimName else null end) dim12,
        max(case when dimensionslot=13 then dimName else null end) dim13,
        max(case when dimensionslot=14 then dimName else null end) dim14,
        max(case when dimensionslot=15 then dimName else null end) dim15
    FROM
        (
            SELECT   /* ORIGSQL: (SELECT dimensionslot, md.name dimName, tbl.name tblname, md.effectivestartdate,(...) */
            dimensionslot,
            md.name AS dimName,
            tbl.name AS tblname,
            md.effectivestartdate,
            md.effectiveenddate
        FROM
            cs_mdltdimension md
        INNER JOIN
            cs_relationalmdlt tbl
            ON tbl.ruleelementseq = md.ruleelementseq
            AND TO_DATE(ADD_SECONDS(tbl.effectiveenddate,(86400*-1))) BETWEEN md.effectivestartdate AND TO_DATE(ADD_SECONDS(md.effectiveenddate  /* ORIGSQL: tbl.effectiveenddate - 1 */
                    ,(86400*-1)))   /* ORIGSQL: md.effectiveenddate - 1 */
            AND tbl.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        WHERE
            md.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    ) group by TBLNAME,
        EFFECTIVESTARTDATE,
        EFFECTIVEENDDATE) WITH READ ONLY