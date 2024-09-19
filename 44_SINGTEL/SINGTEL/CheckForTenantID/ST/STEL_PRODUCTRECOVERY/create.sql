CREATE VIEW "EXT"."STEL_PRODUCTRECOVERY" ( "CLASSIFIERID", "EFFSTART", "EFFEND", "CLASSFIERNAME", "PRICE", "PRICEUT", "COST", "COSTUT", "CATEGORYTREENAME", "CATEGORYNAME", "NAME", "DESCRIPTION", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "GENERICATTRIBUTE5", "GENERICATTRIBUTE6", "GENERICATTRIBUTE7", "GENERICATTRIBUTE8", "GENERICATTRIBUTE9", "GENERICATTRIBUTE10", "GENERICATTRIBUTE11", "GENERICATTRIBUTE12", "GENERICATTRIBUTE13", "GENERICATTRIBUTE14", "GENERICATTRIBUTE15", "GENERICATTRIBUTE16", "GENERICNUMBER1", "GN1UT", "GENERICNUMBER2", "GN2UT", "GENERICNUMBER3", "GN3UT", "GENERICNUMBER4", "GN4UT", "GENERICNUMBER5", "GN5UT", "GENERICNUMBER6", "GN6UT", "GENERICDATE1", "GENERICDATE2", "GENERICDATE3", "GENERICDATE4", "GENERICDATE5", "GENERICDATE6", "GENERICBOOLEAN1", "GENERICBOOLEAN2", "GENERICBOOLEAN3", "GENERICBOOLEAN4", "GENERICBOOLEAN5", "GENERICBOOLEAN6" ) AS (SELECT   /* ORIGSQL: SELECT cl.classifierid, '04/01/2015' effstart, '01/01/2200' effend, cl.name AS C(...) */
        cl.classifierid,
        '04/01/2015' AS effstart,
        '01/01/2200' AS effend,
        cl.name AS ClassfierName,
        gc.price,
        CASE
            WHEN gc.price IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS priceUT,
        gc.cost,
        CASE
            WHEN gc.cost IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS CostUT,
        ct.name AS CategoryTreeName,
        cat.name AS CategoryName,
        bu.name,
        cl.description,
        gc.genericattribute1,
        gc.genericattribute2,
        gc.genericattribute3,
        gc.genericattribute4,
        gc.genericattribute5,
        gc.genericattribute6,
        gc.genericattribute7,
        gc.genericattribute8,
        gc.genericattribute9,
        gc.genericattribute10,
        gc.genericattribute11,
        gc.genericattribute12,
        gc.genericattribute13,
        gc.genericattribute14,
        gc.genericattribute15,
        gc.genericattribute16,
        gc.genericnumber1,
        CASE
            WHEN gc.genericnumber1 IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS gn1ut,
        gc.genericnumber2,
        CASE
            WHEN gc.genericnumber2 IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS gn2ut,
        gc.genericnumber3,
        CASE
            WHEN gc.genericnumber3 IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS gn3ut,
        gc.genericnumber4,
        CASE
            WHEN gc.genericnumber4 IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS gn4ut,
        gc.genericnumber5,
        CASE
            WHEN gc.genericnumber5 IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS gn5ut,
        gc.genericnumber6,
        CASE
            WHEN gc.genericnumber6 IS NOT NULL
            THEN 'MYR'
            ELSE NULL
        END
        AS gn6ut,
        gc.genericdate1,
        gc.genericdate2,
        gc.genericdate3,
        gc.genericdate4,
        gc.genericdate5,
        gc.genericdate6,
        gc.genericboolean1,
        gc.genericboolean2,
        gc.genericboolean3,
        gc.genericboolean4,
        gc.genericboolean5,
        gc.genericboolean6
    FROM
        cs_Classifier cl
    INNER JOIN
        cs_category_classifiers ccc
        ON cl.classifierseq = ccc.classifierseq
        AND cl.tenantid = ccc.tenantid
        -- AND (ccc.effectivestartdate, TO_DATE(ADD_SECONDS(ccc.effectiveenddate,(86400*-1))))  /* ORIGSQL: ccc.effectiveenddate - 1 */
        -- OVERLAPS (cl.effectivestartdate,
        --     TO_DATE(ADD_SECONDS(cl.effectiveenddate,(86400*-1))))  /* ORIGSQL: cl.effectiveenddate - 1 */
        and ccc.effectivestartdate< cl.effectivestartdate
        and ccc.effectiveenddate > cl.effectiveenddate
    INNER JOIN
        cs_category cat
        ON ccc.categorytreeseq = cat.categorytreeseq
        AND ccc.categoryseq = cat.ruleelementseq
        AND cat.tenantid = cl.tenantid
        -- AND (cat.effectivestartdate, TO_DATE(ADD_SECONDS(cat.effectiveenddate,(86400*-1))))  /* ORIGSQL: cat.effectiveenddate - 1 */
        -- OVERLAPS (cl.effectivestartdate,
        --     TO_DATE(ADD_SECONDS(cl.effectiveenddate,(86400*-1))))  /* ORIGSQL: cl.effectiveenddate - 1 */
          and cat.effectivestartdate< cl.effectivestartdate
        and cat.effectiveenddate > cl.effectiveenddate
    INNER JOIN
        cs_categorytree ct
        ON ct.categorytreeseq = cat.categorytreeseq
        AND ct.tenantid = cat.tenantid
        -- AND (ct.effectivestartdate, TO_DATE(ADD_SECONDS(ct.effectiveenddate,(86400*-1))))  /* ORIGSQL: ct.effectiveenddate - 1 */
        -- OVERLAPS (cl.effectivestartdate,
        --     TO_DATE(ADD_SECONDS(cl.effectiveenddate,(86400*-1))))  /* ORIGSQL: cl.effectiveenddate - 1 */
          and ct.effectivestartdate< cl.effectivestartdate
        and ct.effectiveenddate > cl.effectiveenddate
    INNER JOIN
        cs_product gc
        ON gc.classifierseq = cl.classifierseq
        AND gc.tenantid = cl.tenantid
        AND gc.effectivestartdate = cl.effectivestartdate
    LEFT OUTER JOIN
        cs_businessunit bu
        ON BITAND(cl.businessunitmap,bu.mask) = bu.mask
    WHERE
        cat.removedate = TO_DATE('22000101', 'YYYYMMDD')
        AND ccc.removedate = TO_DATE('22000101', 'YYYYMMDD')
        AND cl.removedate = TO_TIMESTAMP('2019-01-30 09:49:15', 'YYYY-MM-DD HH24:MI:SS')
        AND ct.removedate = TO_DATE('22000101', 'YYYYMMDD')
        AND gc.removedate = TO_TIMESTAMP('2019-01-30 09:49:15', 'YYYY-MM-DD HH24:MI:SS')
        AND gc.genericattribute15 IS NOT NULL
        AND gc.genericattribute15 <> 'null') WITH READ ONLY