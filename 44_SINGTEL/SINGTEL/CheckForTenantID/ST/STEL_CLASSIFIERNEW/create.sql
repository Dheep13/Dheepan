CREATE VIEW "EXT"."STEL_CLASSIFIERNEW" ( "CATEGORYNAME", "CATEGORYTREENAME", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE", "CLASSIFIERID", "CLASSIFIERSEQ", "CLASSFIERNAME", "DESCRIPTION", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "GENERICATTRIBUTE5", "GENERICATTRIBUTE6", "GENERICATTRIBUTE7", "GENERICATTRIBUTE8", "GENERICATTRIBUTE9", "GENERICATTRIBUTE10", "GENERICATTRIBUTE11", "GENERICATTRIBUTE12", "GENERICATTRIBUTE13", "GENERICATTRIBUTE14", "GENERICATTRIBUTE15", "GENERICATTRIBUTE16", "GENERICNUMBER1", "GENERICNUMBER2", "GENERICNUMBER3", "GENERICNUMBER4", "GENERICNUMBER5", "GENERICNUMBER6", "GENERICDATE1", "GENERICDATE2", "GENERICDATE3", "GENERICDATE4", "GENERICDATE5", "GENERICDATE6", "GENERICBOOLEAN1", "GENERICBOOLEAN2", "GENERICBOOLEAN3", "GENERICBOOLEAN4", "GENERICBOOLEAN5", "GENERICBOOLEAN6", "PRICE", "COST" ) AS ((((SELECT cat.name CategoryName,
          ct.name CategoryTreeName,
          cl.effectivestartdate,
          cl.effectiveenddate,
          cl.classifierid, cl.classifierseq,
          cl.name AS ClassfierName,
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
          gc.genericnumber2,
          gc.genericnumber3,
          gc.genericnumber4,
          gc.genericnumber5,
          gc.genericnumber6,
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
          gc.genericboolean6,
          NULL price,
          NULL cost
     FROM cs_Classifier cl
          JOIN cs_category_classifiers ccc
             ON cl.classifierseq = ccc.classifierseq
                AND cl.tenantid = ccc.tenantid
                /*AND (ccc.effectivestartdate, ccc.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,cl.effectiveenddate - 1)*/
          		and ccc.effectivestartdate < cl.effectiveenddate
          		and ccc.effectiveenddate > cl.effectivestartdate
          JOIN cs_category cat
             ON     ccc.categorytreeseq = cat.categorytreeseq
                AND ccc.categoryseq = cat.ruleelementseq
                AND cat.tenantid = cl.tenantid
                /*AND (cat.effectivestartdate, cat.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,cl.effectiveenddate - 1)*/
                and cat.effectivestartdate < cl.effectiveenddate
                and cat.effectiveenddate > cl.effectivestartdate
          JOIN cs_categorytree ct
             ON ct.categorytreeseq = cat.categorytreeseq
                AND ct.tenantid = cat.tenantid
                /*AND (ct.effectivestartdate, ct.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,cl.effectiveenddate - 1)*/
                and ct.effectivestartdate < cl.effectiveenddate 
                and ct.effectiveenddate > cl.effectivestartdate
          JOIN cs_Genericclassifier gc
             ON     gc.classifierseq = cl.classifierseq
                AND gc.tenantid = cl.tenantid
                AND gc.effectivestartdate = cl.effectivestartdate
    WHERE     cat.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND ccc.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND cl.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND ct.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND gc.removedate = TO_DATE ('22000101', 'YYYYMMDD')) UNION (SELECT cat.name CategoryName,
          ct.name CategoryTreeName,
          cl.effectivestartdate,
          cl.effectiveenddate,
          cl.classifierid, cl.classifierseq,
          cl.name AS ClassfierName,
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
          gc.genericnumber2,
          gc.genericnumber3,
          gc.genericnumber4,
          gc.genericnumber5,
          gc.genericnumber6,
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
          gc.genericboolean6,
          gc.price,
          gc.cost
     FROM cs_Classifier cl
          JOIN cs_category_classifiers ccc
             ON cl.classifierseq = ccc.classifierseq
                AND cl.tenantid = ccc.tenantid
                /*AND (ccc.effectivestartdate, ccc.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,cl.effectiveenddate - 1)*/
          		and ccc.effectivestartdate < cl.effectiveenddate
          		and ccc.effectiveenddate > cl.effectivestartdate
          JOIN cs_category cat
             ON     ccc.categorytreeseq = cat.categorytreeseq
                AND ccc.categoryseq = cat.ruleelementseq
                AND cat.tenantid = cl.tenantid
                /*AND (cat.effectivestartdate, cat.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,
                                cl.effectiveenddate - 1)*/
                and cat.effectivestartdate < cl.effectiveenddate
          		and cat.effectiveenddate > cl.effectivestartdate
          JOIN cs_categorytree ct
             ON ct.categorytreeseq = cat.categorytreeseq
                AND ct.tenantid = cat.tenantid
                /*AND (ct.effectivestartdate, ct.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,
                                cl.effectiveenddate - 1)*/
                and ct.effectivestartdate < cl.effectiveenddate
          		and ct.effectiveenddate > cl.effectivestartdate
          JOIN cs_product gc
             ON     gc.classifierseq = cl.classifierseq
                AND gc.tenantid = cl.tenantid
                AND gc.effectivestartdate = cl.effectivestartdate
    WHERE     cat.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND ccc.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND cl.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND ct.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND gc.removedate = TO_DATE ('22000101', 'YYYYMMDD'))) UNION (SELECT cat.name CategoryName,
          ct.name CategoryTreeName,
          cl.effectivestartdate,
          cl.effectiveenddate,
          cl.classifierid, cl.classifierseq,
          cl.name AS ClassfierName,
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
          gc.genericnumber2,
          gc.genericnumber3,
          gc.genericnumber4,
          gc.genericnumber5,
          gc.genericnumber6,
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
          gc.genericboolean6,
          NULL price,
          NULL cost
     FROM cs_Classifier cl
          JOIN cs_category_classifiers ccc
             ON cl.classifierseq = ccc.classifierseq
                AND cl.tenantid = ccc.tenantid
                /*AND (ccc.effectivestartdate, ccc.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,
                                cl.effectiveenddate - 1)*/
                 and ccc.effectivestartdate < cl.effectiveenddate
          		and ccc.effectiveenddate > cl.effectivestartdate
          JOIN cs_category cat
             ON     ccc.categorytreeseq = cat.categorytreeseq
                AND ccc.categoryseq = cat.ruleelementseq
                AND cat.tenantid = cl.tenantid
                /*AND (cat.effectivestartdate, cat.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,
                                cl.effectiveenddate - 1)*/
                and cat.effectivestartdate < cl.effectiveenddate
          		and cat.effectiveenddate > cl.effectivestartdate
          JOIN cs_categorytree ct
             ON ct.categorytreeseq = cat.categorytreeseq
                AND ct.tenantid = cat.tenantid
               /* AND (ct.effectivestartdate, ct.effectiveenddate - 1)
                      OVERLAPS (cl.effectivestartdate,
                                cl.effectiveenddate - 1)*/
                 and ct.effectivestartdate < cl.effectiveenddate
          		and ct.effectiveenddate > cl.effectivestartdate
          JOIN cs_Customer gc
             ON     gc.classifierseq = cl.classifierseq
                AND gc.tenantid = cl.tenantid
                AND gc.effectivestartdate = cl.effectivestartdate
    WHERE     cat.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND ccc.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND cl.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND ct.removedate = TO_DATE ('22000101', 'YYYYMMDD')
          AND gc.removedate = TO_DATE ('22000101', 'YYYYMMDD')))) WITH READ ONLY