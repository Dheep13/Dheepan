CREATE VIEW "EXT"."STEL_MREMIT_OTI" ( "NAME", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE", "VALUE", "CHANNEL", "TITLE", "METRIC" ) AS (((((SELECT   /* ORIGSQL: SELECT name, effectivestartdate, effectiveenddate, VALUE, dim0 channel, dim1 tit(...) */
        name,
        effectivestartdate,
        effectiveenddate,
        VALUE,
        dim0 AS channel,
        dim1 AS title,
        dim2 AS metric
    FROM
        ext.stel_lookup
    WHERE
        name IN ('LT_MetricIncentive')) UNION (SELECT   /* ORIGSQL: SELECT name, effectivestartdate, effectiveenddate, VALUE * 100, dim0 channel, di(...) */
        name,
        effectivestartdate,
        effectiveenddate,
        VALUE * 100,
        dim0 AS channel,
        dim1 AS title,
        dim2 AS metric
    FROM
        ext.stel_lookup
    WHERE
        name IN ('LT_Cap')
        AND dim2 = 'Metric 4')) UNION (SELECT   /* ORIGSQL: SELECT name, effectivestartdate, effectiveenddate, VALUE, dim0 channel, dim1 tit(...) */
        name,
        effectivestartdate,
        effectiveenddate,
        VALUE,
        dim0 AS channel,
        dim1 AS title,
        dim2 AS metric
    FROM
        ext.stel_lookup
    WHERE
        name IN ('LT_Cap')
        AND dim2 = 'Metric 2')) UNION (SELECT   /* ORIGSQL: SELECT name, effectivestartdate, effectiveenddate, VALUE, dim0 channel, dim1 tit(...) */
        name,
        effectivestartdate,
        effectiveenddate,
        VALUE,
        dim0 AS channel,
        dim1 AS title,
        dim2 AS metric
    FROM
        ext.stel_lookup
    WHERE
        name IN ('LT_Cap')
        AND dim2 = 'Metric 3'))) WITH READ ONLY