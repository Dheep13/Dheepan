CREATE VIEW "EXT"."STEL_WFDEALERCODES" ( "PARENTPOSITIONNAME", "CHILDPOSITIONS" ) AS (SELECT   /* ORIGSQL: select parentpositionname, listagg(to_char(childpositionname),', ') WITHIN GROUP(...) */
        parentpositionname,
        STRING_AGG(TO_VARCHAR(childpositionname,NULL),', ' ORDER BY parentpositionname) AS ChildPositions  /* ORIGSQL: listagg(to_char(childpositionname),', ') WITHIN GROUP (ORDER BY parentpositionna(...) */
    FROM
        ext.stel_positionrelation
    WHERE
        relnname = 'Dealer-AM Mapping'
        AND relnenddate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
    GROUP BY
        parentpositionname) WITH READ ONLY