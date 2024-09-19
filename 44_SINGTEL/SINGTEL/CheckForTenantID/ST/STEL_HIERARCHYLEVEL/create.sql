CREATE VIEW "EXT"."STEL_HIERARCHYLEVEL" ( "NAME", "HIERARCHYLEVEL" ) AS (SELECT   /* ORIGSQL: SELECT t.name, COUNT(DISTINCT tsub.name) + 1 HierarchyLevel FROM cs_position pos(...) */
        t.name,
        COUNT(DISTINCT tsub.name) + 1 AS HierarchyLevel
    FROM
        cs_position pos
    INNER JOIN
        cs_title t
        ON pos.titleseq = t.ruleelementownerseq
        AND t.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    LEFT OUTER JOIN
        cs_position sub
        ON pos.ruleelementownerseq = sub.managerseq
        AND sub.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    LEFT OUTER JOIN
        cs_title tsub
        ON sub.titleseq = tsub.ruleelementownerseq
        AND tsub.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    WHERE
        pos.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    GROUP BY
        t.name) WITH READ ONLY