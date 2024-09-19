CREATE VIEW "EXT"."VW_AGGREGATEDCREDIT" ( "NAME", "POSITIONSEQ", "PAYEESEQ", "PERIODSEQ", "CREDITTYPEID", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "GENERICATTRIBUTE5", "GENERICATTRIBUTE6", "TENANTID", "PROCESSINGUNITSEQ", "VALUE", "GENERICNUMBER1", "GENERICNUMBER2", "GENERICNUMBER3", "GENERICNUMBER4", "GENERICNUMBER5", "GENERICNUMBER6" ) AS (SELECT   /* ORIGSQL: SELECT c.name, c.positionseq, c.payeeseq, c.periodseq, ct.credittypeid, c.generi(...) */
        c.name,
        c.positionseq,
        c.payeeseq,
        c.periodseq,
        ct.credittypeid,
        c.genericattribute1,
        c.genericattribute2,
        c.genericattribute3,
        c.genericattribute4,
        c.genericattribute5,
        c.genericattribute6,
        c.tenantid,
        c.processingunitseq,
        SUM(IFNULL(c.VALUE, 0)) AS VALUE,  /* ORIGSQL: NVL(c.VALUE, 0) */
        SUM(IFNULL(c.genericnumber1, 0)) AS genericnumber1,  /* ORIGSQL: NVL(c.genericnumber1, 0) */
        SUM(IFNULL(c.genericnumber2, 0)) AS genericnumber2,  /* ORIGSQL: NVL(c.genericnumber2, 0) */
        SUM(IFNULL(c.genericnumber3, 0)) AS genericnumber3,  /* ORIGSQL: NVL(c.genericnumber3, 0) */
        SUM(IFNULL(c.genericnumber4, 0)) AS genericnumber4,  /* ORIGSQL: NVL(c.genericnumber4, 0) */
        SUM(IFNULL(c.genericnumber5, 0)) AS genericnumber5,  /* ORIGSQL: NVL(c.genericnumber5, 0) */
        SUM(IFNULL(c.genericnumber6, 0)) AS genericnumber6  /* ORIGSQL: NVL(c.genericnumber6, 0) */
    FROM
        cs_Credit c
    INNER JOIN
        cs_Credittype ct
        ON ct.datatypeseq = c.credittypeseq
        AND ct.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    GROUP BY
        c.name,
        c.positionseq,
        c.payeeseq,
        c.periodseq,
        ct.credittypeid,
        c.genericattribute1,
        c.genericattribute2,
        c.genericattribute3,
        c.genericattribute4,
        c.tenantid,
        c.processingunitseq,
        c.genericattribute5,
        c.genericattribute6) WITH READ ONLY