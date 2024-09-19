CREATE VIEW "EXT"."STEL_POSITION" ( "RULEELEMENTOWNERSEQ", "TENANTID", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE", "PAYEESEQ", "MANAGERSEQ", "POSNAME", "TITLENAME", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "GENERICATTRIBUTE5", "GENERICATTRIBUTE6", "GENERICATTRIBUTE7", "GENERICATTRIBUTE8", "GENERICATTRIBUTE9", "GENERICATTRIBUTE10", "GENERICATTRIBUTE11", "GENERICATTRIBUTE12", "GENERICATTRIBUTE13", "GENERICATTRIBUTE14", "GENERICATTRIBUTE15", "GENERICATTRIBUTE16", "GENERICNUMBER1", "GENERICNUMBER2", "GENERICNUMBER3", "GENERICNUMBER4", "GENERICNUMBER5", "GENERICNUMBER6", "GENERICDATE1", "GENERICDATE2", "GENERICDATE3", "GENERICDATE4", "GENERICDATE5", "GENERICDATE6", "GENERICBOOLEAN1", "GENERICBOOLEAN2", "GENERICBOOLEAN3", "GENERICBOOLEAN4", "GENERICBOOLEAN5", "GENERICBOOLEAN6", "MGRPOSNAME", "MGREFFECTIVESTARTDATE", "MGREFFECTIVEENDTDATE", "MGRLASTNAME" ) AS (SELECT   /* ORIGSQL: SELECT pos.RULEELEMENTOWNERSEQ, pos.tenantid, pos.EFFECTIVESTARTDATE, pos.EFFECT(...) */
        pos.RULEELEMENTOWNERSEQ,
        pos.tenantid,
        pos.EFFECTIVESTARTDATE,
        pos.EFFECTIVEENDDATE,
        pos.PAYEESEQ,
        pos.MANAGERSEQ,
        pos.NAME AS POSNAME,
        t.name AS TITLENAME,
        pos.GENERICATTRIBUTE1,
        pos.GENERICATTRIBUTE2,
        pos.GENERICATTRIBUTE3,
        pos.GENERICATTRIBUTE4,
        pos.GENERICATTRIBUTE5,
        pos.GENERICATTRIBUTE6,
        pos.GENERICATTRIBUTE7,
        pos.GENERICATTRIBUTE8,
        pos.GENERICATTRIBUTE9,
        pos.GENERICATTRIBUTE10,
        pos.GENERICATTRIBUTE11,
        pos.GENERICATTRIBUTE12,
        pos.GENERICATTRIBUTE13,
        pos.GENERICATTRIBUTE14,
        pos.GENERICATTRIBUTE15,
        pos.GENERICATTRIBUTE16,
        pos.GENERICNUMBER1,
        pos.GENERICNUMBER2,
        pos.GENERICNUMBER3,
        pos.GENERICNUMBER4,
        pos.GENERICNUMBER5,
        pos.GENERICNUMBER6,
        pos.GENERICDATE1,
        pos.GENERICDATE2,
        pos.GENERICDATE3,
        pos.GENERICDATE4,
        pos.GENERICDATE5,
        pos.GENERICDATE6,
        pos.GENERICBOOLEAN1,
        pos.GENERICBOOLEAN2,
        pos.GENERICBOOLEAN3,
        pos.GENERICBOOLEAN4,
        pos.GENERICBOOLEAN5,
        pos.GENERICBOOLEAN6,
        mgr.name AS MGRPOSNAME,
        mgr.effectivestartdate AS mgreffectivestartdate,
        mgr.effectiveenddate AS mgreffectiveendtdate,
        mpar.lastname AS mgrlastname
    FROM
        cs_position pos
    INNER JOIN
        cs_title t
        ON pos.titleseq = t.ruleelementownerseq
        AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1))) BETWEEN t.effectivestartdate AND TO_DATE(ADD_SECONDS(t.effectiveenddate,(86400*-1)))   /* ORIGSQL: t.effectiveenddate - 1 */
                                                                                                                                                         /* ORIGSQL: pos.effectiveenddate - 1 */
        AND pos.tenantid = t.tenantid
    LEFT OUTER JOIN
        cs_position mgr
        ON mgr.ruleelementownerseq = pos.managerseq
        AND mgr.tenantid = pos.tenantid
        AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1))) BETWEEN mgr.effectivestartdate AND TO_DATE(ADD_SECONDS(mgr.effectiveenddate  /* ORIGSQL: pos.effectiveenddate - 1 */
                ,(86400*-1)))   /* ORIGSQL: mgr.effectiveenddate - 1 */
        AND mgr.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    LEFT OUTER JOIN
        cs_participant mpar
        ON mpar.payeeseq = mgr.payeeseq
        AND mpar.tenantid = pos.tenantid
        AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1))) BETWEEN mpar.effectivestartdate AND TO_DATE(ADD_SECONDS(mpar.effectiveenddate  /* ORIGSQL: pos.effectiveenddate - 1 */
                ,(86400*-1)))   /* ORIGSQL: mpar.effectiveenddate - 1 */
        AND mpar.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
    WHERE
        pos.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND t.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        AND pos.tenantid = 'STEL') WITH READ ONLY