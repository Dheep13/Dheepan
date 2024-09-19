CREATE PROCEDURE EXT.STEL_RPT_POST_MREMITSUMMARY
(
    IN in_reporttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                    /* ORIGSQL: in_reporttype IN VARCHAR */
    IN in_periodseq BIGINT,   /* ORIGSQL: in_periodseq IN INTEGER */
    IN in_processingunitseq BIGINT     /* ORIGSQL: in_processingunitseq IN INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    --v_period cs_period%rowtype;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_period ROW LIKE cs_period;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT *
    INTO
        v_period
    FROM
        cs_period
    WHERE
        periodseq = :in_periodseq
        AND removedate >CURRENT_TIMESTAMP;  /* ORIGSQL: sysdate */

    /* ORIGSQL: UPDATE STEL_RPT_DATA_MREMITSUMMARY tgt SET QUANTA1 = (SELECT SUM(VALUE) FROM ste(...) */
    UPDATE EXT.STEL_RPT_DATA_MREMITSUMMARY tgt
        /* --otc = */
        /* --(SELECT SUM (VALUE) */
            /* --   FROM stel_mremit_oti */
            /* -- WHERE                                  --title= tgt.designation */
            /* --    REPLACE (title, ' ', '') = */
            /* --    REPLACE (tgt.designation, ' ', '')), */
        SET
        /* ORIGSQL: QUANTA1 = */
        QUANTA1 = (
            SELECT   /* ORIGSQL: (SELECT SUM(VALUE) FROM stel_mremit_oti WHERE REPLACE(title, ' ', '') = REPLACE((...) */
                SUM(VALUE) 
            FROM
                ext.stel_mremit_oti
            WHERE
                /* --title= tgt.designation */
                REPLACE(title, ' ', '') =
                REPLACE(tgt.designation, ' ', '')
                AND metric = 'Metric 1'
        ),
        /* ORIGSQL: QUANTA2 = */
        QUANTA2 = (
            SELECT   /* ORIGSQL: (SELECT SUM(VALUE) FROM stel_mremit_oti WHERE REPLACE(title, ' ', '') = REPLACE((...) */
                SUM(VALUE)
            FROM
                ext.stel_mremit_oti
            WHERE
                /* --title= tgt.designation */
                REPLACE(title, ' ', '') =
                REPLACE(tgt.designation, ' ', '')
                AND metric = 'Metric 2'
        ),
        /* ORIGSQL: QUANTA3 = */
        QUANTA3 = (
            SELECT   /* ORIGSQL: (SELECT SUM(VALUE) FROM stel_mremit_oti WHERE REPLACE(title, ' ', '') = REPLACE((...) */
                SUM(VALUE)
            FROM
                ext.stel_mremit_oti
            WHERE
                /* --title= tgt.designation */
                REPLACE(title, ' ', '') =
                REPLACE(tgt.designation, ' ', '')
                AND metric = 'Metric 3'
        ),
        /* ORIGSQL: QUANTA4 = */
        QUANTA4 = (
            SELECT   /* ORIGSQL: (SELECT SUM(VALUE) FROM stel_mremit_oti WHERE REPLACE(title, ' ', '') = REPLACE((...) */
                SUM(VALUE)
            FROM
                ext.stel_mremit_oti
            WHERE
                /* --title= tgt.designation */
                REPLACE(title, ' ', '') =
                REPLACE(tgt.designation, ' ', '')
                AND metric = 'Metric 4'
        )
    WHERE
        tgt.periodseq = :in_periodseq
        AND tgt.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: UPDATE STEL_RPT_DATA_MREMITSUMMARY tgt SET GE_ID = Positionname, staff = lastnam(...) */
    UPDATE EXT.STEL_RPT_DATA_MREMITSUMMARY tgt
        SET
        /* ORIGSQL: GE_ID = */
        GE_ID = Positionname,
        /* ORIGSQL: staff = */
        staff = lastname
    WHERE
        tgt.periodseq = :in_periodseq
        AND tgt.processingunitseq = :in_processingunitseq;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Populate data for Payee report
    IF 1 = 1
    THEN 
        /* ORIGSQL: Delete from stel_rpt_data_mremitpayee where periodseq = in_periodseq ; */
        DELETE
        FROM
            ext.stel_rpt_data_mremitpayee
        WHERE
            periodseq = :in_periodseq;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.MREMIT_METRIC_DEF' not found */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_MREMITPAYEE' not found */

        /* ORIGSQL: insert into stel_rpt_data_mremitpayee (PAYEESEQ, POSITIONSEQ, PERIODSEQ, LASTNAM(...) */
        INSERT INTO ext.stel_rpt_data_mremitpayee
            (
                PAYEESEQ,
                POSITIONSEQ,
                PERIODSEQ,
                LASTNAME,
                GE_ID,
                PU_NAME,
                SHOPNAME,
                SECTION,
                SECTIONNAME,
                METRICNAME,
                QUANTA,
                TARGET,
                ACTUAL,
                PERCETAGE_ACTIEVED,
                AMOUNT,
                TOTAL,
                FINALPAYOUT
            )
            
                SELECT   /* ORIGSQL: (select payeeseq,positionseq,periodseq,lastname,ge_id,processingunitname,Null Sh(...) */
                    payeeseq,
                    positionseq,
                    periodseq,
                    lastname,
                    ge_id,
                    processingunitname,
                    NULL AS ShopName,
                    1 AS section,
                    'INDIVIDUAL ACHIEVEMENT' AS SectionName,
                    (
                        SELECT   /* ORIGSQL: (select metric_description from mremit_Metric_Def where metric_name='OTC_Metric1(...) */
                            metric_description
                        FROM
                            mremit_Metric_Def
                        WHERE
                            metric_name = 'OTC_Metric1'
                    ) AS metricName,
                    quanta1 AS Quanta,
                    registration_target AS target,
                    registration_actuals AS Actual,
                    registration_achievement AS Percetage_Actieved,
                    registration_payout AS Amount,
                    NULL AS Total,
                    NULL AS FinalPayout
                FROM
                    EXT.STEL_RPT_DATA_MREMITSUMMARY
                WHERE
                    periodseq = :in_periodseq
                    AND processingunitseq = :in_processingunitseq
            UNION ALL
                SELECT   /* ORIGSQL: select payeeseq,positionseq,periodseq,lastname,ge_id,processingunitname,Null Sho(...) */
                    payeeseq,
                    positionseq,
                    periodseq,
                    lastname,
                    ge_id,
                    processingunitname,
                    NULL AS ShopName,
                    1 AS section,
                    'INDIVIDUAL ACHIEVEMENT' AS SectionName,
                    (
                        SELECT   /* ORIGSQL: (select metric_description from mremit_Metric_Def where metric_name='OTC_Metric2(...) */
                            metric_description
                        FROM
                            mremit_Metric_Def
                        WHERE
                            metric_name = 'OTC_Metric2'
                    ) AS metricName,
                    quanta2 AS Quanta,
                    NULL AS target,
                    first_remittance_count AS Actual,
                    NULL AS Percetage_Actieved,
                    remittance_payout AS Amount,
                    NULL AS Total,
                    NULL AS FinalPayout
                FROM
                    EXT.STEL_RPT_DATA_MREMITSUMMARY
                WHERE
                    periodseq = :in_periodseq
                    AND processingunitseq = :in_processingunitseq
            UNION ALL
                SELECT   /* ORIGSQL: select payeeseq,positionseq,periodseq,lastname,ge_id,processingunitname,Null Sho(...) */
                    payeeseq,
                    positionseq,
                    periodseq,
                    lastname,
                    ge_id,
                    processingunitname,
                    NULL AS ShopName,
                    1 AS section,
                    'TEAM ACHIEVEMENT' AS SectionName,
                    (
                        SELECT   /* ORIGSQL: (select metric_description from mremit_Metric_Def where metric_name='OTC_Metric3(...) */
                            metric_description
                        FROM
                            mremit_Metric_Def
                        WHERE
                            metric_name = 'OTC_Metric3'
                    ) AS metricName,
                    quanta3 AS quanta,
                    active_customer_base_target AS target,
                    active_customer_base_actuals AS actual,
                    active_customer_base_ach AS percetage_actieved,
                    team_payout AS amount,
                    NULL AS total,
                    NULL AS finalpayout
                FROM
                    ext.stel_rpt_data_mremitsummary
                WHERE
                    periodseq = :in_periodseq
                    AND processingunitseq = :in_processingunitseq
            UNION ALL
                SELECT   /* ORIGSQL: select payeeseq,positionseq,periodseq,lastname,ge_id,processingunitname,Null Sho(...) */
                    payeeseq,
                    positionseq,
                    periodseq,
                    lastname,
                    ge_id,
                    processingunitname,
                    NULL AS ShopName,
                    2 AS section,
                    'INDIVIDUAL ACHIEVEMENT' AS SectionName,
                    (
                        SELECT   /* ORIGSQL: (select metric_description from mremit_Metric_Def where metric_name='OTC_Metric4(...) */
                            metric_description
                        FROM
                            mremit_Metric_Def
                        WHERE
                            metric_name = 'OTC_Metric4'
                    ) AS metricName,
                    quanta4 AS quanta,
                    NULL AS target,
                    (IFNULL(NON_COMPLIANCE,0) + IFNULL(COMPLAINTS,0)) AS actual,  /* ORIGSQL: nvl(NON_COMPLIANCE,0) */
                                                                                  /* ORIGSQL: nvl(COMPLAINTS,0) */
                    qualititative_adjustment AS percetage_actieved,
                    qualititative AS amount,
                    NULL AS total,
                    NULL AS finalpayout
                FROM
                    ext.stel_rpt_data_mremitsummary
                WHERE
                    periodseq = :in_periodseq
                    AND processingunitseq = :in_processingunitseq
            ;

        /* ORIGSQL: Commit; */
        COMMIT;  

        /* ORIGSQL: update stel_rpt_data_mremitpayee tgt SET (total) = (SELECT SUM(nvl(TEAM_PAYOUT,0(...) */
        UPDATE ext.stel_rpt_data_mremitpayee tgt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_MREMITSUMMARY' not found */
            SET
            /* ORIGSQL: (total) = */
            (total) = (
                (
                    SELECT   /* ORIGSQL: (select SUM(nvl(TEAM_PAYOUT,0)) from stel_rpt_data_mremitsummary where tgt.payee(...) */
                        SUM(IFNULL(TEAM_PAYOUT,0))   
                    FROM
                        ext.stel_rpt_data_mremitsummary
                    WHERE
                        tgt.payeeseq = payeeseq
                        AND tgt.positionseq = positionseq
                        AND tgt.periodseq = periodseq
                )
            )
        WHERE
            section = 1
            AND sectionname = 'TEAM ACHIEVEMENT'
            AND periodseq = :in_periodseq;

        /* ORIGSQL: Commit; */
        COMMIT;  

        /* ORIGSQL: update stel_rpt_data_mremitpayee tgt SET (total) = (SELECT SUM(nvl(INDIVIDUAL_PA(...) */
        UPDATE ext.stel_rpt_data_mremitpayee tgt 
            SET
            /* ORIGSQL: (total) = */
            (total) = (
                (
                    SELECT   /* ORIGSQL: (select SUM(nvl(INDIVIDUAL_PAYOUT,0)) from stel_rpt_data_mremitsummary where tgt(...) */
                        SUM(IFNULL(INDIVIDUAL_PAYOUT,0))   
                    FROM
                        ext.stel_rpt_data_mremitsummary
                    WHERE
                        tgt.payeeseq = payeeseq
                        AND tgt.positionseq = positionseq
                        AND tgt.periodseq = periodseq
                )
            )
        WHERE
            section = 1
            AND sectionname = 'INDIVIDUAL ACHIEVEMENT'
            AND periodseq = :in_periodseq;

        /* ORIGSQL: Commit; */
        COMMIT;  

        /* ORIGSQL: update stel_rpt_data_mremitpayee tgt SET (total) = (SELECT SUM(nvl(QUALITITATIVE(...) */
        UPDATE ext.stel_rpt_data_mremitpayee tgt 
            SET
            /* ORIGSQL: (total) = */
            (total) = (
                (
                    SELECT   /* ORIGSQL: (select SUM(nvl(QUALITITATIVE,0)) from stel_rpt_data_mremitsummary where tgt.pay(...) */
                        SUM(IFNULL(QUALITITATIVE,0))   
                    FROM
                        ext.stel_rpt_data_mremitsummary
                    WHERE
                        tgt.payeeseq = payeeseq
                        AND tgt.positionseq = positionseq
                        AND tgt.periodseq = periodseq
                )
            )
        WHERE
            section = 2
            AND sectionname = 'INDIVIDUAL ACHIEVEMENT'
            AND periodseq = :in_periodseq;

        /* ORIGSQL: Commit; */
        COMMIT;  

        /* ORIGSQL: update stel_rpt_data_mremitpayee tgt SET (finalpayout) = (SELECT SUM(nvl(FINAL_P(...) */
        UPDATE ext.stel_rpt_data_mremitpayee tgt 
            SET
            /* ORIGSQL: (finalpayout) = */
            (finalpayout) = (
                (
                    SELECT   /* ORIGSQL: (select SUM(nvl(FINAL_PAYOUT,0)) from stel_rpt_data_mremitsummary where tgt.paye(...) */
                        SUM(IFNULL(FINAL_PAYOUT,0))   
                    FROM
                        ext.stel_rpt_data_mremitsummary
                    WHERE
                        tgt.payeeseq = payeeseq
                        AND tgt.positionseq = positionseq
                        AND tgt.periodseq = periodseq
                )
            )
        WHERE
            1 = 1
            AND periodseq = :in_periodseq;

        /* ORIGSQL: Commit; */
        COMMIT;  

        /* ORIGSQL: update stel_rpt_data_mremitpayee tgt SET shopname = (SELECT 'mRemittance - '|| n(...) */
        UPDATE ext.stel_rpt_data_mremitpayee tgt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
            SET
            /* ORIGSQL: shopname = */
            shopname = (
                SELECT   /* ORIGSQL: (Select 'mRemittance - '|| nvl(cls.classfiername,pos.genericattribute2) from cs_(...) */
                    'mRemittance - '|| IFNULL(cls.classfiername, pos.genericattribute2)   
                FROM
                    /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                    EXT.stel_classifier AS cls
                RIGHT OUTER JOIN
                    cs_position AS pos
                    ON pos.genericattribute2 = cls.classifierid  /* ORIGSQL: pos.genericattribute2=cls.classifierid(+) */
                    AND cls.effectivestartdate < :v_period.enddate  /* ORIGSQL: cls.effectivestartdate(+)<v_period.enddate */
                    AND cls.effectiveenddate >= :v_period.enddate  /* ORIGSQL: cls.effectiveenddate(+)>=v_period.enddate */
                WHERE
                    tgt.payeeseq = payeeseq
                    AND tgt.positionseq = ruleelementownerseq
                    AND pos.removedate = :v_period.removedate
                    AND pos.effectiveenddate >= :v_period.enddate
                    AND pos.effectivestartdate < :v_period.enddate
            )
        WHERE
            1 = 1
            AND periodseq = :in_periodseq;

        /* ORIGSQL: Commit; */
        COMMIT;
    END IF;
END