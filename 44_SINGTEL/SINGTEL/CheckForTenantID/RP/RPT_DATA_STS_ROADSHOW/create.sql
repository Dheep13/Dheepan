CREATE PROCEDURE EXT.RPT_DATA_STS_ROADSHOW
(
    IN in_RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_RPTTYPE IN VARCHAR2 */
    IN in_PERIODSEQ DECIMAL(38,10),   /* ORIGSQL: in_PERIODSEQ IN NUMBER */
    IN in_PROCESSINGUNITSEQ DECIMAL(38,10)   /* ORIGSQL: in_PROCESSINGUNITSEQ IN NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Tenant VARCHAR(255) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR2(255) := 'STEL'; */
    DECLARE v_quantity DECIMAL(38,10);  /* ORIGSQL: v_quantity number; */
    DECLARE v_Calendarname VARCHAR(255) = NULL;  /* ORIGSQL: v_Calendarname VARCHAR2(255) := NULL; */
    DECLARE v_periodname VARCHAR(255);  /* ORIGSQL: v_periodname VARCHAR2(255); */
    DECLARE v_start TIMESTAMP;  /* ORIGSQL: v_start date; */
    DECLARE v_End TIMESTAMP;  /* ORIGSQL: v_End date; */
    DECLARE v_Pu DECIMAL(38,10);  /* ORIGSQL: v_Pu number; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT
        startdate,
        enddate,
        name
    INTO
        v_start,
        v_End,
        v_periodname
    FROM
        cs_period
    WHERE
        periodseq = :in_PERIODSEQ
        AND removedate >CURRENT_TIMESTAMP;  /* ORIGSQL: sysdate */
    SELECT
        :in_PROCESSINGUNITSEQ
    INTO
        v_Pu
    FROM
        SYS.DUMMY;  /* ORIGSQL: FROM dual ; */

    /* ORIGSQL: delete from STEL_RPT_DATA_ROADSHOWACTUALS where periodseq=in_periodseq; */
    DELETE
    FROM
        EXT.STEL_RPT_DATA_ROADSHOWACTUALS
    WHERE
        periodseq = :in_PERIODSEQ;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONASSIGNMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_ROADSHOWACTUALS' not found */

    /* ORIGSQL: insert into STEL_RPT_DATA_ROADSHOWACTUALS select c.periodseq, pos.name, s.generi(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PMCREDITTRACE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_CFG_LOOKUP' not found */
    INSERT INTO EXT.STEL_RPT_DATA_ROADSHOWACTUALS
        --[Arun - Block added by Arun to cater for All Points from PMR 1st Apr 2019 - as the original procedure is  showing fewer values]
        SELECT   /* ORIGSQL: select c.periodseq, pos.name, s.genericattribute4 as RSCode, s.genericattribute1(...) */
            c.periodseq,
            pos.name,
            s.genericattribute4 AS RSCode,
            s.genericattribute1 AS ShopCode,
            l.outputlabel AS reportcategory,
            l.outputlabel AS Product,
            c.genericattribute5,
            'CV',
            SUM(c.value)value
        FROM
            cs_measurement m
        INNER JOIN
            cs_pmcredittrace pm
            ON pm.measurementseq = m.measurementseq
        INNER JOIN
            cs_credit c
            ON c.creditseq = pm.creditseq
        INNER JOIN
            cs_salestransaction s
            ON s.salestransactionseq = c.salestransactionseq
        INNER JOIN
            cs_position pos
            ON pos.ruleelementownerseq = c.positionseq
            AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND s.compensationdate BETWEEN pos.effectivestartdate AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1)))   /* ORIGSQL: pos.effectiveenddate-1 */
        INNER JOIN
            ext.stel_rpt_cfg_lookup l
            ON l.rpttype = 'Roadshow'
            AND l.inputname = m.name
        WHERE
            1 = 1--c.genericattribute14='Roadshow'
            AND c.salestransactionseq IN
            (
                SELECT   /* ORIGSQL: (select ta.salestransactionseq from cs_transactionassignment ta where ta.generic(...) */
                    ta.salestransactionseq
                FROM
                    cs_transactionassignment ta
                WHERE
                    ta.genericattribute2 = 'Roadshow'
                    AND ta.processingunitseq = :v_Pu
                    AND ta.tenantid = :v_Tenant
            )
            AND l.inputname LIKE '%Points%' --[Arun 1st Apr 2019 - Added to just look at ConnCount and Not Points]
            AND pm.tenantid = m.tenantid
            AND pm.tenantid = c.tenantid
            AND pm.processingunitseq = m.processingunitseq
            AND pm.processingunitseq = c.processingunitseq
            AND pm.sourceperiodseq = c.periodseq
            AND pm.targetperiodseq = m.periodseq
            AND c.tenantid = s.tenantid
            AND c.processingunitseq = s.processingunitseq
            AND c.compensationdate = s.compensationdate
            AND c.periodseq = :in_PERIODSEQ
            AND m.periodseq = :in_PERIODSEQ
        GROUP BY
            c.periodseq, pos.name
            , s.genericattribute4
            , s.genericattribute1
            ,l.outputlabel
            , c.genericattribute5;

    -- End of Block added by Arun --[Arun - Block added by Arun to cater for All Points from PMR 1st Apr 2019 - as the original procedure is  showing fewer values]
    --Block Comment by [Arun 1st Apr 2019 - To cater for Points from PMRs instead of Credits
    --select c.periodseq, pos.name
    --, s.genericattribute4 as RSCode
    --, s.genericattribute1 as ShopCode, case
    --when c.genericattribute4 like 'SNBB%Bask%' then 'SNBB'
    --when upper(c.genericattribute4) like 'MOB%BASKET%' then 'Mobile VAS'
    --when c.genericattribute4 like 'FTTH%Main%' then 'FTTH'
    --when c.genericattribute4 like 'SGCC' then 'TV App'
    --when c.genericattribute4 like 'DVR' then 'TV App'
    --when c.genericattribute4 like 'TV%Go%' then 'TV App'
    --when c.genericattribute4 like 'TV GA' then 'TV'
    --when upper(c.genericattribute4) like '%HOME%PACK%' then 'Smart Home'
    --else c.genericattribute4 end  as reportcategory
    --, case
        --when c.genericattribute4 like 'SNBB%Bask%' then 'SNBB'
        --when upper(c.genericattribute4) like 'MOB%BASKET%' then 'Mobile VAS'
        --when c.genericattribute4 like 'FTTH%Main%' then 'FTTH'
        --when c.genericattribute4 like 'SGCC' then 'TV App'
        --when c.genericattribute4 like 'DVR' then 'TV App'
        --when c.genericattribute4 like 'TV%Go%' then 'TV App'
        --when c.genericattribute4 like 'TV GA' then 'TV'
        --when upper(c.genericattribute4) like '%HOME%PACK%' then 'Smart Home'
        --else c.genericattribute4 end as Product
        --, c.genericattribute5, 'CV', sum(c.value ) value
        --from cs_credit c
        --join cs_salestransaction s
        --on s.salestransactionseq=c.salestransactionseq
        --join cs_position pos
        --on pos.ruleelementownerseq=c.positionseq
        --and pos.removedate>sysdate and s.compensationdate between pos.effectivestartdate and pos.effectiveenddate-1
        --
        --where c.name like '%Submitted%Points'
        ----and c.genericattribute14='Roadshow'
        --and c.salestransactionseq in (select ta.salestransactionseq from cs_transactionassignment ta
            --                                where ta.genericattribute2='Roadshow' and ta.processingunitseq=v_pu
        -- and ta.tenantid=v_tenant  )
        --and c.tenantid=s.tenantid
        --and c.processingunitseq=s.processingunitseq
        --and c.compensationdate=s.compensationdate
        --and c.genericattribute4 IS NOT NULL
        --and c.periodseq=in_periodseq
        --group by
        --c.periodseq, pos.name
        --, s.genericattribute4
        --, s.genericattribute1
        --, case
            --when c.genericattribute4 like 'SNBB%Bask%' then 'SNBB'
            --when upper(c.genericattribute4) like 'MOB%BASKET%' then 'Mobile VAS'
            --when c.genericattribute4 like 'FTTH%Main%' then 'FTTH'
            --when c.genericattribute4 like 'SGCC' then 'TV App'
            --when c.genericattribute4 like 'DVR' then 'TV App'
            --when c.genericattribute4 like 'TV%Go%' then 'TV App'
            --when c.genericattribute4 like 'TV GA' then 'TV'
            --when upper(c.genericattribute4) like '%HOME%PACK%' then 'Smart Home'
            --else c.genericattribute4 end
            --, c.genericattribute5
            --;
            --Block Comment by [Arun 1st Apr 2019 - To cater for Points from PMRs instead of Credits
            /* ORIGSQL: commit; */
            COMMIT; 

    /* ORIGSQL: insert into STEL_RPT_DATA_ROADSHOWACTUALS select c.periodseq, pos.name, s.generi(...) */
    INSERT INTO STEL_RPT_DATA_ROADSHOWACTUALS
        SELECT   /* ORIGSQL: select c.periodseq, pos.name, s.genericattribute4 as RSCode, s.genericattribute1(...) */
            c.periodseq,
            pos.name,
            s.genericattribute4 AS RSCode,
            s.genericattribute1 AS ShopCode,
            l.outputlabel AS reportcategory,
            l.outputlabel AS Product,
            c.genericattribute5,
            'ConnCount',
            SUM(c.genericnumber1)value
        FROM
            cs_measurement m
        INNER JOIN
            cs_pmcredittrace pm
            ON pm.measurementseq = m.measurementseq
        INNER JOIN
            cs_credit c
            ON c.creditseq = pm.creditseq
        INNER JOIN
            cs_salestransaction s
            ON s.salestransactionseq = c.salestransactionseq
        INNER JOIN
            cs_position pos
            ON pos.ruleelementownerseq = c.positionseq
            AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND s.compensationdate BETWEEN pos.effectivestartdate AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1)))   /* ORIGSQL: pos.effectiveenddate-1 */
        INNER JOIN
            ext.stel_rpt_cfg_lookup l
            ON l.rpttype = 'Roadshow'
            AND l.inputname = m.name
        WHERE
            1 = 1--c.genericattribute14='Roadshow'
            AND c.salestransactionseq IN
            (
                SELECT   /* ORIGSQL: (select ta.salestransactionseq from cs_transactionassignment ta where ta.generic(...) */
                    ta.salestransactionseq
                FROM
                    cs_transactionassignment ta
                WHERE
                    ta.genericattribute2 = 'Roadshow'
                    AND ta.processingunitseq = :v_Pu
                    AND ta.tenantid = :v_Tenant
            )
            AND l.inputname LIKE '%ConnCount%' --[Arun 1st Apr 2019 - Added to just look at ConnCount and Not Points]
            AND pm.tenantid = m.tenantid
            AND pm.tenantid = c.tenantid
            AND pm.processingunitseq = m.processingunitseq
            AND pm.processingunitseq = c.processingunitseq
            AND pm.sourceperiodseq = c.periodseq
            AND pm.targetperiodseq = m.periodseq
            AND c.tenantid = s.tenantid
            AND c.processingunitseq = s.processingunitseq
            AND c.compensationdate = s.compensationdate
            AND c.periodseq = :in_PERIODSEQ
            AND m.periodseq = :in_PERIODSEQ
        GROUP BY
            c.periodseq, pos.name
            , s.genericattribute4
            , s.genericattribute1
            ,l.outputlabel
            , c.genericattribute5;

    /* ORIGSQL: commit; */
    COMMIT;
END