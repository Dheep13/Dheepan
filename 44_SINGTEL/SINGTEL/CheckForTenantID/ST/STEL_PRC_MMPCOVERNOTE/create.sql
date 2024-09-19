CREATE PROCEDURE EXT.STEL_PRC_MMPCOVERNOTE
(
    IN in_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_rpttype VARCHAR2 */
    IN IN_PERIODSEQ BIGINT,   /* ORIGSQL: IN_PERIODSEQ IN INTEGER */
    IN IN_PROCESSINGUNITSEQ BIGINT     /* ORIGSQL: IN_PROCESSINGUNITSEQ IN INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Tenant VARCHAR(10) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'STEL'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'STEL_PRC_MMPCOVERNOTE';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        CALENDARNAME,
        PERIODNAME
    INTO
        v_CalendarName,
        v_PeriodName
    FROM
        cs_periodcalendar
    WHERE
        PERIODSEQ = :IN_PERIODSEQ;

    /* ORIGSQL: DELETE STEL_RPT_MMPCOVERNOTE_TMP WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUN(...) */
    DELETE
    FROM
        STEL_RPT_MMPCOVERNOTE_TMP
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Deleting  table data 
    /* ORIGSQL: DELETE STEL_RPT_MMPCOVERNOTE WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSE(...) */
    DELETE
    FROM
        STEL_RPT_MMPCOVERNOTE
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_MMPCOVERNOTE') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'STEL_RPT_MMPCOVERNOTE');

    -- managing table partitions

    -- FTTH data insertion

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEPOSIT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_MMPCOVERNOTE_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, P(...) */
    INSERT INTO EXT.STEL_RPT_MMPCOVERNOTE_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            VENDOR_NAME,
            PRODUCT_NAME,---based on earninggroupid
            PAYOUT, --value
            CREATEDATE,--curenttimestamp
            reportperiodname,
            EARNINGCodeID,
            GROUPLABEL
        )
        
            SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, IN_PROCESSINGUNITSEQ, v_PeriodName, PRC.NAME, v_(...) */
                :v_Tenant,
                :IN_PERIODSEQ,
                :IN_PROCESSINGUNITSEQ,
                :v_PeriodName,
                PRC.NAME,
                :v_CalendarName,
                PMT.POSITIONSEQ,
                PMT.PAYEESEQ,
                PRT.LASTNAME AS VENDOR_NAME,
                CASE
                    WHEN PMT.EARNINGGROUPID LIKE '%DEL%'
                    THEN 'DEL Fixed Line VAS Incentive'
                    ELSE 'FTTH Activation Incentive'
                END,
                SUM(PMT.VALUE) AS VALUE,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                EARNINGGROUPID,
                CASE
                    WHEN EARNINGCodeID = 'Commission_Distributor Incentive'
                    -- THEN IFNULL(TO_VARCHAR(sapdbmtk.sp_f_dbmtk_truncate_datetime(prd.startdate, 'Q'),'Mon'),'')  /* ORIGSQL: TRUNC(prd.startdate, 'Q') */
                    --                                                                                                               /* ORIGSQL: TO_CHAR(TRUNC (prd.startdate, 'Q'), 'Mon') */
                    -- || ' - '
                    -- || IFNULL(TO_VARCHAR(sapdbmtk.sp_f_dbmtk_truncate_datetime(prd.startdate, 'MM'),'Mon'),'')  /* ORIGSQL: TRUNC(prd.startdate, 'MM') */
                    --                                                                                             /* ORIGSQL: TO_CHAR(TRUNC (prd.startdate, 'MM'), 'Mon') */
                    -- || ' '
                    -- || IFNULL(TO_VARCHAR(prd.startdate,'YYYY'),'')  /* ORIGSQL: TO_CHAR(prd.startdate, 'YYYY') */
                    THEN
                    UPPER(SUBSTRING(MONTHNAME(ext.trunc(prd.startdate, 'QUARTER'))|| ' '||EXTRACT_YEAR(ext.trunc('prd.startdate', 'QUARTER')),1,1))
                    ||
                    LOWER(SUBSTRING(MONTHNAME(ext.trunc(prd.startdate, 'QUARTER'))|| ' '||EXTRACT_YEAR(ext.trunc('prd.startdate', 'QUARTER')),2))
                    
                    
                    ELSE :v_PeriodName
                END,
                /* --POS.GENERICATTRIBUTE11 */
                lkp.StringValue
            FROM
                CS_PARTICIPANT PRT,
                CS_POSITION POS,
                CS_PERIOD PRD,
                CS_PROCESSINGUNIT PRC,
                -- CS_PAYMENT PMT
                CS_DEPOSIT PMT,
                (
                    SELECT   /* ORIGSQL: (SELECT dim0, stringvalue, effectivestartdate, effectiveenddate FROM stel_lookup(...) */
                        dim0,
                        stringvalue,
                        effectivestartdate,
                        effectiveenddate
                    FROM
                        EXT.STEL_lookup lk
                    WHERE
                        lk.name LIKE 'LT_Dealer_Channel Type'
                ) AS lkp
            WHERE
                PRT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRT.effectivestartdate <= prd.enddate
                AND PRT.effectiveenddate > prd.enddate
                AND pos.effectivestartdate <= prd.enddate
                AND pos.effectiveenddate > prd.enddate
                AND pos.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRT.PAYEESEQ = POS.PAYEESEQ
                --and lkp.dim0=pos.name
                AND lkp.dim0 = REPLACE(pos.name, ' - ', '-')
                AND lkp.effectivestartdate <= prd.enddate
                AND lkp.effectiveenddate > prd.enddate
                AND PRD.PERIODSEQ = :IN_PERIODSEQ
                AND PMT.PERIODSEQ = PRD.PERIODSEQ
                AND PMT.EARNINGGROUPID IN
                ('Commission_MMP_FTTH & VAS',
                    'Commission_MMP_FTTH & VAS',
                    'Commission_SER',
                    --,'Commission_MMP_DEL VAS',
                    'Commission',
                    -- 'Commission_MMP_MioTV',
                'Commission_MMP_Smart Home')
                AND PRC.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                AND PRC.PROCESSINGUNITSEQ = PMT.PROCESSINGUNITSEQ
                AND PRT.PAYEESEQ = PMT.PAYEESEQ
            GROUP BY
                :v_Tenant,
                :IN_PERIODSEQ,
                :IN_PROCESSINGUNITSEQ,
                :v_PeriodName,
                PRC.NAME,
                :v_CalendarName,
                PMT.POSITIONSEQ,
                PMT.PAYEESEQ,
                PRT.LASTNAME,
                EARNINGGROUPID,
                CASE
                    WHEN EARNINGCodeID = 'Commission_Distributor Incentive'
                    -- THEN IFNULL(TO_VARCHAR(sapdbmtk.sp_f_dbmtk_truncate_datetime(prd.startdate, 'Q'),'Mon'),'')  /* ORIGSQL: TRUNC(prd.startdate, 'Q') */
                    --                                                                                                               /* ORIGSQL: TO_CHAR(TRUNC (prd.startdate, 'Q'), 'Mon') */
                    -- || ' - '
                    -- || IFNULL(TO_VARCHAR(sapdbmtk.sp_f_dbmtk_truncate_datetime(prd.startdate, 'MM'),'Mon'),'')  /* ORIGSQL: TRUNC(prd.startdate, 'MM') */
                    --                                                                                             /* ORIGSQL: TO_CHAR(TRUNC (prd.startdate, 'MM'), 'Mon') */
                    -- || ' '
                    -- || IFNULL(TO_VARCHAR(prd.startdate,'YYYY'),'')  /* ORIGSQL: TO_CHAR(prd.startdate, 'YYYY') */
                    THEN
                    UPPER(SUBSTRING(MONTHNAME(ext.trunc(prd.startdate, 'QUARTER'))|| ' '||EXTRACT_YEAR(ext.trunc('prd.startdat', 'QUARTER')),1,1))
                    ||
                    LOWER(SUBSTRING(MONTHNAME(ext.trunc(prd.startdate, 'QUARTER'))|| ' '||EXTRACT_YEAR(ext.trunc('prd.startdat', 'QUARTER')),2))
                    ELSE :v_PeriodName
                END,
                --POS.GENERICATTRIBUTE11
                lkp.stringvalue,
                PMT.EARNINGCODEID,
                prd.startdate
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --DEL VAS 
    /* ORIGSQL: INSERT INTO STEL_RPT_MMPCOVERNOTE_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, P(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PMCREDITTRACE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    INSERT INTO EXT.STEL_RPT_MMPCOVERNOTE_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            VENDOR_NAME,
            PRODUCT_NAME,
            PAYOUT,
            CREATEDATE,
            reportperiodname,
            EARNINGCodeID,
            GROUPLABEL
        )
        SELECT   /* ORIGSQL: SELECT v_Tenant, IN_PERIODSEQ, IN_PROCESSINGUNITSEQ, v_PeriodName, 'Singtel_PU',(...) */
            :v_Tenant,
            :IN_PERIODSEQ,
            :IN_PROCESSINGUNITSEQ,
            :v_PeriodName,
            'Singtel_PU',
            :v_CalendarName,
            c.POSITIONSEQ,
            c.PAYEESEQ,
            Par.LASTNAME AS VENDOR_NAME,
            CASE
                WHEN c.genericattribute16 = 'DEL'
                THEN 'Fixed Line VAS (DEL)'
                WHEN c.genericattribute16 = 'HDL'
                THEN 'Fixed Line VAS (MioVoice)'
                ELSE 'Product not found'
            END,
            SUM(c.VALUE) AS VALUE,
            CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
            'Commission_MMP_DEL VAS' AS EARNINGGROUPID,
            pd.name AS v_periodName,
            /* --POS.GENERICATTRIBUTE11 */
            lkp.StringValue
        FROM
            cs_credit c
        INNER JOIN
            cs_pmcredittrace pmt
            ON pmt.creditseq = c.creditseq
        INNER JOIN
            cs_measurement m
            ON m.measurementseq = pmt.measurementseq
        INNER JOIN
            cs_period pd
            ON pd.periodseq = c.periodseq
            AND pd.periodseq = m.periodseq
            AND pd.periodseq = pmt.targetperiodseq
        INNER JOIN
            cs_position pos
            ON pos.ruleelementownerseq = c.positionseq
            AND pos.removedate = :v_eot
            AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN pos.effectivestartdate AND TO_DATE(ADD_SECONDS(pos.effectiveenddate,(86400*-1)))   /* ORIGSQL: pos.effectiveenddate - 1 */
                                                                                                                                                       /* ORIGSQL: pd.enddate - 1 */
        INNER JOIN
            cs_participant par
            ON par.payeeseq = m.payeeseq
            AND par.removedate = :v_eot
            AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: pd.enddate - 1 */
                                                                                                                                                       /* ORIGSQL: par.effectiveenddate - 1 */
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (SELECT dim0, stringvalue, effectivestartdate, effectiveenddate FROM stel_lookup(...) */
                    dim0,
                    stringvalue,
                    effectivestartdate,
                    effectiveenddate
                FROM
                    EXT.STEL_lookup lk
                WHERE
                    lk.name LIKE 'LT_Dealer_Channel Type'
            ) AS lkp
            ON lkp.dim0 = REPLACE(pos.name, ' - ', '-')
            AND lkp.effectivestartdate <= pd.enddate
            AND lkp.effectiveenddate > pd.enddate
        WHERE
            m.name = 'PM_MMP_DEL Fixed Line VAS'
            --and c.genericattribute16 in ( 'DEL','HDL')
            AND pd.periodseq = :IN_PERIODSEQ
            AND c.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND m.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND pmt.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND m.positionseq = c.positionseq
            AND m.tenantid = :v_Tenant
            AND c.tenantid = :v_Tenant
            AND pmt.tenantid = :v_Tenant
        GROUP BY
            :v_Tenant,
            :IN_PERIODSEQ,
            :IN_PROCESSINGUNITSEQ,
            :v_PeriodName,
            'Singtel_PU',
            :v_CalendarName,
            c.POSITIONSEQ,
            c.PAYEESEQ,
            Par.LASTNAME,
            CASE
                WHEN c.genericattribute16 = 'DEL'
                THEN 'Fixed Line VAS (DEL)'
                WHEN c.genericattribute16 = 'HDL'
                THEN 'Fixed Line VAS (MioVoice)'
                ELSE 'Product not found'
            END,
            CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
            'Commission_MMP_DEL VAS',
            pd.name,
            --POS.GENERICATTRIBUTE11
            lkp.StringValue;

    -- MioTV data insertion      

    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_MMPCOVERNOTE_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, P(...) */
    INSERT INTO EXT.STEL_RPT_MMPCOVERNOTE_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            VENDOR_NAME,
            PRODUCT_NAME,
            PAYOUT,
            CREATEDATE,
            EARNINGCodeID,
            GROUPLABEL
        )
        
            SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, IN_PROCESSINGUNITSEQ, v_PeriodName, NAME, v_Cale(...) */
                :v_Tenant,/* -- Modified by sudhir/gopi defect #97 */  :IN_PERIODSEQ,
                :IN_PROCESSINGUNITSEQ,
                :v_PeriodName,
                NAME,
                :v_CalendarName,
                POSITIONSEQ,
                PAYEESEQ,
                VENDOR_NAME,
                'Singtel TV Activation Incentive',
                SUM(VALUE),
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                EARNINGGROUPID,
                stringValue
            FROM
                (
                    SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, IN_PROCESSINGUNITSEQ, v_PeriodName, PRC.NAME, v_(...) */
                        :v_Tenant,
                        :IN_PERIODSEQ,
                        :IN_PROCESSINGUNITSEQ,
                        :v_PeriodName,
                        PRC.NAME,
                        :v_CalendarName,
                        PMT.POSITIONSEQ,
                        PMT.PAYEESEQ,
                        PRT.LASTNAME AS VENDOR_NAME,
                        'Singtel TV Activation Incentive',
                        PMT.VALUE,
                        CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                        EARNINGGROUPID,
                        /* -- POS.GENERICATTRIBUTE11 */
                        lkp.stringValue
                    FROM
                        CS_PARTICIPANT PRT,
                        CS_POSITION POS,
                        CS_PERIOD PRD,
                        CS_PROCESSINGUNIT PRC,
                        CS_DEPOSIT PMT,
                        (
                            SELECT   /* ORIGSQL: (SELECT dim0, stringvalue, effectivestartdate, effectiveenddate FROM stel_lookup(...) */
                                dim0,
                                stringvalue,
                                effectivestartdate,
                                effectiveenddate
                            FROM
                                EXT.STEL_lookup lk
                            WHERE
                                lk.name LIKE 'LT_Dealer_Channel Type'
                        ) AS lkp
                    WHERE
                        PRT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                        AND PRT.effectivestartdate <= prd.enddate
                        AND PRT.effectiveenddate > prd.enddate
                        AND pos.effectivestartdate <= prd.enddate
                        AND pos.effectiveenddate > prd.enddate
                        AND pos.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                        AND PRT.PAYEESEQ = POS.PAYEESEQ
                        AND lkp.dim0 = REPLACE(pos.name, ' - ', '-')
                        --and lkp.dim0=pos.name
                        AND lkp.effectivestartdate <= prd.enddate
                        AND lkp.effectiveenddate > prd.enddate
                        AND PRD.PERIODSEQ = :IN_PERIODSEQ
                        AND PMT.PERIODSEQ = PRD.PERIODSEQ
                        AND PMT.EARNINGGROUPID IN ('Commission_MMP_MioTV')
                        AND PRC.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                        AND PRC.PROCESSINGUNITSEQ = PMT.PROCESSINGUNITSEQ
                        AND PRT.PAYEESEQ = PMT.PAYEESEQ
                    GROUP BY
                        :v_Tenant,
                        :IN_PERIODSEQ,
                        :IN_PROCESSINGUNITSEQ,
                        :v_PeriodName,
                        PRC.NAME,
                        :v_CalendarName,
                        PMT.POSITIONSEQ,
                        PMT.PAYEESEQ,
                        PRT.LASTNAME,
                        PMT.VALUE,
                        EARNINGGROUPID,
                        --POS.GENERICATTRIBUTE11
                        lkp.stringValue
                ) AS dbmtk_corrname_23977
            GROUP BY
                :v_Tenant,
                :IN_PERIODSEQ,
                :IN_PROCESSINGUNITSEQ,
                :v_PeriodName,
                NAME,
                :v_CalendarName,
                POSITIONSEQ,
                PAYEESEQ,
                VENDOR_NAME,
                EARNINGGROUPID,
                stringValue
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Integrated Sales Incentivedata insertion    

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_INCENTIVE' not found */
    /* ORIGSQL: INSERT INTO EXT.STEL_RPT_MMPCOVERNOTE_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, P(...) */
    INSERT INTO EXT.STEL_RPT_MMPCOVERNOTE_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            VENDOR_NAME,
            PRODUCT_NAME,
            PAYOUT,
            CREATEDATE,
            EARNINGCodeID,
            GROUPLABEL
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, IN_PROCESSINGUNITSEQ, v_PeriodName, PRC.NAME, v_(...) */
            :v_Tenant,
            :IN_PERIODSEQ,
            :IN_PROCESSINGUNITSEQ,
            :v_PeriodName,
            PRC.NAME,
            :v_CalendarName,
            PMT.POSITIONSEQ,
            PMT.PAYEESEQ,
            PRT.LASTNAME AS VENDOR_NAME,
            'Singtel TV, FTTH & Mobile Vas',
            SUM(PMT.VALUE) AS VALUE,
            CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
            'Singtel TV, FTTH & Mobile Vas',
            /* -- POS.GENERICATTRIBUTE11 */
            lkp.stringValue
        FROM
            CS_PARTICIPANT PRT,
            CS_POSITION POS,
            CS_PERIOD PRD,
            CS_PROCESSINGUNIT PRC,
            CS_INCENTIVE PMT,
            (
                SELECT   /* ORIGSQL: (SELECT dim0, stringvalue, effectivestartdate, effectiveenddate FROM stel_lookup(...) */
                    dim0,
                    stringvalue,
                    effectivestartdate,
                    effectiveenddate
                FROM
                    EXT.STEL_lookup lk
                WHERE
                    lk.name LIKE 'LT_Dealer_Channel Type'
                    AND dim0 IN('D2615','D2643')
            ) AS lkp
        WHERE
            PRT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRT.effectivestartdate <= prd.enddate
            AND PRT.effectiveenddate > prd.enddate
            AND pos.effectivestartdate <= prd.enddate
            AND pos.effectiveenddate > prd.enddate
            AND pos.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRT.PAYEESEQ = POS.PAYEESEQ
            --AND lkp.dim0 = REPLACE (pos.name, ' - ', '-')  --[arun/avinash - commented based on idscussion on 22Aug2019]
            --and lkp.dim0=pos.name
            AND lkp.effectivestartdate <= prd.enddate
            AND lkp.effectiveenddate > prd.enddate
            AND PRD.PERIODSEQ = :IN_PERIODSEQ
            AND PMT.PERIODSEQ = PRD.PERIODSEQ
            -- AND PMT.EARNINGGROUPID IN ('Commission_MMP_DH Telesales_Bonus')
            AND PMT.GENERICATTRIBUTE1 IN
            ('MMP_DH Telesales_North Star Integrated Team_Bonus','MMP_DH Telesales_Telecontinent Integrated Team_Bonus')
            AND PRC.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            AND PRC.PROCESSINGUNITSEQ = PMT.PROCESSINGUNITSEQ
            AND PRT.PAYEESEQ = PMT.PAYEESEQ
        GROUP BY
            :v_Tenant,
            :IN_PERIODSEQ,
            :IN_PROCESSINGUNITSEQ,
            :v_PeriodName,
            PRC.NAME,
            :v_CalendarName,
            PMT.POSITIONSEQ,
            PMT.PAYEESEQ,
            PRT.LASTNAME,
            --POS.GENERICATTRIBUTE11
            lkp.stringValue
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Adding summary records into final reporting table.
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_MMPCOVERNOTE_TMP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_MMPCOVERNOTE' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_MMPCOVERNOTE (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIO(...) */
    INSERT INTO EXT.STEL_RPT_MMPCOVERNOTE
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            VENDOR_NAME,
            PRODUCT_NAME,
            PAYOUT,
            GROUPLABEL
        )
        SELECT   /* ORIGSQL: (SELECT TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIODNAME, PROCESSINGUNITNAME, (...) */
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            VENDOR_NAME,
            PRODUCT_NAME,
            SUM(PAYOUT) AS PAYOUT,
            GROUPLABEL
        FROM
            EXT.STEL_RPT_MMPCOVERNOTE_TMP
        WHERE
            PERIODSEQ = :IN_PERIODSEQ
            AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
        GROUP BY
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            VENDOR_NAME,
            PRODUCT_NAME,
            GROUPLABEL
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END