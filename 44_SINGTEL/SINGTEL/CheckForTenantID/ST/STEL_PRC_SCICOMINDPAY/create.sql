CREATE PROCEDURE EXT.STEL_PRC_SCICOMINDPAY
(
    IN IN_RPTTYPE VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: IN_RPTTYPE IN VARCHAR2 */
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
    DECLARE v_Tenant VARCHAR(10) = 'LGAP';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'LGAP'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'STEL_PRC_SCICOMINDPAY';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_CUSTOMERMASTER(in_periodseq, in_processingunitseq) */
    CALL EXT.STEL_CUSTOMERMASTER(:IN_PERIODSEQ, :IN_PROCESSINGUNITSEQ);

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

    --     CleanUp existing records for the Period 
    /* ORIGSQL: DELETE STEL_RPT_SCICOMINDPAY WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUNITSE(...) */
    DELETE
    FROM
        EXT.STEL_RPT_SCICOMINDPAY
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_SCICOMINDPAY') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_SCICOMINDPAY');

    -- managing table partitions

    -- Deleting temp table data 
    /* ORIGSQL: DELETE STEL_RPT_SCICOMINDPAY_TMP WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGUN(...) */
    DELETE
    FROM
        EXT.STEL_RPT_SCICOMINDPAY_TMP
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- MOBILE NEW Sales Matrix

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER_IP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* ORIGSQL: INSERT INTO STEL_RPT_SCICOMINDPAY_TMP (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, P(...) */
    INSERT INTO EXT.STEL_RPT_SCICOMINDPAY_TMP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPTITLE,
            SALESREPID,
            EVENTTYPE,
            SECTIONNO,
            PRODUCT,
            OTC,
            TARGET,
            SALES,
            OTC2,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
            T1.TENANTID,
            T1.PERIODSEQ,
            T1.PROCESSINGUNIT,
            T1.PERIODNAME,
            T1.PROCESSINGUNITNAME,
            :v_CalendarName,
            T1.POSITIONSEQ,
            T1.PAYEESEQ,
            T1.PAYEENAME,
            T1.TITLE,
            T1.SALESREPCODE,
            'PRODUCT DETAILS',
            1,
            mes.genericattribute1,
            mes.genericnumber2,
            mes.genericnumber4,
            mes.genericnumber1,
            mes.VALUE,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_POSPART_MASTER_IP T1,
            CS_MEASUREMENT MES
        WHERE
            T1.PERIODSEQ = :IN_PERIODSEQ
            AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
            AND T1.PERIODSEQ = MES.PERIODSEQ
            AND T1.POSITIONSEQ = MES.POSITIONSEQ
            AND T1.PAYEESEQ = MES.PAYEESEQ
            AND mes.genericattribute3 = 'SCICOM-AM/Mgr'
            AND mes.genericattribute2 = 'Target Comm'
            AND mes.genericattribute4 = 'SCICOM-AM'
            AND mes.genericattribute1 IN
            ('MOBILE NEW',
                'MOBILE RECON',
                'FIBRE NEW',
                'FIBRE MIGRATION',
                'FIBRE RECON',
                'SINGTEL TV NEW',
                'SINGTEL TV CONTENTS',
            'MOBILE VAS')
    ;

    /* ORIGSQL: commit; */
    COMMIT;

    -- MOBILE NEW Multiplier

    /*INSERT INTO STEL_RPT_SCICOMINDPAY_TMP (TENANTID,
                                            PERIODSEQ,
                                            PROCESSINGUNITSEQ,
                                            PERIODNAME,
                                            PROCESSINGUNITNAME,
                                            CALENDARNAME,
                                            POSITIONSEQ,
                                            PAYEESEQ,
                                            SALESREPNAME,
                                            SALESREPTITLE,
                                            SALESREPID,
                                            EVENTTYPE,
                                            SECTIONNO,
                                            PRODUCT,
                                            POINTS,
                                            PM_MIN1,
                                            PM_AVG1,
                                            PM_MAX1,
                                            PM_MIN2,
                                            PM_AVG2,
                                            PM_MAX2,
                                        CREATEDATE)*/ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_RPT_SCICOMINDPAY_TMP tgt using (SELECT T1.TENANTID, T1.PERIODSEQ(...) */
    MERGE INTO EXT.STEL_RPT_SCICOMINDPAY_TMP AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (SELECT T1.TENANTID, T1.PERIODSEQ, T1.PROCESSINGUNIT, T1.PERIODNAME, T1.PROCESSI(...) */
                T1.TENANTID,
                T1.PERIODSEQ,
                T1.PROCESSINGUNIT,
                T1.PERIODNAME,
                T1.PROCESSINGUNITNAME,
                :v_CalendarName,
                T1.POSITIONSEQ,
                T1.PAYEESEQ,
                T1.PAYEENAME,
                T1.TITLE,
                T1.SALESREPCODE,
                'PRODUCT DETAILS',
                1,
                mes.genericnumber1,
                TO_DECIMAL(REPLACE_REGEXPR('[A-Za-z_:=, %]' IN MES.GENERICATTRIBUTE5),38,18) AS GENERICATTRIBUTE5,  /* ORIGSQL: TO_NUMBER(REGEXP_REPLACE (MES.GENERICATTRIBUTE5, '[A-Za-z_:=, %]')) */
                                                                                                                                    /* ORIGSQL: REGEXP_REPLACE(MES.GENERICATTRIBUTE5, '[A-Za-z_:=, %]') */
                TO_DECIMAL(REPLACE_REGEXPR('[A-Za-z_:=, %]' IN MES.GENERICATTRIBUTE6),38,18) AS GENERICATTRIBUTE6,  /* ORIGSQL: TO_NUMBER(REGEXP_REPLACE (MES.GENERICATTRIBUTE6, '[A-Za-z_:=, %]')) */
                                                                                                                                    /* ORIGSQL: REGEXP_REPLACE(MES.GENERICATTRIBUTE6, '[A-Za-z_:=, %]') */
                TO_DECIMAL(REPLACE_REGEXPR('[A-Za-z_:=, %]' IN MES.GENERICATTRIBUTE7),38,18) AS GENERICATTRIBUTE7,  /* ORIGSQL: TO_NUMBER(REGEXP_REPLACE (MES.GENERICATTRIBUTE7, '[A-Za-z_:=, %]')) */
                                                                                                                                    /* ORIGSQL: REGEXP_REPLACE(MES.GENERICATTRIBUTE7, '[A-Za-z_:=, %]') */
                mes.genericnumber6,
                mes.genericnumber2,
                mes.genericnumber3,
                mes.genericnumber5,
                CURRENT_TIMESTAMP,  /* ORIGSQL: SYSDATE */
                genericattribute1 AS PRODUCT,
                mes.value
            FROM
                EXT.STEL_POSPART_MASTER_IP T1,
                CS_MEASUREMENT MES
            WHERE
                T1.PERIODSEQ = :IN_PERIODSEQ
                AND T1.PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ
                AND T1.PERIODSEQ = MES.PERIODSEQ
                AND T1.POSITIONSEQ = MES.POSITIONSEQ
                AND T1.PAYEESEQ = MES.PAYEESEQ
                AND mes.genericattribute3 = 'SCICOM-AM/Mgr'
                AND mes.genericattribute2 = 'Multiplier'
                AND mes.genericattribute4 = 'SCICOM-AM'
                AND mes.genericattribute1 IN
                ('MOBILE NEW',
                    'MOBILE RECON',
                    'FIBRE NEW',
                    'FIBRE MIGRATION',
                    'FIBRE RECON',
                    'SINGTEL TV NEW',
                    'SINGTEL TV CONTENTS',
                'MOBILE VAS')
        ) AS src
        ON (src.positionseq = tgt.positionseq
            AND src.periodseq = tgt.periodseq
        AND src.product = tgt.product)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.points = src.genericnumber1,
            tgt.PM_MIN1 = src.genericattribute5,
            tgt.PM_AVG1 = src.genericattribute6,
            tgt.PM_MAX1 = src.genericattribute7,
            tgt.PM_MIN2 = src.genericnumber6,
            tgt.PM_AVG2 = src.genericnumber2,
            tgt.PM_MAX2 = src.genericnumber3,
            --  tgt.otc2 =src.value ,
            --  tgt.team =
            tgt.multiplier = src.genericnumber5,
            tgt.payout = tgt.otc2*(1+src.genericnumber5);

    /* ORIGSQL: commit; */
    COMMIT;

    -- Summing up individual values

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_SCICOMINDPAY_TMP' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_SCICOMINDPAY' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_SCICOMINDPAY (TENANTID, PERIODSEQ, PROCESSINGUNITSEQ, PERIO(...) */
    INSERT INTO EXT.STEL_RPT_SCICOMINDPAY
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPTITLE,
            SALESREPID,
            EVENTTYPE,
            SECTIONNO,
            PRODUCT,
            OTC,
            TARGET,
            SALES,
            POINTS,
            PM_MIN1,
            PM_AVG1,
            PM_MAX1,
            PM_MIN2,
            PM_AVG2,
            PM_MAX2,
            OTC2,
            MULTIPLIER, PAYOUT,
            CREATEDATE
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
            SALESREPNAME,
            SALESREPTITLE,
            SALESREPID,
            EVENTTYPE,
            SECTIONNO,
            PRODUCT,
            SUM(OTC),
            SUM(TARGET),
            SUM(SALES),
            SUM(POINTS),
            MAX(PM_MIN1),
            MAX(PM_AVG1),
            MAX(PM_MAX1),
            SUM(PM_MIN2),
            SUM(PM_AVG2),
            SUM(PM_MAX2),
            SUM(OTC2),
            SUM(MULTIPLIER),
            SUM(PAYOUT),
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        FROM
            EXT.STEL_RPT_SCICOMINDPAY_TMP
        GROUP BY
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNITSEQ,
            PERIODNAME,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            POSITIONSEQ,
            PAYEESEQ,
            SALESREPNAME,
            SALESREPTITLE,
            SALESREPID,
            EVENTTYPE,
            SECTIONNO,
            PRODUCT
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END