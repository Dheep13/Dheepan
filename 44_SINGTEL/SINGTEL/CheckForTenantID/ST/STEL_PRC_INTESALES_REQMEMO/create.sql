CREATE PROCEDURE EXT.STEL_PRC_INTESALES_REQMEMO
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
    DECLARE v_Tenant VARCHAR(10) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR2(10) := 'STEL'; */
    DECLARE v_StMsg VARCHAR(10) = 'START';  /* ORIGSQL: v_StMsg VARCHAR2(10) := 'START'; */
    DECLARE v_EdMsg VARCHAR(10) = 'END';  /* ORIGSQL: v_EdMsg VARCHAR2(10) := 'END'; */
    DECLARE v_ComponentName VARCHAR(100) = NULL;  /* ORIGSQL: v_ComponentName VARCHAR2(100) := NULL; */
    DECLARE v_CalendarName VARCHAR(100) = NULL;  /* ORIGSQL: v_CalendarName VARCHAR2(100) := NULL; */
    DECLARE v_PeriodName VARCHAR(100) = NULL;  /* ORIGSQL: v_PeriodName VARCHAR2(100) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount INTEGER; */

    v_ComponentName = 'EXT.STEL_PRC_INTESALES_REQMEMO';

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

    -- Calling below procedure to populate dealer and vendor code data.

    /* ORIGSQL: execute immediate 'truncate table stel_Classifier_tab'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_CLASSIFIER_TAB' not found */

    /* ORIGSQL: truncate table stel_Classifier_tab ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.stel_Classifier_tab';

    /* ORIGSQL: insert into stel_Classifier_Tab select * from stel_classifier; */
    INSERT INTO ext.stel_Classifier_Tab
        SELECT   /* ORIGSQL: select * from stel_classifier; */
            *
        FROM
            ext.stel_classifier;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_PRC_MMPINTPARTPOS (IN_PERIODSEQ, IN_PROCESSINGUNITSEQ) */
    CALL EXT.STEL_PRC_MMPINTPARTPOS(:IN_PERIODSEQ, :IN_PROCESSINGUNITSEQ);

    --stel_ratetableRange ('RT_MMP_DH Telesales_Bonus');
    /* ORIGSQL: stel_ratetableRange ('RT_MMP_DH Telesales_NSI_Bonus_D2615',IN_PERIODSEQ) */
    CALL EXT.STEL_RATETABLERANGE('RT_MMP_DH Telesales_NSI_Bonus_D2615', :IN_PERIODSEQ);

    --kyap rate table name change
    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'End stel_ratetableRange') */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'End ext.stel_ratetableRange');

    -- Deleting  table data 
    /* ORIGSQL: DELETE STEL_RPT_INTESALES_REQMEMO WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSINGU(...) */
    DELETE
    FROM
        EXT.STEL_RPT_INTESALES_REQMEMO
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'End:DELETE STEL_RPT_INTES(...) */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'End:DELETE EXT.STEL_RPT_INTESALES_REQMEMO');

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_INTESALES_REQMEMO') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_INTESALES_REQMEMO');

    -- managing table partitions

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 1 Insert') */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 1 Insert');

    --- Section 1 insertion.

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDITTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_INTESALES_REQMEMO' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_INTESALES_REQMEMO (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
    INSERT INTO EXT.STEL_RPT_INTESALES_REQMEMO
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            PAYEENAME,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            MGRPOSITIONSEQ,
            MGRNAME,
            SECTIONNO,
            PRODUCT,
            CONNECTION,
            RPTTYPE,
            RPT_HDR_ATTN,
            RPT_FOOTER1,
            RPT_FOOTER2,
            REGNO,
            COMPANYNAME,
            CREATEDATE
        )
        
            SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, v_periodname, S1.PAYEESEQ, S1.PAYEENAME, S1.POSI(...) */
                :v_Tenant,
                :IN_PERIODSEQ,
                :v_PeriodName,
                S1.PAYEESEQ,
                S1.PAYEENAME,
                S1.POSITIONSEQ,
                S1.POSNAME,
                PRC.PROCESSINGUNITSEQ,
                PRC.NAME,
                :v_CalendarName,
                S1.MGRPOSITIONSEQ,
                S1.MGRNAME,
                1,
                /* --CRD.GENERICATTRIBUTE4 */
                /*  case when  CRD.GENERICATTRIBUTE4  IS NULL or crd.genericattribute4 in ('MOB','MBB','null')
                 then to_char(crd.genericattribute16)
                 else                 to_char(CRD.GENERICATTRIBUTE4 )
                 end*/
                crd.genericattribute10 AS PRODUCT,
                SUM(CRD.GENERICNUMBER2) AS CONNECTION,
                'Integrated Sales',
                CL.GENERICATTRIBUTE3,
                CL.GENERICATTRIBUTE4,
                CL.GENERICATTRIBUTE5,
                CL.GENERICATTRIBUTE1,
                CL.GENERICATTRIBUTE6,
                CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
            FROM
                CS_CREDIT CRD,
                CS_PROCESSINGUNIT PRC,
                cs_credittype ctype,

                --stel_classifier_tab prod,
                EXT.STEL_RPT_DATA_FTTHREQMEMO S1,
                (
                    SELECT   /* ORIGSQL: (SELECT genericattribute3, genericattribute4, genericattribute5, genericattribut(...) */
                        genericattribute3,
                        genericattribute4,
                        genericattribute5,
                        genericattribute6,
                        genericattribute1
                    FROM
                        ext.stel_classifier
                    WHERE
                        categorytreename = 'Reporting Config'
                        AND categoryname LIKE 'RequisitionMemo-MMP%'
                        AND classifierid LIKE 'SingTel TV, FTTH%Mobile Vas%'
                ) AS cl
            WHERE
                CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
                AND CRD.PAYEESEQ = S1.PAYEESEQ
                AND CRD.PERIODSEQ = S1.PERIODSEQ
                /*and prod.classifierid = crd.genericattribute15
                 and crd.compensationdate between prod.effectivestartdate and prod.effectiveenddate-1
                 and prod.categorytreename = 'Singtel' and prod.categoryname='PRODUCTS'
                                */
                AND CRD.POSITIONSEQ = S1.POSITIONSEQ
                AND CRD.PROCESSINGUNITSEQ = S1.PROCESSINGUNITSEQ
                AND S1.SALESCHANNEL = 'INT'
                AND ctype.credittypeid IN
                ('North Star Credit', 'North Star Credit Rolled')
                AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
                AND CRD.PERIODSEQ = :IN_PERIODSEQ
                AND CRD.processingunitseq = :IN_PROCESSINGUNITSEQ
            GROUP BY
                :v_Tenant,
                :IN_PERIODSEQ,
                :v_PeriodName,
                S1.PAYEESEQ,
                S1.PAYEENAME,
                S1.POSITIONSEQ,
                S1.POSNAME,
                PRC.PROCESSINGUNITSEQ,
                PRC.NAME,
                :v_CalendarName,
                S1.MGRPOSITIONSEQ,
                CL.GENERICATTRIBUTE3,
                CL.GENERICATTRIBUTE4,
                CL.GENERICATTRIBUTE5,
                CL.GENERICATTRIBUTE1,
                CL.GENERICATTRIBUTE6,
                S1.MGRNAME,
                --CRD.GENERICATTRIBUTE4
                crd.genericattribute10
                /*case when  CRD.GENERICATTRIBUTE4  IS NULL or crd.genericattribute4 in ('MOB','MBB','null')
                then to_char(crd.genericattribute16)
                else                 to_char(CRD.GENERICATTRIBUTE4 )
                end*/
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 2 Insert') */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 2 Insert');

    --- Section 2 insertion.    

    /* ORIGSQL: INSERT INTO STEL_RPT_INTESALES_REQMEMO (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
    INSERT INTO EXT.STEL_RPT_INTESALES_REQMEMO
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            PAYEENAME,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            MGRPOSITIONSEQ,
            MGRNAME,
            SECTIONNO,
            CONNECTION,
            RPTTYPE,
            RPT_HDR_ATTN,
            RPT_FOOTER1,
            RPT_FOOTER2,
            REGNO,
            COMPANYNAME,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, v_periodname, S1.PAYEESEQ, S1.PAYEENAME, S1.POSI(...) */
            :v_Tenant,
            :IN_PERIODSEQ,
            :v_PeriodName,
            S1.PAYEESEQ,
            S1.PAYEENAME,
            S1.POSITIONSEQ,
            S1.POSNAME,
            PRC.PROCESSINGUNITSEQ,
            PRC.NAME,
            :v_CalendarName,
            S1.MGRPOSITIONSEQ,
            S1.MGRNAME,
            2,
            SUM(MES.VALUE) AS CONNECTION,
            'Integrated Sales',
            CL.GENERICATTRIBUTE3,
            CL.GENERICATTRIBUTE4,
            CL.GENERICATTRIBUTE5,
            CL.GENERICATTRIBUTE1,
            CL.GENERICATTRIBUTE6,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        FROM
            CS_MEASUREMENT MES,
            CS_PROCESSINGUNIT PRC,
            EXT.STEL_RPT_DATA_FTTHREQMEMO S1,
            (
                SELECT   /* ORIGSQL: (SELECT genericattribute3, genericattribute4, genericattribute5, genericattribut(...) */
                    genericattribute3,
                    genericattribute4,
                    genericattribute5,
                    genericattribute6,
                    genericattribute1
                FROM
                    ext.stel_classifier
                WHERE
                    categorytreename = 'Reporting Config'
                    AND categoryname LIKE 'RequisitionMemo-MMP%'
                    AND classifierid LIKE 'SingTel TV, FTTH%Mobile Vas%'
            ) AS cl
        WHERE
            MES.PERIODSEQ = S1.PERIODSEQ
            AND MES.POSITIONSEQ = S1.POSITIONSEQ
            AND MES.PROCESSINGUNITSEQ = S1.PROCESSINGUNITSEQ
            AND PRC.PROCESSINGUNITSEQ = MES.PROCESSINGUNITSEQ
            AND S1.SALESCHANNEL = 'INT'
            AND MES.NAME =
            'PM_MMP_DH Telesales North Star Integrated Team_Total Line CT'
            AND MES.PERIODSEQ = :IN_PERIODSEQ
            AND MES.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND MES.VALUE <> 0
        GROUP BY
            :v_Tenant,
            :IN_PERIODSEQ,
            :v_PeriodName,
            S1.PAYEESEQ,
            S1.PAYEENAME,
            S1.POSITIONSEQ,
            S1.POSNAME,
            PRC.PROCESSINGUNITSEQ,
            PRC.NAME,
            :v_CalendarName,
            S1.MGRPOSITIONSEQ,
            CL.GENERICATTRIBUTE3,
            CL.GENERICATTRIBUTE4,
            CL.GENERICATTRIBUTE5,
            CL.GENERICATTRIBUTE1,
            CL.GENERICATTRIBUTE6,
            S1.MGRNAME
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 3 Update') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 3 Update');

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 3 Update') */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 3 Update');   

    /* ORIGSQL: UPDATE STEL_RPT_INTESALES_REQMEMO T1 SET T1.TOTALAMT = (SELECT SUM(INC.VALUE) FR(...) */
    UPDATE EXT.STEL_RPT_INTESALES_REQMEMO T1
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_INCENTIVE' not found */
        SET
        /* ORIGSQL: T1.TOTALAMT = */
        TOTALAMT = (
            SELECT   /* ORIGSQL: (SELECT SUM(INC.VALUE) FROM CS_INCENTIVE INC WHERE INC.GENERICATTRIBUTE1 = 'MMP_(...) */
                SUM(INC.VALUE)
            FROM
                CS_INCENTIVE INC
            WHERE
                INC.GENERICATTRIBUTE1 =
                'MMP_DH Telesales_North Star Integrated Team_Bonus'
                AND INC.PROCESSINGUNITSEQ = T1.PROCESSINGUNITSEQ
                AND T1.POSITIONSEQ = INC.POSITIONSEQ
                AND T1.PAYEESEQ = INC.PAYEESEQ
                AND T1.PERIODSEQ = INC.PERIODSEQ
        )
    WHERE
        T1.PERIODSEQ = :IN_PERIODSEQ
        AND T1.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Rate table data updation

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_RPT_INTESALES_RATEDTLS') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_RPT_INTESALES_RATEDTLS');

    -- managing table partitions

    -- Deleting  table data 
    /* ORIGSQL: DELETE STEL_RPT_INTESALES_RATEDTLS WHERE PERIODSEQ = IN_PERIODSEQ AND PROCESSING(...) */
    DELETE
    FROM
        EXT.STEL_RPT_INTESALES_RATEDTLS
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 4 Insert') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 4 Insert');

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 4 Insert') */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 4 Insert');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_MEASUREMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_FTTHREQMEMO' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RATETABLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_INTESALES_RATEDTLS' not found */

    /* ORIGSQL: INSERT INTO STEL_RPT_INTESALES_RATEDTLS (PERIODSEQ, PERIODNAME, PROCESSINGUNITSE(...) */
    INSERT INTO EXT.STEL_RPT_INTESALES_RATEDTLS
        (
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            TEXT,
            CONNECTIONS,
            PAYOUT,
            RATE
        )
        SELECT   /* ORIGSQL: (SELECT M.PERIODSEQ, v_PeriodName, M.PROCESSINGUNITSEQ, M.PAYEESEQ, M.POSITIONSE(...) */
            M.PERIODSEQ,
            :v_PeriodName,
            M.PROCESSINGUNITSEQ,
            M.PAYEESEQ,
            M.POSITIONSEQ,
            CASE
                WHEN operate = '<='
                THEN IFNULL(rangestart,'') || ' to '|| IFNULL(rangeend,'')
                ELSE IFNULL(rangestart,'') || ' and above' 
            END
            AS TEXT,
            CASE
                WHEN operate = '<='
                THEN LEAST(m.VALUE, rangeend) - rangestart
                ELSE m.VALUE - rangestart
            END
            AS CONNECTIONS,
            CASE
                WHEN operate = '<='
                THEN LEAST(m.VALUE, rangeend) - rangestart
                ELSE m.VALUE - rangestart
            END
            * rate AS Payout,
            A.RATE
        FROM
            ext.stel_Ratetable a
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (SELECT M.* FROM cs_measurement m, STEL_RPT_DATA_FTTHREQMEMO S1 WHERE m.name LIK(...) */
                    M.*
                FROM
                    cs_measurement m,
                    EXT.STEL_RPT_DATA_FTTHREQMEMO S1
                WHERE
                    m.name LIKE 'PM_MMP_DH Telesales North Star Integrated Team_Total Line CT'
                    AND m.VALUE <> 0
                    AND M.PAYEESEQ = S1.PAYEESEQ
                    AND M.POSITIONSEQ = S1.POSITIONSEQ
                    AND M.PERIODSEQ = S1.PERIODSEQ
                    AND m.periodseq = :IN_PERIODSEQ
                    AND M.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            ) AS m
            ON 1 = 1
        WHERE
            a.name = 'RT_MMP_DH Telesales_NSI_Bonus_D2615'
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 1 Complete' || v_EdMsg || ' Period(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 1 Complete'|| IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --- Block for other position. Arun change conditons from here AK 20.08.2019
    --Arun updated the below block with necessary changes [20-Aug-19]

    --stel_ratetableRange ('RT_MMP_DH Telesales_Bonus');
    /* ORIGSQL: stel_ratetableRange ('RT_MMP_DH Telesales_TCI_Bonus_D2643',IN_PERIODSEQ) */
    CALL EXT.STEL_RATETABLERANGE('RT_MMP_DH Telesales_TCI_Bonus_D2643', :IN_PERIODSEQ);

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 2 Telesales Insert 1') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 2 Telesales Insert 1');

    --- Section 1 insertion.
    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 2 Telesales Inser(...) */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 2 Telesales Insert 1');     

    /* ORIGSQL: INSERT INTO STEL_RPT_INTESALES_REQMEMO (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
    INSERT INTO EXT.STEL_RPT_INTESALES_REQMEMO
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            PAYEENAME,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            MGRPOSITIONSEQ,
            MGRNAME,
            SECTIONNO,
            PRODUCT,
            CONNECTION,
            RPTTYPE,
            RPT_HDR_ATTN,
            RPT_FOOTER1,
            RPT_FOOTER2,
            REGNO,
            COMPANYNAME,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, v_periodname, S1.PAYEESEQ, S1.PAYEENAME, S1.POSI(...) */
            :v_Tenant,
            :IN_PERIODSEQ,
            :v_PeriodName,
            S1.PAYEESEQ,
            S1.PAYEENAME,
            S1.POSITIONSEQ,
            S1.POSNAME,
            PRC.PROCESSINGUNITSEQ,
            PRC.NAME,
            :v_CalendarName,
            S1.MGRPOSITIONSEQ,
            S1.MGRNAME,
            1,
            /* --CRD.GENERICATTRIBUTE4 */
            /*  case when  CRD.GENERICATTRIBUTE4  IS NULL or crd.genericattribute4 in ('MOB','MBB','null')
             then to_char(crd.genericattribute16)
             else                 to_char(CRD.GENERICATTRIBUTE4 )
             end*/
            crd.genericattribute10 AS PRODUCT,
            SUM(CRD.GENERICNUMBER2) AS CONNECTION,
            'Telecontinent Sales',
            CL.GENERICATTRIBUTE3,
            CL.GENERICATTRIBUTE4,
            CL.GENERICATTRIBUTE5,
            CL.GENERICATTRIBUTE1,
            CL.GENERICATTRIBUTE6,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        FROM
            CS_CREDIT CRD,
            CS_PROCESSINGUNIT PRC,
            cs_credittype ctype,

            --stel_classifier_tab prod,
            EXT.STEL_RPT_DATA_FTTHREQMEMO S1,
            (
                SELECT   /* ORIGSQL: (SELECT genericattribute3, genericattribute4, genericattribute5, genericattribut(...) */
                    genericattribute3,
                    genericattribute4,
                    genericattribute5,
                    genericattribute6,
                    genericattribute1
                FROM
                    ext.stel_classifier
                WHERE
                    categorytreename = 'Reporting Config'
                    AND categoryname LIKE 'RequisitionMemo-MMP%'
                    AND classifierid LIKE 'SingTel TV, FTTH%Mobile Vas%'
            ) AS cl
        WHERE
            CRD.CREDITTYPESEQ = CTYPE.DATATYPESEQ
            AND CRD.PAYEESEQ = S1.PAYEESEQ
            AND CRD.PERIODSEQ = S1.PERIODSEQ
            /*and prod.classifierid = crd.genericattribute15
             and crd.compensationdate between prod.effectivestartdate and prod.effectiveenddate-1
             and prod.categorytreename = 'Singtel' and prod.categoryname='PRODUCTS'
                            */
            AND CRD.POSITIONSEQ = S1.POSITIONSEQ
            AND CRD.PROCESSINGUNITSEQ = S1.PROCESSINGUNITSEQ
            AND S1.SALESCHANNEL = 'INT'
            AND ctype.credittypeid IN
            ('Telecontinent Credit', 'Telecontinent Credit Rolled')
            AND CTYPE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRC.PROCESSINGUNITSEQ = CRD.PROCESSINGUNITSEQ
            AND CRD.PERIODSEQ = :IN_PERIODSEQ
            AND CRD.processingunitseq = :IN_PROCESSINGUNITSEQ
        GROUP BY
            :v_Tenant,
            :IN_PERIODSEQ,
            :v_PeriodName,
            S1.PAYEESEQ,
            S1.PAYEENAME,
            S1.POSITIONSEQ,
            S1.POSNAME,
            PRC.PROCESSINGUNITSEQ,
            PRC.NAME,
            :v_CalendarName,
            S1.MGRPOSITIONSEQ,
            CL.GENERICATTRIBUTE3,
            CL.GENERICATTRIBUTE4,
            CL.GENERICATTRIBUTE5,
            CL.GENERICATTRIBUTE1,
            CL.GENERICATTRIBUTE6,
            S1.MGRNAME,
            --CRD.GENERICATTRIBUTE4
            crd.genericattribute10
            /*case when  CRD.GENERICATTRIBUTE4  IS NULL or crd.genericattribute4 in ('MOB','MBB','null')
            then to_char(crd.genericattribute16)
            else                 to_char(CRD.GENERICATTRIBUTE4 )
            end*/
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 2 Telesales Insert 2') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 2 Telesales Insert 2');

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 2 Telesales Inser(...) */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 2 Telesales Insert 2');

    --- Section 2 insertion.    

    /* ORIGSQL: INSERT INTO STEL_RPT_INTESALES_REQMEMO (TENANTID, PERIODSEQ, PERIODNAME, PAYEESE(...) */
    INSERT INTO EXT.STEL_RPT_INTESALES_REQMEMO
        (
            TENANTID,
            PERIODSEQ,
            PERIODNAME,
            PAYEESEQ,
            PAYEENAME,
            POSITIONSEQ,
            POSITIONNAME,
            PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME,
            CALENDARNAME,
            MGRPOSITIONSEQ,
            MGRNAME,
            SECTIONNO,
            CONNECTION,
            RPTTYPE,
            RPT_HDR_ATTN,
            RPT_FOOTER1,
            RPT_FOOTER2,
            REGNO,
            COMPANYNAME,
            CREATEDATE
        )
        SELECT   /* ORIGSQL: (SELECT v_Tenant, IN_PERIODSEQ, v_periodname, S1.PAYEESEQ, S1.PAYEENAME, S1.POSI(...) */
            :v_Tenant,
            :IN_PERIODSEQ,
            :v_PeriodName,
            S1.PAYEESEQ,
            S1.PAYEENAME,
            S1.POSITIONSEQ,
            S1.POSNAME,
            PRC.PROCESSINGUNITSEQ,
            PRC.NAME,
            :v_CalendarName,
            S1.MGRPOSITIONSEQ,
            S1.MGRNAME,
            2,
            SUM(MES.VALUE) AS CONNECTION,
            'Telecontinent Sales',
            CL.GENERICATTRIBUTE3,
            CL.GENERICATTRIBUTE4,
            CL.GENERICATTRIBUTE5,
            CL.GENERICATTRIBUTE1,
            CL.GENERICATTRIBUTE6,
            CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
        FROM
            CS_MEASUREMENT MES,
            CS_PROCESSINGUNIT PRC,
            EXT.STEL_RPT_DATA_FTTHREQMEMO S1,
            (
                SELECT   /* ORIGSQL: (SELECT genericattribute3, genericattribute4, genericattribute5, genericattribut(...) */
                    genericattribute3,
                    genericattribute4,
                    genericattribute5,
                    genericattribute6,
                    genericattribute1
                FROM
                    ext.stel_classifier
                WHERE
                    categorytreename = 'Reporting Config'
                    AND categoryname LIKE 'RequisitionMemo-MMP%'
                    AND classifierid LIKE 'SingTel TV, FTTH%Mobile Vas%'
            ) AS cl
        WHERE
            MES.PERIODSEQ = S1.PERIODSEQ
            AND MES.POSITIONSEQ = S1.POSITIONSEQ
            AND MES.PROCESSINGUNITSEQ = S1.PROCESSINGUNITSEQ
            AND PRC.PROCESSINGUNITSEQ = MES.PROCESSINGUNITSEQ
            AND S1.SALESCHANNEL = 'INT'
            AND MES.NAME =
            'PM_MMP_DH Telesales Telecontinent Integrated Team_Total Line CT'
            AND MES.PERIODSEQ = :IN_PERIODSEQ
            AND MES.processingunitseq = :IN_PROCESSINGUNITSEQ
            AND MES.VALUE <> 0
        GROUP BY
            :v_Tenant,
            :IN_PERIODSEQ,
            :v_PeriodName,
            S1.PAYEESEQ,
            S1.PAYEENAME,
            S1.POSITIONSEQ,
            S1.POSNAME,
            PRC.PROCESSINGUNITSEQ,
            PRC.NAME,
            :v_CalendarName,
            S1.MGRPOSITIONSEQ,
            CL.GENERICATTRIBUTE3,
            CL.GENERICATTRIBUTE4,
            CL.GENERICATTRIBUTE5,
            CL.GENERICATTRIBUTE1,
            CL.GENERICATTRIBUTE6,
            S1.MGRNAME
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 2 Telesales Update 3') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 2 Telesales Update 3');

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 2 Telesales Inser(...) */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 2 Telesales Insert 3');   

    /* ORIGSQL: UPDATE STEL_RPT_INTESALES_REQMEMO T1 SET T1.TOTALAMT = (SELECT SUM(INC.VALUE) FR(...) */
    UPDATE EXT.STEL_RPT_INTESALES_REQMEMO T1 
        SET
        /* ORIGSQL: T1.TOTALAMT = */
        TOTALAMT = (
            SELECT   /* ORIGSQL: (SELECT SUM(INC.VALUE) FROM CS_INCENTIVE INC WHERE INC.GENERICATTRIBUTE1 = 'MMP_(...) */
                SUM(INC.VALUE)
            FROM
                CS_INCENTIVE INC
            WHERE
                INC.GENERICATTRIBUTE1 =
                'MMP_DH Telesales_Telecontinent Integrated Team_Bonus'
                AND INC.PROCESSINGUNITSEQ = T1.PROCESSINGUNITSEQ
                AND T1.POSITIONSEQ = INC.POSITIONSEQ
                AND T1.PAYEESEQ = INC.PAYEESEQ
                AND T1.PERIODSEQ = INC.PERIODSEQ
        )
    WHERE
        T1.PERIODSEQ = :IN_PERIODSEQ
        AND T1.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 2 Telesales Insert 4') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 2 Telesales Insert 4');

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 2 Telesales Inser(...) */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 2 Telesales Insert 4');    

    /* ORIGSQL: INSERT INTO STEL_RPT_INTESALES_RATEDTLS (PERIODSEQ, PERIODNAME, PROCESSINGUNITSE(...) */
    INSERT INTO EXT.STEL_RPT_INTESALES_RATEDTLS
        (
            PERIODSEQ,
            PERIODNAME,
            PROCESSINGUNITSEQ,
            PAYEESEQ,
            POSITIONSEQ,
            TEXT,
            CONNECTIONS,
            PAYOUT,
            RATE
        )
        SELECT   /* ORIGSQL: (SELECT M.PERIODSEQ, v_PeriodName, M.PROCESSINGUNITSEQ, M.PAYEESEQ, M.POSITIONSE(...) */
            M.PERIODSEQ,
            :v_PeriodName,
            M.PROCESSINGUNITSEQ,
            M.PAYEESEQ,
            M.POSITIONSEQ,
            CASE
                WHEN operate = '<='
                THEN IFNULL(rangestart,'') || ' to '|| IFNULL(rangeend,'')
                ELSE IFNULL(rangestart,'') || ' and above' 
            END
            AS TEXT,
            CASE
                WHEN operate = '<='
                THEN LEAST(m.VALUE, rangeend) - rangestart
                ELSE m.VALUE - rangestart
            END
            AS CONNECTIONS,
            CASE
                WHEN operate = '<='
                THEN LEAST(m.VALUE, rangeend) - rangestart
                ELSE m.VALUE - rangestart
            END
            * rate AS Payout,
            A.RATE
        FROM
            ext.stel_Ratetable a
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (SELECT M.* FROM cs_measurement m, STEL_RPT_DATA_FTTHREQMEMO S1 WHERE m.name LIK(...) */
                    M.*
                FROM
                    cs_measurement m,
                    EXT.STEL_RPT_DATA_FTTHREQMEMO S1
                WHERE
                    m.name LIKE 'PM_MMP_DH Telesales Telecontinent Integrated Team_Total Line CT'
                    AND m.VALUE <> 0
                    AND M.PAYEESEQ = S1.PAYEESEQ
                    AND M.POSITIONSEQ = S1.POSITIONSEQ
                    AND M.PERIODSEQ = S1.PERIODSEQ
                    AND m.periodseq = :IN_PERIODSEQ
                    AND M.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            ) AS m
            ON 1 = 1
        WHERE
            a.name = 'RT_MMP_DH Telesales_TCI_Bonus_D2643'
    ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Add debug Log for Process END
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'Section 2 Telesales Update 6') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'Section 2 Telesales Update 6');

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'Section 2 Telesales updat(...) */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'Section 2 Telesales update 6');

    ----End of Block - Arun updated the below block with necessary changes [20-Aug-19]
    -- Added below block to remove -ve  connection and -ve payout values.   
    /* ORIGSQL: UPDATE STEL_RPT_INTESALES_RATEDTLS SET CONNECTIONS = 0, PAYOUT = 0 WHERE CONNECT(...) */
    UPDATE EXT.STEL_RPT_INTESALES_RATEDTLS
        SET
        /* ORIGSQL: CONNECTIONS = */
        CONNECTIONS = 0,
        /* ORIGSQL: PAYOUT = */
        PAYOUT = 0
    FROM
        EXT.STEL_RPT_INTESALES_RATEDTLS
    WHERE
        CONNECTIONS < 0;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, 'End Proc') */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, 'End Proc');

    /* ORIGSQL: stel_Sp_logger('STEL_PRC_INTESALES_REQMEMO','REP',0,0,'End Processing') */
    CALL EXT.STEL_SP_LOGGER('EXT.STEL_PRC_INTESALES_REQMEMO', 'REP', 0, 0, 'End Processing');
END