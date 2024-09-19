CREATE PROCEDURE EXT.STEL_CUSTOMERMASTER
(
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
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot CONSTANT DATE := TO_DATE('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'STEL_CUSTOMERMASTER';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: DELETE STEL_POSPART_MASTER WHERE PERIODSEQ = in_PeriodSeq AND PROCESSINGUNIT = I(...) */
    DELETE
    FROM
        EXT.STEL_POSPART_MASTER
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_POSPART_MASTER') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_POSPART_MASTER');

    -- managing table partitions

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BUSINESSUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER' not found */

    /* ORIGSQL: INSERT INTO STEL_POSPART_MASTER (TENANTID, PERIODSEQ, PROCESSINGUNIT, PERIODNAME(...) */
    INSERT INTO EXT.STEL_POSPART_MASTER
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNIT,
            PERIODNAME,
            PROCESSINGUNITNAME,
            POSITIONSEQ,
            MANAGERSEQ,
            PAYEESEQ,
            TITLESEQ,
            PAYEENAME,
            LINEMANAGER,
            JOININGDATE,
            TERMINATIONDATE,
            COUNTRY,
            TITLE,
            EMPCODE,
            SALESREPCODE,
            POSITIONNAME,
            CREATEDATE,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            SALARY,
            HIREDATE,
            PAYEEID
        )
        SELECT   /* ORIGSQL: (SELECT PRC.TENANTID, PRD.PERIODSEQ, PRC.PROCESSINGUNITSEQ, PRD.NAME PERIODNAME,(...) */
            PRC.TENANTID,
            PRD.PERIODSEQ,
            PRC.PROCESSINGUNITSEQ,
            PRD.NAME AS PERIODNAME,
            PRC.NAME AS PROCESSINGUNITNAME,
            TO_VARCHAR(POS.RULEELEMENTOWNERSEQ) AS POSITIONSEQ,  /* ORIGSQL: TO_CHAR(POS.RULEELEMENTOWNERSEQ) */
            TO_VARCHAR(POS.MANAGERSEQ,NULL) AS MANAGERSEQ,  /* ORIGSQL: TO_CHAR(POS.MANAGERSEQ) */
            TO_VARCHAR(PRT.PAYEESEQ,NULL) AS PAYEESEQ,  /* ORIGSQL: TO_CHAR(PRT.PAYEESEQ) */
            TO_VARCHAR(TLT.RULEELEMENTOWNERSEQ) AS TITLESEQ,  /* ORIGSQL: TO_CHAR(TLT.RULEELEMENTOWNERSEQ) */
            TRIM(IFNULL(PRT.FIRSTNAME,'') || ' ' || IFNULL(PRT.LASTNAME,'')) AS SALESREPNAME,
            TRIM(IFNULL(MGR.FIRSTNAME,'') || ' ' || IFNULL(MGR.LASTNAME,'')) AS LINEMANAGER,
            PRT.HIREDATE,
            PRT.TERMINATIONDATE,
            BUS.NAME AS COUNTRY,
            TLT.NAME AS JOBTITLE,
            PRT.GENERICATTRIBUTE2 AS EMPCODE,
            PRT.GENERICATTRIBUTE1 AS SALESREPCODE,
            POS.NAME AS POSITIONNAME,
            CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
            PRT.FIRSTNAME,
            PRT.MIDDLENAME,
            PRT.LASTNAME,
            PRT.SALARY,
            PRT.HIREDATE,
            PYE.PAYEEID
        FROM
            --   cs_credit crd,
            cs_period prd,
            cs_participant prt,
            cs_position pos,
            cs_processingunit prc,
            cs_payee pye,
            cs_businessunit bus,
            CS_TITLE TLT,
            CS_PARTICIPANT MGR,
            CS_POSITION MGRPOS
        WHERE
            POS.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRD.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PYE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND TLT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND MGR.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND MGRPOS.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND MGRPOS.effectivestartdate < prd.enddate
            AND MGRPOS.effectiveenddate >= prd.enddate
            AND MGR.effectivestartdate < prd.enddate
            AND MGR.effectiveenddate >= prd.enddate
            AND TLT.effectivestartdate < prd.enddate
            AND TLT.effectiveenddate >= prd.enddate
            AND POS.effectivestartdate < prd.enddate
            AND POS.effectiveenddate >= prd.enddate
            AND PRT.effectivestartdate < prd.enddate
            AND PRT.effectiveenddate >= prd.enddate
            AND PYE.effectivestartdate < prd.enddate
            AND PYE.effectiveenddate >= prd.enddate
            AND PRT.PAYEESEQ = PYE.PAYEESEQ
            AND PYE.BUSINESSUNITMAP = BUS.MASK
            AND PYE.PAYEESEQ = POS.PAYEESEQ
            AND TLT.RULEELEMENTOWNERSEQ = POS.TITLESEQ
            AND MGR.PAYEESEQ = MGRPOS.PAYEESEQ
            AND POS.MANAGERSEQ = MGRPOS.RULEELEMENTOWNERSEQ
            AND POS.MANAGERSEQ IS NOT NULL
            AND TLT.NAME LIKE 'Advt%'
            -- AND PYE.PAYEEID LIKE 'x%'
            -- AND PYE.PAYEEID NOT LIKE 'xx%'
            AND PRC.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            AND PRD.PERIODSEQ = :IN_PERIODSEQ
        UNION ALL
            SELECT   /* ORIGSQL: SELECT PRC.TENANTID, PRD.PERIODSEQ, PRC.PROCESSINGUNITSEQ, PRD.NAME PERIODNAME, (...) */
                PRC.TENANTID,
                PRD.PERIODSEQ,
                PRC.PROCESSINGUNITSEQ,
                PRD.NAME AS PERIODNAME,
                PRC.NAME AS PROCESSINGUNITNAME,
                TO_VARCHAR(POS.RULEELEMENTOWNERSEQ) AS POSITIONSEQ,  /* ORIGSQL: TO_CHAR(POS.RULEELEMENTOWNERSEQ) */
                TO_VARCHAR(POS.MANAGERSEQ,NULL) AS MANAGERSEQ,  /* ORIGSQL: TO_CHAR(POS.MANAGERSEQ) */
                TO_VARCHAR(PRT.PAYEESEQ,NULL) AS PAYEESEQ,  /* ORIGSQL: TO_CHAR(PRT.PAYEESEQ) */
                TO_VARCHAR(TLT.RULEELEMENTOWNERSEQ) AS TITLESEQ,  /* ORIGSQL: TO_CHAR(TLT.RULEELEMENTOWNERSEQ) */
                TRIM(IFNULL(PRT.FIRSTNAME,'') || ' ' || IFNULL(PRT.LASTNAME,'')) AS SALESREPNAME,
                NULL AS LINEMANAGERNAME,
                PRT.HIREDATE,
                PRT.TERMINATIONDATE,
                BUS.NAME AS COUNTRY,
                TLT.NAME AS JOBTITLE,
                PRT.GENERICATTRIBUTE2 AS EMPCODE,
                PRT.GENERICATTRIBUTE1 AS SALESREPCODE,
                POS.NAME AS POSITIONNAME,
                CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
                PRT.FIRSTNAME,
                PRT.MIDDLENAME,
                PRT.LASTNAME,
                PRT.SALARY,
                PRT.HIREDATE,
                PYE.PAYEEID
            FROM
                --   cs_credit crd,
                cs_period prd,
                cs_participant prt,
                cs_position pos,
                cs_processingunit prc,
                cs_payee pye,
                cs_businessunit bus,
                CS_TITLE TLT
            WHERE
                POS.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRD.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PYE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND TLT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND TLT.effectivestartdate < prd.enddate
                AND TLT.effectiveenddate >= prd.enddate
                AND POS.effectivestartdate < prd.enddate
                AND POS.effectiveenddate >= prd.enddate
                AND PRT.effectivestartdate < prd.enddate
                AND PRT.effectiveenddate >= prd.enddate
                AND PYE.effectivestartdate < prd.enddate
                AND PYE.effectiveenddate >= prd.enddate
                AND PRT.PAYEESEQ = PYE.PAYEESEQ
                AND PYE.BUSINESSUNITMAP = BUS.MASK
                AND PYE.PAYEESEQ = POS.PAYEESEQ
                AND TLT.RULEELEMENTOWNERSEQ = POS.TITLESEQ
                AND POS.MANAGERSEQ IS NULL
                AND TLT.NAME LIKE 'Advt%'
                -- AND PYE.PAYEEID LIKE 'x%'
                -- AND PYE.PAYEEID NOT LIKE 'xx%'
                AND PRC.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                AND PRD.PERIODSEQ = :IN_PERIODSEQ
        ;

    --- Below code is for IP users required for Individual payment reports 
    /* ORIGSQL: DELETE STEL_POSPART_MASTER_IP WHERE PERIODSEQ = in_PeriodSeq AND PROCESSINGUNIT (...) */
    DELETE
    FROM
        EXT.STEL_POSPART_MASTER_IP
    WHERE
        PERIODSEQ = :IN_PERIODSEQ
        AND PROCESSINGUNIT = :IN_PROCESSINGUNITSEQ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_PROC_RPT_partitions (IN_PERIODSEQ, 'STEL_POSPART_MASTER_IP') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:IN_PERIODSEQ, 'EXT.STEL_POSPART_MASTER_IP');

    -- managing table partitions         

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_POSPART_MASTER_IP' not found */

    /* ORIGSQL: INSERT INTO STEL_POSPART_MASTER_IP (TENANTID, PERIODSEQ, PROCESSINGUNIT, PERIODN(...) */
    INSERT INTO EXT.STEL_POSPART_MASTER_IP
        (
            TENANTID,
            PERIODSEQ,
            PROCESSINGUNIT,
            PERIODNAME,
            PROCESSINGUNITNAME,
            POSITIONSEQ,
            MANAGERSEQ,
            PAYEESEQ,
            TITLESEQ,
            PAYEENAME,
            LINEMANAGER,
            JOININGDATE,
            TERMINATIONDATE,
            COUNTRY,
            TITLE,
            EMPCODE,
            SALESREPCODE,
            POSITIONNAME,
            CREATEDATE,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            SALARY,
            HIREDATE,
            PAYEEID
        )
        SELECT   /* ORIGSQL: (SELECT PRC.TENANTID, PRD.PERIODSEQ, PRC.PROCESSINGUNITSEQ, PRD.NAME PERIODNAME,(...) */
            PRC.TENANTID,
            PRD.PERIODSEQ,
            PRC.PROCESSINGUNITSEQ,
            PRD.NAME AS PERIODNAME,
            PRC.NAME AS PROCESSINGUNITNAME,
            TO_VARCHAR(POS.RULEELEMENTOWNERSEQ) AS POSITIONSEQ,  /* ORIGSQL: TO_CHAR(POS.RULEELEMENTOWNERSEQ) */
            TO_VARCHAR(POS.MANAGERSEQ,NULL) AS MANAGERSEQ,  /* ORIGSQL: TO_CHAR(POS.MANAGERSEQ) */
            TO_VARCHAR(PRT.PAYEESEQ,NULL) AS PAYEESEQ,  /* ORIGSQL: TO_CHAR(PRT.PAYEESEQ) */
            TO_VARCHAR(TLT.RULEELEMENTOWNERSEQ) AS TITLESEQ,  /* ORIGSQL: TO_CHAR(TLT.RULEELEMENTOWNERSEQ) */
            TRIM(IFNULL(PRT.FIRSTNAME,'') || ' '|| IFNULL(PRT.LASTNAME,'')) AS SALESREPNAME,
            TRIM(IFNULL(MGR.FIRSTNAME,'') || ' '|| IFNULL(MGR.LASTNAME,'')) AS LINEMANAGER,
            PRT.HIREDATE,
            PRT.TERMINATIONDATE,
            BUS.NAME AS COUNTRY,
            TLT.NAME AS JOBTITLE,
            PRT.GENERICATTRIBUTE2 AS EMPCODE,
            PRT.GENERICATTRIBUTE1 AS SALESREPCODE,
            POS.NAME AS POSITIONNAME,
            CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
            PRT.FIRSTNAME,
            PRT.MIDDLENAME,
            PRT.LASTNAME,
            PRT.SALARY,
            PRT.HIREDATE,
            PYE.PAYEEID
        FROM
            --   cs_credit crd,
            cs_period prd,
            cs_participant prt,
            cs_position pos,
            cs_processingunit prc,
            cs_payee pye,
            cs_businessunit bus,
            CS_TITLE TLT,
            CS_PARTICIPANT MGR,
            CS_POSITION MGRPOS
        WHERE
            POS.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PRD.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND PYE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND TLT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND MGR.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND MGRPOS.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
            AND MGRPOS.effectivestartdate < prd.enddate
            AND MGRPOS.effectiveenddate >= prd.enddate
            AND MGR.effectivestartdate < prd.enddate
            AND MGR.effectiveenddate >= prd.enddate
            AND TLT.effectivestartdate < prd.enddate
            AND TLT.effectiveenddate >= prd.enddate
            AND POS.effectivestartdate < prd.enddate
            AND POS.effectiveenddate >= prd.enddate
            AND PRT.effectivestartdate < prd.enddate
            AND PRT.effectiveenddate >= prd.enddate
            AND PYE.effectivestartdate < prd.enddate
            AND PYE.effectiveenddate >= prd.enddate
            AND PRT.PAYEESEQ = PYE.PAYEESEQ
            AND PYE.BUSINESSUNITMAP = BUS.MASK
            AND PYE.PAYEESEQ = POS.PAYEESEQ
            AND TLT.RULEELEMENTOWNERSEQ = POS.TITLESEQ
            AND MGR.PAYEESEQ = MGRPOS.PAYEESEQ
            AND POS.MANAGERSEQ = MGRPOS.RULEELEMENTOWNERSEQ
            AND POS.MANAGERSEQ IS NOT NULL
            -- AND TLT.NAME LIKE 'Advt%'
            --AND (PYE.PAYEEID LIKE 'IP%' OR PYE.PAYEEID LIKE 'PR%')
            AND PRC.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
            AND PRD.PERIODSEQ = :IN_PERIODSEQ
        UNION ALL
            SELECT   /* ORIGSQL: SELECT PRC.TENANTID, PRD.PERIODSEQ, PRC.PROCESSINGUNITSEQ, PRD.NAME PERIODNAME, (...) */
                PRC.TENANTID,
                PRD.PERIODSEQ,
                PRC.PROCESSINGUNITSEQ,
                PRD.NAME AS PERIODNAME,
                PRC.NAME AS PROCESSINGUNITNAME,
                TO_VARCHAR(POS.RULEELEMENTOWNERSEQ) AS POSITIONSEQ,  /* ORIGSQL: TO_CHAR(POS.RULEELEMENTOWNERSEQ) */
                TO_VARCHAR(POS.MANAGERSEQ,NULL) AS MANAGERSEQ,  /* ORIGSQL: TO_CHAR(POS.MANAGERSEQ) */
                TO_VARCHAR(PRT.PAYEESEQ,NULL) AS PAYEESEQ,  /* ORIGSQL: TO_CHAR(PRT.PAYEESEQ) */
                TO_VARCHAR(TLT.RULEELEMENTOWNERSEQ) AS TITLESEQ,  /* ORIGSQL: TO_CHAR(TLT.RULEELEMENTOWNERSEQ) */
                TRIM(IFNULL(PRT.FIRSTNAME,'') || ' '|| IFNULL(PRT.LASTNAME,'')) AS SALESREPNAME,
                NULL AS LINEMANAGERNAME,
                PRT.HIREDATE,
                PRT.TERMINATIONDATE,
                BUS.NAME AS COUNTRY,
                TLT.NAME AS JOBTITLE,
                PRT.GENERICATTRIBUTE2 AS EMPCODE,
                PRT.GENERICATTRIBUTE1 AS SALESREPCODE,
                POS.NAME AS POSITIONNAME,
                CURRENT_TIMESTAMP AS CREATEDATE,  /* ORIGSQL: SYSDATE */
                PRT.FIRSTNAME,
                PRT.MIDDLENAME,
                PRT.LASTNAME,
                PRT.SALARY,
                PRT.HIREDATE,
                PYE.PAYEEID
            FROM
                --   cs_credit crd,
                cs_period prd,
                cs_participant prt,
                cs_position pos,
                cs_processingunit prc,
                cs_payee pye,
                cs_businessunit bus,
                CS_TITLE TLT
            WHERE
                POS.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PRD.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND PYE.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND TLT.REMOVEDATE = to_date('01/01/2200', 'MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200', 'MM/DD/YYYY') */
                AND TLT.effectivestartdate < prd.enddate
                AND TLT.effectiveenddate >= prd.enddate
                AND POS.effectivestartdate < prd.enddate
                AND POS.effectiveenddate >= prd.enddate
                AND PRT.effectivestartdate < prd.enddate
                AND PRT.effectiveenddate >= prd.enddate
                AND PYE.effectivestartdate < prd.enddate
                AND PYE.effectiveenddate >= prd.enddate
                AND PRT.PAYEESEQ = PYE.PAYEESEQ
                AND PYE.BUSINESSUNITMAP = BUS.MASK
                AND PYE.PAYEESEQ = POS.PAYEESEQ
                AND TLT.RULEELEMENTOWNERSEQ = POS.TITLESEQ
                AND POS.MANAGERSEQ IS NULL
                -- AND TLT.NAME LIKE 'Advt%'
                --AND (PYE.PAYEEID LIKE 'IP%' OR PYE.PAYEEID LIKE 'PR%')
                AND PRC.PROCESSINGUNITSEQ = :IN_PROCESSINGUNITSEQ
                AND PRD.PERIODSEQ = :IN_PERIODSEQ
        ;

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:IN_PERIODSEQ),'') || ' PU - '|| IFNULL(TO_VARCHAR(:IN_PROCESSINGUNITSEQ),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END