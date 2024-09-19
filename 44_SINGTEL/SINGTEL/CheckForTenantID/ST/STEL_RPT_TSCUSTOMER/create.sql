CREATE PROCEDURE EXT.STEL_RPT_TSCUSTOMER
(
    IN in_periodseq BIGINT,   /* ORIGSQL: in_periodseq IN integer */
    IN in_processingunitseq BIGINT     /* ORIGSQL: in_processingunitseq IN integer */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Tenant VARCHAR(255) = 'STEL';  /* ORIGSQL: v_Tenant varchar2(255) := 'STEL'; */
    DECLARE v_StMsg VARCHAR(255) = 'START';  /* ORIGSQL: v_StMsg varchar2(255) := 'START'; */
    DECLARE v_EdMsg VARCHAR(255) = 'END';  /* ORIGSQL: v_EdMsg varchar2(255) := 'END'; */
    DECLARE v_ComponentName VARCHAR(255) = NULL;  /* ORIGSQL: v_ComponentName varchar2(255) := NULL; */
    DECLARE v_eot CONSTANT TIMESTAMP = to_date('01/01/2200', 'MM/DD/YYYY');  /* ORIGSQL: v_eot constant date := to_date('01/01/2200', 'MM/DD/YYYY') ; */

    v_ComponentName = 'stel_rpt_tscustomer';

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_StMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_StMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: delete from stel_rpt_data_tscustmast where periodseq = in_periodseq and processi(...) */
    DELETE
    FROM
        ext.stel_rpt_data_tscustmast
    WHERE
        periodseq = :in_periodseq
        AND processingunitseq = :in_processingunitseq;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: stel_proc_rpt_partitions (in_periodseq, 'stel_rpt_data_tscustmast') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'ext.stel_rpt_data_tscustmast');

    -- managing table partitions

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PAYEE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_DATA_TSCUSTMAST' not found */

    /* ORIGSQL: insert into stel_rpt_data_tscustmast (tenantid, periodseq, periodname, payeeseq,(...) */
    INSERT INTO ext.stel_rpt_data_tscustmast
        (
            tenantid,
            periodseq,
            periodname,
            payeeseq,
            positionseq,
            positionname,
            titleseq,
            titlename,
            processingunitseq,
            processingunitname,
            payeeid,
            firstname,
            middlename,
            lastname,
            hiredate,
            terminationdate,
            managerseq,
            manager,
            team,
            geid,
            salesrepcode
        )
        SELECT   /* ORIGSQL: (select v_Tenant, prd.periodseq as periodseq, prd.name as periodname, p.payeeseq(...) */
            :v_Tenant,
            prd.periodseq AS periodseq,
            prd.name AS periodname,
            p.payeeseq AS payeeseq,
            pos.ruleelementownerseq AS positionseq,
            pos.name AS positionname,
            t.ruleelementownerseq AS titleseq,
            (
                CASE
                    WHEN t.name = 'TeleSales - Sales Executive'
                    THEN 'Sales Rep'
                    WHEN t.name = 'TeleSales - Team Lead'
                    THEN 'TL'
                    WHEN t.name IN('TeleSales - Manager')
                    THEN 'Manager'
                    WHEN t.name IN('TeleSales - Director')
                    THEN 'Director'
                END
            ) AS titlename,
            pu.processingunitseq AS processingunitseq,
            pu.name AS processingunitname,
            p.payeeid AS payeeid,
            par.firstname AS firstname,
            par.middlename AS middlename,
            par.lastname AS lastname,
            par.hiredate AS hiredate,
            par.terminationdate AS terminationdate,
            pos.managerseq AS managerseq,
            mgr.lastname AS manager,
            pos.genericattribute3 AS team,
            par.userid AS geId,
            PAR.GENERICATTRIBUTE1 AS salesrepcode
        FROM
            cs_participant par,
            cs_position pos,
            cs_payee p,
            cs_title t,
            cs_period prd,
            cs_processingunit pu,
            cs_position mgrpos,
            cs_participant mgr
        WHERE
            p.payeeseq = par.payeeseq
            AND par.payeeseq = pos.payeeseq
            AND pos.titleseq = t.ruleelementownerseq
            AND mgr.payeeseq = mgrpos.payeeseq
            AND pos.managerseq = mgrpos.ruleelementownerseq
            AND p.removedate = :v_eot
            AND par.removedate = :v_eot
            AND pos.removedate = :v_eot
            AND t.removedate = :v_eot
            AND prd.removedate = :v_eot
            AND mgrpos.removedate = :v_eot
            AND mgr.removedate = :v_eot
            AND p.effectivestartdate <= prd.enddate
            AND par.effectivestartdate <= prd.enddate
            AND pos.effectivestartdate <= prd.enddate
            AND t.effectivestartdate <= prd.enddate
            AND mgrpos.effectivestartdate <= prd.enddate
            AND mgr.effectivestartdate <= prd.enddate
            AND p.effectiveenddate > prd.enddate
            AND par.effectiveenddate > prd.enddate
            AND pos.effectiveenddate > prd.enddate
            AND t.effectiveenddate > prd.enddate
            AND mgrpos.effectiveenddate > prd.enddate
            AND mgr.effectiveenddate > prd.enddate
            -- Added termination date logic.
            AND (PAR.TERMINATIONDATE >= PRD.STARTDATE
            OR PAR.TERMINATIONDATE IS NULL)
            AND t.name LIKE 'TeleSales%'
            AND pos.genericattribute3 = 'Singtel TV Telesales'
            AND pos.managerseq IS NOT NULL
            AND pu.processingunitseq = :in_processingunitseq
            AND prd.periodseq = :in_periodseq
        UNION ALL
            SELECT   /* ORIGSQL: select v_Tenant, prd.periodseq as periodseq, prd.name as periodname, p.payeeseq (...) */
                :v_Tenant,
                prd.periodseq AS periodseq,
                prd.name AS periodname,
                p.payeeseq AS payeeseq,
                pos.ruleelementownerseq AS positionseq,
                pos.name AS positionname,
                t.ruleelementownerseq AS titleseq,
                (
                    CASE
                        WHEN t.name = 'TeleSales - Sales Executive'
                        THEN 'Sales Rep'
                        WHEN t.name = 'TeleSales - Team Lead'
                        THEN 'TL'
                        WHEN t.name IN('TeleSales - Manager')
                        THEN 'Manager'
                        WHEN t.name IN('TeleSales - Director')
                        THEN 'Director'
                    END
                ) AS titlename,
                pu.processingunitseq AS processingunitseq,
                pu.name AS processingunitname,
                p.payeeid AS payeeid,
                par.firstname AS firstname,
                par.middlename AS middlename,
                par.lastname AS lastname,
                par.hiredate AS hiredate,
                par.terminationdate AS terminationdate,
                pos.managerseq AS managerseq,
                NULL AS manager,
                pos.genericattribute3 AS team,
                par.userid AS geId,
                PAR.GENERICATTRIBUTE1 AS salesrepcode
            FROM
                cs_participant par,
                cs_position pos,
                cs_payee p,
                cs_title t,
                cs_period prd,
                cs_processingunit pu
            WHERE
                p.payeeseq = par.payeeseq
                AND par.payeeseq = pos.payeeseq
                AND pos.titleseq = t.ruleelementownerseq
                AND p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND par.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND t.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND prd.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND p.effectivestartdate <= prd.enddate
                AND par.effectivestartdate <= prd.enddate
                AND pos.effectivestartdate <= prd.enddate
                AND t.effectivestartdate <= prd.enddate
                AND p.effectiveenddate > prd.enddate
                AND par.effectiveenddate > prd.enddate
                AND pos.effectiveenddate > prd.enddate
                AND t.effectiveenddate > prd.enddate
                -- Added termination date logic.
                AND (PAR.TERMINATIONDATE >= PRD.STARTDATE
                OR PAR.TERMINATIONDATE IS NULL)
                AND t.name LIKE 'TeleSales%'
                AND pos.genericattribute3 = 'Singtel TV Telesales'
                AND pos.managerseq IS NULL
                AND pu.processingunitseq = :in_processingunitseq
                AND prd.periodseq = :in_periodseq
        ;

    /* ORIGSQL: commit; */
    COMMIT;

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, v_ComponentName, v_EdMsg || ' PeriodSeq - ' || in_PeriodSeq (...) */
    CALL EXT.STEL_LOG(:v_Tenant, :v_ComponentName, IFNULL(:v_EdMsg,'') || ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END