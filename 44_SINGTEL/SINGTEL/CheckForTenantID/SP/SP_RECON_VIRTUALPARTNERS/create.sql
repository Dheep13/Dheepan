CREATE PROCEDURE EXT.SP_RECON_VIRTUALPARTNERS
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE p_txnmonth TIMESTAMP;  /* ORIGSQL: p_txnmonth date; */
    DECLARE p_semimonth VARCHAR(40);  /* ORIGSQL: p_semimonth varchar2(40); */
    DECLARE v_periodseq DECIMAL(38,10);  /* ORIGSQL: v_periodseq number; */
    DECLARE v_inbound_cfg_parameter ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_RECON_VIRTUALPARTNERS';  /* ORIGSQL: v_proc_name varchar2(127):='SP_RECON_VIRTUALPARTNERS'; */

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        inbound_cfg_parameter;

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */

    SELECT
        to_date(IFNULL(MAX(field1),'') ||'-01','YYYY-MM-DD')  /* ORIGSQL: to_Date(max(field1)||'-01','YYYY-MM-DD') */
    INTO
        p_txnmonth
    FROM
        inbound_data_Staging;

    SELECT
        IFNULL(MAX(field2), 'B')  /* ORIGSQL: nvl(max(field2), 'B') */
    INTO
        p_semimonth
    FROM
        inbound_data_Staging;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Proc Started:' || v_inbound_cfg_parameter.file(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Proc Started:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Month, semiMonth '|| IFNULL(TO_VARCHAR(:p_txnmonth,'YYYYMMDD'),'') || ' '|| IFNULL(:p_semimonth,'')   /* ORIGSQL: SUBSTR(v_proc_name || 'Proc Started:' || v_inbound_cfg_parameter.file_type || '-(...) */
        , :v_rowcount, NULL, NULL);  /* ORIGSQL: to_Char(p_txnmonth,'YYYYMMDD') */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table stel_temp_midmonthpayees'; */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_MIDMONTHPAYEES' not found */

    /* ORIGSQL: truncate table stel_temp_midmonthpayees ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_temp_midmonthpayees';

    /* ORIGSQL: insert into stel_temp_midmonthpayees SELECT nvl(p.payeeid,dim0) dim0, value cuto(...) */
    INSERT INTO stel_temp_midmonthpayees
        SELECT   /* ORIGSQL: SELECT nvl(p.payeeid,dim0) dim0, value cutoff FROM stel_lookup@stelext s join st(...) */
            IFNULL(p.payeeid,dim0) AS dim0,
            value AS cutoff
        FROM
            EXT.stel_lookup s
        INNER JOIN
            EXT.stel_participant p
            ON p.lastname = dim0
            AND p_txnmonth BETWEEN p.effectivestartdate AND ADD_DAYS(p.effectiveenddate,-1)
            /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_participant@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_participant'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_lookup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_lookup'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            s.name = 'LT_VirtualPartners_Rates'
            AND s.dim1 = 'Mid Month Cut Off'
            AND s.dim2 LIKE 'Top Up Revenue%'
            AND value <> 0
            AND :p_txnmonth BETWEEN s.effectivestartdate AND ADD_DAYS(s.effectiveenddate,-1);

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert mid month payees:' || v_inbound_cfg_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert mid month payees:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'MidMonthPAyees'|| IFNULL(TO_VARCHAR(:p_txnmonth,'YYYYMMDD'),'') || ' '|| IFNULL(:p_semimonth,'')   /* ORIGSQL: SUBSTR(v_proc_name || 'Insert mid month payees:' || v_inbound_cfg_parameter.file(...) */
        , :v_rowcount, NULL, NULL);  /* ORIGSQL: to_Char(p_txnmonth,'YYYYMMDD') */

    /* ORIGSQL: commit; */
    COMMIT;  

    /* ORIGSQL: update STEL_DATA_VPRECONSUMMARY SET period = 'B' where period IS NULL and LAST_D(...) */
    UPDATE STEL_DATA_VPRECONSUMMARY
        SET
        /* ORIGSQL: period = */
        period = 'B' 
    FROM
        STEL_DATA_VPRECONSUMMARY
    WHERE
        period IS NULL
        AND LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth);   

    /* ORIGSQL: update STEL_DATA_VENDORDETAIL SET period = 'B' where period IS NULL and LAST_DAY(...) */
    UPDATE STEL_DATA_VENDORDETAIL
        SET
        /* ORIGSQL: period = */
        period = 'B' 
    FROM
        STEL_DATA_VENDORDETAIL
    WHERE
        period IS NULL
        AND LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth);   

    /* ORIGSQL: update STEL_DATA_VENDORDETAIL x SET period = 'A' where payeeid in (SELECT a.paye(...) */
    UPDATE STEL_DATA_VENDORDETAIL x
        SET
        /* ORIGSQL: period = */
        period = 'A' 
    FROM
        STEL_DATA_VENDORDETAIL x
    WHERE
        payeeid  
        IN
        (
            SELECT   /* ORIGSQL: (Select a.payeeid from stel_temp_midmonthpayees a) */
                a.payeeid
            FROM
                stel_temp_midmonthpayees a
        )
        AND TO_VARCHAR(transactiondate,'DD')  /* ORIGSQL: to_char(transactiondate,'DD') */
        <=
        (
            SELECT   /* ORIGSQL: (Select MAX(cutoff) from stel_temp_midmonthpayees a where a.payeeid=x.payeeid) */
                MAX(cutoff) 
            FROM
                stel_temp_midmonthpayees a
            WHERE
                a.payeeid = x.payeeid
        )
        AND LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth);  

    /* ORIGSQL: update STEL_DATA_TOPUPITDM SET recondate=bizdate, period='B' where LAST_DAY(bizd(...) */
    UPDATE STEL_DATA_TOPUPITDM
        SET
        /* ORIGSQL: recondate = */
        recondate = bizdate,
        /* ORIGSQL: period = */
        period = 'B' 
    FROM
        STEL_DATA_TOPUPITDM
    WHERE
        LAST_DAY(bizdate) = LAST_DAY(:p_txnmonth)
        OR LAST_DAY(txndate) = LAST_DAY(:p_txnmonth);   

    /* ORIGSQL: update STEL_DATA_TOPUPITDM SET recondate=txndate where payeeid in (SELECT value (...) */
    UPDATE STEL_DATA_TOPUPITDM
        SET
        /* ORIGSQL: recondate = */
        recondate = txndate
    FROM
        STEL_DATA_TOPUPITDM
    WHERE
        payeeid
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_GENERICPARAMETER' not found */
        IN
        (
            SELECT   /* ORIGSQL: (Select value from inbound_Cfg_genericparameter where key='VPRECON_EXCEPTION') */
                value
            FROM
                inbound_Cfg_genericparameter
            WHERE
                KEY = 'VPRECON_EXCEPTION'
        )
        AND (LAST_DAY(bizdate) = LAST_DAY(:p_txnmonth)
            OR LAST_DAY(txndate) = LAST_DAY(:p_txnmonth));   

    /* ORIGSQL: update STEL_DATA_TOPUPITDM x SET period = 'A' where payeeid in (SELECT a.payeeid(...) */
    UPDATE STEL_DATA_TOPUPITDM x
        SET
        /* ORIGSQL: period = */
        period = 'A' 
    FROM
        STEL_DATA_TOPUPITDM x
    WHERE
        payeeid  
        IN
        (
            SELECT   /* ORIGSQL: (Select a.payeeid from stel_temp_midmonthpayees a) */
                a.payeeid
            FROM
                stel_temp_midmonthpayees a
        )
        AND TO_VARCHAR(recondate,'DD')  /* ORIGSQL: to_char(recondate,'DD') */
        <=
        (
            SELECT   /* ORIGSQL: (Select MAX(cutoff) from stel_temp_midmonthpayees a where a.payeeid=x.payeeid) */
                MAX(cutoff) 
            FROM
                stel_temp_midmonthpayees a
            WHERE
                a.payeeid = x.payeeid
        )
        AND LAST_DAY(recondate) = LAST_DAY(:p_txnmonth);  

    /* ORIGSQL: update STEL_DATA_TOPUPECMS SET period='B' where LAST_DAY(dateofevent) =LAST_DAY((...) */
    UPDATE STEL_DATA_TOPUPECMS
        SET
        /* ORIGSQL: period = */
        period = 'B' 
    FROM
        STEL_DATA_TOPUPECMS
    WHERE
        LAST_DAY(dateofevent) = LAST_DAY(:p_txnmonth);   

    /* ORIGSQL: update STEL_DATA_TOPUPECMS x SET period = 'A' where payeeid in (SELECT a.payeeid(...) */
    UPDATE STEL_DATA_TOPUPECMS x
        SET
        /* ORIGSQL: period = */
        period = 'A' 
    FROM
        STEL_DATA_TOPUPECMS x
    WHERE
        payeeid  
        IN
        (
            SELECT   /* ORIGSQL: (Select a.payeeid from stel_temp_midmonthpayees a) */
                a.payeeid
            FROM
                stel_temp_midmonthpayees a
        )
        AND TO_VARCHAR(dateofevent,'DD')  /* ORIGSQL: to_char(dateofevent,'DD') */
        <=
        (
            SELECT   /* ORIGSQL: (Select MAX(cutoff) from stel_temp_midmonthpayees a where a.payeeid=x.payeeid) */
                MAX(cutoff) 
            FROM
                stel_temp_midmonthpayees a
            WHERE
                a.payeeid = x.payeeid
        )
        AND LAST_DAY(dateofevent) = LAST_DAY(:p_txnmonth);

    /*****************************
    SUMMARY RECON
    ******************************/ 

    /* ORIGSQL: delete from STEL_DATA_VPRECONSUMMARY where LAST_DAY(transactionmonth) =LAST_DAY((...) */
    DELETE
    FROM
        STEL_DATA_VPRECONSUMMARY
    WHERE
        LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth)
        AND diff <> 0
        AND period = :p_semimonth;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Delete last day transactions in STEL_DATA_VPRE(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Delete last day transactions in STEL_DATA_VPRECONSUMMARY :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Delete last day transactions in STEL_DATA_VPRECONSUMMARY Execution Completed txnmonht:'||IFNULL(TO_VARCHAR(:p_txnmonth),''), :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Delete last day transactions in STEL_DATA_VPRECONSUMMARY (...) */

    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_TOPUPITDM' not found */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_VPRECONSUMMARY' not found */
    /* ORIGSQL: insert into STEL_DATA_VPRECONSUMMARY (period, Transactionmonth, payeeid, itdmamo(...) */
    INSERT INTO STEL_DATA_VPRECONSUMMARY
        (
            period, Transactionmonth, payeeid, itdmamount, ecmsamount, adjamount,
            vsumamount
        )
        SELECT   /* ORIGSQL: select M.period, M.TransactionMonth, M.payeeid, nvl(itdm.topupamount,0) itdmAmou(...) */
            M.period,
            M.TransactionMonth,
            M.payeeid,
            IFNULL(itdm.topupamount,0) AS itdmAmount,  /* ORIGSQL: nvl(itdm.topupamount,0) */
            IFNULL(ecms.topupamount,0) AS ecmsAmount,  /* ORIGSQL: nvl(ecms.topupamount,0) */
            IFNULL(adj.topupamount,0) AS AdjAmount,  /* ORIGSQL: nvl(adj.topupamount,0) */
            IFNULL(vsum.topupamount,0) AS VsummaryAmount  /* ORIGSQL: nvl(vsum.topupamount,0) */
        FROM
            (
                SELECT   /* ORIGSQL: (select distinct period, transactionmonth, payeeid, topupamount from (SELECT dis(...) */
                    DISTINCT
                    period,
                    transactionmonth,
                    payeeid,
                    topupamount
                FROM
                    (
                        SELECT   /* ORIGSQL: (select distinct period, LAST_DAY(p_Txnmonth) TransactionMonth, payeeid, 0 topup(...) */
                            DISTINCT
                            period,
                            LAST_DAY(:p_txnmonth) AS TransactionMonth,
                            payeeid,
                            0 AS topupamount
                        FROM
                            stel_data_topupitdm
                        WHERE
                            IFNULL(reconciled,0) = 0  /* ORIGSQL: nvl(reconciled,0) */
                            AND recordstatus = 0
                            AND LAST_DAY(recondate) = LAST_DAY(:p_txnmonth)
                            AND sourceid <> '30'
                            AND period = :p_semimonth
                UNION
                    SELECT   /* ORIGSQL: select distinct period, LAST_DAY(p_Txnmonth) TransactionMonth, payeeid, 0 topupa(...) */
                        DISTINCT
                        period,
                        LAST_DAY(:p_txnmonth) AS TransactionMonth,
                        payeeid,
                        0 AS topupamount
                    FROM
                        stel_data_vendorsummary
                    WHERE
                        IFNULL(reconciled,0) = 0  /* ORIGSQL: nvl(reconciled,0) */
                        AND recordstatus = 0
                        AND LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth)
                        AND period = :p_semimonth
                ) AS dbmtk_corrname_13556
            ) AS M 
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select period, LAST_DAY(recondate) TransactionMonth, payeeid, SUM(val) topupamo(...) */
                    period,
                    LAST_DAY(recondate) AS TransactionMonth,
                    payeeid,
                    SUM(val) AS topupamount
                FROM
                    stel_data_topupitdm
                WHERE
                    LAST_DAY(recondate) = LAST_DAY(:p_txnmonth)
                    AND recordstatus = 0
                GROUP BY
                    LAST_DAY(recondate), payeeid, period
            ) AS itdm
            ON M.transactionmonth = itdm.transactionmonth
            AND m.payeeid = itdm.payeeid
            AND m.period = itdm.period
            /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_TOPUPECMS' not found */
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select period, LAST_DAY(dateofevent) TransactionMonth, payeeid, SUM(transactamt(...) */
                    period,
                    LAST_DAY(dateofevent) AS TransactionMonth,
                    payeeid,
                    SUM(transactamt) AS topupamount
                FROM
                    stel_data_topupecms
                WHERE
                    LAST_DAY(dateofevent) = LAST_DAY(:p_txnmonth)
                    AND recordstatus = 0
                    AND period = :p_semimonth
                GROUP BY
                    LAST_DAY(dateofevent), payeeid, period
            ) AS ecms
            ON M.transactionmonth = ecms.transactionmonth
            AND m.payeeid = ecms.payeeid
            AND m.period = ecms.period
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select p_semimonth period, LAST_DAY(compensationdate) TransactionMonth, positio(...) */
                    :p_semimonth AS period,
                    LAST_DAY(compensationdate) AS TransactionMonth,
                    positionname AS payeeid,
                    SUM(val) AS topupamount
                FROM
                    stel_data_topupadjustments
                WHERE
                    LAST_DAY(compensationdate) = LAST_DAY(:p_txnmonth)
                    AND positionname NOT IN
                    (
                        SELECT   /* ORIGSQL: (select payeeid from stel_Temp_midmonthpayees) */
                            payeeid
                        FROM
                            stel_Temp_midmonthpayees
                    )
                    AND p_semimonth = 'B'
                GROUP BY
                    LAST_DAY(compensationdate), positionname
            ) AS adj
            ON M.transactionmonth = adj.transactionmonth
            AND m.payeeid = adj.payeeid
            AND m.period = adj.period
            /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_VENDORSUMMARY' not found */
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select period, LAST_DAY(TransactionMonth) TransactionMonth, payeeid, SUM(amount(...) */
                    period,
                    LAST_DAY(TransactionMonth) AS TransactionMonth,
                    payeeid,
                    SUM(amount) AS topupamount
                FROM
                    stel_data_vendorsummary
                WHERE
                    LAST_DAY(TransactionMonth) = LAST_DAY(:p_txnmonth)
                    AND recordstatus = 0
                    --exclude previously reconciled ones

                GROUP BY
                    LAST_DAY(TransactionMonth), payeeid, period
            ) AS vsum
            ON M.transactionmonth = vsum.transactionmonth
            AND m.payeeid = vsum.payeeid
            AND m.period = vsum.period
        WHERE
            m.payeeid NOT IN
            (
                SELECT   /* ORIGSQL: (select nvl(payeeid,'x') from stel_Data_vpreconsummary where diff=0 and LAST_DAY(...) */
                    IFNULL(payeeid,'x')
                FROM
                    stel_Data_vpreconsummary
                WHERE
                    diff = 0
                    AND LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth)
                    AND period = :p_semimonth
            );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into STEL_DATA_VPRECONSUMMARY :' || v_i(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert into STEL_DATA_VPRECONSUMMARY :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Insert into STEL_DATA_VPRECONSUMMARY Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert into STEL_DATA_VPRECONSUMMARY :' || v_inbound_cfg_(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /*****************************
    CALCULATE DIFFERENCE
    ******************************/   

    /* ORIGSQL: update STEL_DATA_VPRECONSUMMARY SET diff = nvl(itdmamount+ecmsamount+Adjamount-v(...) */
    UPDATE STEL_DATA_VPRECONSUMMARY
        SET
        /* ORIGSQL: diff = */
        diff = IFNULL(itdmamount+ecmsamount+Adjamount-vsumamount,0)  /* ORIGSQL: nvl(itdmamount+ecmsamount+Adjamount-vsumamount,0) */
    FROM
        STEL_DATA_VPRECONSUMMARY
    WHERE
        transactionmonth = LAST_DAY(:p_txnmonth)
        AND period = :p_semimonth;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update diff STEL_DATA_VPRECONSUMMARY :' || v_i(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update diff STEL_DATA_VPRECONSUMMARY :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update diff STEL_DATA_VPRECONSUMMARY Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update diff STEL_DATA_VPRECONSUMMARY :' || v_inbound_cfg_(...) */

    /*****************************
    MARK RECORDS AS RECONCILED
    ******************************/ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_topupitdm tgt using (SELECT * FROM STEL_DATA_VPRECONSUMMARY(...) */
    MERGE INTO stel_data_topupitdm AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (Select * from STEL_DATA_VPRECONSUMMARY where diff=0 and period=p_semimonth) */
                *
            FROM
                STEL_DATA_VPRECONSUMMARY
            WHERE
                diff = 0
                AND period = :p_semimonth
        ) AS src
        ON (tgt.payeeid = src.payeeid
            AND LAST_DAY(src.transactionmonth) = LAST_DAY(tgt.recondate)
            AND recordstatus = 0
            AND tgt.period = :p_semimonth
            AND tgt.reconciled = 0
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.reconciled = 1;
        

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Mark Reconciled reocrds in stel_data_topupitdm(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Mark Reconciled reocrds in stel_data_topupitdm :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Mark Reconciled reocrds in stel_data_topupitdm Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Mark Reconciled reocrds in stel_data_topupitdm :' || v_in(...) */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_vendorsummary tgt using (SELECT * FROM STEL_DATA_VPRECONSUM(...) */
    MERGE INTO stel_data_vendorsummary AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (Select * from STEL_DATA_VPRECONSUMMARY where diff=0 and period=p_semimonth) */
                *
            FROM
                STEL_DATA_VPRECONSUMMARY
            WHERE
                diff = 0
                AND period = :p_semimonth
        ) AS src
        ON (tgt.payeeid = src.payeeid
        AND src.transactionmonth = tgt.transactionmonth
        AND recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.reconciled = 1;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'set reconcile to 1 in stel_data_vendorsummary (...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'set reconcile to 1 in stel_data_vendorsummary :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'set reconcile to 1 in stel_data_vendorsummary Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'set reconcile to 1 in stel_data_vendorsummary :' || v_inb(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /*****************************
    START DETAIL RECON
    ******************************/ 

    /* ORIGSQL: delete from STEL_DATA_VPRECONDETAIL where LAST_DAY(transactiondate) =LAST_DAY(p_(...) */
    DELETE
    FROM
        STEL_DATA_VPRECONDETAIL
    WHERE
        LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth)
        AND (period = :p_semimonth
        OR period IS NULL);

    /* ORIGSQL: commit; */
    COMMIT;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Delete last day txn in STEL_DATA_VPRECONDETAIL(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Delete last day txn in STEL_DATA_VPRECONDETAIL:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Delete last day txn in STEL_DATA_VPRECONDETAIL Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Delete last day txn in STEL_DATA_VPRECONDETAIL:' || v_inb(...) */

    /* ORIGSQL: insert into STEL_DATA_VPRECONDETAIL (DATASOURCE, TransactionID, TransactionDate,(...) */
    INSERT INTO STEL_DATA_VPRECONDETAIL
        (
            DATASOURCE, TransactionID, TransactionDate, Payeeid, Amount, Phone,
            TopUpType, period
        )
        SELECT   /* ORIGSQL: select 'ITDM', sourceseq, recondate, payeeid, val, msisdn, card_Group, p_semimon(...) */
            'ITDM',
            sourceseq,
            recondate,
            payeeid,
            val,
            msisdn,
            card_Group,
            :p_semimonth
        FROM
            stel_Data_topupitdm
        WHERE
            LAST_DAY(recondate) = LAST_DAY(:p_txnmonth)
            AND recordstatus = 0
            AND payeeid IN
            (
                SELECT   /* ORIGSQL: (select payeeid from STEL_DATA_VPRECONSUMMARY where LAST_DAY(transactionmonth) =(...) */
                    payeeid
                FROM
                    STEL_DATA_VPRECONSUMMARY
                WHERE
                    LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth)
                    AND diff <> 0
            )
            AND period = :p_semimonth
UNION ALL
    SELECT   /* ORIGSQL: select 'ECMS', nvl(origintransactionid,'') ||'-'||nvl(voucherserial,''), dateofe(...) */
        'ECMS',
        IFNULL(origintransactionid,'') ||'-'||IFNULL(voucherserial,''),  /* ORIGSQL: nvl(voucherserial,'') */
                                                                         /* ORIGSQL: nvl(origintransactionid,'') */
        dateofevent,
        payeeid,
        transactamt,
        subscriberid,
        vouchergroup,
        :p_semimonth
    FROM
        stel_Data_topupecms
    WHERE
        LAST_DAY(dateofevent) = LAST_DAY(:p_txnmonth)
        AND recordstatus = 0
        AND (payeeid IN
            (
                SELECT   /* ORIGSQL: (select payeeid from STEL_DATA_VPRECONSUMMARY where LAST_DAY(transactionmonth) =(...) */
                    payeeid
                FROM
                    STEL_DATA_VPRECONSUMMARY
                WHERE
                    LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth)
                    AND diff <> 0
            )
        OR payeeid IS NULL)
        AND period = :p_semimonth
UNION ALL
    SELECT   /* ORIGSQL: select 'ADJ', orderid, compensationdate, positionname payeeid, val, contact, gen(...) */
        'ADJ',
        orderid,
        compensationdate,
        positionname AS payeeid,
        val,
        contact,
        genericattribute2 AS card_Group,
        :p_semimonth
    FROM
        stel_Data_topupadjustments
    WHERE
        LAST_DAY(compensationdate) = LAST_DAY(:p_txnmonth)
        AND positionname IN
        (
            SELECT   /* ORIGSQL: (select payeeid from STEL_DATA_VPRECONSUMMARY where LAST_DAY(transactionmonth) =(...) */
                payeeid
            FROM
                STEL_DATA_VPRECONSUMMARY
            WHERE
                LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth)
                AND diff <> 0
        )
UNION ALL
    SELECT   /* ORIGSQL: select 'VENDORDTL', transactionid, transactiondate, payeeid, amount, phone, topu(...) */
        'VENDORDTL',
        transactionid,
        transactiondate,
        payeeid,
        amount,
        phone,
        topuptype,
        :p_semimonth
    FROM
        stel_Data_vendordetail
    WHERE
        LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth)
        AND recordstatus = 0
        AND payeeid IN
        (
            SELECT   /* ORIGSQL: (select payeeid from STEL_DATA_VPRECONSUMMARY where LAST_DAY(transactionmonth) =(...) */
                payeeid
            FROM
                STEL_DATA_VPRECONSUMMARY
            WHERE
                LAST_DAY(transactionmonth) = LAST_DAY(:p_txnmonth)
                AND diff <> 0
        )
        AND period = :p_semimonth;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into STEL_DATA_VPRECONDETAIL:' || v_inb(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert into STEL_DATA_VPRECONDETAIL:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Insert into STEL_DATA_VPRECONDETAIL Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert into STEL_DATA_VPRECONDETAIL:' || v_inbound_cfg_pa(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /*****************************
    COMPARE AGAINST VENDOR DETAIL
    ******************************/
    --fix for duplicates - aggregate across transactionid  
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_DATA_VPRECONDETAIL tgt using (SELECT a.payeeid, a.transactiondat(...) */
    MERGE INTO STEL_DATA_VPRECONDETAIL AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select a.payeeid, a.transactiondate, a.phone, b.transactionid, a.amount from (S(...) */
                a.payeeid,
                a.transactiondate,
                a.phone,
                b.transactionid,
                a.amount
                /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_VPRECONDETAIL' not found */
            FROM
                (
                    SELECT   /* ORIGSQL: (select payeeid, transactiondate, phone, amount from STEL_DATA_VPRECONDETAIL whe(...) */
                        payeeid,
                        transactiondate,
                        phone,
                        amount
                    FROM
                        STEL_DATA_VPRECONDETAIL
                    WHERE
                        LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth)
                        AND period = :p_semimonth
                        AND datasource <> 'VENDORDTL'
                    GROUP BY
                        payeeid, transactiondate, phone, amount
                ) AS a
                /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_VENDORDETAIL' not found */
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select payeeid, transactiondate, amount, phone, listagg(transactionid,',') with(...) */
                        payeeid,
                        transactiondate,
                        amount,
                        phone,
                        STRING_AGG(transactionid,',' ORDER BY transactionid) AS transactionid  /* ORIGSQL: listagg(transactionid,',') within group (ORDER BY transactionid) */
                    FROM
                        stel_Data_vendordetail
                    WHERE
                        LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth)
                        AND recordstatus = 0
                        AND period = :p_semimonth
                    GROUP BY
                        payeeid, transactiondate, phone, amount
                ) AS b
                ON a.payeeid = b.payeeid
                AND a.transactiondate = b.transactiondate
                AND a.phone = b.phone
                AND a.amount = b.amount
        ) AS src
        ON (tgt.payeeid = src.payeeid
            AND tgt.transactiondate = src.transactiondate
            AND tgt.amount = src.amount
        AND src.phone = tgt.phone
        AND LAST_DAY(tgt.transactiondate) = LAST_DAY(:p_txnmonth)
        AND datasource <> 'VENDORDTL'
        AND period = :p_semimonth
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.reconciled = 1, tgt.vendordetailmatch = IFNULL(src.transactionid,'')||',' ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETA(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL:' || v_i(...) */

    /*
     update STEL_DATA_VPRECONDETAIL tgt
     set reconciled=1
     where tgt.datasource='VENDORDTL' and last_day(tgt.transactiondate) = last_day(p_txnmonth) and period = p_semimonth
     and
     exists
     (select 1 from STEL_DATA_VPRECONDETAIL a where a.datasource<>'VENDORDTL' and last_day(a.transactiondate) = last_day(p_txnmonth)
         and a.reconciled=1 and period = p_semimonth
         and instr(a.vendordetailmatch,tgt.transactionid||',')>0
     );
    */
    /*
    merge into  /*+ use_nl(src,tgt) FULL(tgt) *  STEL_DATA_VPRECONDETAIL tgt
    using (
        select /*+ INDEX(a,STEL_DATA_VPRECONDETAIL_INDEX2)    * vendordetailmatch from STEL_DATA_VPRECONDETAIL a where a.datasource<>'VENDORDTL'
         and a.reconciled=1 and last_day(a.transactiondate) = last_day(p_txnmonth) and
         period = p_semimonth and a.vendordetailmatch IS NOT NULL
    ) src
    on ( instr(src.vendordetailmatch,tgt.transactionid||',')>0)
    when matched then update set tgt.reconciled=1
    where tgt.datasource='VENDORDTL' and last_day(tgt.transactiondate) = last_day(p_txnmonth) and period = p_semimonth;
    */

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETA(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VEND(...) */

    /*
    merge into STEL_DATA_VPRECONDETAIL tgt
    using (select payeeid, transactiondate, sum(amount) amount, phone, transactionid  from stel_Data_vendordetail
        where last_Day(transactiondate)= last_day(p_txnmonth) and recordstatus=0
        
        
        group by  payeeid, transactiondate,   phone , transactionid
    ) src
    on (tgt.payeeid=src.payeeid
        and tgt.transactiondate=src.transactiondate
        and tgt.amount=src.amount
    and src.phone=tgt.phone)
    when matched then update set
    tgt.vendordetailmatch = src.transactionid, tgt.reconciled=1
    where last_day(tgt.transactiondate) = last_day(p_txnmonth);
    */

    /* ORIGSQL: commit; */
    COMMIT;

    -- update VDTL table back as reconciled where there is a match above 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_DATA_TOPUPECMS tgt using (SELECT transactiondate, amount, phone,(...) */
    MERGE INTO STEL_DATA_TOPUPECMS AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select transactiondate, amount, phone, COUNT(*), MAX(payeeid) payeeid from stel(...) */
                transactiondate,
                amount,
                phone,
                COUNT(*),
                MAX(payeeid) AS payeeid
            FROM
                stel_Data_vprecondetail
            WHERE
                LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth)
                AND datasource = 'VENDORDTL'
                AND IFNULL(reconciled,0) = 0  /* ORIGSQL: nvl(reconciled,0) */
                AND period = :p_semimonth
            GROUP BY
                transactiondate, amount, phone
        ) AS src
        ON (tgt.dateofevent = src.transactiondate
        AND tgt.transactamt = src.amount
        AND src.phone = tgt.subscriberid
        AND LAST_DAY(tgt.dateofevent) = LAST_DAY(:p_txnmonth)
        AND recordstatus = 0
        AND period = :p_semimonth
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.payeeid = src.payeeid;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update PayeeId in STEL_DATA_TOPUPECMS:' || v_i(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update PayeeId in STEL_DATA_TOPUPECMS:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update PayeeId in STEL_DATA_TOPUPECMS Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update PayeeId in STEL_DATA_TOPUPECMS:' || v_inbound_cfg_(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /****************Do again after ECMS************/ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_DATA_VPRECONDETAIL tgt using (SELECT a.payeeid, a.transactiondat(...) */
    MERGE INTO STEL_DATA_VPRECONDETAIL AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select a.payeeid, a.transactiondate, a.phone, b.transactionid, a.amount from (S(...) */
                a.payeeid,
                a.transactiondate,
                a.phone,
                b.transactionid,
                a.amount  
            FROM
                (
                    SELECT   /* ORIGSQL: (select payeeid, transactiondate, phone, amount from STEL_DATA_VPRECONDETAIL whe(...) */
                        payeeid,
                        transactiondate,
                        phone,
                        amount
                    FROM
                        STEL_DATA_VPRECONDETAIL
                    WHERE
                        LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth)
                        AND datasource <> 'VENDORDTL'
                        AND period = :p_semimonth
                    GROUP BY
                        payeeid, transactiondate, phone, amount
                ) AS a 
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select payeeid, transactiondate, amount, phone, listagg(transactionid,',') with(...) */
                        payeeid,
                        transactiondate,
                        amount,
                        phone,
                        STRING_AGG(transactionid,',' ORDER BY transactionid) AS transactionid  /* ORIGSQL: listagg(transactionid,',') within group (ORDER BY transactionid) */
                    FROM
                        stel_Data_vendordetail
                    WHERE
                        LAST_DAY(transactiondate) = LAST_DAY(:p_txnmonth)
                        AND recordstatus = 0
                        AND period = :p_semimonth
                    GROUP BY
                        payeeid, transactiondate, phone, amount
                ) AS b
                ON a.payeeid = b.payeeid
                AND a.transactiondate = b.transactiondate
                AND a.phone = b.phone
                AND a.amount = b.amount
               
        ) AS src
        ON (tgt.payeeid = src.payeeid
            AND tgt.transactiondate = src.transactiondate
            AND tgt.amount = src.amount
        AND src.phone = tgt.phone
        AND LAST_DAY(tgt.transactiondate) = LAST_DAY(:p_txnmonth)
        AND datasource <> 'VENDORDTL'
        AND period = :p_semimonth
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.reconciled = 1, tgt.vendordetailmatch = IFNULL(src.transactionid,'')||',' ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETA(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL:' || v_i(...) */

    /*
     update STEL_DATA_VPRECONDETAIL tgt
     set reconciled=1
     where tgt.datasource='VENDORDTL' and last_day(tgt.transactiondate) = last_day(p_txnmonth) and period = p_semimonth
     and
     exists
     (select 1 from STEL_DATA_VPRECONDETAIL a where a.datasource<>'VENDORDTL' and last_day(a.transactiondate) = last_day(p_txnmonth)
         and a.reconciled=1 and period = p_semimonth
         and instr(a.vendordetailmatch,tgt.transactionid||',')>0
     );
    */

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETA(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VENDORDTL Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update reconcile to 1 in STEL_DATA_VPRECONDETAIL for VEND(...) */

    /*************************/

    SELECT
        pd.periodseq
    INTO
        v_periodseq
    FROM
        cs_period pd
    INNER JOIN
        cs_periodtype pt
        ON pt.periodtypeseq = pd.periodtypeseq
        AND pt.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        AND pt.name = 'month'
    INNER JOIN
        cs_calendar c
        ON c.calendarseq = pd.calendarseq
        AND c.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        AND c.name LIKE 'Singtel%Month%'
        /* RESOLVE: Oracle Database link: Remote table/view 'cs_periodtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_periodtype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        /* RESOLVE: Oracle Database link: Remote table/view 'cs_period@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_period'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        /* RESOLVE: Oracle Database link: Remote table/view 'cs_calendar@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_calendar'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
        pd.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
        AND :p_txnmonth BETWEEN pd.startdate AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)));  /* ORIGSQL: pd.enddate-1 */

    -- STEL_PROC_RPT_PARTITIONS_PSEQ@stelext(:v_periodseq,'stel_rpt_data_vpreconsummary');;/* NOT CONVERTED! *//*Deepan: Partitions not required*/
    -- STEL_PROC_RPT_PARTITIONS_PSEQ@stelext(:v_periodseq,'stel_rpt_data_vprecondetail');;/* NOT CONVERTED! *//*Deepan: Partitions not required*/

    /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_RPT_DATA_VPRECONSUMMARY@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_RPT_DATA_VPRECONSUMMARY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: delete from STEL_RPT_DATA_VPRECONSUMMARY@STELEXT where periodseq=v_periodseq; */
    DELETE
    FROM
        EXT.STEL_RPT_DATA_VPRECONSUMMARY
    WHERE
        periodseq = :v_periodseq;

    /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_RPT_DATA_VPRECONSUMMARY@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_RPT_DATA_VPRECONSUMMARY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: INSERT INTO STEL_RPT_DATA_VPRECONSUMMARY@STELEXT (PERIODSEQ, POSITIONSEQ, PERIOD(...) */
    INSERT INTO EXT.STEL_RPT_DATA_VPRECONSUMMARY
        (
            PERIODSEQ, POSITIONSEQ, PERIODNAME, TRANSACTIONMONTH, PAYEEID, ITDMAMOUNT,
            ECMSAMOUNT, ADJAMOUNT, VSUMAMOUNT, DIFF, VENDORNAME, PROCESSINGUNITSEQ,
            PROCESSINGUNITNAME, CALENDARNAME, SOURCEPERIODNAME
        )
        SELECT   /* ORIGSQL: SELECT distinct pd.periodseq, pos.ruleelementownerseq, pd.name, TRANSACTIONMONTH(...) */
            DISTINCT
            pd.periodseq,
            pos.ruleelementownerseq,
            pd.name,
            TRANSACTIONMONTH,
            PAYEEID,
            ITDMAMOUNT,
            ECMSAMOUNT,
            ADJAMOUNT,
            VSUMAMOUNT,
            DIFF,
            par.lastname,
            pu.processingunitseq,
            pu.name,
            c.name,
            pd.name
        FROM
            STEL_DATA_VPRECONSUMMARY vp
        CROSS JOIN
            (
                SELECT   /* ORIGSQL: (select * from cs_processingunit@stelext where name ='Singtel_PU') */
                    *
                FROM
                    cs_processingunit
                    /* RESOLVE: Oracle Database link: Remote table/view 'cs_processingunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_processingunit'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    name = 'Singtel_PU'
            ) AS pu
        INNER JOIN
            cs_period pd
            ON pd.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1))) = vp.transactionmonth  /* ORIGSQL: pd.enddate-1 */
        INNER JOIN
            cs_periodtype pt
            ON pt.periodtypeseq = pd.periodtypeseq
            AND pt.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND pt.name = 'month'
        INNER JOIN
            cs_calendar c
            ON c.calendarseq = pd.calendarseq
            AND c.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND c.name LIKE 'Singtel%Month%'
        INNER JOIN
            cs_position pos
            ON pos.name = vp.payeeid
            AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND vp.transactionmonth BETWEEN pos.effectivestartdate AND ADD_DAYS(pos.effectiveenddate,-1)
        INNER JOIN
            cs_participant par
            ON par.payeeseq = pos.payeeseq
            AND par.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND vp.transactionmonth BETWEEN par.effectivestartdate AND ADD_DAYS(par.effectiveenddate,-1)
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_periodtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_periodtype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_period@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_period'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_participant@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_participant'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_calendar@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_calendar'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            diff <> 0
            AND :p_txnmonth BETWEEN pd.startdate AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
            AND vp.period = :p_semimonth;

    /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_RPT_DATA_VPRECONDETAIL@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_RPT_DATA_VPRECONDETAIL'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: delete from STEL_RPT_DATA_VPRECONDETAIL@STELEXT where periodseq=v_periodseq; */
    DELETE
    FROM
        EXT.STEL_RPT_DATA_VPRECONDETAIL
    WHERE
        periodseq = :v_periodseq;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Delete STEL_RPT_DATA_VPRECONDETAIL for curr Pe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Delete STEL_RPT_DATA_VPRECONDETAIL for curr Period:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Delete STEL_RPT_DATA_VPRECONDETAIL for curr Period Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Delete STEL_RPT_DATA_VPRECONDETAIL for curr Period:' || v(...) */

    /*
    INSERT
    INTO STEL_RPT_DATA_VPRECONDETAIL@stelext
      (
            PERIODSEQ,    POSITIONSEQ,    PERIODNAME,
            TRANSACTIONID,    TRANSACTIONDATE,    PAYEEID,    AMOUNT,    PHONE,    TOPUPTYPE,    VENDORDETAILMATCH,
            RECONCILED,    DATASOURCE,    VENDORNAME,    PROCESSINGUNITSEQ,    PROCESSINGUNITNAME,    CALENDARNAME,
        SOURCEPERIODNAME  )
    
    
         SELECT
         pd.periodseq, pos.ruleelementownerseq, pd.name,
         TRANSACTIONID,   TRANSACTIONDATE,  PAYEEID,  AMOUNT,  PHONE,  TOPUPTYPE,  VENDORDETAILMATCH,
      RECONCILED,  DATASOURCE, par.lastname, pu.processingunitseq, pu.name, c.name, pd.name
    FROM STEL_DATA_VPRECONDETAIL vp
    
    cross join (select * from cs_processingunit@stelext where name ='Singtel_PU') pu
    join cs_period@stelext pd
    on pd.removedate>sysdate
    and vp.transactiondate between pd.startdate and pd.enddate-1
    join cs_periodtype@stelext pt
    on pt.periodtypeseq=pd.periodtypeseq
    and pt.removedate>sysdate
    and pt.name='month'
    join cs_calendar@stelext c
    on c.calendarseq=pd.calendarseq
    and c.removedate>sysdate
    and c.name like 'Singtel%Month%'
    left join cs_position@stelext pos
    on pos.name=vp.payeeid
    and pos.removedate>sysdate
    and vp.transactiondate between pos.effectivestartdate and poADD_DAYS(s.effectiveenddate,-1)
    left join cs_participant@stelext par
    on par.payeeseq=pos.payeeseq and par.removedate>sysdate
    and vp.transactiondate between par.effectivestartdate and par.effectiveenddate-1
    where vp.reconciled IS NULL and
     p_txnmonth between pd.startdate and pd.enddate-1 and vp.period = p_semimonth
    ;
    
        v_rowcount := SQL%ROWCOUNT;
    
          SP_LOGGER (
                 SUBSTR (
                           v_proc_name
                        || 'Insert into STEL_RPT_DATA_VPRECONDETAIL:'
                        || v_inbound_cfg_parameter.file_type
                        || '-FileName:'
                        || v_inbound_cfg_parameter.file_name
                        || '-Date:'
                        || v_inbound_cfg_parameter.file_date,
                        1,
                    255),
                 'Insert into STEL_RPT_DATA_VPRECONDETAIL Execution Completed',
                 v_rowcount,
                 NULL,
             null);
    
       commit;
    
    
    
    */
END