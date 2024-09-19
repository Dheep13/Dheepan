CREATE PROCEDURE EXT.SP_INBOUND_POST_LIVECHAT
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_LIVECHAT';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_LIVECHAT'; */

    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    --STEP 1
    /*set to reconciled*/ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO STEL_DATA_LIVECHAT_ESHOP tgt USING (SELECT bill.contact AS Serv_numbe(...) */
    MERGE INTO STEL_DATA_LIVECHAT_ESHOP AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (SELECT bill.contact Serv_number, bill.custid, st.genericattribute3 DealerCode, (...) */
                bill.contact AS Serv_number,
                bill.custid,
                st.genericattribute3 AS DealerCode,
                st.productid,
                st.ALTERNATEORDERNUMBER,
                st.GENERICATTRIBUTE9,
                st.GENERICATTRIBUTE10,
                st.GENERICATTRIBUTE11,
                st.GENERICATTRIBUTE12,
                st.GENERICATTRIBUTE13,
                st.GENERICATTRIBUTE14,
                st.GENERICATTRIBUTE15,
                st.GENERICATTRIBUTE16,
                st.GENERICATTRIBUTE17,
                st.GENERICATTRIBUTE18,
                st.GENERICATTRIBUTE19,
                st.GENERICATTRIBUTE20,
                st.GENERICATTRIBUTE21,
                st.GENERICATTRIBUTE22,
                st.GENERICATTRIBUTE23,
                st.GENERICATTRIBUTE24,
                st.GENERICATTRIBUTE25,
                st.GENERICATTRIBUTE26,
                st.GENERICATTRIBUTE27,
                st.GENERICATTRIBUTE28,
                st.GENERICATTRIBUTE29,
                st.GENERICATTRIBUTE30,
                st.GENERICATTRIBUTE31,
                st.GENERICATTRIBUTE32,
                st.GENERICNUMBER1,
                (
                    SELECT   /* ORIGSQL: (SELECT name FROM cs_unittype@stelext WHERE UNITTYPESEQ = st.UNITTYPEFORGENERICN(...) */
                        name
                    FROM
                        cs_unittype
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        UNITTYPESEQ = st.UNITTYPEFORGENERICNUMBER1
                        AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                ) AS UNITTYPEFORGENERICNUMBER1,
                st.GENERICNUMBER2,
                (
                    SELECT   /* ORIGSQL: (SELECT name FROM cs_unittype@stelext WHERE UNITTYPESEQ = st.UNITTYPEFORGENERICN(...) */
                        name
                    FROM
                        cs_unittype
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        UNITTYPESEQ = st.UNITTYPEFORGENERICNUMBER2
                        AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                ) AS UNITTYPEFORGENERICNUMBER2,
                st.GENERICNUMBER3,
                (
                    SELECT   /* ORIGSQL: (SELECT name FROM cs_unittype@stelext WHERE UNITTYPESEQ = st.UNITTYPEFORGENERICN(...) */
                        name
                    FROM
                        cs_unittype
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        UNITTYPESEQ = st.UNITTYPEFORGENERICNUMBER3
                        AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                ) AS UNITTYPEFORGENERICNUMBER3,
                st.GENERICNUMBER4,
                (
                    SELECT   /* ORIGSQL: (SELECT name FROM cs_unittype@stelext WHERE UNITTYPESEQ = st.UNITTYPEFORGENERICN(...) */
                        name
                    FROM
                        cs_unittype
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        UNITTYPESEQ = st.UNITTYPEFORGENERICNUMBER4
                        AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                ) AS UNITTYPEFORGENERICNUMBER4,
                st.GENERICNUMBER5,
                (
                    SELECT   /* ORIGSQL: (SELECT name FROM cs_unittype@stelext WHERE UNITTYPESEQ = st.UNITTYPEFORGENERICN(...) */
                        name
                    FROM
                        cs_unittype
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        UNITTYPESEQ = st.UNITTYPEFORGENERICNUMBER5
                        AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                ) AS UNITTYPEFORGENERICNUMBER5,
                st.GENERICNUMBER6,
                (
                    SELECT   /* ORIGSQL: (SELECT name FROM cs_unittype@stelext WHERE UNITTYPESEQ = st.UNITTYPEFORGENERICN(...) */
                        name
                    FROM
                        cs_unittype
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        UNITTYPESEQ = st.UNITTYPEFORGENERICNUMBER6
                        AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                ) AS UNITTYPEFORGENERICNUMBER6,
                st.GENERICDATE1,
                st.GENERICDATE2,
                st.GENERICDATE3,
                st.GENERICDATE4,
                st.GENERICDATE5,
                st.GENERICDATE6,
                st.GENERICBOOLEAN1,
                st.GENERICBOOLEAN2,
                st.GENERICBOOLEAN3,
                st.GENERICBOOLEAN4,
                st.GENERICBOOLEAN5,
                st.GENERICBOOLEAN6
            FROM
                cs_salestransaction st,
                cs_transactionaddress bill,
                stel_data_livechat_eshop x,
                inbound_cfg_parameter par
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                x.service_number = bill.contact -- Service Number
                AND x.dealer_code = st.genericattribute3 -- Dealer Code
                AND x.nric = bill.custid -- CustomerID
                AND X.PRODUCT = st.productid
                AND UPPER(st.genericattribute17) LIKE '%CLOSED%' -- take only Closed Orders only 
                AND st.datasource = 'BCC'
                AND st.salestransactionseq = bill.salestransactionseq
                AND st.billtoaddressseq = bill.transactionaddressseq
                AND par.object_name = 'SP_INBOUND_TXN_MAP'
                AND par.file_name = x.filename
                AND par.file_date = x.filedate
                AND IFNULL(x.recordstatus, 0) = 0  /* ORIGSQL: NVL(x.recordstatus, 0) */
                AND IFNULL(st.genericboolean1, 0) <> 1  /* ORIGSQL: NVL(st.genericboolean1, 0) */
        ) AS src
        ON (src.Serv_number = TGT.SERVICE_NUMBER
            AND src.custid = TGT.NRIC
            AND src.DealerCode = TGT.DEALER_CODE
            AND src.productid = TGT.PRODUCT
            AND IFNULL(tgt.reconciled, 0) = 0  /* ORIGSQL: NVL(tgt.reconciled, 0) */
            AND ABS(MONTHS_BETWEEN(tgt.order_date, tgt.filedate)) < 2)
            AND (tgt.filename, tgt.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT file_name, file_date FROM inbound_cfg_parameter) */
                    file_name,
                    file_date
                FROM
                    ext.inbound_cfg_parameter	
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.genericattribute8 = src.alternateordernumber,
            tgt.genericattribute9 = src.genericattribute9,
            tgt.genericattribute10 = src.genericattribute10,
            tgt.genericattribute11 = src.genericattribute11,
            tgt.genericattribute12 = src.genericattribute12,
            tgt.genericattribute13 = src.genericattribute13,
            tgt.genericattribute14 = src.genericattribute14,
            tgt.genericattribute15 = src.genericattribute15,
            tgt.genericattribute16 = src.genericattribute16,
            tgt.genericattribute17 = src.genericattribute17,
            tgt.genericattribute18 = src.genericattribute18,
            tgt.genericattribute19 = src.genericattribute19,
            tgt.genericattribute20 = src.genericattribute20,
            tgt.genericattribute21 = src.genericattribute21,
            tgt.genericattribute22 = src.genericattribute22,
            tgt.genericattribute23 = src.genericattribute23,
            tgt.genericattribute24 = src.genericattribute24,
            tgt.genericattribute25 = src.genericattribute25,
            tgt.genericattribute26 = src.genericattribute26,
            tgt.genericattribute27 = src.genericattribute27,
            tgt.genericattribute28 = src.genericattribute28,
            tgt.genericattribute29 = src.genericattribute29,
            tgt.genericattribute30 = src.genericattribute30,
            tgt.genericattribute31 = src.genericattribute31,
            tgt.genericattribute32 = src.genericattribute32,
            tgt.genericnumber1 = src.genericnumber1,
            tgt.unittypeforGN1 = src.unittypeforgenericnumber1,
            tgt.genericnumber2 = src.genericnumber2,
            tgt.unittypeforGN2 = src.unittypeforgenericnumber2,
            tgt.genericnumber3 = src.genericnumber3,
            tgt.unittypeforGN3 = src.unittypeforgenericnumber3,
            tgt.genericnumber4 = src.genericnumber4,
            tgt.unittypeforGN4 = src.unittypeforgenericnumber4,
            tgt.genericnumber5 = src.genericnumber5,
            tgt.unittypeforGN5 = src.unittypeforgenericnumber5,
            tgt.genericnumber6 = src.genericnumber6,
            tgt.unittypeforGN6 = src.unittypeforgenericnumber6,
            tgt.genericdate1 = src.genericdate1,
            tgt.genericdate2 = src.genericdate2,
            tgt.genericdate3 = src.genericdate3,
            tgt.genericdate4 = src.genericdate4,
            tgt.genericdate5 = src.genericdate5,
            tgt.genericdate6 = src.genericdate6,
            tgt.genericboolean1 = src.genericboolean1,
            tgt.genericboolean2 = src.genericboolean2,
            tgt.genericboolean3 = src.genericboolean3,
            tgt.genericboolean4 = src.genericboolean4,
            tgt.genericboolean5 = src.genericboolean5,
            tgt.genericboolean6 = src.genericboolean6,
            tgt.reconciled = 1
        /*WHERE
            IFNULL(tgt.reconciled, 0) = 0  -- ORIGSQL: NVL(tgt.reconciled, 0) 
            AND ABS(MONTHS_BETWEEN(tgt.order_date, tgt.filedate)) <
            2
            AND (tgt.filename, tgt.filedate)  
            IN
            (
                SELECT   -- ORIGSQL: (SELECT file_name, file_date FROM inbound_cfg_parameter) 
                    file_name,
                    file_date
                FROM
                    ext.inbound_cfg_parameter)*/
            
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Set Reconcile in STEL_DATA_LIVECHAT_ESHOP :' |(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Set Reconcile in STEL_DATA_LIVECHAT_ESHOP :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Set Reconcile in STEL_DATA_LIVECHAT_ESHOP Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Set Reconcile in STEL_DATA_LIVECHAT_ESHOP :' || v_inbound(...) */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    --STEP 2

    /*check for clawback of previously reconciled data*/ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO STEL_DATA_LIVECHAT_ESHOP tgt USING (SELECT DISTINCT bill.contact AS S(...) */
    MERGE INTO STEL_DATA_LIVECHAT_ESHOP AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_DATA_LIVECHAT_ESHOP' not found */
        USING
        (
            SELECT   /* ORIGSQL: (SELECT DISTINCT bill.contact Serv_number, bill.custid, st.genericattribute3 Dea(...) */
                DISTINCT
                bill.contact AS Serv_number,
                bill.custid,
                st.genericattribute3 AS DealerCode,
                st.productid
            FROM
                cs_salestransaction st,
                cs_transactionaddress bill,
                stel_data_livechat_eshop x,
                inbound_cfg_parameter par
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                x.service_number = bill.contact -- Service Number
                AND x.dealer_code = st.genericattribute3 -- Dealer Code
                AND x.nric = bill.custid -- CustomerID
                AND X.PRODUCT = st.productid
                -- AND UPPER (st.genericattribute17) LIKE '%CLOSED%' -- take only Closed Orders only
                AND st.datasource = 'BCC'
                AND st.salestransactionseq = bill.salestransactionseq
                AND st.billtoaddressseq = bill.transactionaddressseq
                AND par.object_name = 'SP_INBOUND_TXN_MAP'
                AND par.file_name = x.filename
                AND par.file_date = x.filedate
                AND IFNULL(x.recordstatus, 0) = 0  /* ORIGSQL: NVL(x.recordstatus, 0) */
                AND (IFNULL(st.genericboolean1, 0) = 1  /* ORIGSQL: NVL(st.genericboolean1, 0) */
                    OR IFNULL(st.numberofunits, 0) < 0)  /* ORIGSQL: NVL(st.numberofunits, 0) */
        ) AS src
        ON (src.Serv_number = TGT.SERVICE_NUMBER
            AND src.custid = TGT.NRIC
            AND src.DealerCode = TGT.DEALER_CODE
            AND src.productid = TGT.PRODUCT
        	AND tgt.reconciled = 1
            AND ABS(MONTHS_BETWEEN(tgt.order_date, tgt.filedate)) < 2)
            AND (tgt.filename, tgt.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT file_name, file_date FROM inbound_cfg_parameter) */
                    file_name,
                    file_date
                FROM
                    inbound_cfg_parameter
        )
    WHEN MATCHED THEN
        UPDATE SET
            TGT.RECONCILED = -1, TGT.GENERICBOOLEAN1 = 1
        /*WHERE
            tgt.reconciled = 1
            AND ABS(MONTHS_BETWEEN(tgt.order_date, tgt.filedate)) <
            2
            AND (tgt.filename, tgt.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT file_name, file_date FROM inbound_cfg_parameter) 
                    file_name,
                    file_date
                FROM
                    inbound_cfg_parameter) */
            
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Clawback of previously reconciled data :' || v(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Clawback of previously reconciled data :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Clawback of previously reconciled data Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Clawback of previously reconciled data :' || v_inbound_cf(...) */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Step 3

    -- Calling Txn mapping for stage2
    /*sp_inbound_Validator (v_inbound_cfg_parameter.file_type,
                             v_inbound_cfg_parameter.file_name,
                         v_inbound_cfg_parameter.file_date,2);*/

    /* ORIGSQL: sp_inbound_txn_map (v_inbound_cfg_parameter.file_type, v_inbound_cfg_parameter.f(...) */
    CALL EXT.SP_INBOUND_TXN_MAP(:v_inbound_cfg_parameter.file_type, :v_inbound_cfg_parameter.file_name, :v_inbound_cfg_parameter.file_date, 2);
END