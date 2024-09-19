CREATE PROCEDURE EXT.SP_INBOUND_SCII_DASH_MERGE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm VARCHAR2(4000); */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_SCII_DASH_MERGE';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_SCII_DASH_MERGE'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    DECLARE v_prmtr ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'SP_INBOUND_SCII_DASH_MERGE :' || :v_prmtr.file_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'SP_INBOUND_SCII_DASH_MERGE  :'|| IFNULL(:v_prmtr.file_type,'') || '-FileName:'|| IFNULL(:v_prmtr.file_name,'') || '-Date:'|| IFNULL(:v_prmtr.file_date,''),1,255) 
        , 'SP_INBOUND_SCII_DASH_MERGE Execution Started', 0, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'SP_INBOUND_SCII_DASH_MERGE  :' || :v_prmtr.file_type || '-(...) */

    --insert into test_debug values(:v_prmtr.file_name||'-'||v_prmtr.file_date,systimestamp);
    --commit;

    --insert into test_mergeselect
    --   select sum(to_number(topup.crd_amt)) total_amt, txn.BilltoPhone, txn.compensationdate from STEL_DATA_DASHTOPUP topup,
    --    STEL_DATA_DASHSIGNUP signup, inbound_data_txn txn
    --    where nvl(topup.recordstatus,'*')='0'
    -- and nvl(signup.recordstatus,'*')='0'
    -- and topup.msisdn = signup.msisdn
    -- and topup.txn_dt = signup.registered_date  --[Arun: Commented this on 8th May as Date format doesn't match]
    -- and to_char(topup.txn_dt,'DD-MON-YYYY') = substr(to_char(signup.registered_date,'DD-MON-YY'),1,7)||
    --        substr(sysdate,7,2)||substr(to_char(signup.registered_date,'DD-MON-YY'),8,2)  --[Arun: Added this on 8th May as Date format doesn't match]
    -- and to_char(topup.txn_dt,'yyyymmdd') = to_char(txn.compensationdate,'yyyymmdd')
    -- and topup.msisdn = txn.BilltoPhone
    -- and nvl(txn.recordstatus,'*')='0'
    -- and txn.filename = :v_prmtr.file_name
    -- and txn.filedate = :v_prmtr.file_date
    -- and topup.filedate = :v_prmtr.file_date --[Arun added as values getting aggrefated with each file load]
    -- and signup.filedate = :v_prmtr.file_date --[Arun added as values getting aggrefated with each file load]
    --   group by txn.BilltoPhone, txn.compensationdate;

    --  commit; 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_txn tgt using (SELECT SUM(to_number(topup.crd_amt)) AS t(...) */
    MERGE INTO inbound_data_txn AS tgt
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_DASHTOPUP' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_DASHSIGNUP' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_TXN' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select SUM(to_number(topup.crd_amt)) total_amt, txn.BilltoPhone, txn.compensati(...) */
                SUM(TO_DECIMAL(topup.crd_amt,38,18)) AS total_amt,  
                txn.BilltoPhone,
                txn.compensationdate
            FROM
                STEL_DATA_DASHTOPUP topup,
                STEL_DATA_DASHSIGNUP signup,
                inbound_data_txn txn
            WHERE
                IFNULL(topup.recordstatus,'*') = '0'  /* ORIGSQL: nvl(topup.recordstatus,'*') */
                AND IFNULL(signup.recordstatus,'*') = '0'  /* ORIGSQL: nvl(signup.recordstatus,'*') */
                AND topup.msisdn = signup.msisdn
                AND topup.txn_dt = signup.registered_date  --[Arun: Commented this on 8th May as Date format doesn't match]
                -- and to_char(topup.txn_dt,'DD-MON-YYYY') = substr(to_char(signup.registered_date,'DD-MON-YY'),1,7)||
                --        substr(sysdate,7,2)||substr(to_char(signup.registered_date,'DD-MON-YY'),8,2)  --[Arun: Added this on 8th May as Date format doesn't match]
                AND TO_VARCHAR(topup.txn_dt,'yyyymmdd') = TO_VARCHAR(txn.compensationdate,'yyyymmdd')  /* ORIGSQL: to_char(txn.compensationdate,'yyyymmdd') */
                                                                                                       /* ORIGSQL: to_char(topup.txn_dt,'yyyymmdd') */
                AND topup.msisdn = txn.BilltoPhone
                AND IFNULL(txn.recordstatus,'*') = '0'  /* ORIGSQL: nvl(txn.recordstatus,'*') */
                AND txn.filename = :v_prmtr.file_name
                AND txn.filedate = :v_prmtr.file_date
                AND topup.filedate = :v_prmtr.file_date --[Arun added as values getting aggrefated with each file load]
                AND signup.filedate = :v_prmtr.file_date --[Arun added as values getting aggrefated with each file load]
            GROUP BY
                txn.BilltoPhone, txn.compensationdate
        ) AS src
        ON (tgt.BilltoPhone = src.BilltoPhone
        AND tgt.compensationdate = src.compensationdate
        AND tgt.filename = :v_prmtr.file_name
        AND tgt.filedate = :v_prmtr.file_date
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.value = src.total_amt,
            tgt.genericnumber3 = src.total_amt,UNITTYPEFORGENERICNUMBER3 = 'SGD';

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    --  insert into test_debug values(v_rowcount,systimestamp);

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'SP_INBOUND_SCII_DASH_MERGE :' || :v_prmtr.file_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'SP_INBOUND_SCII_DASH_MERGE  :'|| IFNULL(:v_prmtr.file_type,'') || '-FileName:'|| IFNULL(:v_prmtr.file_name,'') || '-Date:'|| IFNULL(:v_prmtr.file_date,''),1,255) 
        , 'SP_INBOUND_SCII_DASH_MERGE Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'SP_INBOUND_SCII_DASH_MERGE  :' || :v_prmtr.file_type || '-(...) */

    --    exception when others then
    --    v_sqlerrm := SUBSTR (SQLERRM, 1, 4000);
    --
    --    insert into outbound_log_details  (file_type,
        --    file_name,
        --    file_date,
    --    exception_message)
    --    values (
        --    :v_prmtr.file_type,
        --    :v_prmtr.file_name,
        --    :v_prmtr.file_date,
        --    v_sqlerrm
    --    );
    --
    --    Commit;
    ----    raise;
END