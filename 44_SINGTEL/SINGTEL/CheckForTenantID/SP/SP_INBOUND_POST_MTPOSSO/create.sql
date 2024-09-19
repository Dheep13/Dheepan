CREATE PROCEDURE EXT.SP_INBOUND_POST_MTPOSSO
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cutoffday DECIMAL(38,10);  /* ORIGSQL: v_cutoffday NUMBER; */
    DECLARE v_oppr ROW LIKE inbound_cfg_BCC_Txn;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_BCC_Txn' not found (for %ROWTYPE declaration) */
    DECLARE v_maxseq DECIMAL(38,10);  /* ORIGSQL: v_maxseq number; */
    DECLARE v_clawbackperiod DECIMAL(38,10);  /* ORIGSQL: v_clawbackperiod number; */
    DECLARE v_filedate TIMESTAMP;  /* ORIGSQL: v_filedate date; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_MTPOSSO';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_MTPOSSO'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */
    DECLARE v_prmtr ROW LIKE inbound_cfg_parameter;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('*****************');

    /* ORIGSQL: dbms_output.put_line ('Start Post MTPOS SalesOrder'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Start Post MTPOS SalesOrder');

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('*****************');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    SELECT
        file_Date/* -- to_char(file_Date,'YYYYMMDD') */
    INTO
        v_filedate
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    --For MMP Smart Home Txns, mark all as KIV. The install file will come in later and update these txns   
    /* ORIGSQL: update inbound_data_txn SET comments = 'No Match Found in Vendor File', channel=(...) */
    UPDATE ext.inbound_data_txn
        SET
        /* ORIGSQL: comments = */
        comments = 'No Match Found in Vendor File',
        /* ORIGSQL: channel = */
        channel = 'SMARTHOME' 
    FROM
        ext.inbound_data_txn
    WHERE
        productname IN
        (
            SELECT   /* ORIGSQL: (select dim0 from stel_lookup@stelext where name='LT_MMP_Smart Home Acc') */
                dim0
            FROM
                EXT.stel_lookup
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_lookup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_lookup_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                name = 'LT_MMP_Smart Home Acc'
        )
        AND filename = :v_prmtr.file_name
        AND recordstatus = 0;

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_TXN' not found */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO inbound_data_txn d USING stel_lookup@stelext s ON (filename = v_prmtr(...) */
    MERGE INTO ext.inbound_data_txn AS d
        USING ext.stel_lookup AS s
        ON (filename = :v_prmtr.file_name
            AND recordstatus = 0
            AND s.name = 'LT_StockCode_Accessories'
        AND s.DIM0 = d.productname)
    WHEN MATCHED THEN
        UPDATE SET d.value = s.STRINGVALUE;

    /* ORIGSQL: commit; */
    COMMIT;

    /*For Smart Home for Internal: Create txn with 'SmartHome Submitted' event type
    The material group/dept have to be filtered
    */

    /* RESOLVE: Oracle Database link: Remote table/view 'stelext.stel_data_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'stelext.stel_data_Salesorder_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: DELETE FROM stelext.stel_data_Salesorder@stelext WHERE filename = v_prmtr.FILE_N(...) */
    DELETE
    FROM
        ext.stel_data_Salesorder
    WHERE
        filename = :v_prmtr.FILE_NAME;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_data_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_data_Salesorder_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: insert into stel_data_Salesorder@stelext select * from stel_Data_Salesorder wher(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_DATA_SALESORDER' not found */
    INSERT INTO EXT.stel_data_Salesorder
        SELECT   /* ORIGSQL: select * from stel_Data_Salesorder where recordstatus=0 and filename = v_prmtr.F(...) */
            *
        FROM
            ext.stel_Data_Salesorder
        WHERE
            recordstatus = 0
            AND filename = :v_prmtr.FILE_NAME;

    /* ORIGSQL: commit; */
    COMMIT;
END