CREATE PROCEDURE EXT.SP_INBOUND_PRE_IDA
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_IDA';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_IDA'; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        INBOUND_CFG_PARAMETER;

    /* ORIGSQL: execute immediate 'truncate table stel_work_rmt0081'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_WORK_RMT0081' not found */

    /* ORIGSQL: truncate table stel_work_rmt0081 ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_work_rmt0081';

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: insert into stel_Work_rmt0081 select contact, to_Char(st.compensationdate,'YYYYM(...) */
    INSERT INTO stel_Work_rmt0081
        SELECT   /* ORIGSQL: select contact, to_Char(st.compensationdate,'YYYYMM') txnmonth, st.genericattrib(...) */
            contact,
            TO_VARCHAR(st.compensationdate,'YYYYMM') AS txnmonth,  /* ORIGSQL: to_Char(st.compensationdate,'YYYYMM') */
            IFNULL(st.genericattribute3,'')||'_'||IFNULL(st.genericattribute4,'') AS vendor_dealer
        FROM
           vw_salestransaction st
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select * from stel_Classifier@stelext where categoryname='IDA Flag') */
                    *
                FROM
                    ext.stel_Classifier
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_Classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_Classifier_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    categoryname = 'IDA Flag'
            ) AS cl
            ON st.compensationdate BETWEEN cl.effectivestartdate AND Add_Days(cl.effectiveenddate,-1)
            AND cl.genericnumber1 = 1
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.vw_salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.vw_salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            st.datasource = 'RMT0081'
            AND TO_VARCHAR(st.compensationdate,'YYYYMM') IN  /* ORIGSQL: to_Char(st.compensationdate,'YYYYMM') */
            (
                SELECT   /* ORIGSQL: (select to_char(to_date(substr(field21,1,10),'DD/MM/YYYY'),'YYYYMM') from inboun(...) */
                    TO_VARCHAR(to_date(SUBSTRING(field21,1,10),'DD/MM/YYYY'),'YYYYMM')   
                FROM
                    inbound_data_Staging
            );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert RMT0081 records :' || v_inbound_cfg_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert RMT0081 records :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Insert RMT0081 records Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert RMT0081 records :' || v_inbound_cfg_parameter.file(...) */

    /* ORIGSQL: update inbound_Data_staging tgt SET error_flag=1, error_message='No correspondin(...) */
    UPDATE ext.inbound_Data_staging tgt
        SET
        /* ORIGSQL: error_flag = */
        error_flag = 1,
        /* ORIGSQL: error_message = */
        error_message = 'No corresponding RMT0081 transaction found' 
    FROM
        ext.inbound_Data_staging tgt
    WHERE
        (field1, TO_VARCHAR(to_date(SUBSTRING(field21,1,10),'DD/MM/YYYY'),'YYYYMM')) NOT  /* ORIGSQL: to_char(to_date(substr(field21,1,10),'DD/MM/YYYY'),'YYYYMM') */
        IN
        (
            SELECT   /* ORIGSQL: (select contact, txnmonth from stel_Work_rmt0081) */
                contact,
                txnmonth
            FROM
                stel_Work_rmt0081
        );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Set Error Flag=1:' || v_inbound_cfg_parameter.(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Set Error Flag=1:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Set Error Flag=1 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Set Error Flag=1:' || v_inbound_cfg_parameter.file_type |(...) */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Staging t using stel_work_rmt0081 s on (t.field1=s.conta(...) */
    MERGE INTO ext.inbound_Data_Staging AS t
        USING stel_work_rmt0081 s
        ON (t.field1 = s.contact
            AND TO_VARCHAR(to_date(SUBSTRING(t.field21,1,10),'DD/MM/YYYY'),'YYYYMM') = s.txnmonth)  /* ORIGSQL: to_char(to_date(substr(t.field21,1,10),'DD/MM/YYYY'),'YYYYMM') */
    WHEN MATCHED THEN
        UPDATE SET
            t.field23 = s.vendor_dealer;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Set Vendor_Dealer:' || v_inbound_cfg_parameter(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Set Vendor_Dealer:'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Set Vendor_Dealer Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Set Vendor_Dealer:' || v_inbound_cfg_parameter.file_type (...) */

    /* ORIGSQL: commit; */
    COMMIT;
END