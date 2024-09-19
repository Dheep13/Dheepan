CREATE PROCEDURE EXT.INBOUND_POST_UPDATEBU_PAYEE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */
    DECLARE v_offset DECIMAL(38,10);  /* ORIGSQL: v_offset number ; */
    DECLARE v_productid VARCHAR(200);  /* ORIGSQL: v_productid VARCHAR2(200); */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_DEL';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_DEL'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */
    DECLARE v_Eot TIMESTAMP = to_date('01-jan-2200','dd-mon-yyyy');  /* ORIGSQL: v_Eot date:=to_Date('01-jan-2200','dd-mon-yyyy') ; */

    --v_inbound_cfg_parameter INBOUND_CFG_PARAMETER%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    DECLARE v_inbound_cfg_parameter ROW LIKE EXT.INBOUND_CFG_PARAMETER;

    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY'); Sanjay commented out

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_ogpo tgt using (SELECT MAX(bu.name) AS bu, t.name FROM D(...) */
    MERGE INTO ext.inbound_data_ogpo AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select MAX(bu.name) bu, t.name from cs_title@stelext t join cs_ruleelementowner(...) */
                MAX(bu.name) AS bu,
                t.name
            FROM
                cs_title t
            INNER JOIN
                cs_ruleelementowner r
                ON t.ruleelementownerseq = r.ruleelementownerseq
                AND r.effectivestartdate = t.effectivestartdate
            INNER JOIN
                cs_businessunit bu
                ON BITAND(bu.mask,r.businessunitmap) = 1
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_title_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_ruleelementowner@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_ruleelementowner_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_businessunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_businessunit_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                t.removedate = :v_Eot
                AND r.removedate = :v_Eot
                AND t.effectiveenddate = :v_Eot
            GROUP BY
                t.name
        ) AS src
        ON (src.name = tgt.titlename AND tgt.recordstatus = 0 AND (tgt.filename, tgt.filedate) IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            )
            --AND tgt.recordstatus = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.businessunitname = src.bu
        --WHERE
        --   (tgt.filename, tgt.filedate)  
        --    IN
        --   (
        --        SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
        --            File_name,
        --            File_date
        --        FROM
        --            ext.inbound_cfg_parameter
        --       WHERE
        --            OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
        --    )
            --AND tgt.recordstatus = 0
           ;

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_OGPT' not found */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_ogpt tgt using (SELECT MAX(businessunitname) AS bu, x.pa(...) */
    MERGE INTO ext.inbound_data_ogpt AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_OGPO' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select MAX(businessunitname) bu, x.payeeid from inbound_data_ogpo x where (x.fi(...) */
                MAX(businessunitname) AS bu,
                x.payeeid
            FROM
                ext.inbound_data_ogpo x
            WHERE
                (x.filename, x.filedate)  
                IN
                (
                    SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                        File_name,
                        File_date
                    FROM
                        ext.inbound_cfg_parameter
                    WHERE
                        OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
                )
                AND x.recordstatus = 0
                group by x.payeeid
        ) AS src
        ON (src.payeeid = tgt.payeeid AND (tgt.filename, tgt.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            )
            AND tgt.recordstatus = 0)
    WHEN MATCHED THEN
        UPDATE
            SET tgt.businessunitname = src.bu
       -- WHERE
        --    (tgt.filename, tgt.filedate)  
        --    IN
        --    (
        --        SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
        --            File_name,
        --            File_date
        --        FROM
        --            inbound_cfg_parameter
        --        WHERE
        --            OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
        --    )
        --    AND tgt.recordstatus = 0
            ;

    /* ORIGSQL: commit; */
    COMMIT;
END