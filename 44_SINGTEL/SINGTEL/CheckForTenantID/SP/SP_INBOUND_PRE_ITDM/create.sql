CREATE PROCEDURE EXT.SP_INBOUND_PRE_ITDM
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_ITDM';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_ITDM'; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
    /* ORIGSQL: update inbound_data_Staging SET field100=substr(field3,13, length(field3)) where(...) */
    UPDATE ext.inbound_data_Staging
        SET
        /* ORIGSQL: field100 = */
        field100 = SUBSTRING(field3,13,LENGTH(field3))  /* ORIGSQL: substr(field3,13, length(field3)) */
    FROM
        ext.inbound_data_Staging
    WHERE
        field3 LIKE '99%'
        AND SUBSTRING(field3,13,LENGTH(field3)) IN  /* ORIGSQL: substr(field3,13, length(field3)) */
        (
            SELECT   /* ORIGSQL: (select p.payeeid from cs_payee@stelext p join cs_position@stelext pos on pos.pa(...) */
                p.payeeid
            FROM
                cs_payee p
            INNER JOIN
                cs_position pos
                ON pos.payeeseq = p.payeeseq
            INNER JOIN
                cs_title t
                ON t.ruleelementownerseq = pos.titleseq
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_title_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_payee@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_payee_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                t.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND t.effectiveenddate >CURRENT_TIMESTAMP   --and p_txnmonth>=t.effectivestartdate
                /* ORIGSQL: sysdate */
                AND p.effectiveenddate >CURRENT_TIMESTAMP   --and p_txnmonth>=p.effectivestartdate
                /* ORIGSQL: sysdate */
                AND pos.effectiveenddate >CURRENT_TIMESTAMP  -- and p_txnmonth>=pos.effectivestartdate
                /* ORIGSQL: sysdate */
                AND t.name LIKE 'Pick%Go%'
        );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert RMT0081 records :' || v_inbound_cfg_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert RMT0081 records :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Insert RMT0081 records Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert RMT0081 records :' || v_inbound_cfg_parameter.file(...) */

    /* ORIGSQL: commit; */
    COMMIT;
END