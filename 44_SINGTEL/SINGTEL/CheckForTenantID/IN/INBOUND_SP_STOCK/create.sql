CREATE PROCEDURE EXT.INBOUND_SP_STOCK
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_proc_name VARCHAR(127) = 'INBOUND_SP_STOCK';  /* ORIGSQL: v_proc_name varchar2(127):='INBOUND_SP_STOCK'; */
    --DECLARE v_parameter Inbound_cfg_Parameter%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_parameter ROW LIKE Inbound_cfg_Parameter;
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    --v_inb_param   INBOUND_CFG_PARAMETER%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    DECLARE v_inb_param ROW LIKE INBOUND_CFG_PARAMETER;
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT
        DISTINCT
        *
    INTO
        v_inb_param
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_CLPR' not found */
    /* ORIGSQL: UPDATE inbound_Data_clpr icl SET icl.recordstatus = (SELECT -1 FROM DBMTK_USER_N(...) */
    UPDATE EXT.inbound_Data_clpr icl
        SET
        /* ORIGSQL: icl.recordstatus = */
        recordstatus = (
            SELECT   /* ORIGSQL: (SELECT -1 FROM stel_classifier@stelext lc WHERE lc.classifierid = icl.productid(...) */
                -1
            FROM
                EXT.stel_classifier lc
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_classifier_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                lc.classifierid = icl.productid
                AND lc.effectivestartdate = icl.effectivestartdate
                AND lc.categorytreename = icl.categorytreename
                AND lc.categoryname = icl.categoryname
                AND lc.genericattribute1 = icl.genericattribute1
                AND lc.genericattribute2 = icl.genericattribute2
                AND lc.genericattribute3 = icl.genericattribute3
                AND lc.genericattribute4 = icl.genericattribute4
        )
    WHERE
        FILENAME = :v_inb_param.FILE_NAME
        AND FILEDATE = :v_inb_param.FILE_DATE
        AND (RECORDSTATUS = '0'
        OR RECORDSTATUS IS NULL);

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || ' Record Status Update :' || v_inb_param.file_t(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || ' Record Status Update :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'INBOUND_SP_STOCK Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || ' Record Status Update :' || v_inb_param.file_type || '-Fi(...) */

    /* ORIGSQL: UPDATE inbound_Data_clpr icl SET icl.recordstatus ='0' where icl.recordstatus is(...) */
    UPDATE EXT.inbound_Data_clpr icl
        SET
        /* ORIGSQL: icl.recordstatus = */
        recordstatus = '0' 
    WHERE
        icl.recordstatus IS NULL;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || ' Record Status Update 2 :' || v_inb_param.file(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || ' Record Status Update 2 :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'INBOUND_SP_STOCK Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || ' Record Status Update 2 :' || v_inb_param.file_type || '-(...) */
END