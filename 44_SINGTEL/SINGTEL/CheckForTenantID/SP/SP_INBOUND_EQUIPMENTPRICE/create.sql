CREATE PROCEDURE EXT.SP_INBOUND_EQUIPMENTPRICE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
   DECLARE v_prmtr ROW LIKE inbound_cfg_parameter; --%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* RESOLVE: Oracle Database link: Remote table/view 'stelext.SAP_EQUIPMENT_PRICE@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'stelext.SAP_EQUIPMENT_PRICE_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: DELETE FROM stelext.SAP_EQUIPMENT_PRICE@stelext WHERE filename = v_prmtr.FILE_NA(...) */
    DELETE
    FROM
       ext.SAP_EQUIPMENT_PRICE
    WHERE
        filename = :v_prmtr.FILE_NAME;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* RESOLVE: Oracle Database link: Remote table/view 'stelext.SAP_EQUIPMENT_PRICE@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'stelext.SAP_EQUIPMENT_PRICE_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: INSERT INTO stelext.SAP_EQUIPMENT_PRICE@stelext SELECT * FROM SAP_EQUIPMENT_PRIC(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.SAP_EQUIPMENT_PRICE' not found */
    INSERT INTO ext.SAP_EQUIPMENT_PRICE
        SELECT   /* ORIGSQL: SELECT * FROM SAP_EQUIPMENT_PRICE WHERE RECORDSTATUS = 0 AND filename = v_prmtr.(...) */
            *
        FROM
            ext.SAP_EQUIPMENT_PRICE
        WHERE
            RECORDSTATUS = 0
            AND filename = :v_prmtr.FILE_NAME
            AND filedate = :v_prmtr.FILE_DATE;

    /* ORIGSQL: COMMIT; */
    COMMIT;
END