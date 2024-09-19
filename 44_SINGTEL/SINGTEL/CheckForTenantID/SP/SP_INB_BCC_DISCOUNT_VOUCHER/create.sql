CREATE PROCEDURE EXT.SP_INB_BCC_DISCOUNT_VOUCHER
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE v_prmtr ROW LIKE inbound_cfg_parameter;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    IF UPPER(:v_prmtr.FILE_TYPE) = 'BCC-SCII-DISCOUNTINFO' 
    THEN
        /* RESOLVE: Oracle Database link: Remote table/view 'stelext.STEL_DATA_DISCOUNT@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'stelext.STEL_DATA_DISCOUNT_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        /* ORIGSQL: DELETE FROM stelext.STEL_DATA_DISCOUNT@stelext WHERE filename = v_prmtr.FILE_NAM(...) */
        DELETE
        FROM
            ext.STEL_DATA_DISCOUNT
        WHERE
            filename = :v_prmtr.FILE_NAME;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: Oracle Database link: Remote table/view 'stelext.STEL_DATA_DISCOUNT@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'stelext.STEL_DATA_DISCOUNT_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        /* ORIGSQL: INSERT INTO stelext.STEL_DATA_DISCOUNT@stelext SELECT * FROM STEL_DATA_DISCOUNT (...) */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_DATA_DISCOUNT' not found */
        INSERT INTO ext.STEL_DATA_DISCOUNT
            SELECT   /* ORIGSQL: SELECT * FROM STEL_DATA_DISCOUNT WHERE RECORDSTATUS = 0 AND filename = v_prmtr.F(...) */
                *
            FROM
                EXT.STEL_DATA_DISCOUNT
            WHERE
                RECORDSTATUS = 0
                AND filename = :v_prmtr.FILE_NAME
                AND filedate = :v_prmtr.FILE_DATE;

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END IF;

    IF UPPER(:v_prmtr.FILE_TYPE) = 'BCC-SCII-VOUCHERINFO' 
    THEN
        /* RESOLVE: Oracle Database link: Remote table/view 'stelext.STEL_DATA_VOUCHER@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'stelext.STEL_DATA_VOUCHER_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        /* ORIGSQL: DELETE FROM stelext.STEL_DATA_VOUCHER@stelext WHERE filename = v_prmtr.FILE_NAME(...) */
        DELETE
        FROM
         ext.STEL_DATA_VOUCHER
        WHERE
            filename = :v_prmtr.FILE_NAME;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* RESOLVE: Oracle Database link: Remote table/view 'stelext.STEL_DATA_VOUCHER@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'stelext.STEL_DATA_VOUCHER_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        /* ORIGSQL: INSERT INTO stelext.STEL_DATA_VOUCHER@stelext SELECT * FROM STEL_DATA_VOUCHER WH(...) */
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_DATA_VOUCHER' not found */
        INSERT INTO ext.STEL_DATA_VOUCHER
            SELECT   /* ORIGSQL: SELECT * FROM STEL_DATA_VOUCHER WHERE RECORDSTATUS = 0 AND filename = v_prmtr.FI(...) */
                *
            FROM
                EXT.STEL_DATA_VOUCHER
            WHERE
                RECORDSTATUS = 0
                AND filename = :v_prmtr.FILE_NAME
                AND filedate = :v_prmtr.FILE_DATE;

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END IF;
END