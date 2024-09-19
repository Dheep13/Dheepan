CREATE PROCEDURE EXT.SP_INBOUND_PRE_LEAVE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_LEAVE' not found */

    /* ORIGSQL: delete from inbound_Data_leave where elasid in (SELECT FIELD8 FROM inbound_Data_(...) */
    DELETE
    FROM
        ext.inbound_Data_leave
    WHERE
        elasid
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
        IN
        (
            SELECT   /* ORIGSQL: (Select FIELD8 from inbound_Data_staging) */
                FIELD8
            FROM
                ext.inbound_Data_staging
        );

    /* ORIGSQL: commit; */
    COMMIT;
END