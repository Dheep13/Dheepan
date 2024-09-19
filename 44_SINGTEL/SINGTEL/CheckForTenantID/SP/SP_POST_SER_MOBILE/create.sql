CREATE PROCEDURE ext.SP_POST_SER_MOBILE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA ext
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_samplefilename VARCHAR(255) = 'SER-Mobile-Transactions';  /* ORIGSQL: v_samplefilename VARCHAR(255) := 'SER-Mobile-Transactions'; */

    DECLARE v_parameter        ROW LIKE ext.Inbound_cfg_Parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'ext.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_CFG_PARAMETER' not found */

    SELECT
        DISTINCT
        *
    INTO
        v_parameter
    FROM
        ext.Inbound_cfg_Parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_DATA_ASSIGNMENT' not found */
    /* ORIGSQL: UPDATE ext.inbound_data_assignment SET filename = v_samplefilename WHERE filename = (...) */
    UPDATE ext.inbound_data_assignment
        SET
        /* ORIGSQL: filename = */
        filename = :v_samplefilename
    FROM
        ext.inbound_data_assignment
    WHERE
        filename = :v_parameter.file_name
        AND filedate = :v_parameter.file_date
        AND recordstatus = 0;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: sp_inbound_txn_map (v_parameter.file_type, v_parameter.file_name, v_parameter.fi(...) */
    
    
    
    CALL ext.SP_INBOUND_TXN_MAP(:v_parameter.file_type, :v_parameter.file_name, :v_parameter.file_date, 2);   

    /* ORIGSQL: UPDATE inbound_data_assignment SET filename = v_parameter.file_name WHERE filena(...) */
    UPDATE ext.inbound_data_assignment
        SET
        /* ORIGSQL: filename = */
        filename = :v_parameter.file_name
    FROM
        ext.inbound_data_assignment
    WHERE
        filename = :v_samplefilename
        AND filedate = :v_parameter.file_date
        AND recordstatus = 0;

    /* ORIGSQL: COMMIT; */
    COMMIT;
END