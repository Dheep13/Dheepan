CREATE PROCEDURE EXT.INBOUND_PRE_UPD_BCC_SCENARIO (IN in_str NVARCHAR(5000))
LANGUAGE SQLSCRIPT
AS
BEGIN
    DECLARE v_prmtr ROW LIKE EXT.INBOUND_CFG_PARAMETER;
    DECLARE v_sql NVARCHAR(5000);
    DECLARE v_TgtTable NVARCHAR(30) DEFAULT 'INBOUND_DATA_STAGING';
    DECLARE v_proc_name NVARCHAR(127) DEFAULT 'INBOUND_PRE_UPD_BCC_SCENARIO';
    DECLARE v_parameter ROW LIKE EXT.INBOUND_CFG_PARAMETER;
    DECLARE v_rowcount INTEGER;

    DECLARE v_tab TABLE (
        ORDER_TYPE NVARCHAR(255),
        TRANSACTION_TYPE NVARCHAR(255),
        SUBTRANSACTION_TYPE NVARCHAR(255),
        RECON_FLAG NVARCHAR(255),
        SERVICE_TYPE NVARCHAR(255),
        COMPONENT_STATUS NVARCHAR(255),
        ORDER_LINE_TYPE NVARCHAR(255),
        GF1 NVARCHAR(255),
        GF2 NVARCHAR(255),
        GF3 NVARCHAR(255),
        GF4 NVARCHAR(255),
        GF5 NVARCHAR(255),
        SCENARIO NVARCHAR(255)
    );
    DECLARE v_tab_row ROW LIKE :v_tab;

    DECLARE v_nvl NVARCHAR(10) DEFAULT '*';

    SELECT DISTINCT *
    INTO v_parameter
    FROM EXT.INBOUND_CFG_PARAMETER;

    INSERT INTO :v_tab VALUES (
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 1),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 2),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 3),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 4),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 5),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 6),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 7),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 8),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 9),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 10),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 11),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 12),
        SUBSTR_REGEXPR('([^,]+)' IN in_str FROM 1 OCCURRENCE 13)
    );

    SELECT *
    INTO v_prmtr
    FROM EXT.INBOUND_CFG_PARAMETER
    WHERE OBJECT_NAME = 'SP_INBOUND_TXN_MAP';

    
    SELECT * INTO v_tab_row FROM :v_tab;

    v_sql := 'UPDATE ' || :v_TgtTable || ' SET SCENARIO = (SELECT MAX(SCENARIO) FROM INBOUND_CFG_BCCSCNEARIOS WHERE ' ||
             'ORDER_TYPE = NVL(''' || :v_tab_row.ORDER_TYPE || ''', ''' || :v_nvl || ''') AND ' ||
             'TRANSACTION_TYPE = NVL(''' || :v_tab_row.TRANSACTION_TYPE || ''', ''' || :v_nvl || ''') AND ' ||
             '(SUBTRANSACTION_TYPE = ''*'' OR SUBTRANSACTION_TYPE = NVL(''' || :v_tab_row.SUBTRANSACTION_TYPE || ''', ''' || :v_nvl || ''')) AND ' ||
             '(RECON_FLAG = ''*'' OR RECON_FLAG = NVL(''' || :v_tab_row.RECON_FLAG || ''', ''' || :v_nvl || ''')) AND ' ||
             '(SERVICE_TYPE = ''*'' OR SERVICE_TYPE = NVL(''' || :v_tab_row.SERVICE_TYPE || ''', ''' || :v_nvl || ''')) AND ' ||
             '(COMPONENT_STATUS = ''*'' OR COMPONENT_STATUS = NVL(''' || :v_tab_row.COMPONENT_STATUS || ''', ''' || :v_nvl || ''')) AND ' ||
             '(ORDER_LINE_TYPE = ''*'' OR ORDER_LINE_TYPE = NVL(''' || :v_tab_row.ORDER_LINE_TYPE || ''', ''' || :v_nvl || ''')) AND ' ||
             '(GENERICFIELD1 = ''*'' OR GENERICFIELD1 = NVL(''' || :v_tab_row.GF1 || ''', ''' || :v_nvl || ''')) AND ' ||
             '(GENERICFIELD2 = ''*'' OR GENERICFIELD2 = NVL(''' || :v_tab_row.GF2 || ''', ''' || :v_nvl || ''')) AND ' ||
             '(GENERICFIELD3 = ''*'' OR GENERICFIELD3 = NVL(''' || :v_tab_row.GF3 || ''', ''' || :v_nvl || ''')) AND ' ||
             '(GENERICFIELD4 = ''*'' OR GENERICFIELD4 = NVL(''' || :v_tab_row.GF4 || ''', ''' || :v_nvl || ''')) AND ' ||
             '(GENERICFIELD5 = ''*'' OR GENERICFIELD5 = NVL(''' || :v_tab_row.GF5 || ''', ''' || :v_nvl || ''')) AND ' ||
             'FILE_TYPE IN (SELECT FILE_TYPE FROM EXT.INBOUND_CFG_PARAMETER)) ' ||
             'WHERE (FILETYPE, FILENAME, FILEDATE) IN (SELECT FILE_TYPE, FILE_NAME, FILE_DATE FROM EXT.INBOUND_CFG_PARAMETER WHERE OBJECT_NAME = ''SP_INBOUND_TXN_MAP'')';

    -- Output the SQL for debugging purposes
    CALL SQLSCRIPT_PRINT:PRINT_LINE (:v_sql);

    -- Execute the dynamic SQL
    EXEC :v_sql;

    -- Get the affected row count
     v_rowcount= ::ROWCOUNT;

    -- Log the execution details
    CALL EXT.STEL_SP_LOGGER(
        LEFT(:v_proc_name || ' Pre Update BCC Scenario :' || :v_parameter.FILE_TYPE || '-FileName:' || :v_parameter.FILE_NAME || '-Date:' || :v_parameter.FILE_DATE, 255),
        'INBOUND_PRE_UPD_BCC_SCENARIO Execution Completed',
        :v_rowcount,
        NULL,
        LEFT(v_sql, 4000)
    );
END