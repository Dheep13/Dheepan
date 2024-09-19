--Need permission to deploy this proc in prod. Will have no impact on any of the existing processes in production
--Once deployed run the below steps in Oracle after each file is loaded.

CALL SP_INBOUND_COUNT_CHECK();
SELECT * FROM inbound_count_check ;



---Below is the actual proc and table that needs to be deployed in proc

CREATE TABLE INBOUND_COUNT_CHECK
(
    FILENAME VARCHAR2(255),
    FILEDATE VARCHAR2(255),
    TABLENAME VARCHAR2(255),
    RECORD_COUNT NUMBER(10)
--    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE OR REPLACE PROCEDURE SP_INBOUND_COUNT_CHECK
AS
    -- Declare variables
    v_file_date TIMESTAMP;
    v_file_name VARCHAR2(200);
    v_file_type VARCHAR2(200);
    v_dynamic_sql VARCHAR2(5000);
    v_dynamic_sql2 VARCHAR2(5000);
    v_dynamic_sql_error VARCHAR2(5000);
    v_dynamic_sql_no_error VARCHAR2(5000);
    v_tgttable VARCHAR2(200);
    v_output VARCHAR2(4000);
    v_staging_table VARCHAR2(200) := 'INBOUND_DATA_STAGING'; -- Variable for staging table name

    -- Declare cursor
    CURSOR cur_tgttables IS
        SELECT DISTINCT ext_in.tgttable
        FROM inbound_cfg_txnfield ext_in
        JOIN all_tab_columns atc
        ON ext_in.tgttable = atc.table_name
        WHERE ext_in.filetype = v_file_type
        AND atc.column_name IN ('FILENAME', 'FILEDATE')
        UNION
        SELECT DISTINCT otgt.tablename
        FROM ext.inbound_cfg_txnfield ext_in
        JOIN ext.INBOUND_CFG_TGTTABLE otgt
            ON otgt.tgttable = ext_in.tgttable
        WHERE ext_in.filetype = :v_file_type;
        
    -- Declare types for dynamic SQL results
    TYPE t_result IS TABLE OF VARCHAR2(4000);
    v_results t_result;

BEGIN
    -- Get file information
    SELECT DISTINCT file_type, file_name, file_date
    INTO v_file_type, v_file_name, v_file_date
    FROM INBOUND_CFG_PARAMETER;

    v_dynamic_sql_error :=
        'INSERT INTO inbound_count_check ' ||
        'SELECT filename, filedate, ''' || v_staging_table || '_Errors'' AS table_name, COUNT(1) ' ||
        'FROM ' || v_staging_table || ' WHERE filename = ''' || v_file_name || ''' ' ||
        'AND filedate = TO_TIMESTAMP(''' || TO_CHAR(v_file_date, 'YYYY-MM-DD HH24:MI:SS.FF') || ''', ''YYYY-MM-DD HH24:MI:SS.FF'') ' ||
        'AND ERROR_MESSAGE IS NOT NULL GROUP BY filename, ''' || v_staging_table || '_Errors'', filedate';

    v_dynamic_sql_no_error :=
        'INSERT INTO inbound_count_check ' ||
        'SELECT filename, filedate, ''' || v_staging_table || ''' AS table_name, COUNT(1) ' ||
        'FROM ' || v_staging_table || ' WHERE filename = ''' || v_file_name || ''' ' ||
        'AND filedate = TO_TIMESTAMP(''' || TO_CHAR(v_file_date, 'YYYY-MM-DD HH24:MI:SS.FF') || ''', ''YYYY-MM-DD HH24:MI:SS.FF'') ' ||
        'AND ERROR_MESSAGE IS NULL GROUP BY filename, ''' || v_staging_table || ''', filedate';

    EXECUTE IMMEDIATE v_dynamic_sql_error;
    EXECUTE IMMEDIATE v_dynamic_sql_no_error;
       
    -- Cursor processing
    FOR cur_row IN cur_tgttables LOOP
        v_tgttable := cur_row.tgttable;

        -- Construct dynamic SQL for counting records
        v_dynamic_sql :=
            'INSERT INTO inbound_count_check ' ||
            'SELECT filename, filedate, ''' || v_tgttable || ''',COUNT(1) ' ||
            'FROM ' || v_tgttable || ' ' ||
            'WHERE filename = ''' || v_file_name || ''' ' ||
            'AND filedate = TO_TIMESTAMP(''' || TO_CHAR(v_file_date, 'YYYY-MM-DD HH24:MI:SS.FF') || ''', ''YYYY-MM-DD HH24:MI:SS.FF'') ' ||
            'GROUP BY filename, ''' || v_tgttable || ''', filedate';

        -- Execute the dynamic SQL
        EXECUTE IMMEDIATE v_dynamic_sql;

    END LOOP;

    -- Commit the transaction
    COMMIT;

    v_output := 'Inbound count process completed successfully.';
    
    -- Log the success message
--    INSERT INTO process_log (log_message, log_timestamp)
--    VALUES (v_output, SYSTIMESTAMP);

EXCEPTION
    WHEN OTHERS THEN
        -- Log the error and rollback
        v_output := 'Error in SP_INBOUND_COUNT_CHECK: ' || SQLERRM;
--        INSERT INTO process_log (log_message, log_timestamp)
--        VALUES (v_output, SYSTIMESTAMP);
        ROLLBACK;
        RAISE;
END SP_INBOUND_COUNT_CHECK;





