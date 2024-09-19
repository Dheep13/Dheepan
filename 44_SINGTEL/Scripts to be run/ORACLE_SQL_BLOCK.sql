DECLARE
    v_file_date TIMESTAMP;
    v_file_name VARCHAR2(200);
    v_file_type VARCHAR2(200);
    v_dynamic_sql VARCHAR2(5000);
    v_tgttable VARCHAR2(200);
    v_staging_table VARCHAR2(200) := 'INBOUND_DATA_STAGING';

    CURSOR cur_tgttables IS
        SELECT DISTINCT ext_in.tgttable
        FROM inbound_cfg_txnfield ext_in
        JOIN all_tab_columns atc
        ON ext_in.tgttable = atc.table_name
        WHERE ext_in.filetype = v_file_type
        AND atc.column_name IN ('FILENAME', 'FILEDATE')
        UNION
        SELECT DISTINCT otgt.tablename
        FROM inbound_cfg_txnfield ext_in
        JOIN INBOUND_CFG_TGTTABLE otgt
        ON otgt.tgttable = ext_in.tgttable
        WHERE ext_in.filetype = v_file_type;

    TYPE rec_result IS RECORD (
        filename VARCHAR2(255),
        filedate TIMESTAMP,
        table_name VARCHAR2(255),
        record_count NUMBER
    );
    TYPE tbl_result IS TABLE OF rec_result;
    v_results tbl_result;

BEGIN
    SELECT DISTINCT file_type, file_name, file_date
    INTO v_file_type, v_file_name, v_file_date
    FROM INBOUND_CFG_PARAMETER;

    -- Error records count
    v_dynamic_sql := 
        'SELECT filename, filedate, ''' || v_staging_table || '_Errors'' AS table_name, COUNT(1) AS record_count ' ||
        'FROM ' || v_staging_table || ' WHERE filename = :1 ' ||
        'AND filedate = :2 ' ||
        'AND ERROR_MESSAGE IS NOT NULL GROUP BY filename, filedate';

    EXECUTE IMMEDIATE v_dynamic_sql BULK COLLECT INTO v_results USING v_file_name, v_file_date;
    
    DBMS_OUTPUT.PUT_LINE('Error Records:');
    DBMS_OUTPUT.PUT_LINE('Filename | Filedate | Table Name | Count');
    FOR i IN 1..v_results.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_results(i).filename || ' | ' || 
                             TO_CHAR(v_results(i).filedate, 'YYYY-MM-DD HH24:MI:SS') || ' | ' || 
                             v_results(i).table_name || ' | ' || 
                             v_results(i).record_count);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- No error records count
    v_dynamic_sql := 
        'SELECT filename, filedate, ''' || v_staging_table || ''' AS table_name, COUNT(1) AS record_count ' ||
        'FROM ' || v_staging_table || ' WHERE filename = :1 ' ||
        'AND filedate = :2 ' ||
        'AND ERROR_MESSAGE IS NULL GROUP BY filename, filedate';

    EXECUTE IMMEDIATE v_dynamic_sql BULK COLLECT INTO v_results USING v_file_name, v_file_date;
    
    DBMS_OUTPUT.PUT_LINE('No Error Records:');
    DBMS_OUTPUT.PUT_LINE('Filename | Filedate | Table Name | Count');
    FOR i IN 1..v_results.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_results(i).filename || ' | ' || 
                             TO_CHAR(v_results(i).filedate, 'YYYY-MM-DD HH24:MI:SS') || ' | ' || 
                             v_results(i).table_name || ' | ' || 
                             v_results(i).record_count);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Cursor processing
    FOR cur_row IN cur_tgttables LOOP
        v_tgttable := cur_row.tgttable;

        v_dynamic_sql :=
            'SELECT filename, filedate, ''' || v_tgttable || ''' AS table_name, COUNT(1) AS record_count ' ||
            'FROM ' || v_tgttable || ' ' ||
            'WHERE filename = :1 ' ||
            'AND filedate = :2 ' ||
            'GROUP BY filename, filedate';

        EXECUTE IMMEDIATE v_dynamic_sql BULK COLLECT INTO v_results USING v_file_name, v_file_date;
        
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_tgttable);
        DBMS_OUTPUT.PUT_LINE('Filename | Filedate | Table Name | Count');
        FOR i IN 1..v_results.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE(v_results(i).filename || ' | ' || 
                                 TO_CHAR(v_results(i).filedate, 'YYYY-MM-DD HH24:MI:SS') || ' | ' || 
                                 v_results(i).table_name || ' | ' || 
                                 v_results(i).record_count);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Inbound count process completed successfully.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error in Inbound Count Check: ' || SQLERRM);
        RAISE;
END;

