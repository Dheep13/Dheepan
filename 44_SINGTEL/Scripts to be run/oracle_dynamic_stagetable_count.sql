DECLARE
    v_file_type VARCHAR2(200);
    v_file_name VARCHAR2(200);
    v_batchname VARCHAR2(200);
    v_dynamic_sql VARCHAR2(5000);

    PROCEDURE execute_and_print(p_sql VARCHAR2, p_title VARCHAR2, p_file_name VARCHAR2, p_batchname VARCHAR2) IS
        v_cursor NUMBER;
        v_columns DBMS_SQL.DESC_TAB;
        v_column_count NUMBER;
        v_column_value VARCHAR2(4000);
        v_rows_fetched NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || '--- ' || p_title || ' ---');
        v_cursor := DBMS_SQL.OPEN_CURSOR;
        DBMS_SQL.PARSE(v_cursor, p_sql, DBMS_SQL.NATIVE);
        DBMS_SQL.DESCRIBE_COLUMNS(v_cursor, v_column_count, v_columns);
        
        FOR i IN 1..v_column_count LOOP
            DBMS_SQL.DEFINE_COLUMN(v_cursor, i, v_column_value, 4000);
            DBMS_OUTPUT.PUT(RPAD(v_columns(i).col_name, 30));
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE(RPAD('-', 30*v_column_count, '-'));
        
        -- Bind variables
        IF p_sql LIKE '%:v_file_name%' THEN
            DBMS_SQL.BIND_VARIABLE(v_cursor, ':v_file_name', p_file_name);
        END IF;
        IF p_sql LIKE '%:v_batchname%' THEN
            DBMS_SQL.BIND_VARIABLE(v_cursor, ':v_batchname', p_batchname);
        END IF;
        
        v_rows_fetched := DBMS_SQL.EXECUTE(v_cursor);
        WHILE DBMS_SQL.FETCH_ROWS(v_cursor) > 0 LOOP
            FOR i IN 1..v_column_count LOOP
                DBMS_SQL.COLUMN_VALUE(v_cursor, i, v_column_value);
                DBMS_OUTPUT.PUT(RPAD(NVL(v_column_value, ' '), 30));
            END LOOP;
            DBMS_OUTPUT.PUT_LINE('');
        END LOOP;
        DBMS_SQL.CLOSE_CURSOR(v_cursor);
    END execute_and_print;

BEGIN
    -- Assume we get these values from somewhere
    v_file_type := 'BCC-SCII-ClosedFixedVoiceStandaloneOrders'; -- Example file type
    v_file_name := 'stel_BCC-SCII-ClosedFixedVoiceStandaloneOrders_20240912061425.txt'; -- Example file name
    v_batchname := 'STEL_TXSTA_PRD.FRA.UC.INT_20240912_002807_BCC-SCII-ClosedFixedVoiceStandaloneOrders.txt'; -- Example batch name

    DBMS_OUTPUT.PUT_LINE('Processing file type: ' || v_file_type);

    CASE
        WHEN v_file_type IN ('BCC-SCII-BundleOrders') THEN
            v_dynamic_sql := 'SELECT * FROM STEL_DATA_BCCBUNDLE WHERE FILENAME = :v_file_name';
            execute_and_print(v_dynamic_sql, v_file_type, v_file_name, v_batchname);

        WHEN v_file_type IN ('BCC-SCII-CancellationOrders') THEN
            v_dynamic_sql := 'SELECT * FROM STEL_DATA_BCCCANCEL WHERE FILENAME = :v_file_name';
            execute_and_print(v_dynamic_sql, v_file_type, v_file_name, v_batchname);

        WHEN v_file_type IN ('BCC-SCII-ChannelPartnerHierarchy', 'BCC-SCII-ChannelPartnerMaster', 'EDW-SCII-CCOProfiles', 'HRCentral-SCII-SalesmanProfile') THEN
            v_dynamic_sql := 'SELECT batchname, stageprocessdate, stageprocessflag, COUNT(1) FROM cs_stageposition WHERE batchname = :v_batchname GROUP BY batchname, stageprocessflag, stageprocessdate';
            execute_and_print(v_dynamic_sql, v_file_type || ' - CS_STAGEPOSITION', v_file_name, v_batchname);
            
            v_dynamic_sql := 'SELECT batchname, stageprocessdate, stageprocessflag, COUNT(1) FROM cs_stageparticipant WHERE batchname = :v_batchname GROUP BY batchname, stageprocessflag, stageprocessdate';
            execute_and_print(v_dynamic_sql, v_file_type || ' - CS_STAGEPARTICIPANT', v_file_name, v_batchname);

        WHEN v_file_type IN ('BCC-SCII-ClosedBroadBandOrders', 'BCC-SCII-ClosedFixedVoiceStandaloneOrders', 'BCC-SCII-ClosedSmartHomeOrders', 'BCC-SCII-ClosedTVOrders', 
                             'BCC-SCII-MTPOSSalesOrders', 'BCC-SCII-ODSDashRegistration', 'BCC-SCII-SubmittedBroadBandOrders', 'BCC-SCII-SubmittedFixedVoiceStandaloneOrders', 
                             'BCC-SCII-SubmittedMobileOrders', 'BCC-SCII-SubmittedSmartHomeOrders', 'BCC-SCII-SubmittedTVOrders', 'BCC-SCII-VoucherInfo', 
                             'EDW-SCII-mRemitTransactions', 'ITDM-SCII-PrepaidTopup', 'SAP-SCII-PhoenixCard', 'SAP-SCII-PrepaidSIM') THEN
            v_dynamic_sql := 'SELECT batchname, stageprocessdate, stageprocessflag, COUNT(1) FROM cs_stagesalestransaction WHERE batchname = :v_batchname GROUP BY batchname, stageprocessflag, stageprocessdate';
            execute_and_print(v_dynamic_sql, v_file_type || ' - CS_STAGESALESTRANSACTION', v_file_name, v_batchname);
            
            v_dynamic_sql := 'SELECT st.batchname, st.stageprocessdate, stageprocessflag, COUNT(1) FROM cs_stagetransactionassign sta, cs_stagesalestransaction st WHERE st.stagesalestransactionseq = sta.stagesalestransactionseq AND st.batchname = :v_batchname GROUP BY st.batchname, stageprocessflag, stageprocessdate';
            execute_and_print(v_dynamic_sql, v_file_type || ' - CS_STAGETRANSACTIONASSIGN', v_file_name, v_batchname);

        WHEN v_file_type IN ('BCC-SCII-EPCExtract', 'BCC-SCII-MTPOSStockInfo') THEN
            v_dynamic_sql := 'SELECT batchname, stageprocessdate, stageprocessflag, COUNT(1) FROM cs_stageproduct WHERE batchname = :v_batchname GROUP BY batchname, stageprocessflag, stageprocessdate';
            execute_and_print(v_dynamic_sql, v_file_type || ' - CS_STAGEPRODUCT', v_file_name, v_batchname);

        WHEN v_file_type IN ('SAP-SCII-Equipmentprice') THEN
            v_dynamic_sql := 'SELECT st.batchname, st.stageprocessdate, stageprocessflag, COUNT(1) FROM cs_stagegenericclassifier st WHERE st.batchname = :v_batchname GROUP BY st.batchname, stageprocessflag, stageprocessdate';
            execute_and_print(v_dynamic_sql, v_file_type || ' - CS_STAGEGENERICCLASSIFIER', v_file_name, v_batchname);

        WHEN v_file_type IN ('BCC-SCII-DiscountInfo', 'EDW-SCII-DashSignUp', 'EDW-SCII-DashTopUp') THEN
            DBMS_OUTPUT.PUT_LINE('No specific SQL provided for file type: ' || v_file_type);

        ELSE
            DBMS_OUTPUT.PUT_LINE('Unknown file type: ' || v_file_type);
    END CASE;

    DBMS_OUTPUT.PUT_LINE('Processing completed.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error processing file type ' || v_file_type || ': ' || SQLERRM);
        RAISE;
END;
