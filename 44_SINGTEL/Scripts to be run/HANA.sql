--1. This is to validate the intermediate table count
--2. The counts will automatically be captured when the file is uploaded to XDL
--3. Run the below sql to just verify if the counts are captured
select * from ext.inbound_count_check;

--4. run the below SQL after the V&T is complete.This to validate the stage count.
--5. Replace :v_file_name with the actual filename
--6. Replace :v_file_date with the actual filedate
INSERT INTO ext.inbound_count_check
select pl.batchname,v_file_date, st.name||'_'||mt.name, me.value
from cs_pipelinerun pl,
cs_stagesummary ss, cs_stagetype st,
cs_metricelement me,cs_metrictype mt
where 1=1
and upper(pl.batchname) = :v_file_name
and pl.starttime>= :v_file_date
and ss.pipelinerunseq=pl.pipelinerunseq
and ss.stagetypeseq=st.stagetypeseq
and st.name in ('Validate','Transfer')
and me.metricsummaryseq=ss.metricsummaryseq
and me.metrictypeseq=mt.metrictypeseq
-- and pl.batchname like '%SubmittedBroadBandOrders%'
order by pl.pipelinerunseq desc;



CREATE OR REPLACE PROCEDURE EXT.SP_INBOUND_COUNT()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    DECLARE v_file_date TIMESTAMP;
    DECLARE v_file_name VARCHAR(200);
    DECLARE v_file_type VARCHAR(200);
    DECLARE v_src_tbl VARCHAR(200);
    DECLARE v_dynamic_sql VARCHAR(5000);
    DECLARE v_dynamic_sql_error VARCHAR(5000);
    DECLARE v_dynamic_sql_no_error VARCHAR(5000);
    DECLARE v_tgttable VARCHAR(200);
    DECLARE v_staging_table VARCHAR2(200) := 'INBOUND_DATA_STAGING'; 
    
    -- Declare cursor for looping through target tables based on filetype and join with sys.table_columns
    DECLARE CURSOR cur_tgttables FOR
        SELECT DISTINCT ext_in.tgttable
        FROM ext.inbound_cfg_txnfield ext_in
        JOIN sys.table_columns sys_col
            ON ext_in.tgttable = sys_col.table_name
        WHERE ext_in.filetype = :v_file_type
        AND sys_col.column_name IN ('FILENAME', 'FILEDATE');
 
     -- Get session context variables
    SELECT CAST(SESSION_CONTEXT('v_file_name') AS VARCHAR(200)) INTO v_file_name FROM sys.dummy;
    SELECT CAST(SESSION_CONTEXT('v_file_type') AS VARCHAR(200)) INTO v_file_type FROM sys.dummy;
    SELECT CAST(SESSION_CONTEXT('v_file_date') AS TIMESTAMP) INTO v_file_date FROM sys.dummy;
    SELECT CAST(SESSION_CONTEXT('v_src_tbl') AS VARCHAR(200)) INTO v_src_tbl FROM sys.dummy;   
    
        v_dynamic_sql_error :=
            'INSERT INTO ext.inbound_count_check ' ||
            'SELECT filename, filedate,''' || v_staging_table || '_Errors'' AS table_name, COUNT(1) ' ||
            'FROM ' || :v_staging_table || ' ' ||
            'WHERE filename = ''' || :v_file_name || ''' ' ||
            'AND filedate = ''' || :v_file_date || ''' ' ||
            'AND ERROR_MESSAGE IS NOT NULL GROUP BY filename, '''||:v_staging_table||'_Errors'', filedate';
   
        v_dynamic_sql_no_error :=
            'INSERT INTO ext.inbound_count_check ' ||
            'SELECT filename, filedate,''' || v_staging_table || ''' AS table_name, COUNT(1) ' ||
            'FROM ' || :v_staging_table || ' ' ||
            'WHERE filename = ''' || :v_file_name || ''' ' ||
            'AND filedate = ''' || :v_file_date || ''' ' ||
            'AND ERROR_MESSAGE IS NULL GROUP BY filename, '''||:v_staging_table||''', filedate';
   
    -- Cursor processing
    FOR cur_row AS cur_tgttables DO
        v_tgttable := cur_row.tgttable;

        -- Construct dynamic SQL for counting records
        v_dynamic_sql := 
            'INSERT INTO ext.inbound_count_check ' ||
            'SELECT filename, filedate,'''||:v_tgttable||''', COUNT(1) ' ||
            'FROM ' || :v_tgttable || ' ' ||	
            'WHERE filename = ''' || :v_file_name || ''' ' ||
            'AND filedate = ''' || :v_file_date || ''' ' ||
            'GROUP BY filename, '''||:v_tgttable||''', filedate';
   INSERT INTO EXT.DYNAMIC_SQL_LOG (PROCEDURE_NAME, SQL_TEXT) VALUES (::CURRENT_OBJECT_NAME, v_dynamic_sql);
        -- Execute the dynamic SQL
        EXECUTE IMMEDIATE :v_dynamic_sql;
    END FOR;
    -- select * from cs_pipelinerun limit 10;

            -- Commit the transaction
COMMIT;
    
END

