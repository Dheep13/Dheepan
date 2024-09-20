CREATE OR REPLACE PROCEDURE EXT.CTAS_VALIDATE_DATA(IN PROCNAME VARCHAR(255), IN BATCHNAME VARCHAR(255),  IN IN_FILTER VARCHAR(1000), OUT p_status INT, OUT p_message NVARCHAR(100))
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
  
    DECLARE v_sql NVARCHAR(1000);
    DECLARE v_insert_sql NVARCHAR(1000);
    DECLARE v_insert_log_sql NVARCHAR(1000);
    DECLARE v_del_sql NVARCHAR(1000);
    DECLARE v_error_message NVARCHAR(1000);
    DECLARE v_proc_name NVARCHAR(255) := UPPER(:PROCNAME); 
    DECLARE v_validation_result integer;
    DECLARE v_create_table_sql NVARCHAR(5000);
    DECLARE v_alter_table_sql NVARCHAR(5000);
    DECLARE v_table_exists INT := 0;
    DECLARE v_eot NVARCHAR(50) :='2200-01-01';
    DECLARE CURSOR cur_row FOR
	SELECT * from ext.ctas_validationrules where procname = UPPER(:PROCNAME) and (filter is null or filter = '');
	DECLARE CURSOR cur_row_with_filter FOR
	SELECT * from ext.ctas_validationrules where procname = UPPER(:PROCNAME) and filter = UPPER(:IN_FILTER) ;
	DECLARE invalid_input CONDITION FOR SQL_ERROR_CODE 10000;
	
	INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, UPPER(:PROCNAME));
	
    v_validation_result :=0;
	p_status := 1;
    p_message := 'Validation passed';
    -- Loop through the rules
   IF :IN_FILTER is NULL or :IN_FILTER='' THEN
   FOR cur_validation as cur_row DO
    -- Generate SQL statement based on validation type
    --If target table does not exist, create it
    SELECT COUNT(*) INTO v_table_exists FROM TABLES WHERE TABLE_NAME = :cur_validation.TargetTableName and SCHEMA_NAME=:cur_validation.schema;
    
    IF :v_table_exists = 0 THEN
        
        v_create_table_sql :='CREATE COLUMN TABLE ' || :cur_validation.schema||'.'||:cur_validation.TargetTableName || ' AS ( SELECT CAST(NULL AS BIGINT) AS LOGID, * FROM ' || :cur_validation.schema ||'.'|| :cur_validation.TableName || ' WHERE 1=0)';
        v_alter_table_sql := 'ALTER TABLE ' || :cur_validation.schema||'.'||:cur_validation.TargetTableName || ' ADD (errormessage nvarchar(1000), LOADTIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP, batchname nvarchar(1000), processflag INT)';
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP, :v_create_table_sql);
        
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP, :v_alter_table_sql);
        -- Execute dynamic SQL
        commit;
        EXECUTE IMMEDIATE :v_create_table_sql;
        EXECUTE IMMEDIATE :v_alter_table_sql;
    END IF;
    
    
    IF :cur_validation.ValidationType = 'MAX_LENGTH' THEN
        v_sql := 'SELECT * FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
                 ' WHERE LENGTH(' || :cur_validation.ColumnName || ') > ' || :cur_validation.ValidationValue ;
        v_del_sql := 'DELETE FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
                 ' WHERE LENGTH(' || :cur_validation.ColumnName || ') > ' || :cur_validation.ValidationValue;


    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql|| :cur_validation.TableName);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
        ELSEIF :cur_validation.ValidationType = 'MIN_VALUE' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' < ' || :cur_validation.ValidationValue;
            v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' < ' || :cur_validation.ValidationValue;
            
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql||:cur_validation.TableName );
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    
        ELSEIF :cur_validation.ValidationType = 'REQUIRED' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE (' || :cur_validation.ColumnName || ' IS NULL OR TRIM(' || :cur_validation.ColumnName || ') = '''')';

          v_del_sql := 'DELETE FROM '  || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE (' || :cur_validation.ColumnName || ' IS NULL OR TRIM(' || :cur_validation.ColumnName || ') = '''')';
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql||:cur_validation.TableName|| :cur_validation.ColumnName);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
       
        ELSEIF :cur_validation.ValidationType = 'SPECIFIC_VALUE' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' = ''' || :cur_validation.ValidationValue ;

           v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' = ''' || :cur_validation.ValidationValue;
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    
    ELSEIF :cur_validation.ValidationType = 'VALUE_NOT_EXISTS' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (' || :cur_validation.ValidationValue ||')' ;

           v_del_sql := 'DELETE FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (' || :cur_validation.ValidationValue ||')' ;
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
        
        ELSEIF :cur_validation.ValidationType = 'DATE_RANGE' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT BETWEEN ''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''' AND ''') || ''' AND procname = ''' || 
         :cur_validation.ProcName || '''';
         
    	v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT BETWEEN ''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''' AND ''') || ''' AND procname = ''' || 
         :cur_validation.ProcName || ''';';
         
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    		
        ELSEIF :cur_validation.ValidationType = 'VALUE_IN_LIST' THEN
         v_sql := 'SELECT * FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''',''') || 
         ''')''';

    	v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''',''') || 
         ''') ''';
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    
        END IF;
        

     v_insert_sql := 'INSERT INTO ' ||:cur_validation.schema||'.'||:cur_validation.TargetTableName || 
                '( SELECT '|| :cur_validation.schema ||'.seq_LOGID.NEXTVAL, *,'''||:cur_validation.validationtype||'-'||:cur_validation.columnname||'-'||:cur_validation.EXCEPTIONMESSAGE||''' ,'||''''||CURRENT_TIMESTAMP||''''||','''||:batchname||''''||',0  AS stageprocessflag FROM (' || :v_sql || ') AS ValidationOutput);';
   
     v_insert_log_sql := 'insert into ext.CTAS_VALIDATIONLOG(LOGID,LOGTIME,PROCEDURENAME,FILENAME,FAILEDVALIDATIONTYPE,ERRORTABLENAME,ERRORMESSAGE,ERRORDESCRIPTION,PROCESSFLAG)' || 
                '( SELECT logid, loadtime '||', ''' || 
                :procname || ''', ''' || 
                :batchname || ''', ''' || 
                :cur_validation.ValidationType || ''', ''' || 
                :cur_validation.TargetTableName || ''', ''' || 
                :cur_validation.ExceptionMessage || ''', ''' || 
                :cur_validation.ExceptionMessage ||'-'||:cur_validation.columnname|| ''', '
                ||' 0 FROM '||:cur_validation.schema||'.'||:cur_validation.TargetTableName|| ' as vl_out where NOT EXISTS ( select * from ext.CTAS_VALIDATIONLOG vl_in where vl_in.logid=vl_out.logid));';
    
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_insert_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);

    commit;
            EXECUTE IMMEDIATE :v_insert_sql;
            -- EXECUTE IMMEDIATE :v_insert_log_sql;
            IF :cur_validation.DeleteFromSource = 'Yes' THEN
    		EXECUTE IMMEDIATE :v_del_sql;
			END IF;
        
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP, 'select count(1) '||'from (' || :v_sql || ') AS ValidationOutput into v_validation_result ');
        commit;
 
        EXECUTE IMMEDIATE  'select count(1) '||'from (' || :v_sql || ') AS ValidationOutput 'INTO v_validation_result;
     IF :cur_validation.RaiseException = 'Yes' and :v_validation_result > 0 THEN
        p_status := 0;
        p_message := :cur_validation.ExceptionMessage;
        BREAK;
    END IF;
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_insert_log_sql);
    EXECUTE IMMEDIATE :v_insert_log_sql;
    commit;
    END FOR;
    
    ELSEIF :IN_FILTER is NOT NULL or :IN_FILTER <> '' THEN
    
    FOR cur_validation as cur_row_with_filter DO
    -- Generate SQL statement based on validation type
    --If target table does not exist, create it
    SELECT COUNT(*) INTO v_table_exists FROM TABLES WHERE TABLE_NAME = :cur_validation.TargetTableName and SCHEMA_NAME=:cur_validation.schema;
    
    IF :v_table_exists = 0 THEN
        
        v_create_table_sql :='CREATE COLUMN TABLE ' || :cur_validation.schema||'.'||:cur_validation.TargetTableName || ' AS ( SELECT CAST(NULL AS BIGINT) AS LOGID, * FROM ' || :cur_validation.schema ||'.'|| :cur_validation.TableName || ' WHERE 1=0)';
        v_alter_table_sql := 'ALTER TABLE ' || :cur_validation.schema||'.'||:cur_validation.TargetTableName || ' ADD (errormessage nvarchar(1000), LOADTIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP, batchname nvarchar(1000), processflag INT)';
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP, :v_create_table_sql);
        
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP, :v_alter_table_sql);
        -- Execute dynamic SQL
        commit;
        EXECUTE IMMEDIATE :v_create_table_sql;
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP,:IN_FILTER );
        EXECUTE IMMEDIATE :v_alter_table_sql;
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP,:IN_FILTER ||' after alter' );
    END IF;
    
    
    IF :cur_validation.ValidationType = 'MAX_LENGTH' THEN
        v_sql := 'SELECT * FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
                 ' WHERE LENGTH(' || :cur_validation.ColumnName || ') > ' || :cur_validation.ValidationValue ;
        v_del_sql := 'DELETE FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
                 ' WHERE LENGTH(' || :cur_validation.ColumnName || ') > ' || :cur_validation.ValidationValue;


    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql|| :cur_validation.TableName);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
        ELSEIF :cur_validation.ValidationType = 'MIN_VALUE' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' < ' || :cur_validation.ValidationValue;
            v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' < ' || :cur_validation.ValidationValue;
            
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql||:cur_validation.TableName );
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    
        ELSEIF :cur_validation.ValidationType = 'REQUIRED' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE (' || :cur_validation.ColumnName || ' IS NULL OR TRIM(' || :cur_validation.ColumnName || ') = '''')';

          v_del_sql := 'DELETE FROM '  || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE (' || :cur_validation.ColumnName || ' IS NULL OR TRIM(' || :cur_validation.ColumnName || ') = '''')';
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql||:cur_validation.TableName|| :cur_validation.ColumnName);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
       
        ELSEIF :cur_validation.ValidationType = 'SPECIFIC_VALUE' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' = ''' || :cur_validation.ValidationValue ;

           v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' = ''' || :cur_validation.ValidationValue;
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    
    ELSEIF :cur_validation.ValidationType = 'VALUE_NOT_EXISTS' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (' || :cur_validation.ValidationValue ||')' ;

           v_del_sql := 'DELETE FROM ' || :cur_validation.SCHEMA || '.' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (' || :cur_validation.ValidationValue ||')' ;
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
        
        ELSEIF :cur_validation.ValidationType = 'DATE_RANGE' THEN
           v_sql := 'SELECT * FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT BETWEEN ''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''' AND ''') || ''' AND procname = ''' || 
         :cur_validation.ProcName || '''';
         
    	v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT BETWEEN ''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''' AND ''') || ''' AND procname = ''' || 
         :cur_validation.ProcName || ''';';
         
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    		
        ELSEIF :cur_validation.ValidationType = 'VALUE_IN_LIST' THEN
         v_sql := 'SELECT * FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''',''') || 
         ''')''';

    	v_del_sql := 'DELETE FROM ' || :cur_validation.TableName || 
         ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (''' || 
         REPLACE(:cur_validation.ValidationValue, ',', ''',''') || 
         ''') ''';
         
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);
    
        END IF;
        

     v_insert_sql := 'INSERT INTO ' ||:cur_validation.schema||'.'||:cur_validation.TargetTableName || 
                '( SELECT '|| :cur_validation.schema ||'.seq_LOGID.NEXTVAL, *,'''||:cur_validation.validationtype||'-'||:cur_validation.columnname||'-'||:cur_validation.EXCEPTIONMESSAGE||''' ,'||''''||CURRENT_TIMESTAMP||''''||','''||:batchname||''''||',0  AS stageprocessflag FROM (' || :v_sql || ') AS ValidationOutput);';
   
     v_insert_log_sql := 'insert into ext.CTAS_VALIDATIONLOG(LOGID,LOGTIME,PROCEDURENAME,FILENAME,FAILEDVALIDATIONTYPE,ERRORTABLENAME,ERRORMESSAGE,ERRORDESCRIPTION,PROCESSFLAG)' || 
                '( SELECT logid, loadtime '||', ''' || 
                :procname || ''', ''' || 
                :batchname || ''', ''' || 
                :cur_validation.ValidationType || ''', ''' || 
                :cur_validation.TargetTableName || ''', ''' || 
                :cur_validation.ExceptionMessage || ''', ''' || 
                :cur_validation.ExceptionMessage ||'-'||:cur_validation.columnname|| ''', '
                ||' 0 FROM '||:cur_validation.schema||'.'||:cur_validation.TargetTableName|| ' as vl_out where NOT EXISTS ( select * from ext.CTAS_VALIDATIONLOG vl_in where vl_in.logid=vl_out.logid));';
     
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_insert_sql);
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_del_sql);

    commit;
            EXECUTE IMMEDIATE :v_insert_sql;
            -- EXECUTE IMMEDIATE :v_insert_log_sql;
            IF :cur_validation.DeleteFromSource = 'Yes' THEN
    		EXECUTE IMMEDIATE :v_del_sql;
			END IF;
        
        INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
        VALUES (CURRENT_TIMESTAMP, 'select count(1) '||'from (' || :v_sql || ') AS ValidationOutput into v_validation_result ');
        commit;
 
        EXECUTE IMMEDIATE  'select count(1) '||'from (' || :v_sql || ') AS ValidationOutput 'INTO v_validation_result;
     IF :cur_validation.RaiseException = 'Yes' and :v_validation_result > 0 THEN
        p_status := 0;
        p_message := :cur_validation.ExceptionMessage;
        BREAK;
    END IF;
    INSERT INTO ext.ctas_SQLLog (Timestamp, SQLString)
    VALUES (CURRENT_TIMESTAMP, :v_insert_log_sql);
    EXECUTE IMMEDIATE :v_insert_log_sql;
    commit;
    END FOR;
    
    
    END IF;
END