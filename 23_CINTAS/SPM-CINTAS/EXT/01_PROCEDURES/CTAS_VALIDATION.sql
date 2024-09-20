CREATE OR REPLACE PROCEDURE EXT.CTAS_VALIDATE_DATA(IN PROCNAME VARCHAR(255), OUT p_status INT, OUT p_message NVARCHAR(100))
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
  
    DECLARE v_sql NVARCHAR(1000);
    DECLARE v_insert_sql NVARCHAR(1000);
    DECLARE v_error_message NVARCHAR(1000);
    DECLARE v_proc_name NVARCHAR(255) := :PROCNAME; 
    DECLARE v_validation_result INTEGER; 
    DECLARE CURSOR cur_row FOR
	SELECT * from ext.ctas_validationrules where procname = :PROCNAME;
	DECLARE invalid_input CONDITION FOR SQL_ERROR_CODE 10000;
	
	p_status := 1;
    p_message := 'Validation passed';

    -- Loop through the rules
    FOR cur_validation as cur_row DO
        -- Generate SQL statement based on validation type
        IF :cur_validation.ValidationType = 'MAX_LENGTH' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE LENGTH(' || :cur_validation.ColumnName || ') > ' || :cur_validation.ValidationValue || ';';
        ELSEIF :cur_validation.ValidationType = 'MIN_VALUE' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' < ' || :cur_validation.ValidationValue || ';';
        ELSEIF :cur_validation.ValidationType = 'REQUIRED' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' IS NULL OR TRIM(' || :cur_validation.ColumnName || ') = '''';';
        ELSEIF :cur_validation.ValidationType = 'SPECIFIC_VALUE' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' = ''' || :cur_validation.ValidationValue || ''';';
        ELSEIF :cur_validation.ValidationType = 'DATE_RANGE' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' NOT BETWEEN ''' || REPLACE(:cur_validation.ValidationValue, ',', ''' AND ''') || ''';';
        ELSEIF :cur_validation.ValidationType = 'VALUE_IN_LIST' THEN
            v_sql := 'SELECT * FROM ' || :cur_validation.TableName || ' WHERE ' || :cur_validation.ColumnName || ' NOT IN (''' || REPLACE(:cur_validation.ValidationValue, ',', ''',''') || ''');';
        END IF;
        
            v_insert_sql := 'INSERT INTO ext.ValidationLog (ProcedureName, TableName, ColumnName, FailedValidationType, ExceptionMessage, FailedRecordID) ' || 
                            'SELECT ''' || :PROCNAME || ''', ''' || 
                            :cur_validation.TableName || ''', ''' || 
                            :cur_validation.ColumnName || ''', ''' || 
                            :cur_validation.ValidationType || ''', ''' || 
                            :cur_validation.ExceptionMessage || ''', salestransactionseq ' || 
                            'FROM (' || :v_sql || ') AS ValidationOutput;';
            EXECUTE IMMEDIATE :v_insert_sql;
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM (' || :v_sql || ') AS ValidationOutput INTO ' || :v_validation_result;
            
        IF :cur_validation.RaiseException = 'Yes' and :v_validation_result > 0 THEN
        p_status := 0;
        p_message := :cur_validation.ExceptionMessage;
		BREAK;
		END IF;
        
    END FOR;
END



CREATE PROCEDURE PerformValidation()
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER 
AS
    v_status INT;
    v_message NVARCHAR(100);
    v_error CONDITION FOR SQL_ERROR_CODE 10000;
BEGIN
    -- Call the validation procedure
    CALL ValidateData(v_status, v_message);
    
    -- Check the returned status and raise an exception if invalid
    IF :v_status = 0 THEN
        SIGNAL v_error SET MESSAGE_TEXT = :v_message;
    END IF;
    
    -- Additional logic here...
END;