CREATE FUNCTION EXT.FN_VALUELOOKUP
(
    IN P_INPUT VARCHAR(75),
    IN P_TABLE VARCHAR(75),
    IN P_FILTER VARCHAR(75),
    IN P_COMPAREFIELD VARCHAR(75),
    IN P_RESULTFIELD VARCHAR(75)
    -- OUT dbmtk_function_result NVARCHAR(10000)
)
RETURNS dbmtk_function_result NVARCHAR(10000)   /* ORIGSQL: RETURN NUMBER */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    DECLARE v_Sql VARCHAR(5000);
    DECLARE v_return VARCHAR(75);

    -- Constructing the dynamic SQL statement
    v_Sql = 'SELECT ' || IFNULL(:P_RESULTFIELD, '') || ' FROM ' || IFNULL(:P_TABLE, '') || 
            ' WHERE ' || IFNULL(:P_FILTER, '') || ' AND TRIM(' || IFNULL(:P_COMPAREFIELD, '') || 
            ') = TRIM(''' || IFNULL(:P_INPUT, '') || ''') AND ROWNUM = 1';

    -- Execute the dynamic SQL and store the result into v_return
    -- EXECUTE IMMEDIATE :v_Sql INTO v_return;

    -- Assign the result to the OUT parameter
    dbmtk_function_result = :v_Sql;
END