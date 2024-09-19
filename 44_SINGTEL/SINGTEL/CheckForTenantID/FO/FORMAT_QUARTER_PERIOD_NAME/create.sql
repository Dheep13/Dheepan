CREATE FUNCTION EXT.FORMAT_QUARTER_PERIOD_NAME (input_string NVARCHAR(50))
 RETURNS output nvarchar(50) LANGUAGE SQLSCRIPT AS
 BEGIN
 
    DECLARE result NVARCHAR(500);

    output = 
        UPPER(SUBSTRING(input_string, 1, 1)) || 
        LOWER(SUBSTRING(input_string, 2, INSTR(input_string, ' - ') - 2)) || 
        ' - ' || 
        UPPER(SUBSTRING(input_string, INSTR(input_string, ' - ') + 3, 1)) || 
        LOWER(SUBSTRING(input_string, INSTR(input_string, ' - ') + 4));

 END