CREATE FUNCTION EXT.get_number_ofstrings
(
    IN in_str VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                             /* ORIGSQL: in_str IN varchar */
    IN in_delimiter VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                  /* ORIGSQL: in_delimiter IN varchar */
)
RETURNS result DECIMAL(38,10)   /* ORIGSQL: return number */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_no_delimiters DECIMAL(38,10);  /* ORIGSQL: v_no_delimiters number; */

    SELECT
        (LENGTH(:in_str) - LENGTH(REPLACE(:in_str,:in_delimiter,'')))/LENGTH(:in_delimiter)
    INTO
        v_no_delimiters
    FROM
        SYS.DUMMY;  /* ORIGSQL: FROM dual ; */

    result = :v_no_delimiters;

END