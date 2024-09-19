CREATE FUNCTION EXT.F_DATE
(
    IN in_date VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                             /* ORIGSQL: in_date IN VARCHAR2 */
)
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: RETURN NUMBER */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_date TIMESTAMP;  /* ORIGSQL: v_date DATE; */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN Others THEN */
        BEGIN 
            dbmtk_function_result = 0;
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        SELECT
            to_date(:in_date)  /* ORIGSQL: to_date(in_date) */
        INTO
            v_date
        FROM
            SYS.DUMMY;  /* ORIGSQL: FROM dual ; */

        dbmtk_function_result = 1;
        RETURN;

        /* ORIGSQL: Exception WHEN Others THEN */
END