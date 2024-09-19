CREATE FUNCTION EXT.IS_DATE
(
    IN in_parameter1 VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                    /* ORIGSQL: in_parameter1 VARCHAR */
    IN in_format VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                               /* ORIGSQL: in_format VARCHAR */
)
RETURNS dbmtk_function_result BIGINT   /* ORIGSQL: RETURN INTEGER */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_date TIMESTAMP;  /* ORIGSQL: v_date DATE; */
    DECLARE v_sql VARCHAR(500);  /* ORIGSQL: v_sql VARCHAR2(500); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN 
            dbmtk_function_result = 1;
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        SELECT
            TO_DATE(:in_parameter1, :in_format)
        INTO
            v_date
        FROM
            SYS.DUMMY;  /* ORIGSQL: FROM DUAL ; */

        dbmtk_function_result = 0;
        RETURN;

        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END