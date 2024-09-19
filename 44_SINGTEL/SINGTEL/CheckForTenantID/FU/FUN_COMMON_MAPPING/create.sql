CREATE FUNCTION EXT.FUN_COMMON_MAPPING
(
    IN p_reportname VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                   /* ORIGSQL: p_reportname varchar2 */
    IN p_rptcolumnname VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                      /* ORIGSQL: p_rptcolumnname varchar2 */
    IN p_mappedfor VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                  /* ORIGSQL: p_mappedfor varchar2 */
    IN p_frequency VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: p_frequency varchar2 */
)
RETURNS dbmtk_function_result VARCHAR(75)   /* ORIGSQL: return varchar2 */
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=75; adjust as needed */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_targetcolumnname VARCHAR(100);  /* ORIGSQL: v_targetcolumnname varchar2(100); */

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        /* ORIGSQL: when no_data_found then */
        BEGIN 
            dbmtk_function_result = 'NULL';
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_COMMON_MAPPING' not found */

        SELECT
            columnname
        INTO
            v_targetcolumnname
        FROM
            ext.rpt_common_mapping
        WHERE
            reportname = :p_reportname
            AND rptcolumnname = :p_rptcolumnname
            AND MAPPEDFOR = :p_mappedfor
            AND FREQUENCY = :p_frequency;

        --(product = p_product or product IS NULL);

        IF :v_targetcolumnname IS NOT NULL
        THEN 
            dbmtk_function_result = :v_targetcolumnname;
            RETURN;
        ELSE  
            dbmtk_function_result = 'NULL';
            RETURN;
        END IF;

        /* ORIGSQL: exception when no_data_found then */
END