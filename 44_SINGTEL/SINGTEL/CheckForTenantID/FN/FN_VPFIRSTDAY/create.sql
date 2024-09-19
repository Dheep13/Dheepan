CREATE FUNCTION EXT.FN_VPFIRSTDAY
(
    IN P_DATE TIMESTAMP,   /* ORIGSQL: P_DATE IN DATE */
    IN P_SEMIMONTH VARCHAR(75) DEFAULT 'B',   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                              /* ORIGSQL: P_SEMIMONTH IN VARCHAR2 DEFAULT 'B' */
    IN P_PAYEE VARCHAR(75) DEFAULT NULL      /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                             /* ORIGSQL: P_PAYEE IN VARCHAR2 DEFAULT null */
)
RETURNS dbmtk_function_result TIMESTAMP   /* ORIGSQL: RETURN DATE */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_date TIMESTAMP;  /* ORIGSQL: v_date date; */

    IF :P_SEMIMONTH = 'B' 
    AND :P_PAYEE  
    IN
    (
        SELECT   /* ORIGSQL: (select payeeid from stel_temp_midmonthpayees) */
            payeeid
        FROM
            EXT.stel_temp_midmonthpayees
    )
    THEN
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_MIDMONTHPAYEES' not found */

        SELECT
            MAX(ADD_DAYS(EXT.trunc(:P_DATE, 'MONTH'), cutoff))/*MAX(ADD_SECONDS(sapdbmtk.sp_f_dbmtk_truncate_datetime(:P_DATE,'MM'),(86400*(cutoff))))*/   /* ORIGSQL: trunc(p_date,'MM') + cutoff */
        INTO
            v_date
        FROM
            EXT.stel_temp_midmonthpayees
        WHERE
            payeeid = :P_PAYEE;
    ELSE 
        SELECT
            MAX(EXT.trunc(:P_DATE, 'MONTH'))/*MAX(sapdbmtk.sp_f_dbmtk_truncate_datetime(:P_DATE,'MM'))*/   /* ORIGSQL: trunc(p_date,'MM') */
        INTO
            v_date
        FROM
            SYS.DUMMY;  /* ORIGSQL: FROM dual ; */
    END IF; 

    dbmtk_function_result = :v_date;
    RETURN;
END