CREATE FUNCTION ext.FN_WORKINGDAYSBETWEEN (
    STARTDATE DATE,
    ENDDATE DATE
)
RETURNS result INTEGER
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE v_ret INTEGER;
    DECLARE v_days_diff INTEGER;

   -- SELECT (TO_DAYS(ENDDATE) - TO_DAYS(STARTDATE)) INTO v_days_diff;

    SELECT COUNT(*)
    INTO v_ret
    FROM (
        SELECT ADD_DAYS(STARTDATE, (t.COUNT - 1)) AS DAY
        FROM ext.NUMBER_TABLE t
      --  WHERE t.COUNT <= v_days_diff
    ) AS generated_dates
    WHERE MOD(TO_CHAR(generated_dates.DAY, 'J'), 7) + 1 NOT IN (6, 7);

    result = v_ret;
END