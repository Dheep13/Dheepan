create FUNCTION ext.fn_get_gst_rate
(
    IN P_STR_COM_DATE TIMESTAMP     /* ORIGSQL: P_STR_COM_DATE IN DATE */
)
RETURNS dbmtk_function_result DECIMAL(38,10)   /* ORIGSQL: return NUMBER */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_GST_DEFAULT_RATE DECIMAL(10,2);  /* ORIGSQL: V_GST_DEFAULT_RATE NUMBER(10,2); */

    SELECT
        value
    INTO
        V_GST_DEFAULT_RATE
    FROM
        EXT.vw_lt_gst_rate
        --where to_date(P_STR_COM_DATE, 'mm/dd/yyyy') >= effectivestartdate
        --and to_date(P_STR_COM_DATE, 'mm/dd/yyyy') < effectiveenddate;
    WHERE
        :P_STR_COM_DATE >= effectivestartdate
        AND :P_STR_COM_DATE < effectiveenddate;

    dbmtk_function_result = :V_GST_DEFAULT_RATE;
END