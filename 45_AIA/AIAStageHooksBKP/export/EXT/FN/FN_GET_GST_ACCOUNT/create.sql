create FUNCTION ext.fn_get_gst_account
(
    IN P_GST_RATE BIGINT,   /* ORIGSQL: P_GST_RATE IN INTEGER */
    IN P_GST_CUR_ACCOUNT VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                         /* ORIGSQL: P_GST_CUR_ACCOUNT IN VARCHAR2 */
    IN P_STR_CYCLEDATE VARCHAR(255) ,
    
    IN STR_DATE_FORMAT_TYPE VARCHAR(50)
    
    
    /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                      /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
)
RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return varchar2 */
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=255; adjust as needed */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_gst_account VARCHAR(50);  /* ORIGSQL: v_gst_account varchar2(50); */

    /* RESOLVE: Identifier not found: Table/view 'AIASCS_MDLTCELL' not found */
    /* RESOLVE: Identifier not found: Table/view 'AIASCS_RELATIONALMDLT' not found */
    /* RESOLVE: Identifier not found: Table/view 'AIASCS_MDLTDIMENSION' not found */
    /* RESOLVE: Identifier not found: Table/view 'AIASCS_MDLTINDEX' not found */
    SELECT
        CS_MDLTCell.STRINGVALUE
    INTO
        v_gst_account
    FROM
        CS_MDLTCell
    INNER JOIN
        CS_RELATIONALMDLT
        ON CS_RELATIONALMDLT.name = 'LT_GST_RATE_ACCOUNT_MAPPING'
        AND CS_RELATIONALMDLT.REMOVEDATE = TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */
    INNER JOIN
        CS_MDLTDimension d1
        ON UPPER(d1.name) = UPPER('Current GST account')
        AND d1.REMOVEDATE = TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */
        AND CS_RELATIONALMDLT.ruleelementseq = d1.ruleelementseq
    INNER JOIN
        CS_MDLTIndex i1
        ON CS_RELATIONALMDLT.ruleelementseq = i1.ruleelementseq
        AND d1.dimensionseq = i1.dimensionseq
        AND i1.REMOVEDATE = TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */
        AND CS_MDLTCell.mdltseq = d1.ruleelementseq
    INNER JOIN
        CS_MDLTDimension d2
        ON UPPER(d2.name) = UPPER('GST Rate')
        AND d2.REMOVEDATE = TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */
        AND CS_RELATIONALMDLT.ruleelementseq = d2.ruleelementseq
    INNER JOIN
        CS_MDLTIndex i2
        ON CS_RELATIONALMDLT.ruleelementseq = i2.ruleelementseq
        AND d2.dimensionseq = i2.dimensionseq
        AND i2.REMOVEDATE = TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */
        AND CS_MDLTCell.mdltseq = d2.ruleelementseq
    WHERE
        CS_MDLTCell.REMOVEDATE = TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */
        AND CS_MDLTCell.DIM0INDEX = i1.ordinal
        AND CS_MDLTCell.DIM1INDEX = i2.ordinal
        AND i1.MINSTRING = :P_GST_CUR_ACCOUNT
        AND i2.MINVALUE = :P_GST_RATE
        AND CS_MDLTCell.EFFECTIVESTARTDATE <= TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE)
        AND CS_MDLTCell.EFFECTIVEENDDATE > TO_DATE(:P_STR_CYCLEDATE, :STR_DATE_FORMAT_TYPE); 

    dbmtk_function_result = :v_gst_account;

END