CREATE FUNCTION EXT.get_unique_expression
(
    IN in_stage DECIMAL(38,10), /* Parameter ':in_stage' added when de-nesting function 'get_unique_expression' from procedure 'SP_INBOUND_VALIDATOR' */   /* ORIGSQL: in_stage IN number */
    IN in_str VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                             /* ORIGSQL: in_str IN varchar */
    IN in_delimiters DECIMAL(38,10),   /* ORIGSQL: in_delimiters IN number */
    IN in_filetype VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_filetype IN varchar */
)
RETURNS result VARCHAR(75)   /* ORIGSQL: return varchar */
/* RESOLVE: Manual edits required: VARCHAR function value(no length): user-configured length=75; adjust as needed */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_no_delimiters DECIMAL(38,10);  /* ORIGSQL: v_no_delimiters number; */
    DECLARE i DECIMAL(38,10) = 1;  /* ORIGSQL: i number:=1; */
    DECLARE v_expr VARCHAR(500) = NULL;  /* ORIGSQL: v_expr varchar2(500):= NULL; */
    DECLARE v_fieldName VARCHAR(255);  /* ORIGSQL: v_fieldName varchar(255); */
    DECLARE v_single_quote VARCHAR(20) = '''';  /* ORIGSQL: v_single_quote varchar(20) :=''''; */
    DECLARE v_single_space VARCHAR(1) = ' ';  /* ORIGSQL: v_single_space varchar(1) :=' '; */
    DECLARE v_comma VARCHAR(5) = ', ';  /* ORIGSQL: v_comma varchar(5) := ', ' ; */

    v_no_delimiters = :in_delimiters;

    /* ORIGSQL: while i<=v_no_delimiters+1 loop */
    WHILE :i <= :v_no_delimiters+1
    DO
        SELECT
            SUBSTRING_REGEXPR('[^||]+' IN in_str FROM 1 OCCURRENCE :i)  /* ORIGSQL: regexp_substr(in_str,'[^||]+',1,i) */
        INTO
            v_fieldName
        FROM
            SYS.DUMMY;  /* ORIGSQL: FROM dual ; */
        BEGIN 
            DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
                /* ORIGSQL: when no_data_found then */
                BEGIN
                    v_expr = IFNULL(:v_fieldName,'') || '||';
                END;


            /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_TXNFIELD' not found */

            SELECT
                DISTINCT
                IFNULL(:v_expr,'') ||IFNULL(sourcefield,'') ||
                CASE
                    WHEN genericexpression IS NOT NULL
                    AND UPPER(DATATYPE) = 'NUMBER' 
                    THEN IFNULL(:v_single_quote,'')||IFNULL(genericexpression,'')||IFNULL(:v_single_quote,'')
                    WHEN genericexpression IS NOT NULL
                    AND UPPER(DATATYPE) = 'DATE' 
                    THEN 'to_date(' ||IFNULL(genericexpression,'')||IFNULL(:v_comma,'') || IFNULL(:v_single_quote,'')||IFNULL(DATE_FORMAT,'')||IFNULL(:v_single_quote,'')||')' 
                    ELSE genericexpression
                END
                || '||'
            INTO
                v_expr
            FROM
                INBOUND_CFG_TXNFIELD
            WHERE
                filetype = :in_filetype
                AND tgtfield = :v_fieldName
                AND IFNULL(stage_number,1) = :in_stage;  /* ORIGSQL: nvl(stage_number,1) */

            /* ORIGSQL: exception when no_data_found then */
        END;

        i = :i + 1;/* NOT CONVERTED! */  /* RESOLVE: Record datatype(not converted): Cannot expand assignment to record variable 'i', rval has non-identical or unresolved datatype */
    END WHILE;  /* ORIGSQL: end loop; */

    v_expr = substring(:v_expr,1,LENGTH(:v_expr) -2);  /* ORIGSQL: substr(v_expr,1,length(v_expr)-2) */

    result = :v_expr;
END