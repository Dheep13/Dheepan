CREATE PROCEDURE EXT.STEL_PROC_RPT_PARTITIONS_PSEQ
(
    IN v_periodseq BIGINT,   /* ORIGSQL: v_periodseq IN INTEGER */
    IN v_table_name VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                  /* ORIGSQL: v_table_name IN VARCHAR2 */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodname VARCHAR(250);  /* ORIGSQL: v_periodname VARCHAR2(250); */
    DECLARE v_partitionname VARCHAR(250);  /* ORIGSQL: v_partitionname VARCHAR2(250); */
    DECLARE v_periodname_partition VARCHAR(250);  /* ORIGSQL: v_periodname_partition VARCHAR2(250); */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIODCALENDAR' not found */

    SELECT
        periodname
    INTO
        v_periodname
    FROM
        cs_periodcalendar
    WHERE
        periodseq = :v_periodseq;

    v_partitionname = 'P_' || IFNULL(REPLACE(:v_periodseq, ' ', '_'),'');

    v_periodname_partition = IFNULL(CHAR(39),'') || IFNULL(TO_VARCHAR(:v_periodseq),'') || IFNULL(CHAR(39),'');  /* ORIGSQL: CHR(39) */
    BEGIN 
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
            /* ORIGSQL: WHEN OTHERS THEN */
            BEGIN
                /* ORIGSQL: NULL; */
                DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */  --Catch exception, do nothing in case partition exists already
            END;



        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE 'alter table ' || v_table_name || ' truncate partition ' || v_(...) */
        EXECUTE IMMEDIATE 'alter table '
        || IFNULL(:v_table_name,'')
        || ' truncate partition '
        || IFNULL(:v_partitionname,'');
        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
    END;
    BEGIN 
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
            /* ORIGSQL: WHEN OTHERS THEN */
            BEGIN
                /* ORIGSQL: NULL; */
                DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */  --Catch exception, do nothing in case partition exists already
            END;


        --        insert into test values();
        --        commit ;

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE 'alter table ' || v_table_name || ' add partition ' || v_parti(...) */
        EXECUTE IMMEDIATE 'alter table '
        || IFNULL(:v_table_name,'')
        || ' add partition '
        || IFNULL(:v_partitionname,'')
        || ' values ('
            || IFNULL(:v_periodname_partition,'')
        || ') ';

        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
    END;
END