CREATE LIBRARY "EXT"."SA_STAGEHOOK" LANGUAGE SQLSCRIPT AS
BEGIN
  PRIVATE VARIABLE v_eot CONSTANT date := to_date('01-jan-2200','dd-mon-yyyy');
  PUBLIC FUNCTION to_string_null
(
    IN i_string VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                               /* ORIGSQL: i_string varchar2 */
)
RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return varchar2 */
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=255; adjust as needed */
SQL SECURITY DEFINER
AS
BEGIN
    IF :i_string IS NULL
    THEN 
        dbmtk_function_result = 'null';
    ELSE  
        dbmtk_function_result = ''''||IFNULL(:i_string,'')||'''';
    END IF;
END;
  PUBLIC FUNCTION to_string_null_num
(
    IN i_number BIGINT   /* ORIGSQL: i_number number */
)
RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return varchar2 */
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=255; adjust as needed */
SQL SECURITY DEFINER
AS
BEGIN
    IF :i_number IS NULL
    THEN 
        dbmtk_function_result = 'null';
    ELSE  
        dbmtk_function_result = TO_VARCHAR(:i_number);  /* ORIGSQL: to_char(i_number) */
  
    END IF;
END;
  PUBLIC FUNCTION get_period_row 
(
    IN i_periodSeq BIGINT     /* ORIGSQL: i_periodSeq integer */
)
 RETURNS TABLE (TENANTID VARCHAR(4) ,
	PERIODSEQ BIGINT ,
	CREATEDATE LONGDATE ,
	REMOVEDATE LONGDATE ,
	CREATEDBY VARCHAR(255) ,
	MODIFIEDBY VARCHAR(255),
	NAME VARCHAR(50),
	SHORTNAME VARCHAR(20),
	DESCRIPTION VARCHAR(255),
	CALENDARSEQ BIGINT ,
	PARENTSEQ BIGINT,
	PERIODTYPESEQ BIGINT ,
	STARTDATE LONGDATE,
	ENDDATE LONGDATE) LANGUAGE SQLSCRIPT AS
 BEGIN
     RETURN  SELECT *
        FROM
            cs_period
        WHERE
            removedate = :v_eot
            AND periodSeq = :i_periodSeq;
            
     BEGIN 
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
            BEGIN
                RESIGNAL;
     END;
 END;

END;
  PUBLIC PROCEDURE log
(
    IN i_text VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                              /* ORIGSQL: i_text varchar2 */
    IN i_value DECIMAL(38,10) DEFAULT NULL     /* ORIGSQL: i_value number := null */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    -- DECLARE v_owner VARCHAR(30);  /* ORIGSQL: v_owner varchar2(30); */ --Deepan : replacing this with CURRENT_OBJECT_SCHEMA
    -- DECLARE v_name VARCHAR(30);  /* ORIGSQL: v_name varchar2(30); */ -- Deepan : replacing this with CURRENT_OBJECT_NAME
    -- DECLARE v_lineno DECIMAL(38,10);  /* ORIGSQL: v_lineno number; */-- Deepan : replacing this with ::CURRENT_LINE_NUMBER
    -- DECLARE v_caller_t VARCHAR(10000);  /* ORIGSQL: v_caller_t varchar2(10000); */

    /* ORIGSQL: pragma autonomous_transaction; */
    BEGIN AUTONOMOUS TRANSACTION
        -- owa_util.who_called_me (:v_owner, :v_name, :v_lineno, :v_caller_t);/* NOT CONVERTED! */  /* RESOLVE: Standard Package call(not converted): 'owa_util.who_called_me' not supported, manual conversion required */
        /* ORIGSQL: insert into cs_debug_Custom (text, value) values (v_owner||'.'||v_name||'[' ||LPAD(to_char(v_lineno),5,'0') ||']:' ||i_text, i_value); */
        INSERT INTO ext.cs_debug_Custom
            (
                text, value
            )
        VALUES (
                IFNULL(::CURRENT_OBJECT_SCHEMA,'')||'.'||IFNULL(::CURRENT_OBJECT_NAME,'')||'[' ||IFNULL(LPAD(TO_VARCHAR(::CURRENT_LINE_NUMBER),5,'0'),'') ||']:' ||IFNULL(:i_text,''),  /* ORIGSQL: to_char(v_lineno) */
                :i_value
        );

        /* ORIGSQL: commit; */
        COMMIT;
    END;
END;
  PUBLIC PROCEDURE run
(
    IN i_stage VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                               /* ORIGSQL: i_stage varchar2 */
    IN i_mode VARCHAR(255) DEFAULT NULL,   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                           /* ORIGSQL: i_mode varchar2 := null */
    IN i_period VARCHAR(255) DEFAULT NULL,   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                             /* ORIGSQL: i_period varchar2 := null */
    IN i_periodSeq BIGINT DEFAULT NULL,   /* ORIGSQL: i_periodSeq integer := null */
    IN i_userName VARCHAR(255) DEFAULT NULL/* -- i_group             varchar2 := null, */
/* -- i_tracing           varchar2 := null, */
,/* -- i_connection        varchar2 := null, */     /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                                                    /* ORIGSQL: i_userName varchar2 := null */
    IN i_calendar VARCHAR(255) DEFAULT NULL,   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                               /* ORIGSQL: i_calendar varchar2 := null */
    IN i_calendarSeq BIGINT DEFAULT NULL,   /* ORIGSQL: i_calendarSeq integer := null */
    IN i_processingUnitSeq BIGINT DEFAULT NULL     /* ORIGSQL: i_processingUnitSeq integer := null */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_sql VARCHAR(1000);  /* ORIGSQL: v_sql varchar2(1000); */
    DECLARE v_periodRow row like cs_period;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'AIASEXT.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_uapu BIGINT;  /* ORIGSQL: v_uapu integer := 38280596832649217; */
    DECLARE v_eot date = to_date('01-jan-2200','dd-mon-yyyy');
    
   


    /* ORIGSQL: for x in (select * from sa_stagehook_ref where stage = i_stage and active = 1 and effectivestartdate <= v_periodRow.startDate and effectiveenddate >= v_periodRow.endDate and (processingunitseq = nvl(i(...) */
    DECLARE CURSOR cursor_pipeline
    FOR
        SELECT   /* ORIGSQL: select * from sa_stagehook_ref where stage = i_stage and active = 1 and effectivestartdate <= v_periodRow.startDate and effectiveenddate >= v_periodRow.endDate and (processingunitseq = nvl(i_processin(...) */
            *
        FROM
            ext.sa_stagehook_ref
        WHERE
            stage = :i_stage
            AND active = 1
            AND effectivestartdate <= :v_periodRow.startDate
            AND effectiveenddate >= :v_periodRow.endDate
            AND (processingunitseq = IFNULL(:i_processingUnitSeq, :v_uapu)  /* ORIGSQL: nvl(i_processingUnitSeq, v_uapu) */
            OR processingunitseq = 0)
        ORDER BY
            call_order;

    --constantspkg.cUnassignedPUSeq;

    /* ORIGSQL: ext.log('Start') */
     
    CALL ext.log('Start');
    select processingunitseq into v_uapu from cs_processingunit;
    /* ORIGSQL: ext.log('inputs:' ||' i_stage='||to_string_null(i_stage) ||',i_mode=' ||to_string_null(i_mode) ||',i_period=' ||to_string_null(i_period) ||',i_periodSeq=' ||to_string_null(i_periodSeq) ||',i_userName=' ||(...) */
    CALL ext.log('inputs:'||' i_stage='||IFNULL(to_string_null(:i_stage),'') ||',i_mode='||IFNULL(to_string_null(:i_mode),'') ||',i_period='||IFNULL(to_string_null(:i_period),'') ||',i_periodSeq='||IFNULL(to_string_null_num(:i_periodSeq),'') ||',i_userName='||IFNULL(to_string_null(:i_userName),'') ||',i_calendar='||IFNULL(to_string_null(:i_calendar),'') ||',i_calendarSeq='||IFNULL(to_string_null_num(:i_calendarSeq),'') ||',i_processingUnitSeq='||IFNULL(to_string_null_num(:i_processingUnitSeq),'') /* --  ||',i_group='||to_string_null(i_group) */  /* ORIGSQL: to_string_null(i_processingUnitSeq) */

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    /* ORIGSQL: to_string_null(i_calendarSeq) */
        /* -- ||',i_tracing='||to_string_null(i_tracing) */
        /* -- ||',i_connection='||to_string_null(i_connection) */);
     

     select *  into v_periodRow from get_period_row(:i_periodSeq);
            
    FOR x AS cursor_pipeline
    DO
        v_sql = :x.call_text;

        /* Substitute the variables */

        v_sql = replace(:v_sql, '<stage>', to_string_null(:i_stage));  /* ORIGSQL: replace(v_sql, '<stage>', to_string_null(i_stage)) */

        v_sql = replace(:v_sql, '<mode>', to_string_null(i_mode));  /* ORIGSQL: replace(v_sql, '<mode>', to_string_null(i_mode)) */

        v_sql = replace(:v_sql, '<period>', to_string_null(i_period));  /* ORIGSQL: replace(v_sql, '<period>', to_string_null(i_period)) */

        v_sql = replace(:v_sql, '<periodSeq>', to_string_null_num(i_periodSeq));  /* ORIGSQL: replace(v_sql, '<periodSeq>', to_string_null(i_periodSeq)) */

        v_sql = replace(:v_sql, '<userName>',  to_string_null(i_userName));  /* ORIGSQL: replace(v_sql, '<userName>', to_string_null(i_userName)) */

        v_sql = replace(:v_sql, '<calendar>', to_string_null(i_calendar));  /* ORIGSQL: replace(v_sql, '<calendar>', to_string_null(i_calendar)) */

        v_sql = replace(:v_sql, '<calendarSeq>', to_string_null_num(i_calendarSeq));  /* ORIGSQL: replace(v_sql, '<calendarSeq>', to_string_null(i_calendarSeq)) */

        v_sql = replace(:v_sql, '<processingUnitSeq>', to_string_null_num(i_processingUnitSeq));  /* ORIGSQL: replace(v_sql, '<processingUnitSeq>', to_string_null(i_processingUnitSeq)) */

        /* ORIGSQL: ext.log('Attempting to execute: "' || v_sql || '"') */
        CALL ext.log('Attempting to execute: "'|| IFNULL(:v_sql,'') || '"');
        BEGIN 
            DECLARE EXIT HANDLER FOR SQLEXCEPTION
                /* ORIGSQL: when others then */
                BEGIN
                    /* ORIGSQL: ext.log('Error:"' || DBMS_UTILITY.FORMAT_ERROR_STACK || chr(30) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE) */
                    
                    CALL ext.log('Error:'|| ::SQL_ERROR_CODE ||' '||::SQL_ERROR_MESSAGE);   /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_STACK' not supported, manual conversion required */
                              /* RESOLVE: Standard Package call(not converted): 'DBMS_UTILITY.FORMAT_ERROR_BACKTRACE' not supported, manual conversion required */
                            /* ORIGSQL: chr(30) */

                    /* ORIGSQL: raise; */
                    RESIGNAL;
                END;

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: execute immediate v_sql; */
            EXECUTE IMMEDIATE :v_sql;

            /* ORIGSQL: exception when others then */
        END;

        /* ORIGSQL: ext.log('Successfully executed: "' || v_sql || '"') */
        CALL ext.log('Successfully executed: "'|| IFNULL(:v_sql,'') || '"');
    END FOR;  /* ORIGSQL: end loop; */

    /* ORIGSQL: ext.log('End') */
    CALL ext.log('End');
END;
END