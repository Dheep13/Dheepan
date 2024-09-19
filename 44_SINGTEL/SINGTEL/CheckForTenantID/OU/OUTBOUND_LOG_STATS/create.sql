CREATE PROCEDURE ext.OUTBOUND_LOG_STATS
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA ext
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE Message1 VARCHAR(255) = 'Stage 1: File to Staging';  /* ORIGSQL: Message1 VARCHAR(255) := 'Stage 1: File to Staging'; */
    DECLARE Message2 VARCHAR(255) = 'Stage 3: Validation on Staging';  /* ORIGSQL: Message2 VARCHAR(255) := 'Stage 3: Validation on Staging'; */
    DECLARE Message3 VARCHAR(255) = 'Stage 4: Staging to ODS Table :';  /* ORIGSQL: Message3 VARCHAR(255) := 'Stage 4: Staging to ODS Table :'; */
    DECLARE Message4 VARCHAR(255) = 'Stage 5: ODS Table to Callidus Final File : ';  /* ORIGSQL: Message4 VARCHAR(255) := 'Stage 5: ODS Table to Callidus Final File : '; */
    DECLARE v_success_rec DECIMAL(38,10) = 0;  /* ORIGSQL: v_success_rec NUMBER := 0; */
    DECLARE v_failure_rec DECIMAL(38,10) = 0;  /* ORIGSQL: v_failure_rec NUMBER := 0; */
    DECLARE v_sql VARCHAR(4000);  /* ORIGSQL: v_sql VARCHAR2(4000); */
    DECLARE v_statusflag DECIMAL(38,10);  /* ORIGSQL: v_statusflag NUMBER; */
	DECLARE v_inbound_cfg_parameter ROW LIKE ext.inbound_cfg_parameter;

    /* ORIGSQL: FOR i IN (SELECT DISTINCT nvl(b.tablename,a.tgttable) tgttable FROM inbound_cfg_(...) */
    DECLARE CURSOR dbmtk_cursor_873
    FOR  
        SELECT   /* ORIGSQL: SELECT DISTINCT nvl(b.tablename,a.tgttable) tgttable FROM ext.inbound_cfg_txnfield a(...) */
            DISTINCT   
            IFNULL(b.tablename,a.tgttable) AS tgttable
        FROM
            ext.inbound_cfg_txnfield a
        LEFT OUTER JOIN
            ext.inbound_cfg_tgttable b
            ON a.tgttable = b.tgttable
        WHERE
            filetype = :v_inbound_cfg_parameter.file_type;

    /* ORIGSQL: FOR i IN (select rownum rn,tgttable from (SELECT DISTINCT nvl(b.tablename,a.tgtt(...) */
    DECLARE CURSOR dbmtk_cursor_876
    FOR
        SELECT   /* ORIGSQL: select ROW_NUMBER() OVER (ORDER BY 0*0) rn,tgttable from (SELECT DISTINCT nvl(b.(...) */
            ROW_NUMBER() OVER (ORDER BY 0*0) AS rn,  /* ORIGSQL: rownum */
            tgttable
            /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_CFG_TXNFIELD' not found */
            /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_CFG_TGTTABLE' not found */
        FROM
            (
                SELECT   /* ORIGSQL: (SELECT DISTINCT nvl(b.tablename,a.tgttable) tgttable FROM inbound_cfg_txnfield (...) */
                    DISTINCT   
                    IFNULL(b.tablename,a.tgttable) AS tgttable
                FROM
                    ext.inbound_cfg_txnfield a
                LEFT OUTER JOIN
                    ext.inbound_cfg_tgttable b
                    ON a.tgttable = b.tgttable
                WHERE
                    filetype = :v_inbound_cfg_parameter.file_type
            ) AS dbmtk_corrname_886;

    -- DECLARE v_inbound_cfg_parameter ext.inbound_cfg_parameter_type;
    -- v_inbound_cfg_parameter   ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'ext.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    --  EXECUTE IMMEDIATE 'truncate table ext.Outbound_log_details drop storage';

    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        ext.inbound_cfg_parameter;

    --delete from ext.Outbound_log_details
    --where (FILE_TYPE,FILE_NAME,FILE_DATE) not in (select FILE_TYPE,
        --FILE_NAME,
    --FILE_DATE from ext.inbound_cfg_parameter);
    --
    --commit;

    /* ORIGSQL: dbms_output.put_line (v_inbound_cfg_parameter.file_date); */

    -- Get Count for Stage 1
    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_DATA_STAGING' not found */

    SELECT
        COUNT(*)
    INTO
        v_success_rec
    FROM
        ext.inbound_data_staging
    WHERE
        (filetype, filename, filedate) IN
        (
            SELECT   /* ORIGSQL: (SELECT v_inbound_cfg_parameter.file_type, v_inbound_cfg_parameter.file_name, v_(...) */
                :v_inbound_cfg_parameter.file_type,
                :v_inbound_cfg_parameter.file_name,
                :v_inbound_cfg_parameter.file_date
            FROM
                SYS.DUMMY   /* ORIGSQL: FROM DUAL) */
        );

    /*
       INSERT INTO ext.Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,RECORDS_PROCESSED,RECORDS_REJECTED)
           VALUES (v_inbound_cfg_parameter.file_type,
                       v_inbound_cfg_parameter.file_name,
                       v_inbound_cfg_parameter.file_date,
                       Message1,
                       v_success_rec,
                   0);
    */

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Get count for Stage 2 
    SELECT
        COUNT(*) 
    INTO
        v_failure_rec
    FROM
        ext.inbound_data_staging
    WHERE
        (filetype, filename, filedate) IN
        (
            SELECT   /* ORIGSQL: (SELECT v_inbound_cfg_parameter.file_type, v_inbound_cfg_parameter.file_name, v_(...) */
                :v_inbound_cfg_parameter.file_type,
                :v_inbound_cfg_parameter.file_name,
                :v_inbound_cfg_parameter.file_date
            FROM
                SYS.DUMMY   /* ORIGSQL: FROM DUAL) */
        )
        AND IFNULL(error_flag, 0) <> 0;  /* ORIGSQL: NVL(error_flag, 0) */

    v_success_rec = :v_success_rec - :v_failure_rec;

    /* RESOLVE: Identifier not found: Table/view 'ext.OUTBOUND_LOG_DETAILS' not found */

    /* ORIGSQL: INSERT INTO Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,(...) */
    INSERT INTO ext.Outbound_log_details
        (
            FILE_TYPE, FILE_NAME, FILE_DATE, STEPS_PROCESSED, RECORDS_PROCESSED, RECORDS_REJECTED
        )
    VALUES (
            :v_inbound_cfg_parameter.file_type,
            :v_inbound_cfg_parameter.file_name,
            :v_inbound_cfg_parameter.file_date,
            :Message2,
            :v_success_rec,
            :v_failure_rec
    );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Get count for Stage 3
    v_failure_rec = 0;

    v_statusflag = 0;

    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_CFG_GENERICPARAMETER' not found */

    SELECT
        VALUE
    INTO
        v_statusflag
    FROM
        ext.INBOUND_CFG_GENERICPARAMETER
    WHERE
        KEY = 'VALIDRECORDSTATUS';

    FOR i AS dbmtk_cursor_873
    DO
        v_sql = 'select count(*) from ';
        v_sql = IFNULL(:v_sql,'') || IFNULL(:i.tgttable,'');

        v_sql = IFNULL(:v_sql,'') || ' where (filename,filedate) in ';
        v_sql = IFNULL(:v_sql,'')
        || ' (select file_name,file_date from ext.inbound_cfg_parameter) ';

        v_sql = IFNULL(:v_sql,'') || ' and nvl(recordstatus,0)<> ';

        v_sql = IFNULL(:v_sql,'') || IFNULL(TO_VARCHAR(:v_statusflag),'');

        -- /* ORIGSQL: DBMS_OUTPUT.put_line (v_sql); */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql INTO v_failure_rec; */
        EXECUTE IMMEDIATE :v_sql INTO v_failure_rec;

        v_sql = 'select count(*) from ';
        v_sql = IFNULL(:v_sql,'') || IFNULL(:i.tgttable,'');

        v_sql = IFNULL(:v_sql,'') || ' where (filename,filedate) in ';
        v_sql = IFNULL(:v_sql,'')
        || ' (select file_name,file_date from ext.inbound_cfg_parameter) ';

        v_sql = IFNULL(:v_sql,'') || ' and nvl(recordstatus,0)= ';

        v_sql = IFNULL(:v_sql,'') || IFNULL(TO_VARCHAR(:v_statusflag),'');

        -- /* ORIGSQL: DBMS_OUTPUT.put_line (v_sql); */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql INTO v_success_rec; */
        EXECUTE IMMEDIATE :v_sql INTO v_success_rec;

        Message3 = IFNULL(:Message3,'') || ' '|| IFNULL(:i.tgttable,''); 

        /* ORIGSQL: INSERT INTO ext.Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,(...) */
        INSERT INTO ext.Outbound_log_details
            (
                FILE_TYPE, FILE_NAME, FILE_DATE, STEPS_PROCESSED, RECORDS_PROCESSED, RECORDS_REJECTED
            )
        VALUES (
                :v_inbound_cfg_parameter.file_type,
                :v_inbound_cfg_parameter.file_name,
                :v_inbound_cfg_parameter.file_date,
                :Message3,
                :v_success_rec,
                :v_failure_rec
        );

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END FOR;  /* ORIGSQL: END LOOP; */

    -- Get count for Stage 4
    v_failure_rec = 0;

    v_statusflag = 0; 

    SELECT
        VALUE
    INTO
        v_statusflag
    FROM
        ext.INBOUND_CFG_GENERICPARAMETER
    WHERE
        KEY = 'VALIDRECORDSTATUS';

    FOR i AS dbmtk_cursor_876
    DO
        v_sql = 'select count(*) from ';
        v_sql = IFNULL(:v_sql,'') || IFNULL(:i.tgttable,'');

        v_sql = IFNULL(:v_sql,'') || ' where (filename,filedate) in ';
        v_sql = IFNULL(:v_sql,'')
        || ' (select file_name,file_date from ext.inbound_cfg_parameter) ';

        v_sql = IFNULL(:v_sql,'') || ' and nvl(recordstatus,0)<> ';

        v_sql = IFNULL(:v_sql,'') || IFNULL(TO_VARCHAR(:v_statusflag),'');

        /* ORIGSQL: DBMS_OUTPUT.put_line (v_sql); */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql INTO v_failure_rec; */
        EXECUTE IMMEDIATE :v_sql INTO v_failure_rec;

        v_sql = 'select count(*) from ';
        v_sql = IFNULL(:v_sql,'') || IFNULL(:i.tgttable,'');

        v_sql = IFNULL(:v_sql,'') || ' where (filename,filedate) in ';
        v_sql = IFNULL(:v_sql,'')
        || ' (select file_name,file_date from ext.inbound_cfg_parameter) ';

        v_sql = IFNULL(:v_sql,'') || ' and nvl(recordstatus,0)= ';

        v_sql = IFNULL(:v_sql,'') || IFNULL(TO_VARCHAR(:v_statusflag),'');

        /* ORIGSQL: DBMS_OUTPUT.put_line (v_sql); */

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql INTO v_success_rec; */
        EXECUTE IMMEDIATE :v_sql INTO v_success_rec;

        --    Message4 := Message4 || ' ' || i.rn; 

        /* ORIGSQL: INSERT INTO ext.Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,(...) */
        INSERT INTO ext.Outbound_log_details
            (
                FILE_TYPE, FILE_NAME, FILE_DATE, STEPS_PROCESSED, RECORDS_PROCESSED, RECORDS_REJECTED
            )
        VALUES (
                :v_inbound_cfg_parameter.file_type,
                :v_inbound_cfg_parameter.file_name,
                :v_inbound_cfg_parameter.file_date,
                IFNULL(:Message4,'') || ' '|| IFNULL(TO_VARCHAR(:i.rn),''),
                :v_success_rec,
                :v_failure_rec
        );

        /* ORIGSQL: COMMIT; */
        COMMIT;
    END FOR;  /* ORIGSQL: END LOOP; */
END