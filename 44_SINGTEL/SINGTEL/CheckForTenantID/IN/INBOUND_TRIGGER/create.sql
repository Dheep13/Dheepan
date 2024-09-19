CREATE PROCEDURE EXT.INBOUND_TRIGGER
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    --DECLARE v_pre_post_sql inbound_cfg_txnfile%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_txnfile' not found (for %ROWTYPE declaration) */
    DECLARE v_pre_post_sql ROW LIKE inbound_cfg_txnfile;
    DECLARE v_sql VARCHAR(4000);  /* ORIGSQL: v_sql VARCHAR2(4000); */
    DECLARE v_proc_name VARCHAR(50) = 'Inbound_trigger';  /* ORIGSQL: v_proc_name varchar2(50):='Inbound_trigger'; */
    DECLARE v_single_quote VARCHAR(20) = '''';  /* ORIGSQL: v_single_quote varchar(20) :=''''; */
    DECLARE v_single_space VARCHAR(1) = ' ';  /* ORIGSQL: v_single_space varchar(1) :=' '; */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm VARCHAR2(4000); */
    DECLARE Message1 VARCHAR(255) = 'Stage 1: File to Staging';  /* ORIGSQL: Message1 VARCHAR(255) := 'Stage 1: File to Staging'; */
    DECLARE Message2 VARCHAR(255) = 'Stage 2: Validation on Staging';  /* ORIGSQL: Message2 VARCHAR(255) := 'Stage 2: Validation on Staging'; */
    DECLARE Message3 VARCHAR(255) = 'Stage 3: Staging to ODS Table :';  /* ORIGSQL: Message3 VARCHAR(255) := 'Stage 3: Staging to ODS Table :'; */
    DECLARE Message4 VARCHAR(255) = 'Stage 4: ODS Table to Callidus Final File : ';  /* ORIGSQL: Message4 VARCHAR(255) := 'Stage 4: ODS Table to Callidus Final File : '; */
    DECLARE v_success_rec DECIMAL(38,10) = 0;  /* ORIGSQL: v_success_rec NUMBER := 0; */
    DECLARE v_failure_rec DECIMAL(38,10) = 0;  /* ORIGSQL: v_failure_rec NUMBER := 0; */
    DECLARE v_temp_rec DECIMAL(38,10) = 0;  /* ORIGSQL: v_temp_rec NUMBER := 0; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;  
     
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            v_sqlerrm = SUBSTRING(::SQL_ERROR_MESSAGE,1,4000);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 4000) */

            /* ORIGSQL: insert into outbound_log_details (file_type, file_name, file_date, exception_mes(...) */
            INSERT INTO ext.outbound_log_details
                (
                    file_type,
                    file_name,
                    file_date,
                    exception_message
                )
            VALUES (
                    :v_inbound_cfg_parameter.file_type,
                    :v_inbound_cfg_parameter.file_name,
                    :v_inbound_cfg_parameter.file_date,
                    :v_sqlerrm
            );

            /* ORIGSQL: Commit; */
            COMMIT;

            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.FILES_ERROR_LOG' not found */

            /* ORIGSQL: insert into FILES_ERROR_LOG (LOGSEQ,file_type, file_name, file_date, exception_m(...) */
            INSERT INTO EXT.FILES_ERROR_LOG
                (
                    LOGSEQ, file_type,
                    file_name,
                    file_date,

                    exception_message
                )
            VALUES (
                    EXT.FILES_ERROR_LOG_SEQ.NEXTVAL,  /* RESOLVE: Identifier not found: Sequence not found: */
                                                                  /* ORIGSQL: FILES_ERROR_LOG_SEQ.NEXTVAL */
                    :v_inbound_cfg_parameter.file_type,
                    :v_inbound_cfg_parameter.file_name,
                    :v_inbound_cfg_parameter.file_date,
                    :v_sqlerrm
            );

            /* ORIGSQL: Commit; */
            COMMIT;

            /* ORIGSQL: raise; */
            RESIGNAL;
        END;


    --v_inbound_cfg_parameter   INBOUND_CFG_PARAMETER%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
   

    /* ORIGSQL: SP_LOGGER (NULL, 'Trigger: Start PreSQL-0', NULL, NULL, null) */
    CALL EXT.STEL_SP_LOGGER(NULL, 'Trigger: Start PreSQL-0', NULL, NULL, NULL);

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.LANDINGPAD_LOG' not found */

    /* ORIGSQL: INSERT INTO LANDINGPAD_LOG SELECT * FROM outbound_log_details; */
    INSERT INTO EXT.LANDINGPAD_LOG
        SELECT   /* ORIGSQL: SELECT * FROM outbound_log_details; */
            *
        FROM
            ext.outbound_log_details;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table outbound_log_details drop storage' ; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.OUTBOUND_LOG_DETAILS' not found */

    /* ORIGSQL: truncate table outbound_log_details drop storage ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE outbound_log_details';

    /* ORIGSQL: dbms_output.put_line('0'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /* ORIGSQL: dbms_output.put_line('1'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('1');
    BEGIN 
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
            /* ORIGSQL: when others then */
            BEGIN
                /* ORIGSQL: NULL; */
                DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
            END;


        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_TXNFILE' not found */

        SELECT *
        INTO
            v_pre_post_sql
        FROM
            ext.inbound_cfg_txnfile
        WHERE
            UPPER(filetype) = UPPER(:v_inbound_cfg_parameter.file_type);

        /* ORIGSQL: exception when others then */
    END;

    -------------------------------

    -----------------------------------------------------------------------------
    /* ORIGSQL: SP_DATA_PATCH(v_inbound_cfg_parameter.file_type) */
    CALL EXT.SP_DATA_PATCH(:v_inbound_cfg_parameter.file_type);

    IF :v_inbound_cfg_parameter.file_type = 'BCC-SCII-BundleOrders' 
    THEN   
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into inbound_data_staging tgt using (WITH x AS (SELECT distinct FIELD25, F(...) */
        MERGE INTO ext.inbound_data_staging AS tgt
            USING (
                WITH 
                x 
                AS (
                    SELECT   /* ORIGSQL: (SELECT distinct FIELD25, FIELD21,FIELD24 FROM inbound_data_staging WHERE FIELD2(...) */
                        DISTINCT
                        FIELD25,
                        FIELD21,
                        FIELD24
                    FROM
                        ext.inbound_data_staging
                    WHERE
                        FIELD25  
                        IN
                        (
                            SELECT   /* ORIGSQL: (select FIELD25 from inbound_data_staging where FIELD3 ='FBB' GROUP BY FIELD25 H(...) */
                                FIELD25
                            FROM
                                ext.inbound_data_staging
                            WHERE
                                FIELD3 = 'FBB'
                            GROUP BY
                                FIELD25
                            HAVING
                                COUNT(FIELD25) > 1
                        )
                        AND FIELD3 = 'FBB'
                    ORDER BY
                        FIELD25
                
                )
                SELECT   /* ORIGSQL: select FIELD25 from x group by FIELD25 having COUNT(FIELD25) >1) src on (tgt.FIE(...) */
                    FIELD25
                FROM
                    x
                    GROUP
                    BY FIELD25
                HAVING
                    COUNT(FIELD25) > 1) src
                ON (tgt.FIELD25 = src.FIELD25)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.field3 = 'FBB-OLD-CHECK';

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :v_inbound_cfg_parameter.file_type = 'BCC-SCII-ClosedBroadBandOrders' 
    THEN   
        /* ORIGSQL: Update INBOUND_DATA_STAGING SET field7 ='C' where filetype ='BCC-SCII-ClosedBroa(...) */
        UPDATE EXT.INBOUND_DATA_STAGING
            SET
            /* ORIGSQL: field7 = */
            field7 = 'C' 
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            filetype = 'BCC-SCII-ClosedBroadBandOrders'
            AND ADD_MONTHS(to_date(FIELD14,'yyyy-mm-dd HH24:MI:SS'),-6) > to_date(FIELD11,'yyyy-mm-dd HH24:MI:SS');  /* ORIGSQL: to_date(FIELD14,'yyyy-mm-dd HH24:MI:SS') */
                                                                                                                                                                                                 /* ORIGSQL: to_date(FIELD11,'yyyy-mm-dd HH24:MI:SS') */

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :v_inbound_cfg_parameter.file_type = 'BCC-SCII-SubmittedBroadBandOrders' 
    THEN   
        /* ORIGSQL: Update INBOUND_DATA_STAGING SET field7 ='C' where filetype ='BCC-SCII-SubmittedB(...) */
        UPDATE EXT.INBOUND_DATA_STAGING
            SET
            /* ORIGSQL: field7 = */
            field7 = 'C' 
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            filetype = 'BCC-SCII-SubmittedBroadBandOrders'
            AND field4 = 'Cease'
            AND field7 = 'AC';

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :v_inbound_cfg_parameter.file_type = 'BCC-SCII-SubmittedTVOrders' 
    THEN   
        /* ORIGSQL: Update INBOUND_DATA_STAGING SET field7 ='C' where filetype ='BCC-SCII-SubmittedT(...) */
        UPDATE EXT.INBOUND_DATA_STAGING
            SET
            /* ORIGSQL: field7 = */
            field7 = 'C' 
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            filetype = 'BCC-SCII-SubmittedTVOrders'
            AND field4 = 'Cease'
            AND field7 = 'AC';

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :v_inbound_cfg_parameter.file_type = 'BCC-SCII-SubmittedMobileOrders' 
    THEN   
        /* ORIGSQL: Update INBOUND_DATA_STAGING SET field7 ='C' where filetype ='BCC-SCII-SubmittedM(...) */
        UPDATE EXT.INBOUND_DATA_STAGING
            SET
            /* ORIGSQL: field7 = */
            field7 = 'C' 
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            filetype = 'BCC-SCII-SubmittedMobileOrders'
            AND field5 = 'Cease'
            AND field9 = 'AC';

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :v_inbound_cfg_parameter.file_type = 'HRCentral-SCII-SalesmanProfile' 
    THEN 
        /* ORIGSQL: delete from INBOUND_DATA_STAGING where FIELD1 in (SELECT FIELD1 FROM temp_test4)(...) */
        DELETE
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            FIELD1
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.TEMP_TEST4' not found */
            IN
            (
                SELECT   /* ORIGSQL: (select FIELD1 from temp_test4) */
                    FIELD1
                FROM
                    temp_test4
            );

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :v_inbound_cfg_parameter.file_type = 'HRCentral-SCII-SalesmanProfile' 
    THEN 
        /* ORIGSQL: delete from INBOUND_DATA_STAGING where FIELD1='1318820' and FIELD2='Oscar Gan Zo(...) */
        DELETE
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            FIELD1 = '1318820'
            AND FIELD2 = 'Oscar Gan Zong Peng'
            AND filetype = 'HRCentral-SCII-SalesmanProfile';

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: delete from INBOUND_DATA_STAGING where FIELD1='1308661' and filetype ='HRCentral(...) */
        DELETE
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            FIELD1 = '1308661'
            AND filetype = 'HRCentral-SCII-SalesmanProfile';
        --added by gopi requested by siti

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /*
    if v_inbound_cfg_parameter.file_type ='HRCentral-SCII-SalesmanProfile'
    then
    delete  from INBOUND_DATA_STAGING  where  FIELD1 in
    (select distinct name from cs_position@stelext)
    and filetype ='HRCentral-SCII-SalesmanProfile';
    commit;
    
    end if;
    */

    IF :v_inbound_cfg_parameter.file_type = 'HRCentral-SCII-SalesmanProfile' 
    THEN   
        /* ORIGSQL: Update INBOUND_DATA_STAGING SET field6 ='' where filetype ='HRCentral-SCII-Sales(...) */
        UPDATE EXT.INBOUND_DATA_STAGING
            SET
            /* ORIGSQL: field6 = */
            field6 = '' 
        FROM
            EXT.INBOUND_DATA_STAGING
        WHERE
            filetype = 'HRCentral-SCII-SalesmanProfile'
            AND FIELD1 IN ('1291164','1331301','1324800','1328168');

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */

    SELECT
        COUNT(*) 
    INTO
        v_success_rec
    FROM
        EXT.INBOUND_DATA_staging
    WHERE
        (UPPER(filetype), filename, filedate) IN
        (
            SELECT   /* ORIGSQL: (SELECT UPPER(v_inbound_cfg_parameter.file_type), v_inbound_cfg_parameter.file_n(...) */
                UPPER(:v_inbound_cfg_parameter.file_type),
                :v_inbound_cfg_parameter.file_name,
                :v_inbound_cfg_parameter.file_date
            FROM
                SYS.DUMMY   /* ORIGSQL: FROM DUAL) */
        );

    /* ORIGSQL: INSERT INTO Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,(...) */
    INSERT INTO ext.Outbound_log_details
        (
            FILE_TYPE, FILE_NAME, FILE_DATE, STEPS_PROCESSED, RECORDS_PROCESSED, RECORDS_REJECTED
        )
    VALUES (
            :v_inbound_cfg_parameter.file_type,
            :v_inbound_cfg_parameter.file_name,
            :v_inbound_cfg_parameter.file_date,
            :Message1,
            :v_success_rec,
            0
    );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: dbms_output.put_line('0002'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0002');

    -- Call Presql
    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Started PreSQL for in :' || v_inbound_cfg_para(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Started PreSQL for in :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_Date,''),1,255) 
        , 'Trigger: Start PreSQL', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Started PreSQL for in :' || v_inbound_cfg_parameter.file_(...) */

    IF :v_pre_post_sql.sqlproc IS NOT NULL
    THEN
        v_sql = :v_pre_post_sql.sqlproc;

        /* ORIGSQL: dbms_output.put_line(v_Sql); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* ORIGSQL: dbms_output.put_line('0003'); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0003');

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;
    END IF; 

    SELECT
        COUNT(*) 
    INTO
        v_success_rec
    FROM
        EXT.INBOUND_DATA_staging
    WHERE
        (UPPER(filetype), filename, filedate) IN
        (
            SELECT   /* ORIGSQL: (SELECT UPPER(v_inbound_cfg_parameter.file_type), v_inbound_cfg_parameter.file_n(...) */
                UPPER(:v_inbound_cfg_parameter.file_type),
                :v_inbound_cfg_parameter.file_name,
                :v_inbound_cfg_parameter.file_date
            FROM
                SYS.DUMMY   /* ORIGSQL: FROM DUAL) */
        );

    /* ORIGSQL: INSERT INTO Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,(...) */
    INSERT INTO Outbound_log_details
        (
            FILE_TYPE, FILE_NAME, FILE_DATE, STEPS_PROCESSED, RECORDS_PROCESSED, RECORDS_REJECTED
        )
    VALUES (
            :v_inbound_cfg_parameter.file_type,
            :v_inbound_cfg_parameter.file_name,
            :v_inbound_cfg_parameter.file_date,
            'Stage 2: After Preprocess',
            :v_success_rec,
            0
    );

    /* ORIGSQL: COMMIT; */
    COMMIT;

    -- Do basic validations on Staging Table
    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Started for in :' || v_inbound_cfg_parameter.f(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Started for in :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_Date,''),1,255) 
        , 'Trigger: Start Validator', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Started for in :' || v_inbound_cfg_parameter.file_type ||(...) */

    /* ORIGSQL: sp_inbound_Validator (v_inbound_cfg_parameter.file_type, v_inbound_cfg_parameter(...) */
    CALL EXT.SP_INBOUND_VALIDATOR(:v_inbound_cfg_parameter.file_type, :v_inbound_cfg_parameter.file_name, :v_inbound_cfg_parameter.file_date, 1);

    /* ORIGSQL: dbms_output.put_line('0004'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0004');

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Started ARCHIVE for in :' || v_inbound_cfg_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Started ARCHIVE for in :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_Date,''),1,255) 
        , 'Trigger: Start POSTVALPROC', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Started ARCHIVE for in :' || v_inbound_cfg_parameter.file(...) */

    IF :v_pre_post_sql.postValidationproc IS NOT NULL
    THEN
        v_sql = :v_pre_post_sql.postValidationproc;

        /* ORIGSQL: dbms_output.put_line('0005'); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0005');

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_sql; */
        EXECUTE IMMEDIATE :v_sql;
    END IF;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Started ARCHIVE for in :' || v_inbound_cfg_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Started ARCHIVE for in :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_Date,''),1,255) 
        , 'Trigger: Start TXNMAP', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Started ARCHIVE for in :' || v_inbound_cfg_parameter.file(...) */

    /* ORIGSQL: sp_inbound_staging_Archive() */
    CALL EXT.SP_INBOUND_STAGING_ARCHIVE();

    /* ORIGSQL: dbms_output.put_line('0006'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0006');

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Started MAP for in :' || v_inbound_cfg_paramet(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Started MAP for in :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_Date,''),1,255) 
        , 'Trigger: Start TXNMAP', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Started MAP for in :' || v_inbound_cfg_parameter.file_typ(...) */

    -- call the Main Mapping Proc
    /* ORIGSQL: sp_inbound_txn_map (v_inbound_cfg_parameter.file_type, v_inbound_cfg_parameter.f(...) */
    CALL EXT.SP_INBOUND_TXN_MAP(:v_inbound_cfg_parameter.file_type, :v_inbound_cfg_parameter.file_name, :v_inbound_cfg_parameter.file_date, 1);

    -- Call the Post Sql Proc
    /* ORIGSQL: dbms_output.put_line('0007'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0007');

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Started PostSQL for in :' || v_inbound_cfg_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Started PostSQL for in :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_Date,''),1,255) 
        , 'Trigger: Start PostSQL'||IFNULL(:v_pre_post_sql.postproc,'null')   /* ORIGSQL: SUBSTR(v_proc_name || 'Started PostSQL for in :' || v_inbound_cfg_parameter.file(...) */
        , NULL, NULL, NULL);  /* ORIGSQL: nvl(v_pre_post_sql.postproc,'null') */

    IF :v_pre_post_sql.postproc IS NOT NULL
    THEN
        v_sql = replace(:v_pre_post_sql.postproc, :v_single_quote, IFNULL(:v_single_quote,'')||IFNULL(:v_single_quote,''));  /* ORIGSQL: replace(v_pre_post_sql.postproc, v_single_quote, v_single_quote||v_single_quote) */

        /* ORIGSQL: dbms_output.put_line(v_sql); */
        --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln(:v_sql);

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: EXECUTE IMMEDIATE v_pre_post_sql.postproc; */
        EXECUTE IMMEDIATE :v_pre_post_sql.postproc;
    END IF;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Ended PostSQL for in :' || v_inbound_cfg_param(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Ended PostSQL for in :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_Date,''),1,255) 
        , 'Trigger: End PostSQL'||IFNULL(:v_pre_post_sql.postproc,'null')   /* ORIGSQL: SUBSTR(v_proc_name || 'Ended PostSQL for in :' || v_inbound_cfg_parameter.file_t(...) */
        , NULL, NULL, NULL);  /* ORIGSQL: nvl(v_pre_post_sql.postproc,'null') */

    /* ORIGSQL: dbms_output.put_line('0008'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('0008');

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: exception when others then */
END