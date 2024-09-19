CREATE PROCEDURE EXT.SP_OGP_BATCHES
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_max DECIMAL(38,10);  /* ORIGSQL: v_max number; */
    DECLARE v_inbound_cfg_parameter ROW LIKE EXT.INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        /* ORIGSQL: when no_data_found then */
        BEGIN
            /* ORIGSQL: NULL; */
            DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
        END;

        /* ORIGSQL: execute immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
        /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
        --CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

        SELECT *
        INTO
            v_inbound_cfg_parameter
        FROM
            EXT.INBOUND_CFG_PARAMETER;  

        /* ORIGSQL: update inbound_data_ogpt SET effectiveenddate=effectiveenddate+1 where effective(...) */
        UPDATE inbound_data_ogpt
            SET
            /* ORIGSQL: effectiveenddate = */
            effectiveenddate = Add_Days(effectiveenddate,+1)
        --FROM
            --ext.inbound_data_ogpt
        WHERE
            effectiveenddate <> TO_DATE('22000101','YYYYMMDD')
            AND recordstatus = 0
            AND filename = :v_inbound_cfg_parameter.file_name
            AND filedate = :v_inbound_cfg_parameter.file_date;  

        /* ORIGSQL: update inbound_data_ogpo SET effectiveenddate=effectiveenddate+1, creditenddate=(...) */
        UPDATE ext.inbound_data_ogpo
            SET
            /* ORIGSQL: effectiveenddate = */
            effectiveenddate = Add_Days(effectiveenddate,+1),
            /* ORIGSQL: creditenddate = */
            creditenddate = Add_Days(creditenddate,+1),
            /* ORIGSQL: processingenddate = */
            processingenddate = Add_Days(processingenddate,+1)
        --FROM
           --ext.inbound_data_ogpo
        WHERE
            effectiveenddate <> TO_DATE('22000101','YYYYMMDD')
            AND recordstatus = 0
            AND filename = :v_inbound_cfg_parameter.file_name
            AND filedate = :v_inbound_cfg_parameter.file_date;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into inbound_data_ogpt tgt using (SELECT payeeid, filename, filedate, effe(...) */
        MERGE INTO ext.inbound_data_ogpt AS tgt
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_OGPT' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select payeeid, filename, filedate, effectivestartdate, ROW_NUMBER() OVER (PART(...) */
                    payeeid,
                    filename,
                    filedate,
                    effectivestartdate,
                    ROW_NUMBER() OVER (PARTITION BY payeeid ORDER BY effectivestartdate ASC) AS batchnumber
                FROM
                    ext.inbound_data_ogpt
                WHERE
                    recordstatus = 0
                    AND filename = :v_inbound_cfg_parameter.file_name
                    AND filedate = :v_inbound_cfg_parameter.file_date
            ) AS src
            ON (tgt.payeeid = src.payeeid
                AND tgt.effectivestartdate = src.effectivestartdate
                AND tgt.filename = src.filename
                AND tgt.filedate = src.filedate
            	AND tgt.recordstatus = 0
                AND tgt.filename = :v_inbound_cfg_parameter.file_name
                AND tgt.filedate = :v_inbound_cfg_parameter.file_date
            )
        WHEN MATCHED THEN
            UPDATE SET tgt.batchnumber = src.batchnumber, tgt.newbatchnumber = src.batchnumber
            --WHERE
                --tgt.recordstatus = 0
                --AND filename = v_inbound_cfg_parameter.file_name
                --AND filedate = v_inbound_cfg_parameter.file_date
                ;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into inbound_data_ogpo tgt using (SELECT positionname, filename, filedate,(...) */
        MERGE INTO inbound_data_ogpo AS tgt 
            USING
            (
                SELECT   /* ORIGSQL: (select positionname, filename, filedate, effectivestartdate, 9000 + ROW_NUMBER((...) */
                    positionname,
                    filename,
                    filedate,
                    effectivestartdate,
                    9000 + ROW_NUMBER() OVER (PARTITION BY positionname ORDER BY effectivestartdate ASC) AS batchnumber
                FROM
                    ext.inbound_data_ogpo o
                INNER JOIN
                    EXT.STEL_HIERARCHYLEVEL l
                    ON o.titlename = l.name
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.STEL_HIERARCHYLEVEL@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.STEL_HIERARCHYLEVEL_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    recordstatus = 0
                    AND managername IS NULL
                    AND filename = :v_inbound_cfg_parameter.file_name
                    AND filedate = :v_inbound_cfg_parameter.file_date
            ) AS src
            ON (tgt.positionname = src.positionname
                AND tgt.effectivestartdate = src.effectivestartdate
                AND tgt.filename = src.filename
                AND tgt.filedate = src.filedate
            	AND tgt.recordstatus = 0
                AND tgt.filename = :v_inbound_cfg_parameter.file_name
                AND tgt.filedate = :v_inbound_cfg_parameter.file_date
            )
        WHEN MATCHED THEN
            UPDATE SET tgt.batchnumber = src.batchnumber
            --WHERE
                --tgt.recordstatus = 0
                --AND filename = v_inbound_cfg_parameter.file_name
                --AND filedate = v_inbound_cfg_parameter.file_date
                ;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_OGPO' not found */

        SELECT
            MAX(batchnumber)
        INTO
            v_max
        FROM
            ext.inbound_data_ogpo
        WHERE
            recordstatus = 0
            AND filename = :v_inbound_cfg_parameter.file_name
            AND filedate = :v_inbound_cfg_parameter.file_date;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into inbound_data_ogpo tgt using (SELECT positionname, filename, filedate,(...) */
        MERGE INTO inbound_data_ogpo AS tgt 
            USING
            (
                SELECT   /* ORIGSQL: (select positionname, filename, filedate, effectivestartdate, l.hierarchylevel *(...) */
                    positionname,
                    filename,
                    filedate,
                    effectivestartdate,
                    l.hierarchylevel *1000+ROW_NUMBER() OVER (PARTITION BY positionname ORDER BY effectivestartdate ASC) AS batchnumber
                FROM
                    ext.inbound_data_ogpo o
                INNER JOIN
                   STEL_HIERARCHYLEVEL l
                    ON o.titlename = l.name
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.STEL_HIERARCHYLEVEL@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.STEL_HIERARCHYLEVEL_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    recordstatus = 0
                    AND managername IS NOT NULL
                    AND filename = :v_inbound_cfg_parameter.file_name
                    AND filedate = :v_inbound_cfg_parameter.file_date
            ) AS src
            ON (tgt.positionname = src.positionname
                AND tgt.effectivestartdate = src.effectivestartdate
                AND tgt.filename = src.filename
                AND tgt.filedate = src.filedate
            	AND tgt.recordstatus = 0
                AND tgt.filename = :v_inbound_cfg_parameter.file_name
                AND tgt.filedate = :v_inbound_cfg_parameter.file_date
            )
        WHEN MATCHED THEN
            UPDATE SET tgt.batchnumber = src.batchnumber
            --WHERE
                --tgt.recordstatus = 0
                --AND filename = v_inbound_cfg_parameter.file_name
                --AND filedate = v_inbound_cfg_parameter.file_date
                ;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into inbound_data_ogpo tgt using (SELECT a.batchnumber, DENSE_RANK() OVER (...) */
        MERGE INTO inbound_data_ogpo AS tgt
            USING
            (
                SELECT   /* ORIGSQL: (select a.batchnumber, DENSE_RANK() OVER (ORDER BY a.batchnumber desc) newbatchn(...) */
                    a.batchnumber,
                    DENSE_RANK() OVER (ORDER BY a.batchnumber DESC) AS newbatchnumber 
                FROM
                    (
                        SELECT   /* ORIGSQL: (select distinct batchnumber from inbound_data_ogpo o where recordstatus=0 and f(...) */
                            DISTINCT
                            batchnumber
                        FROM
                            inbound_data_ogpo o
                        WHERE
                            recordstatus = 0
                            AND filename = :v_inbound_cfg_parameter.file_name
                            AND filedate = :v_inbound_cfg_parameter.file_date
                    ) AS a
                ) AS src
                ON (tgt.batchnumber = src.batchnumber
                	AND tgt.recordstatus = 0
                    AND filename = :v_inbound_cfg_parameter.file_name
                    AND filedate = :v_inbound_cfg_parameter.file_date
                   )
        WHEN MATCHED THEN
            UPDATE SET tgt.newbatchnumber = src.newbatchnumber
            --WHERE
                --tgt.recordstatus = 0
                --AND filename = v_inbound_cfg_parameter.file_name
                --AND filedate = v_inbound_cfg_parameter.file_date
                ;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: exception when no_data_found then */
END