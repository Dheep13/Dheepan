CREATE PROCEDURE ext.SP_INBOUND_VSOP
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA ext
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_VSOP';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_VSOP'; */

    DECLARE v_inb_param   ROW LIKE ext.INBOUND_CFG_PARAMETER;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'ext.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inb_param
    FROM
        ext.INBOUND_CFG_PARAMETER;

    /* ORIGSQL: DELETE STEL_DATA_VSOP WHERE FILENAME = v_inb_param.FILE_NAME AND FILETYPE = v_in(...) */
    DELETE
    FROM
        ext.STEL_DATA_VSOP
    WHERE
        FILENAME = :v_inb_param.FILE_NAME
        AND FILETYPE = :v_inb_param.FILE_TYPE
        AND FILEDATE = :v_inb_param.FILE_DATE;

    /* RESOLVE: Oracle Database link: Remote table/view 'ext.STEL_DATA_VSOP@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'ext.STEL_DATA_VSOP_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: DELETE STEL_DATA_VSOP@stelext WHERE FILENAME = v_inb_param.FILE_NAME AND FILETYP(...) */
    DELETE
    FROM
        ext.STEL_DATA_VSOP
    WHERE
        FILENAME = :v_inb_param.FILE_NAME
        AND FILETYPE = :v_inb_param.FILE_TYPE
        AND FILEDATE = :v_inb_param.FILE_DATE;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Delete existing Records from STEL_DATA_VSOP :'(...) */
    CALL ext.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Delete existing Records from ext.STEL_DATA_VSOP  :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Delete existing Records from ext.STEL_DATA_VSOP Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Delete existing Records from ext.STEL_DATA_VSOP  :' || v_inb_(...) */

    /* RESOLVE: Identifier not found: Table/view 'ext.STEL_DATA_VSOP' not found */

    /* ORIGSQL: INSERT INTO STEL_DATA_VSOP SELECT FILENAME, FILETYPE, FILEDATE, CASE WHEN DBMTK_(...) */
    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_DATA_STAGING' not found */
    INSERT INTO ext.STEL_DATA_VSOP
        SELECT   /* ORIGSQL: SELECT FILENAME, FILETYPE, FILEDATE, CASE WHEN ext.IS_DATE(FIELD1, '(...) */
            FILENAME,
            FILETYPE,
            FILEDATE,
            CASE
                WHEN ext.ISDATE(FIELD1, 'YYYY-MM-DD HH24:MI:SS') = 0  /* ORIGSQL: IS_DATE (FIELD1, 'YYYY-MM-DD HH24:MI:SS') */
                THEN TO_DATE(FIELD1, 'YYYY-MM-DD HH24:MI:SS')  /* ORIGSQL: TO_DATE(FIELD1, 'YYYY-MM-DD HH24:MI:SS') */
                ELSE NULL
            END
            AS DTDISPATCH,
            CASE
                WHEN ext.IS_NUMBER(FIELD2) = 1  /* ORIGSQL: IS_NUMBER (FIELD2) */
                THEN TO_DECIMAL(FIELD2,38,18)  /* ORIGSQL: TO_NUMBER(FIELD2) */
                ELSE NULL
            END
            AS CHRECIPIENT,
            CASE
                WHEN ext.IS_NUMBER(FIELD3) = 1  /* ORIGSQL: IS_NUMBER (FIELD3) */
                THEN TO_DECIMAL(FIELD3,38,18)  /* ORIGSQL: TO_NUMBER(FIELD3) */
                ELSE NULL
            END
            AS CHDOCNO,
            CASE
                WHEN ext.IS_NUMBER(FIELD4) = 1  /* ORIGSQL: IS_NUMBER (FIELD4) */
                THEN TO_DECIMAL(FIELD4,38,18)  /* ORIGSQL: TO_NUMBER(FIELD4) */
                ELSE NULL
            END
            AS CHMATERIALNO,
            FIELD5 AS CHMATERIALDEC,
            CASE
                WHEN ext.IS_NUMBER(FIELD6) = 1  /* ORIGSQL: IS_NUMBER (FIELD6) */
                THEN TO_DECIMAL(FIELD6,38,18)  /* ORIGSQL: TO_NUMBER(FIELD6) */
                ELSE NULL
            END
            AS CHIMEI,
            FIELD7 AS VENDORID,
            FIELD8 AS DEALERID,
            CASE
                WHEN ext.IS_NUMBER(FIELD9) = 1  /* ORIGSQL: IS_NUMBER (FIELD9) */
                THEN TO_DECIMAL(FIELD9,38,18)  /* ORIGSQL: TO_NUMBER(FIELD9) */
                ELSE NULL
            END
            AS CHIMEI,
            FIELD10 AS VCHUSERID,
            CASE
                WHEN ext.ISDATE(FIELD11, 'YYYY-MM-DD HH24:MI:SS') = 0  /* ORIGSQL: IS_DATE (FIELD11, 'YYYY-MM-DD HH24:MI:SS') */
                THEN TO_DATE(FIELD11, 'YYYY-MM-DD HH24:MI:SS')  /* ORIGSQL: TO_DATE(FIELD11, 'YYYY-MM-DD HH24:MI:SS') */
                ELSE NULL
            END
            AS DTMODIFIEDDATE,
            CASE
                WHEN ext.ISDATE(FIELD12, 'YYYY-MM-DD HH24:MI:SS') = 0  /* ORIGSQL: IS_DATE (FIELD12, 'YYYY-MM-DD HH24:MI:SS') */
                THEN TO_DATE(FIELD12, 'YYYY-MM-DD HH24:MI:SS')  /* ORIGSQL: TO_DATE(FIELD12, 'YYYY-MM-DD HH24:MI:SS') */
                ELSE NULL
            END
            AS STATUSDT,
            CASE
                WHEN ext.IS_NUMBER(FIELD13) = 1  /* ORIGSQL: IS_NUMBER (FIELD13) */
                THEN TO_DECIMAL(FIELD13,38,18)  /* ORIGSQL: TO_NUMBER(FIELD13) */
                ELSE NULL
            END
            AS TRANSFERCOST,
            FIELD14 AS ISSOLD,
            CASE
                WHEN ext.ISDATE(FIELD15, 'YYYY-MM-DD HH24:MI:SS') = 0  /* ORIGSQL: IS_DATE (FIELD15, 'YYYY-MM-DD HH24:MI:SS') */
                THEN TO_DATE(FIELD15, 'YYYY-MM-DD HH24:MI:SS')  /* ORIGSQL: TO_DATE(FIELD15, 'YYYY-MM-DD HH24:MI:SS') */
                ELSE NULL
            END
            AS SOLDDATE,
            FIELD16 AS UPDATEDBY,
            FIELD17 AS TRANSCODE,
            FIELD18 AS REMARKS,
            CASE
                WHEN ext.IS_NUMBER(FIELD19) = 1  /* ORIGSQL: IS_NUMBER (FIELD19) */
                THEN TO_DECIMAL(FIELD19,38,18)  /* ORIGSQL: TO_NUMBER(FIELD19) */
                ELSE NULL
            END
            AS DISCOUNTCOST
        FROM
            ext.inbound_data_staging
        WHERE
            filetype = :v_inb_param.FILE_TYPE
            AND filename = :v_inb_param.FILE_NAME
            AND filedate = :v_inb_param.FILE_DATE;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert Staging records STEL_DATA_VSOP :' || v_(...) */
    CALL ext.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert Staging records ext.STEL_DATA_VSOP :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Insert Staging records ext.STEL_DATA_VSOP Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert Staging records ext.STEL_DATA_VSOP :' || v_inb_param.f(...) */

    /* RESOLVE: Oracle Database link: Remote table/view 'ext.ext.STEL_DATA_VSOP@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'ext.STEL_DATA_VSOP_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    /* ORIGSQL: INSERT INTO ext.STEL_DATA_VSOP@stelext select * from ext.STEL_DATA_VSOP WHERE filetype =(...) */
    INSERT INTO ext.STEL_DATA_VSOP
        SELECT   /* ORIGSQL: select * from ext.STEL_DATA_VSOP WHERE filetype = v_inb_param.FILE_TYPE AND filename(...) */
            *
        FROM
            ext.STEL_DATA_VSOP
        WHERE
            filetype = :v_inb_param.FILE_TYPE
            AND filename = :v_inb_param.FILE_NAME
            AND filedate = :v_inb_param.FILE_DATE;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert Staging records ext.STEL_DATA_VSOP :' || v_(...) */
    CALL ext.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert Staging records ext.STEL_DATA_VSOP :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Insert Staging records ext.STEL_DATA_VSOP Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert Staging records ext.STEL_DATA_VSOP :' || v_inb_param.f(...) */
END