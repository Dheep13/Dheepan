CREATE PROCEDURE EXT.SP_INBOUND_SCSS
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_SCSS';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_SCSS'; */
    DECLARE v_error VARCHAR(327);  /* ORIGSQL: v_error varchar2(327); */
    DECLARE v_field66 VARCHAR(50) = 'ST/SINGTELSHOP';  /* ORIGSQL: v_field66 varchar2(50):='ST/SINGTELSHOP'; */
    DECLARE v_seq DECIMAL(38,10);  /* ORIGSQL: v_seq number; */
    DECLARE v_inb_param ROW LIKE ext.INBOUND_CFG_PARAMETER;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /*insert into outbound_log_Details
            select :v_inb_param.file_type, :v_inb_param.file_name,:v_inb_param.file_date, 0, 0, 0, sysdate from dual;
            
            v_error:= SQLERRM;
            update ext.inbound_Data_staging
            set error_message = 'Error in stored procedure. Please verify the date and numeric field formats '||v_error;
            commit;
            */
            /* ORIGSQL: raise; */
            RESIGNAL;
        END;


 /* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inb_param
    FROM
        EXT.INBOUND_CFG_PARAMETER;  

    /* ORIGSQL: UPDATE ext.inbound_Data_Staging SET field100 = -1 WHERE filetype = :v_inb_param.FILE_(...) */
    UPDATE EXT.inbound_data_Staging
        SET
        /* ORIGSQL: field100 = */
        field100 = -1
    FROM
         EXT.inbound_data_Staging
    WHERE
        filetype = :v_inb_param.FILE_TYPE
        AND filename = :v_inb_param.FILE_NAME
        AND filedate = :v_inb_param.FILE_DATE;

    /* ORIGSQL: commit; */
    COMMIT;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update field100=-1 :' || :v_inb_param.file_type(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update field100=-1 :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Update field100=-1 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update field100=-1 :' || :v_inb_param.file_type || '-FileN(...) */

    --FIELD28->Terms MT/SG
    --FIELD12->GA10 1/2

    -- 

    /* ORIGSQL: INSERT INTO ext.inbound_Data_staging (filetype, filename, filedate, seq, field53, fi(...) */
    INSERT INTO ext.inbound_Data_staging
        (
            filetype, filename, filedate, seq, field53, field52,
            field68, field101, field1, field12, field17, field28,
            field58, field57, field66, field11, field16, field18,
            field19, field33, field34, field37, field35, field100,
            field15
        )
        SELECT   /* ORIGSQL: SELECT filetype, filename, filedate, MAX(seq), field53, SUM(field52) field52, SU(...) */
            filetype,
            filename,
            filedate,
            MAX(seq),
            field53,
            -- SUM(field52) AS field52,
            -- SUM(field68) AS field68,
            /*Deepan : Need to check if field52 and field68 are numbers before aggregation? For now just converting to number and aggregating */
            SUM(to_number(field52)) as field52,
            SUM(to_number(field68))  as field68,
            COUNT(*) AS txncount,
            field1,
            field12,
            field17,
            field28,
            field58,
            field57,
            field66,
            field11,
            field16,
            field18,
            field19,
            field33,
            field34,
            field37,
            field35,
            '0',
            field15
        FROM
            ext.inbound_Data_Staging
        WHERE
            field66 = 'ST/SINGTELSHOP'
            AND field100 = -1
        GROUP BY
            field53,
            field1,
            field12,
            field17,
            field28,
            field58,
            field57,
            field66,
            field11,
            field16,
            field18,
            field19,
            field33,
            field34,
            field37,
            field35,
            field15, filetype, filename, filedate;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert ext.inbound_Data_staging with Field100=0 :'(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert ext.inbound_Data_staging with Field100=0 :'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Insert ext.inbound_Data_staging with Field100=0 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert ext.inbound_Data_staging with Field100=0 :' || v_inb_p(...) */

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING_ARCH' not found */

    /* ORIGSQL: INSERT INTO ext.inbound_Data_Staging_Arch SELECT sysdate,ds.* FROM ext.inbound_Data_Stag(...) */
    INSERT INTO ext.inbound_Data_Staging_Arch
        SELECT   /* ORIGSQL: SELECT sysdate,ds.* FROM ext.inbound_Data_Staging ds WHERE ds.field100 = -1 AND ds.f(...) */
            CURRENT_TIMESTAMP,
            ds.*
        FROM
            ext.inbound_Data_Staging ds
        WHERE
            ds.field100 = -1
            AND ds.filetype = :v_inb_param.FILE_TYPE
            AND ds.filename = :v_inb_param.FILE_NAME
            AND ds.filedate = :v_inb_param.FILE_DATE;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert ext.inbound_Data_Staging_Arch:' || v_inb_pa(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert ext.inbound_Data_Staging_Arch:'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Insert ext.inbound_Data_Staging_Arch Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert ext.inbound_Data_Staging_Arch:' || :v_inb_param.file_ty(...) */

    /* ORIGSQL: DELETE FROM ext.inbound_Data_Staging WHERE field100 = -1 AND filetype = :v_inb_param.(...) */
    DELETE
    FROM
        ext.inbound_Data_Staging
    WHERE
        field100 = -1
        AND filetype = :v_inb_param.FILE_TYPE
        AND filename = :v_inb_param.FILE_NAME
        AND filedate = :v_inb_param.FILE_DATE;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update ext.inbound_Data_Staging field100=-1:' || v(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update ext.inbound_Data_Staging field100=-1:'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , 'Update ext.inbound_Data_Staging field100=-1 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update ext.inbound_Data_Staging field100=-1:' || :v_inb_param.(...) */

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */

    SELECT
        MAX(seq)
    INTO
        v_seq
    FROM
        ext.inbound_Data_staging;  

    /* ORIGSQL: Insert into ext.inbound_Data_staging(seq, filetype, filename, filedate, field53, fie(...) */
    INSERT INTO ext.inbound_Data_staging
        (
            seq, filetype, filename, filedate, field53, field52,
            field68, field101, field12, field17, field28, field58,
            field57, field16, field33, field34, field100, field15,
            field66
        )
        SELECT   /* ORIGSQL: Select v_seq+ROW_NUMBER() OVER (ORDER BY 0*0), filetype, filename, filedate, fie(...) */
            :v_seq+ROW_NUMBER() OVER (ORDER BY 0*0),  /* ORIGSQL: rownum */
            filetype,
            filename,
            filedate,
            field53,
            contractval,
            0,
            field101,
            field12 /*'ALL'*/,
            /*'Team'*/ 'ALL',
            field28,
            pos.name,
            'BSC-TEAM',
            'ALL PRODUCTS',
            'SCSS',
            'ALL',
            0,
            'Team',
            :v_field66
        FROM
            cs_position pos
        INNER JOIN
            cs_title t
            ON pos.titleseq = t.ruleelementownerseq 
        CROSS JOIN
            (
                SELECT   /* ORIGSQL: (select SUM(field52) contractval, SUM(field101) field101, field53, filename, fil(...) */
                    -- SUM(field52) AS contractval,
                    -- SUM(field101) AS field101,
                     /*Deepan : Need to check if field52 and field101 are numbers before aggregation? For now just converting to number and aggregating */
                    SUM(to_number(field52)) as contractval,
                    SUM(to_number(field101))  as field101,
                    field53,
                    filename,
                    filedate,
                    filetype,
                    MAX(seq) AS seq,
                    field12,
                    field28
                FROM
                    ext.inbound_Data_Staging
                GROUP BY
                    field53, filename, filedate, filetype, field12, field28
            ) AS cv
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_title'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            t.name LIKE'BSC%'
            AND t.name <> 'BSC-TEAM'
            --and cv.field53=pos.name
            AND t.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND LAST_DAY(TO_DATE(
                    (
                        SELECT   /* ORIGSQL: (select MAX(field53) from ext.inbound_Data_staging) */
                            MAX(field53)
                        FROM
                            ext.inbound_Data_staging
                    )
            ||'01','YYYYMMDD')) BETWEEN pos.effectivestartdate AND add_days(pos.effectiveenddate,-1)
            AND LAST_DAY(TO_DATE(
                    (
                        SELECT   /* ORIGSQL: (select MAX(field53) from ext.inbound_Data_staging) */
                            MAX(field53)
                        FROM
                            ext.inbound_Data_staging
                    )
            ||'01','YYYYMMDD')) BETWEEN t.effectivestartdate AND add_days(t.effectiveenddate,-1);

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '1.insert into ext.inbound_Data_Staging' || v_inb_p(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1.insert into ext.inbound_Data_Staging'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , '1.insert into ext.inbound_Data_Staging Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1.insert into ext.inbound_Data_Staging' || :v_inb_param.file_t(...) */
    SELECT
        MAX(seq)
    INTO
        v_seq
    FROM
        ext.inbound_Data_staging;   

    /* ORIGSQL: Insert into ext.inbound_Data_staging(seq, filetype, filename, filedate, field53, fie(...) */
    INSERT INTO ext.inbound_Data_staging
        (
            seq, filetype, filename, filedate, field53, field52,
            field68, field101, field12, field17, field28, field58,
            field57, field16, field33, field34, field100, field15,
            field66
        )
        SELECT   /* ORIGSQL: Select v_seq+ROW_NUMBER() OVER (ORDER BY 0*0), filetype, filename, filedate, fie(...) */
            :v_seq+ROW_NUMBER() OVER (ORDER BY 0*0),  /* ORIGSQL: rownum */
            filetype,
            filename,
            filedate,
            field53,
            contractval,
            0,
            field101  /*'ALL'*/,
            field12,
            'ALL',
            field28  /*'Indiv'*/,
            pos.name,
            NULL,
            'ALL PRODUCTS',
            'SCSS',
            'ALL',
            0,
            'Indiv',
            :v_field66
        FROM
            cs_position pos
        INNER JOIN
            cs_title t
            ON pos.titleseq = t.ruleelementownerseq
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select SUM(field52) contractval, SUM(field101) field101, field53, field58, file(...) */
                    -- SUM(field52) AS contractval,
                    -- SUM(field101) AS field101,
                     /*Deepan : Need to check if field52 and field101 are numbers before aggregation? For now just converting to number and aggregating */
                    SUM(to_number(field52)) as contractval,
                    SUM(to_number(field101))  as field101,
                    field53,
                    field58,
                    filename,
                    filedate,
                    filetype,
                    MAX(seq) AS seq,
                    field12,
                    field28
                FROM
                    ext.inbound_Data_Staging
                WHERE
                    field15 <> 'Team'
                GROUP BY
                    field53, field58, filename, filedate, filetype, field12, field28
            ) AS cv
            ON field58 = pos.name
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_title@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_title'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            t.name LIKE 'BSC%'
            AND t.name <> 'BSC-TEAM'
            AND t.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
            AND LAST_DAY(TO_DATE(
                    (
                        SELECT   /* ORIGSQL: (select MAX(field53) from ext.inbound_Data_staging) */
                            MAX(field53)
                        FROM
                            ext.inbound_Data_staging
                    )
            ||'01','YYYYMMDD')) BETWEEN pos.effectivestartdate AND add_days(pos.effectiveenddate,-1)
            AND LAST_DAY(TO_DATE(
                    (
                        SELECT   /* ORIGSQL: (select MAX(field53) from ext.inbound_Data_staging) */
                            MAX(field53)
                        FROM
                            ext.inbound_Data_staging
                    )
            ||'01','YYYYMMDD')) BETWEEN t.effectivestartdate AND add_days(t.effectiveenddate,-1);

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '2.insert into ext.inbound_Data_Staging' || v_inb_p(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '2.insert into ext.inbound_Data_Staging'|| IFNULL(:v_inb_param.file_type,'') || '-FileName:'|| IFNULL(:v_inb_param.file_name,'') || '-Date:'|| IFNULL(:v_inb_param.file_date,''),1,255) 
        , '2.insert into ext.inbound_Data_Staging Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '2.insert into ext.inbound_Data_Staging' || :v_inb_param.file_t(...) */

    /* ORIGSQL: update ext.inbound_Data_staging SET field102 = (SELECT MAX(field53) FROM inbound_dat(...) */
    UPDATE ext.inbound_Data_staging 
        SET
        /* ORIGSQL: field102 = */
        field102 = (
            SELECT   /* ORIGSQL: (select MAX(field53) from ext.inbound_Data_staging) */
                MAX(field53)
            FROM
                ext.inbound_Data_staging
        )
    FROM
        ext.inbound_Data_staging;  

    /* ORIGSQL: update ext.inbound_Data_staging SET field1=REPLACE(field1,chr(9),' '), Field2=REPLAC(...) */
    UPDATE ext.inbound_Data_staging
        SET
        /* ORIGSQL: field1 = */
        field1 = REPLACE(field1,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field2 = */
        Field2 = REPLACE(field2,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field3 = */
        Field3 = REPLACE(field3,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field4 = */
        Field4 = REPLACE(field4,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field5 = */
        Field5 = REPLACE(field5,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field6 = */
        Field6 = REPLACE(field6,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field7 = */
        Field7 = REPLACE(field7,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field8 = */
        Field8 = REPLACE(field8,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field9 = */
        Field9 = REPLACE(field9,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field10 = */
        Field10 = REPLACE(field10,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field11 = */
        Field11 = REPLACE(field11,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field12 = */
        Field12 = REPLACE(field12,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field13 = */
        Field13 = REPLACE(field13,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field14 = */
        Field14 = REPLACE(field14,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field15 = */
        Field15 = REPLACE(field15,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field16 = */
        Field16 = REPLACE(field16,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field17 = */
        Field17 = REPLACE(field17,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field18 = */
        Field18 = REPLACE(field18,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field19 = */
        Field19 = REPLACE(field19,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field20 = */
        Field20 = REPLACE(field20,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field21 = */
        Field21 = REPLACE(field21,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field22 = */
        Field22 = REPLACE(field22,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field23 = */
        Field23 = REPLACE(field23,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field24 = */
        Field24 = REPLACE(field24,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field25 = */
        Field25 = REPLACE(field25,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field26 = */
        Field26 = REPLACE(field26,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field27 = */
        Field27 = REPLACE(field27,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field28 = */
        Field28 = REPLACE(field28,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field29 = */
        Field29 = REPLACE(field29,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field30 = */
        Field30 = REPLACE(field30,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field31 = */
        Field31 = REPLACE(field31,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field32 = */
        Field32 = REPLACE(field32,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field33 = */
        Field33 = REPLACE(field33,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field34 = */
        Field34 = REPLACE(field34,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field35 = */
        Field35 = REPLACE(field35,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field36 = */
        Field36 = REPLACE(field36,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field37 = */
        Field37 = REPLACE(field37,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field38 = */
        Field38 = REPLACE(field38,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field39 = */
        Field39 = REPLACE(field39,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field40 = */
        Field40 = REPLACE(field40,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field41 = */
        Field41 = REPLACE(field41,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field42 = */
        Field42 = REPLACE(field42,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field43 = */
        Field43 = REPLACE(field43,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field44 = */
        Field44 = REPLACE(field44,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field45 = */
        Field45 = REPLACE(field45,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field46 = */
        Field46 = REPLACE(field46,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field47 = */
        Field47 = REPLACE(field47,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field48 = */
        Field48 = REPLACE(field48,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field49 = */
        Field49 = REPLACE(field49,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field50 = */
        Field50 = REPLACE(field50,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field51 = */
        Field51 = REPLACE(field51,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field52 = */
        Field52 = REPLACE(field52,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field53 = */
        Field53 = REPLACE(field53,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field54 = */
        Field54 = REPLACE(field54,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field55 = */
        Field55 = REPLACE(field55,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field56 = */
        Field56 = REPLACE(field56,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field57 = */
        Field57 = REPLACE(field57,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field58 = */
        Field58 = REPLACE(field58,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field59 = */
        Field59 = REPLACE(field59,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field60 = */
        Field60 = REPLACE(field60,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field61 = */
        Field61 = REPLACE(field61,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field62 = */
        Field62 = REPLACE(field62,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field63 = */
        Field63 = REPLACE(field63,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field64 = */
        Field64 = REPLACE(field64,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field65 = */
        Field65 = REPLACE(field65,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field66 = */
        Field66 = REPLACE(field66,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field67 = */
        Field67 = REPLACE(field67,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field68 = */
        Field68 = REPLACE(field68,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field69 = */
        Field69 = REPLACE(field69,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field70 = */
        Field70 = REPLACE(field70,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field71 = */
        Field71 = REPLACE(field71,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field72 = */
        Field72 = REPLACE(field72,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field73 = */
        Field73 = REPLACE(field73,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field74 = */
        Field74 = REPLACE(field74,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field75 = */
        Field75 = REPLACE(field75,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field76 = */
        Field76 = REPLACE(field76,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field77 = */
        Field77 = REPLACE(field77,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field78 = */
        Field78 = REPLACE(field78,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field79 = */
        Field79 = REPLACE(field79,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field80 = */
        Field80 = REPLACE(field80,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field81 = */
        Field81 = REPLACE(field81,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field82 = */
        Field82 = REPLACE(field82,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field83 = */
        Field83 = REPLACE(field83,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field84 = */
        Field84 = REPLACE(field84,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field85 = */
        Field85 = REPLACE(field85,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field86 = */
        Field86 = REPLACE(field86,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field87 = */
        Field87 = REPLACE(field87,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field88 = */
        Field88 = REPLACE(field88,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field89 = */
        Field89 = REPLACE(field89,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field90 = */
        Field90 = REPLACE(field90,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field91 = */
        Field91 = REPLACE(field91,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field92 = */
        Field92 = REPLACE(field92,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field93 = */
        Field93 = REPLACE(field93,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field94 = */
        Field94 = REPLACE(field94,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field95 = */
        Field95 = REPLACE(field95,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field96 = */
        Field96 = REPLACE(field96,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field97 = */
        Field97 = REPLACE(field97,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field98 = */
        Field98 = REPLACE(field98,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field99 = */
        Field99 = REPLACE(field99,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field100 = */
        Field100 = REPLACE(field100,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field101 = */
        Field101 = REPLACE(field101,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field102 = */
        Field102 = REPLACE(field102,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field103 = */
        Field103 = REPLACE(field103,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field104 = */
        Field104 = REPLACE(field104,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field105 = */
        Field105 = REPLACE(field105,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field106 = */
        Field106 = REPLACE(field106,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field107 = */
        Field107 = REPLACE(field107,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field108 = */
        Field108 = REPLACE(field108,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field109 = */
        Field109 = REPLACE(field109,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field110 = */
        Field110 = REPLACE(field110,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field111 = */
        Field111 = REPLACE(field111,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field112 = */
        Field112 = REPLACE(field112,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field113 = */
        Field113 = REPLACE(field113,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field114 = */
        Field114 = REPLACE(field114,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field115 = */
        Field115 = REPLACE(field115,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field116 = */
        Field116 = REPLACE(field116,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field117 = */
        Field117 = REPLACE(field117,CHAR(9),' '),  /* ORIGSQL: chr(9) */
        /* ORIGSQL: Field118 = */
        Field118 = REPLACE(field118,CHAR(9),' ')  /* ORIGSQL: chr(9) */
    FROM
        ext.inbound_Data_staging;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: exception when others then */
END