CREATE PROCEDURE EXT.SP_OGPO_HRCENTRALTITLEMAP
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_errm VARCHAR(4000);  /* ORIGSQL: v_errm VARCHAR2(4000); */
    DECLARE v_inbound_cfg_parameter ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_OGPO_HRCENTRALTITLEMAP';  /* ORIGSQL: v_proc_name varchar2(127):='SP_OGPO_HRCENTRALTITLEMAP'; */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            v_errm = SUBSTRING(::SQL_ERROR_MESSAGE,1,4000);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 4000) */

            /* ORIGSQL: dbms_output.put_line('Titlemap Error  '||v_errm); */
            CALL SQLSCRIPT_PRINT:PRINT_LINE('Titlemap Error  '||IFNULL(:v_errm,''));

            /* ORIGSQL: SP_LOGGER ('procedure Name sp_ogpo_hrcentraltitlemap', ' Error in PreSQL', NULL(...) */
            CALL EXT.STEL_SP_LOGGER('procedure Name sp_ogpo_hrcentraltitlemap', ' Error in PreSQL', NULL, NULL, :v_errm);

            /* ORIGSQL: COMMIT; */
            COMMIT;

            /* ORIGSQL: raise; */
            RESIGNAL;
        END;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

        SELECT *
        INTO
            v_inbound_cfg_parameter
        FROM
            ext.inbound_cfg_parameter;

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
        /* ORIGSQL: update ext.inbound_data_staging a SET (a.field41, a.field42, a.field43) = (SELECT b.(...) */
        UPDATE ext.inbound_data_staging a
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_TITLEMAP' not found */
            SET
            /* ORIGSQL: (a.field41, a.field42, a.field43) = */
            (field41, field42, field43) = (
                SELECT   /* ORIGSQL: (select b.position_title, b.channel, CASE WHEN b.reporting_manager_flag = 'Y' TH(...) */
                    b.position_title,
                    b.channel,
                    CASE
                        WHEN b.reporting_manager_flag = 'Y'
                        THEN a.field30
                        WHEN b.reporting_manager_flag = 'N'
                        THEN NULL
                    END
                FROM
                    ext.inbound_cfg_titlemap b
                WHERE
                    IFNULL(a.field26, 'X') = IFNULL(b.ou_lvl_nm_01, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_01, 'X') */
                                                                          /* ORIGSQL: NVL(a.field26, 'X') */
                    AND IFNULL(a.field25, 'X') = IFNULL(b.ou_lvl_nm_02, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_02, 'X') */
                                                                              /* ORIGSQL: NVL(a.field25, 'X') */
                    AND IFNULL(a.field24, 'X') = IFNULL(b.ou_lvl_nm_03, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_03, 'X') */
                                                                              /* ORIGSQL: NVL(a.field24, 'X') */
                    AND IFNULL(a.field23, 'X') = IFNULL(b.ou_lvl_nm_04, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_04, 'X') */
                                                                              /* ORIGSQL: NVL(a.field23, 'X') */
                    AND IFNULL(a.field22, 'X') = IFNULL(b.ou_lvl_nm_05, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_05, 'X') */
                                                                              /* ORIGSQL: NVL(a.field22, 'X') */
                    /* --AND NVL (a.field21, 'X') = NVL (b.ou_lvl_nm_06, 'X')  changed by sankar for hrc file issue on may 22 */
                    AND IFNULL(a.field37, 'X') = IFNULL(b.ou_lvl_nm_06, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_06, 'X') */
                                                                              /* ORIGSQL: NVL(a.field37, 'X') */
                    AND IFNULL(a.field20, 'X') = IFNULL(b.ou_lvl_nm_07, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_07, 'X') */
                                                                              /* ORIGSQL: NVL(a.field20, 'X') */
                    AND IFNULL(a.field19, 'X') = IFNULL(b.ou_lvl_nm_08, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_08, 'X') */
                                                                              /* ORIGSQL: NVL(a.field19, 'X') */
                    AND IFNULL(a.field18, 'X') = IFNULL(b.ou_lvl_nm_09, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_09, 'X') */
                                                                              /* ORIGSQL: NVL(a.field18, 'X') */
                    AND IFNULL(a.field17, 'X') = IFNULL(b.ou_lvl_nm_10, 'X')  /* ORIGSQL: NVL(b.ou_lvl_nm_10, 'X') */
                                                                              /* ORIGSQL: NVL(a.field17, 'X') */
                    AND IFNULL(UPPER(a.field27), 'X') = IFNULL(UPPER(b.title_biz), 'X')  /* ORIGSQL: NVL(upper(b.title_biz), 'X') */
                                                                                         /* ORIGSQL: NVL(upper(a.field27), 'X') */
                    /* AND TO_DATE (a.field39, 'dd/mm/yyyy') > (SELECT datevalue
                                  FROM inbound_cfg_genericparameter
                             WHERE key = 'HR Profile MAX Last date Updated')
                    */
            )
        FROM
            ext.inbound_data_staging a
        WHERE
            (a.filetype, a.filename, a.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT file_type, File_name, File_date FROM ext.inbound_cfg_parameter WHERE OBJECT_(...) */
                    file_type,
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            );

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Field41,Field42,Field43 :' || v_inbound(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Field41,Field42,Field43 :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
            , 'Update Field41,Field42,Field43  Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Field41,Field42,Field43 :' || v_inbound_cfg_parame(...) */

        /* ORIGSQL: dbms_output.put_line('Titlemap Title Rows Updated: '||SQL%ROWCOUNT); */
        CALL  SQLSCRIPT_PRINT:PRINT_LINE('Titlemap Title Rows Updated: '||::ROWCOUNT);  

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: update ext.inbound_data_staging a SET a.field28='PF',A.FIELD42='Direct Sales' where (...) */
        UPDATE ext.inbound_data_staging a
            SET
            /* ORIGSQL: a.field28 = */
            field28 = 'PF',
            /* ORIGSQL: A.FIELD42 = */
            FIELD42 = 'Direct Sales' 
        FROM
            ext.inbound_data_staging a
        WHERE
            a.field1 = '1308661'
            AND (a.filename, a.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM ext.inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            );

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: update ext.inbound_data_staging a SET A.FIELD41='Digital Telesales - Sudong TL' wher(...) */
        UPDATE ext.inbound_data_staging a
            SET
            /* ORIGSQL: A.FIELD41 = */
            FIELD41 = 'Digital Telesales - Sudong TL' 
        FROM
            ext.inbound_data_staging a
        WHERE
            a.field1 IN ('1302782','1312901','1290457','1291164','1291370','1312616','1312582','1324800','1328168')/* -->added by gopi as user Looweida/siti requested */
            AND (a.filename, a.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM ext.inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            );

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: update ext.inbound_data_staging a SET A.FIELD41='Digital Telesales - Sales Manager' (...) */
        UPDATE ext.inbound_data_staging a
            SET
            /* ORIGSQL: A.FIELD41 = */
            FIELD41 = 'Digital Telesales - Sales Manager' 
        FROM
            ext.inbound_data_staging a
        WHERE
            a.field1 = '1252635'
            AND (a.filename, a.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM ext.inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            );

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: update ext.inbound_data_staging a SET A.FIELD29='S3' where a.field1='1253836' AND (a(...) */
        UPDATE ext.inbound_data_staging a
            SET
            /* ORIGSQL: A.FIELD29 = */
            FIELD29 = 'S3' 
        FROM
            ext.inbound_data_staging a
        WHERE
            a.field1 = '1253836'
            AND (a.filename, a.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM ext.inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            );

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: update ext.inbound_data_staging a SET A.FIELD41='STS - Assistant Manager' where a.fi(...) */
        UPDATE ext.inbound_data_staging a
            SET
            /* ORIGSQL: A.FIELD41 = */
            FIELD41 = 'STS - Assistant Manager' 
        FROM
            ext.inbound_data_staging a
        WHERE
            a.field1 IN ('1250187','1251874','1258822')
            AND (a.filename, a.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM ext.inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            );

        /* ORIGSQL: COMMIT; */
        COMMIT;  

        /* ORIGSQL: update ext.inbound_data_staging a SET a.ERROR_MESSAGE = a.ERROR_MESSAGE || 'Title No(...) */
        UPDATE ext.inbound_data_staging a
            SET
            /* ORIGSQL: a.ERROR_MESSAGE = */
            ERROR_MESSAGE = IFNULL(a.ERROR_MESSAGE,'') || 'Title Not Configured in TitleMap',
            /* ORIGSQL: a.error_flag = */
            error_flag = 1
        FROM
            ext.inbound_data_staging a
        WHERE
            a.field41 IS NULL
            AND (a.filename, a.filedate)  
            IN
            (
                SELECT   /* ORIGSQL: (SELECT File_name, File_date FROM ext.inbound_cfg_parameter WHERE OBJECT_NAME = 'SP_(...) */
                    File_name,
                    File_date
                FROM
                    ext.inbound_cfg_parameter
                WHERE
                    OBJECT_NAME = 'SP_INBOUND_TXN_MAP'
            );

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update ErrorFlag and Message for Title :' || v(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update ErrorFlag and Message for Title :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
            , 'Update ErrorFlag and Message for Title Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update ErrorFlag and Message for Title :' || v_inbound_cf(...) */

        /* ORIGSQL: dbms_output.put_line('Titlemap Error Rows Updated: '||SQL%ROWCOUNT); */
        CALL  SQLSCRIPT_PRINT:PRINT_LINE('Titlemap Error Rows Updated: '||::ROWCOUNT);  

        /*UPDATE inbound_data_ogpo a
        SET a.recordstatus = '0'
         WHERE a.genericdate6 = (SELECT datevalue
                                    FROM inbound_cfg_genericparameter
                               WHERE key = 'HR Profile MAX Last date Updated')
         AND (a.filename, a.filedate) IN
                     (SELECT File_name, File_date
                            FROM ext.inbound_cfg_parameter
                       WHERE OBJECT_NAME = 'SP_INBOUND_TXN_MAP');
        dbms_output.put_line('Titlemap OGPO? Rows Updated: '||SQL%ROWCOUNT);
        COMMIT;
        
        UPDATE inbound_data_ogpo a
        SET a.genericdate6 = ''
         WHERE a.genericdate6 IS NOT NULL
         AND (a.filename, a.filedate) IN
                     (SELECT File_name, File_date
                            FROM ext.inbound_cfg_parameter
                       WHERE OBJECT_NAME = 'SP_INBOUND_TXN_MAP');
        */

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END