CREATE PROCEDURE EXT.SP_INB_SCII_DASH_POSTVALIDATE
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_prmtr ROW LIKE inbound_cfg_parameter; --%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_dateerrflag DECIMAL(38,10);  /* ORIGSQL: v_dateerrflag NUMBER; */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            /* ORIGSQL: RAISE; */
            RESIGNAL;
        END;


    /*******
    
     From the BCC file, Reject records with empty Order ID, Enter Date, Customer ID, Service Number
     Duplicate check on Order ID \x96 Take the latest one
     Duplicate check on Customer ID and Product \x96 Take the latest one
     Duplicate check on Order, Customer and Product \x96 Take the latest one
     If the BCC Dash account data has the same Service number for 2 customer IDs in the same month, both are payable \x96 do not reject
    
    Mappings as of 31st Jan 2018 are
    
    Orderid = FIELD2
    Enter date = FIELD3
    Customer ID = FIELD4
    Service Number =  FIELD6
    product = FIELD7
    
    
    *************/

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_VALIDATION_ERRORS' not found */

    SELECT
        COUNT(*)
    INTO
        v_dateerrflag
    FROM
        EXT.INBOUND_VALIDATION_ERRORS
    WHERE
        error_message LIKE '%FIELD3%Date not in required format%';

    -- Mandate Fields

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_POSTVALIDATION_ERRORS' not found */

    /* ORIGSQL: INSERT INTO INBOUND_POSTVALIDATION_ERRORS SELECT filetype, filename, filedate, s(...) */
    INSERT INTO EXT.INBOUND_POSTVALIDATION_ERRORS
        SELECT   /* ORIGSQL: SELECT filetype, filename, filedate, seq, 'FIELD2/FIELD3/FIELD4/FIELD6 are Null'(...) */
            filetype,
            filename,
            filedate,
            seq,
            'FIELD2/FIELD3/FIELD4/FIELD6 are Null' AS error_message,
            'Mandate Fields' AS ERROR_TYPE
        FROM
            ext.inbound_data_staging
        WHERE
            filetype = :v_prmtr.file_type
            AND filename = :v_prmtr.file_name
            AND filedate = :v_prmtr.file_date
            AND (FIELD2 IS NULL
                OR FIELD3 IS NULL
                OR FIELD4 IS NULL
            OR FIELD6 IS NULL);

    /* ORIGSQL: COMMIT; */
    COMMIT;

    IF :v_dateerrflag = 0
    THEN
        -- Duplicate checks for order id
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
        /* ORIGSQL: INSERT INTO INBOUND_POSTVALIDATION_ERRORS SELECT filetype, filename, filedate, s(...) */
        INSERT INTO EXT.INBOUND_POSTVALIDATION_ERRORS
            SELECT   /* ORIGSQL: SELECT filetype, filename, filedate, seq, 'FIELD2 has duplicates and only latest(...) */
                filetype,
                filename,
                filedate,
                seq,
                'FIELD2 has duplicates and only latest one is considered' AS error_message,
                'Duplicate Check' AS ERROR_TYPE
            FROM
                (
                    /*SELECT   /* ORIGSQL: (SELECT x.*, DENSE_RANK() OVER (PARTITION BY FIELD2 ORDER BY TO_DATE(FIELD3, 'DD(...) 
                        x.*,
                        DENSE_RANK()
                        OVER (
                            PARTITION BY FIELD2
                            ORDER BY to_date(FIELD3, 'DD MON YYYY HH24:MI:SS'), "ROWID") AS rnk  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'ROWID' (=reserved word in HANA) renamed to '"ROWID"'; ensure all calls/references are renamed accordingly 
                                                                                                                                                            /* ORIGSQL: TO_DATE(FIELD3, 'DD MON YYYY HH24:MI:SS') 
                                                                                                                                                            /* ORIGSQL: ROWID 
                    FROM
                        ext.inbound_data_staging x
                    WHERE
                        x.filetype = :v_prmtr.file_type
                        AND x.filename = :v_prmtr.file_name
                        AND x.filedate = :v_prmtr.file_date*/ --Sanjay: commenting this as ROWID is not supported in HANA
                
                	SELECT *,
                          DENSE_RANK() OVER (
                           PARTITION BY FIELD2
                           ORDER BY
                             -- TO_DATE(FIELD3, 'DD-MON-YYYY HH24:MI:SS'),
                                  FIELD3, a.rownum
                          ) AS rnk
                        FROM 
							(select row_number() over () as rownum , 
							*  from  ext.inbound_data_staging) a
							WHERE a.filetype = :v_prmtr.file_type
							AND a.filename = :v_prmtr.file_name
							AND a.filedate = :v_prmtr.file_date
							                ) AS dbmtk_corrname_12808
							            WHERE
							               dbmtk_corrname_12808.rnk > 1;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        -- Duplicate checks for Customer ID and Product  
        /* ORIGSQL: INSERT INTO INBOUND_POSTVALIDATION_ERRORS SELECT filetype, filename, filedate, s(...) */
        INSERT INTO EXT.INBOUND_POSTVALIDATION_ERRORS
            SELECT   /* ORIGSQL: SELECT filetype, filename, filedate, seq, 'FIELD4, FIELD7 has duplicates and onl(...) */
                filetype,
                filename,
                filedate,
                seq,
                'FIELD4 , FIELD7 has duplicates and only latest one is considered' AS error_message,
                'Duplicate Check' AS ERROR_TYPE
            FROM
                (
                    /*SELECT   /* ORIGSQL: (SELECT x.*, DENSE_RANK() OVER (PARTITION BY FIELD4, FIELD7 ORDER BY TO_DATE(FIE(...) 
                        x.*,
                        DENSE_RANK()
                        OVER (
                            PARTITION BY FIELD4, FIELD7
                            ORDER BY to_date(FIELD3,'DD MON YYYY HH24:MI:SS'), '"ROWID"') AS rnk  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'ROWID' (=reserved word in HANA) renamed to '"ROWID"'; ensure all calls/references are renamed accordingly */
                                                                                                                                                           /* ORIGSQL: TO_DATE(FIELD3,'DD MON YYYY HH24:MI:SS') */
                                                                                                                                                           /* ORIGSQL: ROWID 
                    FROM
                        ext.inbound_data_staging x
                    WHERE
                        x.filetype = :v_prmtr.file_type
                        AND x.filename = :v_prmtr.file_name
                        AND x.filedate = :v_prmtr.file_date*/
                        SELECT *,
                          DENSE_RANK() OVER (
                           PARTITION BY FIELD4, FIELD7
                           ORDER BY
                             -- TO_DATE(FIELD3, 'DD-MON-YYYY HH24:MI:SS'),
                                  FIELD3, a.rownum
                          ) AS rnk
                        FROM 
							(select row_number() over () as rownum , 
							*  from  ext.inbound_data_staging) a
							WHERE a.filetype = :v_prmtr.file_type
							AND a.filename = :v_prmtr.file_name
							AND a.filedate = :v_prmtr.file_date
							                ) AS dbmtk_corrname_12811
							            WHERE
							               dbmtk_corrname_12811.rnk > 1;

        /* ORIGSQL: COMMIT; */
        COMMIT;

        -- Duplicate checks for Order, Customer and Product  
        /* ORIGSQL: INSERT INTO INBOUND_POSTVALIDATION_ERRORS SELECT filetype, filename, filedate, s(...) */
        INSERT INTO EXT.INBOUND_POSTVALIDATION_ERRORS
            SELECT   /* ORIGSQL: SELECT filetype, filename, filedate, seq, 'FIELD2, FIELD4, FIELD7 has duplicates(...) */
                filetype,
                filename,
                filedate,
                seq,
                'FIELD2 , FIELD4 , FIELD7 has duplicates and only latest one is considered' AS error_message,
                'Duplicate Check' AS ERROR_TYPE
            FROM
                (
                    /*SELECT   /* ORIGSQL: (SELECT x.*, DENSE_RANK() OVER (PARTITION BY FIELD2, FIELD4, FIELD7 ORDER BY TO_(...) 
                        x.*,
                        DENSE_RANK()
                        OVER (
                            PARTITION BY FIELD2, FIELD4, FIELD7
                            ORDER BY to_date(FIELD3, 'DD MON YYYY HH24:MI:SS'), "ROW_ID") AS rnk  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'ROWID' (=reserved word in HANA) renamed to '"ROWID"'; ensure all calls/references are renamed accordingly */
                                                                                                                                                            /* ORIGSQL: TO_DATE(FIELD3, 'DD MON YYYY HH24:MI:SS') */
                                                                                                                                                            /* ORIGSQL: ROWID 
                    FROM
                        ext.inbound_data_staging x
                    WHERE
                        x.filetype = :v_prmtr.file_type
                        AND x.filename = :v_prmtr.file_name
                        AND x.filedate = :v_prmtr.file_date*/
                        SELECT *,
                          DENSE_RANK() OVER (
                           PARTITION BY FIELD2, FIELD4, FIELD7
                           ORDER BY
                             -- TO_DATE(FIELD3, 'DD-MON-YYYY HH24:MI:SS'),
                                  FIELD3, a.rownum
                          ) AS rnk
                        FROM 
							(select row_number() over () as rownum , 
							*  from  ext.inbound_data_staging) a
							WHERE a.filetype = :v_prmtr.file_type
							AND a.filename = :v_prmtr.file_name
							AND a.filedate = :v_prmtr.file_date
							                ) AS dbmtk_corrname_12814
							            WHERE
							               dbmtk_corrname_12814.rnk > 1;
                
            
        COMMIT;
    END IF;

    -- Call the merger procedure

    /* ORIGSQL: sp_errors_merger ('INBOUND_POSTVALIDATION_ERRORS', 'TGT.SEQ') */
    CALL EXT.SP_ERRORS_MERGER('INBOUND_POSTVALIDATION_ERRORS', 'TGT.SEQ');

    /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END