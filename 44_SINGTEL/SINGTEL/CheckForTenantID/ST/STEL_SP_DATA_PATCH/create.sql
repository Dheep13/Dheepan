CREATE PROCEDURE EXT.STEL_SP_DATA_PATCH
(
    IN file_type VARCHAR(75)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                               /* ORIGSQL: file_type IN VARCHAR2 */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/ 
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: NULL; */
            DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
        END;



    /* ORIGSQL: SP_LOGGER (SUBSTR('SP_DATA_PATCH-1' || 'Started :' || file_type, 255), 'SP_DATA_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING('SP_DATA_PATCH-1'|| 'Started :'|| IFNULL(:file_type,''),255) 
        , 'SP_DATA_PATCH Started', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR('SP_DATA_PATCH-1' || 'Started :' || file_type, 255) */

    IF :file_type = 'BCC-SCII-SubmittedBroadBandOrders' 
    THEN
        /* RESOLVE: Identifier not found: Table/view 'STELADMIN.INBOUND_DATA_STAGING' not found */

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x
            /* RESOLVE: Identifier not found: Table/view 'STELADMIN.REF_MONTHLY_DATA_PATCH' not found */
            /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,dealer_code,LOWER(salesman_code) as salesman_co(...) */
                    DISTINCT
                    order_action_id,
                    dealer_code,
                    LOWER(salesman_code) AS salesman_code
                FROM
                    EXT.REF_MONTHLY_DATA_PATCH a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field9
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'SNBB'
            ) AS y
            ON (x.field9 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedBroadBandOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD30 = dealer_code,FIELD32 = salesman_code;

        /* ORIGSQL: commit; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x
            /* RESOLVE: Identifier not found: Table/view 'STELADMIN.PATCH_DEALER' not found */
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,dealer_code FROM STELADMIN.PATCH_DEALER a join (...) */
                    DISTINCT
                    order_action_id,
                    dealer_code
                FROM
                    EXT.PATCH_DEALER a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field9
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'DEALER'
            ) AS y
            ON (x.field9 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedBroadBandOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD30 = dealer_code;

        /* ORIGSQL: commit; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x
            /* RESOLVE: Identifier not found: Table/view 'STELADMIN.PATCH_VENDOR' not found */
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,VENDOR_code FROM STELADMIN.PATCH_VENDOR a join (...) */
                    DISTINCT
                    order_action_id,
                    VENDOR_code
                FROM
                    EXT.PATCH_VENDOR a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field9
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'VENDOR'
            ) AS y
            ON (x.field9 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedBroadBandOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD29 = VENDOR_CODE;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :file_type = 'BCC-SCII-SubmittedTVOrders' 
    THEN 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x  
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,dealer_code,LOWER(salesman_code) as salesman_co(...) */
                    DISTINCT
                    order_action_id,
                    dealer_code,
                    LOWER(salesman_code) AS salesman_code
                FROM
                    EXT.REF_MONTHLY_DATA_PATCH a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field10
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'TV'
            ) AS y
            ON (x.field10 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedTVOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD21 = dealer_code,FIELD23 = salesman_code;

        /* ORIGSQL: commit; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x  
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,dealer_code FROM STELADMIN.PATCH_DEALER a join (...) */
                    DISTINCT
                    order_action_id,
                    dealer_code
                FROM
                    EXT.PATCH_DEALER a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field10
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'DEALER'
            ) AS y
            ON (x.field10 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedTVOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD21 = dealer_code;

        /* ORIGSQL: commit; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x  
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,VENDOR_code FROM STELADMIN.PATCH_VENDOR a join (...) */
                    DISTINCT
                    order_action_id,
                    VENDOR_code
                FROM
                    EXT.PATCH_VENDOR a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field10
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'VENDOR'
            ) AS y
            ON (x.field10 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedTVOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD20 = VENDOR_CODE;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    IF :file_type = 'BCC-SCII-SubmittedMobileOrders' 
    THEN 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x  
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,dealer_code,LOWER(salesman_code) as salesman_co(...) */
                    DISTINCT
                    order_action_id,
                    dealer_code,
                    LOWER(salesman_code) AS salesman_code
                FROM
                    EXT.REF_MONTHLY_DATA_PATCH a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field2
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'MOBILE'
            ) AS y
            ON (x.field2 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedMobileOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD19 = dealer_code,FIELD21 = salesman_code;

        /* ORIGSQL: commit; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x  
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id FROM STELADMIN.REF_MONTHLY_DATA_PATCH a join IN(...) */
                    DISTINCT
                    order_action_id
                FROM
                    EXT.REF_MONTHLY_DATA_PATCH a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field2
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'MOBILECIS'
            ) AS y
            ON (x.field2 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedMobileOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD49 = 'CIS';

        /* ORIGSQL: commit; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x  
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,dealer_code FROM STELADMIN.PATCH_DEALER a join (...) */
                    DISTINCT
                    order_action_id,
                    dealer_code
                FROM
                    EXT.PATCH_DEALER a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field2
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'DEALER'
            ) AS y
            ON (x.field2 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedMobileOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD19 = dealer_code;

        /* ORIGSQL: commit; */
        COMMIT;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: MERGE INTO STELADMIN.INBOUND_DATA_STAGING x USING (SELECT distinct order_action_(...) */
        MERGE INTO EXT.INBOUND_DATA_STAGING AS x  
            USING
            (
                SELECT   /* ORIGSQL: (SELECT distinct order_action_id,VENDOR_code FROM STELADMIN.PATCH_VENDOR a join (...) */
                    DISTINCT
                    order_action_id,
                    VENDOR_code
                FROM
                    EXT.PATCH_VENDOR a
                INNER JOIN
                    INBOUND_dATA_STAGING b
                    ON a.order_action_id = b.field2
                WHERE
                    order_action_id IS NOT NULL
                    AND SHEET = 'VENDOR'
            ) AS y
            ON (x.field2 = y.order_action_id
            AND filetype = 'BCC-SCII-SubmittedMobileOrders')
        WHEN MATCHED THEN
            UPDATE SET
                FIELD18 = VENDOR_CODE;

        /* ORIGSQL: commit; */
        COMMIT;
    END IF;

    /* ORIGSQL: SP_LOGGER (SUBSTR('SP_DATA_PATCH' || 'Ended :' || file_type, 255), 'SP_DATA_PATC(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING('SP_DATA_PATCH'|| 'Ended :'|| IFNULL(:file_type,''),255) 
        , 'SP_DATA_PATCH Ended', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR('SP_DATA_PATCH' || 'Ended :' || file_type, 255) */

    /* ORIGSQL: exception when others then */
END