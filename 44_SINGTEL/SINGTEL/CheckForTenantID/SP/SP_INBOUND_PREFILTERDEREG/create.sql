CREATE PROCEDURE EXT.SP_INBOUND_PREFILTERDEREG
(
    IN p_FILENAME VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: p_FILENAME IN VARCHAR2 */
    IN p_FILEDATE TIMESTAMP     /* ORIGSQL: p_FILEDATE IN DATE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Catname VARCHAR(200) = 'Dereg Eligibility';  /* ORIGSQL: v_Catname varchar2(200):='Dereg Eligibility'; */

    /* ORIGSQL: update inbound_data_staging i SET i.error_flag = '1', i.error_message ='Vendor-D(...) */
    UPDATE inbound_data_staging i
        SET
        /* ORIGSQL: i.error_flag = */
        error_flag = '1',
        /* ORIGSQL: i.error_message = */
        error_message = 'Vendor-Dealer Not Eligible' 
    WHERE
        i.filename = :p_FILENAME
        AND i.filedate = :p_FILEDATE;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_staging tgt using (SELECT seq, filedate, filename FROM i(...) */
    MERGE INTO inbound_data_staging AS tgt
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select seq, filedate, filename from inbound_data_staging i join stel_classifier(...) */
                seq,
                filedate,
                filename
            FROM
                inbound_data_staging i
            INNER JOIN
                EXT.stel_classifier cl
                ON categorytreename = :v_Catname
                AND i.field5 = cl.genericattribute1
                AND i.field6 = cl.genericattribute2
                AND cl.genericattribute3 = 'Y'
                AND TO_DATE(i.field7,'YYYYMMDD') BETWEEN cl.effectivestartdate AND cl.effectiveenddate
                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_classifier'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                i.filename = :p_FILENAME
                AND i.filedate = :p_FILEDATE
        ) AS src
        ON (tgt.seq = src.seq)
    WHEN MATCHED THEN
        UPDATE SET tgt.error_flag = NULL, error_message = NULL;

    /* ORIGSQL: commit; */
    COMMIT;
END