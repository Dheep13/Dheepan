CREATE PROCEDURE EXT.SP_MDLTDIMENSION_MERGER
(
    IN p_FILENAME VARCHAR(75) DEFAULT NULL,   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                              /* ORIGSQL: p_FILENAME IN VARCHAR2 DEFAULT NULL */
    IN p_FILEDATE TIMESTAMP DEFAULT NULL     /* ORIGSQL: p_FILEDATE IN DATE DEFAULT NULL */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_FILENAME VARCHAR(500);  /* ORIGSQL: v_FILENAME VARCHAR2(500); */
    DECLARE v_FILEDATE TIMESTAMP;  /* ORIGSQL: v_FILEDATE DATE; */
    DECLARE v_inbound_cfg_parameter ROW LIKE inbound_cfg_parameter;--%rowtype;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_MDLTDIMENSION_MERGER';  /* ORIGSQL: v_proc_name varchar2(127):='SP_MDLTDIMENSION_MERGER'; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        inbound_cfg_parameter;

    IF :p_FILENAME IS NULL
    OR :p_FILEDATE IS NULL
    THEN 
        SELECT
            file_name,
            file_Date
        INTO
            v_FILENAME,
            v_FILEDATE
        FROM
            Inbound_cfg_Parameter;
    END IF;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_PLMC' not found */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into INBOUND_DATA_PLMC tgt using STEL_LTDIMENSION@STELEXT src on (tgt.mdlt(...) */
    MERGE INTO INBOUND_DATA_PLMC AS tgt
        USING STEL_LTDIMENSION src
        ON (tgt.mdltname = src.tblname
        	AND tgt.filename = :v_FILENAME
            AND tgt.filedate = :v_FILEDATE
            AND recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.dim0name = src.dim0,
            tgt.dim1name = src.dim1,
            tgt.dim2name = src.dim2,
            tgt.dim3name = src.dim3,
            tgt.dim4name = src.dim4,
            tgt.dim5name = src.dim5,
            tgt.dim6name = src.dim6,
            tgt.dim7name = src.dim7,
            tgt.dim8name = src.dim8,
            tgt.dim9name = src.dim9,
            tgt.dim10name = src.dim10,
            tgt.dim11name = src.dim11,
            tgt.dim12name = src.dim12,
            tgt.dim13name = src.dim13,
            tgt.dim14name = src.dim14,
            tgt.dim15name = src.dim15
       -- WHERE
           -- tgt.filename = :v_FILENAME
           -- AND tgt.filedate = :v_FILEDATE
           -- AND recordstatus = 0
           ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Merge Dimension values into INBOUND_DATA_PLMC (...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge Dimension values into INBOUND_DATA_PLMC :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Merge Dimension values into INBOUND_DATA_PLMC Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge Dimension values into INBOUND_DATA_PLMC :' || v_inb(...) */

    /* ORIGSQL: commit; */
    COMMIT;
END