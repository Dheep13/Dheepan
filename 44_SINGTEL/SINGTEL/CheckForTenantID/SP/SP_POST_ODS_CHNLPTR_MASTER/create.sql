CREATE PROCEDURE ext.SP_POST_ODS_CHNLPTR_MASTER
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA ext
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_proc_name VARCHAR(127) = 'SP_POST_ODS_CHNLPTR_MASTER';  /* ORIGSQL: v_proc_name varchar2(127):='SP_POST_ODS_CHNLPTR_MASTER'; */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_rec ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'ext.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    --kyap: cs_participant GA1, GA2, GA3 are maintain manually in commission system; it is used by mcash, VP, etc.
    --prevent inbound_data_ogpt from overwriting the GA1, GA2, GA3 values by merging it from cs_participant

    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_rec
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* RESOLVE: Identifier not found: Table/view 'ext.INBOUND_DATA_OGPT' not found */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_ogpt tgt using (SELECT userid, genericattribute1, generi(...) */
    MERGE INTO ext.inbound_data_ogpt AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select userid, genericattribute1, genericattribute2, genericattribute3 from cs_(...) */
                userid,
                genericattribute1,
                genericattribute2,
                genericattribute3
            FROM
                cs_participant
                /* RESOLVE: Oracle Database link: Remote table/view 'ext.cs_participant@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_participant_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                effectiveenddate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND effectivestartdate <= CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND removedate > CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND (genericattribute1 IS NOT NULL
                    OR genericattribute2 IS NOT NULL
                OR genericattribute3 IS NOT NULL)
        ) AS src
        ON (tgt.payeeid = src.userid
        	AND
            tgt.recordstatus = 0
            AND (filedate, filename)  
            IN
            (
                SELECT   /* ORIGSQL: (select file_Date, file_name from ext.inbound_cfg_parameter) */
                    file_Date,
                    file_name
                FROM
                    ext.inbound_cfg_parameter
            )
        )
	    WHEN MATCHED THEN
	        UPDATE SET
	            tgt.genericattribute1 = src.genericattribute1,
	            tgt.genericattribute2 = src.genericattribute2,
	            tgt.genericattribute3 = src.genericattribute3;
       

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update ext.inbound_data_ogpt GA1, GA2, GA3: ' || v(...) */
    CALL ext.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update ext.inbound_data_ogpt GA1, GA2, GA3: '|| IFNULL(:v_rec.file_type,'') || '-FileName:'|| IFNULL(:v_rec.file_name,'') || '-Date:'|| IFNULL(:v_rec.file_date,''),1,255) 
        , 'Update inbound_data_ogpo Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update ext.inbound_data_ogpt GA1, GA2, GA3: ' || v_rec.file_t(...) */
END