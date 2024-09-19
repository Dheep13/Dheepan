CREATE PROCEDURE EXT.SP_INBOUND_PRE_MMPINSTALL
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_MMPInstall';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_MMPInstall'; */
    DECLARE v_Start TIMESTAMP;  /* ORIGSQL: v_Start date; */
    DECLARE v_End TIMESTAMP;  /* ORIGSQL: v_End date; */
    DECLARE v_cutoff DECIMAL(38,10) = 30;  /* ORIGSQL: v_cutoff number:=30; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    SELECT
        MAX(genericnumber1)
    INTO
        v_cutoff
    FROM
        EXT.stel_classifier
        /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_classifier'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
        classifierid = 'Install_Days'
        AND categorytreename = 'MMP Smart Home';

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Cutoff :' || :v_inbound_cfg_parameter.file_type(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Cutoff :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Cutoff Completed', :v_cutoff, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Cutoff :' || :v_inbound_cfg_parameter.file_type || '-FileN(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        INBOUND_CFG_PARAMETER;

    -- Assuming that the MTPOS SO file has already been loaded in to Commissions.
    -- We're looking for SMART HOME records matching the receipt number from the incoming file
    --Once matched,
    --1. set to payable, if the install month is the same as the comp date month
    --2. set to payable, and update comp date, if the install month is after the comp date month, but within 30 days
    --get the date from the incoming file, and set v_Start and v_End based on the 30 day limit (v_cutoff)
    ----
    /* ORIGSQL: dbms_output.put_line('1'); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('1');

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */

    SELECT
        MIN(TO_DATE(ADD_SECONDS(ADD_MONTHS(LAST_DAY(TO_DATE(TRIM(substring(field15,LOCATE(field15,' ',-1,3),20)),'DD MON YYYY')),-2),(86400*1)))),  /* ORIGSQL: substr(field15,instr(field15,' ',-1,3),20) */
        MAX(LAST_DAY(TO_DATE(TRIM(substring(field15,LOCATE(field15,' ',-1,3),20)),'DD MON YYYY')))  /* ORIGSQL: substr(field15,instr(field15,' ',-1,3),20) */
    INTO
        v_Start,
        v_End
    FROM
        inbound_Data_Staging;

    /* ORIGSQL: dbms_output.put_line('2'); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('2');

    --For better performance
    /* ORIGSQL: execute immediate 'truncate table stel_Temp_transaction'; */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_TRANSACTION' not found */

    /* ORIGSQL: truncate table stel_Temp_transaction ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_Temp_transaction';

    /* ORIGSQL: INSERT INTO STEL_TEMP_TRANSACTION (TENANTID, SALESTRANSACTIONSEQ, SALESORDERSEQ,(...) */
    INSERT INTO STEL_TEMP_TRANSACTION
        (
            TENANTID, SALESTRANSACTIONSEQ, SALESORDERSEQ, LINENUMBER, SUBLINENUMBER, EVENTTYPESEQ,
            PIPELINERUNSEQ, ORIGINTYPEID, COMPENSATIONDATE, BILLTOADDRESSSEQ, SHIPTOADDRESSSEQ, OTHERTOADDRESSSEQ,
            ISRUNNABLE, BUSINESSUNITMAP, ACCOUNTINGDATE, PRODUCTID, PRODUCTNAME, PRODUCTDESCRIPTION,
            NUMBEROFUNITS, UNITVALUE, UNITTYPEFORUNITVALUE, PREADJUSTEDVALUE, UNITTYPEFORPREADJUSTEDVALUE, VALUE,
            UNITTYPEFORVALUE, NATIVECURRENCY, NATIVECURRENCYAMOUNT, DISCOUNTPERCENT, DISCOUNTTYPE, PAYMENTTERMS,
            PONUMBER, CHANNEL, ALTERNATEORDERNUMBER, DATASOURCE, REASONSEQ, COMMENTS,
            GENERICATTRIBUTE1, GENERICATTRIBUTE2, GENERICATTRIBUTE3, GENERICATTRIBUTE4, GENERICATTRIBUTE5, GENERICATTRIBUTE6,
            GENERICATTRIBUTE7, GENERICATTRIBUTE8, GENERICATTRIBUTE9, GENERICATTRIBUTE10, GENERICATTRIBUTE11, GENERICATTRIBUTE12,
            GENERICATTRIBUTE13, GENERICATTRIBUTE14, GENERICATTRIBUTE15, GENERICATTRIBUTE16, GENERICATTRIBUTE17, GENERICATTRIBUTE18,
            GENERICATTRIBUTE19, GENERICATTRIBUTE20, GENERICATTRIBUTE21, GENERICATTRIBUTE22, GENERICATTRIBUTE23, GENERICATTRIBUTE24,
            GENERICATTRIBUTE25, GENERICATTRIBUTE26, GENERICATTRIBUTE27, GENERICATTRIBUTE28, GENERICATTRIBUTE29, GENERICATTRIBUTE30,
            GENERICATTRIBUTE31, GENERICATTRIBUTE32, GENERICNUMBER1, UNITTYPEFORGENERICNUMBER1, GENERICNUMBER2, UNITTYPEFORGENERICNUMBER2,
            GENERICNUMBER3, UNITTYPEFORGENERICNUMBER3, GENERICNUMBER4, UNITTYPEFORGENERICNUMBER4, GENERICNUMBER5, UNITTYPEFORGENERICNUMBER5,
            GENERICNUMBER6, UNITTYPEFORGENERICNUMBER6, GENERICDATE1, GENERICDATE2, GENERICDATE3, GENERICDATE4,
            GENERICDATE5, GENERICDATE6, GENERICBOOLEAN1, GENERICBOOLEAN2, GENERICBOOLEAN3, GENERICBOOLEAN4,
            GENERICBOOLEAN5, GENERICBOOLEAN6, PROCESSINGUNITSEQ, MODIFICATIONDATE, UNITTYPEFORLINENUMBER, UNITTYPEFORSUBLINENUMBER,
            UNITTYPEFORNUMBEROFUNITS, UNITTYPEFORDISCOUNTPERCENT, UNITTYPEFORNATIVECURRENCYAMT, MODELSEQ, SETNUMBER, POSITIONNAME,
            PAYEEID, TITLENAME, CUSTID, CONTACT, COMPANY, AREACODE,
            PHONE, FAX, ADDRESS1, ADDRESS2, ADDRESS3, CITY,
            STATE, COUNTRY, POSTALCODE, INDUSTRY, GEOGRAPHY, ORDERID,
            EVENTTYPEID, RECONFLAG, ORDERTYPE, ORDERLINETYPE, FILETYPE, FILENAME,
            FILEDATE
        )
        SELECT   /* ORIGSQL: select TENANTID, SALESTRANSACTIONSEQ, SALESORDERSEQ, LINENUMBER, SUBLINENUMBER, (...) */
            TENANTID,
            SALESTRANSACTIONSEQ,
            SALESORDERSEQ,
            LINENUMBER,
            SUBLINENUMBER,
            EVENTTYPESEQ,
            PIPELINERUNSEQ,
            ORIGINTYPEID,
            COMPENSATIONDATE,
            BILLTOADDRESSSEQ,
            SHIPTOADDRESSSEQ,
            OTHERTOADDRESSSEQ,
            ISRUNNABLE,
            BUSINESSUNITMAP,
            ACCOUNTINGDATE,
            PRODUCTID,
            PRODUCTNAME,
            PRODUCTDESCRIPTION,
            NUMBEROFUNITS,
            UNITVALUE,
            UNITTYPEFORUNITVALUE,
            PREADJUSTEDVALUE,
            UNITTYPEFORPREADJUSTEDVALUE,
            VALUE,
            UNITTYPEFORVALUE,
            NATIVECURRENCY,
            NATIVECURRENCYAMOUNT,
            DISCOUNTPERCENT,
            DISCOUNTTYPE,
            PAYMENTTERMS,
            PONUMBER,
            CHANNEL,
            ALTERNATEORDERNUMBER,
            DATASOURCE,
            REASONSEQ,
            COMMENTS,
            GENERICATTRIBUTE1,
            GENERICATTRIBUTE2,
            GENERICATTRIBUTE3,
            GENERICATTRIBUTE4,
            GENERICATTRIBUTE5,
            GENERICATTRIBUTE6,
            GENERICATTRIBUTE7,
            GENERICATTRIBUTE8,
            GENERICATTRIBUTE9,
            GENERICATTRIBUTE10,
            GENERICATTRIBUTE11,
            GENERICATTRIBUTE12,
            GENERICATTRIBUTE13,
            GENERICATTRIBUTE14,
            GENERICATTRIBUTE15,
            GENERICATTRIBUTE16,
            GENERICATTRIBUTE17,
            GENERICATTRIBUTE18,
            GENERICATTRIBUTE19,
            GENERICATTRIBUTE20,
            GENERICATTRIBUTE21,
            GENERICATTRIBUTE22,
            GENERICATTRIBUTE23,
            GENERICATTRIBUTE24,
            GENERICATTRIBUTE25,
            GENERICATTRIBUTE26,
            GENERICATTRIBUTE27,
            GENERICATTRIBUTE28,
            GENERICATTRIBUTE29,
            GENERICATTRIBUTE30,
            GENERICATTRIBUTE31,
            GENERICATTRIBUTE32,
            GENERICNUMBER1,
            UNITTYPEFORGENERICNUMBER1,
            GENERICNUMBER2,
            UNITTYPEFORGENERICNUMBER2,
            GENERICNUMBER3,
            UNITTYPEFORGENERICNUMBER3,
            GENERICNUMBER4,
            UNITTYPEFORGENERICNUMBER4,
            GENERICNUMBER5,
            UNITTYPEFORGENERICNUMBER5,
            GENERICNUMBER6,
            UNITTYPEFORGENERICNUMBER6,
            GENERICDATE1,
            GENERICDATE2,
            GENERICDATE3,
            GENERICDATE4,
            GENERICDATE5,
            GENERICDATE6,
            GENERICBOOLEAN1,
            GENERICBOOLEAN2,
            GENERICBOOLEAN3,
            GENERICBOOLEAN4,
            GENERICBOOLEAN5,
            GENERICBOOLEAN6,
            PROCESSINGUNITSEQ,
            MODIFICATIONDATE,
            UNITTYPEFORLINENUMBER,
            UNITTYPEFORSUBLINENUMBER,
            UNITTYPEFORNUMBEROFUNITS,
            UNITTYPEFORDISCOUNTPERCENT,
            UNITTYPEFORNATIVECURRENCYAMT,
            MODELSEQ,
            SETNUMBER,
            POSITIONNAME,
            PAYEEID,
            TITLENAME,
            CUSTID,
            CONTACT,
            COMPANY,
            AREACODE,
            PHONE,
            FAX,
            ADDRESS1,
            ADDRESS2,
            ADDRESS3,
            CITY,
            STATE,
            COUNTRY,
            POSTALCODE,
            INDUSTRY,
            GEOGRAPHY,
            ORDERID,
            EVENTTYPEID,
            RECONFLAG,
            ORDERTYPE,
            ORDERLINETYPE,
            :v_inbound_cfg_parameter.file_type,
            :v_inbound_cfg_parameter.file_name,
            :v_inbound_cfg_parameter.file_Date
        FROM
            EXT.vw_Salestransaction
            /* RESOLVE: Oracle Database link: Remote table/view 'EXT.vw_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.vw_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            eventtypeid = 'MTPOS Sales Order'
            AND compensationdate BETWEEN :v_Start AND :v_End
            AND genericboolean1 = 1 --KIV records
            AND productname IN
            (
                SELECT   /* ORIGSQL: (select dim0 from stel_lookup@stelext where name='LT_MMP_Smart Home Acc') */
                    dim0
                FROM
                    EXT.stel_lookup
                    /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_lookup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_lookup'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    name = 'LT_MMP_Smart Home Acc'
            ) --filter for smart home
            AND setnumber = 1;-- to avoid duplicates

    /* ORIGSQL: dbms_output.put_line('3'); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('3');

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Inserted records :' || v_inbound_cfg_parameter(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Inserted records :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Insert into temp table Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Inserted records :' || :v_inbound_cfg_parameter.file_type (...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_Temp_transaction tgt using (SELECT distinct field14 AS receipt, (...) */
    MERGE INTO stel_Temp_transaction AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select distinct field14 receipt, TO_DATE(TRIM(substr(field15,instr(field15,' ',(...) */
                DISTINCT
                field14 AS receipt,
                TO_DATE(TRIM(substring(field15,LOCATE(field15,' ',-1,3),100)),'DD MON YYYY') AS instdate,  /* ORIGSQL: substr(field15,instr(field15,' ',-1,3),100) */
                field10
            FROM
                inbound_Data_Staging
        ) AS src
        ON (src.receipt = tgt.ponumber
            AND (SECONDS_BETWEEN(tgt.accountingdate,src.instdate)/86400) <= :v_cutoff  /* ORIGSQL: src.instdate - tgt.accountingdate */
            AND src.field10 = tgt.productname --stock code
        ) --accdate has the mtpos txn date, same as comp date

    WHEN MATCHED THEN
        UPDATE
            SET tgt.genericboolean1 = 0
            , tgt.comments = 'Match Found in Vendor File. Install Date:'||IFNULL(TO_VARCHAR(src.instdate,'DD-MON-YYYY'),'')  /* ORIGSQL: to_Char(src.instdate,'DD-MON-YYYY') */
            , tgt.compensationdate =
            CASE
                WHEN TO_VARCHAR(src.instdate,'YYYYMM') = TO_VARCHAR(tgt.compensationdate,'YYYYMM')  /* ORIGSQL: to_char(tgt.compensationdate,'YYYYMM') */
                                                                                                    /* ORIGSQL: to_char(src.instdate,'YYYYMM') */
                THEN tgt.compensationdate
                ELSE src.instdate
            END
            , tgt.genericboolean6 = 1;--this is the indicator to use for filtering
    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Merged records :' || :v_inbound_cfg_parameter.f(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merged records :'|| IFNULL(:v_inbound_cfg_parameter.file_type,'') || '-FileName:'|| IFNULL(:v_inbound_cfg_parameter.file_name,'') || '-Date:'|| IFNULL(:v_inbound_cfg_parameter.file_date,''),1,255) 
        , 'Merge Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merged records :' || :v_inbound_cfg_parameter.file_type ||(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: sp_inbound_txn_map(:v_inbound_cfg_parameter.file_Type, :v_inbound_cfg_parameter.fi(...) */
    CALL EXT.SP_INBOUND_TXN_MAP(:v_inbound_cfg_parameter.file_Type, :v_inbound_cfg_parameter.file_name, :v_inbound_cfg_parameter.file_Date, 2);

    /* ROCEDURE           SP_INBOUND_TXN_MAP (
           p_filetype IN VARCHAR2 DEFAULT NULL,
           p_filename IN VARCHAR2 DEFAULT NULL,
           p_filedate IN DATE DEFAULT NULL,
       p_stage in number default null)*/
END