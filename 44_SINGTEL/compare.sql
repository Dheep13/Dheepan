CREATE PROCEDURE EXT.SP_INBOUND_POST_BCCTXN
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cutoffday DECIMAL(38,10);  /* ORIGSQL: v_cutoffday NUMBER; */
    DECLARE v_oppr ROW LIKE inbound_cfg_BCC_Txn;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.inbound_cfg_BCC_Txn' not found (for %ROWTYPE declaration) */
    DECLARE v_maxseq DECIMAL(38,10);  /* ORIGSQL: v_maxseq number; */
    DECLARE v_clawbackperiod DECIMAL(38,10);  /* ORIGSQL: v_clawbackperiod number; */
    DECLARE v_filedate TIMESTAMP;  /* ORIGSQL: v_filedate date; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_BCCTXN';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_BCCTXN'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */
    DECLARE v_event VARCHAR(255);  /* ORIGSQL: v_event varchar2(255); */
    DECLARE v_minCompDate TIMESTAMP;  /* ORIGSQL: v_minCompDate date; */
    DECLARE v_maxCompDate TIMESTAMP;  /* ORIGSQL: v_maxCompDate date; */
    DECLARE v_sqlerrm VARCHAR(4000);  /* ORIGSQL: v_sqlerrm varchar2(4000); */
    DECLARE v_prmtr ROW LIKE inbound_cfg_parameter;
    DECLARE v_file_date TIMESTAMP;
    DECLARE v_file_name VARCHAR(500);
    DECLARE v_FILE_TYPE NVARCHAR(500);
    DECLARE v_src_tbl VARCHAR(200);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: WHEN OTHERS THEN */
        BEGIN
            v_sqlerrm = SUBSTRING(::SQL_ERROR_MESSAGE,1,4000);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 4000) */

            /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'ERROR in :' || '-FileName:' || v_prmtr.file_na(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || ' ERROR in :'|| '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
                , 'FIELDMAP Execution Error', NULL, NULL, SUBSTRING(:v_sqlerrm,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || 'ERROR in :' || '-FileName:' || :v_file_name || '-Dat(...) */
                );  /* ORIGSQL: SUBSTR(v_sqlerrm, 1, 4000) */

            /* ORIGSQL: raise; */
            RESIGNAL;
            --to raise the error, so that informatica job fails.

            /* ORIGSQL: COMMIT; */
            COMMIT;
        END;


    -- v_prmtr       inbound_cfg_parameter%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: EXECUTE immediate 'alter session set nls_date_format = ''DD-MON-YYYY'' ' ; */
    /* ORIGSQL: alter session set nls_date_format = 'DD-MON-YYYY' ; */
    -- CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    -- CALL SQLSCRIPT_PRINT:PRINT_LINE('*****************');

    /* ORIGSQL: dbms_output.put_line ('Start Post BCC TXN'); */
    -- CALL SQLSCRIPT_PRINT:PRINT_LINE('Start Post BCC TXN');

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('*****************');

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    
    SELECT CAST(SESSION_CONTEXT('v_file_name') AS VARCHAR(200)) INTO v_file_name from sys.dummy;
    SELECT CAST(SESSION_CONTEXT('v_file_type') AS VARCHAR(200)) INTO v_file_type from sys.dummy;
    SELECT CAST(SESSION_CONTEXT('v_file_date') AS TIMESTAMP) INTO v_file_date from sys.dummy;
    SELECT CAST(SESSION_CONTEXT('v_src_tbl') AS VARCHAR(200)) INTO v_src_tbl from sys.dummy;
    
  /*
    SELECT *
    INTO
        v_prmtr
    FROM
        inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';
*/
  /*  SELECT
        file_Date
    INTO
        v_filedate
    FROM
        inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';*/

    /* For handling Historical Data Load Process - Changing the file date from system date to Compdate on the file */
    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_TXN' not found */

    SELECT
        MIN(compensationdate),
        MAX(compensationdate)
    INTO
        v_minCompDate,
        v_maxCompDate
    FROM
        Inbound_data_txn
    WHERE
        filedate = :v_file_date
        AND filename = :v_file_name;

    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_GENERICPARAMETER' not found */

    SELECT
        VALUE
    INTO
        v_cutoffday
    FROM
        inbound_cfg_genericparameter
    WHERE
        KEY = 'BCC Cutoff Date';

    SELECT
        value
    INTO
        v_clawbackperiod
    FROM
        inbound_cfg_genericparameter
    WHERE
        KEY = 'Clawback Cutoff BCC';

    /* ORIGSQL: dbms_output.put_line (:v_file_type); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_file_type);

    /* ORIGSQL: dbms_output.put_line (:v_file_name); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_file_name);

    /* ORIGSQL: dbms_output.put_line (:v_file_date); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE(:v_file_date);

    IF :v_file_type LIKE 'BCC%SCII%%' 
    THEN
        /* ORIGSQL: execute immediate 'Truncate table Inbound_temp_txn drop storage' ; */
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_TEMP_TXN' not found */

        /* ORIGSQL: Truncate table Inbound_temp_txn drop storage ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE Inbound_temp_txn';

        /* ORIGSQL: execute immediate 'Truncate table Inbound_temp_Assignment drop storage' ; */
        /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_TEMP_ASSIGNMENT' not found */

        /* ORIGSQL: Truncate table Inbound_temp_Assignment drop storage ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE Inbound_temp_Assignment';

        /* ORIGSQL: dbms_output.put_line ('1 inserting into temp'); */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('1 inserting into temp');

        --Below block added by Arun on 30th Jan 2020 CR - Email from Babu on 30th Jan 2020
        --Sub:  IMPORTANT - Work around Adjustments and CRs
        -------------------------------------------------------------------
        /*BLOCK to cater Bundle Order ComponentID vs BroadBand Submitted/Closed Orders*/

        /* ORIGSQL: dbms_stats.gather_table_stats(ownname =>'STELADMIN', tabname => 'STEL_DATA_BCCBU(...) */
        -- EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'STELADMIN'|| '.'|| 'STEL_DATA_BCCBUNDLE';--Deepan : Statistics not required

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into inbound_data_txn tgt using (SELECT distinct st.orderid, st.productid,(...) */
        MERGE INTO inbound_data_txn AS tgt 
            /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_BCCBUNDLE' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select distinct st.orderid,st.productid,st.alternateordernumber,st.genericattri(...) */
                    DISTINCT
                    st.orderid,
                    st.productid,
                    st.alternateordernumber,
                    st.genericattribute6,
                    st.compensationdate,
                    bundle.price_plan_component_id AS bProductid,
                    st.billtocontact,
                    BUNDLE.EXISTING_COMPONENT_ID
                FROM
                    inbound_data_txn st
                INNER JOIN
                    EXT.STEL_DATA_BCCBUNDLE bundle
                    ON bundle.ORDER_ID = st.alternateordernumber
                    --and bundle.order_action_id=st.genericattribute6 -- Commented this to remove OAID check on 28th Feb Sankar
                    AND st.genericattribute9 = 'M'
                    AND st.GENERICATTRIBUTE22 = 'AC'
                    AND bundle.BIZ_TYPE = 'FBB'
                WHERE
                    st.compensationdate BETWEEN v_minCompDate AND v_maxCompDate
                    AND IFNULL(st.genericboolean1,0) = 0  /* ORIGSQL: nvl(st.genericboolean1,0) */
                    AND st.eventtypeid IN
                    ('BroadBand Submitted','BroadBand Closed')
                    AND st.filedate = :v_file_date
                    AND st.filename = :v_file_name
                    AND st.recordstatus = 0
            ) AS src
            ON (Src.orderid = tgt.orderid
                AND src.compensationdate = tgt.compensationdate
            AND src.genericattribute6 = tgt.genericattribute6
            AND tgt.compensationdate BETWEEN v_minCompDate AND v_maxCompDate
            AND tgt.filedate = :v_file_date
            AND tgt.filename = :v_file_name
            AND tgt.recordstatus = 0
            )
        WHEN MATCHED THEN
            UPDATE SET tgt.genericattribute20 = 'Product ID updated based on the Bundle Order',
                tgt.genericattribute21 = tgt.productid,tgt.productid = src.bProductid,
                --tgt.genericattribute25=src.EXISTING_COMPONENT_ID -- added this condition to update old comp id as per joanna request by sankar

                tgt.genericattribute25 =
                CASE
                    WHEN tgt.genericattribute5 = 'New'
                    THEN tgt.genericattribute25
                    WHEN tgt.genericattribute26 = 1
                    AND (src.EXISTING_COMPONENT_ID IS NULL
                    OR src.EXISTING_COMPONENT_ID = '')
                    AND tgt.genericattribute25 = tgt.productid
                    THEN src.bProductid
                    ELSE src.EXISTING_COMPONENT_ID
                END
                -- added this condition to update old comp id as per joanna request by sankar
          
                ;

        /*BLOCK END*/
        -------------------------------------------------------------------   

        /* ORIGSQL: update Inbound_data_txn SET genericattribute31='N' where filedate=v_prmtr.file_d(...) */
        UPDATE Inbound_data_txn
            SET
            /* ORIGSQL: genericattribute31 = */
            genericattribute31 = 'N' 
        FROM
            Inbound_data_txn
        WHERE
            filedate = :v_file_date
            AND filename = :v_file_name
            AND genericattribute22 = 'C'
            AND (eventtypeid LIKE '%TV%'
            AND genericattribute10 = 'CE');

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update P to N for TV CE :' || v_prmtr.file_typ(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update P to N for TV CE  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'Update P to N for TV CE Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update P to N for TV CE  :' || :v_file_type || '-Fil(...) */

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: insert into Inbound_temp_txn select * from Inbound_data_txn where filedate=v_prm(...) */
   
   
        INSERT INTO Inbound_temp_txn
            SELECT   /* ORIGSQL: select * from Inbound_data_txn where filedate=:v_file_date and filename=v_p(...) */
                *
            FROM
                ext.Inbound_data_txn
                
            WHERE
                filedate = :v_file_date
                AND filename = :v_file_name
                AND genericattribute22 = 'C'
                AND ((eventtypeid LIKE '%TV%'
                    AND genericattribute10 = 'CE')
                OR eventtypeid NOT LIKE '%TV%')
                AND recordstatus = 0;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Insert into Inbound_temp_txn with GA22=C and R(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert into Inbound_temp_txn with GA22=C and RecStattus=0  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'Insert into Inbound_temp_txn with GA22=C and RecStattus=0 Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert into Inbound_temp_txn with GA22=C and RecStattus=0(...) */

        /* ORIGSQL: Commit; */
        COMMIT;

        SELECT
            MAX(eventtypeid)
        INTO
            v_event
        FROM
            inbound_data_Txn
        WHERE
            recordstatus = 0
            AND (eventtypeid LIKE '%Closed%'
            OR eventtypeid LIKE '%Submitt%')
            AND filename = :v_file_name
            AND filedate = :v_file_date;

        /* ORIGSQL: dbms_output.put_line ('2 inserting into stael_Data_Subscriptions'); */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('2 inserting into stael_Data_Subscriptions');

        --delete from STEL_DATA_SUBSCRIPTIONS where callidussubsc IS NULL or serviceno IS NULL; 

        /* ORIGSQL: INSERT INTO STEL_DATA_SUBSCRIPTIONS (CUSTOMER, SERVICENO, COMPENSATIONDATE, EVEN(...) */
        INSERT INTO STEL_DATA_SUBSCRIPTIONS
            (
                CUSTOMER,
                SERVICENO,
                COMPENSATIONDATE,
                EVENTTYPEID,
                Orig_CUSTOMER,
                orig_SERVICENO,
                COMPONENTID
            )
            SELECT   /* ORIGSQL: select distinct a.billtocustid customer, a.billtocontact serviceno, a.compensati(...) */
                DISTINCT
                a.billtocustid AS customer,
                a.billtocontact AS serviceno,
                a.compensationdate,
                a.eventtypeid,
                a.billtocustid AS orig_customer,
                IFNULL(a.billtofax,a.billtocontact) AS orig_serviceno,  /* ORIGSQL: nvl(a.billtofax,a.billtocontact) */
                a.productid
            FROM
                inbound_data_Txn a 
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select customer, serviceno, eventtypeid, componentid from stel_Data_Subscriptio(...) */
                        customer,
                        serviceno,
                        eventtypeid,
                        componentid
                    FROM
                        stel_Data_Subscriptions
                ) AS x
                ON a.billtocustid = x.customer
                AND a.billtocontact = x.serviceno
                AND a.eventtypeid = x.eventtypeid
                AND a.productid = x.componentid
            WHERE
                recordstatus = '0'
                AND (a.eventtypeid LIKE '%Closed%'
                OR a.eventtypeid LIKE '%Submitt%')
                AND a.filename = :v_file_name
                AND a.filedate = :v_file_date
                AND x.customer IS NULL;

        /*
        Arjun 20190510
        select distinct
        billtocustid customer, billtocontact serviceno, compensationdate,
        eventtypeid, billtocustid orig_customer, nvl(billtofax,billtocontact) orig_serviceno, productid
        from inbound_data_Txn
        where recordstatus=0
        and ( eventtypeid like '%Closed%' or eventtypeid like '%Submitt%')
        and filename=:v_file_name and filedate=:v_file_date
        and (billtocustid , billtocontact ,
        eventtypeid,   productid)
        not in (
            select customer, serviceno, eventtypeid,   componentid
        from stel_Data_Subscriptions) ;
        */

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || '1. Insert into STEL_DATA_SUBSCRIPTIONS :' || v(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1. Insert into STEL_DATA_SUBSCRIPTIONS  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , '1. Insert into STEL_DATA_SUBSCRIPTIONS  Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1. Insert into STEL_DATA_SUBSCRIPTIONS  :' || v_prmtr.fil(...) */

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: dbms_output.put_line ('2 inserted into stael_Data_Subscriptions '||SQL%ROWCOUNT)(...) */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('2 inserted into stael_Data_Subscriptions '||::ROWCOUNT);  

        /* ORIGSQL: dbms_stats.gather_table_stats(ownname =>'STELADMIN', tabname => 'STEL_DATA_SUBSC(...) */
        -- EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'STELADMIN'|| '.'|| 'STEL_DATA_SUBSCRIPTIONS';--Deepan : Statistics not required

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

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
                :v_file_type,
                :v_file_name,
                :v_filedate
            FROM
                EXT.vw_Salestransaction
                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.vw_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.vw_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                (eventtypeid = :v_event)
                AND IFNULL(genericboolean1,0) = 0  /* ORIGSQL: nvl(genericboolean1,0) */
                AND setnumber = 1
                --and compensationdate between v_minCompDate-v_clawbackperiod and v_maxCompDate --30 days
                AND compensationdate BETWEEN TO_DATE(ADD_SECONDS(:v_maxCompDate,(86400*(-1*60)))) AND :v_maxCompDate;--30 days change done by sankar
        /* ORIGSQL: v_maxCompDate-60 */

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Insert stel_temp_transaction :' || v_prmtr.fil(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert stel_temp_transaction  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'insert in stel_temp_transaction Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert stel_temp_transaction  :' || :v_file_type || (...) */

        /* ORIGSQL: dbms_stats.gather_table_stats(ownname =>'STELADMIN', tabname => 'INBOUND_DATA_TX(...) */
        -- EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'STELADMIN'|| '.'|| 'INBOUND_DATA_TXN'; --Deepan : Statistics not requires

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'gather stats complete:' || :v_file_type |(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'gather stats complete:'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'gather stats Completed-INBOUND_DATA_TXN', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'gather stats complete:' || :v_file_type || '-FileNam(...) */

        /* ORIGSQL: dbms_stats.gather_table_stats(ownname =>'STELADMIN', tabname => 'stel_Temp_trans(...) */
        -- EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'STELADMIN'|| '.'|| 'stel_Temp_transaction';--Deepan : Statistics not requires

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'gather stats complete:' || :v_file_type |(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'gather stats complete:'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'gather stats Completed-stel_Temp_transaction', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'gather stats complete:' || :v_file_type || '-FileNam(...) */

        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_SUBSCRIPTIONS' not found */

        SELECT
            IFNULL(MAX(CallidusSubsc),0)  /* ORIGSQL: nvl(max(CallidusSubsc),0) */
        INTO
            v_maxseq
        FROM
            stel_Data_Subscriptions;

        --Sibc no should be unique, not [per customer/svc no 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into stel_Data_Subscriptions tgt using (SELECT ROW_NUMBER() OVER (ORDER BY(...) */
        MERGE INTO stel_Data_Subscriptions AS tgt
            USING
            (
                SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (ORDER BY serviceno) + v_maxSeq rn, a.customer, a.serv(...) */
                    ROW_NUMBER() OVER (/*partition by  customer, serviceno*/ ORDER BY serviceno) + :v_maxseq AS rn,
                    a.customer,
                    a.serviceno   
                FROM
                    (
                        SELECT   /* ORIGSQL: (select distinct customer, serviceno from stel_Data_Subscriptions) */
                            DISTINCT
                            customer,
                            serviceno
                        FROM
                            stel_Data_Subscriptions
                    ) AS a
                ) AS src
                ON (tgt.customer = src.customer
                AND tgt.serviceno = src.serviceno
                AND CallidusSubsc IS NULL
                )
        WHEN MATCHED THEN
            UPDATE SET CallidusSubsc = src.rn
            ;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'SIBC No Update in STEL_DATA_SUBSCRIPTIONS :' |(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'SIBC No Update in STEL_DATA_SUBSCRIPTIONS  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'SIBC No Update in STEL_DATA_SUBSCRIPTIONS Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'SIBC No Update in STEL_DATA_SUBSCRIPTIONS  :' || v_prmtr.(...) */

        /* ORIGSQL: commit; */
        COMMIT;

        -- when the customer changes 

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into stel_Data_Subscriptions tgt using (SELECT serviceno, eventtypeid, MIN(...) */
        MERGE INTO stel_Data_Subscriptions AS tgt 
            USING
            (
                SELECT   /* ORIGSQL: (select serviceno, eventtypeid, MIN(compensationdate) compensationdate, MAX(orig(...) */
                    serviceno,
                    eventtypeid,
                    MIN(compensationdate) AS compensationdate,
                    MAX(orig_customer) AS orig_customer,
                    componentid,
                    MAX(CallidusSubsc) AS CallidusSubsc,
                    MAX(customer) AS customer
                FROM
                    stel_Data_Subscriptions
                WHERE
                    eventtypeid = :v_event
                GROUP BY
                    serviceno, eventtypeid, /*orig_customer, */ componentid--,CallidusSubsc--, customer/orig_Customer//calliducsubsc removed customer from gorup to get aorund data issue
            ) AS src
            ON (src.serviceno = tgt.orig_serviceno
                AND src.eventtypeid = tgt.eventtypeid
                AND src.compensationdate < tgt.compensationdate
                AND src.orig_customer <> tgt.orig_customer
            AND src.componentid = tgt.componentid
            AND tgt.CallidusSubsc IS NOT NULL
            )
        WHEN MATCHED THEN
            UPDATE SET tgt.customer = src.customer, flag = -1, tgt.CallidusSubsc = src.CallidusSubsc
            ;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Customer change update in STEL_DATA_SUBSCRIPTI(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Customer change update in STEL_DATA_SUBSCRIPTIONS  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'Customer change update in STEL_DATA_SUBSCRIPTIONS Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Customer change update in STEL_DATA_SUBSCRIPTIONS  :' || (...) */

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: dbms_output.put_line ('21 '||SQL%ROWCOUNT); */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('21 '||::ROWCOUNT);  

        -- when the service no changes. billtofax contains the previous number
        BEGIN 
            DECLARE EXIT HANDLER FOR SQLEXCEPTION
                /* ORIGSQL: WHEN OTHERS THEN */
                BEGIN
                    v_sqlerrm = SUBSTRING(::SQL_ERROR_MESSAGE,1,4000);  /* ORIGSQL: SUBSTR(SQLERRM, 1, 4000) */

                    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'ERROR in STEL_DATA_SUBSCXRIPT merge. probablt (...) */
                    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'ERROR in STEL_DATA_SUBSCXRIPT merge. probablt duplicates :'|| '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
                        , 'FIELDMAP Execution Error', NULL, NULL, SUBSTRING(:v_sqlerrm,1,4000)   /* ORIGSQL: SUBSTR(v_proc_name || 'ERROR in STEL_DATA_SUBSCXRIPT merge. probablt duplicates (...) */
                        );  /* ORIGSQL: SUBSTR(v_sqlerrm, 1, 4000) */

                    -- raise; --to raise the error, so that informatica job fails.

                    /* ORIGSQL: COMMIT; */
                    COMMIT;

                    /*
                    select count(*), a.orig_Customer,a.eventtypeid, a.compensationdate, a.componentid
                    from (
                        select tgt.*, src.* from  stel_Data_Subscriptions tgt
                        join (select distinct serviceno, eventtypeid, compensationdate, orig_serviceno, componentid, CallidusSubsc, customer
                            from stel_Data_Subscriptions
                            where orig_Serviceno<>serviceno
                            and orig_serviceno
                        ) src
                        on (src.customer = tgt.orig_customer and src.eventtypeid=tgt.eventtypeid
                            and src.compensationdate<tgt.compensationdate and src.orig_serviceno<>tgt.orig_serviceno
                        and src.componentid=tgt.componentid)
                        
                        where tgt.CallidusSubsc IS NOT NULL
                        
                    ) a
                    group by a.orig_Customer,a.eventtypeid, a.compensationdate, a.componentid
                    having count(*)>1*/
                END;

                 
            /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
            /* ORIGSQL: merge into stel_Data_Subscriptions tgt using (SELECT distinct serviceno, eventty(...) */
            MERGE INTO stel_Data_Subscriptions AS tgt 
                USING
                (
                    SELECT   /* ORIGSQL: (select distinct serviceno, eventtypeid, compensationdate, orig_serviceno, compo(...) */
                        DISTINCT
                        serviceno,
                        eventtypeid,
                        compensationdate,
                        orig_serviceno,
                        componentid,
                        CallidusSubsc,
                        customer
                    FROM
                        stel_Data_Subscriptions
                    WHERE
                        orig_Serviceno <> serviceno
                ) AS src
                ON (src.customer = tgt.orig_customer
                    AND src.eventtypeid = tgt.eventtypeid
                    AND src.compensationdate < tgt.compensationdate
                    AND src.orig_serviceno <> tgt.orig_serviceno
                AND src.componentid = tgt.componentid
                AND tgt.CallidusSubsc IS NOT NULL
                )
            WHEN MATCHED THEN
                UPDATE SET tgt.serviceno = src.serviceno, flag = -1, tgt.CallidusSubsc = src.CallidusSubsc
                ;

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'SERVICE No change update in STEL_DATA_SUBSCRIP(...) */
            CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'SERVICE No change update in STEL_DATA_SUBSCRIPTIONS  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
                , 'SERVICE No change update in STEL_DATA_SUBSCRIPTIONS Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'SERVICE No change update in STEL_DATA_SUBSCRIPTIONS  :' |(...) */

            /* ORIGSQL: exception WHEN OTHERS THEN */
        END;

        /* ORIGSQL: dbms_output.put_line ('22 '||SQL%ROWCOUNT); */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('22 '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;  

        /* ORIGSQL: update stel_Data_Subscriptions SET orig_Serviceno=serviceno, orig_Customer=custo(...) */
        UPDATE stel_Data_Subscriptions
            SET
            /* ORIGSQL: orig_Serviceno = */
            orig_Serviceno = serviceno,
            /* ORIGSQL: orig_Customer = */
            orig_Customer = customer,
            /* ORIGSQL: flag = */
            flag = 0
        FROM
            stel_Data_Subscriptions;

        --where flag=-1;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update SERV No, Customer and Flag in STEL_DATA(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update SERV No, Customer and Flag in STEL_DATA_SUBSCRIPTIONS  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , 'Update SERV No, Customer and Flag in STEL_DATA_SUBSCRIPTIONS Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update SERV No, Customer and Flag in STEL_DATA_SUBSCRIPTI(...) */

        /* ORIGSQL: dbms_output.put_line ('23 '||SQL%ROWCOUNT); */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('23 '||::ROWCOUNT);  

        /* ORIGSQL: update inbound_data_Txn t SET ponumber = (SELECT MAX(CallidusSubsc) FROM stel_Da(...) */
        UPDATE inbound_data_Txn t 
            SET
            /* ORIGSQL: ponumber = */
            ponumber = (
                SELECT   /* ORIGSQL: (Select MAX(CallidusSubsc) from stel_Data_Subscriptions s where s.customer=t.bil(...) */
                    MAX(CallidusSubsc)
                FROM
                    stel_Data_Subscriptions s
                WHERE
                    s.customer = t.billtocustid
                    AND s.serviceno = t.billtocontact
                    AND s.componentid = t.productid
                    /* --and s.compensationdate=t.compensationdate */
                    AND s.eventtypeid = t.eventtypeid
            )
        WHERE
            recordstatus = 0
            AND filename = :v_file_name
            AND filedate = :v_file_date;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || '1. Update PONUMBER in INBOUND_DATA_TXN :' || v(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1. Update PONUMBER in INBOUND_DATA_TXN  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , '1. Update PONUMBER in INBOUND_DATA_TXN Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1. Update PONUMBER in INBOUND_DATA_TXN  :' || v_prmtr.fi(...) */

        /* ORIGSQL: update inbound_temp_Txn t SET ponumber = (SELECT MAX(CallidusSubsc) FROM stel_Da(...) */
        UPDATE inbound_temp_Txn t 
            SET
            /* ORIGSQL: ponumber = */
            ponumber = (
                SELECT   /* ORIGSQL: (Select MAX(CallidusSubsc) from stel_Data_Subscriptions s where s.customer=t.bil(...) */
                    MAX(CallidusSubsc)
                FROM
                    stel_Data_Subscriptions s
                WHERE
                    s.customer = t.billtocustid
                    AND s.serviceno = t.billtocontact
                    AND s.componentid = t.productid
                    /* --and s.compensationdate=t.compensationdate  */
                    AND s.eventtypeid = t.eventtypeid
            )
        WHERE
            recordstatus = 0
            AND filename = :v_file_name
            AND filedate = :v_file_date;

        v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

        /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || '2. Update PONUMBER in INBOUND_TEMP_TXN :' || v(...) */
        CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '2. Update PONUMBER in INBOUND_TEMP_TXN  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
            , '2. Update PONUMBER in INBOUND_DATA_TXN Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '2. Update PONUMBER in INBOUND_TEMP_TXN  :' || v_prmtr.fi(...) */

        /* ORIGSQL: dbms_output.put_line ('24 '||SQL%ROWCOUNT); */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('24 '||::ROWCOUNT);  

        /* ORIGSQL: dbms_output.put_line ('2 done witho stel_Data_Subscriptions'); */
        CALL SQLSCRIPT_PRINT:PRINT_LINE('2 done witho stel_Data_Subscriptions');

        /*
        
        insert into Inbound_temp_Assignment
        select * from Inbound_data_Assignment
        where
        filedate=:v_file_date
        and filename=:v_file_name
        and recordstatus=0;
        
        Commit;
        
        Delete from  Inbound_data_txn
        where
        filedate=:v_file_date
        and filename=:v_file_name
        and recordstatus=0;
        
        Commit;
        
         Delete from  Inbound_data_Assignment
        where
        filedate=:v_file_date
        and filename=:v_file_name
        and recordstatus=0;
        
        Commit;
        
        
        
        
        
         */
    END IF;

    -- Cancel Logic

    /*   FOR i
     IN (SELECT ss.*, ROWNUM rn
                    FROM inbound_cfg_BCC_Txn ss
                   WHERE file_type = :v_file_type
     AND file_type = 'BCC-SCII-CancellationOrders')
       LOOP
      */
    -- Inserting Transactions and assignments
    --note: clawback flag in woringly names-  it's a don't pay flag

    /*
    insert into table1(column1)
    select v_filedate from dual;
    commit;
    insert into table2(column1)
    select v_filedate from dual;
    commit;
    */

    /*
    update inbound_data_txn
    set recordstatus=-9
    where filename = :v_file_name and filedate = :v_file_date
    and filename like 'BCC%Cance%';
    
    
    update inbound_data_assignment
    set recordstatus=-9
    where filename = :v_file_name and filedate = :v_file_date
    and filename like 'BCC%Cance%';
    
    
    merge into inbound_data_txn tgt
    using (
        select orderid, linenumber, sublinenumber, eventtypeid, filedate, filename
        , row_number() over( order by filedate desc) rn
        from inbound_Data_txn
        where (orderid, linenumber, sublinenumber, eventtypeid) in (
            select orderid, linenumber, sublinenumber, eventtypeid
            from inbound_Data_txn
            where    filename = :v_file_name and filedate = :v_file_date and recordstatus=-9
            group by orderid, linenumber, sublinenumber, eventtypeid
            having count(*)>1)
        
        
     )src
    on (tgt.orderid=src.orderid and tgt.linenumber=src.linenumber and tgt.sublinenumber=src.sublinenumber)
    when matched then update set
    tgt.recordstatus= case when src.rn=1 then 0 else -2 end
    where recordstatus=0;
    */

    --kyap 20181219: added query, this is extracted from the cancellation query below
    --insert into temp table as there are performance issue in tablespace temp
    /* ORIGSQL: dbms_output.put_line ('29 stel_data_bcccancel_filter insert'); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('29 stel_data_bcccancel_filter insert');

    /* ORIGSQL: execute immediate 'Truncate table stel_data_bcccancel_filter'; */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_BCCCANCEL_FILTER' not found */

    /* ORIGSQL: Truncate table stel_data_bcccancel_filter ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_data_bcccancel_filter';

    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_BCCCANCEL' not found */
    /* ORIGSQL: insert into stel_data_bcccancel_filter SELECT distinct x.orderid, z.crd, z.origi(...) */
    INSERT INTO stel_data_bcccancel_filter
        SELECT   /* ORIGSQL: SELECT distinct x.orderid, z.crd, z.original_order_id, MAX(x.compensationdate) c(...) */
            DISTINCT
            x.orderid,
            z.crd,
            z.original_order_id,
            MAX(x.compensationdate) AS compensationdate
        FROM
            inbound_data_txn x,
            --orig order
            (
                SELECT   /* ORIGSQL: (SELECT DISTINCT original_order_id, cancel_order_id orderid, crd FROM stel_data_(...) */
                    DISTINCT
                    original_order_id,
                    cancel_order_id AS orderid,
                    crd
                FROM
                    stel_data_bcccancel
                    --cancel order
                WHERE
                    filename = :v_file_name
                    AND recordstatus = 0
            ) AS z
        WHERE
            z.original_order_id = x.alternateordernumber
            AND x.recordstatus = 0
            AND (SECONDS_BETWEEN(x.compensationdate,TO_DATE(z.crd,'mm/dd/yyyy'))/86400) <= :v_clawbackperiod  /* ORIGSQL: TO_DATE(z.crd,'mm/dd/yyyy') -x.compensationdate */
        GROUP BY
            x.orderid, z.crd, z.original_order_id;

    /* ORIGSQL: dbms_output.put_line ('30 Cancellation insert'); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('30 Cancellation insert');

    /* RESOLVE: Syntax not supported in target DBMS: INSERT ALL (array-INSERT); not supported in target DBMS, manual conversion required */
    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_BCC_TXN' not found */
 
 /*   INSERT ALL
    INTO inbound_data_txn (FILEDATE,
        FILENAME,
        RECORDSTATUS,
        DOWNLOADED,
        ORDERID,
        LINENUMBER,
        SUBLINENUMBER,
        EVENTTYPEID,
        ACCOUNTINGDATE,
        PRODUCTID,
        PRODUCTNAME,
        PRODUCTDESCRIPTION,
        VALUE,
        UNITTYPEFORVALUE,
        NUMBEROFUNITS,
        UNITVALUE,
        UNITTYPEFORUNITVALUE,
        COMPENSATIONDATE,
        PAYMENTTERMS,
        PONUMBER,
        CHANNEL,
        ALTERNATEORDERNUMBER,
        DATASOURCE,
        NATIVECURRENCY,
        NATIVECURRENCYAMOUNT,
        DISCOUNTPERCENT,
        DISCOUNTTYPE,
        BILLTOCUSTID,
        BILLTOCONTACT,
        BILLTOCOMPANY,
        BILLTOAREACODE,
        BILLTOPHONE,
        BILLTOFAX,
        BILLTOADDRESS1,
        BILLTOADDRESS2,
        BILLTOADDRESS3,
        BILLTOCITY,
        BILLTOSTATE,
        BILLTOCOUNTRY,
        BILLTOPOSTALCODE,
        BILLTOINDUSTRY,
        BILLTOGEOGRAPHY,
        SHIPTOCUSTID,
        SHIPTOCONTACT,
        SHIPTOCOMPANY,
        SHIPTOAREACODE,
        SHIPTOPHONE,
        SHIPTOFAX,
        SHIPTOADDRESS1,
        SHIPTOADDRESS2,
        SHIPTOADDRESS3,
        SHIPTOCITY,
        SHIPTOSTATE,
        SHIPTOCOUNTRY,
        SHIPTOPOSTALCODE,
        SHIPTOINDUSTRY,
        SHIPTOGEOGRAPHY,
        OTHERTOCUSTID,
        OTHERTOCONTACT,
        OTHERTOCOMPANY,
        OTHERTOAREACODE,
        OTHERTOPHONE,
        OTHERTOFAX,
        OTHERTOADDRESS1,
        OTHERTOADDRESS2,
        OTHERTOADDRESS3,
        OTHERTOCITY,
        OTHERTOSTATE,
        OTHERTOCOUNTRY,
        OTHERTOPOSTALCODE,
        OTHERTOINDUSTRY,
        OTHERTOGEOGRAPHY,
        REASONID,
        COMMENTS,
        STAGEPROCESSDATE,
        STAGEPROCESSFLAG,
        BUSINESSUNITNAME,
        BUSINESSUNITMAP,
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
    GENERICBOOLEAN6)
VALUES (FILEDATE,
    FILENAME,
    RECORDSTATUS,
    DOWNLOADED,
    ORDERID,
    LINENUMBER,
    SUBLINENUMBER,
    EVENTTYPEID,
    ACCOUNTINGDATE,
    PRODUCTID,
    PRODUCTNAME,
    PRODUCTDESCRIPTION,
    VALUE,
    UNITTYPEFORVALUE,
    NUMBEROFUNITS,
    UNITVALUE,
    UNITTYPEFORUNITVALUE,
    COMPENSATIONDATE,
    PAYMENTTERMS,
    PONUMBER,
    CHANNEL,
    ALTERNATEORDERNUMBER,
    DATASOURCE,
    NATIVECURRENCY,
    NATIVECURRENCYAMOUNT,
    DISCOUNTPERCENT,
    DISCOUNTTYPE,
    BILLTOCUSTID,
    BILLTOCONTACT,
    BILLTOCOMPANY,
    BILLTOAREACODE,
    BILLTOPHONE,
    BILLTOFAX,
    BILLTOADDRESS1,
    BILLTOADDRESS2,
    BILLTOADDRESS3,
    BILLTOCITY,
    BILLTOSTATE,
    BILLTOCOUNTRY,
    BILLTOPOSTALCODE,
    BILLTOINDUSTRY,
    BILLTOGEOGRAPHY,
    SHIPTOCUSTID,
    SHIPTOCONTACT,
    SHIPTOCOMPANY,
    SHIPTOAREACODE,
    SHIPTOPHONE,
    SHIPTOFAX,
    SHIPTOADDRESS1,
    SHIPTOADDRESS2,
    SHIPTOADDRESS3,
    SHIPTOCITY,
    SHIPTOSTATE,
    SHIPTOCOUNTRY,
    SHIPTOPOSTALCODE,
    SHIPTOINDUSTRY,
    SHIPTOGEOGRAPHY,
    OTHERTOCUSTID,
    OTHERTOCONTACT,
    OTHERTOCOMPANY,
    OTHERTOAREACODE,
    OTHERTOPHONE,
    OTHERTOFAX,
    OTHERTOADDRESS1,
    OTHERTOADDRESS2,
    OTHERTOADDRESS3,
    OTHERTOCITY,
    OTHERTOSTATE,
    OTHERTOCOUNTRY,
    OTHERTOPOSTALCODE,
    OTHERTOINDUSTRY,
    OTHERTOGEOGRAPHY,
    REASONID,
    COMMENTS,
    STAGEPROCESSDATE,
    STAGEPROCESSFLAG,
    BUSINESSUNITNAME,
    BUSINESSUNITMAP,
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
GENERICBOOLEAN6)
INTO inbound_data_assignment (FILEDATE,
FILENAME,
RECORDSTATUS,
DOWNLOADED,
ORDERID,
LINENUMBER,
SUBLINENUMBER,
EVENTTYPEID,
PAYEEID,
PAYEETYPE,
POSITIONNAME,
TITLENAME,
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
GENERICBOOLEAN6)
VALUES (FILEDATE,
FILENAME,
RECORDSTATUS,
DOWNLOADED,
ORDERID,
LINENUMBER,
SUBLINENUMBER,
EVENTTYPEID,
TA_PAYEEID,
TA_Payeetype,
TA_POSITIONNAME,
TA_TITLENAME,
TA_GENERICATTRIBUTE1,
TA_GENERICATTRIBUTE2,
TA_GENERICATTRIBUTE3,
TA_GENERICATTRIBUTE4,
TA_GENERICATTRIBUTE5,
TA_GENERICATTRIBUTE6,
TA_GENERICATTRIBUTE7,
TA_GENERICATTRIBUTE8,
TA_GENERICATTRIBUTE9,
TA_GENERICATTRIBUTE10,
TA_GENERICATTRIBUTE11,
TA_GENERICATTRIBUTE12,
TA_GENERICATTRIBUTE13,
TA_GENERICATTRIBUTE14,
TA_GENERICATTRIBUTE15,
TA_GENERICATTRIBUTE16,
TA_GENERICNUMBER1,
TA_UNITTYPEFORGENERICNUMBER1,
TA_GENERICNUMBER2,
TA_UNITTYPEFORGENERICNUMBER2,
TA_GENERICNUMBER3,
TA_UNITTYPEFORGENERICNUMBER3,
TA_GENERICNUMBER4,
TA_UNITTYPEFORGENERICNUMBER4,
TA_GENERICNUMBER5,
TA_UNITTYPEFORGENERICNUMBER5,
TA_GENERICNUMBER6,
TA_UNITTYPEFORGENERICNUMBER6,
TA_GENERICDATE1,
TA_GENERICDATE2,
TA_GENERICDATE3,
TA_GENERICDATE4,
TA_GENERICDATE5,
TA_GENERICDATE6,
TA_GENERICBOOLEAN1,
TA_GENERICBOOLEAN2,
TA_GENERICBOOLEAN3,
TA_GENERICBOOLEAN4,
TA_GENERICBOOLEAN5,
TA_GENERICBOOLEAN6)
SELECT   
DISTINCT
:v_filedate AS FILEDATE,
:v_file_name AS FILENAME,
0 AS RECORDSTATUS,
NULL AS DOWNLOADED,
CASE
WHEN i.orderid_new = 'Y'
THEN filt.orderid
ELSE txn.orderid
END
AS orderid,
txn.linenumber,
txn.sublinenumber,
txn.eventtypeid,
txn.accountingdate,
txn.productid,
txn.productname,
txn.productdescription,
txn.VALUE * i.VALUE AS VALUE,
txn.unittypeforvalue,
CASE
    WHEN i.cross_cutoffdate = 'Y'
    THEN -1*txn.numberofunits
    ELSE txn.numberofunits
END
AS numberofunits,
txn.unitvalue,
txn.unittypeforunitvalue,
CASE
    WHEN i.cross_cutoffdate = 'N'
    THEN txn.compensationdate
    ELSE TO_DATE(filt.crd,'mm/dd/yyyy')
END
AS compensationdate,
txn.paymentterms,
txn.ponumber,
txn.channel,
txn.alternateordernumber,
txn.datasource,
txn.nativecurrency,
txn.nativecurrencyamount,
txn.discountpercent,
txn.discounttype,
txn.billtocustid,
txn.billtocontact,
txn.billtocompany,
txn.billtoareacode,
txn.billtophone,
txn.billtofax,
txn.billtoaddress1,
txn.billtoaddress2,
txn.billtoaddress3,
txn.billtocity,
txn.billtostate,
txn.billtocountry,
txn.billtopostalcode,
txn.billtoindustry,
txn.billtogeography,
txn.shiptocustid,
txn.shiptocontact,
txn.shiptocompany,
txn.shiptoareacode,
txn.shiptophone,
txn.shiptofax,
txn.shiptoaddress1,
txn.shiptoaddress2,
txn.shiptoaddress3,
txn.shiptocity,
txn.shiptostate,
txn.shiptocountry,
txn.shiptopostalcode,
txn.shiptoindustry,
txn.shiptogeography,
txn.othertocustid,
txn.othertocontact,
txn.othertocompany,
txn.othertoareacode,
txn.othertophone,
txn.othertofax,
txn.othertoaddress1,
txn.othertoaddress2,
txn.othertoaddress3,
txn.othertocity,
txn.othertostate,
txn.othertocountry,
txn.othertopostalcode,
txn.othertoindustry,
txn.othertogeography,
txn.reasonid,
txn.comments,
txn.stageprocessdate,
txn.stageprocessflag,
txn.businessunitname,
txn.businessunitmap,
txn.genericattribute1,
txn.genericattribute2,
txn.genericattribute3,
txn.genericattribute4,
txn.genericattribute5,
txn.genericattribute6,
txn.genericattribute7,
txn.genericattribute8,
txn.genericattribute9,
txn.genericattribute10,
txn.genericattribute11,
txn.genericattribute12,
txn.genericattribute13,
txn.genericattribute14,
txn.genericattribute15,
txn.genericattribute16,
i.order_status AS genericattribute17,
txn.genericattribute18,
txn.genericattribute19,
txn.genericattribute20,
txn.genericattribute21,
txn.genericattribute22,
txn.genericattribute23,
txn.genericattribute24,
txn.genericattribute25,
txn.genericattribute26,
txn.genericattribute27,
txn.genericattribute28,
txn.genericattribute29,
txn.genericattribute30,
txn.genericattribute31,
txn.genericattribute32,
txn.genericnumber1,
txn.unittypeforgenericnumber1,
txn.genericnumber2,
txn.unittypeforgenericnumber2,
txn.genericnumber3,
txn.unittypeforgenericnumber3,
txn.genericnumber4,
txn.unittypeforgenericnumber4,
txn.genericnumber5,
txn.unittypeforgenericnumber5,
txn.genericnumber6,
txn.unittypeforgenericnumber6,
txn.genericdate1,
txn.genericdate2,
txn.genericdate3,
txn.genericdate4,
txn.genericdate5,
txn.genericdate6,
CASE
    WHEN i.clawback_flag = 'Y'
    THEN 1
    ELSE 0
END
AS genericboolean1,
txn.genericboolean2,
txn.genericboolean3,
txn.genericboolean4,
txn.genericboolean5,
txn.genericboolean6,
asign.payeeid AS ta_payeeid,
asign.payeetype AS ta_payeetype,
asign.positionname AS ta_positionname,
asign.titlename AS ta_titlename,
asign.genericattribute1 AS ta_genericattribute1,
asign.genericattribute2 AS ta_genericattribute2,
asign.genericattribute3 AS ta_genericattribute3,
asign.genericattribute4 AS ta_genericattribute4,
asign.genericattribute5 AS ta_genericattribute5,
asign.genericattribute6 AS ta_genericattribute6,
asign.genericattribute7 AS ta_genericattribute7,
asign.genericattribute8 AS ta_genericattribute8,
asign.genericattribute9 AS ta_genericattribute9,
asign.genericattribute10 AS ta_genericattribute10,
asign.genericattribute11 AS ta_genericattribute11,
asign.genericattribute12 AS ta_genericattribute12,
asign.genericattribute13 AS ta_genericattribute13,
asign.genericattribute14 AS ta_genericattribute14,
asign.genericattribute15 AS ta_genericattribute15,
asign.genericattribute16 AS ta_genericattribute16,
asign.genericnumber1 AS ta_genericnumber1,
asign.unittypeforgenericnumber1 AS ta_unittypeforgenericnumber1,
asign.genericnumber2 AS ta_genericnumber2,
asign.unittypeforgenericnumber2 AS ta_unittypeforgenericnumber2,
asign.genericnumber3 AS ta_genericnumber3,
asign.unittypeforgenericnumber3 AS ta_unittypeforgenericnumber3,
asign.genericnumber4 AS ta_genericnumber4,
asign.unittypeforgenericnumber4 AS ta_unittypeforgenericnumber4,
asign.genericnumber5 AS ta_genericnumber5,
asign.unittypeforgenericnumber5 AS ta_unittypeforgenericnumber5,
asign.genericnumber6 AS ta_genericnumber6,
asign.unittypeforgenericnumber6 AS ta_unittypeforgenericnumber6,
asign.genericdate1 AS ta_genericdate1,
asign.genericdate2 AS ta_genericdate2,
asign.genericdate3 AS ta_genericdate3,
asign.genericdate4 AS ta_genericdate4,
asign.genericdate5 AS ta_genericdate5,
asign.genericdate6 AS ta_genericdate6,
asign.genericboolean1 AS ta_genericboolean1,
asign.genericboolean2 AS ta_genericboolean2,
asign.genericboolean3 AS ta_genericboolean3,
asign.genericboolean4 AS ta_genericboolean4,
asign.genericboolean5 AS ta_genericboolean5,
asign.genericboolean6 AS ta_genericboolean6
FROM
inbound_Data_txn txn,
--all historical txns with recstatus=0 for the event types mobile, tv, bb, del
inbound_data_assignment asign,
inbound_cfg_BCC_Txn i,*/ --Deepan :1st portion of insert all

/* (  SELECT distinct x.orderid,
              z.crd,
              z.original_order_id,
              MAX (x.compensationdate) compensationdate
         FROM inbound_data_txn x, --orig order
              (SELECT DISTINCT
                          original_order_id , cancel_order_id orderid, crd
                     FROM stel_data_bcccancel --cancel order
                     WHERE   filename = :v_file_name
         and recordstatus=0
              ) z
        WHERE z.original_order_id = x.alternateordernumber
     and x.recordstatus=0
     and to_Date(z.crd,'mm/dd/yyyy') -x.compensationdate <=v_clawbackperiod
 GROUP BY x.orderid, z.crd, z.original_order_id) filt */ --kyap 20181219: comment out and bring the query out to insert into temp table.
/*stel_data_bcccancel_filter filt
WHERE
i.file_type = 'BCCSCIICancellationOrders' --
AND txn.orderid = asign.orderid
AND txn.linenumber = asign.linenumber
AND txn.sublinenumber = asign.sublinenumber
AND txn.eventtypeid = asign.eventtypeid
AND asign.recordstatus = 0
AND txn.recordstatus = 0
AND txn.orderid = filt.orderid
AND txn.compensationdate = filt.compensationdate
AND txn.filename = asign.filename
AND txn.filedate = asign.filedate
AND (--
    (TO_DECIMAL(TO_VARCHAR(TO_DATE(filt.crd,'mm/dd/yyyy'),'DD'),38,18) < :v_cutoffday8*/--Deepan : Second portion
    
    /*and in the next month or within the same month*/  /* ORIGSQL: TO_NUMBER(TO_CHAR (to_date(filt.crd,'mm/dd/yyyy'), 'DD')) */
                                                                                                                                          /* ORIGSQL: TO_CHAR(to_date(filt.crd,'mm/dd/yyyy'), 'DD') */
   /* AND i.cross_cutoffdate = 'N')
    OR (TO_DECIMAL(TO_VARCHAR(TO_DATE(filt.crd,'mm/dd/yyyy'),'DD'),38,18) >= :v_cutoffday 
                                                                                                           
    AND i.cross_cutoffdate = 'Y')
)
AND txn.eventtypeid = i.eventtypeid;
*/--Deepan : 3rd portion


--Deepan : new insert code, without insert all
    INSERT INTO inbound_data_txn (
        FILEDATE, FILENAME, RECORDSTATUS, DOWNLOADED, ORDERID, LINENUMBER, SUBLINENUMBER, EVENTTYPEID,
        ACCOUNTINGDATE, PRODUCTID, PRODUCTNAME, PRODUCTDESCRIPTION, VALUE, UNITTYPEFORVALUE, NUMBEROFUNITS,
        UNITVALUE, UNITTYPEFORUNITVALUE, COMPENSATIONDATE, PAYMENTTERMS, PONUMBER, CHANNEL, ALTERNATEORDERNUMBER,
        DATASOURCE, NATIVECURRENCY, NATIVECURRENCYAMOUNT, DISCOUNTPERCENT, DISCOUNTTYPE, BILLTOCUSTID, BILLTOCONTACT,
        BILLTOCOMPANY, BILLTOAREACODE, BILLTOPHONE, BILLTOFAX, BILLTOADDRESS1, BILLTOADDRESS2, BILLTOADDRESS3,
        BILLTOCITY, BILLTOSTATE, BILLTOCOUNTRY, BILLTOPOSTALCODE, BILLTOINDUSTRY, BILLTOGEOGRAPHY, SHIPTOCUSTID,
        SHIPTOCONTACT, SHIPTOCOMPANY, SHIPTOAREACODE, SHIPTOPHONE, SHIPTOFAX, SHIPTOADDRESS1, SHIPTOADDRESS2,
        SHIPTOADDRESS3, SHIPTOCITY, SHIPTOSTATE, SHIPTOCOUNTRY, SHIPTOPOSTALCODE, SHIPTOINDUSTRY, SHIPTOGEOGRAPHY,
        OTHERTOCUSTID, OTHERTOCONTACT, OTHERTOCOMPANY, OTHERTOAREACODE, OTHERTOPHONE, OTHERTOFAX, OTHERTOADDRESS1,
        OTHERTOADDRESS2, OTHERTOADDRESS3, OTHERTOCITY, OTHERTOSTATE, OTHERTOCOUNTRY, OTHERTOPOSTALCODE,
        OTHERTOINDUSTRY, OTHERTOGEOGRAPHY, REASONID, COMMENTS, STAGEPROCESSDATE, STAGEPROCESSFLAG, BUSINESSUNITNAME,
        BUSINESSUNITMAP, GENERICATTRIBUTE1, GENERICATTRIBUTE2, GENERICATTRIBUTE3, GENERICATTRIBUTE4, GENERICATTRIBUTE5,
        GENERICATTRIBUTE6, GENERICATTRIBUTE7, GENERICATTRIBUTE8, GENERICATTRIBUTE9, GENERICATTRIBUTE10,
        GENERICATTRIBUTE11, GENERICATTRIBUTE12, GENERICATTRIBUTE13, GENERICATTRIBUTE14, GENERICATTRIBUTE15,
        GENERICATTRIBUTE16, GENERICATTRIBUTE17, GENERICATTRIBUTE18, GENERICATTRIBUTE19, GENERICATTRIBUTE20,
        GENERICATTRIBUTE21, GENERICATTRIBUTE22, GENERICATTRIBUTE23, GENERICATTRIBUTE24, GENERICATTRIBUTE25,
        GENERICATTRIBUTE26, GENERICATTRIBUTE27, GENERICATTRIBUTE28, GENERICATTRIBUTE29, GENERICATTRIBUTE30,
        GENERICATTRIBUTE31, GENERICATTRIBUTE32, GENERICNUMBER1, UNITTYPEFORGENERICNUMBER1, GENERICNUMBER2,
        UNITTYPEFORGENERICNUMBER2, GENERICNUMBER3, UNITTYPEFORGENERICNUMBER3, GENERICNUMBER4, UNITTYPEFORGENERICNUMBER4,
        GENERICNUMBER5, UNITTYPEFORGENERICNUMBER5, GENERICNUMBER6, UNITTYPEFORGENERICNUMBER6, GENERICDATE1,
        GENERICDATE2, GENERICDATE3, GENERICDATE4, GENERICDATE5, GENERICDATE6, GENERICBOOLEAN1, GENERICBOOLEAN2,
        GENERICBOOLEAN3, GENERICBOOLEAN4, GENERICBOOLEAN5, GENERICBOOLEAN6
    )
    SELECT DISTINCT
        :v_filedate, :v_file_name, 0, NULL,
        CASE
            WHEN i.orderid_new = 'Y' THEN filt.orderid
            ELSE txn.orderid
        END,
        txn.linenumber, txn.sublinenumber, txn.eventtypeid, txn.accountingdate, txn.productid, txn.productname,
        txn.productdescription, txn.VALUE * i.VALUE, txn.unittypeforvalue,
        CASE
            WHEN i.cross_cutoffdate = 'Y' THEN -1 * txn.numberofunits
            ELSE txn.numberofunits
        END,
        txn.unitvalue, txn.unittypeforunitvalue,
        CASE
            WHEN i.cross_cutoffdate = 'N' THEN txn.compensationdate
            ELSE TO_DATE(filt.crd, 'mm/dd/yyyy')
        END,
        txn.paymentterms, txn.ponumber, txn.channel, txn.alternateordernumber, txn.datasource, txn.nativecurrency,
        txn.nativecurrencyamount, txn.discountpercent, txn.discounttype, txn.billtocustid, txn.billtocontact,
        txn.billtocompany, txn.billtoareacode, txn.billtophone, txn.billtofax, txn.billtoaddress1, txn.billtoaddress2,
        txn.billtoaddress3, txn.billtocity, txn.billtostate, txn.billtocountry, txn.billtopostalcode, txn.billtoindustry,
        txn.billtogeography, txn.shiptocustid, txn.shiptocontact, txn.shiptocompany, txn.shiptoareacode, txn.shiptophone,
        txn.shiptofax, txn.shiptoaddress1, txn.shiptoaddress2, txn.shiptoaddress3, txn.shiptocity, txn.shiptostate,
        txn.shiptocountry, txn.shiptopostalcode, txn.shiptoindustry, txn.shiptogeography, txn.othertocustid,
        txn.othertocontact, txn.othertocompany, txn.othertoareacode, txn.othertophone, txn.othertofax, txn.othertoaddress1,
        txn.othertoaddress2, txn.othertoaddress3, txn.othertocity, txn.othertostate, txn.othertocountry,
        txn.othertopostalcode, txn.othertoindustry, txn.othertogeography, txn.reasonid, txn.comments,
        txn.stageprocessdate, txn.stageprocessflag, txn.businessunitname, txn.businessunitmap, txn.genericattribute1,
        txn.genericattribute2, txn.genericattribute3, txn.genericattribute4, txn.genericattribute5, txn.genericattribute6,
        txn.genericattribute7, txn.genericattribute8, txn.genericattribute9, txn.genericattribute10, txn.genericattribute11,
        txn.genericattribute12, txn.genericattribute13, txn.genericattribute14, txn.genericattribute15, txn.genericattribute16,
        i.order_status, txn.genericattribute18, txn.genericattribute19, txn.genericattribute20, txn.genericattribute21,
        txn.genericattribute22, txn.genericattribute23, txn.genericattribute24, txn.genericattribute25, txn.genericattribute26,
        txn.genericattribute27, txn.genericattribute28, txn.genericattribute29, txn.genericattribute30, txn.genericattribute31,
        txn.genericattribute32, txn.genericnumber1, txn.unittypeforgenericnumber1, txn.genericnumber2, txn.unittypeforgenericnumber2,
        txn.genericnumber3, txn.unittypeforgenericnumber3, txn.genericnumber4, txn.unittypeforgenericnumber4, txn.genericnumber5,
        txn.unittypeforgenericnumber5, txn.genericnumber6, txn.unittypeforgenericnumber6, txn.genericdate1, txn.genericdate2,
        txn.genericdate3, txn.genericdate4, txn.genericdate5, txn.genericdate6,
        CASE
            WHEN i.clawback_flag = 'Y' THEN 1
            ELSE 0
        END,
        txn.genericboolean2, txn.genericboolean3, txn.genericboolean4, txn.genericboolean5, txn.genericboolean6
    FROM inbound_Data_txn txn
    JOIN inbound_data_assignment asign ON
        txn.orderid = asign.orderid AND txn.linenumber = asign.linenumber AND txn.sublinenumber = asign.sublinenumber
        AND txn.eventtypeid = asign.eventtypeid AND txn.filename = asign.filename AND txn.filedate = asign.filedate
    JOIN inbound_cfg_BCC_Txn i ON i.file_type = 'BCCSCIICancellationOrders' AND txn.eventtypeid = i.eventtypeid
    JOIN stel_data_bcccancel_filter filt ON txn.orderid = filt.orderid AND txn.compensationdate = filt.compensationdate
    WHERE asign.recordstatus = 0 AND txn.recordstatus = 0
    AND ((TO_DECIMAL(TO_VARCHAR(TO_DATE(filt.crd, 'mm/dd/yyyy'), 'DD'), 38, 18) < :v_cutoffday AND i.cross_cutoffdate = 'N')
    OR (TO_DECIMAL(TO_VARCHAR(TO_DATE(filt.crd, 'mm/dd/yyyy'), 'DD'), 38, 18) >= :v_cutoffday AND i.cross_cutoffdate = 'Y'));

    -- Insert into inbound_data_assignment
    INSERT INTO inbound_data_assignment (
        FILEDATE, FILENAME, RECORDSTATUS, DOWNLOADED, ORDERID, LINENUMBER, SUBLINENUMBER, EVENTTYPEID,
        PAYEEID, PAYEETYPE, POSITIONNAME, TITLENAME, GENERICATTRIBUTE1, GENERICATTRIBUTE2, GENERICATTRIBUTE3,
        GENERICATTRIBUTE4, GENERICATTRIBUTE5, GENERICATTRIBUTE6, GENERICATTRIBUTE7, GENERICATTRIBUTE8,
        GENERICATTRIBUTE9, GENERICATTRIBUTE10, GENERICATTRIBUTE11, GENERICATTRIBUTE12, GENERICATTRIBUTE13,
        GENERICATTRIBUTE14, GENERICATTRIBUTE15, GENERICATTRIBUTE16, GENERICNUMBER1, UNITTYPEFORGENERICNUMBER1,
        GENERICNUMBER2, UNITTYPEFORGENERICNUMBER2, GENERICNUMBER3, UNITTYPEFORGENERICNUMBER3, GENERICNUMBER4,
        UNITTYPEFORGENERICNUMBER4, GENERICNUMBER5, UNITTYPEFORGENERICNUMBER5, GENERICNUMBER6, UNITTYPEFORGENERICNUMBER6,
        GENERICDATE1, GENERICDATE2, GENERICDATE3, GENERICDATE4, GENERICDATE5, GENERICDATE6, GENERICBOOLEAN1,
        GENERICBOOLEAN2, GENERICBOOLEAN3, GENERICBOOLEAN4, GENERICBOOLEAN5, GENERICBOOLEAN6
    )
    SELECT DISTINCT
        :v_filedate, :v_file_name, 0, NULL,
        CASE
            WHEN i.orderid_new = 'Y' THEN filt.orderid
            ELSE txn.orderid
        END,
        txn.linenumber, txn.sublinenumber, txn.eventtypeid,
        asign.payeeid, asign.payeetype, asign.positionname, asign.titlename,
        asign.genericattribute1, asign.genericattribute2, asign.genericattribute3, asign.genericattribute4,
        asign.genericattribute5, asign.genericattribute6, asign.genericattribute7, asign.genericattribute8,
        asign.genericattribute9, asign.genericattribute10, asign.genericattribute11, asign.genericattribute12,
        asign.genericattribute13, asign.genericattribute14, asign.genericattribute15, asign.genericattribute16,
        asign.genericnumber1, asign.unittypeforgenericnumber1, asign.genericnumber2, asign.unittypeforgenericnumber2,
        asign.genericnumber3, asign.unittypeforgenericnumber3, asign.genericnumber4, asign.unittypeforgenericnumber4,
        asign.genericnumber5, asign.unittypeforgenericnumber5, asign.genericnumber6, asign.unittypeforgenericnumber6,
        asign.genericdate1, asign.genericdate2, asign.genericdate3, asign.genericdate4, asign.genericdate5,
        asign.genericdate6, asign.genericboolean1, asign.genericboolean2, asign.genericboolean3, asign.genericboolean4,
        asign.genericboolean5, asign.genericboolean6
    FROM inbound_Data_txn txn
    JOIN inbound_data_assignment asign ON
        txn.orderid = asign.orderid AND txn.linenumber = asign.linenumber AND txn.sublinenumber = asign.sublinenumber
        AND txn.eventtypeid = asign.eventtypeid AND txn.filename = asign.filename AND txn.filedate = asign.filedate
    JOIN inbound_cfg_BCC_Txn i ON i.file_type = 'BCCSCIICancellationOrders' AND txn.eventtypeid = i.eventtypeid
    JOIN stel_data_bcccancel_filter filt ON txn.orderid = filt.orderid AND txn.compensationdate = filt.compensationdate
    WHERE asign.recordstatus = 0 AND txn.recordstatus = 0
    AND ((TO_DECIMAL(TO_VARCHAR(TO_DATE(filt.crd, 'mm/dd/yyyy'), 'DD'), 38, 18) < :v_cutoffday AND i.cross_cutoffdate = 'N')
    OR (TO_DECIMAL(TO_VARCHAR(TO_DATE(filt.crd, 'mm/dd/yyyy'), 'DD'), 38, 18) >= :v_cutoffday AND i.cross_cutoffdate = 'Y'));


    --

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Insert 30 Cancellation :' || :v_file_type(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert 30 Cancellation  :'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Insert 30 Cancellation Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert 30 Cancellation  :' || :v_file_type || '-File(...) */

    /* ORIGSQL: dbms_output.put_line ('31 Cancellation insert rows: '||SQL%ROWCOUNT); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('31 Cancellation insert rows: '||::ROWCOUNT);  

    /* ORIGSQL: COMMIT; */
    COMMIT;  

    /* ORIGSQL: update inbound_Data_Txn SET shiptoaddress3 = orderid||'-'||linenumber||'-'||subl(...) */
    UPDATE inbound_Data_Txn
        SET
        /* ORIGSQL: shiptoaddress3 = */
        shiptoaddress3 = IFNULL(TO_VARCHAR(orderid),'')||'-'||IFNULL(TO_VARCHAR(linenumber),'')||'-'||IFNULL(TO_VARCHAR(sublinenumber),'')
    FROM
        inbound_Data_Txn
    WHERE
        recordstatus = 0
        AND filename = :v_file_name
        AND filedate = :v_file_date;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || '1.Update to ShiptoAddress:' || v_prmtr.file_ty(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '1.Update to ShiptoAddress:'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , '1. Update to ShiptoAddress Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '1.Update to ShiptoAddress:' || :v_file_type || '-Fil(...) */

    /* ORIGSQL: commit; */
    COMMIT;  

    /* ORIGSQL: update inbound_temp_Txn SET shiptoaddress3 = orderid||'-'||linenumber||'-'||subl(...) */
    UPDATE inbound_temp_Txn
        SET
        /* ORIGSQL: shiptoaddress3 = */
        shiptoaddress3 = IFNULL(TO_VARCHAR(orderid),'')||'-'||IFNULL(TO_VARCHAR(linenumber),'')||'-'||IFNULL(TO_VARCHAR(sublinenumber),'')
    FROM
        inbound_temp_Txn
    WHERE
        recordstatus = 0
        AND filename = :v_file_name
        AND filedate = :v_file_date;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || '2.Update to ShiptoAddress:' || v_prmtr.file_ty(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || '2.Update to ShiptoAddress:'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , '2. Update to ShiptoAddress Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || '2.Update to ShiptoAddress:' || :v_file_type || '-Fil(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_temp_Txn t using (SELECT DENSE_RANK() OVER (PARTITION BY ponu(...) */
    MERGE INTO inbound_temp_Txn AS t
        USING
        (
            SELECT   /* ORIGSQL: (select DENSE_RANK() OVER (PARTITION BY ponumber, billtocontact, billtocustid, p(...) */
                DENSE_RANK() OVER (PARTITION BY ponumber, billtocontact, billtocustid, productid ORDER BY billtostate) AS rn,/* --denserank, not rownum if the order has repeating C/AC items */  ponumber,
                billtocontact,
                billtocustid,
                productid,
                billtostate   
            FROM
                (
                    SELECT   /* ORIGSQL: (select distinct ponumber, billtocontact, billtocustid, productid, billtostate f(...) */
                        DISTINCT
                        ponumber,
                        billtocontact,
                        billtocustid,
                        productid,
                        billtostate
                    FROM
                        inbound_temp_Txn
                ) AS dbmtk_corrname_5784
            ) AS s
            ON (s.ponumber = t.ponumber
                AND s.productid = t.productid
                AND s.billtocontact = t.billtocontact
                AND s.billtocustid = t.billtocustid
            AND s.billtostate = t.billtostate)
    WHEN MATCHED THEN
        UPDATE SET
            t.tempfield1 = s.rn;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_Txn t using (SELECT DENSE_RANK() OVER (PARTITION BY ponu(...) */
    MERGE INTO inbound_data_Txn AS t
        USING
        (
            SELECT   /* ORIGSQL: (select DENSE_RANK() OVER (PARTITION BY ponumber, billtocontact, billtocustid, p(...) */
                DENSE_RANK() OVER (PARTITION BY ponumber, billtocontact, billtocustid, productid,filename ORDER BY billtostate) AS rn,
                ponumber,
                billtocontact,
                billtocustid,
                productid,
                billtostate,
                filename   
            FROM
                (
                    SELECT   /* ORIGSQL: (select distinct ponumber, billtocontact, billtocustid, productid, billtostate, (...) */
                        DISTINCT
                        ponumber,
                        billtocontact,
                        billtocustid,
                        productid,
                        billtostate,
                        filename
                    FROM
                        inbound_data_Txn
                        --needs to be filtered for performance
                    WHERE
                        recordstatus = 0
                        AND filename = :v_file_name
                        AND filedate = :v_file_date
                ) AS dbmtk_corrname_5787
            ) AS s
            ON (s.ponumber = t.ponumber
                AND s.productid = t.productid
                AND s.billtocontact = t.billtocontact
                AND s.billtocustid = t.billtocustid
                AND s.filename = t.filename
                AND s.billtostate = t.billtostate
                AND recordstatus = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            t.tempfield1 = s.rn, t.genericnumber4 = s.rn, t.unittypeforgenericnumber4 = 'quantity'
        ;

    /* ORIGSQL: dbms_output.put_line ('31 Start Deletion'); */
    -- CALL SQLSCRIPT_PRINT:PRINT_LINE('31 Start Deletion');

    --For order ceased that are aldready in TC. This does not handled cessations in the same day's file 
    /* ORIGSQL: delete from stel_Temp_cessations tgt where TRIM(tgt.filename) =TRIM(v_prmtr.file(...) */
    DELETE
    FROM
        stel_Temp_cessations
        tgt
    WHERE
        TRIM(tgt.filename) = TRIM(:v_file_name);

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Delete from stel_Temp_Cessations' || v_prmtr.f(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Delete from stel_Temp_Cessations'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Delete from stel_Temp_Cessations Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Delete from stel_Temp_Cessations' || :v_file_type ||(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: delete from stel_Temp_cessationsCE tgt where TRIM(tgt.filename) =TRIM(v_prmtr.fi(...) */
    DELETE
    FROM
        stel_Temp_cessationsCE
        tgt
    WHERE
        TRIM(tgt.filename) = TRIM(:v_file_name);

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Delete from stel_Temp_Cessations' || v_prmtr.f(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Delete from stel_Temp_Cessations'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Delete from stel_Temp_Cessations Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Delete from stel_Temp_Cessations' || :v_file_type ||(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: dbms_output.put_line ('31 Finish Deletion' ||:v_file_name); */
    CALL SQLSCRIPT_PRINT:PRINT_LINE('31 Finish Deletion'||IFNULL(:v_file_name,''));

    --cessations for txns that are in Commissions already  
    /* ORIGSQL: insert into stel_Temp_cessations select distinct :v_file_name,v_prmtr.file_(...) */
    INSERT INTO stel_Temp_cessations
        SELECT   /* ORIGSQL: select distinct :v_file_name,:v_file_date,ce.orderid ceorderid, txn.li(...) */
            DISTINCT
            :v_file_name,
            :v_file_date,
            ce.orderid AS ceorderid,
            txn.linenumber AS celinenumber,
            txn.sublinenumber AS cesublinenumber,
            txn.orderid AS txnorderid,
            txn.linenumber AS txnlinenumber,
            txn.sublinenumber AS txnsublinenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.linenumber
                ELSE ce.linenumber
            END
            AS linenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.sublinenumber
                ELSE ce.sublinenumber
            END
            AS sublinenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.orderid
                ELSE ce.orderid
            END
            AS orderid,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN '1'
                ELSE '0'
            END
            AS clawbackflag,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 1
                ELSE -1
            END
            AS unitsmultiplier,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN ce.compensationdate
                ELSE txn.compensationdate
            END
            AS accdate,
            ce.compensationdate AS ceasedate,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 'Ceased by: '|| IFNULL(ce.orderid,'')||'-'||IFNULL(TO_VARCHAR(ce.linenumber),'')||'-'||IFNULL(TO_VARCHAR(ce.sublinenumber),'')
                ELSE 'Reversal of '||IFNULL(txn.orderid,'')||'-'||IFNULL(TO_VARCHAR(txn.linenumber),'')||'-'||IFNULL(TO_VARCHAR(txn.sublinenumber),'')
            END
            AS remarks
            ,/* --cehiptaddress3 commented out on 20170906 */  /* --  txn.shiptoaddress3 */
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.shiptoaddress3
                ELSE txn.shiptoaddress3
                /* -- last part chnaged from ce to txn */
                /* --this is for txns that are in commissions, so they are not in this file.  */
                /* -- we use the original txn.STA3 to avoid doing a separate insert */
                /* -- the issue is whther it should be gb1=1 or numberunits=-1, which is determined based on the date logic */
            END
            AS shiptoaddress3,
            i.cross_cutoffdate
        FROM
            inbound_Temp_txn ce
        INNER JOIN
            inbound_data_Txn txn
            ON ce.ponumber = txn.ponumber
            --and txn.genericattribute10
            AND (ce.productid = txn.productid
            AND ce.eventtypeid NOT LIKE '%TV%')
            -- and ce.productid=txn.productid
            AND txn.recordstatus = 0
            -- and txn.filename= :v_file_name
            /* join inbound_data_assignment asign
            on asign.orderid=txn.orderid
            and asign.linenumber=txn.linenumber and asign.sublinenumber=txn.sublinenumber
            and asign.recordstatus=0 and txn.filename=asign.filename and txn.filedate=asign.filedate*/
        INNER JOIN
            STEL_TEMP_TRANSACTION st
            --copy of vw_Salestransaction
            ON st.orderid = txn.orderid
            AND st.linenumber = txn.linenumber
            AND st.sublinenumber = txn.sublinenumber
            AND txn.eventtypeid = st.eventtypeid
            AND IFNULL(st.genericboolean1,0) = 0 --don't cease txns that are already ceased
            /* ORIGSQL: nvl(st.genericboolean1,0) */
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (Select distinct customer, componentid, serviceno, eventtypeid, callidussubsc fr(...) */
                    DISTINCT
                    customer,
                    componentid,
                    serviceno,
                    eventtypeid,
                    callidussubsc
                FROM
                    stel_Data_Subscriptions
            ) AS subs
            ON ce.ponumber = subs.callidussubsc
            AND ce.productid = subs.componentid
            AND ce.billtocustid = subs.customer
            AND ce.billtocontact = subs.serviceno
            AND ce.tempfield1 = txn.tempfield1
        INNER JOIN
            inbound_cfg_BCC_Txn i
            ON i.eventtypeid = txn.eventtypeid
        WHERE
            ce.filename = :v_file_name
            AND i.scenario = 'BCC-VAS-Cease'
            AND i.file_type = :v_file_type
            AND txn.genericattribute22 <> 'C'
            AND ce.genericattribute22 = 'C'
            AND IFNULL(ce.genericattribute31, 'N') <> 'P'   /* ORIGSQL: nvl(ce.genericattribute31,'N') */
            /*
            and  (
                  (  nvl(ce.genericattribute31,'N')<>'P' and  ce.genericattribute10<>'CE' and ce.eventtypeid like '%TV%')
                 or
                  (  nvl(ce.genericattribute31,'N') in ('P','N') and  ce.genericattribute10='CE' and ce.eventtypeid like '%TV%')
                 or
                    (ce.eventtypeid not like '%TV%')
                
                )
            */
            AND IFNULL(txn.genericattribute31, 'N') <> 'P'   /* ORIGSQL: nvl(txn.genericattribute31,'N') */
            --and st.comments not like '%Treat as New%' --1123 CR
            AND IFNULL(st.comments, '*') NOT LIKE '%Treat as New%' --bugfix by KY, as comments IS NULL when empty
            /* ORIGSQL: nvl(st.comments,'*') */

            AND ce.compensationdate <= TO_DATE(ADD_SECONDS(txn.compensationdate,(86400*(:v_clawbackperiod))))   /* ORIGSQL: txn.compensationdate+ v_clawbackperiod */
            AND (
                (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) < :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                        /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    AND i.cross_cutoffdate = 'N'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') >TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                              /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                OR (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) >= :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                            /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    AND i.cross_cutoffdate = 'Y'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') > TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                               /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */

                OR --in the same mth
                (
                    TO_VARCHAR(ce.compensationdate,'YYYYMM') =TO_VARCHAR(txn.compensationdate,'YYYYMM')  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                         /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                    AND i.cross_cutoffdate = 'N'
                )
            )
            /*and txn.genericattribute7 in (select subset_scenario from  inbound_cfg_bccsuperscnearios
                where superset_scenario = 'BCC-VAS-Cease'
            and enable_flag=1)*/;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations 1' || v_prmtr(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert from stel_Temp_Cessations 1'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Insert into stel_Temp_Cessations 1 Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations 1' || :v_file_type (...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --for cessations in the same day file  
    /* ORIGSQL: insert into stel_Temp_cessations select distinct :v_file_name,v_prmtr.file_(...) */
    INSERT INTO stel_Temp_cessations
        SELECT   /* ORIGSQL: select distinct :v_file_name,:v_file_date,ce.orderid ceorderid, txn.li(...) */
            DISTINCT
            :v_file_name,
            :v_file_date,
            ce.orderid AS ceorderid,
            txn.linenumber AS celinenumber,
            txn.sublinenumber AS cesublinenumber,
            txn.orderid AS txnorderid,
            txn.linenumber AS txnlinenumber,
            txn.sublinenumber AS txnsublinenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.linenumber
                ELSE ce.linenumber
            END
            AS linenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.sublinenumber
                ELSE ce.sublinenumber
            END
            AS sublinenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.orderid
                ELSE IFNULL(ce.orderid,'')||'CE' 
            END
            AS orderid,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN '1'
                ELSE '0'
            END
            AS clawbackflag,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 1
                ELSE -1
            END
            AS unitsmultiplier,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN ce.compensationdate
                ELSE txn.compensationdate
            END
            AS accdate,
            ce.compensationdate AS ceasedate,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 'Ceased by: '|| IFNULL(ce.orderid,'')||'-'||IFNULL(TO_VARCHAR(ce.linenumber),'')||'-'||IFNULL(TO_VARCHAR(ce.sublinenumber),'')
                ELSE 'Reversal of '|| IFNULL(txn.orderid,'')||'-'||IFNULL(TO_VARCHAR(txn.linenumber),'')||'-'||IFNULL(TO_VARCHAR(txn.sublinenumber),'')
            END
            AS remarks,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.shiptoaddress3
                ELSE ce.shiptoaddress3/*changed from txn to ce for DEL issue on 20171013*/
            END
            AS shiptoaddress3,
            i.cross_cutoffdate
        FROM
            inbound_Temp_txn ce
        INNER JOIN
            inbound_data_Txn txn
            ON ce.ponumber = txn.ponumber
            AND ce.billtostate = txn.billtostate
            AND (ce.productid = txn.productid
            AND ce.eventtypeid NOT LIKE '%TV%')
            AND txn.recordstatus = 0
            AND CE.BILLTOSTATE >= TXN.BILLTOSTATE
            AND txn.filename = :v_file_name
            /* join inbound_data_assignment asign
            on asign.orderid=txn.orderid
            and asign.linenumber=txn.linenumber and asign.sublinenumber=txn.sublinenumber
            and asign.recordstatus=0 and txn.filename=asign.filename and txn.filedate=asign.filedate*/
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (Select distinct customer, componentid, serviceno, eventtypeid, callidussubsc fr(...) */
                    DISTINCT
                    customer,
                    componentid,
                    serviceno,
                    eventtypeid,
                    callidussubsc
                FROM
                    stel_Data_Subscriptions
            ) AS subs
            ON ce.ponumber = subs.callidussubsc
            AND ce.productid = subs.componentid
            AND ce.billtocustid = subs.customer
            AND ce.billtocontact = subs.serviceno
            AND ce.tempfield1 = txn.tempfield1
        INNER JOIN
            inbound_cfg_BCC_Txn i
            ON i.eventtypeid = txn.eventtypeid
        WHERE
            ce.filename = :v_file_name
            AND i.scenario = 'BCC-VAS-Cease'
            AND i.file_type = :v_file_type
            AND ce.recordstatus = 0
            AND txn.genericattribute22 <> 'C'
            AND ce.genericattribute22 = 'C'
            AND IFNULL(ce.genericattribute31, 'N') <> 'P'   /* ORIGSQL: nvl(ce.genericattribute31,'N') */
            AND IFNULL(txn.genericattribute31, 'N') <> 'P'   /* ORIGSQL: nvl(txn.genericattribute31,'N') */
            --and ce.billtocustid='S0206475H'
            AND ce.compensationdate <= TO_DATE(ADD_SECONDS(txn.compensationdate,(86400*(:v_clawbackperiod))))   /* ORIGSQL: txn.compensationdate+ v_clawbackperiod */
            AND (
                (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) < :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                        /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    --next mth, before 15th
                    AND i.cross_cutoffdate = 'N'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') >TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                              /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                OR --next mt, after 15th
                (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) >= :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                         /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    AND i.cross_cutoffdate = 'Y'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') > TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                               /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                OR --in the same mth
                (
                    TO_VARCHAR(ce.compensationdate,'YYYYMM') =TO_VARCHAR(txn.compensationdate,'YYYYMM')  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                         /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                    AND i.cross_cutoffdate = 'N'
                )
            )
            /* and txn.genericattribute7 in (select subset_scenario from  inbound_cfg_bccsuperscnearios
                where superset_scenario = 'BCC-VAS-Cease'
            and enable_flag=1)*/;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations 2' || v_prmtr(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert from stel_Temp_Cessations 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Insert into stel_Temp_Cessations 2 Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations 2' || :v_file_type (...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --special handling for TV
    -- same file

    /*
    if CE, then check if new for the subscription happened in 30 days
    if yes, then cease all for that subscription
    */   

    /* ORIGSQL: insert into stel_Temp_CessationsCE select distinct :v_file_name,v_prmtr.fil(...) */
    INSERT INTO stel_Temp_CessationsCE
        SELECT   /* ORIGSQL: select distinct :v_file_name,:v_file_date, ce.orderid ceorderid, txn.l(...) */
            DISTINCT
            :v_file_name,
            :v_file_date,
            ce.orderid AS ceorderid,
            txn.linenumber AS celinenumber,
            txn.sublinenumber AS cesublinenumber,
            txn.orderid AS txnorderid,
            txn.linenumber AS txnlinenumber,
            txn.sublinenumber AS txnsublinenumber,
            0 AS linenumber,
            0 AS sublinenumber,
            0 AS orderid,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN '1'
                ELSE '0'
            END
            AS clawbackflag,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 1
                ELSE -1
            END
            AS unitsmultiplier,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN ce.compensationdate
                ELSE txn.compensationdate
            END
            AS accdate,
            ce.compensationdate AS ceasedate,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 'Ceased by: '|| IFNULL(ce.orderid,'')
                ELSE 'Reversal of '|| IFNULL(txn.orderid,'')||'-'||IFNULL(TO_VARCHAR(txn.linenumber),'')||'-'||IFNULL(TO_VARCHAR(txn.sublinenumber),'')
            END
            AS remarks,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.shiptoaddress3
                ELSE ce.shiptoaddress3
            END
            AS shiptoaddress3,
            i.cross_cutoffdate
        FROM
            inbound_data_Txn txn
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select ce.ponumber, MIN(ce.compensationdate) compensationdate, MIN(ce.orderid) (...) */
                    ce.ponumber,
                    MIN(ce.compensationdate) AS compensationdate,
                    MIN(ce.orderid) AS orderid,
                    MIN(ce.shiptoaddress3) AS shiptoaddress3
                FROM
                    inbound_temp_txn ce
                INNER JOIN
                    inbound_Data_txn txn
                    ON ce.ponumber = txn.ponumber
                    AND txn.recordstatus = 0
                    AND txn.filename = ce.filename
                    --and txn.genericattribute10 in ('PR','CH')
                    AND txn.genericattribute9 = ce.genericattribute9
                    AND ce.compensationdate <= TO_DATE(ADD_SECONDS(txn.compensationdate,(86400*30)))   /* ORIGSQL: txn.compensationdate+ 30 */
                    AND ce.eventtypeid = txn.eventtypeid
                WHERE
                    ce.genericattribute10 = 'CE'
                    --and ce.billtocustid='S0206475H'
                    AND ce.filename = :v_file_name
                    AND ce.genericattribute9 = 'M'
                GROUP BY
                    ce.ponumber
            ) AS ce
            ON ce.ponumber = txn.ponumber
        INNER JOIN
            inbound_cfg_BCC_Txn i
            ON i.eventtypeid = txn.eventtypeid
            AND i.scenario = 'BCC-VAS-Cease'
            AND i.file_type = :v_file_type
        WHERE
            txn.genericattribute22 <> 'C'
            AND IFNULL(txn.genericattribute31, 'N') <> 'P'  /* ORIGSQL: nvl(txn.genericattribute31,'N') */
            AND txn.recordstatus = 0
            AND txn.genericattribute10 IN ('PR','CH')
            AND txn.filename = :v_file_name

            AND ce.compensationdate <= TO_DATE(ADD_SECONDS(txn.compensationdate,(86400*(:v_clawbackperiod))))   /* ORIGSQL: txn.compensationdate+ v_clawbackperiod */
            AND (
                (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) < :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                        /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    --next mth, before 15th
                    AND i.cross_cutoffdate = 'N'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') >TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                              /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                OR --next mt, after 15th
                (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) >= :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                         /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    AND i.cross_cutoffdate = 'Y'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') > TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                               /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                OR --in the same mth
                (
                    TO_VARCHAR(ce.compensationdate,'YYYYMM') =TO_VARCHAR(txn.compensationdate,'YYYYMM')  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                         /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                    AND i.cross_cutoffdate = 'N'
                )
            );

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations CE 1' || v_pr(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert from stel_Temp_Cessations CE 1'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Insert into stel_Temp_Cessations 2 Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations CE 1' || v_prmtr.file_ty(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --pre loaded file

    --pre loaded file  

    /* ORIGSQL: insert into stel_Temp_cessationsCE select distinct :v_file_name,v_prmtr.fil(...) */
    INSERT INTO stel_Temp_cessationsCE
        SELECT   /* ORIGSQL: select distinct :v_file_name,:v_file_date,ce.orderid ceorderid, txn.li(...) */
            DISTINCT
            :v_file_name,
            :v_file_date,
            ce.orderid AS ceorderid,
            txn.linenumber AS celinenumber,
            txn.sublinenumber AS cesublinenumber,
            txn.orderid AS txnorderid,
            txn.linenumber AS txnlinenumber,
            txn.sublinenumber AS txnsublinenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.linenumber
                ELSE ce.linenumber
            END
            AS linenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.sublinenumber
                ELSE ce.sublinenumber
            END
            AS sublinenumber,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.orderid
                ELSE ce.orderid
            END
            AS orderid,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN '1'
                ELSE '0'
            END
            AS clawbackflag,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 1
                ELSE -1
            END
            AS unitsmultiplier,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN ce.compensationdate
                ELSE txn.compensationdate
            END
            AS accdate,
            ce.compensationdate AS ceasedate,
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN 'Ceased by: '|| IFNULL(ce.orderid,'')||'-'||IFNULL(TO_VARCHAR(ce.linenumber),'')||'-'||IFNULL(TO_VARCHAR(ce.sublinenumber),'')
                ELSE 'Reversal of '||IFNULL(txn.orderid,'')||'-'||IFNULL(TO_VARCHAR(txn.linenumber),'')||'-'||IFNULL(TO_VARCHAR(txn.sublinenumber),'')
            END
            AS remarks
            ,/* --cehiptaddress3 commented out on 20170906 */  /* --  txn.shiptoaddress3 */
            CASE
                WHEN i.cross_cutoffdate = 'N'
                THEN txn.shiptoaddress3
                ELSE txn.shiptoaddress3
                /* -- last part chnaged from ce to txn */
                /* --this is for txns that are in commissions, so they are not in this file.  */
                /* -- we use the original txn.STA3 to avoid doing a separate insert */
                /* -- the issue is whther it should be gb1=1 or numberunits=-1, which is determined based on the date logic */
            END
            AS shiptoaddress3,
            i.cross_cutoffdate
        FROM
            inbound_Temp_txn ce
        INNER JOIN
            inbound_data_Txn txn
            ON ce.ponumber = txn.ponumber
            AND ce.genericattribute10 = 'CE'
            AND txn.genericattribute9 = ce.genericattribute9
            AND ce.eventtypeid = txn.eventtypeid
            AND ce.eventtypeid LIKE 'TV%'
            AND txn.recordstatus = 0
        INNER JOIN
            STEL_TEMP_TRANSACTION st
            --copy of vw_Salestransaction
            ON st.orderid = txn.orderid
            AND st.linenumber = txn.linenumber
            AND st.sublinenumber = txn.sublinenumber
            AND txn.eventtypeid = st.eventtypeid
            AND IFNULL(st.genericboolean1,0) = 0 --don't cease txns that are already ceased
            /* ORIGSQL: nvl(st.genericboolean1,0) */
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (Select distinct customer, componentid, serviceno, eventtypeid, callidussubsc fr(...) */
                    DISTINCT
                    customer,
                    componentid,
                    serviceno,
                    eventtypeid,
                    callidussubsc
                FROM
                    stel_Data_Subscriptions
            ) AS subs
            ON ce.ponumber = subs.callidussubsc
            AND ce.productid = subs.componentid
            AND ce.billtocustid = subs.customer
            AND ce.billtocontact = subs.serviceno
            AND ce.tempfield1 = txn.tempfield1
        INNER JOIN
            inbound_cfg_BCC_Txn i
            ON i.eventtypeid = txn.eventtypeid
        WHERE
            1 = 1
            AND --ce.filename=:v_file_name
            i.scenario = 'BCC-VAS-Cease'
            AND i.file_type = :v_file_type
            AND txn.genericattribute22 <> 'C'
            AND IFNULL(txn.genericattribute31, 'N') <> 'P'   /* ORIGSQL: nvl(txn.genericattribute31,'N') */
            AND txn.recordstatus = 0
            AND txn.genericattribute10 IN ('PR','CH')
            --and txn.filename= :v_file_name

            AND ce.compensationdate <= TO_DATE(ADD_SECONDS(txn.compensationdate,(86400*(:v_clawbackperiod))))   /* ORIGSQL: txn.compensationdate+ v_clawbackperiod */
            AND (
                (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) < :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                        /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    --next mth, before 15th
                    AND i.cross_cutoffdate = 'N'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') >TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                              /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                OR --next mt, after 15th
                (TO_DECIMAL(TO_VARCHAR(ce.Compensationdate,'DD'),38,18) >= :v_cutoffday  /* ORIGSQL: TO_NUMBER(TO_CHAR (ce.Compensationdate, 'DD')) */
                                                                                         /* ORIGSQL: TO_CHAR(ce.Compensationdate, 'DD') */
                    AND i.cross_cutoffdate = 'Y'
                    AND TO_VARCHAR(ce.compensationdate,'YYYYMM') > TO_VARCHAR(txn.compensationdate,'YYYYMM'))  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                               /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                OR --in the same mth
                (
                    TO_VARCHAR(ce.compensationdate,'YYYYMM') =TO_VARCHAR(txn.compensationdate,'YYYYMM')  /* ORIGSQL: to_Char(txn.compensationdate,'YYYYMM') */
                                                                                                         /* ORIGSQL: to_Char(ce.compensationdate,'YYYYMM') */
                    AND i.cross_cutoffdate = 'N'
                )
            )
            /*and txn.genericattribute7 in (select subset_scenario from  inbound_cfg_bccsuperscnearios
                where superset_scenario = 'BCC-VAS-Cease'
            and enable_flag=1)*/;

    /*
    where txn.genericattribute22<>'C'
     and nvl(txn.genericattribute31,'N')<>'P'
    and txn.recordstatus=0
     and txn.genericattribute10 in ('PR','CH')
    and txn.filename= :v_file_name
    
     and ce.compensationdate<=txn.compensationdate+ v_clawbackperiod
     and
      (
          (TO_NUMBER (TO_CHAR (ce.Compensationdate, 'DD')) < v_cutoffday
              --next mth, before 15th
             and i.cross_cutoffdate='N' and to_Char(ce.compensationdate,'YYYYMM')>to_Char(txn.compensationdate,'YYYYMM') )
         OR --next mt, after 15th
          (TO_NUMBER (TO_CHAR (ce.Compensationdate, 'DD')) >= v_cutoffday
             and i.cross_cutoffdate='Y' and to_Char(ce.compensationdate,'YYYYMM')> to_Char(txn.compensationdate,'YYYYMM') )
         OR --in the same mth
            (
                 to_Char(ce.compensationdate,'YYYYMM')=to_Char(txn.compensationdate,'YYYYMM') and i.cross_cutoffdate='N'
            )
      )
     */

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations CE 2' || v_pr(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert from stel_Temp_Cessations CE 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Insert into stel_Temp_Cessations 2 Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert from stel_Temp_Cessations CE 2' || v_prmtr.file_ty(...) */

    --this merge is for cessations that happen in the same file
    --2018 june adding min to remarks to cater for bad data 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Txn tgt using (SELECT shiptoaddress3, MIN(orderid) AS or(...) */
    MERGE INTO inbound_Data_Txn AS tgt
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_CESSATIONS' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select shiptoaddress3, MIN(orderid) orderid, linenumber, sublinenumber, clawbac(...) */
                shiptoaddress3,
                MIN(orderid) AS orderid,
                linenumber,
                sublinenumber,
                clawbackflag,
                MIN(remarks) AS remarks,
                MIN(ceasedate) AS ceasedate,
                MIN(accdate) AS accdate,
                unitsmultiplier
            FROM
                stel_Temp_cessations tgt
            WHERE
                tgt.filename = :v_file_name
                AND (cross_cutoffdate = 'N'
                    OR (cross_cutoffdate = 'Y'
                AND unitsmultiplier > - 1))
            GROUP BY
                shiptoaddress3, linenumber, sublinenumber, clawbackflag, unitsmultiplier
        ) AS src
        ON (tgt.shiptoaddress3 = src.shiptoaddress3
        	AND recordstatus = 0
            AND tgt.filename = :v_file_name
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.orderid = src.orderid
            ,tgt.linenumber = src.linenumber
            ,tgt.sublinenumber = src.sublinenumber
            ,tgt.genericboolean1 = src.clawbackflag
            ,tgt.comments = src.remarks
            ,tgt.genericdate1 = src.ceasedate
            ,tgt.numberofunits = tgt.numberofunits*unitsmultiplier
            ,tgt.accountingdate = src.accdate
            --, tgt.shiptoaddress3 = txnorderid||'-'||txnlinenumber||'-'||txnsublinenumber
       ;--and tgt.filedate=:v_file_date

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-1', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    /* ORIGSQL: execute immediate 'Truncate table stel_Temp_cessationsCE_temp drop storage' ; */
    /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_CESSATIONSCE_TEMP' not found */

    /* ORIGSQL: Truncate table stel_Temp_cessationsCE_temp drop storage ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_Temp_cessationsCE_temp';

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-2', 1, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    /* ORIGSQL: insert into stel_Temp_cessationsCE_temp select shiptoaddress3, MIN(orderid) orde(...) */
    INSERT INTO stel_Temp_cessationsCE_temp
        SELECT   /* ORIGSQL: select shiptoaddress3, MIN(orderid)orderid, linenumber, sublinenumber, clawbackf(...) */
            shiptoaddress3,
            MIN(orderid)orderid,
            linenumber,
            sublinenumber,
            clawbackflag,
            MIN(remarks) AS remarks,
            ceasedate,
            accdate,
            unitsmultiplier
        FROM
            stel_Temp_cessationsCE tgt
        WHERE
            tgt.filename = :v_file_name
            AND (cross_cutoffdate = 'N')
            AND orderid <> '0'
            AND shiptoaddress3 NOT IN
            (
                SELECT   /* ORIGSQL: (select shiptoaddress3 from stel_Temp_cessations x where x.filename=v_prmtr.file(...) */
                    shiptoaddress3
                FROM
                    stel_Temp_cessations x
                WHERE
                    x.filename = :v_file_name
                    AND (x.cross_cutoffdate = 'N'
                        OR (x.cross_cutoffdate = 'Y'
                    AND x.unitsmultiplier > - 1))
            )
        GROUP BY
            shiptoaddress3, linenumber, sublinenumber, clawbackflag, ceasedate, accdate, unitsmultiplier;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-3', 1, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Txn tgt using (SELECT * FROM stel_Temp_cessationsCE_temp(...) */
    MERGE INTO inbound_Data_Txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select * from stel_Temp_cessationsCE_temp) */
                *
            FROM
                stel_Temp_cessationsCE_temp
        ) AS src
        ON (tgt.shiptoaddress3 = src.shiptoaddress3
        	AND recordstatus = 0
            AND tgt.filename = :v_file_name
        )
    WHEN MATCHED THEN
        UPDATE
            SET
            --tgt.orderid=src.orderid
            --,tgt.linenumber=src.linenumber
            --,tgt.sublinenumber=src.sublinenumber
            tgt.genericboolean1 = src.clawbackflag
            ,tgt.comments =IFNULL(src.remarks,IFNULL(src.remarks,'')||'CE Complete')  /* ORIGSQL: nvl(src.remarks,src.remarks||'CE Complete') */
            ,tgt.genericdate1 = src.ceasedate
            ,tgt.numberofunits = tgt.numberofunits*unitsmultiplier
            ,tgt.accountingdate = src.accdate
            --, tgt.shiptoaddress3 = txnorderid||'-'||txnlinenumber||'-'||txnsublinenumber
       ;--and tgt.filedate=:v_file_date

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-4', 1, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-5', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    --this is to update the negative txn with the attributes of the original txn 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Txn tgt using (SELECT stc_shiptoaddress3, stc_orderid, s(...) */
    MERGE INTO inbound_Data_Txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select stc_shiptoaddress3, stc_orderid, stc_linenumber, stc_sublinenumber, claw(...) */
                stc_shiptoaddress3,
                stc_orderid,
                stc_linenumber,
                stc_sublinenumber,
                clawbackflag,
                stc_remarks,
                ceasedate,
                accdate,
                unitsmultiplier,
                orig.*  
            FROM
                (
                    SELECT   /* ORIGSQL: (select shiptoaddress3 stc_shiptoaddress3, MIN(orderid) stc_orderid, linenumber (...) */
                        shiptoaddress3 AS stc_shiptoaddress3,
                        MIN(orderid) AS stc_orderid,
                        linenumber AS stc_linenumber,
                        sublinenumber AS stc_sublinenumber,
                        clawbackflag,
                        remarks AS stc_remarks,
                        ceasedate,
                        accdate,
                        unitsmultiplier,
                        txnorderid,
                        txnlinenumber,
                        txnsublinenumber,
                        filename
                    FROM
                        stel_Temp_cessations tgt
                    WHERE
                        tgt.filename = :v_file_name
                        AND cross_cutoffdate = 'Y'
                        AND unitsmultiplier = -1
                    GROUP BY
                        shiptoaddress3, linenumber, sublinenumber, clawbackflag,remarks, ceasedate, accdate, unitsmultiplier
                        ,txnorderid, txnlinenumber, txnsublinenumber, filename
                ) AS t
            INNER JOIN
                inbound_data_txn orig
                ON orig.filename = t.filename
                AND orig.genericattribute22 = 'AC'
                AND orig.orderid = t.txnorderid
                AND orig.linenumber = t.txnlinenumber
                AND orig.sublinenumber = t.txnsublinenumber
        ) AS src
        ON (tgt.shiptoaddress3 = src.stc_shiptoaddress3
        	AND tgt.recordstatus = 0
            AND tgt.filename = :v_file_name
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.orderid = src.stc_orderid
            ,tgt.linenumber = src.stc_linenumber
            ,tgt.sublinenumber = src.stc_sublinenumber
            ,tgt.genericboolean1 = src.clawbackflag
            ,tgt.comments = src.stc_remarks
            ,tgt.genericdate1 = src.ceasedate
            ,tgt.numberofunits = tgt.numberofunits*unitsmultiplier
            ,tgt.accountingdate = src.accdate
            ,tgt.value = src.value
            ,tgt.genericattribute1 = src.genericattribute1
            ,tgt.genericattribute2 = src.genericattribute2
            ,tgt.genericattribute3 = src.genericattribute3
            ,tgt.genericattribute4 = src.genericattribute4
            ,tgt.genericattribute5 = src.genericattribute5
            ,tgt.genericattribute6 = src.genericattribute6
            ,tgt.genericattribute7 = src.genericattribute7
            ,tgt.genericattribute8 = src.genericattribute8
            ,tgt.genericattribute9 = src.genericattribute9
            ,tgt.genericattribute10 = src.genericattribute10
            ,tgt.genericattribute11 = src.genericattribute11
            ,tgt.genericattribute12 = src.genericattribute12
            ,tgt.genericattribute13 = src.genericattribute13
            ,tgt.genericattribute14 = src.genericattribute14
            ,tgt.genericattribute15 = src.genericattribute15
            ,tgt.genericattribute16 = src.genericattribute16
            ,tgt.genericattribute17 = src.genericattribute17
            ,tgt.genericattribute18 = src.genericattribute18
            ,tgt.genericattribute19 = src.genericattribute19
            ,tgt.genericattribute20 = src.genericattribute20
            ,tgt.genericattribute21 = src.genericattribute21
            ,tgt.genericattribute22 = src.genericattribute22
            ,tgt.genericattribute23 = src.genericattribute23
            ,tgt.genericattribute24 = src.genericattribute24
            ,tgt.genericattribute25 = src.genericattribute25
            ,tgt.genericattribute26 = src.genericattribute26
            ,tgt.genericattribute27 = src.genericattribute27
            ,tgt.genericattribute28 = src.genericattribute28
            ,tgt.genericattribute29 = src.genericattribute29
            ,tgt.genericattribute30 = src.genericattribute30
            ,tgt.genericattribute31 = src.genericattribute31
            ,tgt.genericattribute32 = src.genericattribute32
            ,tgt.genericboolean2 = src.genericboolean2
            ,tgt.genericboolean3 = src.genericboolean3
            ,tgt.genericboolean4 = src.genericboolean4
            ,tgt.genericboolean5 = src.genericboolean5
            ,tgt.genericboolean6 = src.genericboolean6
            ,tgt.genericdate2 = src.genericdate2
            ,tgt.genericdate3 = src.genericdate3
            ,tgt.genericdate4 = src.genericdate4
            ,tgt.genericdate5 = src.genericdate5
            ,tgt.genericdate6 = src.genericdate6
            ,tgt.genericnumber1 = src.genericnumber1
            ,tgt.genericnumber2 = src.genericnumber2
            ,tgt.genericnumber3 = src.genericnumber3
            ,tgt.genericnumber4 = src.genericnumber4
            ,tgt.genericnumber5 = src.genericnumber5
            ,tgt.genericnumber6 = src.genericnumber6
            ,tgt.unittypeforgenericnumber1 = src.unittypeforgenericnumber1
            ,tgt.unittypeforgenericnumber2 = src.unittypeforgenericnumber2
            ,tgt.unittypeforgenericnumber3 = src.unittypeforgenericnumber3
            ,tgt.unittypeforgenericnumber4 = src.unittypeforgenericnumber4
            ,tgt.unittypeforgenericnumber5 = src.unittypeforgenericnumber5
            ,tgt.unittypeforgenericnumber6 = src.unittypeforgenericnumber6

            --, tgt.shiptoaddress3 = txnorderid||'-'||txnlinenumber||'-'||txnsublinenumber
        ;--and tgt.filedate=:v_file_date

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-6', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    --this mergfe is for cessations that happen with prior txn in a diffeernt file 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Txn tgt using (SELECT shiptoaddress3, MIN(orderid) AS or(...) */
    MERGE INTO inbound_Data_Txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select shiptoaddress3, MIN(orderid) orderid, linenumber, sublinenumber, clawbac(...) */
                shiptoaddress3,
                MIN(orderid) AS orderid,
                linenumber,
                sublinenumber,
                clawbackflag,
                remarks,
                ceasedate,
                accdate,
                unitsmultiplier
            FROM
                stel_Temp_cessations tgt
            WHERE
                tgt.filename = :v_file_name
                AND tgt.orderid  
                IN
                (
                    SELECT   /* ORIGSQL: (Select a.orderid from stel_temp_transaction a) */
                        a.orderid
                    FROM
                        stel_temp_transaction a
                ) --added on 11 14 2017. txn should exist in TC for this seciton to fire
                AND tgt.ceorderid  
                =
                (
                    SELECT   /* ORIGSQL: (select MAX(t.ceorderid) from stel_Temp_cessations t where t.filename=v_prmtr.fi(...) */
                        MAX(t.ceorderid)
                    FROM
                        stel_Temp_cessations t
                    WHERE
                        t.filename = :v_file_name
                        AND t.shiptoaddress3 = tgt.shiptoaddress3
                )

            GROUP BY
                shiptoaddress3, linenumber, sublinenumber, clawbackflag,remarks, ceasedate, accdate, unitsmultiplier
        ) AS src
        ON (tgt.shiptoaddress3 = src.shiptoaddress3
        	AND recordstatus = 0
            AND tgt.filename <> :v_file_name
            AND tgt.filename  
            =
            (
                SELECT   /* ORIGSQL: (Select MAX(x.filename) from inbound_Data_Txn x where x.shiptoaddress3=tgt.shipt(...) */
                    MAX(x.filename)
                FROM
                    inbound_Data_Txn x
                WHERE
                    x.shiptoaddress3 = tgt.shiptoaddress3
                    AND x.recordstatus = 0
                    AND x.filename <> :v_file_name
            )
        )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.orderid =SUBSTRING(IFNULL(src.orderid,'')||
                CASE   /* ORIGSQL: substr(src.orderid|| CASE WHEN unitsmultiplier=-1 THEN 'CE' ELSE '' END,1,40) */
                    WHEN unitsmultiplier = -1
                    THEN 'CE'
                    ELSE ''
                END
            ,1,40)
            ,tgt.linenumber = src.linenumber
            ,tgt.sublinenumber = src.sublinenumber
            ,tgt.genericboolean1 = src.clawbackflag
            ,tgt.comments = src.remarks
            ,tgt.genericdate1 = src.ceasedate
            ,tgt.numberofunits = tgt.numberofunits*unitsmultiplier
            ,tgt.accountingdate = src.accdate
            ,tgt.compensationdate =
            CASE
                WHEN unitsmultiplier = -1
                THEN src.ceasedate
                ELSE tgt.compensationdate
            END
            ,tgt.filename = :v_file_name
            ,tgt.filedate = :v_file_date
            --, tgt.shiptoaddress3 = txnorderid||'-'||txnlinenumber||'-'||txnsublinenumber
        
            --and tgt.filedate=:v_file_date
            --and pick up the latest record for that STA3 ,
            -- normally there should only be one anyway, but in UAT ehre are many due to dupliicate loads

            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-7', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    ---For CEs with -1 from prev file or current file, or gb1=1 from prev file, insert
    --For CEs with +1, gb1=1, update existing txns in same file
    --merge into in

    /*
    
    1.       \x93New Order\x94 raised by \x93Vendor A\x94 with \x93Price plan X\x94 and \x93Change plan\x94 transaction raised by same \x93Vendor A\x94 with \x93Price Plan Y\x94 within same commission month, then system needs to pay to the \x93Vendor A\x94 for the \x93Price plan Y\x94.
    2.       \x93New Order\x94 raised by \x93Vendor A\x94 with \x93Price plan X\x94 and \x93Change plan\x94 transaction raised by different \x93Vendor B\x94 with \x93Price Plan Y\x94 within same commission month, then system needs to pay to the \x93Vendor A\x94 for the \x93Price plan X\x94. In this case \x93Vendor B\x94 is not eligible to get commission.
    3.       \x93Change plan\x94 transactions across the commission month is not payable.
    
    */

    --get Vendor 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_Txn tgt using (SELECT st.orderid, st.linenumber, st.subl(...) */
    MERGE INTO inbound_data_Txn AS tgt 
        USING
        (
            SELECT distinct   /* ORIGSQL: (select st.orderid, st.linenumber, st.sublinenumber, nvl(nvl(x.parentpositionnam(...) */
                st.orderid,
                st.linenumber,
                st.sublinenumber,
                IFNULL(IFNULL(x.parentpositionname, x2.parentposnname),x3.parentposnname) AS vendor  /* ORIGSQL: nvl(nvl(x.parentpositionname, x2.parentposnname),x3.parentposnname) */
            FROM
                inbound_Data_txn st
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select parentpositionname, childpositionname, relnstartdate,relnenddate from st(...) */
                        parentpositionname,
                        childpositionname,
                        relnstartdate,
                        relnenddate
                    FROM
                        EXT.stel_positionrelation_ext
                        /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_positionrelation@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_positionrelation'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        relnname = 'MMP Vendor Dealer'
                ) AS X
                ON st.compensationdate BETWEEN X.relnstartdate AND add_days(X.relnenddate,-1)
                AND childpositionname = st.genericattribute4
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select nvl(m.name,p.name) parentposnname, p.name childposnname, p.effectivestar(...) */
                        IFNULL(m.name,p.name) AS parentposnname,
                        p.name AS childposnname,
                        p.effectivestartdate,
                        p.effectiveenddate
                    FROM
                        cs_position p
                    LEFT OUTER JOIN
                        cs_position m
                        ON p.managerseq = m.ruleelementownerseq
                        AND m.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND p.effectivestartdate BETWEEN m.effectivestartdate AND add_days(m.effectiveenddate,-1)
                        /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND p.name IN
                        (
                            SELECT   /* ORIGSQL: (Select dim0 from stel_lookup@stelext where name like 'LT_Dealer_Channel Type') */
                                dim0
                            FROM
                                EXT.stel_lookup
                                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_lookup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_lookup'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            WHERE
                                name LIKE 'LT_Dealer_Channel Type'
                        )
                    ) AS X2
                    ON X2.childposnname = st.genericattribute4
                    AND st.compensationdate BETWEEN X2.effectivestartdate AND add_days(X2.effectiveenddate,-1)
                LEFT OUTER JOIN
                    (
                        SELECT   /* ORIGSQL: (select nvl(m.name,p.name) parentposnname, p.name childposnname, p.effectivestar(...) */
                            IFNULL(m.name,p.name) AS parentposnname,
                            p.name AS childposnname,
                            p.effectivestartdate,
                            p.effectiveenddate
                        FROM
                            cs_position p
                        LEFT OUTER JOIN
                            cs_position m
                            ON p.managerseq = m.ruleelementownerseq
                            AND m.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            AND p.effectivestartdate BETWEEN m.effectivestartdate AND add_days(m.effectiveenddate,-1)
                            /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        WHERE
                            p.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            AND p.name IN
                            (
                                SELECT   /* ORIGSQL: (Select dim0 from stel_lookup@stelext where name like 'LT_Dealer_Channel Type') */
                                    dim0
                                FROM
                                    EXT.stel_lookup
                                    /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_lookup@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_lookup'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                WHERE
                                    name LIKE 'LT_Dealer_Channel Type'
                            )
                        ) AS X3
                        ON X3.childposnname = st.genericattribute3
                        AND st.compensationdate BETWEEN X3.effectivestartdate AND add_days(X3.effectiveenddate,-1)
                    WHERE
                        st.filename = :v_file_name
                        and st.filedate = :v_file_date--Deepan: Added filedate condition to restrict to unique batch
                        AND st.recordstatus = 0
                        AND st.eventtypeid = 'BroadBand Closed'
                ) AS src
                ON(tgt.orderid = src.orderid
                AND tgt.linenumber = src.linenumber
                AND tgt.sublinenumber = src.sublinenumber
                AND tgt.filename = :v_file_name
--                AND tgt.filedate = :v_file_date--Deepan: Added filedate condition to restrict to unique batch
            AND tgt.recordstatus = 0
            AND tgt.eventtypeid = 'BroadBand Closed'
                )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.genericattribute21 = src.vendor
        ;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-8', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    --same file and same venodr 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_Txn tgt using (SELECT txn.ponumber, MAX(txn2.orderid) AS(...) */
    MERGE INTO inbound_data_Txn AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (select txn.ponumber, MAX(txn2.orderid) as NewOrdId, txn.orderid from inbound_da(...) */
                txn.ponumber,
                MAX(txn2.orderid) AS NewOrdId,
                txn.orderid
            FROM
                inbound_data_Txn txn
            INNER JOIN
                inbound_data_Txn txn2
                ON txn2.genericattribute11 = 'Change Main Plan'
                AND txn.ponumber = txn2.ponumber
            WHERE
                txn.filename = :v_file_name
                AND txn.filedate = :v_file_date
                AND txn.recordstatus = 0
                AND txn2.filename = :v_file_name
                AND txn2.filedate = :v_file_date
                AND txn2.recordstatus = 0
                AND txn.genericattribute5 = 'New'
                AND txn.genericattribute9 = 'M'
                AND txn2.genericattribute9 = 'M'
                AND txn.eventtypeid LIKE '%BroadBand Closed%' --for external only
                AND txn.orderid <> txn2.orderid
                --and in the same mth
                AND TO_VARCHAR(txn.compensationdate,'yyyymm') = TO_VARCHAR(txn2.compensationdate,'yyyymm')  /* ORIGSQL: to_Char(txn2.compensationdate,'yyyymm') */
                                                                                                            /* ORIGSQL: to_Char(txn.compensationdate,'yyyymm') */
                AND txn.genericattribute21 = txn2.genericattribute21 --same vendor
                AND txn.genericattribute22 = 'AC'
                AND txn2.genericattribute22 = 'AC'
            GROUP BY
                txn.ponumber, txn.orderid
        ) AS src
        ON ((tgt.orderid = src.orderid
            OR tgt.orderid = src.newordid)
        AND tgt.ponumber = src.ponumber
        AND tgt.filename = :v_file_name
                AND tgt.filedate = :v_file_date
                AND tgt.recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET
            Comments =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Replaced with Change Plan:'||IFNULL(src.newordid,'')
                ELSE 'Treat as New Plan. Replace:'||IFNULL(src.orderid,'')
            END,
            tgt.genericattribute5 =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Replaced'
                ELSE 'New'
            END
            /*end, tgt.comments = case
             when tgt.orderid=src.orderid then 'Unceased'
             else tgt.comments
             end(*/
                , tgt.genericboolean1 =
                CASE
                    WHEN tgt.orderid = src.orderid
                    THEN 0
                    ELSE tgt.genericboolean1
                END
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-9', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    --same file diff vednnor 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_Txn tgt using (SELECT txn.ponumber, MAX(txn2.orderid) AS(...) */
    MERGE INTO inbound_data_Txn AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (select txn.ponumber, MAX(txn2.orderid) as NewOrdId, txn.orderid from inbound_da(...) */
                txn.ponumber,
                MAX(txn2.orderid) AS NewOrdId,
                txn.orderid
            FROM
                inbound_data_Txn txn
            INNER JOIN
                inbound_data_Txn txn2
                ON txn2.genericattribute11 = 'Change Main Plan'
                AND txn.ponumber = txn2.ponumber
            WHERE
                txn.filename = :v_file_name
                AND txn.filedate = :v_file_date
                AND txn.recordstatus = 0
                AND txn2.filename = :v_file_name
                AND txn2.filedate = :v_file_date
                AND txn2.recordstatus = 0
                AND txn.genericattribute5 = 'New'
                AND txn.genericattribute9 = 'M'
                AND txn2.genericattribute9 = 'M'
                AND txn.eventtypeid LIKE '%BroadBand Closed%' --for external only
                AND txn.orderid <> txn2.orderid
                --and in the same mth
                AND TO_VARCHAR(txn.compensationdate,'yyyymm') = TO_VARCHAR(txn2.compensationdate,'yyyymm')  /* ORIGSQL: to_Char(txn2.compensationdate,'yyyymm') */
                                                                                                            /* ORIGSQL: to_Char(txn.compensationdate,'yyyymm') */
                AND txn.genericattribute21 <> txn2.genericattribute21 --diff vendor
                AND txn.genericattribute22 = 'AC'
                AND txn2.genericattribute22 = 'AC'
            GROUP BY
                txn.ponumber, txn.orderid
        ) AS src
        ON ((tgt.orderid = src.orderid
            OR tgt.orderid = src.newordid)
        AND tgt.ponumber = src.ponumber
        	AND tgt.filename = :v_file_name
            AND tgt.filedate = :v_file_date
            AND tgt.recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET
            Comments =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Unceased due to Change Main Paln by another vendor'
                ELSE 'Ignore'
            END,
            tgt.genericboolean1 =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 0
                ELSE tgt.genericboolean1
            END
        ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-9', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    -- on a diff day (previoulsy loaded into TC)
    ---same vendor
    --this merge affects only the CP in the current file. and the New that has been cpoiue dover by the ceasing logic 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_txn tgt using (SELECT txn.ponumber, MAX(txn2.orderid) AS(...) */
    MERGE INTO inbound_Data_txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select txn.ponumber, MAX(txn2.orderid) as NewOrdId, txn.orderid from vw_Salestr(...) */
                txn.ponumber,
                MAX(txn2.orderid) AS NewOrdId,
                txn.orderid
            FROM
                EXT.vw_Salestransaction txn
            INNER JOIN
                inbound_Data_txn txn2
                ON txn2.genericattribute11 = 'Change Main Plan'
                AND txn.ponumber = txn2.ponumber
                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.vw_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.vw_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                txn2.filename = :v_file_name
                AND txn2.filedate = :v_file_date
                AND txn2.recordstatus = 0
                AND txn.genericattribute5 = 'New'
                AND txn.genericattribute9 = 'M'
                AND txn2.genericattribute9 = 'M'
                AND txn.orderid <> txn2.orderid
                AND txn.genericattribute11 = 'New'
                AND txn.genericattribute9 = 'M'
                AND txn2.genericattribute9 = 'M'
                AND txn.eventtypeid LIKE '%BroadBand Closed%' --for external only
                AND txn.orderid <> txn2.orderid
                --and in the same mth
                AND TO_VARCHAR(txn.compensationdate,'yyyymm') = TO_VARCHAR(txn2.compensationdate,'yyyymm')  /* ORIGSQL: to_Char(txn2.compensationdate,'yyyymm') */
                                                                                                            /* ORIGSQL: to_Char(txn.compensationdate,'yyyymm') */
                AND txn.genericattribute21 = txn2.genericattribute21 --same vendor
                AND txn.genericattribute22 = 'AC'
                AND txn2.genericattribute22 = 'AC'

            GROUP BY
                txn.ponumber, txn.orderid
        ) AS src
        ON ((tgt.orderid = src.orderid
            OR tgt.orderid = src.newordid)
        AND tgt.ponumber = src.ponumber
        AND tgt.filename = :v_file_name
            AND tgt.filedate = :v_file_date
            AND tgt.recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET
            Comments =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Replaced with Change Plan:'||IFNULL(src.newordid,'')
                ELSE 'Treat as New Plan. Replace:'||IFNULL(src.orderid,'')
            END,
            tgt.genericattribute5 =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Replaced'
                ELSE 'New'
            END
        ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-10', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    /*
    --same vdr
    --this merge is to overwite the original New Txn as Replaced, if the txn was already in TC
    
     merge into  inbound_Data_txn tgt
     using (
         select txn.ponumber, max(txn2.orderid) as NewOrdId, txn.orderid
         from vw_Salestransaction@stelext txn
         join  inbound_Data_txn txn2
         on txn2.genericattribute11 = 'Change Main Plan'
         and txn.ponumber=txn2.ponumber
         where   txn2.filename = :v_file_name and txn2.filedate=:v_file_date and txn2.recordstatus=0
         and txn.genericattribute5='New'
         and txn.genericattribute9='M' and txn2.genericattribute9='M'
        
         and txn.genericattribute11='New'
         and txn.genericattribute9='M' and txn2.genericattribute9='M'
         and txn.eventtypeid like '%BroadBand Closed%' --for external only
         and txn.orderid<>txn2.orderid
          --and in the same mth
         and to_Char(txn.compensationdate,'yyyymm') = to_Char(txn2.compensationdate,'yyyymm')
         and txn.genericattribute21=txn2.genericattribute21 --same vendor
         and txn.genericattribute22='AC'
         and txn2.genericattribute22='AC'
        
         group by txn.ponumber , txn.orderid
        
     )src
     on ((tgt.orderid=src.orderid or tgt.orderid=src.newordid) and tgt.ponumber=src.ponumber)
      when matched then update set
      tempfield4=filename,
      filename = :v_file_name
      ,filedate=:v_file_date
      ,recordstatus=0
     ,Comments = case
     when tgt.orderid=src.orderid then 'Replaced with Change Plan:'||src.newordid
     else 'Treat as New Plan. Replace:'||src.orderid
     end,
     tgt.genericattribute5 = case
     when tgt.orderid=src.orderid then 'Replaced'
     else 'New'
     end
     where tgt.filename <> :v_file_name and tgt.recordstatus=0
     and tgt.filename = (Select max(x.filename) from inbound_Data_Txn x where x.shiptoaddress3=tgt.shiptoaddress3 and x.eventtypeid=tgt.eventtypeid
         and x.recordstatus=0
    and x.filename<>:v_file_name)
    
     ;--difference is here
    */

    -- diff vednor

    --replacing txn, requires no change. it will not be paid due to txn type
    --replaced txn, have to uncease it
    --above the ceasing logic must have created a GB1=1 version of the original txn with the same filename. have to set gb1=0 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_txn tgt using (SELECT txn.ponumber, MAX(txn2.orderid) AS(...) */
    MERGE INTO inbound_Data_txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select txn.ponumber, MAX(txn2.orderid) as NewOrdId, txn.orderid from vw_Salestr(...) */
                txn.ponumber,
                MAX(txn2.orderid) AS NewOrdId,
                txn.orderid
            FROM
                EXT.vw_Salestransaction txn
            INNER JOIN
                inbound_Data_txn txn2
                ON txn2.genericattribute11 = 'Change Main Plan'
                AND txn.ponumber = txn2.ponumber
                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.vw_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.vw_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                txn2.filename = :v_file_name
                AND txn2.filedate = :v_file_date
                AND txn2.recordstatus = 0
                AND txn.genericattribute5 = 'New'
                AND txn.genericattribute9 = 'M'
                AND txn2.genericattribute9 = 'M'
                AND txn.orderid <> txn2.orderid
                AND txn.genericattribute11 = 'New'
                AND txn.genericattribute9 = 'M'
                AND txn2.genericattribute9 = 'M'
                AND txn.eventtypeid LIKE '%BroadBand Closed%' --for external only
                AND txn.orderid <> txn2.orderid
                --and in the same mth
                AND TO_VARCHAR(txn.compensationdate,'yyyymm') = TO_VARCHAR(txn2.compensationdate,'yyyymm')  /* ORIGSQL: to_Char(txn2.compensationdate,'yyyymm') */
                                                                                                            /* ORIGSQL: to_Char(txn.compensationdate,'yyyymm') */
                AND txn.genericattribute21 <> txn2.genericattribute21 --same vendor
                AND txn.genericattribute22 = 'AC'
                AND txn2.genericattribute22 = 'AC'

            GROUP BY
                txn.ponumber, txn.orderid
        ) AS src
        ON ((tgt.orderid = src.orderid
            OR tgt.orderid = src.newordid)
        AND tgt.ponumber = src.ponumber
        AND tgt.filename = :v_file_name
            AND tgt.filedate = :v_file_date
            AND tgt.recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET
            Comments =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Unceased due to Change Main Paln by another vendor'
                ELSE 'Ignore'
            END,
            tgt.genericboolean1 =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 0
                ELSE tgt.genericboolean1
            END
      ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and othe(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Merge on inbound_Data_Txn for OrderId and other attributes 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Merge on inbound_Data_Txn Completed-11', NULL, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Merge on inbound_Data_Txn for OrderId and other attribute(...) */

    -- Replaced with the Below Merge statement to get the Original Transaction details   
    /* ORIGSQL: update inbound_Data_Txn tgt SET genericboolean1=1, comments = 'Cessation Txn' wh(...) */
    UPDATE inbound_Data_Txn tgt
        SET
        /* ORIGSQL: genericboolean1 = */
        genericboolean1 = 1,
        /* ORIGSQL: comments = */
        comments = 'Cessation Txn' 
    WHERE
        recordstatus = 0
        AND tgt.filename = :v_file_name
        AND genericattribute22 = 'C';  

    /* ORIGSQL: update inbound_Data_Txn tgt SET genericboolean1=0, genericattribute22='AC' where(...) */
    UPDATE inbound_Data_Txn tgt
        SET
        /* ORIGSQL: genericboolean1 = */
        genericboolean1 = 0,
        /* ORIGSQL: genericattribute22 = */
        genericattribute22 = 'AC' 
    WHERE
        recordstatus = 0
        AND tgt.filename = :v_file_name
        AND genericattribute22 = 'C'
        AND numberofunits = -1;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update GB1' || :v_file_type || '-FileName(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update GB1'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update GB1 Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update GB1' || :v_file_type || '-FileName:' || v_prm(...) */

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO inbound_Data_Txn tgt USING (SELECT ceorderid, shiptoaddress3, accdate(...) */
    MERGE INTO inbound_Data_Txn AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (SELECT ceorderid, shiptoaddress3, accdate, ceasedate FROM (SELECT ceorderid, sh(...) */
                ceorderid,
                shiptoaddress3,
                accdate,
                ceasedate  
            FROM
                (
                    SELECT   /* ORIGSQL: (SELECT ceorderid, shiptoaddress3, accdate, ceasedate, ROW_NUMBER() OVER (PARTIT(...) */
                        ceorderid,
                        shiptoaddress3,
                        accdate,
                        ceasedate,
                        ROW_NUMBER() OVER (PARTITION BY ceorderid, accdate, ceasedate ORDER BY ceorderid, accdate, ceasedate) AS rn
                    FROM
                        stel_Temp_cessations s
                    WHERE
                        filename = :v_file_name
                ) AS dbmtk_corrname_5831
            WHERE
                rn = 1
        ) AS src
        ON (tgt.orderid = src.ceorderid
            AND tgt.accountingdate = src.accdate
            AND tgt.compensationdate = src.ceasedate
            AND tgt.recordstatus = 0
            AND tgt.filename = :v_file_name
            AND tgt.genericattribute22 = 'C'
        )
    WHEN MATCHED THEN
        UPDATE
            SET comments = 'Ceased Txn - '|| IFNULL(src.shiptoaddress3,'')
        ;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: MERGE INTO inbound_Data_Txn tgt USING (SELECT ceorderid, shiptoaddress3, accdate(...) */
    MERGE INTO inbound_Data_Txn AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (SELECT ceorderid, shiptoaddress3, accdate, ceasedate FROM (SELECT ceorderid, sh(...) */
                ceorderid,
                shiptoaddress3,
                accdate,
                ceasedate
                /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_CESSATIONSCE' not found */
            FROM
                (
                    SELECT   /* ORIGSQL: (SELECT ceorderid, shiptoaddress3, accdate, ceasedate, ROW_NUMBER() OVER (PARTIT(...) */
                        ceorderid,
                        shiptoaddress3,
                        accdate,
                        ceasedate,
                        ROW_NUMBER() OVER (PARTITION BY ceorderid, accdate, ceasedate ORDER BY ceorderid, accdate, ceasedate) AS rn
                    FROM
                        stel_Temp_cessationsCE s
                    WHERE
                        filename = :v_file_name
                ) AS dbmtk_corrname_5834
            WHERE
                rn = 1
        ) AS src
        ON (tgt.orderid = src.ceorderid
            AND tgt.accountingdate = src.accdate
        AND tgt.compensationdate = src.ceasedate
        AND tgt.recordstatus = 0
            AND tgt.filename = :v_file_name
            AND tgt.genericattribute22 = 'C'
        )
    WHEN MATCHED THEN
        UPDATE
            SET comments = 'Ceased Txn - '|| IFNULL(src.shiptoaddress3,'')
        ;

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update Comments' || :v_file_type || '-Fil(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Comments'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update Comments Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Comments' || :v_file_type || '-FileName:' || (...) */

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update ceased Order' || :v_file_type || '(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update ceased Order'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update ceased Order Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update ceased Order' || :v_file_type || '-FileName:'(...) */

    /* ORIGSQL: update inbound_data_assignment SET genericattribute6 = orderid||'-'||linenumber|(...) */
    UPDATE inbound_data_assignment
        SET
        /* ORIGSQL: genericattribute6 = */
        genericattribute6 = IFNULL(TO_VARCHAR(orderid),'')||'-'||IFNULL(TO_VARCHAR(linenumber),'')||'-'||IFNULL(TO_VARCHAR(sublinenumber),'')
    FROM
        inbound_data_assignment
    WHERE
        recordstatus = 0
        AND filename = :v_file_name;

    --and filedate= :v_file_date;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'GA6 Update in Assignment' || :v_file_type(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'GA6 Update in Assignment'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'GA6 Update in Assignment Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'GA6 Update in Assignment' || :v_file_type || '-FileN(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --for same file cessations
    --here we have to copy the asisgnment as well 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_assignment asign using (SELECT * FROM inbound_Data_txn w(...) */
    MERGE INTO inbound_data_assignment AS asign 
        USING
        (
            SELECT   /* ORIGSQL: (Select * from inbound_Data_txn where recordstatus=0 and filename=v_prmtr.file_n(...) */
                *
            FROM
                inbound_Data_txn
            WHERE
                recordstatus = 0
                AND filename = :v_file_name
                AND filedate = :v_file_date
                AND shiptoaddress3 <> IFNULL(TO_VARCHAR(orderid),'')||'-'||IFNULL(TO_VARCHAR(linenumber),'')||'-'||IFNULL(TO_VARCHAR(sublinenumber),'') --to match to CE 
        ) AS txn
        ON (asign.genericattribute6 = txn.shiptoaddress3
        	AND asign.recordstatus = 0
            AND asign.filename = asign.filename
        )
    WHEN MATCHED THEN
        UPDATE SET
            asign.orderid = txn.orderid,
            asign.linenumber = txn.linenumber,
            asign.sublinenumber = txn.sublinenumber,
            asign.positionname =
            CASE
                WHEN asign.genericattribute3 = 'Dealer'
                THEN txn.genericattribute4
                WHEN asign.genericattribute3 = 'Vendor'
                THEN txn.genericattribute3
                WHEN asign.genericattribute3 = 'Salesman'
                THEN txn.genericattribute2
            END
        ;
    --and asign.filedate=asign.filedate;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update Assignment Key' || :v_file_type ||(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Assignment Key'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update Assignment Key Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Assignment Key' || :v_file_type || '-FileName(...) */

    --for cessations for past files
    -- Removed remarks column from stel_Temp_cessations select by Sankar added below line to check april submit bb files   
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_assignment tgt using (WITH x AS (SELECT shiptoaddress3, (...) */
    MERGE INTO inbound_data_assignment AS tgt
        USING (
            --select shiptoaddress3, min(orderid) orderid, linenumber, sublinenumber, clawbackflag, ceasedate, accdate, unitsmultiplier
            --from stel_Temp_cessations tgt
            --where tgt.filename=:v_file_name
            --group by shiptoaddress3, linenumber, sublinenumber, clawbackflag, ceasedate, accdate, unitsmultiplier

            WITH 
            x 
            AS (
                SELECT   /* ORIGSQL: (select shiptoaddress3, MIN(orderid) orderid, linenumber, sublinenumber, clawbac(...) */
                    shiptoaddress3,
                    MIN(orderid) AS orderid,
                    linenumber,
                    sublinenumber,
                    clawbackflag,
                    ceasedate,
                    accdate,
                    unitsmultiplier
                FROM
                    stel_Temp_cessations tgt
                WHERE
                    tgt.filename = :v_file_name
                GROUP BY
                    shiptoaddress3, linenumber, sublinenumber, clawbackflag, ceasedate, accdate, unitsmultiplier
            )  
            SELECT   /* ORIGSQL: select shiptoaddress3, MIN(orderid) orderid, linenumber, sublinenumber, clawback(...) */
                shiptoaddress3,
                MIN(orderid) AS orderid,
                linenumber,
                sublinenumber,
                clawbackflag,
                ceasedate,
                accdate,
                unitsmultiplier
            FROM
                stel_Temp_cessations tgt
            WHERE
                tgt.filename = :v_file_name
                AND shiptoaddress3 NOT IN
                (
                    SELECT   /* ORIGSQL: (select shiptoaddress3 from x group by shiptoaddress3 having COUNT(shiptoaddress(...) */
                        shiptoaddress3
                    FROM
                        x
                    GROUP BY
                        shiptoaddress3
                    HAVING
                        COUNT(shiptoaddress3) > 1
                )
            GROUP BY
                shiptoaddress3, linenumber, sublinenumber, clawbackflag, ceasedate, accdate, unitsmultiplier

                --with x as (
                    --select shiptoaddress3, min(orderid) orderid, linenumber, sublinenumber, clawbackflag,remarks, ceasedate, accdate, unitsmultiplier
                    --from stel_Temp_cessations tgt
                    --where tgt.filename=:v_file_name--'stel_BCC-SCII-SubmittedBroadBandOrders_20210400000203.txt'
                    --group by shiptoaddress3, linenumber, sublinenumber, clawbackflag,remarks, ceasedate, accdate, unitsmultiplier
                --)

                --select  shiptoaddress3,  orderid, linenumber, sublinenumber, clawbackflag, max(ceasedate)ceasedate, accdate, unitsmultiplier
                --from stel_Temp_cessations tgt
                --where tgt.filename=:v_file_name--'stel_BCC-SCII-SubmittedBroadBandOrders_20210400000203.txt'
                -- and shiptoaddress3 not in  (select shiptoaddress3 from x group by shiptoaddress3 having count(shiptoaddress3)>1)
                --group by shiptoaddress3,orderid, linenumber, sublinenumber, clawbackflag,  accdate, unitsmultiplier

            ) src
            ON (tgt.genericattribute6 = src.shiptoaddress3
            	AND tgt.recordstatus = 0
            AND tgt.filename <> :v_file_name --and tgt.orderid not like '%CE'
            --and tgt.filedate=:v_file_date
            --and pick up the latest record for that STA3 ,
            -- normally there should only be one anyway, but in UAT ehre are many due to dupliicate loads
            AND tgt.filename
            /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_ASSIGNMENT' not found */
            =
            (
                SELECT   /* ORIGSQL: (Select MAX(x.filename) from inbound_Data_assignment x where x.genericattribute6(...) */
                    MAX(x.filename)
                FROM
                    inbound_Data_assignment x
                WHERE
                    x.genericattribute6 = tgt.genericattribute6
                    AND x.recordstatus = 0 --and x.orderid not like '%CE'
                    AND x.filename <> :v_file_name
            )
            )
    WHEN MATCHED THEN
        UPDATE
            SET tgt.orderid =
            CASE
                WHEN tgt.orderid LIKE '%CE'
                THEN tgt.orderid
                ELSE IFNULL(src.orderid,'')||
                CASE 
                    WHEN unitsmultiplier = -1
                    THEN 'CE'
                    ELSE ''
                END
            END
            ,tgt.linenumber = src.linenumber
            ,tgt.sublinenumber = src.sublinenumber
            ,tgt.filename = :v_file_name
            ,tgt.filedate = :v_file_date
            --, tgt.shiptoaddress3 = txnorderid||'-'||txnlinenumber||'-'||txnsublinenumber
        ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update Assignment Key 2' || :v_file_type (...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Assignment Key 2'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update Assignment Key Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Assignment Key 2' || :v_file_type || '-FileNa(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_data_assignment asign using (SELECT * FROM inbound_Data_txn w(...) */
    MERGE INTO inbound_data_assignment AS asign 
        USING
        (
            SELECT   /* ORIGSQL: (Select * from inbound_Data_txn where recordstatus=0 and filename=v_prmtr.file_n(...) */
                *
            FROM
                inbound_Data_txn
            WHERE
                recordstatus = 0
                AND filename = :v_file_name
                AND filedate = :v_file_date
                AND shiptoaddress3 <> IFNULL(TO_VARCHAR(orderid),'')||'-'||IFNULL(TO_VARCHAR(linenumber),'')||'-'||IFNULL(TO_VARCHAR(sublinenumber),'') --to match to oirignal 
        ) AS txn
        ON (asign.genericattribute6 = txn.shiptoaddress3
        	AND
            asign.recordstatus = 0
            AND asign.filename = asign.filename
            AND asign.orderid LIKE '%CE'
        )
    WHEN MATCHED THEN
        UPDATE SET
            asign.positionname =
            CASE
                WHEN asign.genericattribute3 = 'Dealer'
                OR asign.genericattribute1 = 'Dealer'
                THEN txn.genericattribute4
                WHEN asign.genericattribute3 = 'Vendor'
                OR asign.genericattribute1 = 'Vendor'
                THEN txn.genericattribute3
                WHEN asign.genericattribute3 = 'Salesman'
                OR asign.genericattribute1 = 'Salesman'
                THEN txn.genericattribute2
            END
        ;
    --and asign.filedate=asign.filedate;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update Assignment Posn' || :v_file_type |(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Assignment Posn'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update Assignment Key Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Assignment Posn' || :v_file_type || '-FileNam(...) */

    -- update txn Type to Recon   

    /* ORIGSQL: update inbound_Data_Txn SET genericattribute5='Recon' where nvl(trim(genericattr(...) */
    UPDATE inbound_Data_Txn
        SET
        /* ORIGSQL: genericattribute5 = */
        genericattribute5 = 'Recon' 
    FROM
        inbound_Data_Txn
    WHERE
        IFNULL(TRIM(genericattribute26),'0') = '1'   /* ORIGSQL: nvl(trim(genericattribute26),'0') */
        AND filedate = :v_file_date
        AND filename = :v_file_name
        AND recordstatus = 0;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update TXN TYPE to RECON' || :v_file_type(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update TXN TYPE to RECON'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update TXN TYPE to RECON Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update TXN TYPE to RECON' || :v_file_type || '-FileN(...) */

    -- update txn Type to Migration

    /* MOVED to SH
    merge into  inbound_Data_Txn tgt
    using (
        select t.orderid, t.linenumber, t.sublinenumber, t.eventtypeid
        from inbound_Data_Txn t
        join stel_Classifier@stelext p1
        on t.productid = p1.classifierid
        and p1.categorytreename = 'Singtel'
        and p1.categoryname = 'PRODUCTS'
        
        join stel_classifier@stelext p2
        on t.genericattribute25 = p2.classifierid --existing component id
        and p2.categorytreename ='Singtel'
        and p2.categoryname = 'PRODUCTS'
        
        where   filedate=:v_file_date
         and filename=:v_file_name
         and recordstatus=0
        and p1.genericattribute10 in ('FTTH','Fibre')
        and p2.genericattribute10 in ( 'ADSL','PSTN')
        and filename like '%Broad%'
    ) src
    on (tgt.orderid= src.orderid and tgt.linenumber=src.linenumber
    and tgt.sublinenumber=src.sublinenumber and  tgt.eventtypeid=src.eventtypeid)
    when matched then update
    set genericattribute5='Migration'
    where nvl(trim(genericattribute26),'0') = '1'
     and filedate=:v_file_date
     and filename=:v_file_name
     and recordstatus=0
     and filename like '%Broad%';
    
       v_rowcount := SQL%ROWCOUNT;
    
          STEL_SP_LOGGER (
                 SUBSTR (
                           v_proc_name
                        || 'Update TXN TYPE to MIGRATION'
                        || :v_file_type
                        || '-FileName:'
                        || :v_file_name
                        || '-Date:'
                        || :v_file_date,
                        1,
                    255),
                 'Update TXN TYPE to MIGRATION Execution Completed',
                 v_rowcount,
                 NULL,
             null);
    
    */

    /*
    
    Mobile Combo 3 and above change to SIM Only Plan with 12 months Contract
    Mobile Combo 1 / 2 change to SIM Only Plan with 12 months Contract
    Mobile Combo change to SIM Only Plan with No Contract
    */ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Txn tgt using (SELECT t.orderid, t.linenumber, t.subline(...) */
    MERGE INTO inbound_Data_Txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select t.orderid, t.linenumber, t.sublinenumber, t.eventtypeid from inbound_Dat(...) */
                t.orderid,
                t.linenumber,
                t.sublinenumber,
                t.eventtypeid
            FROM
                inbound_Data_Txn t
            INNER JOIN
                EXT.stel_classifier_tab p2
                ON t.genericattribute25 = p2.classifierid --existing component id
                AND p2.categorytreename = 'Singtel'
                AND p2.categoryname = 'PRODUCTS'

                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_classifier_tab@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_classifier_tab'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                filedate = :v_file_date
                AND filename = :v_file_name
                AND recordstatus = 0
                AND (UPPER(p2.classfiername) LIKE '%COMBO%')
                AND UPPER(t.shiptoaddress2) LIKE '%SIM%ONLY%' 
                AND filename LIKE '%Mobile%'
                AND t.shiptopostalcode = '0'
        ) AS src
        ON (tgt.orderid = src.orderid
            AND tgt.linenumber = src.linenumber
            AND tgt.sublinenumber = src.sublinenumber
        AND tgt.eventtypeid = src.eventtypeid
        AND filedate = :v_file_date
            AND filename = :v_file_name
            AND recordstatus = 0
            AND filename LIKE '%Mobile%'
        )
    WHEN MATCHED THEN
        UPDATE
            SET genericattribute15 = 'Mobile Combo change to SIM Only Plan with No Contract'
        ;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Txn tgt using (SELECT t.orderid, t.linenumber, t.subline(...) */
    MERGE INTO inbound_Data_Txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select t.orderid, t.linenumber, t.sublinenumber, t.eventtypeid from inbound_Dat(...) */
                t.orderid,
                t.linenumber,
                t.sublinenumber,
                t.eventtypeid
            FROM
                inbound_Data_Txn t
            INNER JOIN
                EXT.stel_classifier_tab p2
                ON t.genericattribute25 = p2.classifierid --existing component id
                AND p2.categorytreename = 'Singtel'
                AND p2.categoryname = 'PRODUCTS'

                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_classifier_tab@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_classifier_tab'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                filedate = :v_file_date
                AND filename = :v_file_name
                AND recordstatus = 0
                AND (UPPER(p2.classfiername) LIKE '%COMBO%3%' 
                    OR UPPER(p2.classfiername) LIKE '%COMBO%4%' 
                    OR UPPER(p2.classfiername) LIKE '%COMBO%5%' 
                    OR UPPER(p2.classfiername) LIKE '%COMBO%6%')
                AND UPPER(t.shiptoaddress2) LIKE '%SIM%ONLY%' 
                AND filename LIKE '%Mobile%'
                AND t.shiptopostalcode = '12'
        ) AS src
        ON (tgt.orderid = src.orderid
            AND tgt.linenumber = src.linenumber
            AND tgt.sublinenumber = src.sublinenumber
        AND tgt.eventtypeid = src.eventtypeid
        AND filedate = :v_file_date
            AND filename = :v_file_name
            AND recordstatus = 0
            AND filename LIKE '%Mobile%'
        )
    WHEN MATCHED THEN
        UPDATE
            SET genericattribute15 = 'Mobile Combo 3 and above change to SIM Only Plan with 12 months Contract'
        ;

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Txn tgt using (SELECT t.orderid, t.linenumber, t.subline(...) */
    MERGE INTO inbound_Data_Txn AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select t.orderid, t.linenumber, t.sublinenumber, t.eventtypeid from inbound_Dat(...) */
                t.orderid,
                t.linenumber,
                t.sublinenumber,
                t.eventtypeid
            FROM
                inbound_Data_Txn t

                --join stel_classifier@stelext p2 --[arun 26th nov 2019 - commented to improve perfomrance with a table]
            INNER JOIN
                EXT.stel_classifier_tab p2
                --[arun 26th nov 2019 - commented to improve perfomrance with a table]
                ON t.genericattribute25 = p2.classifierid --existing component id
                AND p2.categorytreename = 'Singtel'
                AND p2.categoryname = 'PRODUCTS'

                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_classifier_tab@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_classifier_tab'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                filedate = :v_file_date
                AND filename = :v_file_name
                AND recordstatus = 0
                AND (UPPER(p2.classfiername) LIKE '%COMBO%1%' 
                    OR UPPER(p2.classfiername) LIKE '%COMBO%2%')
                AND UPPER(t.shiptoaddress2) LIKE '%SIM%ONLY%' 
                AND filename LIKE '%Mobile%'
                AND t.shiptopostalcode = '12'
        ) AS src
        ON (tgt.orderid = src.orderid
            AND tgt.linenumber = src.linenumber
            AND tgt.sublinenumber = src.sublinenumber
        AND tgt.eventtypeid = src.eventtypeid
        AND filedate = :v_file_date
            AND filename = :v_file_name
            AND recordstatus = 0
            AND filename LIKE '%Mobile%'
        )
    WHEN MATCHED THEN
        UPDATE
            SET genericattribute15 = 'Mobile Combo 1 / 2 change to SIM Only Plan with 12 months Contract'
      ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: STEL_SP_LOGGER (SUBSTR(v_proc_name || 'Update GA15 for Mobile scenarios' || v_prmtr.f(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update GA15 for Mobile scenarios'|| IFNULL(:v_file_type,'') || '-FileName:'|| IFNULL(:v_file_name,'') || '-Date:'|| IFNULL(:v_file_date,''),1,255) 
        , 'Update TXN TYPE to MIGRATION Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update GA15 for Mobile scenarios' || :v_file_type ||(...) */

    /*
    
    
    merge into inbound_Data_Assignment tgt
    using (
        select orderid, linenumber, sublinenumber, positionname, max(rowid) row_id from inbound_Data_Assignment
        where recordstatus=0 and filename=:v_file_name
        group by orderid, linenumber, sublinenumber, positionname
        having count(*)>1 ) src
    on (tgt.rowid<>src.row_id and tgt.orderid=src.orderid and tgt.linenumber=src.linenumber
    and tgt.sublinenumber=src.sublinenumber and tgt.positionname=src.positionname)
    when matched then update set recordstatus=-9
    where recordstatus=0 and filename=:v_file_name;
    
    
    --temporary hack to handle UAT files with more than 30 days data across months
    merge into inbound_Data_txn tgt
    using (
        select orderid, linenumber, sublinenumber,  max(rowid) row_id from inbound_Data_txn
        where recordstatus=0 and filename=:v_file_name
        group by orderid, linenumber, sublinenumber
        having count(*)>1 ) src
    on (tgt.rowid<>src.row_id and tgt.orderid=src.orderid and tgt.linenumber=src.linenumber
    and tgt.sublinenumber=src.sublinenumber )
    when matched then update set recordstatus=-9
    where recordstatus=0 and filename=:v_file_name;
    */

    /* ORIGSQL: commit; */
    COMMIT;

    -- Update GA7 to the correct Product category
    -- can be done in classify
    /*
    Mobile RES
    TV Apps
    FTTH
    TV
    DASH
    MBB
    MBB
    Mobile CIS
    FTTH
    Home VAS
    Mobile RES
    Mobile CIS
    Mobile VAS
    SNBB VAS
    */

    --For internal aggregation per product category for DigiTelesales etc.
    IF :v_file_type LIKE 'BCC%SCII%Submit%' 
    THEN
        /* move to stagehook after classify
        select
        from cs_Salestransaction
        group by genericattribute7
        */

        /* ORIGSQL: NULL; */
        DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */
    END IF; 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_Assignment a using (SELECT ORDERID, LINENUMBER, SUBLINEN(...) */
    MERGE INTO inbound_Data_Assignment AS a 
        USING
        (
            SELECT   /* ORIGSQL: (select ORDERID,LINENUMBER,SUBLINENUMBER,EVENTTYPEID, positionname, MAX(CONV_SQL(...) */
                ORDERID,
                LINENUMBER,
                SUBLINENUMBER,
                EVENTTYPEID,
                positionname,
                MAX("$rowid$") AS rid  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                                     /* ORIGSQL: rowid */
            FROM
                inbound_Data_assignment
            WHERE
                recordstatus = 0
                AND filename = :v_file_name
                AND filedate = :v_file_date --[arun added to improve perf 26th nov 2019]
            GROUP BY
                ORDERID,LINENUMBER,SUBLINENUMBER,EVENTTYPEID, positionname
        ) AS b
        ON (a.orderid = b.orderid
            AND a.linenumber = b.linenumber
            AND a.sublinenumber = b.sublinenumber
            AND a.eventtypeid = b.eventtypeid
            AND a.positionname = b.positionname
            AND a."$rowid$"  <> b.rid
        	AND a.filename = :v_file_name
            AND filedate = :v_file_date
        )  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                                      /* ORIGSQL: a.rowid */
    WHEN MATCHED THEN
        UPDATE SET a.recordstatus = -7
        ;--[arun added to improve perf 26th nov 2019]

    /* ORIGSQL: commit; */
    COMMIT;

    --workarounds for possible data issue in UAT 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into inbound_Data_txn a using (SELECT ORDERID, LINENUMBER, SUBLINENUMBER, (...) */
    MERGE INTO inbound_Data_txn AS a 
        USING
        (
            SELECT   /* ORIGSQL: (select ORDERID,LINENUMBER,SUBLINENUMBER,EVENTTYPEID, MAX(rowid) (...) */
                ORDERID,
                LINENUMBER,
                SUBLINENUMBER,
                EVENTTYPEID,
                MAX("$rowid$") AS rid  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                                     /* ORIGSQL: rowid */
            FROM
                inbound_Data_txn
            WHERE
                recordstatus = 0
                AND filename = :v_file_name
                AND filedate = :v_file_date --[arun added to improve perf 26th nov 2019]
            GROUP BY
                ORDERID,LINENUMBER,SUBLINENUMBER,EVENTTYPEID
        ) AS b
        ON (a.orderid = b.orderid
            AND a.linenumber = b.linenumber
            AND a.sublinenumber = b.sublinenumber
            AND a.eventtypeid = b.eventtypeid --and a.positionname=b.positionname
            AND a."$rowid$"  <> b.rid
        	AND
            a.filename = :v_file_name
            AND filedate = :v_file_date
            AND a.filename = :v_file_name
            AND filedate = :v_file_date
        )  /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'rowid' (=reserved word in HANA) renamed to '"rowid"'; ensure all calls/references are renamed accordingly */
                                      /* ORIGSQL: a.rowid */
    WHEN MATCHED THEN
        UPDATE SET a.recordstatus = -7
        ;--[arun added to improve perf 26th nov 2019]

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXCEPTION WHEN OTHERS THEN */
END
