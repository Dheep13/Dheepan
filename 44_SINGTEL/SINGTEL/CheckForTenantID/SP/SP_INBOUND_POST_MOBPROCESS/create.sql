CREATE PROCEDURE EXT.SP_INBOUND_POST_MOBPROCESS
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Sql VARCHAR(4000);  /* ORIGSQL: v_Sql varchar2(4000); */
    DECLARE v_Singlequote VARCHAR(1) = '''';  /* ORIGSQL: v_Singlequote varchar2(1):=''''; */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:=null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_MOBPROCESS';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_MOBPROCESS'; */

    DECLARE v_param ROW LIKE EXT.INBOUND_CFG_PARAMETER ;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_param
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    /* ORIGSQL: execute immediate '  alter session set nls_Date_Format = ''DD-MON-YYYY''' ; */
    /* ORIGSQL: alter session set nls_Date_Format = 'DD-MON-YYYY' ; */
    -- CALL sapdbmtk.sp_dbmtk_set_option_session('datetime_to_string_format', 'DD-MON-YYYY');

    /* ORIGSQL: execute immediate 'Truncate table LP_STEL_CLASSIFIER drop storage' ; */
    /* RESOLVE: Identifier not found: Table/view 'EXT.LP_STEL_CLASSIFIER' not found */

    /* ORIGSQL: Truncate table LP_STEL_CLASSIFIER drop storage ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.LP_STEL_CLASSIFIER';

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'LP_STEL_CLASSIFIER BEFORE :' || v_param.file_t(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(:v_proc_name || 'LP_STEL_CLASSIFIER BEFORE :'|| :v_param.file_type || '-FileName:'|| :v_param.file_name || '-Date:'|| :v_param.file_date,1,255) 
        , 'LP_STEL_CLASSIFIER BEFORE', 0, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'LP_STEL_CLASSIFIER BEFORE :' || v_param.file_type || '-Fi(...) */

    /* ORIGSQL: INSERT INTO LP_STEL_CLASSIFIER select * from STEL_CLASSIFIER@STELEXT where categ(...) */
    INSERT INTO EXT.LP_STEL_CLASSIFIER
        SELECT   /* ORIGSQL: select * from STEL_CLASSIFIER@STELEXT where categorytreename in ('StockCode','Si(...) */
            *
        FROM
            EXT.STEL_CLASSIFIER
            /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_CLASSIFIER@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_CLASSIFIER'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            categorytreename IN ('StockCode','Singtel');

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'LP_STEL_CLASSIFIER CREATION COMPLETED :' || v_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'LP_STEL_CLASSIFIER CREATION COMPLETED :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'LP_STEL_CLASSIFIER CREATION COMPLETED', 0, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'LP_STEL_CLASSIFIER CREATION COMPLETED :' || v_param.file_(...) */

    /* ORIGSQL: dbms_stats.gather_table_stats(ownname =>'STELADMIN', tabname => 'LP_STEL_CLASSIF(...) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'STELADMIN'|| '.'|| 'EXT.LP_STEL_CLASSIFIER';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */
    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'LP_STEL_CLASSIFIER STATS COMPLETED :' || v_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'LP_STEL_CLASSIFIER STATS COMPLETED :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'LP_STEL_CLASSIFIER STATS COMPLETED', 0, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'LP_STEL_CLASSIFIER STATS COMPLETED :' || v_param.file_typ(...) */

    --customer Id IS NULL, still load in but set comment
    --20190221: leave this here. no need to move to SH. Does not affect comp.   
    /* ORIGSQL: update stel_data_txn_mobile SET comments='Customer ID Missing' where filename = (...) */
    UPDATE EXT.stel_data_txn_mobile
        SET
        /* ORIGSQL: comments = */
        comments = 'Customer ID Missing' 
    FROM
        EXT.stel_data_txn_mobile
    WHERE
        filename = :v_param.file_name
        AND filedate = :v_param.file_Date
        AND recordstatus = 0
        AND billtocustid IS NULL;/* --and channel='SERS' */

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Comments with Customer Missing :' || v_(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(:v_proc_name || 'Update Comments with Customer Missing :'|| :v_param.file_type || '-FileName:'|| :v_param.file_name || '-Date:'|| :v_param.file_date ,1,255) 
        , 'Update Comments with Customer Missing Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Comments with Customer Missing :' || v_param.file_(...) */

    --SERS Roadshow identification

    /* List of roadhsow dealer codes:
    select distinct classifierid from stel_classifier@stelext
    where categorytreename='Roadshow Codes' AND CATEOGRYNAME='Roadshow'
    
    We can replace this with a relnship?
    */
    --Dealer Code from Roadshow
    --20190221: Move to SH 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_txn_mobile tgt using (SELECT classifierid AS RdCode, a.Gene(...) */
    MERGE INTO stel_data_txn_mobile AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select classifierid RdCode, a.Genericattribute2 dlrcode, a.effectivestartdate, (...) */
                classifierid AS RdCode,
                a.Genericattribute2 AS dlrcode,
                a.effectivestartdate,
                a.effectiveenddate,
                mgr.name AS VendorCode
            FROM
                EXT.stel_Classifier a
            INNER JOIN
                cs_position pos
                ON pos.name = a.genericattribute2
                AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND add_days(a.effectiveenddate,-1) BETWEEN pos.effectivestartdate AND add_days(pos.effectiveenddate,-1)
            INNER JOIN
                cs_position mgr
                ON pos.managerseq = mgr.ruleelementownerseq
                AND mgr.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND add_days(a.effectiveenddate,-1) BETWEEN mgr.effectivestartdate AND add_days(mgr.effectiveenddate,-1)
                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_Classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_Classifier'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                categorytreename = 'Roadshow Codes'
                AND CATEGoRYNAME = 'Roadshow'
        ) AS src
        ON (tgt.compensationdate BETWEEN src.effectivestartdate AND add_days(src.effectiveenddate,-1)
        AND tgt.genericattribute4 = src.rdCode AND
            filename = :v_param.file_name
            AND filedate = :v_param.file_Date
            AND recordstatus = 0)
    WHEN MATCHED THEN
        UPDATE SET tgt.tempfield1 = src.dlrCode, tgt.tempfield2 = src.VendorCode;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update DealerCode and VenderCode :' || v_param(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(:v_proc_name || 'Update DealerCode and VenderCode :'|| :v_param.file_type || '-FileName:'|| :v_param.file_name || '-Date:'|| :v_param.file_date ,1,255) 
        , 'Update DealerCode and VenderCode Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update DealerCode and VenderCode :' || v_param.file_type (...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Dealer Code from SalesPerson (Primary Dealer ID - GA1)
    --if ga1 = tempfield1, AI and MSF go to the same shop. (set assignment to be tempfield1 above)
    -- if ga1<>tempfield1, MSF goes to tempfield1 shop, AI goes to GA1 salesperson (assignment is still to dealer code tempfield1
        -- another assignment to be created for
        --Chane plan as new line
        -- this whole section can go into the post_bcctxn proc
        --on the same day (in the same file)

        --po number has to run first
        --20190221: This is already in RFC SH. Can be removed here? 
        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into stel_data_txn_mobile tgt using (SELECT txn.ponumber, MAX(txn2.orderid(...) */
        MERGE INTO stel_data_txn_mobile AS tgt
            /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_TXN_MOBILE' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select txn.ponumber, MAX(txn2.orderid) as NewOrdId, txn.orderid from stel_data_(...) */
                    txn.ponumber,
                    MAX(txn2.orderid) AS NewOrdId,
                    txn.orderid
                FROM
                    stel_data_txn_mobile txn
                INNER JOIN
                    stel_data_txn_mobile txn2
                    ON txn2.genericattribute11 = 'Change Main Plan'
                    AND txn.ponumber = txn2.ponumber
                WHERE
                    txn.filename = :v_param.file_name
                    AND txn.filedate = :v_param.file_Date
                    AND txn.recordstatus = 0
                    AND txn2.filename = :v_param.file_name
                    AND txn2.filedate = :v_param.file_Date
                    AND txn2.recordstatus = 0
                    AND txn.genericattribute11 = 'New'
                    AND txn.genericattribute9 = 'M'
                    AND txn2.genericattribute9 = 'M'
                GROUP BY
                    txn.ponumber, txn.orderid
            ) AS src
            ON ((tgt.orderid = src.orderid
                OR tgt.orderid = src.newordid)
            AND tgt.ponumber = src.ponumber
            AND tgt.filename = :v_param.file_name
            AND tgt.filedate = :v_param.file_Date
            AND tgt.recordstatus = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            tempfield3 =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Replaced with Change Plan'
                ELSE 'Treat as New Plan'
            END
        ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Change plan as new line Same Day :' || v_param(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Change plan as new line Same Day :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Change plan as new line same day Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Change plan as new line Same Day :' || v_param.file_type (...) */

    --20190221: This is already in SH. Can be removed?
    -- on a diff day (previoulsy loaded into TC) 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_txn_mobile tgt using (SELECT txn.ponumber, MAX(txn2.orderid(...) */
    MERGE INTO stel_data_txn_mobile AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select txn.ponumber, MAX(txn2.orderid) as NewOrdId, txn.orderid from vw_Salestr(...) */
                txn.ponumber,
                MAX(txn2.orderid) AS NewOrdId,
                txn.orderid
            FROM
                EXT.vw_Salestransaction txn
            INNER JOIN
                stel_data_txn_mobile txn2
                ON txn2.genericattribute11 = 'Change Main Plan'
                AND txn.ponumber = txn2.ponumber
                /* RESOLVE: Oracle Database link: Remote table/view 'EXT.vw_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.vw_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                txn2.filename = :v_param.file_name
                AND txn2.filedate = :v_param.file_Date
                AND txn2.recordstatus = 0
                AND txn.genericattribute11 = 'New'
                AND txn.genericattribute9 = 'M'
                AND txn2.genericattribute9 = 'M'
            GROUP BY
                txn.ponumber, txn.orderid
        ) AS src
        ON ((tgt.orderid = src.orderid
            OR tgt.orderid = src.newordid)
        AND tgt.ponumber = src.ponumber and 
            tgt.filename = :v_param.file_name
            AND tgt.filedate = :v_param.file_Date
            AND tgt.recordstatus = 0)
    WHEN MATCHED THEN
        UPDATE SET
            tempfield3 =
            CASE
                WHEN tgt.orderid = src.orderid
                THEN 'Replaced with Change Plan'
                ELSE 'Treat as New Plan'
            END
       ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Change plan on diff day :' || v_param.file_typ(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Change plan on diff day :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Change plan on diff day Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Change plan on diff day :' || v_param.file_type || '-File(...) */

    -- Cancelled/Ceased orders

    --mbb classification
    --20190221: Already in SH

    --mbb rejections
    --20190221: Move to SH for SER Event types 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_txn_mobile tgt using (SELECT txn.orderid, txn.linenumber, t(...) */
    MERGE INTO stel_data_txn_mobile AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select txn.orderid, txn.linenumber, txn.sublinenumber from stel_data_txn_mobile(...) */
                txn.orderid,
                txn.linenumber,
                txn.sublinenumber
            FROM
                stel_data_txn_mobile txn 
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select s.*, s.genericattribute4 brand, s.genericattribute1 dept, s.genericattri(...) */
                        s.*,
                        s.genericattribute4 AS brand,
                        s.genericattribute1 AS dept,
                        s.genericattribute3 AS mat
                    /* --from stel_Classifier@stelext  s) stock --COMMENTED THIS LINE and added below for performance sankar */
                FROM
                    EXT.LP_STEL_CLASSIFIER s
            ) AS stock
            ON txn.genericattribute28 = stock.classifierid
            AND stock.categorytreename = 'StockCode'
            AND stock.categoryname = 'PRODUCTS'
            AND txn.compensationdate BETWEEN stock.effectivestartdate AND add_days(stock.effectiveenddate,-1)
            --join (select s.* from stel_Classifier@stelext  s) prod --COMMENTED THIS LINE and added below for performance sankar 
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select s.* from LP_STEL_CLASSIFIER s) */
                    s.*
                FROM
                    EXT.LP_STEL_CLASSIFIER s
            ) AS prod
            ON prod.categorytreename = 'Singtel'
            AND prod.categoryname = 'PRODUCTS'
            AND txn.productid = prod.classifierid
        WHERE
            1 = 1
            AND stock.dept NOT LIKE '%Dongle%'
            AND txn.genericattribute14 = 'MBB'
            AND txn.shiptopostalcode = 0
    ) AS src
    ON (tgt.orderid = src.orderid
        AND tgt.linenumber = src.linenumber
    AND tgt.sublinenumber = src.sublinenumber
    AND tgt.filename = :v_param.file_name
            AND tgt.filedate = :v_param.file_Date
            AND tgt.recordstatus = 0)
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute15 = 'MBB Rejection - No Tie in except Dongle'
        ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update MBB Rejections :' || v_param.file_type (...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update MBB Rejections :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update MBB Rejections Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update MBB Rejections :' || v_param.file_type || '-FileNa(...) */

    --20190221: Move to SH if its still needed commented by sankar and uncommented again to process regular files its verified 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_txn_mobile tgt using (SELECT distinct txn.orderid, txn.line(...) */
    MERGE INTO stel_data_txn_mobile AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select distinct txn.orderid, txn.linenumber, txn.sublinenumber from stel_data_t(...) */
                DISTINCT
                txn.orderid,
                txn.linenumber,
                txn.sublinenumber
            FROM
                stel_data_txn_mobile txn 
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select s.*, s.genericattribute4 brand, s.genericattribute1 dept, s.genericattri(...) */
                        s.*,
                        s.genericattribute4 AS brand,
                        s.genericattribute1 AS dept,
                        s.genericattribute3 AS mat
                    /* -- from stel_Classifier@stelext  s) stock */
                FROM
                    EXT.LP_STEL_CLASSIFIER s
            ) AS stock
            ON txn.genericattribute28 = stock.classifierid
            AND stock.categorytreename = 'StockCode'
            AND stock.categoryname = 'PRODUCTS'
            AND txn.compensationdate BETWEEN stock.effectivestartdate AND add_days(stock.effectiveenddate,-1)
            --join (select s.* from stel_Classifier@stelext  s) prod 
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select s.* from LP_STEL_CLASSIFIER s) */
                    s.*
                FROM
                    EXT.LP_STEL_CLASSIFIER s
            ) AS prod
            ON prod.categorytreename = 'Singtel'
            AND prod.categoryname = 'PRODUCTS'
            AND txn.productid = prod.classifierid
        WHERE
            1 = 1 --and txn.orderid in ( '197847073A197847074A185187412626262719' ,'216502916A216502917A103908512678969569')
            AND txn.genericattribute14 = 'MBB'
            AND IFNULL(txn.genericattribute8, '0') = '0'   /* ORIGSQL: nvl(txn.genericattribute8,'0') */
    ) AS src
    ON (tgt.orderid = src.orderid
        AND tgt.linenumber = src.linenumber
    AND tgt.sublinenumber = src.sublinenumber
    AND tgt.filename = :v_param.file_name
            AND tgt.filedate = :v_param.file_Date
            AND tgt.recordstatus = 0)
    WHEN MATCHED THEN
        UPDATE SET genericattribute15 = 'MBB Rejection - No Capacity'
        ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update MBB Rejections - No Capacity:' || v_par(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update MBB Rejections - No Capacity:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update MBB Rejections - No CapacityExecution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update MBB Rejections - No Capacity:' || :v_param.file_typ(...) */

    --20190221: Move to SH from DEV environment
    --smae month dup check
    ----same file check

    --loaded into tc check

    --cross mth dup check

    --cross mth CI check

    --MSF eligibility

    --IMEI check Singtel

    ---IMEI check Usage
    ---VSOP file may come in late, can move this to stagehook?
    --20190221: Move to SH   

    /* ORIGSQL: update stel_data_txn_mobile tgt SET tempfield4 = 'IMEI not Found in VSOP' where (...) */
    UPDATE stel_data_txn_mobile tgt
        SET
        /* ORIGSQL: tempfield4 = */
        tempfield4 = 'IMEI not Found in VSOP' 
    WHERE
        billtopostalcode NOT
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_VSOP' not found */
        IN
        (
            SELECT   /* ORIGSQL: (select to_char(chimei) from stel_Data_vsop) */
                TO_VARCHAR(chimei)
            FROM
                stel_Data_vsop
        )
        AND tgt.filename = :v_param.file_name
        AND tgt.filedate = :v_param.file_Date
        AND tgt.recordstatus = 0;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update IMEI not Found in VSOP:' || :v_param.fil(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update IMEI not Found in VSOP:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update IMEI not Found in VSOP Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update IMEI not Found in VSOP:' || :v_param.file_type || '(...) */

    /* ORIGSQL: update stel_data_txn_mobile tgt SET tempfield4 = 'IMEI used before' where billto(...) */
    UPDATE stel_data_txn_mobile tgt
        SET
        /* ORIGSQL: tempfield4 = */
        tempfield4 = 'IMEI used before' 
    WHERE
        billtopostalcode IS NOT NULL /* -- --[Arun - Added on 7th Sep 2019 as Mobile Submitted was failing] */
        AND billtopostalcode
        /* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_USEDIMEI' not found */
        IN
        (
            SELECT   /* ORIGSQL: (select distinct to_char(imei) from stel_Data_usedimei where (IMEI IS NOT NULL A(...) */
                DISTINCT   
                TO_VARCHAR(imei)
            FROM
                stel_Data_usedimei
            WHERE
                (IMEI IS NOT NULL
                AND SERVICENO IS NOT NULL) --[Arun - Added on 7th Sep 2019 as Mobile Submitted was failing]
                AND (IFNULL(customerid,'')||'-'||IFNULL(serviceno,'') <> IFNULL(tgt.billtocustid,'')||'-'||IFNULL(tgt.billtocontact,''))
                OR customerid = 'USED'
        )
        AND tgt.filename = :v_param.file_name
        AND tgt.filedate = :v_param.file_Date
        AND tgt.recordstatus = 0;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update IMEI used before:' || :v_param.file_type(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update IMEI used before:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update IMEI used before Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update IMEI used before:' || :v_param.file_type || '-FileN(...) */

    /* ORIGSQL: insert into stel_Data_usedimei select distinct billtopostalcode, compensationdat(...) */
    INSERT INTO stel_Data_usedimei
        SELECT   /* ORIGSQL: select distinct billtopostalcode, compensationdate, billtocustid, billtocontact (...) */
            DISTINCT
            billtopostalcode,
            compensationdate,
            billtocustid,
            billtocontact
        FROM
            stel_data_txn_mobile txn
        WHERE
            txn.filename = :v_param.file_name
            AND txn.filedate = :v_param.file_Date
            AND txn.recordstatus = 0
EXCEPT

    /* ORIGSQL: minus */
    SELECT   /* ORIGSQL: select to_char(imei), compdate, customerid, serviceno from stel_Data_usedimei; */
        TO_VARCHAR(imei),
        compdate,
        customerid,
        serviceno
    FROM
        stel_Data_usedimei;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into stel_Data_usedimei table:' || v_pa(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Insert into stel_Data_usedimei table:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Insert into stel_Data_usedimei table Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Insert into stel_Data_usedimei table:' || :v_param.file_ty(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --SAP protection period per model
    /*
    txn model - get from stock
    txn sap customer code - participant ga12
    
    
    1.    Join the BCC order to SAP based on
    -     BCC Model (txn.ga28). = SAP Model (sap.stockcode)
    -    BCC Vendor Code = Callidus Profile Vendor Code (par.ga12) and SAP Customer Code = Callidus Profile Customer Code
    -    BCC Order Entry Date between SAP Start Date and End Date
    
    If the BCC Order Entry Date is more than 28 days after the End Date of the Transfer Cost in SAP, then the model is not in the protection period.
    Else, the model is price protected.
    */

    --20190221: Move to SH
    --[arun commented this block as this is moved to SH SER Mobile - STARTTTTT]
    /*
    merge into  stel_data_txn_mobile tgt
    using (select
        case when txn.accountingdate - sap.startdate <=28 then 'Y' else 'N' end as protectionflag
        ,orderid, linenumber, sublinenumber
        ,sap.txnprice, sap.costprice
        from  stel_data_txn_mobile txn
        join stel_data_transfercost sap
        on sap.stockcode=txn.genericattribute28
        and txn.accountingdate between sap.startdate and nvl(sap.enddate,to_date('22000101','YYYYMMDD'))-1
        join stel_participant@stelext par
        on par.payeeid=txn.genericattribute3
        and par.genericattribute12 = sap.customer
        and txn.accountingdate between par.effectivestartdate and  par.effectiveenddate-1
        where txn.filename = :v_param.file_name and txn.filedate=:v_param.file_Date and txn.recordstatus=0
    ) src
    on (Src.orderid=tgt.orderid and src.linenumber=tgt.linenumber and src.sublinenumber=tgt.sublinenumber)
    when matched then update set
    tgt.protectionflag=src.protectionflag,
    tgt.saptxprice=txnprice,
    tgt.sapcostprice=costprice
    
    where  tgt.filename = :v_param.file_name and tgt.filedate=:v_param.file_Date and tgt.recordstatus=0;
    
            v_rowcount := SQL%ROWCOUNT;
    
              SP_LOGGER (
                 SUBSTR (
                           v_proc_name
                        || 'Update ProtectionFlag SAP taxPrice and SAP CostPrice:'
                        || :v_param.file_type
                        || '-FileName:'
                        || :v_param.file_name
                        || '-Date:'
                        || :v_param.file_date,
                        1,
                    255),
                 'Update ProtectionFlag SAP taxPrice and SAP CostPrice Execution Completed',
                 v_rowcount,
                 NULL,
             null);
    
     commit;
    
    */

    --[arun commented this block as this is moved to SH SER Mobile - ENDDD]

    /*
    If model is price protected:
    -    Get the VSOP transfer cost and stamp it on the transaction
    If model is not cost protected (any more)
    -    Get the latest SAP Price (irrespective of the actual transfer cost that is in VSOP), since the protection has ended, and stamp it on the transaction
    */ 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_txn_mobile tgt using (SELECT distinct txn.orderid, txn.line(...) */
    MERGE INTO stel_data_txn_mobile AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (select distinct txn.orderid, txn.linenumber, txn.sublinenumber, vsop.transferco(...) */
                DISTINCT
                txn.orderid,
                txn.linenumber,
                txn.sublinenumber,
                vsop.transfercost
            FROM
                stel_data_txn_mobile txn
            INNER JOIN
                stel_Data_vsop vsop
                ON txn.billtopostalcode =TO_VARCHAR(vsop.chimei) --Arun[26th Apr 19] Added to_char to chimei as datatype was number and proc was failing --assuming only this join is needed
                /* ORIGSQL: to_char(vsop.chimei) */
            WHERE
                IFNULL(txn.imeimatch,'Y') = 'Y'   /* ORIGSQL: nvl(txn.imeimatch,'Y') */
                AND txn.filename = :v_param.file_name
                AND txn.filedate = :v_param.file_Date
                AND txn.recordstatus = 0
                AND vsop.transfercost > 0 --[Arun 5th Sep 2019 - Added this condition as the proc is failing]
        ) AS src
        ON (Src.orderid = tgt.orderid
            AND src.linenumber = tgt.linenumber
        AND src.sublinenumber = tgt.sublinenumber 
        AND tgt.filename = :v_param.file_name
            AND tgt.filedate = :v_param.file_Date
            AND tgt.recordstatus = 0)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.vsopcost = src.transfercost
        ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update vsopcost:' || :v_param.file_type || '-Fi(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update vsopcost:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update vsopcost Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update vsopcost:' || :v_param.file_type || '-FileName:' ||(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    /*
    Note that for IMEI replacements, the transfer price will not be in VSOP, as no transaction come in through VSOP
    in this case, the SAP price will be used, using the Order Entry Date to look up the price.
    */   

    /* ORIGSQL: update stel_data_txn_mobile tgt SET finalCost = CASE WHEN protectionflag='Y' THE(...) */
    UPDATE stel_data_txn_mobile tgt
        SET
        /* ORIGSQL: finalCost = */
        finalCost =
        CASE 
            WHEN protectionflag = 'Y'
            THEN IFNULL(vsopcost,saptxprice)  /* ORIGSQL: nvl(vsopcost,saptxprice) */
            WHEN protectionflag = 'N'
            THEN saptxprice
            ELSE 0
        END
    WHERE
        tgt.filename = :v_param.file_name
        AND tgt.filedate = :v_param.file_Date
        AND tgt.recordstatus = 0
        AND tgt.channel = 'SER';

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update FinalCost for SER:' || :v_param.file_typ(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update FinalCost for SER:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update FinalCost Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update FinalCost for SER:' || :v_param.file_type || '-File(...) */

    /* ORIGSQL: commit; */
    COMMIT;  

    /* ORIGSQL: update stel_data_txn_mobile tgt SET finalCost = sapcostprice where tgt.filename (...) */
    UPDATE stel_data_txn_mobile tgt
        SET
        /* ORIGSQL: finalCost = */
        finalCost = sapcostprice
    WHERE
        tgt.filename = :v_param.file_name
        AND tgt.filedate = :v_param.file_Date
        AND tgt.recordstatus = 0
        AND tgt.channel = 'TEPL';

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update FinalCost for TEPL:' || :v_param.file_ty(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update FinalCost for TEPL:'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update FinalCost TEPL Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update FinalCost for TEPL:' || :v_param.file_type || '-Fil(...) */

    -------------------[Arun SER INDICATOR Fix done on 11th Mar 2019 from this line till the comment below 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into STEL_DATA_TXN_MOBILE tgt using (SELECT st.seq AS seq, lt.channel, lt.(...) */
    MERGE INTO STEL_DATA_TXN_MOBILE AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select st.seq seq,lt.channel, lt.dealer from STEL_DATA_TXN_MOBILE st join (SELE(...) */
                st.seq AS seq,
                lt.channel,
                lt.dealer
            FROM
                STEL_DATA_TXN_MOBILE st
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select dim0 dealer, stringvalue channel, effectivestartdate, effectiveenddate f(...) */
                        dim0 AS dealer,
                        stringvalue AS channel,
                        effectivestartdate,
                        effectiveenddate
                    FROM
                        EXT.stel_lookup
                        /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_lookup@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.stel_lookup'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        name LIKE 'LT_Dealer_Channel Type'
                ) AS lt
                ON lt.dealer = st.genericattribute3
                AND lt.channel = 'SER'
                AND st.compensationdate BETWEEN lt.effectivestartdate AND add_days(lt.effectiveenddate,-1)
            WHERE
                st.filename = :v_param.file_name
                AND st.filedate = :v_param.file_Date
                AND st.recordstatus = 0
        ) AS src
        ON (src.seq = tgt.seq)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.SERINDICATOR = 'Y';

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update SER Indicator for SER Mobile' || v_para(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update SER Indicator for SER Mobile'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update ER Indicator for SER Mobile Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update SER Indicator for SER Mobile' || :v_param.file_type(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    ---------------------------------------------------[Arun bug fix for SER INDICATOR DONE ON 11th Mar 2019] Ends here
END