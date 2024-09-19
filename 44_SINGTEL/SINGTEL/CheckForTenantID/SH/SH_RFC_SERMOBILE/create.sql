CREATE PROCEDURE EXT.SH_RFC_SERMOBILE
(
    IN p_PERIODSEQ DECIMAL(38,10),   /* ORIGSQL: p_PERIODSEQ IN NUMBER */
    IN p_PROCESSINGUNITSEQ DECIMAL(38,10)   /* ORIGSQL: p_PROCESSINGUNITSEQ IN NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE DBMTK_TMPVAR_INT_1 BIGINT; /*sapdbmtk-generated help variable*/

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_StartDate TIMESTAMP;  /* ORIGSQL: v_StartDate date; */
    DECLARE v_EndDate TIMESTAMP;  /* ORIGSQL: v_EndDate date; */
    DECLARE v_Sql VARCHAR(4000);  /* ORIGSQL: v_Sql varchar2(4000); */
    DECLARE v_Singlequote VARCHAR(1) = '''';  /* ORIGSQL: v_Singlequote varchar2(1):=''''; */
    DECLARE v_delproductid VARCHAR(255);  /* ORIGSQL: v_delproductid varchar2(255); */
    DECLARE v_rowcount DECIMAL(38,10);  /* ORIGSQL: v_rowcount number; */
    DECLARE v_eot TIMESTAMP = TO_DATE('22000101','YYYYMMDD');  /* ORIGSQL: v_eot date:=TO_DATE('22000101','YYYYMMDD') ; */
    DECLARE v_sgd VARCHAR(255);  /* ORIGSQL: v_sgd varchar2(255); */
    DECLARE v_eventtypeseq VARCHAR(255);  /* ORIGSQL: v_eventtypeseq varchar2(255); */

    --added by kyap
    DECLARE v_billtoatypeseq DECIMAL(38,10);  /* ORIGSQL: v_billtoatypeseq number; */
    DECLARE v_shiptoatypeseq DECIMAL(38,10);  /* ORIGSQL: v_shiptoatypeseq number; */

    /* ORIGSQL: NULL; */
    DBMTK_TMPVAR_INT_1 = 0;/* sapdbmtk: this is a dummy statement to avoid syntax errors, if possible, delete this line */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT
        startdate,
        enddate
    INTO
        v_StartDate,
        v_EndDate
    FROM
        cs_period
    WHERE
        periodseq = :p_PERIODSEQ
        AND removedate = :v_eot;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_ADDRESSTYPE' not found */

    SELECT
        addresstypeseq
    INTO
        v_shiptoatypeseq
    FROM
        cs_addresstype
    WHERE
        addresstypeid = 'SHIPTO';

    SELECT
        addresstypeseq
    INTO
        v_billtoatypeseq
    FROM
        cs_addresstype
    WHERE
        addresstypeid = 'BILLTO';

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_UNITTYPE' not found */

    SELECT
        unittypeseq
    INTO
        v_sgd
    FROM
        cs_unittype
    WHERE
        removedate = :v_eot
        AND name = 'SGD';

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 0','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 0', 'SH', 0, :v_rowcount, 'Running');

    /* ORIGSQL: execute immediate 'truncate table stel_Classifier_Tab'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_CLASSIFIER_TAB' not found */

    /* ORIGSQL: truncate table stel_Classifier_Tab ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ext.stel_Classifier_Tab';

    /* ORIGSQL: insert into stel_classifier_Tab select * from stel_Classifier where effectivesta(...) */
    INSERT INTO ext.stel_classifier_Tab
        SELECT   /* ORIGSQL: select * from stel_Classifier where effectivestartdate<v_enddate and effectiveen(...) */
            *
        FROM
            ext.stel_Classifier
        WHERE
            effectivestartdate < :v_EndDate
            AND effectiveenddate >= :v_StartDate;

    /* ORIGSQL: dbms_stats.gather_table_stats(ownname =>'STELEXT', tabname => 'stel_classifier_T(...) */
    EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'STELEXT'|| '.'|| 'stel_classifier_Tab';

    /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

    /***
    Roadhsow id and assignments
    --TBC
    ***/

    ---do resets here
    /*8
    
    EA1	default	MSF/IMEI Error Message
    EA2 	default	Duplicate Check Message
    com.callidus.pipeline.dataobjects.inputdata.salestransaction.SalesTransactionTxt.extendedGenericDate1_DisplayName	default	Order Close Date
    com.callidus.pipeline.dataobjects.inputdata.salestransaction.SalesTransactionTxt.extendedGenericNumber1_DisplayName	default	SAP Transfer Price
    com.callidus.pipeline.dataobjects.inputdata.salestransaction.SalesTransactionTxt.extendedGenericNumber2_DisplayName	default	SAP Cost
    com.callidus.pipeline.dataobjects.inputdata.salestransaction.SalesTransactionTxt.extendedGenericNumber3_DisplayName	default	VSOP Transfer Cost
    com.callidus.pipeline.dataobjects.inputdata.salestransaction.SalesTransactionTxt.extendedGenericNumber4_DisplayName	default	Final Cost
    EB1	default	Protection Flag
    EB2	default	IMEI Mis-Match Indicator
    EB3 	default	Duplicate Check Indicator
    
    
    */   

    /* ORIGSQL: update cs_gaSalestransaction tgt SET genericattribute1 =null, genericattribute2=(...) */
    UPDATE cs_gaSalestransaction tgt
        SET
        /* ORIGSQL: genericattribute1 = */
        genericattribute1 = NULL,
        /* ORIGSQL: genericattribute2 = */
        genericattribute2 = NULL,
        /* ORIGSQL: genericboolean1 = */
        genericboolean1 = 0,
        /* ORIGSQL: genericboolean2 = */
        genericboolean2 = 0,
        /* ORIGSQL: genericboolean3 = */
        genericboolean3 = 0
    WHERE
        tgt.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        AND tgt.tenantid = 'STEL'
        AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        AND tgt.pagenumber = 0;

    /* ORIGSQL: commit; */
    COMMIT;
    --

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 10','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 10', 'SH', 0, :v_rowcount, 'Running');

    --dbms_stats.gather_table_stats(ownname =>'STELEXT',
        --                               tabname => 'stel_Classifier_tab',
    --                                                              cascade => true );

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 11','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 11', 'SH', 0, :v_rowcount, 'Running'); 

    SELECT
        addresstypeseq
    INTO
        v_shiptoatypeseq
    FROM
        cs_addresstype
    WHERE
        addresstypeid = 'SHIPTO';

    SELECT
        addresstypeseq
    INTO
        v_billtoatypeseq
    FROM
        cs_addresstype
    WHERE
        addresstypeid = 'BILLTO';

    /***
    MBB Rejection 1
    ***/      

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_salestransaction tgt using (WITH txn AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_salestransaction AS tgt
        USING (

            WITH 
            txn    
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq, txn.compensationdate, txn.tenantid, txn.process(...) */
                    txn.salestransactionseq,
                    txn.compensationdate,
                    txn.tenantid,
                    txn.processingunitseq,
                    IFNULL(txn.genericattribute8, '0') AS ga8,  /* ORIGSQL: nvl(txn.genericattribute8,'0') */
                    tad.postalcode,
                    txn.genericattribute28
                FROM
                    cs_salestransaction txn
                    -- join cs_Transactionaddress tad --Sankar Commented this and handled below in logic 29 Feb
                    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONADDRESS' not found */
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='SHIPTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'SHIPTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND tad.compensationdate = txn.compensationdate
                        -- and tad.addresstypeseq =v_shiptoatypeseq  --Sankar Commented this and handled in above logic 29 feb
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = txn.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')
                    WHERE
                        txn.genericattribute14 = 'MBB'
                        --and tad.postalcode='0'
                        AND txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND (IFNULL(txn.genericattribute8, '0') = '0'   /* ORIGSQL: nvl(txn.genericattribute8,'0') */
                        OR tad.postalcode = '0')
                        AND txn.genericattribute28 IS NOT NULL--[arun added as this retunrs null on 29th Nov 2019]
                        AND tad.postalcode IS NOT NULL--[arun added as this retunrs null on 29th Nov 2019]

                
                )
                SELECT   /* ORIGSQL: select txn.salestransactionseq, txn.compensationdate, txn.tenantid, txn.processi(...) */
                    txn.salestransactionseq,
                    txn.compensationdate,
                    txn.tenantid,
                    txn.processingunitseq,
                    txn.ga8,
                    stock.dept,
                    txn.postalcode
                FROM
                    txn txn 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select s.classifierid,s.categorytreename,s.categoryname,s.effectivestartdate,s.(...) */
                            s.classifierid,
                            s.categorytreename,
                            s.categoryname,
                            s.effectivestartdate,
                            TO_DATE(ADD_SECONDS(s.effectiveenddate,(86400*-1))) AS effenddate,  /* ORIGSQL: s.effectiveenddate-1 */
                            s.genericattribute4 AS brand,
                            s.genericattribute1 AS dept,
                            s.genericattribute3 AS mat
                        FROM
                            stel_Classifier_tab s
                    ) AS stock
                    ON txn.genericattribute28 = stock.classifierid
                    AND stock.categorytreename = 'StockCode'
                    AND stock.categoryname = 'PRODUCTS'
                    AND txn.compensationdate BETWEEN stock.effectivestartdate AND stock.effenddate
                    AND txn.genericattribute28 IS NOT NULL--[arun added as this retunrs null on 29th Nov 2019]
                    AND txn.postalcode IS NOT NULL--[arun added as this retunrs null on 29th Nov 2019]
                    /*join (select s.* from stel_Classifier_tab  s) prod
                    on prod.categorytreename = 'Singtel'
                    and prod.categoryname='PRODUCTS'
                    and txn.productid=prod.classifierid*/ --arjun 20190610
                WHERE
                    1 = 1
                    --and lower(stock.dept) not like '%dongle%'

                ) src
                ON (tgt.salestransactionseq = src.salestransactionseq
                    AND tgt.tenantid = src.tenantid
                    AND tgt.processingunitseq = src.processingunitseq
                AND tgt.compensationdate = src.compensationdate
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))
                	AND tgt.tenantid = 'STEL'
                	AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                )
    WHEN MATCHED THEN
        UPDATE SET
            comments =
            CASE
                WHEN LOWER(src.dept) NOT LIKE '%dongle%' 
                AND src.postalcode = '0'
                THEN 'MBB Rejection - No Tie in except Dongle'
                WHEN src.GA8 = '0'
                THEN 'MBB Rejection - No Capacity'
                ELSE tgt.comments
            END
        --WHERE
        --   tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 30','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 30', 'SH', 0, :v_rowcount, 'Running');

    /* ORIGSQL: commit; */
    COMMIT;
    --
    --Arun comment started from this line to ebd of proc
    /*
    merge into  cs_salestransaction tgt
    using (
        select txn.salestransactionseq, txn.compensationdate, txn.tenantid, txn.processingunitseq
        from  cs_salestransaction txn
        join cs_Transactionaddress tad
        on txn.salestransactionseq=tad.salestransactionseq
        and tad.tenantid='STEL'
        and tad.processingunitseq=p_processingunitseq
        and tad.compensationdate=txn.compensationdate
        and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='SHIPTO')
        join cs_eventtype et
        on et.removedate>sysdate and et.datatypeseq=txn.eventtypeseq and et.eventtypeid in ('Mobile SER R1','Mobile SER R2')
        join   (select s.*, s.genericattribute4 brand, s.genericattribute1 dept, s.genericattribute3 mat
        from stel_Classifier_tab  s) stock
        on txn.genericattribute28=stock.classifierid
        and stock.categorytreename = 'StockCode'
        and stock.categoryname='PRODUCTS'
        and txn.compensationdate between stock.effectivestartdate and stock.effectiveenddate-1
        /#join (select s.* from stel_Classifier_tab  s) prod
        on prod.categorytreename = 'Singtel'
        and prod.categoryname='PRODUCTS'
        and txn.productid=prod.classifierid*/ --arjun 20190610 
        /*
        where 1=1
        and lower(stock.dept) not like '%dongle%'
        and txn.genericattribute14='MBB'
        and tad.postalcode='0'
        and txn.compensationdate between v_Startdate and v_Enddate-1
        and txn.tenantid='STEL'
        and txn.processingunitseq=p_processingunitseq
    )src
    on (tgt.salestransactionseq=src.salestransactionseq and tgt.tenantid=src.tenantid
    and tgt.processingunitseq=src.processingunitseq and tgt.compensationdate=src.compensationdate)
    when matched then update set
    comments = 'MBB Rejection - No Tie in except Dongle'
    where tgt.compensationdate between v_Startdate and v_Enddate-1
    and tgt.tenantid='STEL'
    and tgt.processingunitseq=p_processingunitseq
    ;
    
    v_rowcount := SQL%ROWCOUNT;
    stel_Sp_logger('SH_RFC_MOBILESER 30','SH',0,v_rowcount,'Running');
    */
    /***
    MBB Rejection 2 -  have toc heck against LLD if this is still required
    ***/
    /*
     merge into  cs_salestransaction tgt
     using (
         select txn.salestransactionseq, txn.compensationdate, txn.tenantid, txn.processingunitseq
         from  cs_salestransaction txn
         join cs_eventtype et
         on et.removedate>sysdate and et.datatypeseq=txn.eventtypeseq and et.eventtypeid in ('Mobile SER R1','Mobile SER R2')
         join   (select s.*, s.genericattribute4 brand, s.genericattribute1 dept, s.genericattribute3 mat
         from stel_Classifier_tab  s) stock
         on txn.genericattribute28=stock.classifierid
         and stock.categorytreename = 'StockCode'
         and stock.categoryname='PRODUCTS'
         and txn.compensationdate between stock.effectivestartdate and stock.effectiveenddate-1
         /#join (select s.* from stel_Classifier_tab  s) prod
          on prod.categorytreename = 'Singtel'
         and prod.categoryname='PRODUCTS'
         and txn.productid=prod.classifierid
         */
        /*where 1=1
        and txn.genericattribute14='MBB'
        and nvl(txn.genericattribute8,'0')='0'
        
        and txn.compensationdate between v_Startdate and v_Enddate-1
        and txn.tenantid='STEL'
        and txn.processingunitseq=p_processingunitseq
    )src
    on (tgt.salestransactionseq=src.salestransactionseq and tgt.tenantid=src.tenantid
    and tgt.processingunitseq=src.processingunitseq and tgt.compensationdate=src.compensationdate)
    when matched then update set
    comments = 'MBB Rejection - No Capacity'
    where tgt.compensationdate between v_Startdate and v_Enddate-1
    and tgt.tenantid='STEL'
    and tgt.processingunitseq=p_processingunitseq
    ;
    
    
    v_rowcount := SQL%ROWCOUNT;
    stel_Sp_logger('SH_RFC_MOBILESER 40','SH',0,v_rowcount,'Running');*/
    /***
    Same mth duplicate check
    
    Going to update EB3 as the indicator for non payment for duplicate checks. This can be copied over to GB1 if required later.
    EA2 will have the comments
    
    a.	Same Month Check
    If there are multiple transactions in the month (i.e. entered in same the month), with the same service # and order enter date, only the later transaction is considered for calculation.
    
    -	The Product Sim Only  is excluded from this check
    */ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT txn.accountingdate, tad.phone(...) */
    MERGE INTO cs_gasalestransaction AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (select txn.accountingdate, tad.phone, MIN(txn.salestransactionseq) stseq from c(...) */
                txn.accountingdate,
                tad.phone,
                MIN(txn.salestransactionseq) AS stseq
            FROM
                cs_salestransaction txn
            INNER JOIN
                cs_eventtype et
                ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND et.datatypeseq = txn.eventtypeseq
                AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 29 FEB 
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                        *
                    FROM
                        cs_transactionaddress
                    WHERE
                        addresstypeseq 

                        =
                        (
                            SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                addresstypeseq
                            FROM
                                cs_addresstype
                            WHERE
                                addresstypeid = 'BILLTO'
                        )
                    ) AS tad
                    ON txn.salestransactionseq = tad.salestransactionseq
                    AND tad.tenantid = 'STEL'
                    AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND txn.compensationdate = tad.compensationdate
                    -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- sankar commented this and handled in above logic 29 FEB
                WHERE
                    txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txn.tenantid = 'STEL'
                    AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND txn.genericattribute19 IS NULL /*Not incl Sim only*/
                    AND txn.genericattribute9 = 'M'
                GROUP BY
                    txn.accountingdate, tad.phone
                HAVING
                    COUNT(*) > 1
            ) AS src
            ON (src.stseq = tgt.salestransactionseq 
            	AND tgt.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))
            	AND tgt.tenantid = 'STEL'
            	AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                 AND tgt.pagenumber = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'Same Month Duplicate Check'
            , genericboolean3 = 1
        --WHERE
         --   tgt.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
         --   AND tgt.tenantid = 'STEL'
         --   AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
         --   AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 43','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 43', 'SH', 0, :v_rowcount, 'Running');

    /* ORIGSQL: commit; */
    COMMIT;
    --
    /*
    -	If one transaction is SIM only (with or without contract) /(new or recon) , and the other is a CI, only the CI transaction is paid
    -	The sim only order will need to have a rejection reason stamped on the transaction that indicates this.
    */ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT accountingdate, phone, SimOnl(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select accountingdate,phone,SimOnlyCount,CICount,SOSEQ,CISEQ from (SELECT txn.a(...) */
                accountingdate,
                phone,
                SimOnlyCount,
                CICount,
                SOSEQ,
                CISEQ    
            FROM
                (
                    SELECT   /* ORIGSQL: (select txn.accountingdate, tad.phone,SUM(CASE WHEN txn.genericattribute19='Sim (...) */
                        txn.accountingdate,
                        tad.phone,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute19 = 'Sim Only Indicator'
                                THEN 1
                                ELSE 0
                            END
                        ) AS SimOnlyCount,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute19 IS NULL
                                AND genericattribute5 = 'Recon'
                                THEN 1
                                ELSE 0
                            END
                        ) AS CICount,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute19 = 'Sim Only Indicator'
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS SOSEQ,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute19 IS NULL
                                AND genericattribute5 = 'Recon'
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS CISEQ
                    FROM
                        cs_salestransaction txn
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = txn.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                        --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                                *
                            FROM
                                cs_transactionaddress
                            WHERE
                                addresstypeseq 

                                =
                                (
                                    SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                    WHERE
                                        addresstypeid = 'BILLTO'
                                )
                            ) AS tad
                            ON txn.salestransactionseq = tad.salestransactionseq
                            AND tad.tenantid = 'STEL'
                            AND txn.compensationdate = tad.compensationdate
                            AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                            -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                        WHERE
                            txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                            AND txn.tenantid = 'STEL'
                            AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                            AND txn.genericattribute9 = 'M'
                        GROUP BY
                            txn.accountingdate, tad.phone
                    ) AS dbmtk_corrname_20915
                WHERE
                    simonlycount >= 1
                    AND cicount >= 1
            ) AS src
            ON (src.soseq = tgt.salestransactionseq
                AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                AND tgt.tenantid = 'STEL'
                AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                AND tgt.pagenumber = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'CI and Sim Only. Only CI is Paid'
            , genericboolean3 =
            CASE
                WHEN tgt.salestransactionseq = src.soseq
                THEN 1
                ELSE 0
            END
        --WHERE
        --   tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --   AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 47-1','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 47-1', 'SH', 0, :v_rowcount, 'Running'); 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT accountingdate, phone, SimOnl(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select accountingdate,phone,SimOnlyCount,CICount,SOSEQ,CISEQ from (SELECT txn.a(...) */
                accountingdate,
                phone,
                SimOnlyCount,
                CICount,
                SOSEQ,
                CISEQ    
            FROM
                (
                    SELECT   /* ORIGSQL: (select txn.accountingdate, tad.phone,SUM(CASE WHEN txn.genericattribute19='Sim (...) */
                        txn.accountingdate,
                        tad.phone,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute19 = 'Sim Only Indicator'
                                THEN 1
                                ELSE 0
                            END
                        ) AS SimOnlyCount,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute19 IS NULL
                                AND genericattribute5 = 'Recon'
                                THEN 1
                                ELSE 0
                            END
                        ) AS CICount,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute19 = 'Sim Only Indicator'
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS SOSEQ,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute19 IS NULL
                                AND genericattribute5 = 'Recon'
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS CISEQ
                    FROM
                        cs_salestransaction txn
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = txn.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                        --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                                *
                            FROM
                                cs_transactionaddress
                            WHERE
                                addresstypeseq 

                                =
                                (
                                    SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                    WHERE
                                        addresstypeid = 'BILLTO'
                                )
                            ) AS tad
                            ON txn.salestransactionseq = tad.salestransactionseq
                            AND tad.tenantid = 'STEL'
                            AND txn.compensationdate = tad.compensationdate
                            AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                            -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')  -- Sankar commented this and handled in above logic
                        WHERE
                            txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                            AND txn.tenantid = 'STEL'
                            AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                            AND txn.genericattribute9 = 'M'
                        GROUP BY
                            txn.accountingdate, tad.phone
                    ) AS dbmtk_corrname_20919
                WHERE
                    simonlycount >= 1
                    AND cicount >= 1
            ) AS src
            ON (src.ciseq = tgt.salestransactionseq
            	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                AND tgt.tenantid = 'STEL'
                AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                AND tgt.pagenumber = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'CI and Sim Only. Only CI is Paid'
            , genericboolean3 =
            CASE
                WHEN tgt.salestransactionseq = src.soseq
                THEN 1
                ELSE 0
            END
       -- WHERE
       --     tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
       --    AND tgt.tenantid = 'STEL'
       --     AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
       --     AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 47-2','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 47-2', 'SH', 0, :v_rowcount, 'Running');

    /*
    -	Sim only New And Sim Only CP (Change) in the same month, commission is paid based on the first order, irrespective of which dealer changes it.
    
    */ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT accountingdate, phone, NewCou(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select accountingdate,phone,NewCount,ChangeCount,newSeq,CISEQ from (SELECT txn.(...) */
                accountingdate,
                phone,
                NewCount,
                ChangeCount,
                newSeq,
                CISEQ    
            FROM
                (
                    SELECT   /* ORIGSQL: (select txn.accountingdate, tad.phone,SUM(CASE WHEN txn.genericattribute5='New' (...) */
                        txn.accountingdate,
                        tad.phone,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute5 = 'New'
                                THEN 1
                                ELSE 0
                            END
                        ) AS NewCount,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute19 IN ('Change','Recon')
                                THEN 1
                                ELSE 0
                            END
                        ) AS ChangeCount,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute5 = 'New'
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS newSeq,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute19 IN ('Change','Recon')
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS CISEQ
                    FROM
                        cs_salestransaction txn
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = txn.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                        --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                                *
                            FROM
                                cs_transactionaddress
                            WHERE
                                addresstypeseq 

                                =
                                (
                                    SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                    WHERE
                                        addresstypeid = 'BILLTO'
                                )
                            ) AS tad
                            ON txn.salestransactionseq = tad.salestransactionseq
                            AND tad.tenantid = 'STEL'
                            AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                            AND txn.compensationdate = tad.compensationdate
                            -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')-- Sankar commented this and handled in above logic
                        WHERE
                            txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                            AND txn.tenantid = 'STEL'
                            AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                            AND txn.genericattribute9 = 'M'
                            AND txn.genericattribute19 = 'Sim Only Indicator'
                        GROUP BY
                            txn.accountingdate, tad.phone
                    ) AS dbmtk_corrname_20923
                WHERE
                    newcount >= 1
                    AND ChangeCount >= 1
            ) AS src
            ON (src.newseq = tgt.salestransactionseq
            	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                AND tgt.tenantid = 'STEL'
                AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                AND tgt.pagenumber = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'SO New and Change. Only New is Paid'
            , genericboolean3 =
            CASE
                WHEN tgt.salestransactionseq = src.ciseq
                THEN 1
                ELSE 0
            END
       -- WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 51-a','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 51-a', 'SH', 0, :v_rowcount, 'Running'); 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT accountingdate, phone, NewCou(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select accountingdate,phone,NewCount,ChangeCount,newSeq,CISEQ from (SELECT txn.(...) */
                accountingdate,
                phone,
                NewCount,
                ChangeCount,
                newSeq,
                CISEQ    
            FROM
                (
                    SELECT   /* ORIGSQL: (select txn.accountingdate, tad.phone,SUM(CASE WHEN txn.genericattribute5='New' (...) */
                        txn.accountingdate,
                        tad.phone,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute5 = 'New'
                                THEN 1
                                ELSE 0
                            END
                        ) AS NewCount,
                        SUM(
                            CASE 
                                WHEN txn.genericattribute19 IN ('Change','Recon')
                                THEN 1
                                ELSE 0
                            END
                        ) AS ChangeCount,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute5 = 'New'
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS newSeq,
                        MAX(
                            CASE 
                                WHEN txn.genericattribute19 IN ('Change','Recon')
                                THEN txn.salestransactionseq
                                ELSE -1
                            END
                        ) AS CISEQ
                    FROM
                        cs_salestransaction txn
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = txn.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                        --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                                *
                            FROM
                                cs_transactionaddress
                            WHERE
                                addresstypeseq 

                                =
                                (
                                    SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                    WHERE
                                        addresstypeid = 'BILLTO'
                                )
                            ) AS tad
                            ON txn.salestransactionseq = tad.salestransactionseq
                            AND tad.tenantid = 'STEL'
                            AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                            AND txn.compensationdate = tad.compensationdate
                            -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                        WHERE
                            txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                            AND txn.tenantid = 'STEL'
                            AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                            AND txn.genericattribute9 = 'M'
                            AND txn.genericattribute19 = 'Sim Only Indicator'
                        GROUP BY
                            txn.accountingdate, tad.phone
                    ) AS dbmtk_corrname_20927
                WHERE
                    newcount >= 1
                    AND ChangeCount >= 1
            ) AS src
            ON (src.ciseq = tgt.salestransactionseq
            	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                AND tgt.tenantid = 'STEL'
                AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                AND tgt.pagenumber = 0
            )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'SO New and Change. Only New is Paid'
            , genericboolean3 =
            CASE
                WHEN tgt.salestransactionseq = src.ciseq
                THEN 1
                ELSE 0
            END
        --WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
       --     AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
       --    AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 51-b','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 51-b', 'SH', 0, :v_rowcount, 'Running');

    /**
    -	Sim only CP change (Contract/Non Contract) to Sim only non-Contract is not payable.
    --------------This would not be paid anyway, since it's not a RECON. No handling is needed--------------
    
    -	If there are two transactions \x93 Sim only CP (Change) and Sim only CP(Change)\x94 within the same month,
    then system has to check for the movement of the Sim only plan,
    if first Sim only CP Plan movement is to Sim only non-Contract plan,
    then system should not pay commission for the first transaction. Second sim only CP is payable.
    
    ------------This would not be paid anyway, since it's not a RECON. only the CP would be payable----------
     ***/

    /* ORIGSQL: commit; */
    COMMIT;
    --

    /***
    b.	Cross Month NL Check
    For NL transactions, if there are multiple transactions within the last 3 months
    (i.e. Entered from 1st Sep to 30th November for the November Calculation) with the same Service # and order enter Date.
    ---------not sure this makes sense. if the order enter date is the same, then they are automatically in the same month
    ---------- codewritten is assuming same phone number only---------------
    
    IF a duplicate is found, the new record is rejected, with the exceptions below:
    */     

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (

            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq, txn.eventtypeseq, tad.phone, txn.compensationda(...) */
                    txn.salestransactionseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute9 = 'M'
                        AND txn.genericattribute19 IS NULL
                        AND txn.genericattribute5 = 'New'
                )
                SELECT   /* ORIGSQL: select txn.salestransactionseq newseq from x txn INNER join x txnold on txnold.c(...) */
                    txn.salestransactionseq AS newseq
                FROM
                    x txn
                INNER JOIN
                    x txnold
                    ON txnold.compensationdate < txn.compensationdate
                    AND txn.phone = txnold.phone
                    AND txnold.eventtypeseq = txn.eventtypeseq
                WHERE
                    txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1))) /*
                    
                    select distinct txn.salestransactionseq newseq   from cs_salestransaction txn
                    join cs_eventtype et
                     on et.removedate>sysdate and et.datatypeseq=txn.eventtypeseq and et.eventtypeid in ('Mobile SER R1','Mobile SER R2')
                    
                     join cs_transactionaddress tad
                        on txn.salestransactionseq=tad.salestransactionseq
                     and tad.tenantid='STEL'
                     and tad.processingunitseq=p_processingunitseq and txn.compensationdate=tad.compensationdate
                     and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
                     join   cs_salestransaction txnOld
                     on txnold.eventtypeseq=txn.eventtypeseq
                     join cs_transactionaddress tad2
                        on txnOld.salestransactionseq=tad2.salestransactionseq and txnOld.compensationdate=tad2.compensationdate
                     and tad2.tenantid='STEL'
                     and tad2.processingunitseq=p_processingunitseq
                     and tad2.addresstypeseq =tad.addresstypeseq
                    
                     where txn.compensationdate between v_Startdate and v_Enddate-1
                     and txn.tenantid='STEL'
                     and txn.processingunitseq=p_processingunitseq
                     and txn.genericattribute19 IS NULL
                     and txn.genericattribute9='M'
                     and txnold.compensationdate between add_months(v_Startdate,-2) and v_Enddate-1
                     and txnold.compensationdate<txn.compensationdate
                     and txnold.tenantid='STEL'
                     and txnold.processingunitseq=p_processingunitseq
                     and txnold.genericattribute19 IS NULL
                     and txnold.genericattribute9='M'
                     and tad.phone=tad2.phone*/   /* ORIGSQL: v_Enddate-1 */
                ) src
                ON (src.newseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'Cross Mth NL check. New Txn is rejected'
            , genericboolean3 =
            CASE
                WHEN tgt.salestransactionseq = src.newseq
                THEN 1
                ELSE 0
            END
        --WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
        ;    

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (

            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq, txn.eventtypeseq, tad.phone, txn.compensationda(...) */
                    txn.salestransactionseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')  -- Sankar commented this and handled in above logic
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute9 = 'M'
                        AND txn.genericattribute19 IS NULL
                        AND txn.genericattribute5 = 'New'
                )
                SELECT   /* ORIGSQL: select txnOld.salestransactionseq oldseq from x txn INNER join x txnold on txnol(...) */
                    txnOld.salestransactionseq AS oldseq
                FROM
                    x txn
                INNER JOIN
                    x txnold
                    ON txnold.compensationdate < txn.compensationdate
                    AND txn.phone = txnold.phone
                    AND txnold.eventtypeseq = txn.eventtypeseq
                WHERE
                    txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1))) /*
                    select distinct  txnold.salestransactionseq oldseq  from cs_salestransaction txn
                    join cs_eventtype et
                     on et.removedate>sysdate and et.datatypeseq=txn.eventtypeseq and et.eventtypeid in ('Mobile SER R1','Mobile SER R2')
                    
                     join cs_transactionaddress tad
                        on txn.salestransactionseq=tad.salestransactionseq and txn.compensationdate=tad.compensationdate
                     and tad.tenantid='STEL'
                     and tad.processingunitseq=p_processingunitseq
                     and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
                     join   cs_salestransaction txnOld
                     on txnold.eventtypeseq=txn.eventtypeseq
                     join cs_transactionaddress tad2
                        on txnOld.salestransactionseq=tad2.salestransactionseq
                     and tad2.tenantid='STEL'
                     and tad2.processingunitseq=p_processingunitseq
                     and tad2.addresstypeseq =tad.addresstypeseq and txnOld.compensationdate=tad2.compensationdate
                    
                     where txn.compensationdate between v_Startdate and v_Enddate-1
                     and txn.tenantid='STEL'
                     and txn.processingunitseq=p_processingunitseq
                     and txn.genericattribute19 IS NULL
                     and txn.genericattribute9='M'
                     and txnold.compensationdate between add_months(v_Startdate,-2) and v_Enddate-1
                     and txnold.compensationdate<txn.compensationdate
                     and txnold.tenantid='STEL'
                     and txnold.processingunitseq=p_processingunitseq
                     and txnold.genericattribute19 IS NULL
                     and txnold.genericattribute9='M'
                     and tad.phone=tad2.phone*/   /* ORIGSQL: v_Enddate-1 */
                ) src
                ON (src.oldseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'Cross Mth NL check. New Txn is rejected'
            , genericboolean3 = 0
        --WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
       --     AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 52','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 52', 'SH', 0, :v_rowcount, 'Running');

    /*
    b.	Cross Month NL Check contd..
    Exceptions
    -	If one transaction is SIM only (with or without contract) in a prior month,
    and the other is a CI in the current month,
    this pair is also excluded from this check.
    Both the Sim only and CI transactions are payable (This does not check for Dealer codes \x96 it can be sold by any dealer/roadshow)
    -------------No code needed for this, this is the default behaviour--------------
    
    -	If one transaction is SIM only Starter Pack with CI in a prior month,
    and the other is a CI in the current month, the new CI is not paid
    (This does not check for Dealer codes \x96 it can be sold by any dealer/roadshow)
     ***/     

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (
            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq newseq, txn.eventtypeseq, tad.phone, txn.compens(...) */
                    txn.salestransactionseq AS newseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute9 = 'M'
                        AND txn.genericattribute19 = 'Sim Only Indicator'
                        AND txn.genericattribute5 = 'Recon'
                )
                SELECT   /* ORIGSQL: select distinct txn.newseq from x txn INNER join x txnOld on txn.eventtypeseq=tx(...) */
                    --  select txn.newseq, txnold.newseq as oldseq --[Arun commented this originalblock as this was causing duplicates - 18th Nov 2019]
                    DISTINCT
                    txn.newseq
                FROM
                    x txn
                INNER JOIN
                    x txnOld
                    ON txn.eventtypeseq = txnold.eventtypeseq
                WHERE
                    txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1))) --and txn.genericattribute19  ='Sim Only Indicator'
                    /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate < txn.compensationdate
                    --and txnold.genericattribute19 ='Sim Only Indicator'
                    --and txnold.genericattribute5='Recon'
                    AND txn.phone = txnold.phone
                    /* and txnold is sim only starter pack/#*20190223: Have to see how to identify this*/

                ) src
                --on (src.newseq=tgt.salestransactionseq or src.oldseq=tgt.salestransactionseq) --[Arun commented this originalblock as this was causing duplicates - 18th Nov 2019]
                ON (src.newseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                ) --[Arun added this block as this was causing duplicates - 18th Nov 2019]
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'SOS and SO CI. New CI is not paid'
            --  , genericboolean3= case when tgt.salestransactionseq=src.newseq then 1 else 0 end --[Arun commented this originalblock as this was causing duplicates - 18th Nov 2019]
            , genericboolean3 = 1 ----[Arun added this block as this was causing duplicates - 18th Nov 2019]
        --WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 60-A','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 60-A', 'SH', 0, :v_rowcount, 'Running');

    /* ORIGSQL: commit; */
    COMMIT;
    --

    ------[Arun added this block as this was causing duplicates - 18th Nov 2019]     
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (
            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq newseq, txn.eventtypeseq, tad.phone, txn.compens(...) */
                    txn.salestransactionseq AS newseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute9 = 'M'
                        AND txn.genericattribute19 = 'Sim Only Indicator'
                        AND txn.genericattribute5 = 'Recon'
                )
                SELECT   /* ORIGSQL: select distinct txnold.newseq as oldseq from x txn INNER join x txnOld on txn.ev(...) */
                    DISTINCT
                    txnold.newseq AS oldseq
                FROM
                    x txn
                INNER JOIN
                    x txnOld
                    ON txn.eventtypeseq = txnold.eventtypeseq
                WHERE
                    txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1))) --and txn.genericattribute19  ='Sim Only Indicator'
                    /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate < txn.compensationdate
                    --and txnold.genericattribute19 ='Sim Only Indicator'
                    --and txnold.genericattribute5='Recon'
                    AND txn.phone = txnold.phone
                    /* and txnold is sim only starter pack/#*20190223: Have to see how to identify this*/

                ) src
                ON (src.oldseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'SOS and SO CI. New CI is not paid'
            , genericboolean3 = 0
        --WHERE
        --   tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 60-B','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 60-B', 'SH', 0, :v_rowcount, 'Running');

    ---[Arun added this block as this was causing duplicates - 18th Nov 2019]
    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 61-A','SH',0,0,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 61-A', 'SH', 0, 0, 'Running');

    /***
    c.	Cross Month CI Check
    For CI transactions, if there are multiple transactions within the last 2 months (i.e. Entered from 1st Oct to 30th November
    for the November Calculation) with the same Service # and order enter Date
    
    If a duplicate is found, the new record is rejected, with the exceptions below:
    
    -	If one transaction is SIM only (with or without contract) in a prior month, and the other is a CI in the current month,
    this pair is also excluded from this check. Both the Sim only and CI transactions are payable
    
    -----------No extra code needed----------------
    
    -	If one transaction is SIM only with CI in a prior month, and the other is a CI in the current month, the new CI is not paid
    -	If one transaction is SIM only Starter Pack with CI in a prior month, and the other is a CI in the current month, the new CI is not paid
    
    -----------Covered above after '52'----------------
    
    -	If one transaction is MobileShare with CI in a prior month, and the other is a CI in the current month, the new CI is not paid
    
    ***/      

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (

            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq newseq, txn.eventtypeseq, tad.phone, txn.compens(...) */
                    txn.salestransactionseq AS newseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute9 = 'M'
                        AND txn.genericattribute19 IS NULL
                        AND txn.genericattribute5 = 'Recon'
                )
                SELECT   /* ORIGSQL: select txn.newseq from x txn INNER join x txnOld on txn.eventtypeseq=txnold.even(...) */
                    txn.newseq /* --, txnold.newseq as oldseq  */
                FROM
                    x txn
                INNER JOIN
                    x txnOld
                    ON txn.eventtypeseq = txnold.eventtypeseq
                WHERE
                    txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate < txn.compensationdate
                    AND txn.phone = txnold.phone

                    /*
                     where txn.compensationdate between v_Startdate and v_Enddate-1
                     and txn.tenantid='STEL'
                     and txn.processingunitseq=p_processingunitseq
                     and txn.genericattribute19 IS NULL
                     and txn.genericattribute9='M'
                     and txnold.compensationdate between add_months(v_Startdate,-2) and v_Enddate-1
                     and txnold.compensationdate<txn.compensationdate
                     and txnold.tenantid='STEL'
                     and txnold.processingunitseq=p_processingunitseq
                     and txnold.genericattribute19 IS NULL
                     and txnold.genericattribute5='Recon'
                     and txn.genericattribute5='Recon'
                     and txnold.genericattribute9='M'
                     and tad.phone=tad2.phone -- not sure if another filter is required here. only using service no for now.
                     --order enter date doesn't make sense as noted above
                    */

                ) src
                --on (src.newseq=tgt.salestransactionseq or src.oldseq=tgt.salestransactionseq)
                ON (src.newseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'Cross Month CI Check. New CI is not paid'
            --, genericboolean3= case when tgt.salestransactionseq=src.newseq then 1 else 0 end
            , genericboolean3 = 1
        --WHERE
        --   tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --     AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 61-A','SH',0,v_rowcount,'Completed') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 61-A', 'SH', 0, :v_rowcount, 'Completed');

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 61-B','SH',0,0,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 61-B', 'SH', 0, 0, 'Running');     

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (

            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq newseq, txn.eventtypeseq, tad.phone, txn.compens(...) */
                    txn.salestransactionseq AS newseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')
                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute9 = 'M'
                        AND txn.genericattribute19 IS NULL
                        AND txn.genericattribute5 = 'Recon'
                )
                SELECT   /* ORIGSQL: select txnold.newseq as oldseq from x txn INNER join x txnOld on txn.eventtypese(...) */
                    txnold.newseq AS oldseq
                FROM
                    x txn
                INNER JOIN
                    x txnOld
                    ON txn.eventtypeseq = txnold.eventtypeseq
                WHERE
                    txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate < txn.compensationdate
                    AND txn.phone = txnold.phone

                    /*
                     where txn.compensationdate between v_Startdate and v_Enddate-1
                     and txn.tenantid='STEL'
                     and txn.processingunitseq=p_processingunitseq
                     and txn.genericattribute19 IS NULL
                     and txn.genericattribute9='M'
                     and txnold.compensationdate between add_months(v_Startdate,-2) and v_Enddate-1
                     and txnold.compensationdate<txn.compensationdate
                     and txnold.tenantid='STEL'
                     and txnold.processingunitseq=p_processingunitseq
                     and txnold.genericattribute19 IS NULL
                     and txnold.genericattribute5='Recon'
                     and txn.genericattribute5='Recon'
                     and txnold.genericattribute9='M'
                     and tad.phone=tad2.phone -- not sure if another filter is required here. only using service no for now.
                     --order enter date doesn't make sense as noted above
                    */

                ) src
                ON (src.oldseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'Cross Month CI Check. New CI is not paid'
            , genericboolean3 = 0
        --WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
            ;

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 61-B','SH',0,v_rowcount,'Completed') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 61-B', 'SH', 0, :v_rowcount, 'Completed');

    /* ORIGSQL: commit; */
    COMMIT;
    --
    ---
    --stel_Sp_logger('SH_RFC_MOBILESER 62','SH',0,0,'Running');

    /*-	If one transaction is MobileShare with CI in a prior month, and the other is a CI in the current month, the new CI is not paid
    ----have to add mobileshare identificatoin
    
    */
    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 65-1','SH',0,0,'Start') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 65-1', 'SH', 0, 0, 'Start');     

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (

            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq newseq, txn.eventtypeseq, tad.phone, txn.compens(...) */
                    txn.salestransactionseq AS newseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate,
                    txn.genericattribute19
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute5 = 'Recon'
                        AND txn.genericattribute9 = 'M'
                )
                SELECT   /* ORIGSQL: select distinct txn.newseq from x txn INNER join x txnOld on txn.eventtypeseq=tx(...) */
                    --  select txn.newseq, txnold.newseq as oldseq [Arun commented on 19th Nov]
                    DISTINCT
                    txn.newseq
                FROM
                    x txn
                INNER JOIN
                    x txnOld
                    ON txn.eventtypeseq = txnold.eventtypeseq
                WHERE
                    txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate < txn.compensationdate
                    AND txn.phone = txnold.phone
                    AND txn.genericattribute19 IS NULL
                    -- and txnold is sim only starter pack/**20190223: Have to see how to identify this*/

                    /*
                    
                    select txn.salestransactionseq newseq, txnold.salestransactionseq oldseq  from cs_salestransaction txn
                    join cs_eventtype et
                     on et.removedate>sysdate and et.datatypeseq=txn.eventtypeseq and et.eventtypeid in ('Mobile SER R1','Mobile SER R2')
                    
                     join cs_transactionaddress tad
                        on txn.salestransactionseq=tad.salestransactionseq and txn.compensationdate=tad.compensationdate
                     and tad.tenantid='STEL'
                     and tad.processingunitseq=p_processingunitseq
                     and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
                     join   cs_salestransaction txnOld
                     on txnold.eventtypeseq=txn.eventtypeseq
                     join cs_transactionaddress tad2
                        on txnOld.salestransactionseq=tad2.salestransactionseq and txnOld.compensationdate=tad2.compensationdate
                     and tad2.tenantid='STEL'
                     and tad2.processingunitseq=p_processingunitseq
                     and tad2.addresstypeseq =tad.addresstypeseq
                    
                     where txn.compensationdate between v_Startdate and v_Enddate-1
                     and txn.tenantid='STEL'
                     and txn.processingunitseq=p_processingunitseq
                     and txn.genericattribute19 IS NULL
                     and txn.genericattribute9='M'
                     and txnold.compensationdate between add_months(v_Startdate,-2) and v_Enddate-1
                     and txnold.compensationdate<txn.compensationdate
                     and txnold.tenantid='STEL'
                     and txnold.processingunitseq=p_processingunitseq
                      --and txnold.genericattribute19 ='Sim Only Indicator' /#tXNold has to be mobileshare
                     and txnold.genericattribute5='Recon'
                     and txn.genericattribute5='Recon'
                     and txnold.genericattribute9='M'
                     and tad.phone=tad2.phone
                     and txnold is sim only starter pack/#*20190223: Have to see how to identify this*/

                ) src
                --on (src.newseq=tgt.salestransactionseq or src.oldseq=tgt.salestransactionseq) --[Arun commented as duplicates are created
                ON (src.newseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'MobileShare and  CI. New CI is not paid'
            --  , genericboolean3= case when tgt.salestransactionseq=src.newseq then 1 else 0 end --[Arun commented as duplicates are created
            , genericboolean3 = 1
        --WHERE
         --   tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
         --   AND tgt.tenantid = 'STEL'
         --   AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 65-1','SH',0,v_rowcount,'End') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 65-1', 'SH', 0, :v_rowcount, 'End');

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 65-2','SH',0,0,'Start') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 65-2', 'SH', 0, 0, 'Start');     

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (WITH x AS (SELECT txn.salestransacti(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING (

            WITH 
            x   
            AS (
                SELECT   /* ORIGSQL: (select txn.salestransactionseq newseq, txn.eventtypeseq, tad.phone, txn.compens(...) */
                    txn.salestransactionseq AS newseq,
                    txn.eventtypeseq,
                    tad.phone,
                    txn.compensationdate,
                    txn.genericattribute19
                FROM
                    cs_salestransaction txn
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = txn.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')

                    --join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                            *
                        FROM
                            cs_transactionaddress
                        WHERE
                            addresstypeseq 

                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON txn.salestransactionseq = tad.salestransactionseq
                        AND txn.compensationdate = tad.compensationdate
                        AND tad.tenantid = 'STEL'
                        AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                        -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')-- Sankar commented this and handled in above logic
                    WHERE
                        txn.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.genericattribute5 = 'Recon'
                        AND txn.genericattribute9 = 'M'
                )
                SELECT   /* ORIGSQL: select distinct txnold.newseq as oldseq from x txn INNER join x txnOld on txn.ev(...) */
                    DISTINCT
                    txnold.newseq AS oldseq
                FROM
                    x txn
                INNER JOIN
                    x txnOld
                    ON txn.eventtypeseq = txnold.eventtypeseq
                WHERE
                    txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate BETWEEN ADD_MONTHS(:v_StartDate,-2) AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND txnold.compensationdate < txn.compensationdate
                    AND txn.phone = txnold.phone
                    AND txn.genericattribute19 IS NULL
                    -- and txnold is sim only starter pack/**20190223: Have to see how to identify this*/

                    /*
                    
                    select txn.salestransactionseq newseq, txnold.salestransactionseq oldseq  from cs_salestransaction txn
                    join cs_eventtype et
                     on et.removedate>sysdate and et.datatypeseq=txn.eventtypeseq and et.eventtypeid in ('Mobile SER R1','Mobile SER R2')
                    
                     join cs_transactionaddress tad
                        on txn.salestransactionseq=tad.salestransactionseq and txn.compensationdate=tad.compensationdate
                     and tad.tenantid='STEL'
                     and tad.processingunitseq=p_processingunitseq
                     and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
                     join   cs_salestransaction txnOld
                     on txnold.eventtypeseq=txn.eventtypeseq
                     join cs_transactionaddress tad2
                        on txnOld.salestransactionseq=tad2.salestransactionseq and txnOld.compensationdate=tad2.compensationdate
                     and tad2.tenantid='STEL'
                     and tad2.processingunitseq=p_processingunitseq
                     and tad2.addresstypeseq =tad.addresstypeseq
                    
                     where txn.compensationdate between v_Startdate and v_Enddate-1
                     and txn.tenantid='STEL'
                     and txn.processingunitseq=p_processingunitseq
                     and txn.genericattribute19 IS NULL
                     and txn.genericattribute9='M'
                     and txnold.compensationdate between add_months(v_Startdate,-2) and v_Enddate-1
                     and txnold.compensationdate<txn.compensationdate
                     and txnold.tenantid='STEL'
                     and txnold.processingunitseq=p_processingunitseq
                      --and txnold.genericattribute19 ='Sim Only Indicator' /#tXNold has to be mobileshare
                     and txnold.genericattribute5='Recon'
                     and txn.genericattribute5='Recon'
                     and txnold.genericattribute9='M'
                     and tad.phone=tad2.phone
                     and txnold is sim only starter pack/#*20190223: Have to see how to identify this*/

                ) src
                ON (src.oldseq = tgt.salestransactionseq
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute2 = 'MobileShare and  CI. New CI is not paid'
            , genericboolean3 = 0
        --WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 65-2','SH',0,v_rowcount,'End') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 65-2', 'SH', 0, :v_rowcount, 'End');

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 70','SH',0,0,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 70', 'SH', 0, 0, 'Running');

    /* ORIGSQL: commit; */
    COMMIT;
    --

    /**
    CHECK USED IMEI,
    **/

    --check the one time list +- any additions in classifiers
    --one time list has to be uplaoded manually, there is no dropbox template for it. 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT distinct salestransactionseq,(...) */
    MERGE INTO cs_gasalestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select distinct salestransactionseq, processingunitseq, tenantid, compensationd(...) */
                DISTINCT
                salestransactionseq,
                processingunitseq,
                tenantid,
                compensationdate    
            FROM
                (
                    SELECT   /* ORIGSQL: (select ta.salestransactionseq, ta.custid ||'-'|| ta.contact CustService, postal(...) */
                        ta.salestransactionseq,
                        IFNULL(ta.custid,'') ||'-'|| IFNULL(TO_VARCHAR(ta.contact),'') AS CustService,
                        postalcode AS imei,
                        ta.processingunitseq,
                        ta.tenantid,
                        st.compensationdate
                        /* --from cs_transactionaddress ta-- Sankar commented this and handled in below logic */ 
                    FROM
                        (
                            SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                                *
                            FROM
                                cs_transactionaddress
                            WHERE
                                addresstypeseq 

                                =
                                (
                                    SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                    WHERE
                                        addresstypeid = 'BILLTO'
                                )
                            ) AS ta
                        INNER JOIN
                            cs_salestransaction st
                            ON st.salestransactionseq = ta.salestransactionseq
                            AND st.processingunitseq = ta.processingunitseq
                            AND st.compensationdate = ta.compensationdate
                            AND st.tenantid = ta.tenantid
                        INNER JOIN
                            cs_eventtype et
                            ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            AND et.datatypeseq = st.eventtypeseq
                            AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')
                            --join cs_addresstype at on ta.addresstypeseq=at.addresstypeseq -- Sankar commented this and handled in above logic
                        WHERE
                            --addresstypeid='BILLTO' -- Sankar commented this and handled in above logic
                            --and  -- Sankar commented this and handled in above logic
                            ta.processingunitseq = :p_PROCESSINGUNITSEQ
                            AND st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                            AND ta.tenantid = 'STEL'
                    ) AS new
                    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_DATA_USEDIMEI' not found */
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select imei, compdate, customerid, serviceno from stel_Data_usedimei where imei(...) */
                            imei,
                            compdate,
                            customerid,
                            serviceno
                        FROM
                            stel_Data_usedimei
                        WHERE
                            imei NOT  
                            IN
                            (
                                SELECT   /* ORIGSQL: (select classifierid from stel_Classifier_tab where categorytreename='IMEI Excep(...) */
                                    classifierid
                                FROM
                                    stel_Classifier_tab
                                WHERE
                                    categorytreename = 'IMEI Exceptions'
                                    AND genericattribute1 = 'REMOVE'
                            )
                UNION
                    SELECT   /* ORIGSQL: select to_char(classifierid), genericdate1, to_char(genericattribute2) customeri(...) */
                        TO_VARCHAR(classifierid),
                        genericdate1,
                        TO_VARCHAR(genericattribute2) AS customerid,  /* ORIGSQL: to_char(genericattribute2) */
                        TO_VARCHAR(genericattribute3) AS serviceno /* --classifier has to be set up this way for 'ADD' entries */  /* ORIGSQL: to_char(genericattribute3) */
                    FROM
                        stel_Classifier_tab
                    WHERE
                        categorytreename = 'IMEI Exceptions'
                        AND genericattribute1 = 'ADD'
                ) AS old
                ON old.imei = new.imei
                AND IFNULL(old.customerid,'')||'-'||IFNULL(serviceno,'') <> new.CustService
        ) AS src
        ON (tgt.salestransactionseq = src.salestransactionseq
            AND tgt.tenantid = src.tenantid
            AND tgt.processingunitseq = src.processingunitseq
        AND tgt.compensationdate = src.compensationdate
        	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
            AND tgt.tenantid = 'STEL'
            AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
            AND tgt.pagenumber = 0
        )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute1 = 'IMEI Used Before' --EA1 is MSF Rejection Comment
            , genericboolean2 = 1
       -- WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
       --     AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
       --     AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 80','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 80', 'SH', 0, :v_rowcount, 'Running');

    /*Check imei found in VSOP*/ 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT ta.salestransactionseq, ta.cu(...) */
    MERGE INTO cs_gasalestransaction AS tgt  
        USING
        (
            SELECT   /* ORIGSQL: (select ta.salestransactionseq, ta.custid ||'-'|| ta.contact CustService, postal(...) */
                ta.salestransactionseq,
                IFNULL(ta.custid,'') ||'-'|| IFNULL(TO_VARCHAR(ta.contact),'') AS CustService,
                postalcode AS imei,
                ta.processingunitseq,
                ta.tenantid,
                st.compensationdate
                /* --from cs_transactionaddress ta -- Sankar commented this and handled in below logic */ 
            FROM
                (
                    SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                        *
                    FROM
                        cs_transactionaddress
                    WHERE
                        addresstypeseq 

                        =
                        (
                            SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                                addresstypeseq
                            FROM
                                cs_addresstype
                            WHERE
                                addresstypeid = 'BILLTO'
                        )
                    ) AS ta
                INNER JOIN
                    cs_salestransaction st
                    ON st.salestransactionseq = ta.salestransactionseq
                    AND st.processingunitseq = ta.processingunitseq
                    AND ta.compensationdate = st.compensationdate
                    AND st.tenantid = ta.tenantid
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = st.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')
                    --join cs_addresstype at -- Sankar commented this and handled in above logic
                    --on ta.addresstypeseq=at.addresstypeseq -- Sankar commented this and handled in above logic
                WHERE
                    --addresstypeid='BILLTO' -- Sankar commented this and handled in above logic
                    --and -- Sankar commented this and handled in above logic
                    ta.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND ta.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND ta.tenantid = 'STEL'
                    AND ta.postalcode NOT
                    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
                    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
                    IN
                    (
                        SELECT   /* ORIGSQL: (select genericattribute5 imei from cs_Salestransaction txn join cs_eventtype e (...) */
                            genericattribute5 AS imei
                        FROM
                            cs_Salestransaction txn
                        INNER JOIN
                            cs_eventtype e
                            ON e.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            AND e.datatypeseq = txn.eventtypeseq
                            AND e.eventtypeid IN ('VSOP IMEI')
                        WHERE
                            txn.tenantid = 'STEL'
                            AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                            --no date filter here for now - not sure if we can limit it to one or two motnhs. e.g. compdate>=add_months(v_Statdate,-2)
                    )
                ) AS src
                ON (tgt.salestransactionseq = src.salestransactionseq
                    AND tgt.tenantid = src.tenantid
                    AND tgt.processingunitseq = src.processingunitseq
                AND tgt.compensationdate = src.compensationdate
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
    WHEN MATCHED THEN
        UPDATE SET
            genericattribute1 = 'IMEI Not in VSOP' --EA1 is MSF Rejection Comment
            , genericboolean2 = 1
        --WHERE
        --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
        --    AND tgt.tenantid = 'STEL'
         --   AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
        --    AND tgt.pagenumber = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 90','SH',0,v_rowcount,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 90', 'SH', 0, :v_rowcount, 'Running');

    /**
    
    ADD TO USED IMEI LIST
    **/   

    /* ORIGSQL: insert into stel_Data_usedimei select postalcode imei, st.compensationdate, ta.c(...) */
    INSERT INTO stel_Data_usedimei
        SELECT   /* ORIGSQL: select postalcode imei, st.compensationdate, ta.custid, ta.contact CustService f(...) */
            postalcode AS imei,
            st.compensationdate,
            ta.custid,
            ta.contact AS CustService
            /* --from cs_transactionaddress ta -- Sankar commented this and handled in below logic */
        FROM
            (
                SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                    *
                FROM
                    cs_transactionaddress
                WHERE
                    addresstypeseq

                    =
                    (
                        SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') */
                            addresstypeseq
                        FROM
                            cs_addresstype
                        WHERE
                            addresstypeid = 'BILLTO'
                    )) AS ta
                INNER JOIN
                    cs_salestransaction st
                    ON st.salestransactionseq = ta.salestransactionseq
                    AND st.processingunitseq = ta.processingunitseq
                    AND ta.compensationdate = st.compensationdate
                    AND st.tenantid = ta.tenantid
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = st.eventtypeseq
                    AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')
                    --join cs_addresstype at
                    --on ta.addresstypeseq=at.addresstypeseq
                WHERE
                    --addresstypeid='BILLTO'
                    --and
                    ta.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND st.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND ta.tenantid = 'STEL'
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

            /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 100','SH',0,v_rowcount,'Running') */
            CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 100', 'SH', 0, :v_rowcount, 'Running');

            /* ORIGSQL: commit; */
            COMMIT;
            --
            /**
            
            Protection PERiod logic
             **/ 
            /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
            /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT CASE WHEN txn.accountingdate (...) */
            MERGE INTO cs_gasalestransaction AS tgt  
                USING
                (
                    SELECT   /* ORIGSQL: (select CASE WHEN txn.accountingdate - sap.startdate <=28 THEN 'Y' ELSE 'N' END (...) */
                        CASE
                            WHEN (SECONDS_BETWEEN(sap.startdate,txn.accountingdate)/86400) <= 28  /* ORIGSQL: txn.accountingdate - sap.startdate */
                            THEN 'Y'
                            ELSE 'N'
                        END
                        AS protectionflag,
                        txn.salestransactionseq,
                        txn.compensationdate,
                        txn.tenantid,
                        sap.txprice,
                        sap.costprice,
                        txn.processingunitseq
                    FROM
                        CS_SALESTRANSACTION txn
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = txn.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2', 'Mobile Submitted')--affects TEPL as well 
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select GENERICATTRIBUTE1 stockcode, effectivestartdate startdate, effectiveendd(...) */
                                GENERICATTRIBUTE1 AS stockcode,
                                effectivestartdate AS startdate,
                                effectiveenddate AS enddate,
                                genericnumber1 AS txprice,
                                genericnumber2 AS costprice,
                                GENERICATTRIBUTE2 AS customer
                            FROM
                                stel_classifier_tab
                            WHERE
                                categorytreename = 'SAP Equipment Price'
                        ) AS sap
                        ON sap.stockcode = txn.genericattribute28
                        AND txn.accountingdate BETWEEN sap.startdate AND TO_DATE(ADD_SECONDS(IFNULL(sap.enddate,TO_DATE('22000101','YYYYMMDD')),(86400*-1)))   /* ORIGSQL: nvl(sap.enddate,to_date('22000101','YYYYMMDD')) */
                                                                                                                                                               /* ORIGSQL: IFNULL(sap.enddate,TO_DATE('22000101','YYYYMMDD')) -1 */
                    INNER JOIN
                        stel_participant par
                        ON par.payeeid = txn.genericattribute3
                        AND par.genericattribute12 = sap.customer
                        AND txn.accountingdate BETWEEN par.effectivestartdate AND TO_DATE(ADD_SECONDS(par.effectiveenddate,(86400*-1)))   /* ORIGSQL: par.effectiveenddate-1 */
                    WHERE
                        1 = 1
                        AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND txn.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND txn.tenantid = 'STEL'
                ) AS src
                ON (Src.salestransactionseq = tgt.salestransactionseq
                    AND tgt.compensationdate = src.compensationdate
                    AND tgt.processingunitseq = src.processingunitseq
                AND src.tenantid = tgt.tenantid
                	AND tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                    AND tgt.tenantid = 'STEL'
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    AND tgt.pagenumber = 0
                )
            WHEN MATCHED THEN
                UPDATE SET
                    /*tgt.protectionflag=src.protectionflag,
                    tgt.saptxprice=txnprice,
                    tgt.sapcostprice=costprice
                    */
                    tgt.genericboolean1 = src.protectionflag,
                    tgt.genericnumber1 = txprice, tgt.unittypeforgenericnumber1 = :v_sgd, --sap transfer price
                    tgt.genericnumber2 = costprice, tgt.unittypeforgenericnumber2 = :v_sgd --sap cost price
                --WHERE
                --    tgt.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                --    AND tgt.tenantid = 'STEL'
                --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                --    AND tgt.pagenumber = 0
                    ;

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 110','SH',0,v_rowcount,'Running') */
            CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 110', 'SH', 0, :v_rowcount, 'Running');

            /*
            If model is price protected:
            -    Get the VSOP transfer cost and stamp it on the transaction
            If model is not cost protected (any more)
            -    Get the latest SAP Price (irrespective of the actual transfer cost that is in VSOP), since the protection has ended, and stamp it on the transaction
            */ 
            /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
            /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT txn.salestransactionseq, to_n(...) */
            MERGE INTO cs_gasalestransaction AS tgt 
                /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_GASALESTRANSACTION' not found */
                USING
                (
                    SELECT   /* ORIGSQL: (select txn.salestransactionseq, to_number(nvl(vsop.genericattribute12,'0')) tra(...) */
                        txn.salestransactionseq,
                        TO_DECIMAL(IFNULL(vsop.genericattribute12, '0'),38,18) AS transfercost  /* ORIGSQL: to_number(nvl(vsop.genericattribute12,'0')) */
                    FROM
                        cs_salestransaction txn
                    INNER JOIN
                        cs_gasalestransaction gatxn
                        ON gatxn.salestransactionseq = txn.salestransactionseq
                        AND gatxn.tenantid = 'STEL'
                        AND gatxn.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND gatxn.pagenumber = 0
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = txn.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')
                        -- join cs_transactionaddress tad -- Sankar commented this and handled in below logic 
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (select * from cs_transactionaddress where addresstypeseq = (SELECT addresstypes(...) */
                                *
                            FROM
                                cs_transactionaddress
                            WHERE
                                addresstypeseq 

                                =
                                (
                                    SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype where addresstypeid='SHIPTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                    WHERE
                                        addresstypeid = 'SHIPTO'
                                )
                            ) AS tad
                            ON txn.salestransactionseq = tad.salestransactionseq
                            AND tad.tenantid = 'STEL'
                            AND tad.processingunitseq = :p_PROCESSINGUNITSEQ
                            -- and tad.addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO') -- Sankar commented this and handled in above logic
                        INNER JOIN
                            cs_salestransaction vsop
                            ON tad.postalcode = vsop.genericattribute5 --chimei --assuming only this join is needed
                        INNER JOIN
                            cs_eventtype e
                            ON e.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            AND e.datatypeseq = vsop.eventtypeseq
                            AND e.eventtypeid IN ('VSOP IMEI')
                        WHERE
                            IFNULL(/*txn.imeimatch*/gatxn.genericboolean2,0) = 0  /* ORIGSQL: nvl(gatxn.genericboolean2,0) */
                            AND txn.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                            AND txn.tenantid = 'STEL'
                            AND txn.processingunitseq = :p_PROCESSINGUNITSEQ
                    ) AS src
                    ON (Src.salestransactionseq = tgt.salestransactionseq
                    	AND tgt.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND tgt.tenantid = 'STEL'
                        AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                        AND tgt.pagenumber = 0
                    )
            WHEN MATCHED THEN
                UPDATE SET
                    --tgt.vsopcost=src.transfercost
                    tgt.genericnumber3 = src.transfercost, tgt.unittypeforgenericnumber3 = :v_sgd
                --WHERE
                --    tgt.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                --    AND tgt.tenantid = 'STEL'
                --   AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                --    AND tgt.pagenumber = 0
                    ;

            /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 120','SH',0,v_rowcount,'Running') */
            CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 120', 'SH', 0, :v_rowcount, 'Running');

            /*
            Note that for IMEI replacements, the transfer price will not be in VSOP, as no transaction come in through VSOP
            in this case, the SAP price will be used, using the Order Entry Date to look up the price.
            */   

            /* ORIGSQL: update cs_gasalestransaction tgt SET genericnumber4= CASE WHEN genericboolean1=1(...) */
            UPDATE cs_gasalestransaction tgt
                SET
                /* ORIGSQL: genericnumber4 = */
                genericnumber4 =
                CASE 
                    WHEN genericboolean1 = 1
                    THEN IFNULL(genericnumber3,genericnumber1)  /* ORIGSQL: nvl(genericnumber3,genericnumber1) */
                    ELSE IFNULL(genericnumber2,0)  /* ORIGSQL: nvl(genericnumber2,0) */
                END,
                /* ORIGSQL: tgt.unittypeforgenericnumber4 = */
                unittypeforgenericnumber4 = :v_sgd
                /*
                finalCost =
                case when protectionflag='Y' then nvl(vsopcost,saptxprice)
                when protectionflag = 'N' then saptxprice
                else 0 end
                */
            WHERE
                tgt.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                AND tgt.tenantid = 'STEL'
                AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                AND tgt.pagenumber = 0
                AND tgt.salestransactionseq   
                IN
                (
                    SELECT   /* ORIGSQL: (select salestransactionseq from cs_Salestransaction st join cs_eventtype et on (...) */
                        salestransactionseq
                    FROM
                        cs_Salestransaction st
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = st.eventtypeseq
                        AND et.eventtypeid IN ('Mobile SER R1','Mobile SER R2')
                        AND st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND st.tenantid = 'STEL'
                        AND st.processingunitseq = :p_PROCESSINGUNITSEQ
                );

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 130','SH',0,v_rowcount,'Running') */
            CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 130', 'SH', 0, :v_rowcount, 'Running');   

            /* ORIGSQL: update cs_gasalestransaction tgt SET genericnumber4=genericnumber2, tgt.unittype(...) */
            UPDATE cs_gasalestransaction tgt
                SET
                /* ORIGSQL: genericnumber4 = */
                genericnumber4 = genericnumber2/* --finalcost=sapcostprice */,
                /* ORIGSQL: tgt.unittypeforgenericnumber4 = */
                unittypeforgenericnumber4 = :v_sgd
            WHERE
                tgt.compensationdate BETWEEN :v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                AND tgt.tenantid = 'STEL'
                AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                AND tgt.pagenumber = 0
                AND tgt.salestransactionseq   
                IN
                (
                    SELECT   /* ORIGSQL: (select salestransactionseq from cs_Salestransaction st join cs_eventtype et on (...) */
                        salestransactionseq
                    FROM
                        cs_Salestransaction st
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = st.eventtypeseq
                        AND et.eventtypeid IN ('Mobile Submitted')--FOR TEPL
                        AND st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND st.tenantid = 'STEL'
                        AND st.processingunitseq = :p_PROCESSINGUNITSEQ
                );

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 140','SH',0,v_rowcount,'Running') */
            CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 140', 'SH', 0, :v_rowcount, 'Running');

            /**
            
             Stamp close date on R2 txns and R1 txns
             **/ 

            /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
            /* ORIGSQL: merge into cs_gasalestransaction tgt using (SELECT s.salestransactionseq, s.tena(...) */
            MERGE INTO cs_gasalestransaction AS tgt  
                USING
                (
                    SELECT   /* ORIGSQL: (SELECT s.salestransactionseq, s.tenantid,s.processingunitseq, x.compdate CloseD(...) */
                        s.salestransactionseq,
                        s.tenantid,
                        s.processingunitseq,
                        x.compdate AS CloseDate,
                        'CLOSED' AS Status,
                        e.eventtypeid
                    FROM
                        cs_salestransaction S
                    INNER JOIN
                        cs_eventtype e
                        ON S.eventtypeseq = e.datatypeseq
                        AND e.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                    INNER JOIN
                        (
                            SELECT   /* ORIGSQL: (SELECT st.alternateordernumber, st.genericattribute6, MAX(St.compensationdate) (...) */
                                --might need tuning, since GA6,alt order number are not indexed.
                                --might help to move a list of closed orders out to a tem table filtered for event type, GA9='M' etc.
                                ---------------------------------------
                                -- for R1, it should be in the same month
                                -- for R2, it whould be within the last 2-3 months
                                st.alternateordernumber,
                                st.genericattribute6,
                                MAX(St.compensationdate) AS compdate
                            FROM
                                cs_Salestransaction st
                            INNER JOIN
                                cs_eventtype et
                                ON st.eventtypeseq =
                                et.datatypeseq
                                AND et.removedate > CURRENT_TIMESTAMP   /* ORIGSQL: SYSDATE */
                            WHERE
                                st.datasource = 'BCC'
                                AND (et.eventtypeid = 'Mobile Closed')
                                AND st.processingunitseq = :p_PROCESSINGUNITSEQ
                                AND st.tenantid = 'STEL'
                                AND st.compensationdate >= ADD_MONTHS(:v_StartDate,-3)
                            GROUP BY
                                st.alternateordernumber,
                                st.genericattribute6
                        ) AS x
                        ON x.genericattribute6 = s.genericattribute6
                        AND x.alternateordernumber =
                        s.alternateordernumber
                    WHERE
                        e.eventtypeid IN ('Mobile SER R1', 'Mobile SER R2')
                        AND s.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
                        AND s.tenantid = 'STEL'
                        AND s.processingunitseq = :p_PROCESSINGUNITSEQ
                ) AS src
                ON (tgt.salestransactionseq = src.salestransactionseq
                    AND tgt.tenantid = src.tenantid
                AND tgt.processingunitseq = src.processingunitseq
                	AND tgt.tenantid = 'STEL'
                    AND tgt.pagenumber = 0
                    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.genericdate1 = src.CloseDate
                    --this will update for R1 as well, but the rule should filter for Month (Submit date)= Month(close date)
                    --keeping that logic in rules rather than SH
                --WHERE
                 --   tgt.tenantid = 'STEL'
                 --   AND tgt.pagenumber = 0
                --    AND tgt.processingunitseq = :p_PROCESSINGUNITSEQ
                    ;

            v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

            /* ORIGSQL: stel_Sp_logger('SH_RFC_MOBILESER 150','SH',0,v_rowcount,'Running') */
            CALL EXT.STEL_SP_LOGGER('SH_RFC_MOBILESER 150', 'SH', 0, :v_rowcount, 'Running');

            /* ORIGSQL: commit; */
            COMMIT;
END