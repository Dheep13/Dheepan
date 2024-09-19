CREATE PROCEDURE EXT.SP_INBOUND_POST_MOBILESERR2
(in in_processingUnitName nvarchar(50))
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cutoffday DECIMAL(38,10);  /* ORIGSQL: v_cutoffday NUMBER; */
    DECLARE v_oppr ROW LIKE ext.inbound_cfg_BCC_Txn;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_BCC_Txn' not found (for %ROWTYPE declaration) */
    DECLARE v_inbound_cfg_parameter ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */
    DECLARE v_maxseq DECIMAL(38,10);  /* ORIGSQL: v_maxseq number; */
    DECLARE v_clawbackperiod DECIMAL(38,10);  /* ORIGSQL: v_clawbackperiod number; */
    DECLARE v_filedate TIMESTAMP;  /* ORIGSQL: v_filedate date; */
    DECLARE v_proc_name VARCHAR(127) = 'sp_inbound_Post_MobileSERR2';  /* ORIGSQL: v_proc_name varchar2(127):='sp_inbound_Post_MobileSERR2'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */
    DECLARE v_event VARCHAR(255) = 'Mobile SER R1';  /* ORIGSQL: v_event varchar2(255):='Mobile SER R1'; */
    DECLARE v_eventNew VARCHAR(255) = 'Mobile SER R2';  /* ORIGSQL: v_eventNew varchar2(255):='Mobile SER R2'; */
    DECLARE v_offsetMonths DECIMAL(38,10) = 2;  /* ORIGSQL: v_offsetMonths number:=2; */
    DECLARE v_minCompDate TIMESTAMP;  /* ORIGSQL: v_minCompDate date; */
    DECLARE v_maxCompDate TIMESTAMP;  /* ORIGSQL: v_maxCompDate date; */
    DECLARE v_Start TIMESTAMP;  /* ORIGSQL: v_Start date; */
    DECLARE v_end TIMESTAMP;  /* ORIGSQL: v_end date; */
    DECLARE v_filename VARCHAR(250);  /* ORIGSQL: v_filename varchar2(250); */
    DECLARE p_txnmonth TIMESTAMP;  /* ORIGSQL: p_txnmonth date; */
    DECLARE v_prmtr  ROW LIKE ext.inbound_cfg_parameter;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        ext.inbound_cfg_parameter;

    --This is to copy the data when R1 closude is triggered.
    /*
    Copy exactly as-is in COMM
    Two changes
    1. Event type is R2
    2. Comp Date is moved forward by x months
    
    */
    --FIELD 1 is expected to be YYYY-MM
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */

   SELECT 
    TO_DATE(MAX(FIELD1) || '-01', 'YYYY-MM-DD') AS p_txnmonth, 
    TO_DATE(MAX(FIELD1) || '-01', 'YYYY-MM-DD') AS v_Start, 
    LAST_DAY(TO_DATE(MAX(FIELD1) || '-01', 'YYYY-MM-DD')) AS v_End
FROM 
   ext.inbound_data_Staging;

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Proc Started:' || v_inbound_cfg_parameter.file(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(:v_proc_name || 'Proc Started:'||
    :v_inbound_cfg_parameter.file_type||'-FileName:'|| :v_inbound_cfg_parameter.file_name || '-Date:'|| :v_inbound_cfg_parameter.file_date,1,255) 
        , 'Month, semiMonth '|| TO_VARCHAR(:p_txnmonth,'YYYYMMDD')   /* ORIGSQL: SUBSTR(v_proc_name || 'Proc Started:' || v_inbound_cfg_parameter.file_type || '-(...) */
        , :v_rowcount, NULL, NULL);  /* ORIGSQL: to_Char(p_txnmonth,'YYYYMMDD') */

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_TXN' not found */

    /* ORIGSQL: INSERT INTO inbound_data_txn (filedate, filename, recordstatus, downloaded, orde(...) */
    INSERT INTO ext.inbound_data_txn
        (
            filedate, filename, recordstatus, downloaded, orderid, linenumber,
            sublinenumber, eventtypeid, accountingdate, productid, productname, productdescription,
            value, unittypeforvalue, numberofunits, unitvalue, unittypeforunitvalue, compensationdate,
            paymentterms, ponumber, channel, alternateordernumber, datasource, nativecurrency,
            nativecurrencyamount, discountpercent, discounttype, billtocustid, billtocontact, billtocompany,
            billtoareacode, billtophone, billtofax, billtoaddress1, billtoaddress2, billtoaddress3,
            billtocity, billtostate, billtocountry, billtopostalcode, billtoindustry, billtogeography,
            shiptocustid, shiptocontact, shiptocompany, shiptoareacode, shiptophone, shiptofax,
            shiptoaddress1, shiptoaddress2, shiptoaddress3, shiptocity, shiptostate, shiptocountry,
            shiptopostalcode, shiptoindustry, shiptogeography, othertocustid, othertocontact, othertocompany,
            othertoareacode, othertophone, othertofax, othertoaddress1, othertoaddress2, othertoaddress3,
            othertocity, othertostate, othertocountry, othertopostalcode, othertoindustry, othertogeography,
            reasonid, comments, stageprocessdate, stageprocessflag, businessunitname, businessunitmap,
            genericattribute1, genericattribute2, genericattribute3, genericattribute4, genericattribute5, genericattribute6,
            genericattribute7, genericattribute8, genericattribute9, genericattribute10, genericattribute11, genericattribute12,
            genericattribute13, genericattribute14, genericattribute15, genericattribute16, genericattribute17, genericattribute18,
            genericattribute19, genericattribute20, genericattribute21, genericattribute22, genericattribute23, genericattribute24,
            genericattribute25, genericattribute26, genericattribute27, genericattribute28, genericattribute29, genericattribute30,
            genericattribute31, genericattribute32, genericnumber1, unittypeforgenericnumber1, genericnumber2, unittypeforgenericnumber2,
            genericnumber3, unittypeforgenericnumber3, genericnumber4, unittypeforgenericnumber4, genericnumber5, unittypeforgenericnumber5,
            genericnumber6, unittypeforgenericnumber6, genericdate1, genericdate2, genericdate3, genericdate4,
            genericdate5, genericdate6, genericboolean1, genericboolean2, genericboolean3, genericboolean4,
            genericboolean5, genericboolean6
        )
        SELECT   /* ORIGSQL: select v_filedate, v_filename, 0 recordstatus, 0 downloaded, so.orderid, st.line(...) */
            :v_filedate,
            :v_filename,
            0 AS recordstatus,
            0 AS downloaded,
            so.orderid,
            st.linenumber,
            st.sublinenumber,
            :v_eventNew AS eventtypeid,
            st.accountingdate,
            st.productid,
            st.productname,
            st.productdescription,
            st.value,
            st.unittypeforvalue,
            st.numberofunits,
            st.unitvalue,
            st.unittypeforunitvalue,
            ADD_MONTHS(st.compensationdate,:v_offsetMonths),
            st.paymentterms,
            st.ponumber,
            st.channel,
            st.alternateordernumber,
            st.datasource,
            st.nativecurrency,
            st.nativecurrencyamount,
            st.discountpercent,
            st.discounttype,
            bt.custid AS BTCUSTID,
            bt.contact AS BTCONTACT,
            bt.company AS BTCOMPANY,
            bt.areacode AS BTAREACODE,
            bt.phone AS BTPHONE,
            bt.fax AS BTFAX,
            bt.address1 AS BTADDRESS1,
            bt.address2 AS BTADDRESS2,
            bt.address3 AS BTADDRESS3,
            bt.city AS BTCITY,
            bt.state AS BTSTATE,
            bt.country AS BTCOUNTRY,
            bt.postalcode AS BTPOSTALCODE,
            bt.industry AS BTINDUSTRY,
            bt.geography AS BTGEOGRAPHY,
            sh.custid AS SHCUSTID,
            sh.contact AS SHCONTACT,
            sh.company AS SHCOMPANY,
            sh.areacode AS SHAREACODE,
            sh.phone AS SHPHONE,
            sh.fax AS SHFAX,
            sh.address1 AS SHADDRESS1,
            sh.address2 AS SHADDRESS2,
            sh.address3 AS SHADDRESS3,
            sh.city AS SHCITY,
            sh.state AS SHSTATE,
            sh.country AS SHCOUNTRY,
            sh.postalcode AS SHPOSTALCODE,
            sh.industry AS SHINDUSTRY,
            sh.geography AS SHGEOGRAPHY,
            ot.custid AS OTCUSTID,
            ot.contact AS OTCONTACT,
            ot.company AS OTCOMPANY,
            ot.areacode AS OTAREACODE,
            ot.phone AS OTPHONE,
            ot.fax AS OTFAX,
            ot.address1 AS OTADDRESS1,
            ot.address2 AS OTADDRESS2,
            ot.address3 AS OTADDRESS3,
            ot.city AS OTCITY,
            ot.state AS OTSTATE,
            ot.country AS OTCOUNTRY,
            ot.postalcode AS OTPOSTALCODE,
            ot.industry AS OTINDUSTRY,
            ot.geography AS OTGEOGRAPHY,
            NULL AS reasonid,
            st.comments,
            NULL AS stageprocessdate,
            NULL AS stageprocessflag,
            'ConSales_External' AS businessunitname,
            NULL AS businessunitmap,
            st.genericattribute1,
            st.genericattribute2,
            st.genericattribute3,
            st.genericattribute4,
            st.genericattribute5,
            st.genericattribute6,
            st.genericattribute7,
            st.genericattribute8,
            st.genericattribute9,
            st.genericattribute10,
            st.genericattribute11,
            st.genericattribute12,
            st.genericattribute13,
            st.genericattribute14,
            st.genericattribute15,
            st.genericattribute16,
            st.genericattribute17,
            st.genericattribute18,
            st.genericattribute19,
            st.genericattribute20,
            st.genericattribute21,
            st.genericattribute22,
            st.genericattribute23,
            st.genericattribute24,
            st.genericattribute25,
            st.genericattribute26,
            st.genericattribute27,
            st.genericattribute28,
            st.genericattribute29,
            st.genericattribute30,
            st.genericattribute31,
            st.genericattribute32,
            st.genericnumber1,
            ut1.name AS unittypeforgenericnumber1,
            st.genericnumber2,
            ut2.name AS unittypeforgenericnumber2,
            st.genericnumber3,
            ut3.name AS unittypeforgenericnumber3,
            st.genericnumber4,
            ut4.name AS unittypeforgenericnumber4,
            st.genericnumber5,
            ut5.name AS unittypeforgenericnumber5,
            st.genericnumber6,
            ut6.name AS unittypeforgenericnumber6,
            st.genericdate1,
            st.genericdate2,
            st.genericdate3,
            st.genericdate4,
            st.genericdate5,
            st.genericdate6,
            st.genericboolean1,
            st.genericboolean2,
            st.genericboolean3,
            st.genericboolean4,
            st.genericboolean5,
            st.genericboolean6
        FROM
           cs_Salestransaction st
        INNER JOIN
           CS_Salesorder so
            ON so.salesorderseq = st.salesorderseq
            AND so.removedate = TO_DATE('22000101','YYYYMMDD')
            AND so.processingunitseq = st.processingunitseq
            AND so.tenantid = st.tenantid
        INNER JOIN
           cs_eventtype et
            ON et.datatypeseq = st.eventtypeseq
            AND et.removedate = TO_DATE('22000101','YYYYMMDD')
            AND et.tenantid = st.tenantid
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select * from cs_transactionaddress@stelext where addresstypeseq in (SELECT add(...) */
                    *
                FROM
                   cs_transactionaddress
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    addresstypeseq IN
                    (
                        SELECT   /* ORIGSQL: (select addresstypeseq from cs_addresstype@stelext where addresstypeid='BILLTO') */
                            addresstypeseq
                        FROM
                           cs_addresstype
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_addresstype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_addresstype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        WHERE
                            addresstypeid = 'BILLTO'
                    )
                ) AS bt
                ON bt.salestransactionseq = st.salestransactionseq
                AND bt.processingunitseq = st.processingunitseq
                AND bt.tenantid = st.tenantid
                AND bt.compensationdate = st.compensationdate
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select * from cs_transactionaddress@stelext where addresstypeseq in (SELECT add(...) */
                        *
                    FROM
                       cs_transactionaddress
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        addresstypeseq IN
                        (
                            SELECT   /* ORIGSQL: (select addresstypeseq from cs_addresstype@stelext where addresstypeid='SHIPTO') */
                                addresstypeseq
                            FROM
                               cs_addresstype
                                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_addresstype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_addresstype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            WHERE
                                addresstypeid = 'SHIPTO'
                        )
                    ) AS sh
                    ON sh.salestransactionseq = st.salestransactionseq
                    AND sh.processingunitseq = st.processingunitseq
                    AND sh.tenantid = st.tenantid
                    AND sh.compensationdate = st.compensationdate
                LEFT OUTER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress@stelext where addresstypeseq in (SELECT add(...) */
                            *
                        FROM
                           cs_transactionaddress
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        WHERE
                            addresstypeseq IN
                            (
                                SELECT   /* ORIGSQL: (select addresstypeseq from cs_addresstype@stelext where addresstypeid='OTHERTO'(...) */
                                    addresstypeseq
                                FROM
                                   cs_addresstype
                                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_addresstype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_addresstype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                WHERE
                                    addresstypeid = 'OTHERTO'
                            )
                        ) AS ot
                        ON ot.salestransactionseq = st.salestransactionseq
                        AND ot.processingunitseq = st.processingunitseq
                        AND ot.tenantid = st.tenantid
                        AND ot.compensationdate = st.compensationdate
                    LEFT OUTER JOIN
                       cs_unittype ut1
                        ON st.unittypeforgenericnumber1 = ut1.unittypeseq
                        AND ut1.removedate = TO_DATE('22000101','YYYYMMDD')
                        AND ut1.tenantid = st.tenantid
                    LEFT OUTER JOIN
                       cs_unittype ut2
                        ON st.unittypeforgenericnumber2 = ut2.unittypeseq
                        AND ut2.removedate = TO_DATE('22000101','YYYYMMDD')
                        AND ut2.tenantid = st.tenantid
                    LEFT OUTER JOIN
                       cs_unittype ut3
                        ON st.unittypeforgenericnumber3 = ut3.unittypeseq
                        AND ut3.removedate = TO_DATE('22000101','YYYYMMDD')
                        AND ut3.tenantid = st.tenantid
                    LEFT OUTER JOIN
                       cs_unittype ut4
                        ON st.unittypeforgenericnumber4 = ut4.unittypeseq
                        AND ut4.removedate = TO_DATE('22000101','YYYYMMDD')
                        AND ut4.tenantid = st.tenantid
                    LEFT OUTER JOIN
                       cs_unittype ut5
                        ON st.unittypeforgenericnumber5 = ut5.unittypeseq
                        AND ut5.removedate = TO_DATE('22000101','YYYYMMDD')
                        AND ut5.tenantid = st.tenantid
                    LEFT OUTER JOIN
                       cs_unittype ut6
                        ON st.unittypeforgenericnumber6 = ut6.unittypeseq
                        AND ut6.removedate = TO_DATE('22000101','YYYYMMDD')
                        AND ut6.tenantid = st.tenantid
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_eventtype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.CS_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.CS_Salesorder'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        st.tenantid = 'STEL'
                        AND st.processingunitseq =
                        (
                            SELECT   /* ORIGSQL: (Select processingunitseq from cs_processingunit@stelext where name='Singtel_PU'(...) */
                                processingunitseq
                            FROM
                               cs_processingunit
                                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_processingunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_processingunit'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            WHERE
                                name = 'Singtel_PU'
                        )
                        AND et.eventtypeid = :v_event
                        AND st.compensationdate BETWEEN :v_Start AND :v_end;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_ASSIGNMENT' not found */

    /* ORIGSQL: INSERT INTO inbound_data_assignment (filedate, filename, recordstatus, downloade(...) */
    INSERT INTO EXT.inbound_data_assignment
        (
            filedate, filename, recordstatus, downloaded, orderid, linenumber,
            sublinenumber, eventtypeid, payeeid, payeetype, positionname, titlename,
            genericattribute1, genericattribute2, genericattribute3, genericattribute4, genericattribute5, genericattribute6,
            genericattribute7, genericattribute8, genericattribute9, genericattribute10, genericattribute11, genericattribute12,
            genericattribute13, genericattribute14, genericattribute15, genericattribute16, genericnumber1, unittypeforgenericnumber1,
            genericnumber2, unittypeforgenericnumber2, genericnumber3, unittypeforgenericnumber3, genericnumber4, unittypeforgenericnumber4,
            genericnumber5, unittypeforgenericnumber5, genericnumber6, unittypeforgenericnumber6, genericdate1, genericdate2,
            genericdate3, genericdate4, genericdate5, genericdate6, genericboolean1, genericboolean2,
            genericboolean3, genericboolean4, genericboolean5, genericboolean6
        )
        SELECT   /* ORIGSQL: SELECT v_filedate, v_filename, 0 recordstatus, 0 downloaded, SO.orderid, ST.line(...) */
            :v_filedate,
            :v_filename,
            0 AS recordstatus,
            0 AS downloaded,
            SO.orderid,
            ST.linenumber,
            ST.sublinenumber,
            :v_eventNew AS eventtypeid,
            TA.payeeid,
            NULL AS payeetype,
            TA.positionname,
            TA.titlename,
            TA.genericattribute1,
            TA.genericattribute2,
            TA.genericattribute3,
            TA.genericattribute4,
            TA.genericattribute5,
            TA.genericattribute6,
            TA.genericattribute7,
            TA.genericattribute8,
            TA.genericattribute9,
            TA.genericattribute10,
            TA.genericattribute11,
            TA.genericattribute12,
            TA.genericattribute13,
            TA.genericattribute14,
            TA.genericattribute15,
            TA.genericattribute16,
            TA.genericnumber1,
            UT1.NAME AS unittypeforgenericnumber1,
            TA.genericnumber2,
            UT2.NAME AS unittypeforgenericnumber2,
            TA.genericnumber3,
            UT3.NAME AS unittypeforgenericnumber3,
            TA.genericnumber4,
            UT4.NAME AS unittypeforgenericnumber4,
            TA.genericnumber5,
            UT5.NAME AS unittypeforgenericnumber5,
            TA.genericnumber6,
            UT6.NAME AS unittypeforgenericnumber6,
            TA.genericdate1,
            TA.genericdate2,
            TA.genericdate3,
            TA.genericdate4,
            TA.genericdate5,
            TA.genericdate6,
            TA.genericboolean1,
            TA.genericboolean2,
            TA.genericboolean3,
            TA.genericboolean4,
            TA.genericboolean5,
            TA.genericboolean6
        FROM
           cs_Salestransaction st
        INNER JOIN
           cs_transactionassignment ta
            ON ta.processingunitseq = st.processingunitseq
            AND ta.tenantid = st.tenantid
            AND ta.salestransactionseq = st.salestransactionseq
            AND ta.compensationdate = st.compensationdate
        INNER JOIN
           CS_Salesorder so
            ON so.salesorderseq = st.salesorderseq
            AND so.removedate = TO_DATE('22000101','YYYYMMDD')
            AND so.processingunitseq = st.processingunitseq
            AND so.tenantid = st.tenantid
        INNER JOIN
           cs_eventtype et
            ON et.datatypeseq = st.eventtypeseq
            AND et.removedate = TO_DATE('22000101','YYYYMMDD')
            AND et.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut1
            ON ta.unittypeforgenericnumber1 = ut1.unittypeseq
            AND ut1.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut1.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut2
            ON ta.unittypeforgenericnumber2 = ut2.unittypeseq
            AND ut2.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut2.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut3
            ON ta.unittypeforgenericnumber3 = ut3.unittypeseq
            AND ut3.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut3.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut4
            ON ta.unittypeforgenericnumber4 = ut4.unittypeseq
            AND ut4.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut4.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut5
            ON ta.unittypeforgenericnumber5 = ut5.unittypeseq
            AND ut5.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut5.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut6
            ON ta.unittypeforgenericnumber6 = ut6.unittypeseq
            AND ut6.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut6.tenantid = st.tenantid
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionassignment@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionassignment'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_eventtype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.CS_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.CS_Salesorder'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            st.tenantid = 'STEL'
            AND st.processingunitseq =
            (
                SELECT   /* ORIGSQL: (Select processingunitseq from cs_processingunit@stelext where name='Singtel_PU'(...) */
                    processingunitseq
                FROM
                   cs_processingunit
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_processingunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_processingunit'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    name = 'Singtel_PU'
            )
            AND et.eventtypeid = :v_event
            AND st.compensationdate BETWEEN :v_Start AND :v_end;

    /* ORIGSQL: commit; */
    COMMIT;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_GATXN' not found */

    /* ORIGSQL: INSERT INTO inbound_data_gatxn (filedate, filename, recordstatus, downloaded, or(...) */
    INSERT INTO EXT.inbound_data_gatxn
        (
            filedate, filename, recordstatus, downloaded, orderid, linenumber,
            sublinenumber, eventtypeid, pagenumber, genericattribute1, genericattribute2, genericattribute3,
            genericattribute4, genericattribute5, genericattribute6, genericattribute7, genericattribute8, genericattribute9,
            genericattribute10, genericattribute11, genericattribute12, genericattribute13, genericattribute14, genericattribute15,
            genericattribute16, genericattribute17, genericattribute18, genericattribute19, genericattribute20, genericdate1,
            genericdate2, genericdate3, genericdate4, genericdate5, genericdate6, genericdate7,
            genericdate8, genericdate9, genericdate10, genericdate11, genericdate12, genericdate13,
            genericdate14, genericdate15, genericdate16, genericdate17, genericdate18, genericdate19,
            genericdate20, genericboolean1, genericboolean2, genericboolean3, genericboolean4, genericboolean5,
            genericboolean6, genericboolean7, genericboolean8, genericboolean9, genericboolean10, genericboolean11,
            genericboolean12, genericboolean13, genericboolean14, genericboolean15, genericboolean16, genericboolean17,
            genericboolean18, genericboolean19, genericboolean20, genericnumber1, unittypeforgenericnumber1, genericnumber2,
            unittypeforgenericnumber2, genericnumber3, unittypeforgenericnumber3, genericnumber4, unittypeforgenericnumber4, genericnumber5,
            unittypeforgenericnumber5, genericnumber6, unittypeforgenericnumber6, genericnumber7, unittypeforgenericnumber7, genericnumber8,
            unittypeforgenericnumber8, genericnumber9, unittypeforgenericnumber9, genericnumber10, unittypeforgenericnumber10, genericnumber11,
            unittypeforgenericnumber11, genericnumber12, unittypeforgenericnumber12, genericnumber13, unittypeforgenericnumber13, genericnumber14,
            unittypeforgenericnumber14, genericnumber15, unittypeforgenericnumber15, genericnumber16, unittypeforgenericnumber16, genericnumber17,
            unittypeforgenericnumber17, genericnumber18, unittypeforgenericnumber18, genericnumber19, unittypeforgenericnumber19, genericnumber20,
            unittypeforgenericnumber20
        )
        SELECT   /* ORIGSQL: select v_filedate, v_filename, 0 recordstatus, 0 downloaded, so.orderid, st.line(...) */
            :v_filedate,
            :v_filename,
            0 AS recordstatus,
            0 AS downloaded,
            so.orderid,
            st.linenumber,
            st.sublinenumber,
            et.eventtypeid,
            ga.pagenumber,
            ga.genericattribute1,
            ga.genericattribute2,
            ga.genericattribute3,
            ga.genericattribute4,
            ga.genericattribute5,
            ga.genericattribute6,
            ga.genericattribute7,
            ga.genericattribute8,
            ga.genericattribute9,
            ga.genericattribute10,
            ga.genericattribute11,
            ga.genericattribute12,
            ga.genericattribute13,
            ga.genericattribute14,
            ga.genericattribute15,
            ga.genericattribute16,
            ga.genericattribute17,
            ga.genericattribute18,
            ga.genericattribute19,
            ga.genericattribute20,
            ga.genericdate1,
            ga.genericdate2,
            ga.genericdate3,
            ga.genericdate4,
            ga.genericdate5,
            ga.genericdate6,
            ga.genericdate7,
            ga.genericdate8,
            ga.genericdate9,
            ga.genericdate10,
            ga.genericdate11,
            ga.genericdate12,
            ga.genericdate13,
            ga.genericdate14,
            ga.genericdate15,
            ga.genericdate16,
            ga.genericdate17,
            ga.genericdate18,
            ga.genericdate19,
            ga.genericdate20,
            ga.genericboolean1,
            ga.genericboolean2,
            ga.genericboolean3,
            ga.genericboolean4,
            ga.genericboolean5,
            ga.genericboolean6,
            ga.genericboolean7,
            ga.genericboolean8,
            ga.genericboolean9,
            ga.genericboolean10,
            ga.genericboolean11,
            ga.genericboolean12,
            ga.genericboolean13,
            ga.genericboolean14,
            ga.genericboolean15,
            ga.genericboolean16,
            ga.genericboolean17,
            ga.genericboolean18,
            ga.genericboolean19,
            ga.genericboolean20,
            ga.genericnumber1,
            ut1.name AS unittypeforgenericnumber1,
            ga.genericnumber2,
            ut2.name AS unittypeforgenericnumber2,
            ga.genericnumber3,
            ut3.name AS unittypeforgenericnumber3,
            ga.genericnumber4,
            ut4.name AS unittypeforgenericnumber4,
            ga.genericnumber5,
            ut5.name AS unittypeforgenericnumber5,
            ga.genericnumber6,
            ut6.name AS unittypeforgenericnumber6,
            ga.genericnumber7,
            ut7.name AS unittypeforgenericnumber7,
            ga.genericnumber8,
            ut8.name AS unittypeforgenericnumber8,
            ga.genericnumber9,
            ut9.name AS unittypeforgenericnumber9,
            ga.genericnumber10,
            ut10.name AS unittypeforgenericnumber10,
            ga.genericnumber11,
            ut11.name AS unittypeforgenericnumber11,
            ga.genericnumber12,
            ut12.name AS unittypeforgenericnumber12,
            ga.genericnumber13,
            ut13.name AS unittypeforgenericnumber13,
            ga.genericnumber14,
            ut14.name AS unittypeforgenericnumber14,
            ga.genericnumber15,
            ut15.name AS unittypeforgenericnumber15,
            ga.genericnumber16,
            ut16.name AS unittypeforgenericnumber16,
            ga.genericnumber17,
            ut17.name AS unittypeforgenericnumber17,
            ga.genericnumber18,
            ut18.name AS unittypeforgenericnumber18,
            ga.genericnumber19,
            ut19.name AS unittypeforgenericnumber19,
            ga.genericnumber20,
            ut20.name AS unittypeforgenericnumber20
        FROM
           cs_Salestransaction st
        INNER JOIN
           cs_gasalestransaction ga
            ON ga.processingunitseq = st.processingunitseq
            AND ga.tenantid = st.tenantid
            AND ga.salestransactionseq = st.salestransactionseq
            AND ga.compensationdate = st.compensationdate
        INNER JOIN
           CS_Salesorder so
            ON so.salesorderseq = st.salesorderseq
            AND so.removedate = TO_DATE('22000101','YYYYMMDD')
            AND so.processingunitseq = st.processingunitseq
            AND so.tenantid = st.tenantid
        INNER JOIN
           cs_eventtype et
            ON et.datatypeseq = st.eventtypeseq
            AND et.removedate = TO_DATE('22000101','YYYYMMDD')
            AND et.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut1
            ON ga.unittypeforgenericnumber1 = ut1.unittypeseq
            AND ut1.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut1.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut2
            ON ga.unittypeforgenericnumber2 = ut2.unittypeseq
            AND ut2.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut2.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut3
            ON ga.unittypeforgenericnumber3 = ut3.unittypeseq
            AND ut3.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut3.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut4
            ON ga.unittypeforgenericnumber4 = ut4.unittypeseq
            AND ut4.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut4.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut5
            ON ga.unittypeforgenericnumber5 = ut5.unittypeseq
            AND ut5.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut5.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut6
            ON ga.unittypeforgenericnumber6 = ut6.unittypeseq
            AND ut6.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut6.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut7
            ON ga.unittypeforgenericnumber7 = ut7.unittypeseq
            AND ut7.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut7.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut8
            ON ga.unittypeforgenericnumber8 = ut8.unittypeseq
            AND ut8.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut8.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut9
            ON ga.unittypeforgenericnumber9 = ut9.unittypeseq
            AND ut9.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut9.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut10
            ON ga.unittypeforgenericnumber10 = ut10.unittypeseq
            AND ut10.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut10.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut11
            ON ga.unittypeforgenericnumber11 = ut11.unittypeseq
            AND ut11.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut11.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut12
            ON ga.unittypeforgenericnumber12 = ut12.unittypeseq
            AND ut12.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut12.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut13
            ON ga.unittypeforgenericnumber13 = ut13.unittypeseq
            AND ut13.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut13.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut14
            ON ga.unittypeforgenericnumber14 = ut2.unittypeseq
            AND ut14.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut14.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut15
            ON ga.unittypeforgenericnumber15 = ut15.unittypeseq
            AND ut15.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut15.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut16
            ON ga.unittypeforgenericnumber16 = ut16.unittypeseq
            AND ut16.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut16.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut17
            ON ga.unittypeforgenericnumber17 = ut17.unittypeseq
            AND ut17.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut17.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut18
            ON ga.unittypeforgenericnumber18 = ut18.unittypeseq
            AND ut18.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut18.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut19
            ON ga.unittypeforgenericnumber19 = ut19.unittypeseq
            AND ut19.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut19.tenantid = st.tenantid
        LEFT OUTER JOIN
           cs_unittype ut20
            ON ga.unittypeforgenericnumber20 = ut20.unittypeseq
            AND ut20.removedate = TO_DATE('22000101','YYYYMMDD')
            AND ut20.tenantid = st.tenantid
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_unittype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_unittype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_gasalestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_gasalestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_eventtype'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salestransaction'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.CS_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.CS_Salesorder'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            st.tenantid = 'STEL'
            AND st.processingunitseq =
            (
                SELECT   /* ORIGSQL: (Select processingunitseq from cs_processingunit@stelext where name='Singtel_PU'(...) */
                    processingunitseq
                FROM
                   cs_processingunit
                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_processingunit@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_processingunit'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                WHERE
                    -- name = 'Singtel_PU'
                    name = :in_processingUnitName
            )
            AND et.eventtypeid = :v_event
            AND st.compensationdate BETWEEN :v_Start AND :v_end;

    /* ORIGSQL: commit; */
    COMMIT;
END