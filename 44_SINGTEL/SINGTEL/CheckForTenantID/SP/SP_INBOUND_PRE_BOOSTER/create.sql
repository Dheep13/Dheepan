CREATE PROCEDURE EXT.SP_INBOUND_PRE_BOOSTER
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rowcount BIGINT = NULL;  /* ORIGSQL: v_rowcount integer:= null; */
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_PRE_IDA';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_PRE_IDA'; */
    DECLARE v_Startdate TIMESTAMP;  /* ORIGSQL: v_Startdate date; */
    DECLARE v_Enddate TIMESTAMP;  /* ORIGSQL: v_Enddate date; */
    DECLARE p_processingunitseq DECIMAL(38,10) = 38280596832649218;  /* ORIGSQL: p_processingunitseq number:=38280596832649218; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    SELECT
        LAST_DAY(file_date),
        ADD_MONTHS(TO_DATE(ADD_SECONDS(LAST_DAY(file_Date),(86400*1))),-1)  /* ORIGSQL: LAST_DAY(file_Date) +1 */
    INTO
        v_Enddate,
        v_Startdate
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    --temp for UAT
    SELECT
        '1-nov-2017',
        '30-nov-2017'
    INTO
        v_Startdate,
        v_Enddate
    FROM
        SYS.DUMMY;  /* ORIGSQL: FROM dual ; */

    /* ORIGSQL: execute immediate 'truncate table  stel_Temp_Bundlebooster'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_TEMP_BUNDLEBOOSTER' not found */

    /* ORIGSQL: truncate table stel_Temp_Bundlebooster ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_Temp_Bundlebooster';

    /*PERIOD
    CUSTID
    SMCODE
    GENERICATTRIBUTE5
    COMPDATE
    MOBILEORDERID
    BBORDERID
    TVORDERID
    GENERICBOOLEAN1
    NUMBEROFUNITS
    */ 
    /* ORIGSQL: insert into stel_Temp_Bundlebooster select to_Char(v_Enddate-1,'DD-MON-YYYY'), m(...) */
    INSERT INTO ext.stel_Temp_Bundlebooster
        SELECT   /* ORIGSQL: select to_Char(v_Enddate-1,'DD-MON-YYYY'), m.custid, m.smcode, b.genericattribut(...) */
            TO_VARCHAR(TO_DATE(ADD_SECONDS(:v_Enddate,(86400*-1))),'DD-MON-YYYY'),  
            m.custid,
            m.smcode,
            b.genericattribute5,
            TO_VARCHAR(GREATEST(m.compdate, b.compdate, t.compdate),'DD-MON-YYYY') AS compdate,  /* ORIGSQL: to_char(greatest(m.compdate, b.compdate, t.compdate),'DD-MON-YYYY') */
            m.orderid,
            b.orderid,
            t.orderid,
            0,
            1
           /* m.alt,
            b.alt,
            t.alt,
            m.dealer*/ -- Sanjay: Commenting it as there are two many values the original proc is in invalid state 
        FROM
            (
                SELECT   /* ORIGSQL: (select custid, st.genericattribute2 smcode, st.genericattribute5, MAX(st.compen(...) */
                    custid,
                    st.genericattribute2 AS smcode,
                    st.genericattribute5,
                    MAX(st.compensationdate) AS compdate,
                    MAX(so.orderid) AS orderid,
                    MAX(Alternateordernumber) AS alt,
                    MAX(st.genericattribute4) AS dealer
                FROM
                    cs_Salestransaction st
                INNER JOIN
                    cs_eventtype et
                    ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                    AND et.datatypeseq = st.eventtypeseq
                LEFT OUTER JOIN
                    (
                        SELECT   /* ORIGSQL: (select * from cs_transactionaddress@stelext where addresstypeseq = (SELECT addr(...) */
                            *
                        FROM
                            cs_transactionaddress
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        WHERE
                            addresstypeseq
                            =
                            (
                                SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype@stelext where addresstypeid='BILLTO') */
                                    addresstypeseq
                                FROM
                                    cs_addresstype
                                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_addresstype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_addresstype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                WHERE
                                    addresstypeid = 'BILLTO'
                            )
                        ) AS tad
                        ON st.salestransactionseq = tad.salestransactionseq
                        AND st.tenantid = tad.tenantid
                    INNER JOIN
                        cs_Salesorder so
                        ON so.salesorderseq = st.salesorderseq
                        AND so.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_eventtype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salesorder_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        eventtypeid = 'Mobile Submitted'
                        AND UPPER(st.genericattribute5) IN ('NEW','RECON')
                        AND UPPER(st.genericattribute22) = 'AC'
                        AND IFNULL(st.genericboolean1,0) = 0  /* ORIGSQL: nvl(st.genericboolean1,0) */
                        AND st.compensationdate BETWEEN TO_DATE(ADD_SECONDS(:v_Startdate,(86400*(-1*31)))) AND TO_DATE(ADD_SECONDS(:v_Enddate,(86400*-1)))   /* ORIGSQL: v_Startdate-31 */
                                                                                                                                                             /* ORIGSQL: v_Enddate-1 */
                        AND st.tenantid = 'STEL'
                        AND st.processingunitseq = :p_processingunitseq

                    GROUP BY
                        custid, st.genericattribute2,st.genericattribute5
                ) AS m
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select custid, st.genericattribute2 smcode, st.genericattribute5, MAX(st.compen(...) */
                        custid,
                        st.genericattribute2 AS smcode,
                        st.genericattribute5,
                        MAX(st.compensationdate) AS compdate,
                        MAX(so.orderid) AS orderid,
                        MAX(Alternateordernumber) AS alt
                    FROM
                        cs_Salestransaction st
                    INNER JOIN
                        cs_eventtype et
                        ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND et.datatypeseq = st.eventtypeseq
                    LEFT OUTER JOIN
                        (
                            SELECT   /* ORIGSQL: (select * from cs_transactionaddress@stelext where addresstypeseq = (SELECT addr(...) */
                                *
                            FROM
                                cs_transactionaddress
                                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            WHERE
                                addresstypeseq
                                =
                                (
                                    SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype@stelext where addresstypeid='BILLTO') */
                                        addresstypeseq
                                    FROM
                                        cs_addresstype
                                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_addresstype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_addresstype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                    WHERE
                                        addresstypeid = 'BILLTO'
                                )
                            ) AS tad
                            ON st.salestransactionseq = tad.salestransactionseq
                            AND st.tenantid = tad.tenantid
                        INNER JOIN
                           cs_Salesorder so
                            ON so.salesorderseq = st.salesorderseq
                            AND so.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_eventtype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salesorder_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                        WHERE
                            eventtypeid = 'BroadBand Submitted'
                            AND UPPER(st.genericattribute5) IN ('NEW','RECON')
                            AND UPPER(st.genericattribute22) = 'AC'
                            AND IFNULL(st.genericboolean1,0) = 0  /* ORIGSQL: nvl(st.genericboolean1,0) */
                            AND st.compensationdate BETWEEN TO_DATE(ADD_SECONDS(:v_Startdate,(86400*(-1*31)))) AND TO_DATE(ADD_SECONDS(:v_Enddate,(86400*-1)))   /* ORIGSQL: v_Startdate-31 */
                                                                                                                                                                 /* ORIGSQL: v_Enddate-1 */
                            AND st.tenantid = 'STEL'
                            AND st.processingunitseq = :p_processingunitseq

                        GROUP BY
                            custid, st.genericattribute2,st.genericattribute5
                    ) AS b
                    ON m.custid = b.custid
                    AND m.smcode = b.smcode
                INNER JOIN
                    (
                        SELECT   /* ORIGSQL: (select custid, st.genericattribute2 smcode, st.genericattribute5, MAX(st.compen(...) */
                            custid,
                            st.genericattribute2 AS smcode,
                            st.genericattribute5,
                            MAX(st.compensationdate) AS compdate,
                            MAX(so.orderid) AS orderid,
                            MAX(Alternateordernumber) AS alt
                        FROM
                            cs_Salestransaction st
                        INNER JOIN
                            cs_eventtype et
                            ON et.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                            AND et.datatypeseq = st.eventtypeseq
                        LEFT OUTER JOIN
                            (
                                SELECT   /* ORIGSQL: (select * from cs_transactionaddress@stelext where addresstypeseq = (SELECT addr(...) */
                                    *
                                FROM
                                   cs_transactionaddress
                                    /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_transactionaddress@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_transactionaddress_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                WHERE
                                    addresstypeseq
                                    =
                                    (
                                        SELECT   /* ORIGSQL: (Select addresstypeseq from cs_addresstype@stelext where addresstypeid='BILLTO') */
                                            addresstypeseq
                                        FROM
                                            cs_addresstype
                                            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_addresstype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_addresstype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                        WHERE
                                            addresstypeid = 'BILLTO'
                                    )
                                ) AS tad
                                ON st.salestransactionseq = tad.salestransactionseq
                                AND st.tenantid = tad.tenantid
                            INNER JOIN
                                cs_Salesorder so
                                ON so.salesorderseq = st.salesorderseq
                                AND so.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_eventtype@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_eventtype_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_Salesorder@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_Salesorder_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                            WHERE
                                eventtypeid = 'TV Submitted'
                                AND UPPER(st.genericattribute5) IN ('NEW','RECON')
                                AND UPPER(st.genericattribute22) = 'AC'
                                AND IFNULL(st.genericboolean1,0) = 0  /* ORIGSQL: nvl(st.genericboolean1,0) */
                                AND st.compensationdate BETWEEN TO_DATE(ADD_SECONDS(:v_Startdate,(86400*(-1*31)))) AND TO_DATE(ADD_SECONDS(:v_Enddate,(86400*-1)))   /* ORIGSQL: v_Startdate-31 */
                                                                                                                                                                     /* ORIGSQL: v_Enddate-1 */
                                AND st.tenantid = 'STEL'
                                AND st.processingunitseq = :p_processingunitseq
                            GROUP BY
                                custid, st.genericattribute2,st.genericattribute5
                        ) AS t
                        ON t.custid = m.custid
                        AND m.smcode = t.smcode
                    WHERE
                        1 = 1

                        AND GREATEST(m.compdate, b.compdate, t.compdate) BETWEEN :v_Startdate AND TO_DATE(ADD_SECONDS(:v_Enddate,(86400*-1))) --only run for one day at a time on a daily schedule
                        /* ORIGSQL: v_Enddate-1 */
                        AND (
                            (GREATEST(m.compdate, b.compdate, t.compdate) = m.compdate
                            AND m.genericattribute5 = 'New')
                            OR (GREATEST(m.compdate, b.compdate, t.compdate) = b.compdate
                            AND b.genericattribute5 = 'New')
                            OR (GREATEST(m.compdate, b.compdate, t.compdate) = t.compdate
                            AND t.genericattribute5 = 'New')
                        )
                        --AND (GREATEST(m.compdate, b.compdate, t.compdate) , LEAST(m.compdate, b.compdate, t.compdate) <= 7);
                        AND DAYS_BETWEEN(GREATEST(m.compdate, b.compdate, t.compdate), LEAST(m.compdate, b.compdate, t.compdate) )<= 7;

    /*
    
    \x95	For each TV transaction, use the customer ID to look up the Broadband file and Mobile file. The exact criteria is
    1.	BB.CustomerID = TV.CustomerID =Mobile.CustomerID
    2.	BB.TxnType = \x91New\x92 or Tv.TxnType=\x92New\x92 or Mobile.TxnType = \x91New\x92
    3.	BB.SalesmandCode = TV.SalesmanCode = Mobile.SalesmanCode
    4.	Max(BB.Submit Date, TV.Submit Date, Mobile.SubmitDate)  - Min (BB.Submit Date, TV.Submit Date, MobileSubmitDate) <=7
    
    i.e.
    \x95	the Customer ID should be the same across all 3 files
    \x95	The Txn Type for at least one of the records should be New
    \x95	The SalesmanCode should be the same in all 3 records
    \x95	The transactions should have occurred within 30 days (even if the first one was in a previous calendar month)
    
    In the event that any of the 3 transactions above is ceased/cancelled within 30 days of its submission, the bundle booster is effectively ceased as well.
    */

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: execute immediate 'truncate table inbound_Data_staging'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_DATA_STAGING' not found */

    /* ORIGSQL: truncate table inbound_Data_staging ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE inbound_Data_staging';

    /* ORIGSQL: insert into inbound_data_staging(FILETYPE, FILENAME, FILEDATE, ERROR_MESSAGE, ER(...) */
    INSERT INTO ext.inbound_data_staging
        (
            FILETYPE, FILENAME, FILEDATE, ERROR_MESSAGE, ERROR_FLAG, SEQ,
            FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6,
            FIELD7, FIELD8, FIELD9, FIELD10, field11, field12,
            field13, field14
        )
        /*
        1PERIOD
        2CUSTID
        3SMCODE
        4GENERICATTRIBUTE5
        5COMPDATE
        6MOBILEORDERID
        7BBORDERID
        8TVORDERID
        9GENERICBOOLEAN1
        10NUMBEROFUNITS
        */

        SELECT   /* ORIGSQL: select distinct v_inbound_cfg_parameter.file_type, v_inbound_cfg_parameter.file_(...) */
            DISTINCT
            :v_inbound_cfg_parameter.file_type,
            :v_inbound_cfg_parameter.file_name,
            :v_inbound_cfg_parameter.file_date,
            NULL,
            0,
            ROW_NUMBER() OVER (ORDER BY 0*0),  /* ORIGSQL: rownum */
            PERIOD,
            CUSTID,
            SMCODE,
            GENERICATTRIBUTE5,
            COMPDATE,
            MOBILEORDERID,
            BBORDERID,
            TVORDERID,
            GENERICBOOLEAN1,
            NUMBEROFUNITS,
            'altmob',
            'altbb',
            'alttv',
            'dealer'-- Sanjay: Commenting it as there are two many values the original proc is in invalid state 
        FROM
            ext.stel_Temp_Bundlebooster;

    /* ORIGSQL: commit; */
    COMMIT;
END