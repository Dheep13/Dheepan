CREATE PROCEDURE EXT.SP_INBOUND_POST_BCCBOOSTER
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodstart TIMESTAMP;  /* ORIGSQL: v_periodstart date; */
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    DECLARE v_periodend TIMESTAMP;  /* ORIGSQL: v_periodend date; */
    DECLARE v_procname VARCHAR(50) = 'SP_INBOUND_POST_BCCBOOSTER';  /* ORIGSQL: v_procname varchar2(50):='SP_INBOUND_POST_BCCBOOSTER'; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        EXT.INBOUND_CFG_PARAMETER;

    -- to run on a daily schedule, after the basic validations are done

    -- run on records that are loaded into Commissions

    /* ORIGSQL: execute immediate 'truncate table stel_Temp_transaction'; */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_TEMP_TRANSACTION' not found */

    /* ORIGSQL: truncate table stel_Temp_transaction ; */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stel_Temp_transaction';

    /* ORIGSQL: INSERT INTO STEL_TEMP_TRANSACTION (TENANTID, SALESTRANSACTIONSEQ, SALESORDERSEQ,(...) */
    INSERT INTO EXT.STEL_TEMP_TRANSACTION
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
            /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.vw_Salestransaction@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.vw_Salestransaction_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
        WHERE
            eventtypeid LIKE '%Submitted'
            AND IFNULL(genericboolean1,0) = 0  /* ORIGSQL: nvl(genericboolean1,0) */
            AND setnumber = 1
            AND compensationdate BETWEEN :v_periodstart AND :v_periodend;--30 days

    /* ORIGSQL: commit; */
    COMMIT;

    --Detect matches and store in temp table

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

    /* ORIGSQL: delete from stel_Temp_Bundlebooster where period = v_periodend; */
    DELETE
    FROM
        ext.stel_Temp_Bundlebooster
    WHERE
        period = :v_periodend;

    /* ORIGSQL: commit; */
    COMMIT;  

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_TEMP_BUNDLEBOOSTER' not found */

    /* ORIGSQL: insert into stel_Temp_Bundlebooster select v_periodend, m.custid, m.smcode, b.ge(...) */
    INSERT INTO ext.stel_Temp_Bundlebooster
        SELECT   /* ORIGSQL: select v_periodend, m.custid, m.smcode, b.genericattribute5, GREATEST(m.compdate(...) */
            v_periodend,
            m.custid,
            m.smcode,
            b.genericattribute5,
            GREATEST(m.compdate, b.compdate, t.compdate)compdate,
            m.orderid,
            b.orderid,
            t.orderid,
            Null,
            Null
        FROM
            (
                SELECT   /* ORIGSQL: (select custid, genericattribute2 smcode,genericattribute5, MAX(compensationdate(...) */
                    custid,
                    genericattribute2 AS smcode,
                    genericattribute5,
                    MAX(compensationdate) AS compdate,
                    MAX(orderid) AS orderid
                FROM
                    ext.stel_temp_Transaction
                WHERE
                    eventtypeid = 'Mobile Submitted'
                    AND UPPER(genericattribute5) IN ('NEW','RECON')
                    AND UPPER(genericattribute22) = 'AC'
                GROUP BY
                    custid, genericattribute2,genericattribute5
            ) AS m
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select custid, genericattribute2 smcode,genericattribute5, MAX(compensationdate(...) */
                    custid,
                    genericattribute2 AS smcode,
                    genericattribute5,
                    MAX(compensationdate) AS compdate,
                    MAX(orderid) AS orderid
                FROM
                    ext.stel_temp_Transaction
                WHERE
                    eventtypeid = 'Broadband Submitted'
                    AND UPPER(genericattribute5) IN ('NEW','RECON')
                    AND UPPER(genericattribute22) = 'AC'
                GROUP BY
                    custid, genericattribute2,genericattribute5
            ) AS b
            ON m.custid = b.custid
            AND m.smcode = b.smcode
        INNER JOIN
            (
                SELECT   /* ORIGSQL: (select custid, genericattribute2 smcode,genericattribute5, MAX(compensationdate(...) */
                    custid,
                    genericattribute2 AS smcode,
                    genericattribute5,
                    MAX(compensationdate) AS compdate,
                    MAX(orderid) AS orderid
                FROM
                    ext.stel_temp_Transaction
                WHERE
                    eventtypeid = 'TV Submitted'
                    AND UPPER(genericattribute5) IN ('NEW','RECON')
                    AND UPPER(genericattribute22) = 'AC'
                GROUP BY
                    custid, genericattribute2,genericattribute5
            ) AS t
            ON t.custid = m.custid
            AND m.smcode = t.smcode
        WHERE
            1 = 1

            AND GREATEST(m.compdate, b.compdate, t.compdate) = :v_periodend --only run for one day at a time on a daily schedule
            AND (
                (GREATEST(m.compdate, b.compdate, t.compdate) = m.compdate
                AND m.genericattribute5 = 'New')
                OR (GREATEST(m.compdate, b.compdate, t.compdate) = b.compdate
                AND b.genericattribute5 = 'New')
                OR (GREATEST(m.compdate, b.compdate, t.compdate) = t.compdate
                AND t.genericattribute5 = 'New')
            );

    /* ORIGSQL: commit; */
    COMMIT;

    --for cessations from past periods
    /*compare order ids (from above and from cs_Salestransaction) againstlist of ceased order ids
    based on the cessation date, either set flag to 0 or units to -1*/ 

    /* ORIGSQL: delete from stel_Temp_Bundlebooster where mobileorderid in (SELECT txnorderid FR(...) */
    DELETE
    FROM
        ext.stel_Temp_Bundlebooster
    WHERE
        mobileorderid
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_TEMP_CESSATIONS' not found */
        IN
        (
            SELECT   /* ORIGSQL: (Select txnorderid from stel_temp_Cessations) */
                txnorderid
            FROM
                ext.stel_temp_Cessations
        )
        OR bborderid  
        IN
        (
            SELECT   /* ORIGSQL: (Select txnorderid from stel_temp_Cessations) */
                txnorderid
            FROM
                ext.stel_temp_Cessations
        )
        OR tvorderid  
        IN
        (
            SELECT   /* ORIGSQL: (Select txnorderid from stel_temp_Cessations) */
                txnorderid
            FROM
                ext.stel_temp_Cessations
        );

    /* ORIGSQL: commit; */
    COMMIT;

    /*
    select
    from stel_temp_transaction st
    where st.eventtypeid='BundleBooster Submitted'
    and (st.genericattribute30 in (Select txnorderid from stel_temp_Cessations)
        or  st.genericattribute31 in (Select txnorderid from stel_temp_Cessations)
        or  st.genericattribute32 in (Select txnorderid from stel_temp_Cessations) )
    ;
    */

    /* ORIGSQL: commit; */
    COMMIT;

    --  insert a Bundle Booster Transaction. if it already exists, it will be overwritten due to the same order id etc.
    --Order ID - CustID+SMCode, Linenumber - max date from above. ET BundleBooster Submitted
    --ccall inb stg arch
    --insert into inbound_Data_Staging
    --txnmap
END