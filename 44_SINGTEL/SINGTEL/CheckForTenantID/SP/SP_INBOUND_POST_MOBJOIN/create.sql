CREATE PROCEDURE EXT.SP_INBOUND_POST_MOBJOIN
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
    DECLARE v_proc_name VARCHAR(127) = 'SP_INBOUND_POST_MOBJOIN';  /* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_POST_MOBJOIN'; */
    DECLARE v_rowcount BIGINT;  /* ORIGSQL: v_rowcount integer; */

    DECLARE v_param ROW LIKE INBOUND_CFG_PARAMETER;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_param
    FROM
        ext.INBOUND_CFG_PARAMETER;

    -- This is to join the Mobile files
    /*
    BCC Closed
    BCC Submitted
    Discount
    Voucher
    SalesOrder
    
    The joined data is inserted into the temp_txnmobile table for further processing.
    
    -- For discount filtering etc. it can also be done in the SH,
    so that a reload is not required every time the elgible discounts/vouchers are changed
    However, that may slow down the PL, so we will do it here. Can be moved later.
    
    */

    /**
    These fields need to be updated:
    Channel - SERS or TEPL - do in mobjoin (can be done earlier, but it might be better for error rereuns to do everything in one place)
    GA28 with stock code  - do in mobjoin
    GN1 with Equip Code - do in mobjoin
     Gn5/6 with Discount and Voucher - do in mobjoin
    TAssignment
    Gn1 - Discount qty - do in mobjoin
    Gn2 - Voucher Qty - do in mobjoin
    
    union the BizSales data here
    **/

    --Channel update

    --to get the channel, we do the roadhsow dealer code update first

    --SERS Roadshow identification

    /* List of roadhsow dealer codes:
    select distinct classifierid from stel_classifier@stelext
    where categorytreename='Roadshow Codes' AND CATEOGRYNAME='Roadshow'
    
    */
    --Dealer Code from Roadshow
    --20190221:MOVETOSH  
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_data_txn_mobile tgt using (SELECT classifierid AS RdCode, a.Gene(...) */
    MERGE INTO ext.stel_data_txn_mobile AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select classifierid RdCode, a.Genericattribute2 dlrcode, a.effectivestartdate, (...) */
                classifierid AS RdCode,
                a.Genericattribute2 AS dlrcode,
                a.effectivestartdate,
                a.effectiveenddate,
                mgr.name AS VendorCode
            FROM
                stel_Classifier a
            INNER JOIN
                cs_position pos
                ON pos.name = a.genericattribute2
                AND pos.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND Add_Days(a.effectiveenddate,-1) BETWEEN pos.effectivestartdate AND Add_days(pos.effectiveenddate,-1)
            INNER JOIN
                cs_position mgr
                ON pos.managerseq = mgr.ruleelementownerseq
                AND mgr.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                AND Add_Days(a.effectiveenddate,-1) BETWEEN mgr.effectivestartdate AND Add_Days(mgr.effectiveenddate,-1)
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_Classifier@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_Classifier_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'DBMTK_USER_NAME.cs_position_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
            WHERE
                categorytreename = 'Roadshow Codes'
                AND CATEGoRYNAME = 'Roadshow'
        ) AS src
        ON (tgt.compensationdate BETWEEN src.effectivestartdate AND Add_Days(src.effectiveenddate,-1)
            AND tgt.genericattribute4 = src.rdCode
        	AND filename = :v_param.file_name
            AND filedate = :v_param.file_Date
            AND recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET tgt.genericattribute1 = tgt.assignee1,
            tgt.dealercode = src.dlrCode,
            tgt.tempfield2 = src.VendorCode,
            tgt.assignee1 = src.dlrcode
        --WHERE
            --filename = v_param.file_name
            --AND filedate = v_param.file_Date
            --AND recordstatus = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'DealerCode from RoadShow :' || v_param.file_ty(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'DealerCode from RoadShow  :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'DealerCode from RoadShow Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'DealerCode from RoadShow  :' || v_param.file_type || '-Fi(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --20190221 : this can be moved to SH RESETASSIGNMENTs
    --assignee1 will go into posnname later. it hsa to be intiailly popuakted through txnmap with the Dealer Cod
    --Dealer Code from SalesPerson (Primary Dealer ID - GA1)
    --if ga1(primary dealerid) = dlrcode, AI and MSF go to the same shop. (set assignment to be tempfield1 above)
    -- if ga1 (primary dealerid)<>dlrcode, MSF goes to tempfield1 shop, AI goes to GA1 salesperson (assignment is still to dealer code tempfield1
        -- another assignment to be created for this with AI flag   
        /* ORIGSQL: update stel_Data_txn_mobile SET assignee2 = genericattribute1 where filename = v(...) */
        UPDATE ext.stel_Data_txn_mobile
            SET
            /* ORIGSQL: assignee2 = */
            assignee2 = genericattribute1
        FROM
            ext.stel_Data_txn_mobile
        WHERE
            filename = :v_param.file_name
            AND filedate = :v_param.file_Date
            AND recordstatus = 0
            AND dealercode <> genericattribute1;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update assignee2 in stel_Data_txn_mobile :' ||(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update assignee2 in stel_Data_txn_mobile :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update assignee2 in stel_Data_txn_mobile Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update assignee2 in stel_Data_txn_mobile :' || v_param.fi(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --update the channel from GA11
    --20190221:This is just to separate SER/TEPL? logic needs to be updated and lef here. may impact the next procedure  --2019Mar14th Arun added the logic for SER 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_Data_txn_mobile tgt using (SELECT a.seq, lt.channel AS channel F(...) */
    MERGE INTO stel_Data_txn_mobile AS tgt
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_DATA_TXN_MOBILE' not found */
        USING
        (
            SELECT   /* ORIGSQL: (select a.seq, lt.channel channel from stel_Data_txn_mobile a join (SELECT dim0 (...) */
                a.seq,
                lt.channel AS channel /* -- [Arun 14th Mar2019] */
                /* --nvl(p.genericattribute11,m.genericattribute11) channel [Arun 14th Mar2019] commented as part of bug fix as Channel doesn't come from position */
            FROM
                stel_Data_txn_mobile a
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select dim0 dealer, stringvalue channel, effectivestartdate, effectiveenddate f(...) */
                        dim0 AS dealer,
                        stringvalue AS channel,
                        effectivestartdate,
                        effectiveenddate  /* -- [Arun 14th Mar2019] */
                    FROM
                        EXT.stel_lookup
                        -- [Arun 14th Mar2019]
                        /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_lookup@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_lookup_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        name LIKE 'LT_Dealer_Channel Type'
                ) AS lt
                -- [Arun 14th Mar2019]
                ON lt.dealer = a.genericattribute3
                AND lt.channel = 'SER' -- [Arun 14th Mar2019]
                /*stel_position@stelext p
                on   a.dealercode=p.posname*/
                AND a.compensationdate BETWEEN lt.effectivestartdate AND Add_Days(lt.effectiveenddate,-1)
                --join stel_position@stelext m                          [Arun 14th Mar2019] commented as part of bug fix as Channel doesn't come from position
                --on p.managerseq=m.ruleelementownerseq    [Arun 14th Mar2019] commented as part of bug fix as Channel doesn't come from position
                --and a.compensationdate between m.effectivestartdate and m.effectiveenddate-1
            WHERE
                a.filename = :v_param.file_name
                AND a.filedate = :v_param.file_Date
                AND a.recordstatus = 0
        ) AS src
        ON (tgt.seq = src.seq
        	AND filename = :v_param.file_name
            AND filedate = :v_param.file_Date
            AND recordstatus = 0
        )
    WHEN MATCHED THEN
        UPDATE SET
            tgt.channel = src.channel
        --WHERE
            --filename = v_param.file_name
            --AND filedate = v_param.file_Date
            --AND recordstatus = 0
            ;

    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Channel from :' || v_param.file_type ||(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Channel from :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update Channel from stel_lookup for Vendor Code - Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Channel from :' || v_param.file_type || '-FileName(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --2019Mar14th Arun added the logic for TEPL 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into stel_Data_txn_mobile tgt using (SELECT a.seq, lt.channel AS channel F(...) */
    MERGE INTO stel_Data_txn_mobile AS tgt 
        USING
        (
            SELECT   /* ORIGSQL: (select a.seq, lt.channel channel from stel_Data_txn_mobile a join (SELECT dim0 (...) */
                a.seq,
                lt.channel AS channel /* -- [Arun 14th Mar2019] */
            FROM
                stel_Data_txn_mobile a
                -- [Arun 14th Mar2019]
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select dim0 dealer, stringvalue channel, effectivestartdate, effectiveenddate f(...) */
                        dim0 AS dealer,
                        stringvalue AS channel,
                        effectivestartdate,
                        effectiveenddate /* -- [Arun 14th Mar2019] */
                    FROM
                       EXT.stel_lookup
                        -- [Arun 14th Mar2019]
                        /* RESOLVE: Oracle Database link: Remote table/view 'DBMTK_USER_NAME.stel_lookup@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'DBMTK_USER_NAME.stel_lookup_AT_STELEXT'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
                    WHERE
                        name LIKE 'LT_Dealer_Channel Type'
                ) AS lt
                -- [Arun 14th Mar2019]
                ON lt.dealer = a.genericattribute4
                AND lt.channel = 'TEPL' -- [Arun 14th Mar2019]
                AND a.compensationdate BETWEEN lt.effectivestartdate AND Add_Days(lt.effectiveenddate,-1) -- [Arun 14th Mar2019]
            WHERE
                a.filename = :v_param.file_name
                AND a.filedate = :v_param.file_Date
                AND a.recordstatus = 0 -- [Arun 14th Mar2019]
        ) AS src
        -- [Arun 14th Mar2019]
        ON (tgt.seq = src.seq
        	AND filename = :v_param.file_name
            AND filedate = :v_param.file_Date
            AND recordstatus = 0
        ) -- [Arun 14th Mar2019]
    WHEN MATCHED THEN
        UPDATE SET -- [Arun 14th Mar2019]
            tgt.channel = src.channel -- [Arun 14th Mar2019]
        --WHERE
            --filename = v_param.file_name
            --AND filedate = v_param.file_Date
            --AND recordstatus = 0;-- [Arun 14th Mar2019]
          ;
    v_rowcount = ::ROWCOUNT;  /* ORIGSQL: SQL%ROWCOUNT */

    /* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update Channel from :' || v_param.file_type ||(...) */
    CALL EXT.STEL_SP_LOGGER(SUBSTRING(IFNULL(:v_proc_name,'') || 'Update Channel from :'|| IFNULL(:v_param.file_type,'') || '-FileName:'|| IFNULL(:v_param.file_name,'') || '-Date:'|| IFNULL(:v_param.file_date,''),1,255) 
        , 'Update Channel from stel_lookup for Dealer Code - Execution Completed', :v_rowcount, NULL, NULL);  /* ORIGSQL: SUBSTR(v_proc_name || 'Update Channel from :' || v_param.file_type || '-FileName(...) */

    /* ORIGSQL: commit; */
    COMMIT;

    --Stock and Equip Code updates - Requires the SalesORder file
    --20190221:May not be needed anymore if this gets added to the BCC file - [Arun20190313] - This is commented out as STOCKCODE now comes from BCC
    /*
    merge into stel_Data_txn_mobile tgt
    using(
        select a.seq, max(b.stockcode) stockcode, count(*) cnt from stel_Data_txn_mobile a
        join stel_Data_Salesorder b
        on b.orderid=a.alternateordernumber and b.orderactionid=a.genericattribute6 and b.materialgroup='DEVICE'
        where a.filename = v_param.file_name and a.filedate=v_param.file_Date and a.recordstatus=0
        group by a.seq
        --might need more joins here
        
    ) src
    on (tgt.seq=src.seq)
    when matched then update set
    tgt.genericattribute28=src.stockcode
    where filename = v_param.file_name and filedate=v_param.file_Date and recordstatus=0
    ;
    
    v_rowcount := SQL%ROWCOUNT;
    
          SP_LOGGER (
                 SUBSTR (
                           v_proc_name
                        || 'Update Stock and Equip Code:'
                        || v_param.file_type
                        || '-FileName:'
                        || v_param.file_name
                        || '-Date:'
                        || v_param.file_date,
                        1,
                    255),
                 'Update Stock and Equip Code Execution Completed',
                 v_rowcount,
                 NULL,
             null);
    
    commit;
    */

    --Disc
    --moved to SH already

    --Voucher

    --moved to SH already

    --BizSales -- Not needed
    /*
    select so.key..., sum(quantity), sum(discountamount), sum(totaldiscountamount), sum (itemtaxamount)
    from stel_Data_Salesorder so
    left join stel_Data_discount d
    on so.orderid = d.orderid
    and so.orderactionid=d.orderactionid
    and
    
    select so.key..., sum(quantity), sum(value), sum(totaldiscountamount), sum (itemtaxamount)
    from stel_Data_Salesorder so
    left join stel_Data_voucher v
    on so.orderid = v.orderid
    and so.orderactionid=v.orderactionid
    and v.vouchertypeid in (select classifierid from stel_Classifer@stelext
      where categorytreename = '' and categoryname = '' and gneeircattribute1='1')
    
    */
END