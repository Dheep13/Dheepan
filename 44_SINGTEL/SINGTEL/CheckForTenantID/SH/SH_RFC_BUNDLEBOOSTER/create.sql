CREATE PROCEDURE EXT.SH_RFC_BUNDLEBOOSTER
(
    IN P_PERIODSEQ DECIMAL(38,10),   /* ORIGSQL: P_PERIODSEQ IN NUMBER */
    IN P_PROCESSINGUNITSEQ DECIMAL(38,10)   /* ORIGSQL: P_PROCESSINGUNITSEQ IN NUMBER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_StartDate TIMESTAMP;  /* ORIGSQL: v_StartDate date; */
    DECLARE v_EndDate TIMESTAMP;  /* ORIGSQL: v_EndDate date; */
    DECLARE v_delproductid VARCHAR(255);  /* ORIGSQL: v_delproductid varchar2(255); */
    DECLARE v_rowcount DECIMAL(38,10);  /* ORIGSQL: v_rowcount number; */
    DECLARE v_Eot TIMESTAMP = TO_DATE('22000101','YYYYMMDD');  /* ORIGSQL: v_Eot date:=TO_DATE('22000101','YYYYMMDD') ; */

    /* ORIGSQL: stel_Sp_logger('SH_RFC_BUNDLEBOOSTER PRE','SH',0,0,'Running') */
    CALL EXT.STEL_SP_LOGGER('SH_RFC_BUNDLEBOOSTER PRE', 'SH', 0, 0, 'Running');

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
        periodseq = :P_PERIODSEQ
        AND removedate >CURRENT_TIMESTAMP;  /* ORIGSQL: sysdate */

    --reset 

    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_salestransaction tgt using (SELECT st.salestransactionseq, st.comp(...) */
    MERGE INTO cs_salestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select st.salestransactionseq, st.compensationdate, st.genericattribute5 from v(...) */
                st.salestransactionseq,
                st.compensationdate,
                st.genericattribute5
            FROM
                vw_Salestransaction st
            WHERE
                st.eventtypeid = 'Bundle Booster'
                AND st.orderid LIKE 'Boost%'
                AND ST.SETNUMBER = 1
                AND st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
--                AND st.tenantid = 'STEL'
                AND st.processingunitseq = :P_PROCESSINGUNITSEQ
        ) AS src
        ON (tgt.salestransactionseq = src.salestransactionseq
        AND tgt.compensationdate = src.compensationdate --AND tgt.tenantid = 'STEL' 
        AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ)
    WHEN MATCHED THEN
        UPDATE
            SET tgt.genericboolean1 = 0
        --WHERE
        --   tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ
            ;

    --mobile 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_salestransaction tgt using (SELECT st.salestransactionseq, st.comp(...) */
    MERGE INTO cs_salestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select st.salestransactionseq, st.compensationdate, st.genericattribute5, s.* f(...) */
                st.salestransactionseq,
                st.compensationdate,
                st.genericattribute5,
                s.*
            FROM
                vw_Salestransaction st
                /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
                /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select orderid from cs_salestransaction x join cs_Salesorder o on o.salesorders(...) */
                        orderid
                    FROM
                        cs_salestransaction x
                    INNER JOIN
                        cs_Salesorder o
                        ON o.salesorderseq = x.salesorderseq
                        AND o.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.genericboolean1 = 1
                ) AS s
                ON st.genericattribute5 = s.orderid
            WHERE
                st.eventtypeid = 'Bundle Booster'
                AND st.orderid LIKE 'Boost%'
                AND ST.SETNUMBER = 1
                AND st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
--                AND st.tenantid = 'STEL'
                AND st.processingunitseq = :P_PROCESSINGUNITSEQ
        ) AS src
        ON (tgt.salestransactionseq = src.salestransactionseq
        AND tgt.compensationdate = src.compensationdate --AND tgt.tenantid = 'STEL' 
        AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ)
    WHEN MATCHED THEN
        UPDATE
            SET tgt.genericboolean1 = 1
        --WHERE
        --   tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ
            ;

    --bb 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_salestransaction tgt using (SELECT st.salestransactionseq, st.comp(...) */
    MERGE INTO cs_salestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select st.salestransactionseq, st.compensationdate, st.genericattribute5, s.* f(...) */
                st.salestransactionseq,
                st.compensationdate,
                st.genericattribute5,
                s.*
            FROM
                vw_Salestransaction st  
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select orderid from cs_salestransaction x join cs_Salesorder o on o.salesorders(...) */
                        orderid
                    FROM
                        cs_salestransaction x
                    INNER JOIN
                        cs_Salesorder o
                        ON o.salesorderseq = x.salesorderseq
                        AND o.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.genericboolean1 = 1
                ) AS s
                ON st.genericattribute6 = s.orderid
            WHERE
                st.eventtypeid = 'Bundle Booster'
                AND st.orderid LIKE 'Boost%'
                AND ST.SETNUMBER = 1
                AND st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
--                AND st.tenantid = 'STEL'
                AND st.processingunitseq = :P_PROCESSINGUNITSEQ
        ) AS src
        ON (tgt.salestransactionseq = src.salestransactionseq
        AND tgt.compensationdate = src.compensationdate --ANd tgt.tenantid = 'STEL' 
        AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ)
    WHEN MATCHED THEN
        UPDATE
            SET tgt.genericboolean1 = 1
        --WHERE
        --    tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ
            ;
    --tv 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: merge into cs_salestransaction tgt using (SELECT st.salestransactionseq, st.comp(...) */
    MERGE INTO cs_salestransaction AS tgt
        USING
        (
            SELECT   /* ORIGSQL: (select st.salestransactionseq, st.compensationdate, st.genericattribute5, s.* f(...) */
                st.salestransactionseq,
                st.compensationdate,
                st.genericattribute5,
                s.*
            FROM
                vw_Salestransaction st  
            INNER JOIN
                (
                    SELECT   /* ORIGSQL: (select orderid from cs_salestransaction x join cs_Salesorder o on o.salesorders(...) */
                        orderid
                    FROM
                        cs_salestransaction x
                    INNER JOIN
                        cs_Salesorder o
                        ON o.salesorderseq = x.salesorderseq
                        AND o.removedate >CURRENT_TIMESTAMP   /* ORIGSQL: sysdate */
                        AND x.genericboolean1 = 1
                ) AS s
                ON st.genericattribute7 = s.orderid
            WHERE
                st.eventtypeid = 'Bundle Booster'
                AND st.orderid LIKE 'Boost%'
                AND ST.SETNUMBER = 1
                AND st.compensationdate BETWEEN v_StartDate AND TO_DATE(ADD_SECONDS(:v_EndDate,(86400*-1)))   /* ORIGSQL: v_Enddate-1 */
--                AND st.tenantid = 'STEL'
                AND st.processingunitseq = :P_PROCESSINGUNITSEQ
        ) AS src
        ON (tgt.salestransactionseq = src.salestransactionseq
        AND tgt.compensationdate = src.compensationdate --AND tgt.tenantid = 'STEL' 
        AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ)
    WHEN MATCHED THEN
        UPDATE
            SET tgt.genericboolean1 = 1
        --WHERE
        --    tgt.tenantid = 'STEL'
        --    AND tgt.processingunitseq = :P_PROCESSINGUNITSEQ
            ;

    /*
    execute immediate 'truncate table  stel_Temp_Bundlebooster';
    /#PERIOD
    CUSTID
    SMCODE
    GENERICATTRIBUTE5
    COMPDATE
    MOBILEORDERID
    BBORDERID
    TVORDERID
    GENERICBOOLEAN1
    NUMBEROFUNITS
    *//*
    insert into stel_Temp_Bundlebooster
    select v_Enddate-1, m.custid, m.smcode, m.genericattribute5,  greatest(m.compdate, b.compdate, t.compdate) compdate
    , m.orderid, b.orderid, t.orderid, 0, 1
    from
    (select custid, st.genericattribute2 smcode, st.genericattribute5 , max(st.compensationdate) compdate, max(st.alternateordernumber) orderid
        from cs_Salestransaction st
        join cs_eventtype et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        left join ( select * from cs_transactionaddress where addresstypeseq
                    = (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
        ) tad
        on st.salestransactionseq=tad.salestransactionseq
        and st.tenantid=tad.tenantid
        where eventtypeid='Mobile Submitted'
        and upper(genericattribute5) in ('NEW','RECON')
        and upper(genericattribute22)='AC' and nvl(genericboolean1,0)=0
        and st.compensationdate between v_Startdate-31 and v_Enddate-1
        and st.tenantid='STEL'
        and st.processingunitseq=p_processingunitseq
        
        group by custid, genericattribute2 ,genericattribute5
    ) m
    join
    (select custid, st.genericattribute2 smcode, st.genericattribute5 , max(st.compensationdate) compdate, max(st.alternateordernumber) orderid
        from cs_Salestransaction st
        join cs_eventtype et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        left join ( select * from cs_transactionaddress where addresstypeseq
                    = (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
        ) tad
        on st.salestransactionseq=tad.salestransactionseq
        and st.tenantid=tad.tenantid
        where eventtypeid='Broadband Submitted'
        and upper(genericattribute5) in ('NEW','RECON')
        and upper(genericattribute22)='AC' and nvl(genericboolean1,0)=0
        and st.compensationdate between v_Startdate-31 and v_Enddate-1
        and st.tenantid='STEL'
        and st.processingunitseq=p_processingunitseq
        
    group by custid, genericattribute2 ,genericattribute5) b
    on m.custid=b.custid and m.smcode=b.smcode
    join
    (select custid, st.genericattribute2 smcode, st.genericattribute5 , max(st.compensationdate) compdate, max(st.alternateordernumber) orderid
        from cs_Salestransaction st
        join cs_eventtype et on et.removedate>sysdate and et.datatypeseq=st.eventtypeseq
        left join ( select * from cs_transactionaddress where addresstypeseq
                    = (Select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
        ) tad
        on st.salestransactionseq=tad.salestransactionseq
        and st.tenantid=tad.tenantid
        where eventtypeid='TV Submitted'
        and upper(genericattribute5) in ('NEW','RECON')
        and upper(genericattribute22)='AC' and nvl(genericboolean1,0)=0
        and st.compensationdate between v_Startdate-31 and v_Enddate-1
        and st.tenantid='STEL'
        and st.processingunitseq=p_processingunitseq
    group by custid, genericattribute2 ,genericattribute5) t
    on t.custid=m.custid and m.smcode=t.smcode
    where 1=1
    
    and greatest(m.compdate, b.compdate, t.compdate) between v_Startdate and v_Enddate-1 --only run for one day at a time on a daily schedule
    and (
          (greatest(m.compdate, b.compdate, t.compdate) = m.compdate and m.genericattribute5='New')
         OR
          (greatest(m.compdate, b.compdate, t.compdate) = b.compdate and b.genericattribute5='New')
         OR
          (greatest(m.compdate, b.compdate, t.compdate) = t.compdate and t.genericattribute5='New')
      )
    and
    (greatest(m.compdate, b.compdate, t.compdate) - least(m.compdate, b.compdate, t.compdate) <=7)
    
    ;*/

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
END