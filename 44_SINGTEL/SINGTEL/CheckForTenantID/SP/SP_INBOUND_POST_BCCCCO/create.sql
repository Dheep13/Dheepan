CREATE PROCEDURE EXT.SP_INBOUND_POST_BCCCCO
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_cutoffday DECIMAL(38,10);  /* ORIGSQL: v_cutoffday NUMBER; */
    DECLARE v_oppr ROW LIKE inbound_cfg_BCC_Txn;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_BCC_Txn' not found (for %ROWTYPE declaration) */
    DECLARE v_maxseq DECIMAL(38,10);  /* ORIGSQL: v_maxseq number; */

    DECLARE v_prmtr ROW LIKE inbound_cfg_parameter;--%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.inbound_cfg_parameter' not found (for %ROWTYPE declaration) */

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('*****************');

    /* ORIGSQL: dbms_output.put_line ('Start Post BCC CCO'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('Start Post BCC CCO');

    /* ORIGSQL: dbms_output.put_line ('*****************'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('*****************');

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found */

    SELECT *
    INTO
        v_prmtr
    FROM
        ext.inbound_cfg_parameter
    WHERE
        object_name = 'SP_INBOUND_TXN_MAP';

    /* ORIGSQL: dbms_output.put_line ('51 TV ARPU Start'); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('51 TV ARPU Start');

    /*identify cco transactions - MobileVAS*/

    /*
    
    Saleman code to be used for filtering
    
    2.	This file will be filtered for CFU  = \x91Consumer\x92
    3.	The Transaction\x92s Dealer Code should match with one of the defined CCO Dealer codes
    4.	The Salesman code should be a valid CCO salesman code
    4.5.	System will reject transactions with CRD 2 months earlier than Closed date.
    5.6.	Data is picked up for a month based on the Order Close Date
    7.	Reject duplicated order based on same orderno, svcno, ordertype, compid.
    Product with the same service number in the same month with different order number will be treated as duplicate. Only the first record will be considered for commission.
    
    6.	For duplicated record checking, should exclude the daily product as daily product can be provide multiple times.
    8.	Reject order cease within the same month.
    7.9.	Exclude all orders which has been auto-reconnected after TOS (Temporary Out of Service
        */

        --in the closed mobile post sql
        /*
        insert into inbound_Data_assignment(orderid, linenumber, sublinenumber, eventtypeid, positionname)
        select orderid, linenumber, sublinenumber, eventtypeid,  genericattribute1 salesmancode
        from inbound_data_txn txn
        where
        txn.genericattribute1 in
        (select p.name from cs_position@stelext p
            join cs_Title@stelext t on
            p.titleseq=t.ruleelementownerseq
            and t.effectiveenddate>sysdate
            
            where t.name like 'CCO%' and t.removedate>sysdate and p.removedate>sysdate
            and txn.compensationdate between p.effectivestartdate and p.effectiveenddate-1
        )
        and upper(txn.genericattribute16) = upper('Consumer')
        
        and add_months(txn.genericdate1,2) < txn.compensationdate
        and txn.genericattribute9='S';*/
        --and txn.genericattribute7 in ()

        /* for TV Content CCO
        
        Saleman code to be used for filtering
        
        *2.	This file will be filtered for CFU  = \x91Consumer\x92
        3.	The Transaction\x92s Dealer Code should match with one of the defined CCO Dealer codes
        *4.	The Salesman code should be a valid CCO salesman code
        $5.	Data is picked up for a month based on the Order Close Date
        ^6.	Reject duplicated order based on same orderno, svcno, ordertype, compid. Product with the same service number in the same month with different order number will be treated as duplicate. Only the first record will be considered for commission.
        ^7.	Reject order cease within the same month.
        ^8.	Reject orders ceased within 30 days from the original order, up until the 15th of the next month
        ^9.	IF a cessation order for a previously paid order comes in on the 16th to 31st of the month, treat it as a negative value transaction (this leads to a clawback)
        ^10.	The ARPU per TV Content is looked up and stamped on the transaction. The uploaded product list has the components and the List Price which is used to determine the changed ARPU.
        */
        --in the closed TV post sql
        /*
        insert into inbound_Data_assignment(orderid, linenumber, sublinenumber, eventtypeid, positionname)
        select orderid, linenumber, sublinenumber, eventtypeid,  genericattribute1 salesmancode
        from inbound_data_txn txn
        where
        txn.genericattribute1 in
        (select p.name from cs_position@stelext p
            join cs_Title@stelext t on
            p.titleseq=t.ruleelementownerseq
            and t.effectiveenddate>sysdate
            
            where t.name like 'CCO%' and t.removedate>sysdate and p.removedate>sysdate
            and txn.compensationdate between p.effectivestartdate and p.effectiveenddate-1
        )
        and upper(txn.genericattribute16) = upper('Consumer')
        
        and txn.genericattribute9='S';
        
        
        */
END