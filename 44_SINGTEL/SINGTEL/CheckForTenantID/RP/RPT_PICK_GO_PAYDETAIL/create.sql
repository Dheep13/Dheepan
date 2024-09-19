CREATE PROCEDURE EXT.RPT_PICK_GO_PAYDETAIL
(
    IN in_rpttype VARCHAR(75),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=75; adjust as needed */
                                 /* ORIGSQL: in_rpttype varchar2 */
    IN in_periodseq BIGINT,   /* ORIGSQL: in_periodseq IN INTEGER */
    IN in_processingunitseq BIGINT     /* ORIGSQL: in_processingunitseq IN INTEGER */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_eot TIMESTAMP = to_date('01-JAN-2200', 'dd-mon-yyyy');  /* ORIGSQL: v_eot DATE := TO_DATE('01-JAN-2200', 'dd-mon-yyyy') ; */

    DECLARE Proc_name VARCHAR(255) = 'RPT_PICK_GO_PAYDETAIL';  /* ORIGSQL: Proc_name VARCHAR(255) := 'RPT_PICK_GO_PAYDETAIL'; */
    DECLARE v_Tenant VARCHAR(4) = 'STEL';  /* ORIGSQL: v_Tenant VARCHAR(4) := 'STEL'; */
    --DECLARE v_period cs_period%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_period ROW LIKE cs_period;
    DECLARE v_calendarname VARCHAR(255);  /* ORIGSQL: v_calendarname VARCHAR(255); */
    --DECLARE v_qtr cs_period%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE v_qtr ROW LIKE cs_period;
    DECLARE v_startdate TIMESTAMP;  /* ORIGSQL: v_startdate date; */
    DECLARE v_enddate TIMESTAMP;  /* ORIGSQL: v_enddate date; */

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT *
    INTO
        v_period
    FROM
        cs_period
    WHERE
        removedate = :v_eot
        AND periodseq = :in_periodseq;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CALENDAR' not found */

    SELECT
        name
    INTO
        v_calendarname
    FROM
        cs_calendar
    WHERE
        removedate = :v_eot
        AND calendarseq = :v_period.calendarseq;

    SELECT *
    INTO
        v_qtr
    FROM
        cs_period
    WHERE
        periodseq = :v_period.parentseq
        AND removedate = :v_eot;

    SELECT
        MIN(startdate),
        MAX(enddate)
    INTO
        v_startdate,
        v_enddate
    FROM
        cs_period
    WHERE
        /*parentseq*/ periodseq = :v_period.periodseq /*parentseq*/
        AND removedate = :v_eot;-- and enddate<=v_period.enddate;

    /* ORIGSQL: dbms_output.put_line ('startdate : ' ||v_startdate); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('startdate : ' ||IFNULL(TO_VARCHAR(:v_startdate),''));

    /* ORIGSQL: dbms_output.put_line ('enddate : ' ||v_enddate); */
    --CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('enddate : ' ||IFNULL(TO_VARCHAR(:v_enddate),''));

    -- Add debug Log for Process START
    /* ORIGSQL: STEL_log (v_Tenant, Proc_name, 'Proc Started ' || ' PeriodSeq - ' || in_periodse(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :Proc_name, 'Proc Started '|| ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;

    /* ORIGSQL: stel_proc_rpt_partitions (in_periodseq, 'STEL_RPT_PICK_GO_PAYDETAIL') */
    CALL EXT.STEL_PROC_RPT_PARTITIONS(:in_periodseq, 'EXT.STEL_RPT_PICK_GO_PAYDETAIL');

    -- managing table partitions

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.STEL_RPT_PICK_GO_PAYDETAIL' not found */

    /* ORIGSQL: insert into STEL_RPT_PICK_GO_PAYDetail (positionseq,payeeseq,positionname,period(...) */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONASSIGNMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONADDRESS' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_POSITION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PARTICIPANT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_BUSINESSUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PROCESSINGUNIT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TITLE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_CREDIT' not found */
    INSERT INTO EXT.STEL_RPT_PICK_GO_PAYDetail
        (
            positionseq, payeeseq, positionname, periodseq, processingunitseq, periodname,
            calendarname, processingunitname, quartername, subscription_id, service_no, customer_nric,
            service_type, status, order_no, product_code, pick_go_code, pick_go_partner_code,
            pick_go_partner_desc, pick_go_promo_code, order_entered_date, ceased_date, reason, ratesymbol
        )
        SELECT   /* ORIGSQL: select pos.ruleelementownerseq positionseq,pos.payeeseq,pos.name Positionname,v_(...) */
            pos.ruleelementownerseq AS positionseq,
            pos.payeeseq,
            pos.name AS Positionname,
            :v_period.periodseq AS periodseq,
            pu.processingunitseq,
            :v_period.name AS periodname,
            :v_calendarname AS Calendarname,
            pu.name AS Processinunitname,
            :v_period.name AS QuarterName,
            tad.city AS Subscription_ID,
            tad.contact AS Service_No,
            tad.custid AS Customer_NRIC,
            CASE
                WHEN et.eventtypeid LIKE 'Top%Up%'
                THEN 'Prepaid Top Up'
                ELSE st.productname
            END
            AS Service_Type,
            CASE
                WHEN IFNULL(st.numberofunits,0) = -1  /* ORIGSQL: nvl(st.numberofunits,0) */
                THEN 'Clawback'
                WHEN IFNULL(st.genericboolean1,0) = 1  /* ORIGSQL: nvl(st.genericboolean1,0) */
                THEN 'Ceased in Same Month'
                WHEN IFNULL(c.value,0) <> 0  /* ORIGSQL: nvl(c.value,0) */
                OR et.eventtypeid = 'Payment Adjustment'
                THEN 'Payable'
                ELSE 'Non Payable'
            END
            AS status,
            IFNULL(st.Alternateordernumber, so.orderid) AS Order_No,  /* ORIGSQL: Nvl(st.Alternateordernumber, so.orderid) */
            st.productid AS Product_Code,
            NULL AS Pick_Go_Code,
            ta.positionname AS Pick_Go_Partner_Code,
            par.lastname AS pick_go_partner_desc,
            st.productdescription AS Pick_Go_Promo_Code,
            st.compensationdate /*Arjun 20170403 used to be gd2*/ AS Order_Entered_Date,
            st.genericdate1 /*st.genericdate3*/ AS Ceased_Date,
            CASE
                WHEN st.genericboolean1 = 1
                THEN 'Ceased within 30 days '
                WHEN et.eventtypeid = 'Payment Adjustment'
                THEN st.genericattribute2
                ELSE NULL
            END
            AS Reason,
            CASE
                WHEN et.eventtypeid LIKE 'Top%Up%'
                THEN '%'
                ELSE '$'
            END
            AS rateSymbol
        FROM
            cs_salestransaction st
        LEFT OUTER JOIN
            cs_salesorder so
            ON st.salesorderseq = so.salesorderseq
            AND so.removedate = :v_eot
        INNER JOIN
            cs_transactionassignment ta
            ON st.salestransactionseq = ta.salestransactionseq
            AND st.tenantid = ta.tenantid
            AND st.compensationdate = ta.compensationdate
            AND st.processingunitseq = ta.processingunitseq
        LEFT OUTER JOIN
            cs_transactionaddress tad
            ON st.billtoaddressseq = tad.transactionaddressseq
        INNER JOIN
            cs_position pos
            ON ta.positionname = pos.name
            AND pos.effectivestartdate < :v_enddate
            AND pos.effectiveenddate >= :v_enddate
        INNER JOIN
            cs_participant par
            ON par.removedate = :v_eot
            AND pos.payeeseq = par.payeeseq
            AND par.effectivestartdate < :v_enddate
            AND par.effectiveenddate >= :v_enddate
        INNER JOIN
            cs_businessunit bu
            ON st.businessunitmap = bu.mask
        INNER JOIN
            cs_processingunit pu
            ON st.processingunitseq = pu.processingunitseq
        INNER JOIN
            cs_title ti
            ON ti.name LIKE '%Pick%Go%'
            AND ti.removedate = :v_eot
            AND ti.effectiveenddate = :v_eot
            AND ti.ruleelementownerseq = pos.titleseq
        INNER JOIN
            cs_eventtype et
            ON et.datatypeseq = st.eventtypeseq
        LEFT OUTER JOIN
            cs_credit c
            ON UPPER(c.name) LIKE 'D%PICK%GO%' 
            AND st.salestransactionseq = c.salestransactionseq
        WHERE
            pos.removedate = :v_eot
            AND et.removedate = :v_eot
            AND st.compensationdate >= :v_startdate
            AND st.compensationdate < :v_enddate
            AND et.eventtypeid IN ('DASH Pay Order','Music Order','SIM Only Order','Payment Adjustment','Top Up Revenue','Top Up Revenue Adjustment');

    --and st.salestransactionseq=tad.salestransactionseq

    /* ORIGSQL: Commit; */
    COMMIT;

    -- Add debug Log for Process End
    /* ORIGSQL: STEL_log (v_Tenant, Proc_name, 'Proc Finished ' || ' PeriodSeq - ' || in_periods(...) */
    CALL EXT.STEL_LOG(:v_Tenant, :Proc_name, 'Proc Finished '|| ' PeriodSeq - '|| IFNULL(TO_VARCHAR(:in_periodseq),'') || ' PU - '|| IFNULL(TO_VARCHAR(:in_processingunitseq),''));

    /* ORIGSQL: COMMIT; */
    COMMIT;
END