CREATE PROCEDURE EXT.PRC_BASE_SALESTRANSACTION_R5
(
    --IN vperiodseq cs_period.periodseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_period.periodseq' not found (for %TYPE declaration) */
    IN vperiodseq BIGINT,
                                              /* RESOLVE: Datatype unresolved: Datatype (cs_period.periodseq%TYPE) not resolved for parameter 'PRC_BASE_SALESTRANSACTION_R5.vperiodseq' */
                                              /* ORIGSQL: vperiodseq IN cs_period.periodseq%TYPE */
    --IN vprocessingunitseq cs_processingunit.processingunitseq%TYPE,   /* RESOLVE: Identifier not found: Table/Column 'cs_processingunit.processingunitseq' not found (for %TYPE declaration) */
    IN vprocessingunitseq BIGINT,
                                                                      /* RESOLVE: Datatype unresolved: Datatype (cs_processingunit.processingunitseq%TYPE) not resolved for parameter 'PRC_BASE_SALESTRANSACTION_R5.vprocessingunitseq' */
                                                                      /* ORIGSQL: vprocessingunitseq IN cs_processingunit.processingunitseq%TYPE */
    --IN vcalendarseq cs_period.calendarseq%TYPE      /* RESOLVE: Identifier not found: Table/Column 'cs_period.calendarseq' not found (for %TYPE declaration) */
    IN vcalendarseq BIGINT
                                                    /* RESOLVE: Datatype unresolved: Datatype (cs_period.calendarseq%TYPE) not resolved for parameter 'PRC_BASE_SALESTRANSACTION_R5.vcalendarseq' */
                                                    /* ORIGSQL: vcalendarseq IN cs_period.calendarseq%TYPE */
)
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    -------------------------------------------------------------------------------------------------------------------
    -- Purpose:
    --
    -- Design objectives:
    --
    -------------------------------------------------------------------------------------------------------------------
    -- Modification Log:
    -- Date             Author        Description
    -------------------------------------------------------------------------------------------------------------------
    -- 30-Nov-2017      Tharanikumar  Initial release
    --
    --
    -------------------------------------------------------------------------------------------------------------------

    DECLARE vprocname VARCHAR(30) = UPPER('PRC_BASE_SALESTRANSACTION_R5');  /* ORIGSQL: vprocname VARCHAR2(30) := UPPER('PRC_BASE_SALESTRANSACTION_R5') ; */
    DECLARE vsqlerrm VARCHAR(3900);  /* ORIGSQL: vsqlerrm VARCHAR2(3900); */
    --DECLARE vprocessingunitrow cs_processingunit%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_processingunit' not found (for %ROWTYPE declaration) */
    DECLARE vprocessingunitrow ROW LIKE cs_processingunit;
    --DECLARE vperiodcalendarrow cs_period%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_period' not found (for %ROWTYPE declaration) */
    DECLARE vperiodcalendarrow ROW LIKE cs_period;
    /* RESOLVE: Identifier not found: Table/Column 'cs_credit.pipelinerundate' not found (for %TYPE declaration) */
    --DECLARE vcalendarrow cs_calendar%ROWTYPE;;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.cs_calendar' not found (for %ROWTYPE declaration) */
    DECLARE vcalendarrow ROW LIKE cs_calendar;
    --DECLARE vSalesTransaction EXT.PRC_BASE_SALESTRANSACTION_R5__stType;  /* ORIGSQL: vSalesTransaction stType; */

    --:vpipelinerundate       cs_credit.pipelinerundate%TYPE;;/* NOT CONVERTED! */
    DECLARE vpipelinerundate ROW LIKE cs_credit;

    --exec prc_logevent_R5(null,vProcName,'R5 Report table Cursor Start',NULL,current_timestamp);

    --Cursor C_salestrasaction is

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'stType' to 'DBMTK_USER_NAME.PRC_BASE_SALESTRANSACTION_R5__stType'
    TYPE stType IS TABLE OF DBMTK_USER_NAME.RPT_BASE_SALESTRANSACTION_R5%ROWTYPE INDEX BY PLS_INTEGER;
    ---end of TYPE definition commented out---*/ 
    --exec prc_logevent_R5(null,vProcName,'R5 Report table Cursor Complete',NULL,current_timestamp);

    /* ORIGSQL: Execute IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ; */
    /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION ENABLE PARALLEL' not supported; convert manually */
    /* ALTER SESSION ENABLE PARALLEL DML ; */

    --exec prc_logevent_R5(null,vProcName,'R5 Report table Cursor Start',NULL,current_timestamp);

    --OPEN C_salestrasaction;
    --LOOP
    --BEGIN
    --     FETCH C_salestrasaction BULK COLLECT INTO vSalesTransaction LIMIT 10000;

    --   FORALL i IN 1..vSalesTransaction.COUNT
    /* ORIGSQL: INSERT / *+ APPEND PARALLEL * / */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_GASALESTRANSACTION' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONASSIGNMENT' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESORDER' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_TRANSACTIONADDRESS' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_ADDRESSTYPE' not found */
    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.RPT_BASE_SALESTRANSACTION_R5' not found */

    /* ORIGSQL: INSERT INTO rpt_base_salestransaction_R5 NOLOGGING SELECT stt.TENANTID, stt.SALE(...) */
    INSERT INTO ext.rpt_base_salestransaction_R5
        --NOLOGGING

        SELECT   /* ORIGSQL: SELECT stt.TENANTID, stt.SALESTRANSACTIONSEQ, stt.SALESORDERSEQ, stt.LINENUMBER,(...) */
            stt.TENANTID,
            stt.SALESTRANSACTIONSEQ,
            stt.SALESORDERSEQ,
            stt.LINENUMBER,
            stt.SUBLINENUMBER,
            stt.EVENTTYPESEQ,
            stt.EVENTTYPEID,
            stt.PIPELINERUNSEQ,
            stt.ORIGINTYPEID,
            stt.COMPENSATIONDATE,
            stt.BILLTOADDRESSSEQ,
            stt.SHIPTOADDRESSSEQ,
            stt.OTHERTOADDRESSSEQ,
            stt.ISRUNNABLE,
            stt.BUSINESSUNITMAP,
            stt.ACCOUNTINGDATE,
            stt.PRODUCTID,
            stt.PRODUCTNAME,
            stt.PRODUCTDESCRIPTION,
            stt.NUMBEROFUNITS,
            stt.UNITVALUE,
            stt.UNITTYPEFORUNITVALUE,
            stt.PREADJUSTEDVALUE,
            stt.UNITTYPEFORPREADJUSTEDVALUE,
            stt.VALUE,
            stt.UNITTYPEFORVALUE,
            stt.NATIVECURRENCY,
            stt.NATIVECURRENCYAMOUNT,
            stt.DISCOUNTPERCENT,
            stt.DISCOUNTTYPE,
            stt.PAYMENTTERMS,
            stt.PONUMBER,
            stt.CHANNEL,
            stt.ALTERNATEORDERNUMBER,
            stt.DATASOURCE,
            stt.REASONSEQ,
            stt.COMMENTS,
            stt.GENERICATTRIBUTE1,
            stt.GENERICATTRIBUTE2,
            stt.GENERICATTRIBUTE3,
            stt.GENERICATTRIBUTE4,
            stt.GENERICATTRIBUTE5,
            stt.GENERICATTRIBUTE6,
            stt.GENERICATTRIBUTE7,
            stt.GENERICATTRIBUTE8,
            stt.GENERICATTRIBUTE9,
            stt.GENERICATTRIBUTE10,
            stt.GENERICATTRIBUTE11,
            stt.GENERICATTRIBUTE12,
            stt.GENERICATTRIBUTE13,
            stt.GENERICATTRIBUTE14,
            stt.GENERICATTRIBUTE15,
            stt.GENERICATTRIBUTE16,
            stt.GENERICATTRIBUTE17,
            stt.GENERICATTRIBUTE18,
            stt.GENERICATTRIBUTE19,
            stt.GENERICATTRIBUTE20,
            stt.GENERICATTRIBUTE21,
            stt.GENERICATTRIBUTE22,
            stt.GENERICATTRIBUTE23,
            stt.GENERICATTRIBUTE24,
            stt.GENERICATTRIBUTE25,
            stt.GENERICATTRIBUTE26,
            stt.GENERICATTRIBUTE27,
            stt.GENERICATTRIBUTE28,
            stt.GENERICATTRIBUTE29,
            stt.GENERICATTRIBUTE30,
            stt.GENERICATTRIBUTE31,
            stt.GENERICATTRIBUTE32,
            stt.GENERICNUMBER1,
            stt.UNITTYPEFORGENERICNUMBER1,
            stt.GENERICNUMBER2,
            stt.UNITTYPEFORGENERICNUMBER2,
            stt.GENERICNUMBER3,
            stt.UNITTYPEFORGENERICNUMBER3,
            stt.GENERICNUMBER4,
            stt.UNITTYPEFORGENERICNUMBER4,
            stt.GENERICNUMBER5,
            stt.UNITTYPEFORGENERICNUMBER5,
            stt.GENERICNUMBER6,
            stt.UNITTYPEFORGENERICNUMBER6,
            stt.GENERICDATE1,
            stt.GENERICDATE2,
            stt.GENERICDATE3,
            stt.GENERICDATE4,
            stt.GENERICDATE5,
            stt.GENERICDATE6,
            stt.GENERICBOOLEAN1,
            stt.GENERICBOOLEAN2,
            stt.GENERICBOOLEAN3,
            stt.GENERICBOOLEAN4,
            stt.GENERICBOOLEAN5,
            stt.GENERICBOOLEAN6,
            stt.PROCESSINGUNITSEQ,
            stt.MODIFICATIONDATE,
            stt.UNITTYPEFORLINENUMBER,
            stt.UNITTYPEFORSUBLINENUMBER,
            stt.UNITTYPEFORNUMBEROFUNITS,
            stt.UNITTYPEFORDISCOUNTPERCENT,
            stt.UNITTYPEFORNATIVECURRENCYAMT,
            stt.MODELSEQ,
            stt.EXTENDEDATTRIBUTE1,
            stt.EXTENDEDATTRIBUTE2,
            stt.EXTENDEDATTRIBUTE3,
            stt.EXTENDEDATTRIBUTE4,
            stt.EXTENDEDATTRIBUTE5,
            stt.EXTENDEDATTRIBUTE6,
            stt.EXTENDEDATTRIBUTE7,
            stt.EXTENDEDATTRIBUTE8,
            stt.EXTENDEDATTRIBUTE9,
            stt.EXTENDEDATTRIBUTE10,
            stt.EXTENDEDATTRIBUTE11,
            stt.EXTENDEDATTRIBUTE12,
            stt.EXTENDEDATTRIBUTE13,
            stt.EXTENDEDATTRIBUTE14,
            stt.EXTENDEDATTRIBUTE15,
            stt.EXTENDEDATTRIBUTE16,
            stt.EXTENDEDATTRIBUTE17,
            stt.EXTENDEDATTRIBUTE18,
            stt.EXTENDEDATTRIBUTE19,
            stt.EXTENDEDATTRIBUTE20,
            stt.EXTENDEDDATE1,
            stt.EXTENDEDDATE2,
            stt.EXTENDEDDATE3,
            stt.EXTENDEDDATE4,
            stt.EXTENDEDDATE5,
            stt.EXTENDEDDATE6,
            stt.EXTENDEDDATE7,
            stt.EXTENDEDDATE8,
            stt.EXTENDEDDATE9,
            stt.EXTENDEDDATE10,
            stt.EXTENDEDDATE11,
            stt.EXTENDEDDATE12,
            stt.EXTENDEDDATE13,
            stt.EXTENDEDDATE14,
            stt.EXTENDEDDATE15,
            stt.EXTENDEDDATE16,
            stt.EXTENDEDDATE17,
            stt.EXTENDEDDATE18,
            stt.EXTENDEDDATE19,
            stt.EXTENDEDDATE20,
            stt.EXTENDEDBOOLEAN1,
            stt.EXTENDEDBOOLEAN2,
            stt.EXTENDEDBOOLEAN3,
            stt.EXTENDEDBOOLEAN4,
            stt.EXTENDEDBOOLEAN5,
            stt.EXTENDEDBOOLEAN6,
            stt.EXTENDEDBOOLEAN7,
            stt.EXTENDEDBOOLEAN8,
            stt.EXTENDEDBOOLEAN9,
            stt.EXTENDEDBOOLEAN10,
            stt.EXTENDEDBOOLEAN11,
            stt.EXTENDEDBOOLEAN12,
            stt.EXTENDEDBOOLEAN13,
            stt.EXTENDEDBOOLEAN14,
            stt.EXTENDEDBOOLEAN15,
            stt.EXTENDEDBOOLEAN16,
            stt.EXTENDEDBOOLEAN17,
            stt.EXTENDEDBOOLEAN18,
            stt.EXTENDEDBOOLEAN19,
            stt.EXTENDEDBOOLEAN20,
            stt.EXTENDEDNUMBER1,
            stt.UNITTYPEFOREXTENDEDNUMBER1,
            stt.EXTENDEDNUMBER2,
            stt.UNITTYPEFOREXTENDEDNUMBER2,
            stt.EXTENDEDNUMBER3,
            stt.UNITTYPEFOREXTENDEDNUMBER3,
            stt.EXTENDEDNUMBER4,
            stt.UNITTYPEFOREXTENDEDNUMBER4,
            stt.EXTENDEDNUMBER5,
            stt.UNITTYPEFOREXTENDEDNUMBER5,
            stt.EXTENDEDNUMBER6,
            stt.UNITTYPEFOREXTENDEDNUMBER6,
            stt.EXTENDEDNUMBER7,
            stt.UNITTYPEFOREXTENDEDNUMBER7,
            stt.EXTENDEDNUMBER8,
            stt.UNITTYPEFOREXTENDEDNUMBER8,
            stt.EXTENDEDNUMBER9,
            stt.UNITTYPEFOREXTENDEDNUMBER9,
            stt.EXTENDEDNUMBER10,
            stt.UNITTYPEFOREXTENDEDNUMBER10,
            stt.EXTENDEDNUMBER11,
            stt.UNITTYPEFOREXTENDEDNUMBER11,
            stt.EXTENDEDNUMBER12,
            stt.UNITTYPEFOREXTENDEDNUMBER12,
            stt.EXTENDEDNUMBER13,
            stt.UNITTYPEFOREXTENDEDNUMBER13,
            stt.EXTENDEDNUMBER14,
            stt.UNITTYPEFOREXTENDEDNUMBER14,
            stt.EXTENDEDNUMBER15,
            stt.UNITTYPEFOREXTENDEDNUMBER15,
            stt.EXTENDEDNUMBER16,
            stt.UNITTYPEFOREXTENDEDNUMBER16,
            stt.EXTENDEDNUMBER17,
            stt.UNITTYPEFOREXTENDEDNUMBER17,
            stt.EXTENDEDNUMBER18,
            stt.UNITTYPEFOREXTENDEDNUMBER18,
            stt.EXTENDEDNUMBER19,
            stt.UNITTYPEFOREXTENDEDNUMBER19,
            stt.EXTENDEDNUMBER20,
            stt.UNITTYPEFOREXTENDEDNUMBER20,
            stt.TRNSASSIGNPAEEID,
            stt.TRNSASSIGNPOSITIONNAME,
            stt.TRNSASSIGNTITLENAME,
            stt.TRNSASSIGNATTRIBUTE1,
            stt.TRNSASSIGNATTRIBUTE2,
            stt.TRNSASSIGNATTRIBUTE3,
            stt.TRNSASSIGNATTRIBUTE4,
            stt.TRNSASSIGNATTRIBUTE5,
            stt.TRNSASSIGNATTRIBUTE6,
            stt.TRNSASSIGNATTRIBUTE7,
            stt.TRNSASSIGNATTRIBUTE8,
            stt.TRNSASSIGNATTRIBUTE9,
            stt.TRNSASSIGNATTRIBUTE10,
            stt.TRNSASSIGNATTRIBUTE11,
            stt.TRNSASSIGNATTRIBUTE12,
            stt.TRNSASSIGNATTRIBUTE13,
            stt.TRNSASSIGNATTRIBUTE14,
            stt.TRNSASSIGNATTRIBUTE15,
            stt.TRNSASSIGNATTRIBUTE16,
            stt.TRNSASSIGNNUMBER1,
            stt.UNITTYPEFORTRNSASSIGNNUMBER1,
            stt.TRNSASSIGNNUMBER2,
            stt.UNITTYPEFORTRNSASSIGNNUMBER2,
            stt.TRNSASSIGNNUMBER3,
            stt.UNITTYPEFORTRNSASSIGNNUMBER3,
            stt.TRNSASSIGNNUMBER4,
            stt.UNITTYPEFORTRNSASSIGNNUMBER4,
            stt.TRNSASSIGNNUMBER5,
            stt.UNITTYPEFORTRNSASSIGNNUMBER5,
            stt.TRNSASSIGNNUMBER16,
            stt.UNITTYPEFORTRNSASSIGNNUMBER6,
            stt.TRNSASSIGNDATE1,
            stt.TRNSASSIGNDATE2,
            stt.TRNSASSIGNDATE3,
            stt.TRNSASSIGNDATE4,
            stt.TRNSASSIGNDATE5,
            stt.TRNSASSIGNDATE6,
            stt.TRNSASSIGNBOOLEAN1,
            stt.TRNSASSIGNBOOLEAN2,
            stt.TRNSASSIGNBOOLEAN3,
            stt.TRNSASSIGNBOOLEAN4,
            stt.TRNSASSIGNBOOLEAN5,
            stt.TRNSASSIGNBOOLEAN6,
            NULL,/* --cll.name, */  so.orderid AS SALESORDERID,
            adr.custid AS CUSTID,
            adr.contact AS CONTACT,
            adr.state AS STATE,
            adr.city AS CITY,
            adr.postalcode AS POSTALCODE,
            adr.fax AS FAX,
            adr.address1 AS ADDRESS1,
            adr.country AS COUNTRY
        FROM
            /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
            (
                SELECT   /* ORIGSQL: (SELECT ad.custid custid, ad.salestransactionseq, ad.transactionaddressseq, ad.C(...) */
                    ad.custid AS custid,
                    ad.salestransactionseq,
                    ad.transactionaddressseq,
                    ad.CONTACT,
                    ad.STATE,
                    ad.CITY,
                    ad.POSTALCODE,
                    ad.FAX,
                    ad.ADDRESS1,
                    ad.COUNTRY
                FROM
                    cs_transactionaddress ad,
                    cs_addresstype adt
                WHERE
                    adt.ADDRESSTYPEID = 'BILLTO'
                    AND adt.addresstypeseq = ad.addresstypeseq
            ) AS adr
        RIGHT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (SELECT st.TENANTID TENANTID, st.SALESTRANSACTIONSEQ SALESTRANSACTIONSEQ, st.SAL(...) */
                    st.TENANTID AS TENANTID,
                    st.SALESTRANSACTIONSEQ AS SALESTRANSACTIONSEQ,
                    st.SALESORDERSEQ AS SALESORDERSEQ,
                    st.LINENUMBER AS LINENUMBER,
                    st.SUBLINENUMBER AS SUBLINENUMBER,
                    st.EVENTTYPESEQ AS EVENTTYPESEQ,
                    et.EVENTTYPEID AS EVENTTYPEID,
                    st.PIPELINERUNSEQ AS PIPELINERUNSEQ,
                    st.ORIGINTYPEID AS ORIGINTYPEID,
                    st.COMPENSATIONDATE AS COMPENSATIONDATE,
                    st.BILLTOADDRESSSEQ AS BILLTOADDRESSSEQ,
                    st.SHIPTOADDRESSSEQ AS SHIPTOADDRESSSEQ,
                    st.OTHERTOADDRESSSEQ AS OTHERTOADDRESSSEQ,
                    st.ISRUNNABLE AS ISRUNNABLE,
                    st.BUSINESSUNITMAP AS BUSINESSUNITMAP,
                    st.ACCOUNTINGDATE AS ACCOUNTINGDATE,
                    st.PRODUCTID AS PRODUCTID,
                    st.PRODUCTNAME AS PRODUCTNAME,
                    st.PRODUCTDESCRIPTION AS PRODUCTDESCRIPTION,
                    st.NUMBEROFUNITS AS NUMBEROFUNITS,
                    st.UNITVALUE AS UNITVALUE,
                    st.UNITTYPEFORUNITVALUE AS UNITTYPEFORUNITVALUE,
                    st.PREADJUSTEDVALUE AS PREADJUSTEDVALUE,
                    st.UNITTYPEFORPREADJUSTEDVALUE AS UNITTYPEFORPREADJUSTEDVALUE,
                    st.VALUE AS VALUE,
                    st.UNITTYPEFORVALUE AS UNITTYPEFORVALUE,
                    st.NATIVECURRENCY AS NATIVECURRENCY,
                    st.NATIVECURRENCYAMOUNT AS NATIVECURRENCYAMOUNT,
                    st.DISCOUNTPERCENT AS DISCOUNTPERCENT,
                    st.DISCOUNTTYPE AS DISCOUNTTYPE,
                    st.PAYMENTTERMS AS PAYMENTTERMS,
                    st.PONUMBER AS PONUMBER,
                    st.CHANNEL AS CHANNEL,
                    st.ALTERNATEORDERNUMBER AS ALTERNATEORDERNUMBER,
                    st.DATASOURCE AS DATASOURCE,
                    st.REASONSEQ AS REASONSEQ,
                    st.COMMENTS AS COMMENTS,
                    st.GENERICATTRIBUTE1 AS GENERICATTRIBUTE1,
                    st.GENERICATTRIBUTE2 AS GENERICATTRIBUTE2,
                    st.GENERICATTRIBUTE3 AS GENERICATTRIBUTE3,
                    st.GENERICATTRIBUTE4 AS GENERICATTRIBUTE4,
                    st.GENERICATTRIBUTE5 AS GENERICATTRIBUTE5,
                    st.GENERICATTRIBUTE6 AS GENERICATTRIBUTE6,
                    st.GENERICATTRIBUTE7 AS GENERICATTRIBUTE7,
                    st.GENERICATTRIBUTE8 AS GENERICATTRIBUTE8,
                    st.GENERICATTRIBUTE9 AS GENERICATTRIBUTE9,
                    st.GENERICATTRIBUTE10 AS GENERICATTRIBUTE10,
                    st.GENERICATTRIBUTE11 AS GENERICATTRIBUTE11,
                    st.GENERICATTRIBUTE12 AS GENERICATTRIBUTE12,
                    st.GENERICATTRIBUTE13 AS GENERICATTRIBUTE13,
                    st.GENERICATTRIBUTE14 AS GENERICATTRIBUTE14,
                    st.GENERICATTRIBUTE15 AS GENERICATTRIBUTE15,
                    st.GENERICATTRIBUTE16 AS GENERICATTRIBUTE16,
                    st.GENERICATTRIBUTE17 AS GENERICATTRIBUTE17,
                    st.GENERICATTRIBUTE18 AS GENERICATTRIBUTE18,
                    st.GENERICATTRIBUTE19 AS GENERICATTRIBUTE19,
                    st.GENERICATTRIBUTE20 AS GENERICATTRIBUTE20,
                    st.GENERICATTRIBUTE21 AS GENERICATTRIBUTE21,
                    st.GENERICATTRIBUTE22 AS GENERICATTRIBUTE22,
                    st.GENERICATTRIBUTE23 AS GENERICATTRIBUTE23,
                    st.GENERICATTRIBUTE24 AS GENERICATTRIBUTE24,
                    st.GENERICATTRIBUTE25 AS GENERICATTRIBUTE25,
                    st.GENERICATTRIBUTE26 AS GENERICATTRIBUTE26,
                    st.GENERICATTRIBUTE27 AS GENERICATTRIBUTE27,
                    st.GENERICATTRIBUTE28 AS GENERICATTRIBUTE28,
                    st.GENERICATTRIBUTE29 AS GENERICATTRIBUTE29,
                    st.GENERICATTRIBUTE30 AS GENERICATTRIBUTE30,
                    st.GENERICATTRIBUTE31 AS GENERICATTRIBUTE31,
                    st.GENERICATTRIBUTE32 AS GENERICATTRIBUTE32,
                    st.GENERICNUMBER1 AS GENERICNUMBER1,
                    st.UNITTYPEFORGENERICNUMBER1 AS UNITTYPEFORGENERICNUMBER1,
                    st.GENERICNUMBER2 AS GENERICNUMBER2,
                    st.UNITTYPEFORGENERICNUMBER2 AS UNITTYPEFORGENERICNUMBER2,
                    st.GENERICNUMBER3 AS GENERICNUMBER3,
                    st.UNITTYPEFORGENERICNUMBER3 AS UNITTYPEFORGENERICNUMBER3,
                    st.GENERICNUMBER4 AS GENERICNUMBER4,
                    st.UNITTYPEFORGENERICNUMBER4 AS UNITTYPEFORGENERICNUMBER4,
                    st.GENERICNUMBER5 AS GENERICNUMBER5,
                    st.UNITTYPEFORGENERICNUMBER5 AS UNITTYPEFORGENERICNUMBER5,
                    st.GENERICNUMBER6 AS GENERICNUMBER6,
                    st.UNITTYPEFORGENERICNUMBER6 AS UNITTYPEFORGENERICNUMBER6,
                    st.GENERICDATE1 AS GENERICDATE1,
                    st.GENERICDATE2 AS GENERICDATE2,
                    st.GENERICDATE3 AS GENERICDATE3,
                    st.GENERICDATE4 AS GENERICDATE4,
                    st.GENERICDATE5 AS GENERICDATE5,
                    st.GENERICDATE6 AS GENERICDATE6,
                    st.GENERICBOOLEAN1 AS GENERICBOOLEAN1,
                    st.GENERICBOOLEAN2 AS GENERICBOOLEAN2,
                    st.GENERICBOOLEAN3 AS GENERICBOOLEAN3,
                    st.GENERICBOOLEAN4 AS GENERICBOOLEAN4,
                    st.GENERICBOOLEAN5 AS GENERICBOOLEAN5,
                    st.GENERICBOOLEAN6 AS GENERICBOOLEAN6,
                    st.PROCESSINGUNITSEQ AS PROCESSINGUNITSEQ,
                    st.MODIFICATIONDATE AS MODIFICATIONDATE,
                    st.UNITTYPEFORLINENUMBER AS UNITTYPEFORLINENUMBER,
                    st.UNITTYPEFORSUBLINENUMBER AS UNITTYPEFORSUBLINENUMBER,
                    st.UNITTYPEFORNUMBEROFUNITS AS UNITTYPEFORNUMBEROFUNITS,
                    st.UNITTYPEFORDISCOUNTPERCENT AS UNITTYPEFORDISCOUNTPERCENT,
                    st.UNITTYPEFORNATIVECURRENCYAMT AS UNITTYPEFORNATIVECURRENCYAMT,
                    st.MODELSEQ AS MODELSEQ,
                    ga.GENERICATTRIBUTE1 AS EXTENDEDATTRIBUTE1,
                    ga.GENERICATTRIBUTE2 AS EXTENDEDATTRIBUTE2,
                    ga.GENERICATTRIBUTE3 AS EXTENDEDATTRIBUTE3,
                    ga.GENERICATTRIBUTE4 AS EXTENDEDATTRIBUTE4,
                    ga.GENERICATTRIBUTE5 AS EXTENDEDATTRIBUTE5,
                    ga.GENERICATTRIBUTE6 AS EXTENDEDATTRIBUTE6,
                    ga.GENERICATTRIBUTE7 AS EXTENDEDATTRIBUTE7,
                    ga.GENERICATTRIBUTE8 AS EXTENDEDATTRIBUTE8,
                    ga.GENERICATTRIBUTE9 AS EXTENDEDATTRIBUTE9,
                    ga.GENERICATTRIBUTE10 AS EXTENDEDATTRIBUTE10,
                    ga.GENERICATTRIBUTE11 AS EXTENDEDATTRIBUTE11,
                    ga.GENERICATTRIBUTE12 AS EXTENDEDATTRIBUTE12,
                    ga.GENERICATTRIBUTE13 AS EXTENDEDATTRIBUTE13,
                    ga.GENERICATTRIBUTE14 AS EXTENDEDATTRIBUTE14,
                    ga.GENERICATTRIBUTE15 AS EXTENDEDATTRIBUTE15,
                    ga.GENERICATTRIBUTE16 AS EXTENDEDATTRIBUTE16,
                    ga.GENERICATTRIBUTE17 AS EXTENDEDATTRIBUTE17,
                    ga.GENERICATTRIBUTE18 AS EXTENDEDATTRIBUTE18,
                    ga.GENERICATTRIBUTE19 AS EXTENDEDATTRIBUTE19,
                    ga.GENERICATTRIBUTE20 AS EXTENDEDATTRIBUTE20,
                    ga.GENERICDATE1 AS EXTENDEDDATE1,
                    ga.GENERICDATE2 AS EXTENDEDDATE2,
                    ga.GENERICDATE3 AS EXTENDEDDATE3,
                    ga.GENERICDATE4 AS EXTENDEDDATE4,
                    ga.GENERICDATE5 AS EXTENDEDDATE5,
                    ga.GENERICDATE6 AS EXTENDEDDATE6,
                    ga.GENERICDATE7 AS EXTENDEDDATE7,
                    ga.GENERICDATE8 AS EXTENDEDDATE8,
                    ga.GENERICDATE9 AS EXTENDEDDATE9,
                    ga.GENERICDATE10 AS EXTENDEDDATE10,
                    ga.GENERICDATE11 AS EXTENDEDDATE11,
                    ga.GENERICDATE12 AS EXTENDEDDATE12,
                    ga.GENERICDATE13 AS EXTENDEDDATE13,
                    ga.GENERICDATE14 AS EXTENDEDDATE14,
                    ga.GENERICDATE15 AS EXTENDEDDATE15,
                    ga.GENERICDATE16 AS EXTENDEDDATE16,
                    ga.GENERICDATE17 AS EXTENDEDDATE17,
                    ga.GENERICDATE18 AS EXTENDEDDATE18,
                    ga.GENERICDATE19 AS EXTENDEDDATE19,
                    ga.GENERICDATE20 AS EXTENDEDDATE20,
                    ga.GENERICBOOLEAN1 AS EXTENDEDBOOLEAN1,
                    ga.GENERICBOOLEAN2 AS EXTENDEDBOOLEAN2,
                    ga.GENERICBOOLEAN3 AS EXTENDEDBOOLEAN3,
                    ga.GENERICBOOLEAN4 AS EXTENDEDBOOLEAN4,
                    ga.GENERICBOOLEAN5 AS EXTENDEDBOOLEAN5,
                    ga.GENERICBOOLEAN6 AS EXTENDEDBOOLEAN6,
                    ga.GENERICBOOLEAN7 AS EXTENDEDBOOLEAN7,
                    ga.GENERICBOOLEAN8 AS EXTENDEDBOOLEAN8,
                    ga.GENERICBOOLEAN9 AS EXTENDEDBOOLEAN9,
                    ga.GENERICBOOLEAN10 AS EXTENDEDBOOLEAN10,
                    ga.GENERICBOOLEAN11 AS EXTENDEDBOOLEAN11,
                    ga.GENERICBOOLEAN12 AS EXTENDEDBOOLEAN12,
                    ga.GENERICBOOLEAN13 AS EXTENDEDBOOLEAN13,
                    ga.GENERICBOOLEAN14 AS EXTENDEDBOOLEAN14,
                    ga.GENERICBOOLEAN15 AS EXTENDEDBOOLEAN15,
                    ga.GENERICBOOLEAN16 AS EXTENDEDBOOLEAN16,
                    ga.GENERICBOOLEAN17 AS EXTENDEDBOOLEAN17,
                    ga.GENERICBOOLEAN18 AS EXTENDEDBOOLEAN18,
                    ga.GENERICBOOLEAN19 AS EXTENDEDBOOLEAN19,
                    ga.GENERICBOOLEAN20 AS EXTENDEDBOOLEAN20,
                    ga.GENERICNUMBER1 AS EXTENDEDNUMBER1,
                    ga.UNITTYPEFORGENERICNUMBER1 AS UNITTYPEFOREXTENDEDNUMBER1,
                    ga.GENERICNUMBER2 AS EXTENDEDNUMBER2,
                    ga.UNITTYPEFORGENERICNUMBER2 AS UNITTYPEFOREXTENDEDNUMBER2,
                    ga.GENERICNUMBER3 AS EXTENDEDNUMBER3,
                    ga.UNITTYPEFORGENERICNUMBER3 AS UNITTYPEFOREXTENDEDNUMBER3,
                    ga.GENERICNUMBER4 AS EXTENDEDNUMBER4,
                    ga.UNITTYPEFORGENERICNUMBER4 AS UNITTYPEFOREXTENDEDNUMBER4,
                    ga.GENERICNUMBER5 AS EXTENDEDNUMBER5,
                    ga.UNITTYPEFORGENERICNUMBER5 AS UNITTYPEFOREXTENDEDNUMBER5,
                    ga.GENERICNUMBER6 AS EXTENDEDNUMBER6,
                    ga.UNITTYPEFORGENERICNUMBER6 AS UNITTYPEFOREXTENDEDNUMBER6,
                    ga.GENERICNUMBER7 AS EXTENDEDNUMBER7,
                    ga.UNITTYPEFORGENERICNUMBER7 AS UNITTYPEFOREXTENDEDNUMBER7,
                    ga.GENERICNUMBER8 AS EXTENDEDNUMBER8,
                    ga.UNITTYPEFORGENERICNUMBER8 AS UNITTYPEFOREXTENDEDNUMBER8,
                    ga.GENERICNUMBER9 AS EXTENDEDNUMBER9,
                    ga.UNITTYPEFORGENERICNUMBER9 AS UNITTYPEFOREXTENDEDNUMBER9,
                    ga.GENERICNUMBER10 AS EXTENDEDNUMBER10,
                    ga.UNITTYPEFORGENERICNUMBER10 AS UNITTYPEFOREXTENDEDNUMBER10,
                    ga.GENERICNUMBER11 AS EXTENDEDNUMBER11,
                    ga.UNITTYPEFORGENERICNUMBER11 AS UNITTYPEFOREXTENDEDNUMBER11,
                    ga.GENERICNUMBER12 AS EXTENDEDNUMBER12,
                    ga.UNITTYPEFORGENERICNUMBER12 AS UNITTYPEFOREXTENDEDNUMBER12,
                    ga.GENERICNUMBER13 AS EXTENDEDNUMBER13,
                    ga.UNITTYPEFORGENERICNUMBER13 AS UNITTYPEFOREXTENDEDNUMBER13,
                    ga.GENERICNUMBER14 AS EXTENDEDNUMBER14,
                    ga.UNITTYPEFORGENERICNUMBER14 AS UNITTYPEFOREXTENDEDNUMBER14,
                    ga.GENERICNUMBER15 AS EXTENDEDNUMBER15,
                    ga.UNITTYPEFORGENERICNUMBER15 AS UNITTYPEFOREXTENDEDNUMBER15,
                    ga.GENERICNUMBER16 AS EXTENDEDNUMBER16,
                    ga.UNITTYPEFORGENERICNUMBER16 AS UNITTYPEFOREXTENDEDNUMBER16,
                    ga.GENERICNUMBER17 AS EXTENDEDNUMBER17,
                    ga.UNITTYPEFORGENERICNUMBER17 AS UNITTYPEFOREXTENDEDNUMBER17,
                    ga.GENERICNUMBER18 AS EXTENDEDNUMBER18,
                    ga.UNITTYPEFORGENERICNUMBER18 AS UNITTYPEFOREXTENDEDNUMBER18,
                    ga.GENERICNUMBER19 AS EXTENDEDNUMBER19,
                    ga.UNITTYPEFORGENERICNUMBER19 AS UNITTYPEFOREXTENDEDNUMBER19,
                    ga.GENERICNUMBER20 AS EXTENDEDNUMBER20,
                    ga.UNITTYPEFORGENERICNUMBER20 AS UNITTYPEFOREXTENDEDNUMBER20,
                    trnsa.PAYEEID AS TRNSASSIGNPAEEID,
                    trnsa.POSITIONNAME AS TRNSASSIGNPOSITIONNAME,
                    trnsa.TITLENAME AS TRNSASSIGNTITLENAME,
                    trnsa.GENERICATTRIBUTE1 AS TRNSASSIGNATTRIBUTE1,
                    trnsa.GENERICATTRIBUTE2 AS TRNSASSIGNATTRIBUTE2,
                    trnsa.GENERICATTRIBUTE3 AS TRNSASSIGNATTRIBUTE3,
                    trnsa.GENERICATTRIBUTE4 AS TRNSASSIGNATTRIBUTE4,
                    trnsa.GENERICATTRIBUTE5 AS TRNSASSIGNATTRIBUTE5,
                    trnsa.GENERICATTRIBUTE6 AS TRNSASSIGNATTRIBUTE6,
                    trnsa.GENERICATTRIBUTE7 AS TRNSASSIGNATTRIBUTE7,
                    trnsa.GENERICATTRIBUTE8 AS TRNSASSIGNATTRIBUTE8,
                    trnsa.GENERICATTRIBUTE9 AS TRNSASSIGNATTRIBUTE9,
                    trnsa.GENERICATTRIBUTE10 AS TRNSASSIGNATTRIBUTE10,
                    trnsa.GENERICATTRIBUTE11 AS TRNSASSIGNATTRIBUTE11,
                    trnsa.GENERICATTRIBUTE12 AS TRNSASSIGNATTRIBUTE12,
                    trnsa.GENERICATTRIBUTE13 AS TRNSASSIGNATTRIBUTE13,
                    trnsa.GENERICATTRIBUTE14 AS TRNSASSIGNATTRIBUTE14,
                    trnsa.GENERICATTRIBUTE15 AS TRNSASSIGNATTRIBUTE15,
                    trnsa.GENERICATTRIBUTE16 AS TRNSASSIGNATTRIBUTE16,
                    trnsa.GENERICNUMBER1 AS TRNSASSIGNNUMBER1,
                    trnsa.UNITTYPEFORGENERICNUMBER1 AS UNITTYPEFORTRNSASSIGNNUMBER1,
                    trnsa.GENERICNUMBER2 AS TRNSASSIGNNUMBER2,
                    trnsa.UNITTYPEFORGENERICNUMBER2 AS UNITTYPEFORTRNSASSIGNNUMBER2,
                    trnsa.GENERICNUMBER3 AS TRNSASSIGNNUMBER3,
                    trnsa.UNITTYPEFORGENERICNUMBER3 AS UNITTYPEFORTRNSASSIGNNUMBER3,
                    trnsa.GENERICNUMBER4 AS TRNSASSIGNNUMBER4,
                    trnsa.UNITTYPEFORGENERICNUMBER4 AS UNITTYPEFORTRNSASSIGNNUMBER4,
                    trnsa.GENERICNUMBER5 AS TRNSASSIGNNUMBER5,
                    trnsa.UNITTYPEFORGENERICNUMBER5 AS UNITTYPEFORTRNSASSIGNNUMBER5,
                    trnsa.GENERICNUMBER6 AS TRNSASSIGNNUMBER16,
                    trnsa.UNITTYPEFORGENERICNUMBER6 AS UNITTYPEFORTRNSASSIGNNUMBER6,
                    trnsa.GENERICDATE1 AS TRNSASSIGNDATE1,
                    trnsa.GENERICDATE2 AS TRNSASSIGNDATE2,
                    trnsa.GENERICDATE3 AS TRNSASSIGNDATE3,
                    trnsa.GENERICDATE4 AS TRNSASSIGNDATE4,
                    trnsa.GENERICDATE5 AS TRNSASSIGNDATE5,
                    trnsa.GENERICDATE6 AS TRNSASSIGNDATE6,
                    trnsa.GENERICBOOLEAN1 AS TRNSASSIGNBOOLEAN1,
                    trnsa.GENERICBOOLEAN2 AS TRNSASSIGNBOOLEAN2,
                    trnsa.GENERICBOOLEAN3 AS TRNSASSIGNBOOLEAN3,
                    trnsa.GENERICBOOLEAN4 AS TRNSASSIGNBOOLEAN4,
                    trnsa.GENERICBOOLEAN5 AS TRNSASSIGNBOOLEAN5,
                    trnsa.GENERICBOOLEAN6 AS TRNSASSIGNBOOLEAN6
                FROM
                    /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                    cs_eventtype AS et
                INNER JOIN
                    cs_period AS per
                    ON per.removedate = et.removedate
                    AND per.periodseq = :vperiodseq
                    AND per.removedate = to_date('01/01/2200','MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200','MM/DD/YYYY') */
                INNER JOIN
                    cs_salestransaction AS st
                    ON st.eventtypeseq = et.datatypeseq
                    AND st.processingunitseq = :vprocessingunitseq --and st.GENERICATTRIBUTE3=Trnsa.Positionname --sudhir
                    --and Trnsa.Positionname='T0001'--sudhir
                    /* ORIGSQL: st.processingunitseq = vprocessingunitseq */
                LEFT OUTER JOIN
                    cs_gasalestransaction AS ga
                    ON st.salestransactionseq = ga.salestransactionseq  /* ORIGSQL: st.salestransactionseq = ga.salestransactionseq(+) */
                    AND st.compensationdate < per.enddate
                    AND st.compensationdate >= per.startdate
                INNER JOIN
                    cs_transactionassignment AS trnsa
                    ON st.salesorderseq = trnsa.salesorderseq
                    AND st.salestransactionseq = trnsa.salestransactionseq
            ) AS stt
            ON stt.billtoaddressseq = adr.transactionaddressseq  /* ORIGSQL: stt.billtoaddressseq = adr.transactionaddressseq(+) */
            AND stt.salestransactionseq = adr.salestransactionseq  /* ORIGSQL: stt.salestransactionseq = adr.salestransactionseq(+) */
        LEFT OUTER JOIN
            (
                SELECT   /* ORIGSQL: (select so.orderid,so.salesorderseq from cs_salesorder so where so.processinguni(...) */
                    so.orderid,
                    so.salesorderseq
                FROM
                    cs_salesorder so
                WHERE
                    so.processingunitseq = :vprocessingunitseq
                    AND so.removedate = to_date('01/01/2200','MM/DD/YYYY')  /* ORIGSQL: TO_DATE('01/01/2200','MM/DD/YYYY') */
            ) AS so
            ON --stt.productname = cll.classifierid(+)
            stt.salesorderseq = so.salesorderseq;--    (
    --      select cl.classifierid classifierid, ct.name name
    --      from
    --            cs_classifier cl,
    --            cs_category_classifiers cc,
    --            cs_category ct,
    --            cs_categorytree cg
    --      where cl.classifierseq = cc.classifierseq
    -- and cc.categoryseq = ct.ruleelementseq
    -- and cg.categorytreeseq = ct.categorytreeseq
    -- and cg.name = 'Product_Offer_Type'
    -- and cl.removedate = PKG_REPORTING_EXTRACT.cEndofTime
    -- and cc.removedate = PKG_REPORTING_EXTRACT.cEndofTime
    -- and ct.removedate = PKG_REPORTING_EXTRACT.cEndofTime
    -- and cg.removedate = PKG_REPORTING_EXTRACT.cEndofTime
--    )cll
/* ORIGSQL: stt.salesorderseq = so.salesorderseq(+) */

/* ORIGSQL: COMMIT; */
COMMIT;

    --    EXIT WHEN C_salestrasaction%NOTFOUND;
    --END;
    --END LOOP;
    --CLOSE C_salestrasaction;

    --exec prc_logevent_R5(null,vProcName,'R5 Report table Cursor Start',NULL,current_timestamp);

    /* ORIGSQL: EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML' ; */
    /* RESOLVE: ALTER SESSION statement: Statement 'ALTER SESSION DISABLE PARALLEL' not supported; convert manually */
    /* ALTER SESSION DISABLE PARALLEL DML ; */

    --  EXCEPTION
    -- WHEN OTHERS THEN
    --            vsqlerrm := SUBSTR (SQLERRM, 1, 3900);
    --            prc_logevent(vperiodcalendarrow.name,vprocname,'ERROR',NULL,vsqlerrm);
    --            raise_application_error( -20911,'Error raised: '||vprocname||' Failed: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' - '||vsqlerrm);
END