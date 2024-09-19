CREATE VIEW "EXT"."STEL_TXNASSTAGE" ( "ORDERID", "LINENUMBER", "SUBLINENUMBER", "EVENTTYPEID", "ACCOUNTINGDATE", "PRODUCTID", "PRODUCTNAME", "PRODUCTDESCRIPTION", "VALUE", "UNITTYPEFORVALUE", "NUMBEROFUNITS", "UNITVALUE", "UNITTYPEFORUNITVALUE", "COMPENSATIONDATE", "PAYMENTTERMS", "PONUMBER", "CHANNEL", "ALTERNATEORDERNUMBER", "DATASOURCE", "NATIVECURRENCY", "NATIVECURRENCYAMOUNT", "DISCOUNTPERCENT", "DISCOUNTTYPE", "BTCUSTID", "BTCONTACT", "BTCOMPANY", "BTAREACODE", "BTPHONE", "BTFAX", "BTADDRESS1", "BTADDRESS2", "BTADDRESS3", "BTCITY", "BTSTATE", "BTCOUNTRY", "BTPOSTALCODE", "BTINDUSTRY", "BTGEOGRAPHY", "SHCUSTID", "SHCONTACT", "SHCOMPANY", "SHAREACODE", "SHPHONE", "SHFAX", "SHADDRESS1", "SHADDRESS2", "SHADDRESS3", "SHCITY", "SHSTATE", "SHCOUNTRY", "SHPOSTALCODE", "SHINDUSTRY", "SHGEOGRAPHY", "OTCUSTID", "OTCONTACT", "OTCOMPANY", "OTAREACODE", "OTPHONE", "OTFAX", "OTADDRESS1", "OTADDRESS2", "OTADDRESS3", "OTCITY", "OTSTATE", "OTCOUNTRY", "OTPOSTALCODE", "OTINDUSTRY", "OTGEOGRAPHY", "REASONID", "COMMENTS", "STAGEPROCESSDATE", "STAGEPROCESSFLAG", "BUSINESSUNITNAME", "BUSINESSUNITMAP", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "GENERICATTRIBUTE5", "GENERICATTRIBUTE6", "GENERICATTRIBUTE7", "GENERICATTRIBUTE8", "GENERICATTRIBUTE9", "GENERICATTRIBUTE10", "GENERICATTRIBUTE11", "GENERICATTRIBUTE12", "GENERICATTRIBUTE13", "GENERICATTRIBUTE14", "GENERICATTRIBUTE15", "GENERICATTRIBUTE16", "GENERICATTRIBUTE17", "GENERICATTRIBUTE18", "GENERICATTRIBUTE19", "GENERICATTRIBUTE20", "GENERICATTRIBUTE21", "GENERICATTRIBUTE22", "GENERICATTRIBUTE23", "GENERICATTRIBUTE24", "GENERICATTRIBUTE25", "GENERICATTRIBUTE26", "GENERICATTRIBUTE27", "GENERICATTRIBUTE28", "GENERICATTRIBUTE29", "GENERICATTRIBUTE30", "GENERICATTRIBUTE31", "GENERICATTRIBUTE32", "GENERICNUMBER1", "UNITTYPEFORGENERICNUMBER1", "GENERICNUMBER2", "UNITTYPEFORGENERICNUMBER2", "GENERICNUMBER3", "UNITTYPEFORGENERICNUMBER3", "GENERICNUMBER4", "UNITTYPEFORGENERICNUMBER4", "GENERICNUMBER5", "UNITTYPEFORGENERICNUMBER5", "GENERICNUMBER6", "UNITTYPEFORGENERICNUMBER6", "GENERICDATE1", "GENERICDATE2", "GENERICDATE3", "GENERICDATE4", "GENERICDATE5", "GENERICDATE6", "GENERICBOOLEAN1", "GENERICBOOLEAN2", "GENERICBOOLEAN3", "GENERICBOOLEAN4", "GENERICBOOLEAN5", "GENERICBOOLEAN6" ) AS (select	so.orderid,
    st.linenumber,
    st.sublinenumber,
    et.eventtypeid,
    st.accountingdate,
    st.productid,
    st.productname,
    st.productdescription,
    st.value,
    st.unittypeforvalue,
    st.numberofunits,
    st.unitvalue,
    st.unittypeforunitvalue,
    st.compensationdate,
    st.paymentterms,
    st.ponumber,
    st.channel,
    st.alternateordernumber,
    st.datasource,
    st.nativecurrency,
    st.nativecurrencyamount,
    st.discountpercent,
    st.discounttype,
    bt.custid BTCUSTID,
    bt.contact BTCONTACT,
    bt.company BTCOMPANY,
    bt.areacode BTAREACODE,
    bt.phone BTPHONE,
    bt.fax BTFAX,
    bt.address1 BTADDRESS1,
    bt.address2 BTADDRESS2,
    bt.address3 BTADDRESS3,
    bt.city  BTCITY,
    bt.state  BTSTATE,
    bt.country  BTCOUNTRY,
    bt.postalcode  BTPOSTALCODE,
    bt.industry  BTINDUSTRY,
    bt.geography  BTGEOGRAPHY,
    sh.custid  SHCUSTID,
    sh.contact SHCONTACT,
    sh.company SHCOMPANY,
    sh.areacode SHAREACODE,
    sh.phone SHPHONE,
    sh.fax SHFAX,
    sh.address1 SHADDRESS1,
    sh.address2 SHADDRESS2,
    sh.address3 SHADDRESS3,
    sh.city SHCITY,
    sh.state SHSTATE,
    sh.country SHCOUNTRY,
    sh.postalcode SHPOSTALCODE,
    sh.industry SHINDUSTRY,
    sh.geography SHGEOGRAPHY,
    ot.custid OTCUSTID,
    ot.contact OTCONTACT,
    ot.company OTCOMPANY,
    ot.areacode OTAREACODE,
    ot.phone OTPHONE,
    ot.fax OTFAX,
    ot.address1 OTADDRESS1,
    ot.address2 OTADDRESS2,
    ot.address3 OTADDRESS3,
    ot.city OTCITY,
    ot.state OTSTATE,
    ot.country OTCOUNTRY,
    ot.postalcode OTPOSTALCODE,
    ot.industry OTINDUSTRY,
    ot.geography OTGEOGRAPHY,
    null reasonid,
    st.comments,
    null stageprocessdate,
    null stageprocessflag,
    'ConSales_External' businessunitname,
    null businessunitmap,
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
    ut1.name unittypeforgenericnumber1,
    st.genericnumber2,
    ut2.name unittypeforgenericnumber2,
    st.genericnumber3,
    ut3.name unittypeforgenericnumber3,
    st.genericnumber4,
    ut4.name unittypeforgenericnumber4,
    st.genericnumber5,
    ut5.name unittypeforgenericnumber5,
    st.genericnumber6,
    ut6.name unittypeforgenericnumber6,
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
	
	from cs_Salestransaction st
	join CS_Salesorder so on so.salesorderseq=st.salesorderseq and so.removedate=to_Date('22000101','YYYYMMDD')
    and so.processingunitseq=st.processingunitseq and so.tenantid=st.tenantid
	join cs_eventtype et on et.datatypeseq=st.eventtypeseq and et.removedate=to_Date('22000101','YYYYMMDD')
    and et.tenantid=st.tenantid
	left join  ( select * from cs_transactionaddress
     where addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='BILLTO')
        ) bt on bt.salestransactionseq=st.salestransactionseq
        and bt.processingunitseq=st.processingunitseq and bt.tenantid=st.tenantid and bt.compensationdate=st.compensationdate
	left join  ( select * from cs_transactionaddress
     where addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='SHIPTO')
        ) sh on sh.salestransactionseq=st.salestransactionseq
        and sh.processingunitseq=st.processingunitseq and sh.tenantid=st.tenantid and sh.compensationdate=st.compensationdate
	left join  ( select * from cs_transactionaddress
     where addresstypeseq in (select addresstypeseq from cs_addresstype where addresstypeid='OTHERTO')
         ) ot on ot.salestransactionseq=st.salestransactionseq
         and ot.processingunitseq=st.processingunitseq and ot.tenantid=st.tenantid and ot.compensationdate=st.compensationdate
	left join cs_unittype ut1 on st.unittypeforgenericnumber1=ut1.unittypeseq and ut1.removedate=to_Date('22000101','YYYYMMDD') 
        and ut1.tenantid=st.tenantid
	left join cs_unittype ut2 on st.unittypeforgenericnumber2=ut2.unittypeseq and ut2.removedate=to_Date('22000101','YYYYMMDD') 
        and ut2.tenantid=st.tenantid
	left join cs_unittype ut3 on st.unittypeforgenericnumber3=ut3.unittypeseq and ut3.removedate=to_Date('22000101','YYYYMMDD') 
        and ut3.tenantid=st.tenantid
	left join cs_unittype ut4 on st.unittypeforgenericnumber4=ut4.unittypeseq and ut4.removedate=to_Date('22000101','YYYYMMDD') 
        and ut4.tenantid=st.tenantid
	left join cs_unittype ut5 on st.unittypeforgenericnumber5=ut5.unittypeseq and ut5.removedate=to_Date('22000101','YYYYMMDD') 
        and ut5.tenantid=st.tenantid
	left join cs_unittype ut6 on st.unittypeforgenericnumber6=ut6.unittypeseq and ut6.removedate=to_Date('22000101','YYYYMMDD') 
        and ut6.tenantid=st.tenantid
    where st.tenantid='STEL' and st.processingunitseq  = (Select processingunitseq from cs_processingunit where name='Singtel_PU')) WITH READ ONLY