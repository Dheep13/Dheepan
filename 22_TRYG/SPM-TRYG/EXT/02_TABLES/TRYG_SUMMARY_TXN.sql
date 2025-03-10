

DROP TABLE EXT.TRYG_SUMMARY_TXN;
CREATE COLUMN TABLE EXT.TRYG_SUMMARY_TXN (
    TENANTID VARCHAR(4),
    STAGESALESTRANSACTIONSEQ BIGINT CS_FIXED NOT NULL,
    BATCHNAME VARCHAR(90) NOT NULL,
    ORDERID VARCHAR(40) NOT NULL,
    LINENUMBER BIGINT CS_FIXED NOT NULL,
    SUBLINENUMBER BIGINT CS_FIXED NOT NULL,
    EVENTTYPEID VARCHAR(40) NOT NULL,
    SALESTRANSACTIONSEQ BIGINT CS_FIXED,
    SALESORDERSEQ BIGINT CS_FIXED,
    ACCOUNTINGDATE LONGDATE CS_LONGDATE,
    PRODUCTID VARCHAR(127),
    PRODUCTNAME VARCHAR(100),
    PRODUCTDESCRIPTION VARCHAR(255),
    VALUE DECIMAL(25, 10) CS_FIXED NOT NULL,
    UNITTYPEFORVALUE VARCHAR(40) NOT NULL,
    NUMBEROFUNITS DECIMAL(25, 10) CS_FIXED,
    UNITVALUE DECIMAL(25, 10) CS_FIXED,
    UNITTYPEFORUNITVALUE VARCHAR(40),
    COMPENSATIONDATE LONGDATE CS_LONGDATE NOT NULL,
    PAYMENTTERMS NVARCHAR(10),
    PONUMBER NVARCHAR(30),
    CHANNEL VARCHAR(20),
    ALTERNATEORDERNUMBER VARCHAR(40),
    DATASOURCE VARCHAR(20),
    NATIVECURRENCY VARCHAR(40),
    NATIVECURRENCYAMOUNT DECIMAL(25, 10) CS_FIXED,
    DISCOUNTPERCENT DECIMAL(25, 10) CS_FIXED,
    DISCOUNTTYPE VARCHAR(20),
    BILLTOCUSTID NVARCHAR(50),
    BILLTOCONTACT NVARCHAR(127),
    BILLTOCOMPANY NVARCHAR(90),
    BILLTOAREACODE NVARCHAR(5),
    BILLTOPHONE NVARCHAR(35),
    BILLTOFAX NVARCHAR(35),
    BILLTOADDRESS1 NVARCHAR(255),
    BILLTOADDRESS2 NVARCHAR(255),
    BILLTOADDRESS3 NVARCHAR(255),
    BILLTOCITY NVARCHAR(90),
    BILLTOSTATE NVARCHAR(90),
    BILLTOCOUNTRY NVARCHAR(90),
    BILLTOPOSTALCODE NVARCHAR(40),
    BILLTOINDUSTRY NVARCHAR(100),
    BILLTOGEOGRAPHY NVARCHAR(100),
    SHIPTOCUSTID NVARCHAR(50),
    SHIPTOCONTACT NVARCHAR(127),
    SHIPTOCOMPANY NVARCHAR(90),
    SHIPTOAREACODE NVARCHAR(5),
    SHIPTOPHONE NVARCHAR(35),
    SHIPTOFAX NVARCHAR(35),
    SHIPTOADDRESS1 NVARCHAR(255),
    SHIPTOADDRESS2 NVARCHAR(255),
    SHIPTOADDRESS3 NVARCHAR(255),
    SHIPTOCITY NVARCHAR(90),
    SHIPTOSTATE NVARCHAR(90),
    SHIPTOCOUNTRY NVARCHAR(90),
    SHIPTOPOSTALCODE NVARCHAR(40),
    SHIPTOINDUSTRY NVARCHAR(100),
    SHIPTOGEOGRAPHY NVARCHAR(100),
    OTHERTOCUSTID NVARCHAR(50),
    OTHERTOCONTACT NVARCHAR(127),
    OTHERTOCOMPANY NVARCHAR(90),
    OTHERTOAREACODE NVARCHAR(5),
    OTHERTOPHONE NVARCHAR(35),
    OTHERTOFAX NVARCHAR(35),
    OTHERTOADDRESS1 NVARCHAR(255),
    OTHERTOADDRESS2 NVARCHAR(255),
    OTHERTOADDRESS3 NVARCHAR(255),
    OTHERTOCITY NVARCHAR(90),
    OTHERTOSTATE NVARCHAR(90),
    OTHERTOCOUNTRY NVARCHAR(90),
    OTHERTOPOSTALCODE NVARCHAR(40),
    OTHERTOINDUSTRY NVARCHAR(100),
    OTHERTOGEOGRAPHY NVARCHAR(100),
    REASONID VARCHAR(40),
    COMMENTS VARCHAR(255),
    STAGEPROCESSDATE LONGDATE CS_LONGDATE,
    STAGEPROCESSFLAG SMALLINT CS_INT NOT NULL,
    BUSINESSUNITNAME VARCHAR(255),
    BUSINESSUNITMAP BIGINT CS_FIXED,
    GENERICATTRIBUTE1 VARCHAR(255),
    GENERICATTRIBUTE2 VARCHAR(255),
    GENERICATTRIBUTE3 VARCHAR(255),
    GENERICATTRIBUTE4 VARCHAR(255),
    GENERICATTRIBUTE5 VARCHAR(255),
    GENERICATTRIBUTE6 VARCHAR(255),
    GENERICATTRIBUTE7 VARCHAR(255),
    GENERICATTRIBUTE8 VARCHAR(255),
    GENERICATTRIBUTE9 VARCHAR(255),
    GENERICATTRIBUTE10 VARCHAR(255),
    GENERICATTRIBUTE11 VARCHAR(255),
    GENERICATTRIBUTE12 VARCHAR(255),
    GENERICATTRIBUTE13 VARCHAR(255),
    GENERICATTRIBUTE14 VARCHAR(255),
    GENERICATTRIBUTE15 VARCHAR(255),
    GENERICATTRIBUTE16 VARCHAR(255),
    GENERICATTRIBUTE17 VARCHAR(255),
    GENERICATTRIBUTE18 VARCHAR(255),
    GENERICATTRIBUTE19 VARCHAR(255),
    GENERICATTRIBUTE20 VARCHAR(255),
    GENERICATTRIBUTE21 VARCHAR(255),
    GENERICATTRIBUTE22 VARCHAR(255),
    GENERICATTRIBUTE23 VARCHAR(255),
    GENERICATTRIBUTE24 VARCHAR(255),
    GENERICATTRIBUTE25 VARCHAR(255),
    GENERICATTRIBUTE26 VARCHAR(255),
    GENERICATTRIBUTE27 VARCHAR(255),
    GENERICATTRIBUTE28 VARCHAR(255),
    GENERICATTRIBUTE29 VARCHAR(255),
    GENERICATTRIBUTE30 VARCHAR(255),
    GENERICATTRIBUTE31 VARCHAR(255),
    GENERICATTRIBUTE32 VARCHAR(255),
    GENERICNUMBER1 DECIMAL(25, 10) CS_FIXED,
    UNITTYPEFORGENERICNUMBER1 VARCHAR(40),
    GENERICNUMBER2 DECIMAL(25, 10) CS_FIXED,
    UNITTYPEFORGENERICNUMBER2 VARCHAR(40),
    GENERICNUMBER3 DECIMAL(25, 10) CS_FIXED,
    UNITTYPEFORGENERICNUMBER3 VARCHAR(40),
    GENERICNUMBER4 DECIMAL(25, 10) CS_FIXED,
    UNITTYPEFORGENERICNUMBER4 VARCHAR(40),
    GENERICNUMBER5 DECIMAL(25, 10) CS_FIXED,
    UNITTYPEFORGENERICNUMBER5 VARCHAR(40),
    GENERICNUMBER6 DECIMAL(25, 10) CS_FIXED,
    UNITTYPEFORGENERICNUMBER6 VARCHAR(40),
    GENERICDATE1 LONGDATE CS_LONGDATE,
    GENERICDATE2 LONGDATE CS_LONGDATE,
    GENERICDATE3 LONGDATE CS_LONGDATE,
    GENERICDATE4 LONGDATE CS_LONGDATE,
    GENERICDATE5 LONGDATE CS_LONGDATE,
    GENERICDATE6 LONGDATE CS_LONGDATE,
    GENERICBOOLEAN1 SMALLINT CS_INT,
    GENERICBOOLEAN2 SMALLINT CS_INT,
    GENERICBOOLEAN3 SMALLINT CS_INT,
    GENERICBOOLEAN4 SMALLINT CS_INT,
    GENERICBOOLEAN5 SMALLINT CS_INT,
    GENERICBOOLEAN6 SMALLINT CS_INT,
    STAGEERRORCODE BIGINT CS_FIXED,
    COMPENSATIONDATE_OLD LONGDATE CS_LONGDATE,
    PUSEQ_OLD BIGINT CS_FIXED
) UNLOAD PRIORITY 5 AUTO MERGE;