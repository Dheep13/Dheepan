import sqlite3

# Database connection
db_connection = sqlite3.connect('local.db')
cursor = db_connection.cursor()

# Create a table that matches the structure of CS_Salestransaction in HANA
cursor.execute('''
    CREATE TABLE  CS_SALESTRANSACTION(
	"TENANTID" VARCHAR(4),
	"SALESTRANSACTIONSEQ" BIGINT  NOT NULL,
	"SALESORDERSEQ" BIGINT  NOT NULL,
	"LINENUMBER" BIGINT  NOT NULL,
	"SUBLINENUMBER" BIGINT  NOT NULL,
	"EVENTTYPESEQ" BIGINT  NOT NULL,
	"PIPELINERUNSEQ" BIGINT ,
	"ORIGINTYPEID" VARCHAR(10) NOT NULL,
	"COMPENSATIONDATE" LONGDATE  NOT NULL,
	"BILLTOADDRESSSEQ" BIGINT ,
	"SHIPTOADDRESSSEQ" BIGINT ,
	"OTHERTOADDRESSSEQ" BIGINT ,
	"ISRUNNABLE" SMALLINT  NOT NULL,
	"BUSINESSUNITMAP" BIGINT ,
	"ACCOUNTINGDATE" LONGDATE ,
	"PRODUCTID" VARCHAR(127),
	"PRODUCTNAME" VARCHAR(100),
	"PRODUCTDESCRIPTION" VARCHAR(255),
	"NUMBEROFUNITS" DECIMAL(25, 10) ,
	"UNITVALUE" DECIMAL(25, 10) ,
	"UNITTYPEFORUNITVALUE" BIGINT ,
	"PREADJUSTEDVALUE" DECIMAL(25, 10)  NOT NULL,
	"UNITTYPEFORPREADJUSTEDVALUE" BIGINT  NOT NULL,
	"VALUE" DECIMAL(25, 10)  NOT NULL,
	"UNITTYPEFORVALUE" BIGINT  NOT NULL,
	"NATIVECURRENCY" VARCHAR(40),
	"NATIVECURRENCYAMOUNT" DECIMAL(25, 10) ,
	"DISCOUNTPERCENT" DECIMAL(25, 10) ,
	"DISCOUNTTYPE" VARCHAR(20),
	"PAYMENTTERMS" VARCHAR(10),
	"PONUMBER" VARCHAR(30),
	"CHANNEL" VARCHAR(20),
	"ALTERNATEORDERNUMBER" VARCHAR(40),
	"DATASOURCE" VARCHAR(20),
	"REASONSEQ" BIGINT ,
	"COMMENTS" VARCHAR(255),
	"GENERICATTRIBUTE1" VARCHAR(255),
	"GENERICATTRIBUTE2" VARCHAR(255),
	"GENERICATTRIBUTE3" VARCHAR(255),
	"GENERICATTRIBUTE4" VARCHAR(255),
	"GENERICATTRIBUTE5" VARCHAR(255),
	"GENERICATTRIBUTE6" VARCHAR(255),
	"GENERICATTRIBUTE7" VARCHAR(255),
	"GENERICATTRIBUTE8" VARCHAR(255),
	"GENERICATTRIBUTE9" VARCHAR(255),
	"GENERICATTRIBUTE10" VARCHAR(255),
	"GENERICATTRIBUTE11" VARCHAR(255),
	"GENERICATTRIBUTE12" VARCHAR(255),
	"GENERICATTRIBUTE13" VARCHAR(255),
	"GENERICATTRIBUTE14" VARCHAR(255),
	"GENERICATTRIBUTE15" VARCHAR(255),
	"GENERICATTRIBUTE16" VARCHAR(255),
	"GENERICATTRIBUTE17" VARCHAR(255),
	"GENERICATTRIBUTE18" VARCHAR(255),
	"GENERICATTRIBUTE19" VARCHAR(255),
	"GENERICATTRIBUTE20" VARCHAR(255),
	"GENERICATTRIBUTE21" VARCHAR(255),
	"GENERICATTRIBUTE22" VARCHAR(255),
	"GENERICATTRIBUTE23" VARCHAR(255),
	"GENERICATTRIBUTE24" VARCHAR(255),
	"GENERICATTRIBUTE25" VARCHAR(255),
	"GENERICATTRIBUTE26" VARCHAR(255),
	"GENERICATTRIBUTE27" VARCHAR(255),
	"GENERICATTRIBUTE28" VARCHAR(255),
	"GENERICATTRIBUTE29" VARCHAR(255),
	"GENERICATTRIBUTE30" VARCHAR(255),
	"GENERICATTRIBUTE31" VARCHAR(255),
	"GENERICATTRIBUTE32" VARCHAR(255),
	"GENERICNUMBER1" DECIMAL(25, 10) ,
	"UNITTYPEFORGENERICNUMBER1" BIGINT ,
	"GENERICNUMBER2" DECIMAL(25, 10) ,
	"UNITTYPEFORGENERICNUMBER2" BIGINT ,
	"GENERICNUMBER3" DECIMAL(25, 10) ,
	"UNITTYPEFORGENERICNUMBER3" BIGINT ,
	"GENERICNUMBER4" DECIMAL(25, 10) ,
	"UNITTYPEFORGENERICNUMBER4" BIGINT ,
	"GENERICNUMBER5" DECIMAL(25, 10) ,
	"UNITTYPEFORGENERICNUMBER5" BIGINT ,
	"GENERICNUMBER6" DECIMAL(25, 10) ,
	"UNITTYPEFORGENERICNUMBER6" BIGINT ,
	"GENERICDATE1" LONGDATE ,
	"GENERICDATE2" LONGDATE ,
	"GENERICDATE3" LONGDATE ,
	"GENERICDATE4" LONGDATE ,
	"GENERICDATE5" LONGDATE ,
	"GENERICDATE6" LONGDATE ,
	"GENERICBOOLEAN1" SMALLINT ,
	"GENERICBOOLEAN2" SMALLINT ,
	"GENERICBOOLEAN3" SMALLINT ,
	"GENERICBOOLEAN4" SMALLINT ,
	"GENERICBOOLEAN5" SMALLINT ,
	"GENERICBOOLEAN6" SMALLINT ,
	"PROCESSINGUNITSEQ" BIGINT  NOT NULL,
	"MODIFICATIONDATE" LONGDATE  NOT NULL,
	"UNITTYPEFORLINENUMBER" BIGINT ,
	"UNITTYPEFORSUBLINENUMBER" BIGINT ,
	"UNITTYPEFORNUMBEROFUNITS" BIGINT ,
	"UNITTYPEFORDISCOUNTPERCENT" BIGINT ,
	"UNITTYPEFORNATIVECURRENCYAMT" BIGINT ,
	"MODELSEQ" BIGINT  NOT NULL,
	"ISPURGED" SMALLINT 
)
''')

# Commit the table creation
db_connection.commit()

# Close the database connection
db_connection.close()
