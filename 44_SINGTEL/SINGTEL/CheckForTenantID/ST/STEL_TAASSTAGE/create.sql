CREATE VIEW "EXT"."STEL_TAASSTAGE" ( "ORDERID", "LINENUMBER", "SUBLINENUMBER", "EVENTTYPEID", "PAYEEID", "PAYEETYPE", "POSITIONNAME", "TITLENAME", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "GENERICATTRIBUTE5", "GENERICATTRIBUTE6", "GENERICATTRIBUTE7", "GENERICATTRIBUTE8", "GENERICATTRIBUTE9", "GENERICATTRIBUTE10", "GENERICATTRIBUTE11", "GENERICATTRIBUTE12", "GENERICATTRIBUTE13", "GENERICATTRIBUTE14", "GENERICATTRIBUTE15", "GENERICATTRIBUTE16", "GENERICNUMBER1", "UNITTYPEFORGENERICNUMBER1", "GENERICNUMBER2", "UNITTYPEFORGENERICNUMBER2", "GENERICNUMBER3", "UNITTYPEFORGENERICNUMBER3", "GENERICNUMBER4", "UNITTYPEFORGENERICNUMBER4", "GENERICNUMBER5", "UNITTYPEFORGENERICNUMBER5", "GENERICNUMBER6", "UNITTYPEFORGENERICNUMBER6", "GENERICDATE1", "GENERICDATE2", "GENERICDATE3", "GENERICDATE4", "GENERICDATE5", "GENERICDATE6", "GENERICBOOLEAN1", "GENERICBOOLEAN2", "GENERICBOOLEAN3", "GENERICBOOLEAN4", "GENERICBOOLEAN5", "GENERICBOOLEAN6" ) AS (SELECT
     
    SO.orderid,
    ST.linenumber,
    ST.sublinenumber,
    ET.eventtypeid,
    TA.payeeid,
    null payeetype,
    TA.positionname,
    TA.titlename,
    TA.genericattribute1,
    TA.genericattribute2,
    TA.genericattribute3,
    TA.genericattribute4,
    TA.genericattribute5,
    TA.genericattribute6,
    TA.genericattribute7,
    TA.genericattribute8,
    TA.genericattribute9,
    TA.genericattribute10,
    TA.genericattribute11,
    TA.genericattribute12,
    TA.genericattribute13,
    TA.genericattribute14,
    TA.genericattribute15,
    TA.genericattribute16,
    TA.genericnumber1,
    UT1.NAME unittypeforgenericnumber1,
    TA.genericnumber2,
    UT2.NAME unittypeforgenericnumber2,
    TA.genericnumber3,
    UT3.NAME unittypeforgenericnumber3,
    TA.genericnumber4,
    UT4.NAME unittypeforgenericnumber4,
    TA.genericnumber5,
    UT5.NAME unittypeforgenericnumber5,
    TA.genericnumber6,
    UT6.NAME unittypeforgenericnumber6,
    TA.genericdate1,
    TA.genericdate2,
    TA.genericdate3,
    TA.genericdate4,
    TA.genericdate5,
    TA.genericdate6,
    TA.genericboolean1,
    TA.genericboolean2,
    TA.genericboolean3,
    TA.genericboolean4,
    TA.genericboolean5,
    TA.genericboolean6

  	from cs_Salestransaction st
    JOIN cs_transactionassignment ta on
    ta.processingunitseq=st.processingunitseq and ta.tenantid=st.tenantid
    and ta.salestransactionseq=st.salestransactionseq and ta.compensationdate=st.compensationdate
	join CS_Salesorder so on so.salesorderseq=st.salesorderseq and so.removedate=to_Date('22000101','YYYYMMDD')
    and so.processingunitseq=st.processingunitseq and so.tenantid=st.tenantid
	join cs_eventtype et on et.datatypeseq=st.eventtypeseq and et.removedate=to_Date('22000101','YYYYMMDD')
    and et.tenantid=st.tenantid
    left join cs_unittype ut1 on ta.unittypeforgenericnumber1=ut1.unittypeseq and ut1.removedate=to_Date('22000101','YYYYMMDD') 
        and ut1.tenantid=st.tenantid
	left join cs_unittype ut2 on ta.unittypeforgenericnumber2=ut2.unittypeseq and ut2.removedate=to_Date('22000101','YYYYMMDD') 
        and ut2.tenantid=st.tenantid
	left join cs_unittype ut3 on ta.unittypeforgenericnumber3=ut3.unittypeseq and ut3.removedate=to_Date('22000101','YYYYMMDD') 
        and ut3.tenantid=st.tenantid
	left join cs_unittype ut4 on ta.unittypeforgenericnumber4=ut4.unittypeseq and ut4.removedate=to_Date('22000101','YYYYMMDD') 
        and ut4.tenantid=st.tenantid
	left join cs_unittype ut5 on ta.unittypeforgenericnumber5=ut5.unittypeseq and ut5.removedate=to_Date('22000101','YYYYMMDD') 
        and ut5.tenantid=st.tenantid
	left join cs_unittype ut6 on ta.unittypeforgenericnumber6=ut6.unittypeseq and ut6.removedate=to_Date('22000101','YYYYMMDD') 
        and ut6.tenantid=st.tenantid
    where st.tenantid='STEL' and st.processingunitseq  = (Select processingunitseq from cs_processingunit where name='Singtel_PU')) WITH READ ONLY