CREATE VIEW "EXT"."STEL_GATXNASSTAGE" ( "ORDERID", "LINENUMBER", "SUBLINENUMBER", "EVENTTYPEID", "PAGENUMBER", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "GENERICATTRIBUTE5", "GENERICATTRIBUTE6", "GENERICATTRIBUTE7", "GENERICATTRIBUTE8", "GENERICATTRIBUTE9", "GENERICATTRIBUTE10", "GENERICATTRIBUTE11", "GENERICATTRIBUTE12", "GENERICATTRIBUTE13", "GENERICATTRIBUTE14", "GENERICATTRIBUTE15", "GENERICATTRIBUTE16", "GENERICATTRIBUTE17", "GENERICATTRIBUTE18", "GENERICATTRIBUTE19", "GENERICATTRIBUTE20", "GENERICDATE1", "GENERICDATE2", "GENERICDATE3", "GENERICDATE4", "GENERICDATE5", "GENERICDATE6", "GENERICDATE7", "GENERICDATE8", "GENERICDATE9", "GENERICDATE10", "GENERICDATE11", "GENERICDATE12", "GENERICDATE13", "GENERICDATE14", "GENERICDATE15", "GENERICDATE16", "GENERICDATE17", "GENERICDATE18", "GENERICDATE19", "GENERICDATE20", "GENERICBOOLEAN1", "GENERICBOOLEAN2", "GENERICBOOLEAN3", "GENERICBOOLEAN4", "GENERICBOOLEAN5", "GENERICBOOLEAN6", "GENERICBOOLEAN7", "GENERICBOOLEAN8", "GENERICBOOLEAN9", "GENERICBOOLEAN10", "GENERICBOOLEAN11", "GENERICBOOLEAN12", "GENERICBOOLEAN13", "GENERICBOOLEAN14", "GENERICBOOLEAN15", "GENERICBOOLEAN16", "GENERICBOOLEAN17", "GENERICBOOLEAN18", "GENERICBOOLEAN19", "GENERICBOOLEAN20", "GENERICNUMBER1", "UNITTYPEFORGENERICNUMBER1", "GENERICNUMBER2", "UNITTYPEFORGENERICNUMBER2", "GENERICNUMBER3", "UNITTYPEFORGENERICNUMBER3", "GENERICNUMBER4", "UNITTYPEFORGENERICNUMBER4", "GENERICNUMBER5", "UNITTYPEFORGENERICNUMBER5", "GENERICNUMBER6", "UNITTYPEFORGENERICNUMBER6", "GENERICNUMBER7", "UNITTYPEFORGENERICNUMBER7", "GENERICNUMBER8", "UNITTYPEFORGENERICNUMBER8", "GENERICNUMBER9", "UNITTYPEFORGENERICNUMBER9", "GENERICNUMBER10", "UNITTYPEFORGENERICNUMBER10", "GENERICNUMBER11", "UNITTYPEFORGENERICNUMBER11", "GENERICNUMBER12", "UNITTYPEFORGENERICNUMBER12", "GENERICNUMBER13", "UNITTYPEFORGENERICNUMBER13", "GENERICNUMBER14", "UNITTYPEFORGENERICNUMBER14", "GENERICNUMBER15", "UNITTYPEFORGENERICNUMBER15", "GENERICNUMBER16", "UNITTYPEFORGENERICNUMBER16", "GENERICNUMBER17", "UNITTYPEFORGENERICNUMBER17", "GENERICNUMBER18", "UNITTYPEFORGENERICNUMBER18", "GENERICNUMBER19", "UNITTYPEFORGENERICNUMBER19", "GENERICNUMBER20", "UNITTYPEFORGENERICNUMBER20" ) AS (select
    
    so.orderid,
    st.linenumber,
    st.sublinenumber,
    et.eventtypeid,
    ga.pagenumber,
    
    ga.genericattribute1,
    ga.genericattribute2,
    ga.genericattribute3,
    ga.genericattribute4,
    ga.genericattribute5,
    ga.genericattribute6,
    ga.genericattribute7,
    ga.genericattribute8,
    ga.genericattribute9,
    ga.genericattribute10,
    ga.genericattribute11,
    ga.genericattribute12,
    ga.genericattribute13,
    ga.genericattribute14,
    ga.genericattribute15,
    ga.genericattribute16,
    ga.genericattribute17,
    ga.genericattribute18,
    ga.genericattribute19,
    ga.genericattribute20,
    ga.genericdate1,
    ga.genericdate2,
    ga.genericdate3,
    ga.genericdate4,
    ga.genericdate5,
    ga.genericdate6,
    ga.genericdate7,
    ga.genericdate8,
    ga.genericdate9,
    ga.genericdate10,
    ga.genericdate11,
    ga.genericdate12,
    ga.genericdate13,
    ga.genericdate14,
    ga.genericdate15,
    ga.genericdate16,
    ga.genericdate17,
    ga.genericdate18,
    ga.genericdate19,
    ga.genericdate20,
    ga.genericboolean1,
    ga.genericboolean2,
    ga.genericboolean3,
    ga.genericboolean4,
    ga.genericboolean5,
    ga.genericboolean6,
    ga.genericboolean7,
    ga.genericboolean8,
    ga.genericboolean9,
    ga.genericboolean10,
    ga.genericboolean11,
    ga.genericboolean12,
    ga.genericboolean13,
    ga.genericboolean14,
    ga.genericboolean15,
    ga.genericboolean16,
    ga.genericboolean17,
    ga.genericboolean18,
    ga.genericboolean19,
    ga.genericboolean20,
    ga.genericnumber1,
    ut1.name unittypeforgenericnumber1,
    ga.genericnumber2,
    ut2.name unittypeforgenericnumber2,
    ga.genericnumber3,
    ut3.name unittypeforgenericnumber3,
    ga.genericnumber4,
    ut4.name unittypeforgenericnumber4,
    ga.genericnumber5,
    ut5.name unittypeforgenericnumber5,
    ga.genericnumber6,
    ut6.name unittypeforgenericnumber6,
    ga.genericnumber7,
    ut7.name unittypeforgenericnumber7,
    ga.genericnumber8,
    ut8.name unittypeforgenericnumber8,
    ga.genericnumber9,
    ut9.name unittypeforgenericnumber9,
    ga.genericnumber10,
    ut10.name unittypeforgenericnumber10,
    ga.genericnumber11,
    ut11.name unittypeforgenericnumber11,
    ga.genericnumber12,
    ut12.name unittypeforgenericnumber12,
    ga.genericnumber13,
    ut13.name unittypeforgenericnumber13,
    ga.genericnumber14,
    ut14.name unittypeforgenericnumber14,
    ga.genericnumber15,
    ut15.name unittypeforgenericnumber15,
    ga.genericnumber16,
    ut16.name unittypeforgenericnumber16,
    ga.genericnumber17,
    ut17.name unittypeforgenericnumber17,
    ga.genericnumber18,
    ut18.name unittypeforgenericnumber18,
    ga.genericnumber19,
    ut19.name unittypeforgenericnumber19,
    ga.genericnumber20,
    ut20.name unittypeforgenericnumber20
FROM
   	 cs_Salestransaction st
    JOIN cs_gasalestransaction ga on
    ga.processingunitseq=st.processingunitseq and ga.tenantid=st.tenantid
    and ga.salestransactionseq=st.salestransactionseq and ga.compensationdate=st.compensationdate
	join CS_Salesorder so on so.salesorderseq=st.salesorderseq and so.removedate=to_Date('22000101','YYYYMMDD')
    and so.processingunitseq=st.processingunitseq and so.tenantid=st.tenantid
	join cs_eventtype et on et.datatypeseq=st.eventtypeseq and et.removedate=to_Date('22000101','YYYYMMDD')
    and et.tenantid=st.tenantid
    left join cs_unittype ut1 on ga.unittypeforgenericnumber1=ut1.unittypeseq and ut1.removedate=to_Date('22000101','YYYYMMDD') 
        and ut1.tenantid=st.tenantid
	left join cs_unittype ut2 on ga.unittypeforgenericnumber2=ut2.unittypeseq and ut2.removedate=to_Date('22000101','YYYYMMDD') 
        and ut2.tenantid=st.tenantid
	left join cs_unittype ut3 on ga.unittypeforgenericnumber3=ut3.unittypeseq and ut3.removedate=to_Date('22000101','YYYYMMDD') 
        and ut3.tenantid=st.tenantid
	left join cs_unittype ut4 on ga.unittypeforgenericnumber4=ut4.unittypeseq and ut4.removedate=to_Date('22000101','YYYYMMDD') 
        and ut4.tenantid=st.tenantid
	left join cs_unittype ut5 on ga.unittypeforgenericnumber5=ut5.unittypeseq and ut5.removedate=to_Date('22000101','YYYYMMDD') 
        and ut5.tenantid=st.tenantid
	left join cs_unittype ut6 on ga.unittypeforgenericnumber6=ut6.unittypeseq and ut6.removedate=to_Date('22000101','YYYYMMDD') 
        and ut6.tenantid=st.tenantid
        
    left join cs_unittype ut7 on ga.unittypeforgenericnumber7=ut7.unittypeseq and ut7.removedate=to_Date('22000101','YYYYMMDD') 
        and ut7.tenantid=st.tenantid
	left join cs_unittype ut8 on ga.unittypeforgenericnumber8=ut8.unittypeseq and ut8.removedate=to_Date('22000101','YYYYMMDD') 
        and ut8.tenantid=st.tenantid
	left join cs_unittype ut9 on ga.unittypeforgenericnumber9=ut9.unittypeseq and ut9.removedate=to_Date('22000101','YYYYMMDD') 
        and ut9.tenantid=st.tenantid
	left join cs_unittype ut10 on ga.unittypeforgenericnumber10=ut10.unittypeseq and ut10.removedate=to_Date('22000101','YYYYMMDD') 
        and ut10.tenantid=st.tenantid
	left join cs_unittype ut11 on ga.unittypeforgenericnumber11=ut11.unittypeseq and ut11.removedate=to_Date('22000101','YYYYMMDD') 
        and ut11.tenantid=st.tenantid
	left join cs_unittype ut12 on ga.unittypeforgenericnumber12=ut12.unittypeseq and ut12.removedate=to_Date('22000101','YYYYMMDD') 
        and ut12.tenantid=st.tenantid
    left join cs_unittype ut13 on ga.unittypeforgenericnumber13=ut13.unittypeseq and ut13.removedate=to_Date('22000101','YYYYMMDD') 
        and ut13.tenantid=st.tenantid
	left join cs_unittype ut14 on ga.unittypeforgenericnumber14=ut2.unittypeseq and ut14.removedate=to_Date('22000101','YYYYMMDD') 
        and ut14.tenantid=st.tenantid
	left join cs_unittype ut15 on ga.unittypeforgenericnumber15=ut15.unittypeseq and ut15.removedate=to_Date('22000101','YYYYMMDD') 
        and ut15.tenantid=st.tenantid
	left join cs_unittype ut16 on ga.unittypeforgenericnumber16=ut16.unittypeseq and ut16.removedate=to_Date('22000101','YYYYMMDD') 
        and ut16.tenantid=st.tenantid
	left join cs_unittype ut17 on ga.unittypeforgenericnumber17=ut17.unittypeseq and ut17.removedate=to_Date('22000101','YYYYMMDD') 
        and ut17.tenantid=st.tenantid
	left join cs_unittype ut18 on ga.unittypeforgenericnumber18=ut18.unittypeseq and ut18.removedate=to_Date('22000101','YYYYMMDD') 
        and ut18.tenantid=st.tenantid
        
	left join cs_unittype ut19 on ga.unittypeforgenericnumber19=ut19.unittypeseq and ut19.removedate=to_Date('22000101','YYYYMMDD') 
        and ut19.tenantid=st.tenantid
	left join cs_unittype ut20 on ga.unittypeforgenericnumber20=ut20.unittypeseq and ut20.removedate=to_Date('22000101','YYYYMMDD') 
        and ut20.tenantid=st.tenantid
    where st.tenantid='STEL' and st.processingunitseq  = (Select processingunitseq from cs_processingunit where name='Singtel_PU')) WITH READ ONLY