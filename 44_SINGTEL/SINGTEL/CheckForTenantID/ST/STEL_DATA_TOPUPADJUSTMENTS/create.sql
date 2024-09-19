CREATE VIEW "EXT"."STEL_DATA_TOPUPADJUSTMENTS" ( "COMPENSATIONDATE", "VAL", "NUMBEROFUNITS", "EVENTTYPEID", "CONTACT", "POSITIONNAME", "GENERICATTRIBUTE1", "GENERICATTRIBUTE2", "GENERICATTRIBUTE3", "GENERICATTRIBUTE4", "ORDERID", "ALTERNATEORDERNUMBER" ) AS (SELECT st.compensationdate,
          st.VALUE val,
          st.numberofunits,
          et.eventtypeid,
          tad.contact,
          ta.positionname,
          st.genericattribute1,
          st.genericattribute2,
          st.genericattribute3,
          st.genericattribute4,
          so.orderid,
          st.alternateordernumber
     FROM cs_Salestransaction st
          JOIN cs_Salesorder so
             ON so.removedate > CURRENT_DATE
                AND so.salesorderseq = st.salesorderseq
          JOIN cs_eventtype et
             ON st.eventtypeseq = et.datatypeseq AND et.removedate > CURRENT_DATE
          LEFT JOIN CS_TRANSACTIONADDRESS tad
             ON tad.salestransactionseq = st.salestransactionseq
                AND tad.transactionaddressseq = st.billtoaddressseq
          JOIN cs_transactionassignment ta
             ON ta.salestransactionseq = st.salestransactionseq
                AND ta.setnumber = 1
    WHERE UPPER (et.eventtypeid) LIKE '%TOP%UP%REVE%ADJUST%') WITH READ ONLY