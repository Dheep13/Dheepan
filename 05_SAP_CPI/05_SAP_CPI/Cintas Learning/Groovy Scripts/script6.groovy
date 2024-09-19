/*
 The integration developer needs to create the method processData 
 This method takes Message object of package com.sap.gateway.ip.core.customdev.util 
which includes helper methods useful for the content developer:
The methods available are:
    public java.lang.Object getBody()
	public void setBody(java.lang.Object exchangeBody)
    public java.util.Map<java.lang.String,java.lang.Object> getHeaders()
    public void setHeaders(java.util.Map<java.lang.String,java.lang.Object> exchangeHeaders)
    public void setHeader(java.lang.String name, java.lang.Object value)
    public java.util.Map<java.lang.String,java.lang.Object> getProperties()
    public void setProperties(java.util.Map<java.lang.String,java.lang.Object> exchangeProperties) 
    public void setProperty(java.lang.String name, java.lang.Object value)
    public java.util.List<com.sap.gateway.ip.core.customdev.util.SoapHeader> getSoapHeaders()
    public void setSoapHeaders(java.util.List<com.sap.gateway.ip.core.customdev.util.SoapHeader> soapHeaders) 
       public void clearSoapHeaders()
 */
import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
//import groovy.xml.XmlSlurper;
import groovy.xml.*;
import groovy.util.slurpersupport.GPathResult;
import java.util.List.*;
import com.sap.it.api.ITApiFactory;
import com.sap.it.api.mapping.ValueMappingApi;

def path(GPathResult node) {
    def result = [node.name()]
    def pathWalker = [hasNext: { -> node.parent() != node }, next: { -> node = node.parent() }] as Iterator
    (result + pathWalker.collect { it.name() }).reverse().join('/')
}


def Message processData(Message message) {
    //Body 
       def body = message.getBody(java.lang.String);
       def xmlparsedstr = new XmlSlurper().parseText(body);
       def xmlstr=[]
      StringBuffer targetxml = new StringBuffer();
      
def valueMapApi = ITApiFactory.getApi(ValueMappingApi.class, null);

xmlstr.add("<Participants>")
xmlparsedstr.PerPerson.each { PerPerson ->
 xmlstr.add("<Participant>")
 PerPerson.depthFirst().findAll { it.children().size() == 0 }
 .each { node ->
  xmlstr.add("${path(node)}=${node.text()}")
 }
 xmlstr.add("</Participant>")
}
xmlstr.add("</Participants>")

for (String tag : xmlstr) {
    if (tag.matches("(.*)Participant(.*)"))
    {
   targetxml.append(tag);
    }
    else
    {
    
    List xmltags = tag.split("="); 
    def xmlelement = xmltags[0];
    def xmlvalue = xmltags[1] ?: '' ;
    if (xmlvalue.matches("9999-12-31(.*)"))
    {
    xmlvalue.replace("9999-12-31", "2200-01-01")
    }
    def mappedtag= valueMapApi.getMappedValue("SuccessFactors", "PerPerson", xmlelement, "Commission", "Participant");    
    if(mappedtag != null)
    {
        if (mappedtag == "effectiveEndDate" && xmlvalue == "")
        {
            xmlvalue = "2200-01-01T00:00:00.000";
        }
     if (mappedtag.matches("(.*),(.*)"))
     {
      def mappedfields = mappedtag.split(',').collect{it as String} 
      for (String field : mappedfields )
      {
   targetxml.append("<"+field+">");
   targetxml.append(xmlvalue);
   targetxml.append("</"+field+">");  
      }
     }
     else if (mappedtag.matches("(.*)\\.(.*)"))
     {
        List mapobject = mappedtag.split("\\.");
        def mapparentobj=mapobject[0];
        def mapchildobj=mapobject[1];
         targetxml.append("<"+mapparentobj+">");
         targetxml.append("<"+mapchildobj+">");
         targetxml.append(xmlvalue);
         targetxml.append("</"+mapchildobj+">"); 
         targetxml.append("</"+mapparentobj+">");  
     }
     else
     {
   targetxml.append("<"+mappedtag+">");
   targetxml.append(xmlvalue);
   targetxml.append("</"+mappedtag+">");
     }
    }
    }
}



//InputStream stream = new ByteArrayInputStream(str.getBytes("UTF-8"));
    message.setBody(targetxml);
       return message;
}