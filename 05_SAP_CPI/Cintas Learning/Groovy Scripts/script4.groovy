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
import groovy.json.*;
def Message processData(Message message) {
    //Body 
   def body = message.getBody(java.lang.String) as String;

   def map = message.getProperties();


def topVal = map.get("TopVal") ;
def skipVal = map.get("SkipVal");
def totalCount = map.get("TotalRecordCount");
def loopindex = map.get("CamelLoopIndex");
//def response =map.get("CommResponse")

def Xml1 = new XmlSlurper().parseText(body);
Xml1.skip.replaceNode {}
Xml1.top.replaceNode {}
Xml1.prev.replaceNode {}
Xml1.next.replaceNode {}

/*
def Xml2Slurper = new XmlSlurper();
def Xml2;
def getItems = { xml -> xml.'**'.findAll{it.name() == 'participants'} }

/*
if (response)
{
 Xml2 = Xml2Slurper.parseText(response)
 getItems(Xml2)?.collect{ Xml1.participant.appendNode(it)}
 def xml2output=getItems?.collect{ Xml1.participant.appendNode(it)}
}*/

loopindex = loopindex + 1;
def tempSkipVal = skipVal.toInteger() + topVal.toInteger()
skipVal = tempSkipVal.toString()

message.setProperty("TopVal",topVal);
message.setProperty("SkipVal", skipVal);
message.setProperty("CamelLoopIndex", loopindex);
//message.setProperty("CommResponse",groovy.xml.XmlUtil.serialize(Xml1));




message.setBody(groovy.xml.XmlUtil.serialize(Xml1));

       return message;
}