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
import groovy.util.XmlSlurper.*;
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.*
import java.util.List.*;
import com.sap.it.api.ITApiFactory;
import com.sap.it.api.mapping.ValueMappingApi;
import java.text.SimpleDateFormat;

def Message processData(Message message) {
    //Body 
        def body = message.getBody(java.lang.String) as String;
       
      def map = message.getProperties();
      def CommPayees = map.get("CMPayees");
      def CMResp = map.get("CMResponse");
      def validationflag=map.get("ENABLE_VALIDATION");
 //def Payees= CommPayees.split(',').collect{it as String}
 
 //def getPayees = { xml -> xml.'**'.findAll{it.name() == 'payeeId'} };

def service = new Factory(DataStoreService.class).getService()
def result =""
	//Check if valid service instance was retrieved
	if( service != null) {			
		//Read data store entry via id
		def dsEntry = service.get("Commission","Participant")
		if (dsEntry != null){
		 result = new String(dsEntry.getDataAsArray())}
	}


      def date = new Date();
      def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
      
input='''
<Participants>
</Participants>
'''

def Parser = new XmlParser();
def SFResponse = Parser.parseText(body) ;
def CommResponse = Parser.parseText(result) ;
def invaliddata = Parser.parseText(input);
def changedata = Parser.parseText(input);
def versiondata = Parser.parseText(input);
def versionupdatedata = Parser.parseText(input);
def newdata = Parser.parseText(input);
def valueMapApi = ITApiFactory.getApi(ValueMappingApi.class, null);
def eventversion= valueMapApi.getMappedValue("SuccessFactors", "PerPerson", "EventVersion", "Commission", "Participant");    
def mandatorycols= valueMapApi.getMappedValue("SuccessFactors", "PerPerson", "MandatoryFields", "Commission", "Participant");  

    
 SFResponse.'**'.findAll{it.name() =='Participant'}.each
 {perParticipant ->
 
def invalid=0
def missingkeys=[];

if (validationflag == 'True')
{
def mandatoryfields =mandatorycols.split(',').collect{it as String}
for (String field : mandatoryfields )
      {
          if (perParticipant."${field}".text() =='')   
          {
            missingkeys.add(field);
          }
      }
      
      if ( !missingkeys.empty )
      {
          def mk = "Missing Key Columns "+missingkeys
          perParticipant.appendNode("Invalid", [:], mk);
          def participantxml= Parser.parseText(groovy.xml.XmlUtil.serialize(perParticipant)) 
          invaliddata.append(participantxml)
          invalid=1
      }    
}

   if (invalid != 1 && CommResponse != null)
   {
       def newhires=0;
           
 CommResponse.'**'.findAll{it.name() =='Participant'}.each
 {perCommParticipant ->
 
     
     Payee=perCommParticipant.payeeId.text()
     SFPayee=perParticipant.payeeId.text()
     if ( SFPayee.toUpperCase() == Payee.toUpperCase()) 
     {
        def payeeseq=perCommParticipant.payeeSeq.text()
        def esd=perCommParticipant.effectiveStartDate.text()
        def eed=perCommParticipant.effectiveEndDate.text()
     //perParticipant.appendNode{payeeSeq(payeeseq)}
     newhires=1;      
     perParticipant.appendNode("payeeSeq", [:], payeeseq);
     perParticipant.effectiveStartDate.replaceNode{effectiveStartDate(esd)}
     perParticipant.effectiveEndDate.replaceNode{effectiveEndDate(eed)}
    
     
    def modified=0
    def str=[]
    perParticipant.children().each{ node ->
         def SFfield=node.name()
         def SFval=node.text()
         def CMval=perCommParticipant."${SFfield}".text()
         if ( CMval.toUpperCase() != SFval.toUpperCase() )
            {
            str.add(node.name());
            modified=1;
            }
            }
            
    if (modified ==1 )
    {
    
         def event =eventversion.split(',').collect{it as String}
         if(event != null && str != null && str.intersect(event))
         {
             perParticipant.effectiveStartDate.replaceNode{effectiveStartDate(df.format(date))}
             
            def participantxml=  Parser.parseText(groovy.xml.XmlUtil.serialize(perParticipant)) 
             versiondata.append(participantxml)
             //perCommParticipant.effectiveEndDate=df.format(date.plus(-1))
             def Commnode=  Parser.parseText(groovy.xml.XmlUtil.serialize(perCommParticipant)) 
             versionupdatedata.append(Commnode);
             
         }
         else
         {
            def newNode = Parser.parseText(groovy.xml.XmlUtil.serialize(perParticipant)) 
             changedata.append(newNode);
           //  perParticipant.replaceNode {}  
         }
     }
     else
     {
        perParticipant.replaceNode {}   
     }

}
}
if ( newhires == 0 && perParticipant.effectiveEndDate.text().contains("2200-01-01") )
{
    def participantxml= Parser.parseText(groovy.xml.XmlUtil.serialize(perParticipant)) 
    newdata.append(participantxml);
}
}
 } 
 
versionupd=Parser.parseText(groovy.xml.XmlUtil.serialize(versionupdatedata))
 
  versionupd.'**'.findAll{it.name() =='Participant'}.each
 {perCommParticipant ->
  perCommParticipant.effectiveEndDate.replaceNode{effectiveEndDate(df.format(date.plus(-1)))}
 }
 
 
message.setProperty("Invalid",groovy.xml.XmlUtil.serialize(invaliddata));
message.setProperty("Changed",groovy.xml.XmlUtil.serialize(changedata));
message.setProperty("Versioned",groovy.xml.XmlUtil.serialize(versiondata));
message.setProperty("VersionUpdate",groovy.xml.XmlUtil.serialize(versionupd));
message.setProperty("NewHires",groovy.xml.XmlUtil.serialize(newdata));

       message.setBody(groovy.xml.XmlUtil.serialize(SFResponse));
       return message;
}