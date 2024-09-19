
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
import java.time.*;

def Message processData(Message message) {
    //Body 
    def body = message.getBody(java.lang.String) as String;
       
      def map = message.getProperties();
      def CommPayees = map.get("CMPayees");
      def CMResp = map.get("CMResponse");
      def validationflag=map.get("ENABLE_VALIDATION");
      def orgobject=map.get("ORGANIZATION_OBJECT");
      def targetagency="Commission"+orgobject
      def parenttag=orgobject+"s"
def service = new Factory(DataStoreService.class).getService()
def result =""
	//Check if valid service instance was retrieved
	if( service != null) {			
		//Read data store entry via id
		def dsEntry = service.get("Commission",orgobject)
		if (dsEntry != null){
		 result = new String(dsEntry.getDataAsArray())}
	}


      def date = new Date();
      def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
      
input='''
<'''+parenttag+'''>
</'''+parenttag+'''>
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
def eventversion= valueMapApi.getMappedValue("SuccessFactors", "Configuration", "Versioned", targetagency, "Configuration");    
def mandatorycols= valueMapApi.getMappedValue("SuccessFactors", "Configuration", "Mandatory", targetagency, "Configuration");  

    
 SFResponse.'**'.findAll{it.name() == orgobject}.each
 {perObject ->
 
def invalid=0
def missingkeys=[];

// check if Validation Flag has been enabled
if (validationflag == 'True')
{
def mandatoryfields =mandatorycols.split(',').collect{it as String}
for (String field : mandatoryfields )
      {
          if (perObject."${field}".text() =='')   
          {
            missingkeys.add(field);
          }
      }
      
      if ( !missingkeys.empty )
      {
          def mk = "Missing Key Columns "+missingkeys
          perObject.appendNode("Invalid", [:], mk);
          def objectxml= Parser.parseText(groovy.xml.XmlUtil.serialize(perObject)) 
          invaliddata.append(objectxml)
          invalid=1
      }    
}

if (invalid != 1 && CommResponse != null)
   {
       def newhires=0;
           
 CommResponse.'**'.findAll{it.name() =='Participant'}.each
 {perCommParticipant ->
 
     
     Payee=perCommParticipant.payeeId.text()
     SFPayee=perObject.payeeId.text()
     //Check if Payee Exists in Commission
     if ( SFPayee.toUpperCase() == Payee.toUpperCase()) 
     {
        def payeeseq=perCommParticipant.payeeSeq.text()
        def esd=perCommParticipant.effectiveStartDate.text()
        def eed=perCommParticipant.effectiveEndDate.text()
     newhires=1;      
     perObject.appendNode("payeeSeq", [:], payeeseq);
     perObject.effectiveStartDate.replaceNode{effectiveStartDate(esd)}
     perObject.effectiveEndDate.replaceNode{effectiveEndDate(eed)}
    
    
    def modified=0
    def str=[]
    perObject.children().each{ node ->
         def SFfield=node.name()
         def SFval=node.text()
         def CMval=perCommParticipant."${SFfield}".text()
         //Data Preparation: Format Date Field prior comparing data
       if (CMval.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}[+-]\\d{2}:\\d{2}"))
        {
           Date Cdate = Date.parse("yyyy-MM-dd'T'HH:mm:ss.SSSX",CMval);
           Cdate.clearTime();
           CMval=Cdate.format("yyyy-MM-dd'T'HH:mm:ss.SSS")
        }
       if (SFval.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}[+-]\\d{2}:\\d{2}"))
        {
           Date Cdate = Date.parse("yyyy-MM-dd'T'HH:mm:ss.SSSX",SFval);
           Cdate.clearTime();
           SFval=Cdate.format("yyyy-MM-dd'T'HH:mm:ss.SSS")
        }
        
        // Retain Manual Overriden fields in Commission
        if (SFval == "" && CMval != "")
        {
           perObject."${SFfield}".replaceNode{"${SFfield}"(CMval)} 
        }
        
         //Compare data elements of the Participant Record
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
             perObject.effectiveStartDate.replaceNode{effectiveStartDate(df.format(date))}
             
            def objectxml=  Parser.parseText(groovy.xml.XmlUtil.serialize(perObject)) 
             versiondata.append(objectxml)
             def Commnode=  Parser.parseText(groovy.xml.XmlUtil.serialize(perCommParticipant)) 
             versionupdatedata.append(Commnode);
             
         }
         else
         {
            def newNode = Parser.parseText(groovy.xml.XmlUtil.serialize(perObject)) 
             changedata.append(newNode);
         }
     }
     else
     {
        perObject.replaceNode {}   
     }

}
}
if ( newhires == 0 && perObject.effectiveEndDate.text().contains("2200-01-01") )
{
    def objectxml= Parser.parseText(groovy.xml.XmlUtil.serialize(perObject)) 
    newdata.append(objectxml);
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