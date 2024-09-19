
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
      def validationflag=map.get("ENABLE_VALIDATION");


def service = new Factory(DataStoreService.class).getService()
def result =""
	//Check if valid service instance was retrieved
	if( service != null) {			
		//Read data store entry via id
		def dsEntry = service.get("Commission","Position")
		if (dsEntry != null){
		 result = new String(dsEntry.getDataAsArray())}
	}


      def date = new Date();
      def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
      
input='''
<Positions>
</Positions>
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
def eventversion= valueMapApi.getMappedValue("SuccessFactors", "PerPosition", "EventVersion", "Commission", "Position");    
def mandatorycols= valueMapApi.getMappedValue("SuccessFactors", "PerPosition", "MandatoryFields", "Commission", "Position");  

    
 SFResponse.'**'.findAll{it.name() =='Position'}.each
 {perPosition ->
 
def invalid=0
def missingkeys=[];

if (validationflag == 'True')
{
def mandatoryfields =mandatorycols.split(',').collect{it as String}
for (String field : mandatoryfields )
      {
          if (perPosition."${field}".text() =='')   
          {
            missingkeys.add(field);
          }
      }
      
      if ( !missingkeys.empty )
      {
          def mk = "Missing Key Columns "+missingkeys
          perPosition.appendNode("Invalid", [:], mk);
          def positionxml= Parser.parseText(groovy.xml.XmlUtil.serialize(perPosition)) 
          invaliddata.append(positionxml)
          invalid=1
      }    
}

   if (invalid != 1 && CommResponse != null)
   {
       def newhires=0;
           
 CommResponse.'**'.findAll{it.name() =='Position'}.each
 {perCommPosition ->
 
     
     
     SFPayee=perPosition.payee.payeeId.text()
     SFPosition=perPosition.name.text()
     Position=perCommPosition.name.text()
     Payee=perCommPosition.payee.payeeId.text()
     
     if ( SFPosition.toUpperCase() == Position.toUpperCase() && SFPayee.toUpperCase() == Payee.toUpperCase()) 
     {
       def Positionseq=perCommPosition.ruleElementOwnerSeq.text()
        def esd=perCommPosition.effectiveStartDate.text()
        def eed=perCommPosition.effectiveEndDate.text()
        newhires=1;
     perPosition.appendNode("ruleElementOwnerSeq", [:], Positionseq);
     perPosition.effectiveStartDate.replaceNode{effectiveStartDate(esd)}
     perPosition.creditStartDate.replaceNode{creditStartDate(esd)}
     perPosition.processingStartDate.replaceNode{processingStartDate(esd)}
     perPosition.effectiveEndDate.replaceNode{effectiveEndDate(eed)}
     perPosition.creditEndDate.replaceNode{creditEndDate(eed)}
     perPosition.processingEndDate.replaceNode{processingEndDate(eed)}
    
     
    def modified=0
    def str=[]
    perPosition.children().each{ node ->
         def SFfield=node.name()
         def SFval=node.text()
         def CMval=perCommPosition."${SFfield}".text()
       if (CMval.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}-\\d{2}:\\d{2}"))
        {
           Date Cdate = Date.parse("yyyy-MM-dd'T'HH:mm:ss.SSSX",CMval);
           Cdate.clearTime();
           CMval=Cdate.format("yyyy-MM-dd'T'HH:mm:ss.SSS")
        }
       if (SFval.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}-\\d{2}:\\d{2}"))
        {
           Date Cdate = Date.parse("yyyy-MM-dd'T'HH:mm:ss.SSSX",SFval);
           Cdate.clearTime();
           SFval=Cdate.format("yyyy-MM-dd'T'HH:mm:ss.SSS")
        }
        
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
             perPosition.effectiveStartDate.replaceNode{effectiveStartDate(df.format(date))}
             perPosition.creditStartDate.replaceNode{creditStartDate(df.format(date))}
             perPosition.processingStartDate.replaceNode{processingStartDate(df.format(date))}
             
            def positionxml=  Parser.parseText(groovy.xml.XmlUtil.serialize(perPosition)) 
             versiondata.append(positionxml)
             def Commnode=  Parser.parseText(groovy.xml.XmlUtil.serialize(perCommPosition))
             versionupdatedata.append(Commnode);
             
         }
         else
         {
            def newNode = Parser.parseText(groovy.xml.XmlUtil.serialize(perPosition)) 
             changedata.append(newNode);
         }
     }
     else
     {
        perPosition.replaceNode {}   
     }

}
}
if ( newhires == 0 && perPosition.effectiveEndDate.text().contains("2200-01-01") )
{
    def positionxml= Parser.parseText(groovy.xml.XmlUtil.serialize(perPosition)) 
    newdata.append(positionxml);
}
}
 } 
 
versionupd=Parser.parseText(groovy.xml.XmlUtil.serialize(versionupdatedata))
 
  versionupd.'**'.findAll{it.name() =='Position'}.each
 {perCommPosition ->
  perCommPosition.effectiveEndDate.replaceNode{effectiveEndDate(df.format(date.plus(-1)))}
 }
 
 
message.setProperty("Invalid",groovy.xml.XmlUtil.serialize(invaliddata));
message.setProperty("Changed",groovy.xml.XmlUtil.serialize(changedata));
message.setProperty("Versioned",groovy.xml.XmlUtil.serialize(versiondata));
message.setProperty("VersionUpdate",groovy.xml.XmlUtil.serialize(versionupd));
message.setProperty("NewHires",groovy.xml.XmlUtil.serialize(newdata));

       message.setBody(groovy.xml.XmlUtil.serialize(SFResponse));
       return message;
}