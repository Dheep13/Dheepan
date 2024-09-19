
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
		def dsEntry = service.get("Commission","Title")
		if (dsEntry != null){
		 result = new String(dsEntry.getDataAsArray())}
	}


      def date = new Date();
      def df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
      
input='''
<Titles>
</Titles>
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
def eventversion= valueMapApi.getMappedValue("SuccessFactors", "PerTitle", "EventVersion", "Commission", "Title");    
def mandatorycols= valueMapApi.getMappedValue("SuccessFactors", "PerTitle", "MandatoryFields", "Commission", "Title");  

    
 SFResponse.'**'.findAll{it.name() =='Title'}.each
 {perTitle ->
 
def invalid=0
def missingkeys=[];

if (validationflag == 'True')
{
def mandatoryfields =mandatorycols.split(',').collect{it as String}
for (String field : mandatoryfields )
      {
          if (perTitle."${field}".text() =='')   
          {
            missingkeys.add(field);
          }
      }
      
      if ( !missingkeys.empty )
      {
          def mk = "Missing Key Columns "+missingkeys
          perTitle.appendNode("Invalid", [:], mk);
          def Titlexml= Parser.parseText(groovy.xml.XmlUtil.serialize(perTitle)) 
          invaliddata.append(Titlexml)
          invalid=1
      }    
}

   if (invalid != 1 && CommResponse != null)
   {
       def newtitles=0;
           
 CommResponse.'**'.findAll{it.name() =='Title'}.each
 {perCommTitle ->
 
     
     Title=perCommTitle.name.text()
     SFTitle=perTitle.name.text()
     if ( SFTitle.toUpperCase() == Title.toUpperCase()) 
     {
        def ruleElementOwnerSeq=perCommTitle.ruleElementOwnerSeq.text()
        def esd=perCommTitle.effectiveStartDate.text()
        def eed=perCommTitle.effectiveEndDate.text()
     
     newtitles=1;      
     perTitle.appendNode("ruleElementOwnerSeq", [:], ruleElementOwnerSeq);
     perTitle.effectiveStartDate.replaceNode{effectiveStartDate(esd)}
     perTitle.effectiveEndDate.replaceNode{effectiveEndDate(eed)}
    
     
    def modified=0
    def str=[]
    perTitle.children().each{ node ->
         def SFfield=node.name()
         def SFval=node.text()
         def CMval=perCommTitle."${SFfield}".text()
       if (CMval.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}-\\d{2}:\\d{2}"))
        {
           Date Cdate = Date.parse("yyyy-MM-dd'T'HH:mm:ss.SSSX",CMval);
           Cdate.clearTime();
           CMval=Cdate.format("yyyy-MM-dd'T'HH:mm:ss.SSS")
           println(CMval);
        }
       if (SFval.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}-\\d{2}:\\d{2}"))
        {
           Date Cdate = Date.parse("yyyy-MM-dd'T'HH:mm:ss.SSSX",SFval);
           Cdate.clearTime();
           SFval=Cdate.format("yyyy-MM-dd'T'HH:mm:ss.SSS")
           println(SFval);
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
             perTitle.effectiveStartDate.replaceNode{effectiveStartDate(df.format(date))}
             
            def titlexml=  Parser.parseText(groovy.xml.XmlUtil.serialize(perTitle)) 
             versiondata.append(titlexml)
             def Commnode=  Parser.parseText(groovy.xml.XmlUtil.serialize(perCommTitle)) 
             versionupdatedata.append(Commnode);
             
         }
         else
         {
            def newNode = Parser.parseText(groovy.xml.XmlUtil.serialize(perTitle)) 
             changedata.append(newNode);
           //  perTitle.replaceNode {}  
         }
     }
     else
     {
        perTitle.replaceNode {}   
     }

}
}
if ( newtitles == 0 && perTitle.effectiveEndDate.text().contains("2200-01-01") )
{
    def titlexml= Parser.parseText(groovy.xml.XmlUtil.serialize(perTitle)) 
    newdata.append(titlexml);
}
}
 } 
 
versionupd=Parser.parseText(groovy.xml.XmlUtil.serialize(versionupdatedata))
 
  versionupd.'**'.findAll{it.name() =='Title'}.each
 {perCommTitle ->
  perCommTitle.effectiveEndDate.replaceNode{effectiveEndDate(df.format(date.plus(-1)))}
 }
 
 
message.setProperty("Invalid",groovy.xml.XmlUtil.serialize(invaliddata));
message.setProperty("Changed",groovy.xml.XmlUtil.serialize(changedata));
message.setProperty("Versioned",groovy.xml.XmlUtil.serialize(versiondata));
message.setProperty("VersionUpdate",groovy.xml.XmlUtil.serialize(versionupd));
message.setProperty("Newtitles",groovy.xml.XmlUtil.serialize(newdata));

       message.setBody(groovy.xml.XmlUtil.serialize(SFResponse));
       return message;
}