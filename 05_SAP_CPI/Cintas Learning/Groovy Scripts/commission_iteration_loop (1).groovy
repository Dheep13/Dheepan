
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

def Xml1 = new XmlSlurper().parseText(body);
Xml1.skip.replaceNode {}
Xml1.top.replaceNode {}
Xml1.prev.replaceNode {}
Xml1.next.replaceNode {}

loopindex = loopindex + 1;
def tempSkipVal = skipVal.toInteger() + topVal.toInteger();
skipVal = tempSkipVal.toString()

message.setProperty("TopVal",topVal);
message.setProperty("SkipVal", skipVal);
message.setProperty("CamelLoopIndex", loopindex);

message.setBody(groovy.xml.XmlUtil.serialize(Xml1));

       return message;
}