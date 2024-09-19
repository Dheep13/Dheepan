
import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
def Message processData(Message message) {
    //Body 
def body = message.getBody(java.lang.String) as String;

def map = message.getProperties();

def Xml1 = new XmlSlurper().parseText(body);
Xml1.manager.replaceNode {}

message.setBody(groovy.xml.XmlUtil.serialize(Xml1));

       return message;
}