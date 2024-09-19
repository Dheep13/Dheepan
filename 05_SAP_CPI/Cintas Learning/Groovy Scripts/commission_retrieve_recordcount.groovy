
import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
import groovy.json.*;
def Message processData(Message message) {
    //Body 
       def body = message.getBody(java.lang.String) as String;

def slurper = new JsonSlurper()
def result = slurper.parseText(body);


       message.setBody(null);
     
       message.setProperty("TotalRecordCount", result.total);
       def maxiter= (result.total/100)+1.toInteger();
       message.setProperty("MaxIterVal", maxiter);
       return message;
}