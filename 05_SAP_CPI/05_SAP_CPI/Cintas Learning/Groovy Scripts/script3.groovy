import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
import groovy.json.JsonOutput

def Message processData(Message message) {
	
	def body = message.getBody(java.lang.String) as String;
	body = body.substring(body.indexOf(':'));
  
		def json_to_str=body.substring(1,body.indexOf(']',body.length()- 3)+1);  
	
	def json=JsonOutput.prettyPrint(json_to_str);
	     
	message.setBody(json);
	
	
	return message;
}