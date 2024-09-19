import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/* This script formats the time to be used in building the query. */

def Message processData(Message message) {
	
	prop = message.getProperties();
	head = message.getHeaders();

	String modDate="";
	Date today = new Date();
	DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
	String todayInString = dateFormat.format(today);
	
	if(!(prop.get("EXTRACT_START_DATE").trim().isEmpty()))
	{
		modDate = prop.get("EXTRACT_START_DATE");
		message.setProperty("LastExecutionReference_Type","External_Parameter");
		message.setProperty("LastExecutionPersisted_Value",modDate);
	}
	else if(!(head.get("LastModifiedDate").trim().isEmpty()))
	{
		modDate = head.get("LastModifiedDate");
		message.setProperty("LastExecutionReference_Type","Persisted_Value");
		message.setProperty("LastExecutionPersisted_Value",modDate);

	}
	else
	{
		modDate=todayInString;
		message.setProperty("LastExecutionReference_Type","First_Run");
		message.setProperty("LastExecutionPersisted_Value",modDate);
	}
	
	if (modDate.size()>5)
   		modDate=modDate.substring(0,modDate.size()-5)+"Z";
   	
	message.setProperty("TEMP_LAST_MODIFIED_DATE", modDate);

	return message;
}