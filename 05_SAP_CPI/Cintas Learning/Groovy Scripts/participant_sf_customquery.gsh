import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.sap.it.api.ITApiFactory;
import com.sap.it.api.mapping.ValueMappingApi;

/* This scripts builds the query to get the Employee data from SuccessFactors . */

def Message processData(Message message) {
	 
	def body = message.getBody();
	def pMap = message.getProperties();
	
	def orgobject = pMap.get("ORGANIZATION_OBJECT");
	def runMode = pMap.get("RUN_MODE");
	def LastModifiedDate = pMap.get("TEMP_LAST_MODIFIED_DATE").trim();
	def targetagency="Commission"+orgobject
	
	def PayeeID = pMap.get("PAYEEID_EXTERNAL");
	

//Retrieve values from Participant Configuration Controller Value Mapping	
	def valueMapApi = ITApiFactory.getApi(ValueMappingApi.class, null);
    def Sf_select_field = valueMapApi.getMappedValue("SuccessFactors", "Configuration", "SFFields", targetagency, "Configuration");
    def Sf_entities = valueMapApi.getMappedValue("SuccessFactors", "Configuration", "SFEntities", targetagency, "Configuration");
    def sfkey = valueMapApi.getMappedValue("SuccessFactors", "Configuration", "SFKey", targetagency, "Configuration");
    def sf_filter = valueMapApi.getMappedValue("SuccessFactors", "Configuration", "SFFilter", targetagency , "Configuration");
   
   def EOT=pMap.get("EndOfTime");
    
	StringBuffer filter = new StringBuffer();
	StringBuffer query= new StringBuffer();
	StringBuffer commstr = new StringBuffer();
	DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	Date date = new Date();
	
	commstr.append ("(effectiveEndDate eq " +EOT+ ")");
	
	def entity=Sf_entities.split(',').collect{it as String};
		
	if(!(LastModifiedDate.trim().isEmpty()) && runMode.toUpperCase() != "FULL" && (PayeeID.trim().isEmpty()) )
	{
	    filter.append("(lastModifiedDateTime ge datetimeoffset'" +LastModifiedDate+ "' ");
	    for (et in entity)
	    {
	   if (et != "personEmpTerminationInfoNav")
	   {
		filter.append("or "+et+"/lastModifiedDateTime ge datetimeoffset'"+LastModifiedDate+ "' ");
	   }
	    }
	    filter.append(")");

	}
	if(!(PayeeID.trim().isEmpty()) && runMode.toUpperCase() != "FULL")
	{	
	    if (filter.length() != 0 )
        {
           filter.append(" and "); 
        }
        
		if(PayeeID.contains("!"))
		{
		    if (PayeeID.contains(","))
		    {
		        PayeeID = PayeeID.toString().replace("!",""); 
		        	def pid=PayeeID.split(',').collect{it as String};
    		filter.append(" (");
    		commstr.append (" and (");
    		int x = 1
    		for (payee in pid)
    		{
    		if ( x == pid.size()) 
    		{
    		    filter.append("  "+sfkey+" ne '" + payee + "')");
    		    commstr.append (" payeeId ne '" + payee + "')");
    		}
    		else 
    		{
    		    filter.append("  "+sfkey+" ne '" + payee + "' and");
    		    commstr.append ("  payeeId ne '" + payee + "' and");
    		}
    		x++;
    		}
    		
		    }
		    else {
		   PayeeID = PayeeID.toString().replace("!","");
		   filter.append( sfkey+" ne '" + PayeeID + "'");
		   commstr.append (" and  payeeId ne '" + PayeeID + "'");
		    }
		}
		else if (PayeeID.contains(","))
		{
    		def pid=PayeeID.split(',').collect{it as String};
    		filter.append(" (");
    		commstr.append (" and (");
    		int x = 1
    		for (payee in pid)
    		{
    		if ( x == pid.size()) 
    		{
    		    filter.append(" "+sfkey+" eq '" + payee + "')");
    		    commstr.append (" payeeId eq '" + payee + "')");
    		}
    		else 
    		{
    		    filter.append(" "+sfkey+" eq '" + payee + "' or");
    		    commstr.append (" payeeId eq '" + payee + "' or");
    		}
    		x++;
    		}
		}
		else
		{
    	filter.append(sfkey+" eq '" + PayeeID + "'");
    	commstr.append (" and payeeId eq '" + PayeeID + "'");
		}

	}
    if (sf_filter!= "" )
    {
        if (filter.length() != 0 )
        {
           filter.append(" and "); 
        }
        filter.append(sf_filter.toString());
    }

    message.setProperty("SelectStr",Sf_select_field.toString());
    message.setProperty("Entity",Sf_entities.toString());
	message.setProperty("QueryFilter",filter.toString());
	message.setProperty("CommFilter",commstr.toString());
	
	
	if (Sf_select_field != "" || Sf_select_field.toString().toUpperCase() != "ALL")
	{
	    query.append("\$select=");
	    query.append(Sf_select_field.toString());
	    query.append("&")
	}
	
	if (Sf_entities != "" )
	{
	    query.append("\$expand=");
	    query.append(Sf_entities.toString());
	    query.append("&")
	}
	
	if (filter!= "" )
	{
	    query.append("\$filter=");
	    query.append(filter.toString());
	}
	
	message.setProperty("SFQuery",query.toString());
	
	
	return message;
}