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
	
	def Position= pMap.get("position_ext");
	def LastModifiedDate = pMap.get("TEMP_LAST_MODIFIED_DATE").trim();

	def valueMapApi = ITApiFactory.getApi(ValueMappingApi.class, null);
    def Sf_select_field = valueMapApi.getMappedValue("SuccessFactors", "PerPosition", "SFFields", "Commission", "Position");
    def Sf_entities = valueMapApi.getMappedValue("SuccessFactors", "PerPosition", "SFEntities", "Commission", "Position");
    def sfkey = valueMapApi.getMappedValue("SuccessFactors", "PerPosition", "SFKey", "Commission", "Position");
   
   def EOT=pMap.get("EndOfTime");
    
	StringBuffer str = new StringBuffer();
	StringBuffer query= new StringBuffer();
	StringBuffer commstr = new StringBuffer();
	DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	Date date = new Date();
	
    message.setProperty("SelectStr",Sf_select_field.toString());
    message.setProperty("Entity",Sf_entities.toString());
    
	
	commstr.append ("(effectiveEndDate eq " +EOT+ ")");
	
		def entity=Sf_entities?.split(',').collect{it as String};
		
	if(!(LastModifiedDate.trim().isEmpty()))
	{
	    str.append("(lastModifiedDateTime ge datetimeoffset'" +LastModifiedDate+ "' ");
	    for (et in entity)
	    {
	   if (et != "personEmpTerminationInfoNav")
	   {
		str.append("or "+et+"/lastModifiedDateTime ge datetimeoffset'"+LastModifiedDate+ "' ");
	   }
	    }
	    str.append(")");

	}
	if(!(Position.trim().isEmpty()))
	{			
		if(Position.contains("!"))
		{
		    if (Position.contains(","))
		    {
		        Position= Position.toString().replace("!",""); 
		        	def pid=Position.split(',').collect{it as String};
    		str.append(" and (");
    		commstr.append (" and (");
    		int x = 1
    		for (payee in pid)
    		{
    		if ( x == pid.size()) 
    		{
    		    str.append("  "+sfkey+" ne '" + payee + "')");
    		    commstr.append (" name ne '" + payee + "')");
    		}
    		else 
    		{
    		    str.append("  "+sfkey+" ne '" + payee + "' and");
    		    commstr.append ("  name ne '" + payee + "' and");
    		}
    		x++;
    		}
    		
		    }
		    else {
		   Position= Position.toString().replace("!","");
		   str.append(" and  "+sfkey+" ne '" + Position+ "'");
		   commstr.append (" and  name ne '" + Position+ "'");
		    }
		}
		else if (Position.contains(","))
		{
    		def pid=Position.split(',').collect{it as String};
    		str.append(" and (");
    		commstr.append (" and (");
    		int x = 1
    		for (payee in pid)
    		{
    		if ( x == pid.size()) 
    		{
    		    str.append(" "+sfkey+" eq '" + payee + "')");
    		    commstr.append (" name eq '" + payee + "')");
    		}
    		else 
    		{
    		    str.append(" "+sfkey+" eq '" + payee + "' or");
    		    commstr.append (" name eq '" + payee + "' or");
    		}
    		x++;
    		}	    
		}
		else
		{
    	str.append(" and "+sfkey+" eq '" + Position+ "'");
    	commstr.append (" and name eq '" + Position+ "'");
		}

	}

	message.setProperty("QueryFilter",str.toString());
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
	
	if (str != "" )
	{
	    query.append("\$filter=");
	    query.append(str.toString());
	}
	
	message.setProperty("Query",query.toString());
	
	
	return message;
}