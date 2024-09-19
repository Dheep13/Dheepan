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
	
	def Title = pMap.get("title_ext");
	def LastModifiedDate = pMap.get("TEMP_LAST_MODIFIED_DATE").trim();
	
	def valueMapApi = ITApiFactory.getApi(ValueMappingApi.class, null);
    def Sf_select_field = valueMapApi.getMappedValue("SuccessFactors", "PerTitle", "SFFields", "Commission", "Title");
    def Sf_entities = valueMapApi.getMappedValue("SuccessFactors", "PerTitle", "SFEntities", "Commission", "Title");
	def sfkey = valueMapApi.getMappedValue("SuccessFactors", "PerTitle", "SFKey", "Commission", "Title");
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
	//	 str.append("(lastModifiedDateTime ge datetimeoffset'" +LastModifiedDate+ "' ");
	    for (et in entity)
	    {
	   if (et != "personEmpTerminationInfoNav")
	   {
		str.append("or "+et+"/lastModifiedDateTime ge datetimeoffset'"+LastModifiedDate+ "' ");
	   }
	    }
	 //   str.append(") and ");

	}
	if(!(Title?.trim().isEmpty()))
	{			
		if(Title.contains("!"))
		{
		  str.append(" and  "+sfkey+" ne '" + Title + "'");
		   commstr.append (" and  name ne '" + Title + "'");
		    											   
		}
		else if (Title.contains(","))
		{    		
    		def pid=Title?.split(',').collect{it as String};
    		str.append(" and (");
    		commstr.append (" and (");
    		int x = 1
    		for (tit in pid)
    		{
    		if ( x == tit.size()) 
    		{
			    str.append(" "+sfkey+" eq '" + tit + "')");    		    
    		    commstr.append (" name eq '" + tit + "')");
    		}
    		else 
    		{
    		    str.append(" "+sfkey+" eq '" + tit + "' or");
    		    commstr.append (" name eq '" + tit + "' or");
    		}
    		x++;
    		}
		}
		else
		{    	
    	str.append(sfkey+" eq '" + Title + "'");
    	commstr.append (" and name eq '" + Title + "'");
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
	
	if (Sf_entities )
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