/************************************************************

Utils script for Commissions DB integration.

@author	Raul Corrales Segura
@since	2021-06-01

************************************************************/

/**
* Execute SQL statement utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-06-01
*/
def execute(String sql) {  
  
  try {
    def db = resp.dbConnect('datasource.HANA')
    db.execute(sql)
  } catch(e) {
    logger.error("COMMONS_DB_EXECUTE", e)
    throw e
  }

}

/**
* Execute SQL batch Update utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def batchUpdate(def sqlList) {
  
  try {
    def db = resp.dbConnect('datasource.HANA')
    def sqlArray = sqlList.toArray(new String[sqlList.size()])
    db.batchUpdate(sqlArray)
  } catch(e) {
    logger.error("COMMONS_DB_BATCHUPDATE", e)
    throw e
  }
  
} 

/**
* Run SQL query and return results set utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def find(def query) {

  try {
    
    def db = resp.dbConnect('datasource.HANA')
    return db.queryForList(query) 
    
  } catch(e) {
    logger.error("COMMONS_DB_FIND", e)
    throw e
  }

}

/**
* Run SQL query with pagination and return results set utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def findWithPagination(def query) {

  def results = []
  
  try {
    
    def db = resp.dbConnect('datasource.HANA')
    
    def pageNum		= 0
    def pageSize	= 1000
    def finalPage	= false
    
    while(!finalPage) {
      
      def pageResults = db.queryForList(query, pageSize, pageNum)       
      results.addAll(pageResults)
      
      if (pageResults.size() < pageSize) {
        finalPage = true
      }

      pageNum++;

    }

  } catch(e) {
    logger.error("COMMONS_DB_FINDWITHPAGINATION", e)
    throw e
  }
    
  return results

}

/**
* Run SQL query for page and return results set utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def findForPage(def query, def pageSize, def pageNum) {

  def results = []
  
  try {
    
    def db = resp.dbConnect('datasource.HANA')
    results = db.queryForList(query, pageSize, pageNum)       
    
  } catch(e) {
    logger.error("COMMONS_DB_FINDWITHPAGINATION", e)
    throw e
  }
    
  return results

}

/**
* Run SQL query and return an entry utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def get(def query) {
  
  try {
    
    def db = resp.dbConnect('datasource.HANA')
    return db.queryForMap(query)
                    
  } catch(e) {
    logger.error("COMMONS_DB_GET", e)
    throw e  
  } 
}
 
/**
* Execute Stored Procedure utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def executeStoredProcedure(String name, Map inputs, Map params) {
  
  try {
    
    def db = resp.dbConnect('datasource.HANA')

    //Create stored procedure and add params
    
    def sp = db.createStoreProcedure(name)
    Map <String, String> inParams = new HashMap()    
    
    inputs.each{entry -> sp.addInParameter(entry.key, entry.value)}    
    params.each{entry -> inParams.put(entry.key, entry.value)}
    
    //Execute stored procedure
    
    def result = db.execute(sp, inParams)
    
  } catch(e) {
    logger.error("COMMONS_DB_EXECUTESTOREDPROCEDURE", e)
    throw e  
  } 
}