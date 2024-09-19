/*********************************************

Check the KPI Process Flag in EXT.CTAS_DAILYKPI_CTL
and if Y, drop a trigger file in the SFTP dropox in
the format "DAILYKPI_INB_<YYYYMMDD>_<hhmiss>.txt".

@author Matthew Sosebee
@since	2023-08-04

*********************************************/

def sftpUtils	= resp.importScript("commons_sftp_utils");
def dbUtils 	= resp.importScript("commons_db_utils");

def query = 
"""
SELECT DISTINCT KPI_PROCESS_FLAG
FROM EXT.CTAS_DAILYKPI_CTL
WHERE KPI_PROCESS_FLAG = 'Y'
""";

def results = dbUtils.invoke("findWithPagination", query);
if (results?.size() > 0){
  //TODO - drop trigger file 
  
  def triggerFileName = sftpUtils.invoke("getFileName", 'DAILYKPI_INB','txt' );
  
  def content = "";
  def triggerFile = resp.newFile(triggerFileName, content);

  try {

    //Upload CDL trigger file.
    sftpUtils.invoke("uploadFileToDropBox", triggerFile, triggerFileName);

  }
  catch(e) {

    //If there is an exception when uploading the file we close the case in Exception Queue status.
    logger.error("DROP_INBOUND_FILE", e);
  } 
  
}