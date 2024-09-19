import java.text.DateFormat;
import java.text.SimpleDateFormat;

/************************************************************

Utils script for (secure) SFTP integration to upload/download files
to/from SFTP Drop Box server.

@author	Raul Corrales Segura
@since	2021-08-01

************************************************************/

/**
* Upload File to SFTP Drop Box Server utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def uploadFileToDropBox(def uploadFile, def uploadFileName) {
  
  //Get SFTP connection params
  
  def params = getConnectionParams()
  
  logger.info("COMMONS_SFTP", "uploadFileToDropBox() > Params: " + params + " - FileName: " + uploadFileName)
  
  //Get PPK File
  
  def ppkFile = resp.getStorage().getFile(resp.getAppParam("sftp.ppk"))
  
  def ftpClient
  
  try {
    
    //Open SFTP connection
    
    ftpClient = resp.ftpConnect(params, ppkFile)
	
    //Upload file
    
    def outFile = ftpClient.upload(uploadFile, "/inbound/", uploadFileName)
	
  } finally {

    //Close SFTP connection
    
    if(ftpClient) {
      ftpClient.close()
    }
    
  }
}

/**
* Download File from SFTP Drop Box Server utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-04
*/
def downloadFileFromDropBox(def fileName) {
  
  //Get SFTP connection params
  
  def params = getConnectionParams()
  
  logger.info("COMMONS_SFTP", "downloadFileFromDropBox() >> Params: " + params + " - FileName: " + fileName)
  
  def outFile
  
  //Get PPK File
  
  def ppkFile = resp.getStorage().getFile(resp.getAppParam("sftp.ppk"))
  def ftpClient
  
  try {
    
    //Open SFTP connection
    
    ftpClient = resp.ftpConnect(params, ppkFile)
	
    //Download file
    
    outFile = ftpClient.download("/outbound/", fileName)
	
  } finally {

    //Close SFTP connection
    
    if(ftpClient) {
      ftpClient.close()
    }
    
  }
  
  return outFile
  
}

/**
* Get list of File from a specific folder in SFTP Drop Box Server utils method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2022-07-07
*/
def listFromDropBox(def path) {

  //Get SFTP connection params
  
  def params = getConnectionParams()
  
  logger.info("COMMONS_SFTP", "listFromDropBox() >> Params: " + params + " - Path: " + path)
  
  def files
  
  //Get PPK File
  
  def ppkFile = resp.getStorage().getFile(resp.getAppParam("sftp.ppk"))
  def ftpClient
  
  try {
    
    //Open SFTP connection
    
    ftpClient = resp.ftpConnect(params, ppkFile)
	
    //List of files
    
    files = ftpClient.list(path)
	
  } finally {

    //Close SFTP connection
    
    if(ftpClient) {
      ftpClient.close()
    }
    
  }
  
  return files
  
}

/**
* Get File Name utils method with CDL naming convention.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2021-08-24
*/
def getFileName(def name, def extension) {
    
  DateFormat df = new SimpleDateFormat("YYYYMMdd")
  Calendar date = Calendar.getInstance()  
  def dateTrans = date.format('YYYYMMdd')
  def timeTrans = date.format('hhmmss')
  def tenant = resp.getAppParam('sftp.username')
  
  //def uploadFileName = tenant + "_" + name + "_" + dateTrans + "_" + timeTrans + "." + extension;
  def uploadFileName = name + "_" + dateTrans + "_" + timeTrans + "." + extension
  
  return uploadFileName  

}

/**
* Get SFTP connection params method.
*
* @author  Raul Corrales-Segura
* @version 1.0
* @since   2022-07-07
*/
def getConnectionParams() {
 
  def params = [
    "protocol"			: "SFTP",
    "authentication"	: "PRIVATE_KEY",
    "server"			: resp.getAppParam("sftp.host"), 
    "port"				: resp.getAppParam("sftp.port").toString(),
    "username"			: resp.getAppParam("sftp.username")
  ]
  
  return params

}