 CREATE REMOTE SOURCE "YOUR REMOTE SOURCE NAME"
	ADAPTER "ODataAdapter" AT LOCATION DPSERVER
		CONFIGURATION '<?xml version="1.0" encoding="UTF-8"?>
<ConnectionProperties name="UI">
  <PropertyEntry name="URL">https://<YOUR TENANTID>.callidusondemand.com/TrueComp-SaaS/CommissionsService.svc/</PropertyEntry>
  <PropertyEntry name="proxyserver"></PropertyEntry>
  <PropertyEntry name="proxyport"></PropertyEntry>
  <PropertyEntry name="proxyAuthentication">false</PropertyEntry>
  <PropertyEntry name="truststore"></PropertyEntry>
  <PropertyEntry name="isfiletruststore"></PropertyEntry>
  <PropertyEntry name="supportformatquery"></PropertyEntry>
  <PropertyEntry name="requireCSRFheader"></PropertyEntry>
  <PropertyEntry name="CSRFheadername"></PropertyEntry>
  <PropertyEntry name="CSRFheaderfetchvalue"></PropertyEntry>
  <PropertyEntry name="supportdatefunctions"></PropertyEntry>
  <PropertyEntry name="shownavigationproperties"></PropertyEntry>
  <PropertyEntry name="supportEncodingGzip"></PropertyEntry>
  <PropertyEntry name="followRedirects"></PropertyEntry>
  <PropertyEntry name="extraconnectionparameters"></PropertyEntry>
  <PropertyEntry name="verifyServerCertificate"></PropertyEntry>
  <PropertyEntry name="extraHeaderparameters"></PropertyEntry>
  <PropertyEntry name="convertToLocalTimeZone"></PropertyEntry>
  <PropertyEntry name="enableSSLAnonymous"></PropertyEntry>
  <PropertyEntry name="supportNCLOBForEdmString"></PropertyEntry>
  <PropertyEntry name="HTTPConnectionTimeout"></PropertyEntry>
</ConnectionProperties>'
WITH CREDENTIAL TYPE 'PASSWORD' USING       
               '<CredentialEntry name="password">              
               <user>enter your username</user>              
               <password>enter your password here </password>       
               </CredentialEntry>';
