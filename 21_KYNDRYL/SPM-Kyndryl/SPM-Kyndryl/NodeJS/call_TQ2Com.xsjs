/*eslint no-console: 0, no-unused-vars: 0, quotes: 0 */
function callProc() {
	var conn;
	conn = $.hdb.getConnection();
	var ts = new Date().toISOString();

	conn.executeUpdate('call EXT.KYN_Lib_TQ2Com:run()');
	
	conn.commit();
	conn.close();	
	console.log("call_TQ2Com.xsjs:callProc: ", ts);
}