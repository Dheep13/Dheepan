/*eslint no-console: 0, no-unused-vars: 0, quotes: 0 */
function callProc() {
	var conn;
	conn = $.hdb.getConnection();
	var ts = new Date().toISOString();

	conn.executeUpdate('call EXT.KYN_Lib_Schedule:Run()');
	
	conn.commit();
	conn.close();	
	console.log("call_Schedule.xsjs:callProc: ", ts);
}