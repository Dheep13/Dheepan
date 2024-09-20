CREATE PROCEDURE EXT.CTAS_SP_ERROR_OUT(OUT FILENAME varchar(255), IN plRunSeq varchar(255) default '',IN pPeriodName varchar(255) default '' ) 
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for the inbound Payroll Outbound

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       07-SEP-2023		Deepan		     Initial creation

***************************************************************************************************/
DECLARE v_proc_name varchar2(50);
DECLARE v_timestamp VARCHAR(20);
DECLARE v_periodName varchar2(50);
DECLARE vPeriodSeq bigint;
DECLARE dynamic_sql NVARCHAR(1000);
DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;

DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN 
    rollback;
    ext.ctas_event_log (v_proc_name,::SQL_ERROR_CODE || ' . ' ||::SQL_ERROR_MESSAGE,0);
    commit;
resignal;
END;

SELECT 
(RIGHT(TO_CHAR(current_timestamp, 'YYYYMMDD'), 6)||'_'||TO_CHAR(current_timestamp, 'HH24MISS'))
into v_timestamp
FROM dummy;
v_proc_name :=::CURRENT_OBJECT_NAME;

ext.ctas_event_log (:v_proc_name,'Start',0);

delete from ext.ctas_error_detail_current;

insert into ext.ctas_error_detail_current
(select * from ext.ctas_error_detail where processflag=0);

commit;

ext.ctas_event_log (:v_proc_name,'End',0);

update ext.ctas_error_detail set processflag=3;

FILENAME := 'ERROR_OUT_'||:v_timestamp||'_Draft.txt';
commit;
end