 CREATE procedure comAuditLog(IN i_eventType VARCHAR(255),  
    IN i_userId VARCHAR(255),   
    IN i_objectName VARCHAR(255),   
    IN i_log VARCHAR(255)) as
  begin
 declare v_tenantid varchar(20);
    DECLARE gv_error VARCHAR(1000); /* package/session variable */
	  DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Info [SP_UPDATE_DO_QUARTERLY]: The stage hook will be skip in current period.';
            SET SESSION 'GV_ERROR' = :gv_error;
            --comExceptionHandling(sqlcode,sqlerrm);
        END;

       SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
select tenantid into v_tenantid from cs_tenant;
	  
    return;

    INSERT INTO CS_AUDITLOG_CUSTOM
      (tenantid,
       auditLogSeq,
       objectSeq,
       eventDate,
       eventType,
       userId,
       objectName,
       eventDescription,
       objectType,
       businessUnitMap)
    select v_tenantid,
       AIA_Sequence_msg.nextval+row_number () over (order by 0*0),
       0, -- objSeq,
       current_date, --ImportUtils.vPipelineRunDate,
       i_eventType,
       i_userId,
       i_objectName,
       i_log,
       null, -- objectType
       null -- businessUnitMap
       from dummy;

    commit;

  end;