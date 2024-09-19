CREATE procedure exec_post_allocate 
as
  begin

	DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE Gv_Periodname VARCHAR(100); /* package/session variable */
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */
    DECLARE gv_calendarSeq BIGINT; /* package/session variable */
	dbmtk_init_session_global();
    /* retrieve the package/session variables referenced in this procedure */
    SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GV_PERIODNAME') INTO Gv_Periodname FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_CALENDARSEQ') AS BIGINT) INTO gv_calendarSeq FROM SYS.DUMMY ;
    /* end of package/session variables */

  --return;
    Log('110');
    comInit();
    if gv_processingUnitSeq != 38280596832649218 then
      return;
    end if;
    Log('exec_pre_allocate 110');
    Log('exec_post_allocate 111');
    exec_post_allocate1('Post-PostAllocate',
                       gv_periodName,
                       gv_periodSeq,
                       gv_calendarSeq,
                       gv_processingUnitSeq);
    Log('exec_post_allocate 111');
  end --exec_post_allocate;