CREATE procedure exec_pre_allocate 
as
begin
	DECLARE Gv_Periodseq BIGINT; /* package/session variable */
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE Gv_Periodname VARCHAR(100); /* package/session variable */
    DECLARE gv_calendarSeq BIGINT; /* package/session variable */
    
    dbmtk_init_session_global();
    /* retrieve the package/session variables referenced in this procedure */
    SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
    SELECT SESSION_CONTEXT('GV_PERIODNAME') INTO Gv_Periodname FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_CALENDARSEQ') AS BIGINT) INTO gv_calendarSeq FROM SYS.DUMMY ;
    /* end of package/session variables */

	--return;
    Log('comInit 108');
    comInit();
  --version 21 GST enhancement
    Log('SP_UPDATE_GST');
    SP_UPDATE_GST(gv_PeriodSeq);
    Log('SP_UPDATE_GST');
    if gv_processingUnitSeq != 38280596832649218 then
      return;
    end if;
    Log('comInit 108');

    Log('exec_pre_allocate 109');
    exec_pre_allocate1('Pre-Allocate',gv_periodName,gv_periodSeq,gv_calendarSeq,gv_processingUnitSeq);
    Log('exec_pre_allocate 109');

  end --exec_pre_allocate;