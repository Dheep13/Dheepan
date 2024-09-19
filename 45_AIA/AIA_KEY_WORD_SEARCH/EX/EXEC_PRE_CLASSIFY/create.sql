CREATE procedure exec_pre_classify
as
  begin
	DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */
    dbmtk_init_session_global();
   
    SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;

	  
  --return;
    Cominit();
	
   if gv_processingUnitSeq != 38280596832649218 then
      return;
    end if;

    --clean up assignment
    Log('Comcleanassignment 106');
    Comcleanassignment(Gv_Periodseq);
    Log('Comcleanassignment 106');

    -- commented as PIAOR stagehook is not longer executed
    /*Log('exec_pre_classify 107');
    exec_pre_classify('Pre-classify',
                      gv_periodName,
                      gv_periodSeq,
                      gv_calendarSeq,
                      gv_processingUnitSeq);
    Log('exec_pre_classify 107');*/


  end --exec_pre_classify;