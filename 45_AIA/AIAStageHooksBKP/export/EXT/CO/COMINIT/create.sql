CREATE procedure comInit() 
as
begin
	DECLARE Gv_Periodseq BIGINT; /* package/session variable */
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE gv_plStartTime TIMESTAMP; /* package/session variable */
    DECLARE Gv_Pipelinerunseq BIGINT; /* package/session variable */
    DECLARE gv_isYearEnd BIGINT; /* package/session variable */
    DECLARE gv_calendarSeq BIGINT; /* package/session variable */
    DECLARE Gv_Periodname VARCHAR(100); /* package/session variable */
    DECLARE cdt_EndOfTime date := to_date('2200-01-01','yyyy-mm-dd');
    declare v_tenentid varchar(25);

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE INVALID_PIPELINE CONDITION;  /* ORIGSQL: INVALID_PIPELINE EXCEPTION; */
    DECLARE INVALID_PERIOD CONDITION;  /* ORIGSQL: INVALID_PERIOD EXCEPTION; */
    DECLARE INVALID_FIXEDVALUE CONDITION;  /* ORIGSQL: Invalid_Fixedvalue Exception; */
    DECLARE INVALID_PIPELINERUNSEQ CONDITION;  /* ORIGSQL: Invalid_PipelineRunSeq exception; */

    DECLARE EXIT HANDLER FOR INVALID_PIPELINE
        BEGIN
            comDebugger('Initializtion', 'Due to some gloable pipeline information is not found, the stagehook will abort');
	        SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT ='Due to some global pipeline information is not found, the stagehook will abort';
        END;

    DECLARE EXIT HANDLER FOR INVALID_PIPELINERUNSEQ
        BEGIN
            comDebugger('Initializtion', 'Due to some gloable pipeline run seq is not zero, the stagehook will abort');
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = 'Due to some global pipeline run seq is not zero, the stagehook will abort';
        END;

    DECLARE EXIT HANDLER FOR INVALID_PERIOD
        BEGIN
            comDebugger('Initializtion', 'Due to some gloable period information is not found, the stagehook will abort');
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT ='Due to some global period information is not found, the stagehook will abort';
        END;

    DECLARE EXIT HANDLER FOR INVALID_FIXEDVALUE
        BEGIN
			comDebugger('Initializtion', 'Due to some gloable fixed value is not found, the stagehook will abort');
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT ='Due to some global fixed value is not found, the stagehook will abort';
        END;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT ='Initializtion procedure has failed: ' ||::SQL_ERROR_MESSAGE;  /* ORIGSQL: sqlerrm */
        END;
 		
       select tenantid into v_tenentid from cs_tenant;
       
        /* retrieve the package/session variables referenced in this procedure */
        SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
        SELECT TO_TIMESTAMP(SESSION_CONTEXT('GV_PLSTARTTIME'), 'yyyy Mon dd hh24:mi:ss:ff3') INTO gv_plStartTime FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PIPELINERUNSEQ') AS BIGINT) INTO Gv_Pipelinerunseq FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_ISYEAREND') AS BIGINT) INTO gv_isYearEnd FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_CALENDARSEQ') AS BIGINT) INTO gv_calendarSeq FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('GV_PERIODNAME') INTO Gv_Periodname FROM SYS.DUMMY ;
        /* end of package/session variables */



    --dbms_output.put_line('xxxxxx');
    BEGIN 
        DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            /* ORIGSQL: WHEN NO_DATA_FOUND THEN */
            BEGIN
                --     gv_PeriodSeq := 2533274790395933;
                --     gv_processingUnitSeq := 38280596832649218;
                --     gv_plStartTime := sysdate;
                --     Gv_Pipelinerunseq    := 1;
                /* ORIGSQL: raise INVALID_PIPELINE; */
                SIGNAL INVALID_PIPELINE;
            END;



      Log('65');

      select max(periodseq),
             max(processingUnitSeq),
             max(starttime),
             Max(Pipelinerunseq)
        into gv_PeriodSeq,
             gv_processingUnitSeq,
             gv_plStartTime,
             Gv_Pipelinerunseq
        from cs_pipelinerun
       Where tenantid=v_tenentid and  Pipelinerunseq In (Select Max(Pipelinerunseq)
                                  From Cs_Pipelinerun
                                 Where tenantid=v_tenentid and  Periodseq Is Not Null
                                --and periodseq=2533274790398965
                                )
         AND COMMAND = 'PipelineRun';
        
        Log('65');

      If Gv_Pipelinerunseq = 0 then signal INVALID_PIPELINERUNSEQ; end if;

      --     gv_PeriodSeq := 2533274790395933;
      --     gv_processingUnitSeq := 38280596832649218;
      --     gv_plStartTime := sysdate;
      --     Gv_Pipelinerunseq    := 1;
  
    END;

    Log('StageHook start for period [' || Gv_Periodseq || ']' ||
                         'processingunitseq [' || Gv_Processingunitseq ||
                         '] plStartDate[' || Gv_Plstarttime ||
                         '] gv_pipelineRunSeq[' || Gv_Pipelinerunseq ||
                         '] gv_isYearEnd[' || Gv_Isyearend ||
                         '] gv_calendarSeq[' || Gv_Calendarseq || ']');

    Log('66');

    Begin
  DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            BEGIN
                --    Gv_Periodname := 'May 2015';
                --    V_Periodstartdate := sysdate;
                --    gv_calendarSeq := 2251799813685249;
                /* ORIGSQL: raise INVALID_PERIOD; */
                SIGNAL INVALID_PERIOD;
            END;
	    
	    select name, startdate, calendarSeq
        Into Gv_Periodname, V_Periodstartdate, gv_calendarSeq --add the calendarseq on nov/11/2014
        from cs_period
       Where Periodseq = Gv_Periodseq
         AND REMOVEDATE = Cdt_Endoftime;
  
    END;

    Log('67');

    Log('68');
    BEGIN

      select ifnull(max(value), 0)
        into gv_isYearEnd
        from cs_fixedValue
       where name = 'FV_Mo_End'
         And Removedate = Cdt_Endoftime
         And Effectivestartdate <= v_periodStartDate
         and effectiveEndDate > v_periodStartDate
         and exists
       (select 1
                from cs_period p, cs_period py, Cs_Periodtype Pt
               where py.periodtypeseq = pt.periodtypeseq
                 and py.enddate = p.enddate
                 And P.Periodseq = Gv_Periodseq
                 and pt.name = 'year'
                 And Py.Calendarseq = Gv_Calendarseq
                 and py.removedate = Cdt_Endoftime
                 And Pt.Removedate = Cdt_Endoftime
                 and p.removedate = Cdt_Endoftime);

  Log('68 gv_isYearEnd'||gv_isYearEnd);

  
    Log('68');
   
    	SET SESSION 'GV_PERIODSEQ' = CAST(:Gv_Periodseq AS VARCHAR(512));
       	SET SESSION 'GV_PROCESSINGUNITSEQ' = CAST(:Gv_Processingunitseq AS VARCHAR(512));  
        SET SESSION 'GV_PLSTARTTIME' = TO_VARCHAR(:gv_plStartTime, 'yyyy Mon dd hh24:mi:ss:ff3');
        SET SESSION 'GV_PIPELINERUNSEQ' = CAST(:Gv_Pipelinerunseq AS VARCHAR(512));
        SET SESSION 'GV_PERIODNAME' = :Gv_Periodname;
        SET SESSION 'GV_CALENDARSEQ' = CAST(:gv_calendarSeq AS VARCHAR(512));
        

    /* Comdebugger('initialized','StageHook start for period ['||Gv_Periodseq||']'||'processingunitseq ['||Gv_Processingunitseq||
      '] plStartDate['||Gv_Plstarttime||'] gv_pipelineRunSeq['||Gv_Pipelinerunseq||'] gv_isYearEnd['||Gv_Isyearend||'] gv_calendarSeq['||Gv_Calendarseq||']');
    */

    -- gv_hryc:=comGetEventtypeSeq('H_RYC');
end;
 

  end-- comInit