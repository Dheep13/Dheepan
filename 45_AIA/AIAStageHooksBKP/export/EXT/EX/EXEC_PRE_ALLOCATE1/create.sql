CREATE procedure exec_pre_allocate1(	IN stage VARCHAR(255),  /* ORIGSQL: stage IN varchar2 */
											    IN period VARCHAR(255),   /* ORIGSQL: period IN varchar2 */
											    IN periodSeq BIGINT,   /* ORIGSQL: periodSeq IN int */
											    IN calendarSeq BIGINT,   /* ORIGSQL: calendarSeq IN int */
											    IN processingUnitSeq BIGINT     /* ORIGSQL: processingUnitSeq IN int */) 
as
begin

	DECLARE Gv_Periodseq BIGINT; /* package/session variable */
    DECLARE gv_calendarSeq BIGINT; /* package/session variable */
    DECLARE cdt_EndOfTime date = EndOfTime();
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE V_Periodstartdate TIMESTAMP;  /* ORIGSQL: V_Periodstartdate date; */
    DECLARE V_Periodenddate TIMESTAMP;  /* ORIGSQL: V_Periodenddate date; */
    declare v_tenantid varchar(20)=ext.gettenantid();

    /* retrieve the package/session variables referenced in this procedure */
    SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_CALENDARSEQ') AS BIGINT) INTO gv_calendarSeq FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
    /* end of package/session variables */ 


 Select Startdate, Enddate
      Into V_Periodstartdate, V_Periodenddate
      From Cs_Period
     Where Periodseq = gv_Periodseq
       and calendarseq = gv_calendarseq
       and removedate = cdt_endoftime;

Log('71 Periodseq ' || gv_Periodseq);

/*Arjun 0523. Set ga4 to null for setnumber1*/

update cs_Transactionassignment ta
set genericattribute4=null, genericattribute10=null
where tenantid=v_tenantid and processingunitseq=gv_ProcessingUnitSeq
and setnumber in (1,41,51) --added 41/51 on 6/12
and Compensationdate >= V_Periodstartdate
               And Compensationdate < V_Periodenddate;

Log('71 Update GA4/10 to null '||::ROWCOUNT);
commit;
    Log('SP_UPDATE_TXN 100');
    SP_UPDATE_TXN(gv_PeriodSeq);
    Log('SP_UPDATE_TXN 100');

    --Log('SP_TXA_PIAOR 101');
    --SP_TXA_PIAOR(gv_PeriodSeq);
    --Log('SP_TXA_PIAOR 101');

    Log('SP_TXA_NADOR_EXCEPTION 102');
    SP_TXA_NADOR_EXCEPTION(gv_PeriodSeq);
    Log('SP_TXA_NADOR_EXCEPTION 102');

    --for revamp begin
    --Batch2
    Log('SP_TXA_OVERRIDING_ASSIGNMENT 201');
    SP_TXA_OVERRIDING_ASSIGNMENT(periodSeq);
    Log('SP_TXA_OVERRIDING_ASSIGNMENT 201');
    --for revamp end

    commit;

  end --exec_pre_allocate;