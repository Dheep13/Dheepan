CREATE procedure exec_post_allocate1( IN stage VARCHAR(255),  /* ORIGSQL: stage IN varchar2 */
    											IN period VARCHAR(255),   /* ORIGSQL: period IN varchar2 */
											    IN periodSeq BIGINT,   /* ORIGSQL: periodSeq IN int */
											    IN calendarSeq BIGINT,   /* ORIGSQL: calendarSeq IN int */
											    IN processingUnitSeq BIGINT     /* ORIGSQL: processingUnitSeq IN int */) 
sql security invoker 
  as
  begin
 	DECLARE Gv_Periodseq BIGINT; /* package/session variable */

    /* retrieve the package/session variables referenced in this procedure */
    SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;

    --comDebugger('post_allowacte',periodSeq||',PUSeq'||processingUnitSeq);

    --execute nador validation
    Log('SP_UPDATE_NADOR 103');
    SP_UPDATE_NADOR(gv_PeriodSeq);
    Log('SP_UPDATE_NADOR 103');

    --execute pbu buyout

    Log('SP_UPDATE_PBUBUYOUT 104');
    SP_UPDATE_PBUBUYOUT(gv_PeriodSeq);
    Log('SP_UPDATE_PBUBUYOUT 104');

    --execute do brunei quarterly

    Log('SP_UPDATE_DO_QUARTERLY 105');
    SP_UPDATE_DO_QUARTERLY(gv_PeriodSeq);
    Log('SP_UPDATE_DO_QUARTERLY 105');

    --Added for version 3 by Win Tan
    --for Fair BSC
    Log('[CB] SP_CLAWBACK_CALCULATION 202 start');
    SP_CLAWBACK_CALCULATION(periodSeq); -- TBD
    Log('[CB] SP_CLAWBACK_CALCULATION 202 end');
    --for Fair BSC end
    --for Fair BSC FA
    Log('[CB] SP_CLAWBACK_CALCULATION FA 202 start');
    SP_CLAWBACK_CALCULATION_FA(periodSeq);--TBD
    Log('[CB] SP_CLAWBACK_CALCULATION FA 202 end');
    --for Fair BSC FA end

    --ensure the seq is updated
    Log('106');
    /*sequencegenpkg.updateSeq('salesTransactionSeq');*/ -- no access in HANA 
    Log('106');

  END --exec_post_allocate;