 CREATE procedure comUpdPMCreditTrace(in i_component varchar2(500)) 
 as
  begin
	    declare vSeq bigint;
	    DECLARE gv_error VARCHAR(1000); /* package/session variable */
	    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [' || IFNULL(:i_component,'') || ' pmCreditTrace]: ' ||::SQL_ERROR_MESSAGE ;  /* ORIGSQL: sqlerrm */

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            /*dbms_utility.format_error_backtrace;*/
            /* Saving modified package/session variable 'gv_error': */ 
            SET SESSION 'GV_ERROR' = :gv_error;
            comDebugger('pm trace', 'err:'|| IFNULL(:gv_error,''));
            ROLLBACK;
        END;

  SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
  SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
           
    --refresh PMCreditTrace

    Log('55');

    execute immediate 'delete from cs_PMCreditTrace t
     where exists (select 1
              from sh_sequence scm
             where 1 = 1
               and scm.seqType = ''PMSEQ''
               and t.measurementSeq = scm.businessSeq)';

    Commit;
    Log('55');

    --vSeq := SequenceGenPkg.GetNextFullSeq('auditLogSeq', classid.cIdAuditLog);
    ---- update CS_PMCreditTrace

    Log('56');

  execute immediate   'insert into cs_pmCreditTrace
      (creditSeq,
       measurementSeq,
       ruleSeq,
       pipelineRunSeq,
       targetPeriodSeq,
       sourcePeriodSeq,
       sourceorigintypeid,
       contributionValue,
       unittypeforContributionValue,
       businessunitMap,
       processingUnitSeq)
      select creditSeq,
             measurementSeq,
             ifnull(ruleSeq,0),
             pipelineRunSeq,
             targetPeriodSeq,
             sourcePeriodSeq,
             sourceOriginTypeId,
             contributionValue,
             unitTypeForContributionValue,
             businessUnitMap,
             processingunitseq
        from ( select
                 rownum as rn,
                 cc.creditSeq,
                 cm.measurementSeq,
                 cm.ruleSeq,
                 (select Max(Pipelinerunseq)
                    From Cs_Pipelinerun
                   Where Periodseq = Cc.Periodseq
                      and command = ''PipelineRun'') as pipelineRunSeq,
                  cm.periodSeq as targetPeriodSeq,
                  cc.periodSeq as sourcePeriodSeq,
                  cc.originTypeId as sourceOriginTypeId,
                  cc.value as contributionValue,
                  cc.unitTypeForValue as unitTypeForContributionValue,
                  cc.businessUnitMap,
                  cc.processingunitseq
                from cs_credit cc, cs_measurement cm,(select 
                            scc.businessSeq as ccseq,
                            scc.periodseq   as pseq,
                            scm.businessSeq as pmseq
                             from sh_sequence scc, sh_sequence scm
                            where scc.seqtype = ''CREDITSEQ''
                              and scm.seqtype = ''PMSEQ''
                              and scc.payeeseq = scm.payeeseq
                              and scc.positionseq = scm.positionseq
                              and scc.periodSeq = scm.periodSeq) csp
               Where  Cc.Processingunitseq = '||Gv_Processingunitseq||'
                  and Cm.Processingunitseq = '||Gv_Processingunitseq||'
                  And Cc.Origintypeid In (''calculated'', ''imported'')
                  and csp.pmseq = cm.measurementSeq
                  and csp.ccseq = cc.creditseq) t';


    commit;
    Log('57');



    --comAuditLog('PMCreditTrace','Administrator','PMCreditTrace','');
    --SequenceGenPkg.updateSeq('auditLogSeq');


  end;