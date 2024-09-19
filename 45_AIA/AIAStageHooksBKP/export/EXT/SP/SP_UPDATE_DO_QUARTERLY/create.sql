CREATE procedure SP_UPDATE_DO_QUARTERLY(in i_periodSeq bigint) 
sql security invoker
aS
  begin 
	  
	DECLARE gv_error VARCHAR(1000); /* package/session variable */
    DECLARE Gv_Periodname VARCHAR(100); /* package/session variable */
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE gv_prePeriodSeq2 BIGINT; /* package/session variable */
    DECLARE gv_prePeriodSeq1 BIGINT; /* package/session variable */
    DECLARE cdt_EndOfTime date= EndOfTime();
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_rec BIGINT = 0;  /* ORIGSQL: v_rec int := 0; */
    DECLARE v_threshold BIGINT = 0;  /* ORIGSQL: v_threshold int := 0; */
    DECLARE V_Rtn BIGINT = 0;  /* ORIGSQL: V_Rtn Int := 0; */
    DECLARE V_Credittypeseq BIGINT = 0;  /* ORIGSQL: V_Credittypeseq Int := 0; */
    DECLARE v_componentValue VARCHAR(30) = 'DO_PM';  /* ORIGSQL: v_componentValue VARCHAR2(30) := 'DO_PM'; */
    DECLARE vSQL VARCHAR(4000);  /* ORIGSQL: vSQL varchar2(4000); */
    declare v_tenantid varchar(20) = gettenantid();

/*    type PartName IS VARRAY(10) OF VARCHAR2(255);
    vParName PartName;*/

    DECLARE i INT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [SP_UPDATE_DO_QUARTERLY]: ' || ::SQL_ERROR_MESSAGE || ' - ' ||  /* ORIGSQL: sqlerrm */
            /*dbms_utility.format_error_backtrace||*/ ' - on Period:' ||
            IFNULL(:Gv_Periodname,'');
            SET SESSION 'GV_ERROR' = :gv_error;
            ROLLBACK;
            RESIGNAL; --Deepan : Added resignal since the dml on cs tables wasn't erroring out
        END;

         /* retrieve the package/session variables referenced in this procedure */
        SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
        SELECT SESSION_CONTEXT('GV_PERIODNAME') INTO Gv_Periodname FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PREPERIODSEQ2') AS BIGINT) INTO gv_prePeriodSeq2 FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PREPERIODSEQ1') AS BIGINT) INTO gv_prePeriodSeq1 FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
        /* end of package/session variables */


      comDebugger('SP_UPDATE_DO_QUARTERLY',
                'StageHook start for period [' || i_periodSeq || ']' ||
                'at [' || to_timestamp(current_date) || ']');

    ---init global variable
    v_rtn := comGetQuarterMonth(i_periodSeq);

    if v_rtn < 1 then
      return;
    end if;

    ---clean up temp table


/*    comInitialPartition('DO', v_componentValue, i_periodSeq);*/
    --Gv_Preperiodseq1:=2533274790395933;
    --Gv_Preperiodseq2:=2533274790395934;
    --I_Periodseq:=2533274790395935;

    execute immediate 'truncate table sh_sequence';

    begin
	   declare exit handler for sqlexception 
	   begin v_threshold := 1250; end; 

      select refIntValue
        into v_threshold
        from sh_reference
       where refId = 'DO_QTR_THRESHOLD';
    
    end;

    v_credittypeseq := comgetcredittypeseq('DO_QTR');

    --Maintenance.Enablepdml;

    comDebugger('SQL Performance',
                'Stagehook[SP_UPDATE_DO_QUARTERLY]-SQL1 START:' || current_date);

    Log('18');

   /* vParName := partname(' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ');

    vParName(1) := GETSUBPARTITIONNAME('CS_CREDIT',I_Periodseq,gv_ProcessingUnitSeq) ;
    vParName(2) := GETSUBPARTITIONNAME('CS_CREDIT',gv_prePeriodSeq2,gv_ProcessingUnitSeq);
    vParName(3) := GETSUBPARTITIONNAME('CS_CREDIT',Gv_Preperiodseq1,gv_ProcessingUnitSeq);


    ----aggregate the agcy credit
    -- credit type seq index
    for i in 1 .. 3 loop

      vSQL := 'Insert  Into ';
      vSQL := vSQL ||
              'sh_query_result (component,objOutputName,payeeSeq,positionSeq,periodSeq,value, ';
      vSQL := vSQL ||
              'Genericnumber1,Genericsequence1,Genericsequence2,Genericsequence3,Genericsequence4,Genericboolean1,Genericnumber2,Genericattribute1) ';
      vSQL := vSQL ||
              'select ''' ||
              v_componentValue ||
              ''' as component,''PM_DO_QTR_BN'' as pm_name, cc.payeeSeq,cc.positionSeq, ';
      vSQL := vSQL || ':i_periodSeq,sum(cc.value) as aggValue, ';
      vSQL := vSQL ||
              'count(cc.creditSeq) as numberOfCredit,cp.ruleElementOwnerSeq as agntPost, ';
      vSQL := vSQL ||
              'Cp.Payeeseq As Agntpayee , Cc.Periodseq ,Cc.Credittypeseq,0 As Validflag, :V_Threshold  As Threshold, Cc.Genericattribute12 As Agentcode ';
      vSQL := vSQL || 'From Cs_Credit '|| 'SUBPARTITION (' || vParName(i) ||              ') '|| ' Cc, Cs_Position Cp,';
      vSQL := vSQL || 'Cs_Position Leaderpos ';
      vSQL := vSQL || 'Where  cc.creditTypeSeq=:v_credittypeseq ';
      vSQL := vSQL || 'and cp.name=''BRT''||cc.genericAttribute12 ';
      vSQL := vSQL || 'and cp.removeDate=:cdt_EndOfTime ';
      vSQL := vSQL || 'and cc.compensationDate >= cp.effectiveStartDate ';
      vSQL := vSQL || 'And Cc.Compensationdate < Cp.Effectiveenddate ';
      vSQL := vSQL || 'and leaderpos.removeDate=:cdt_EndOfTime ';
      vSQL := vSQL ||
              'and cc.compensationDate >= leaderpos.effectiveStartDate ';
      vSQL := vSQL ||
              'And Cc.Compensationdate < Leaderpos.Effectiveenddate ';
      vSQL := vSQL || 'And Leaderpos.Name = ''BRT''||Cp.Genericattribute2 ';
      vSQL := vSQL ||
              'and cc.positionseq <> Leaderpos.ruleElementOwnerSeq ';
      vSQL := vSQL ||
              'group by cc.payeeseq,cc.positionseq,cp.ruleElementOwnerSeq,cp.payeeSeq,cc.periodSeq,cc.creditTypeSeq,cc.genericAttribute12 ';

      dbms_output.put_line('sql ' || i || ' IS ' || vSQL);
      Log('sql ' || i || ' IS ' || vSQL);

      begin
        EXECUTE IMMEDIATE vSQL
          USING i_periodSeq, V_Threshold, v_credittypeseq, cdt_EndOfTime, cdt_EndOfTime;
      exception
        when others then
          Log('sql error ' || i || ' IS ' || sqlerrm);

      end;
      COMMIT;
      Log('18');
    end loop;
*/
   
   begin 
	   declare exit handler for sqlexception 
	   begin Log('sql error IS ' || ::SQL_ERROR_MESSAGE); end;
	   
    insert into sh_query_result (component,
	objOutputName,
	payeeSeq,
	positionSeq,
	periodSeq,
	value,
	Genericnumber1,
	Genericsequence1,
	Genericsequence2,
	Genericsequence3,
	Genericsequence4,
	Genericboolean1,
	Genericnumber2,
	Genericattribute1)
select 	v_componentValue as component,
	'PM_DO_QTR_BN' as pm_name,
	cc.payeeSeq,
	cc.positionSeq,
	:i_periodSeq,
	sum(cc.value) as aggValue,
	count(cc.creditSeq) as numberOfCredit,
	cp.ruleElementOwnerSeq as agntPost,
	Cp.Payeeseq as Agntpayee ,
	Cc.Periodseq ,
	Cc.Credittypeseq,
	0 as Validflag,
	:V_Threshold as Threshold,
	Cc.Genericattribute12 as Agentcode
from
	Cs_Credit cc,
	Cs_Position Cp,
	Cs_Position Leaderpos
where
	cc.creditTypeSeq =:v_credittypeseq
	and cc.periodseq in (I_Periodseq,gv_prePeriodSeq2,Gv_Preperiodseq1)
	and cc.processingunitseq = gv_ProcessingUnitSeq
	and cp.name = 'BRT' || cc.genericAttribute12
	and cp.removeDate =:cdt_EndOfTime
	and cc.compensationDate >= cp.effectiveStartDate
	and Cc.Compensationdate < Cp.Effectiveenddate
	and leaderpos.removeDate =:cdt_EndOfTime
	and cc.compensationDate >= leaderpos.effectiveStartDate
	and Cc.Compensationdate < Leaderpos.Effectiveenddate
	and Leaderpos.Name = 'BRT' || Cp.Genericattribute2
	and cc.positionseq <> Leaderpos.ruleElementOwnerSeq
group by
	cc.payeeseq,
	cc.positionseq,
	cp.ruleElementOwnerSeq,
	cp.payeeSeq,
	cc.periodSeq,
	cc.creditTypeSeq,
	cc.genericAttribute12;
 	
commit;
Log('18');
end;


   
   
    select count(*)
      into v_rec
      from (select 1
              from sh_query_result
             Where Component = V_Componentvalue
               and periodSeq = gv_periodseq
               limit 1);

    if v_rec <= 0 then
      comDebugger('DO_QTR', 'No any query result');
      return;
    end if;

    Log('v_rec count is greater than 0, proceeding with processing....');
    Log('19');

    ----validate with PM_PID_PM
    merge into sh_query_result r
    using (select cm.payeeSeq, cm.positionSeq, sum(cm.value) as aggValue
             from cs_measurement cm
            where cm.tenantid=v_tenantid and Cm.Periodseq In
                  (I_Periodseq, Gv_Preperiodseq1, Gv_Preperiodseq2)
              AND CM.PROCESSINGUNITSEQ = gv_processingunitseq
              and cm.name = 'PM_PIB_BN'
            group by cm.payeeSeq, cm.positionSeq) m
    on (r.genericSequence2 = m.payeeSeq and r.genericSequence1 = m.positionSeq And R.Component = V_Componentvalue and r.periodSeq = gv_periodSeq)

    when matched then
      update
         set r.genericBoolean1 = case
                                   when r.genericNumber2 <= m.aggValue then
                                    1
                                   else
                                    0
                                 end,
             r.genericNumber3  = m.aggValue;

    commit;
    Log('19');

    Log('20');



    Insert Into Sh_Sequence
    select cc.creditSeq,
           'CREDITSEQ' as seqType,
           cc.payeeSeq,
           cc.positionSeq,
           i_periodSeq
      from cs_credit cc, sh_query_result r
     Where r.Component = V_Componentvalue
       and r.periodseq = gv_periodSeq
       and r.genericBoolean1 = 1
       and cc.creditTypeSeq = r.genericSequence4
       and cc.payeeSeq = r.payeeSeq
       and cc.positionSeq = r.positionSeq
       and cc.periodSeq = r.genericSequence3
       and cc.genericAttribute12 = r.genericAttribute1 -- check agentcode
       and cc.processingUnitseq = gv_processingUnitSeq
       and cc.origintypeid in ('calculated', 'imported');


        Log('20 '||::ROWCOUNT);
        commit;



    Log('21');

--Deepan : UnComment the below statement 
/*
    execute immediate 'Update Cs_Credit t
       Set Genericboolean3 = 1
     Where   Processingunitseq = Gv_Processingunitseq
       and t.Creditseq in (Select Sh.Businessseq
                             From Sh_Sequence Sh
                            Where Sh.Seqtype = ''CREDITSEQ'')';
*/
    commit;
    Log('21');

    Log('22');

    --identify pm seq
    insert into sh_sequence
      select distinct cm.measurementSeq,
                      'PMSEQ' as seqType,
                      cm.payeeSeq,
                      cm.positionSeq,
                      cm.periodSeq
        from cs_measurement cm, sh_query_result r
       Where cm.tenantid=v_tenantid and R.Component = V_Componentvalue
         and r.periodSeq = gv_periodseq
         and r.genericBoolean1 = 1
         and r.objOutputName = cm.name
         and r.payeeSeq = cm.payeeSeq
         and r.positionSeq = cm.positionSeq
         and r.periodSeq = cm.periodSeq
         and cm.processingUnitseq = gv_processingUnitSeq;

    commit;
    Log('22');

    Log('23');
    --Deepan : UnComment the below statement 
  /*  execute immediate 'merge into cs_measurement cm
    using (select r.payeeSeq,
                  r.positionSeq,
                  r.periodSeq,
                  sum(r.value) as aggValue,
                  sum(r.genericNumber1) as numberOfCredits,
                  s.businessSeq
             from sh_query_result r, sh_sequence s
            Where R.Component = '''|| V_Componentvalue||'''
              and r.periodseq = '||gv_periodSeq||'
              and r.genericBoolean1 = 1
              and r.positionSeq = s.positionSeq
              and r.payeeSeq = s.payeeSeq
              and r.periodSeq = s.periodSeq
              and s.seqType = ''PMSEQ''
            group by R.payeeSeq, R.positionSeq, R.periodSeq, S.businessSeq) t
    on (t.businessSeq = cm.measurementSeq)
    when matched then
      update
         set cm.value = t.aggValue, cm.numberOfCredits = t.numberOfCredits';
*/
    COMMIT;
    Log('23');

    Log('24');
    comUpdPMCreditTrace('SP_UPDATE_DO_QUARTERLY');
    Log('24');
    Log('End of '|| ::CURRENT_OBJECT_NAME);

    --comDebugger('SP_UPDATE_DO_QUARTERLY','StageHook completed for period ['||i_periodSeq||']'||'at ['||to_timestamp(current_date)||']');

  end