CREATE PROCEDURE SP_UPDATE_NADOR(in i_periodSeq bigint ) 
sql security invoker 
as 
begin
    DECLARE gv_error VARCHAR(1000); /* package/session variable */
    DECLARE Gv_Periodname VARCHAR(100); /* package/session variable */
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE gv_prePeriodSeq1 BIGINT; /* package/session variable */
    DECLARE gv_prePeriodSeq2 BIGINT; /* package/session variable */
    DECLARE cdt_EndOfTime date := to_date('2200-01-01','yyyy-mm-dd');
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */	
    
    DECLARE v_periodSeq BIGINT = :i_periodSeq;  /* ORIGSQL: v_periodSeq int := i_periodSeq; */
    DECLARE v_rec BIGINT = 0;  /* ORIGSQL: v_rec int := 0; */
    DECLARE v_rtn BIGINT = 0;  /* ORIGSQL: v_rtn int := 0; */
    DECLARE V_Credittypeseq BIGINT;  /* ORIGSQL: V_Credittypeseq Int; */

    --for revamp
    --begin
    DECLARE V_Credittypeseq_w BIGINT;  /* ORIGSQL: V_Credittypeseq_w Int; */
    DECLARE V_Credittypeseq_w_dup BIGINT;  /* ORIGSQL: V_Credittypeseq_w_dup Int; */
    DECLARE V_Credittypeseq_wc_dup BIGINT;  /* ORIGSQL: V_Credittypeseq_wc_dup Int; */

    --end

    DECLARE v_componentValue VARCHAR(30) = 'NADOR_VALIDATION';  /* ORIGSQL: v_componentValue varchar2(30) := 'NADOR_VALIDATION'; */
    DECLARE vSQL VARCHAR(4000);  /* ORIGSQL: vSQL varchar2(4000); */
    DECLARE i INT;
    declare v_tenantid varchar(255);
   	DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [SP_UPDATE_NADOR]: ' ||::SQL_ERROR_MESSAGE || ' - ' || ' - on Period: ' ||gv_periodName;  

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
--            dbms_utility.format_error_backtrace;
            /* Saving modified package/session variable 'gv_error': */ 
            SET SESSION 'GV_ERROR' = :gv_error;

            /* ORIGSQL: comDebugger('pm trace', 'err:' || gv_error) */
            comDebugger('pm trace', 'err:'|| IFNULL(:gv_error,''));

            rollback; 
          	resignal; 
        END;
       
  
    
    /*type PartName IS VARRAY(10) OF VARCHAR2(255);    vParName PartName;*/
   
	select tenantid into v_tenantid from cs_tenant;

SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
SELECT SESSION_CONTEXT('GV_PERIODNAME') INTO Gv_Periodname FROM SYS.DUMMY ;
SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
SELECT CAST(SESSION_CONTEXT('GV_PREPERIODSEQ1') AS BIGINT) INTO gv_prePeriodSeq1 FROM SYS.DUMMY ;
SELECT CAST(SESSION_CONTEXT('GV_PREPERIODSEQ2') AS BIGINT) INTO gv_prePeriodSeq2 FROM SYS.DUMMY ;
SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
        

  
	v_rtn := comGetQuarterMonth(i_periodSeq);

    if v_rtn < 1 then
      return;
    end if;

    --gv_prePeriodSeq2 :=  2533274790395933;
    --gv_prePeriodSeq2 := 2533274790395934;
    --2533274790395935

    ----clean up temp tables

    /*delete from sh_query_result
    where component='NADOR_VALIDATION';
    COMMIT;*/

/* 	comInitialPartition('NV', v_componentValue, i_periodSeq);*/

    Log('Start 4');

    execute immediate 'truncate table sh_sequence';

    Log('End  4');

    V_Credittypeseq := Comgetcredittypeseq('NADOR');
    --for revamp
    --begin
    V_Credittypeseq_w      := Comgetcredittypeseq('NADOR_W');
    V_Credittypeseq_w_dup  := Comgetcredittypeseq('NADOR_W_DUPLICATE');
    V_Credittypeseq_wc_dup := Comgetcredittypeseq('NADOR_WC_DUPLICATE');
    --end

    ---enable parallel server option
    --Maintenance.Enablepdml;

    Log('Start  5');
    --comDebugger('SQL Performance','Stagehook[SP_UPDATE_NADOR]-SQL1 START:'||SYSDATE);

/*
     vParName := partname(' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ');


    vParName(1) := GETSUBPARTITIONNAME('CS_CREDIT',v_periodSeq,gv_ProcessingUnitSeq) ;
    Log('   par name for ' || v_periodSeq || ' is ' || vParName(1));

    vParName(2) := GETSUBPARTITIONNAME('CS_CREDIT',gv_prePeriodSeq1,gv_ProcessingUnitSeq) ;
    Log('   par name for ' || gv_prePeriodSeq1 || ' is ' || vParName(2));

    vParName(3) := GETSUBPARTITIONNAME('CS_CREDIT',gv_prePeriodSeq2,gv_ProcessingUnitSeq) ;
    Log('   par name for ' || gv_prePeriodSeq2 || ' is ' || vParName(3));
*/

   /* for i in 1 .. 3 
    do

      Log('Start  5 Loop ' || i || ' vParName ' || vParName(i));
      vSQL := 'insert into  sh_query_result(Component,Objoutputname,Payeeseq,Positionseq,Periodseq,Value,genericSequence1,genericSequence2,genericnumber1,Genericnumber2,Genericsequence3,Genericsequence4,Genericboolean1,Genericnumber3,Genericattribute1,Genericboolean3)';
      vSQL := vSQL ||
              'select  ''' ||
              v_componentValue ||
              ''',''PM_NADOR_CM'',cc.payeeseq, cc.positionseq,' ||
              v_periodSeq ||
              ', sum(cc.value), cp.ruleelementownerseq,cp.payeeseq, ';
      --vSQL := vSQL || '   cc.genericnumber4 ,count(*) as recCnt, '; version 7 comment
      vSQL := vSQL || '   decode(cc.genericnumber6, 9999,cc.genericnumber4, cc.genericnumber6) ,count(*) as recCnt, ';
      --for revamp begin
      vSQL := vSQL || '  cc.creditTypeSeq, cc.periodSeq,';
      --end revamp
      vSQL := vSQL ||
              '0 As Validflag, Max(T.Thresholdamt),Cc.Genericattribute12, decode(cc.genericnumber6, 9999,0,1) ';
      vSQL := vSQL || 'From Cs_Credit ' || '  subpartition  (' || vParName(i) || ')' || ' cc, Cs_Position Cp,  Sh_Tmp_Threshold T ';
      --for revamp begin
      vSQL := vSQL || 'Where cp.tenantid=''AIAS'' and cc.creditTypeSeq in (' || v_creditTypeSeq || ',' ||
              v_creditTypeSeq_w || ',' || V_Credittypeseq_w_dup || ',' ||
              V_Credittypeseq_wc_dup || ')';
      --end revamp
      vSQL := vSQL || '   and ''SGT''||cc.genericAttribute12=cp.name ';
      vSQL := vSQL || '   and cp.removeDate=:cdt_EndOfTime';
      vSQL := vSQL || '   AND cp.effectiveStartDate<=cc.compensationDate ';
      vSQL := vSQL || '   and cp.effectiveEndDate>cc.compensationDate ';
      --vSQL := vSQL || '   and t.beginQuarter(+) <= cc.genericnumber4 '; version 7 comment
      --vSQL := vSQL || '   and t.endQuarter(+) >=  cc.genericnumber4 ';  version 7 comment
      vSQL := vSQL || '   and t.beginQuarter(+) <= decode(cc.genericnumber6, 9999,cc.genericnumber4, cc.genericnumber6) ';
      vSQL := vSQL || '   and t.endQuarter(+) >=  decode(cc.genericnumber6, 9999,cc.genericnumber4, cc.genericnumber6) ';

      vSQL := vSQL ||
              '  group by cc.payeeseq, cc.positionseq, cp.ruleElementOwnerSeq ,cp.payeeseq, decode(cc.genericnumber6, 9999,cc.genericnumber4, cc.genericnumber6),cc.creditTypeSeq,cc.periodSeq,cc.genericattribute12, decode(cc.genericnumber6, 9999,0,1) ';

      dbms_output.put_line('Running ' || vSQL);
      Log('Running ' || vSQL);
      begin
        execute immediate vSQL
          using cdt_EndOfTime;
      exception
        when others then
          Log('Error ' || sqlerrm);
      end;

      Log('End  5');

      commit;
    end loop;*/
   
   
-- above sql rewritten in hana 
   
insert into sh_query_result(Component,	Objoutputname,	Payeeseq,	Positionseq,	Periodseq,	Value,	genericSequence1,	genericSequence2,	genericnumber1,	Genericnumber2,	Genericsequence3,	Genericsequence4,	Genericboolean1,	Genericnumber3,	Genericattribute1,	Genericboolean3)
select
	v_componentValue,
	'PM_NADOR_CM',
	cc.payeeseq,
	cc.positionseq,
	v_periodSeq ,
	sum(cc.value),
	cp.ruleelementownerseq,
	cp.payeeseq,
	map(cc.genericnumber6,	9999,	cc.genericnumber4,	cc.genericnumber6) ,
	count(*) as recCnt,
	cc.creditTypeSeq,
	cc.periodSeq,
	0 as Validflag,
	Max(T.Thresholdamt),
	Cc.Genericattribute12,
	map(cc.genericnumber6,	9999,	0,	1)
	from
	Cs_Position Cp 
join Cs_Credit cc on ('SGT' || cc.genericAttribute12 = cp.name
	and cp.removeDate =:cdt_EndOfTime
	and cp.effectiveStartDate <= cc.compensationDate
	and cp.effectiveEndDate>cc.compensationDate)
left join Sh_Tmp_Threshold T on (t.beginQuarter <= map(cc.genericnumber6,	9999,	cc.genericnumber4,	cc.genericnumber6)
	and t.endQuarter >= map(cc.genericnumber6,	9999,	cc.genericnumber4,	cc.genericnumber6))
where cc.creditTypeSeq in (v_creditTypeSeq, v_creditTypeSeq_w, V_Credittypeseq_w_dup, V_Credittypeseq_wc_dup)
and cc.periodseq in (v_periodSeq,gv_prePeriodSeq1,gv_prePeriodSeq2)
and cc.processingunitseq = gv_ProcessingUnitSeq
group by
	cc.payeeseq,
	cc.positionseq,
	cp.ruleElementOwnerSeq ,
	cp.payeeseq,
	map(cc.genericnumber6,	9999,	cc.genericnumber4,	cc.genericnumber6),
	cc.creditTypeSeq,
	cc.periodSeq,
	cc.genericattribute12,
	map(cc.genericnumber6,	9999,	0,	1);
   
      Log('End  5');

    --version 7 update old agent position seqno for FA code

    Merge into sh_query_result R
    using (
           with tmp_position_info as
           (select cp.name as code_no,ruleelementownerseq,cd.enddate
              from cs_position cp,cs_period cd
             where cp.tenantid=cd.tenantid
               and cp.removeDate=cdt_endoftime
               and cd.removeDate=cdt_endoftime
               and cd.periodseq = v_periodSeq
               and cp.effectiveStartDate<=add_days(cd.enddate,-1)
               --version 8
               --and cp.effectiveEndDate>=cd.enddate-1
               and cp.effectiveEndDate>add_days(cd.enddate,-1)
            )  -- tmp_position_info
        select ce.payeeid as com_agt,
               cg.genericattribute5 as old_agt,
               t1.ruleelementownerseq as old_agt_positonNo
           from CS_PAYEE ce,
                Cs_Gaparticipant cg,
                tmp_position_info t1
        where  ce.payeeseq=cg.payeeseq
           and ce.islast=1
           and cg.pagenumber=0
           and cg.effectivestartdate>=ce.effectivestartdate
           and cg.effectiveenddate<=ce.effectiveenddate
           and ce.removedate=cdt_endoftime
           and cg.removedate=cdt_endoftime
           and cg.genericattribute5 is not null
       and ce.effectivestartdate<=add_days(t1.enddate,-1)
       --version 8
       --and ce.effectiveenddate>=t1.enddate-1
       and ce.effectiveenddate>add_days(t1.enddate,-1)
       and t1.code_no='SGT'||cg.genericattribute5) S
    on ('SGT'||R.Genericattribute1=S.com_agt and R.Genericboolean3=1 and R.Periodseq=v_periodSeq)
    When Matched Then
      update set R.genericSequence5=S.old_agt_positonNo;


   log('5a '|| ::ROWCOUNT);

   commit;


   --version 7 update old agency position seqno for FA code

   Merge into sh_query_result R
    using (
           with tmp_position_info as
           (select cp.name,genericattribute2,ruleelementownerseq,cd.enddate
              from cs_position cp,cs_period cd
             where  cp.tenantid=cd.tenantid
               and cp.removeDate=cdt_endoftime
               and cd.removeDate=cdt_endoftime
               and cd.periodseq = v_periodSeq
               and cp.effectiveStartDate<=add_days(cd.enddate,-1)
               --version 8
               --and cp.effectiveEndDate>=cd.enddate-1
               and cp.effectiveEndDate>add_days(cd.enddate,-1)
            )  -- tmp_position_info
        select ce.payeeid as Agy_Leader,
           t1.ruleelementownerseq as Agy_Positionseq,
               cg.genericattribute6 as old_agy,
               t2.ruleelementownerseq as old_agy_positonNo
         ---v20 Fix: (NADOR issue)
         , cg.genericattribute5 as old_agt_code
         , t2.genericattribute2 as old_agy_leader_code
         ---v20 Fix: (NADOR issue) End
           from CS_PAYEE ce,
                Cs_Gaparticipant cg,
                tmp_position_info t1,
        		tmp_position_info t2
        where  ce.payeeseq=cg.payeeseq
           and ce.islast=1
           and cg.pagenumber=0
           and cg.effectivestartdate>=ce.effectivestartdate
           and cg.effectiveenddate<=ce.effectiveenddate
           and ce.removedate=cdt_endoftime
           and cg.removedate=cdt_endoftime
       and cg.genericattribute5 is not null
           and cg.genericattribute6 is not null
       and ce.effectivestartdate<=add_days(t1.enddate,-1)
       --version 8
       --and ce.effectiveenddate>=t1.enddate-1
       and ce.effectiveenddate>add_days(t1.enddate,-1)
           and 'SGT'||t1.genericattribute2=ce.payeeid
       and t2.name='SGY'||cg.genericattribute6) S
    on (R.Positionseq=S.Agy_Positionseq and R.Genericboolean3=1 and R.Periodseq=v_periodSeq)
    When Matched Then
  ---v20 original (NADOR issue)
    ---  update set R.Genericboolean2 = S.old_agy_positonNo;
  ---v20 original (NADOR issue) End

  ---v20 Fix: (NADOR issue)
         update set R.Genericboolean2 = S.old_agy_positonNo,
                R.Genericboolean4 = S.old_agt_code, -- Participant EA5
          R.Genericboolean5 = S.old_agy_leader_code; -- Position leader code
  ---v20 Fix: (NADOR issue) End

   log('5b '||::ROWCOUNT);

   commit;


  --version 7 update new agent position seqno for AGY code,if exists


  Merge into sh_query_result R
    using (
           with tmp_position_info as
           (select cp.name as code_no,ruleelementownerseq,cd.enddate
              from cs_position cp,cs_period cd
	             where cp.tenantid=cd.tenantid
               and cp.removeDate=cdt_endoftime
               and cd.removeDate=cdt_endoftime
               and cd.periodseq = v_periodSeq
               and cp.effectiveStartDate<=add_days(cd.enddate,-1)
               --version 8
               --and cp.effectiveEndDate>=cd.enddate-1
               and cp.effectiveEndDate>add_days(cd.enddate,-1)
            )  -- tmp_position_info
        select ce.payeeid as AGY_agt,
               cg.genericattribute4 as FA_agt,
               t1.ruleelementownerseq as FA_agt_positonNo
           from CS_PAYEE ce,
                Cs_Gaparticipant cg,
                tmp_position_info t1
        where  ce.payeeseq=cg.payeeseq
           and ce.islast=1
           and cg.pagenumber=0
           and cg.effectivestartdate>=ce.effectivestartdate
           and cg.effectiveenddate<=ce.effectiveenddate
           and ce.removedate=cdt_endoftime
           and cg.removedate=cdt_endoftime
           and cg.genericattribute4 is not null
       and ce.effectivestartdate<=add_days(t1.enddate,-1)
       --version 8
       --and ce.effectiveenddate>=t1.enddate-1
       and ce.effectiveenddate>add_days(t1.enddate,-1)
           and t1.code_no='SGT'||cg.genericattribute4) S
    on ('SGT'||R.Genericattribute1=S.AGY_agt and R.Genericboolean3=0 and R.Periodseq=v_periodSeq)
    When Matched Then
      update set R.genericSequence5=S.FA_agt_positonNo;


   log('5c '||::ROWCOUNT);

   commit;



    select count(*)
      into v_rec
      from (select 1
              from sh_query_result
             Where Component = V_Componentvalue
               and periodSeq = gv_periodseq
               limit 1);

    if v_rec = 0 then
      comDebugger('nador', '0.no nador credit found');
      return;
    end if;

    Log('6');



    --merge into sh_query_result r
    --using (SELECT payeeSeq, positionSeq, sum(cm.value) as aggValue
    --         from cs_measurement cm
    --        where tenantid='AIAS' and cm.name in ('PM_FYP_Life', 'PM_FYP_PA')
    --          And Cm.Periodseq In
    --              (V_Periodseq, Gv_Preperiodseq1, Gv_Preperiodseq2)
    --          and cm.processingunitseq = gv_processingunitseq
    --        group by cm.payeeSeq, cm.positionSeq) cm
    --on (cm.payeeseq = r.genericSequence2
    --    and cm.positionseq = r.genericSequence1
    --    and R.Component = V_Componentvalue
    --    and r.periodSeq = gv_periodSeq)
    --when matched then
    --  update
    --     set r.genericNumber4  = cm.aggValue,
    --         r.genericBoolean1 = case
    --                               when cm.aggValue >= r.genericNumber3 then
    --                                1
    --                               else
    --                                0
    --                             end;
    --
    --commit;
    --Log('6');


    --version 7 update transfer agt old biz FYP

    merge into sh_query_result r
    using (SELECT payeeSeq, positionSeq, sum(cm.value) as aggValue
             from cs_measurement cm
            where cm.name in ('PM_FYP_Life', 'PM_FYP_PA','PM_FYP_TPAH_COMP')-- version 24 For MAS Section86 project Nador
              And Cm.Periodseq in (V_Periodseq, Gv_Preperiodseq1, Gv_Preperiodseq2)
              and cm.processingunitseq = gv_processingunitseq
            group by cm.payeeSeq, cm.positionSeq) cm
    on ( cm.positionseq= r.genericSequence5 --old agent position seq
        and R.Component = V_Componentvalue
        and r.periodSeq = gv_periodSeq)
    when matched then
      update
         set r.genericNumber4  = cm.aggValue,
             r.genericBoolean1 = case
                                   when cm.aggValue >= r.genericNumber3 then
                                    1
                                   else
                                    0
                                 end;

    commit;

   Log('6a');

    merge into sh_query_result r
    using (SELECT payeeSeq, positionSeq, sum(cm.value) as aggValue
             from cs_measurement cm
            where cm.name in ('PM_FYP_Life', 'PM_FYP_PA','PM_FYP_TPAH_COMP')-- version 24 For MAS Section86 project Nador
              And Cm.Periodseq in (V_Periodseq, Gv_Preperiodseq1, Gv_Preperiodseq2)
              and cm.processingunitseq = gv_processingunitseq
            group by cm.payeeSeq, cm.positionSeq) cm
    on (cm.payeeseq = r.genericSequence2
        and cm.positionseq = r.genericSequence1
        and R.Component = V_Componentvalue
        and r.periodSeq = gv_periodSeq)
    when matched then
      update
         set r.genericNumber4  = map(r.genericNumber4,null,0,r.genericNumber4)+cm.aggValue,           --version 7 new+old biz value summary
             r.genericBoolean1 = case
                                   when map(r.genericNumber4,null,0,r.genericNumber4)+cm.aggValue >= r.genericNumber3 then
                                    1
                                   else
                                    0
                                 end;

    commit;
    Log('6');

    Log('7');


    insert into sh_sequence
     select DISTINCT cc.creditSeq,
               'CREDITSEQ',
               --cc.payeeseq,
         -- v20 start
         -- r.Genericboolean2,  --old agency code -- v20 original
               (case when (r.Genericboolean4 = r.Genericboolean5) then r.Genericboolean2
             else r.Positionseq
             end),
         -- v20 end
               cc.positionseq,
               r.periodSeq
        from cs_credit cc, sh_query_result r
       where cc.payeeseq = r.payeeseq
         and cc.positionseq = r.positionseq
         and cc.periodSeq = r.genericSequence4
            --and cc.periodSeq in (v_periodSeq,gv_prePeriodSeq1,gv_prePeriodSeq2) -- the period contain entire quarter
         and cc.creditTypeSeq = r.genericSequence3
         and cc.genericattribute12 = r.genericattribute1
         And R.Component = V_Componentvalue
         and r.periodseq = gv_periodseq
         and r.genericBoolean1 = 1
         and cc.processingUnitseq = gv_processingUnitSeq
         and cc.origintypeid in ('calculated', 'imported')
         --and cc.genericnumber4 < 13;
         and map(cc.genericnumber6, 9999,cc.genericnumber4, cc.genericnumber6) <13;

    commit;
   
    Log('7');


    ---------------------update eligible NADOR CREDIT

    Log('8');


    execute immediate 'Update Cs_Credit t
       Set Genericboolean3 = 1
     Where Processingunitseq = '||Gv_Processingunitseq||'
       and t.Creditseq in (Select Sh.Businessseq
                             From Sh_Sequence Sh
                            Where Sh.Seqtype = ''CREDITSEQ'')';

    commit;
    Log('8');

    ----DBMS_OUTPUT.put_line('nador credit updated');

    ------INSERT PMSEQ to sequence table

    Log('9');

    insert into sh_sequence
    select 
      DISTINCT cm.measurementSeq,
               'PMSEQ',
               cm.payeeseq,
               cm.positionSeq,
               cm.periodSeq
      from cs_measurement cm, sh_query_result r
     where 1=1
       --version 7
       --and cm.payeeseq = r.payeeseq
       --and cm.positionseq = r.positionseq
       and (  (cm.payeeseq = r.payeeseq and cm.positionseq = r.positionseq  and r.Genericboolean3=0)
       -- v20 start
       -- or (cm.positionseq=r.Genericboolean2 and r.Genericboolean3=1) -- v20 original
           or (cm.payeeseq = r.payeeseq and cm.positionseq = r.positionseq  and r.Genericboolean3=1 and r.Genericboolean4 <> r.Genericboolean5)
           or (cm.positionseq=r.Genericboolean2 and r.Genericboolean3=1 and r.Genericboolean4 = r.Genericboolean5)
       -- v20 end
           )
       and cm.periodSeq = r.periodSeq
       and cm.name = r.objOutputName
       And R.Component = V_Componentvalue
       and r.periodseq = gv_periodSeq
       and r.genericBoolean1 = 1
       and cm.processingUnitseq = gv_processingUnitSeq;

    commit;
    Log('9');

    --comDebugger('nador','5.get pm seq');

    ---update PM

    Log('10');

    execute immediate 'merge into cs_measurement cm
    using (SELECT s.payeeSeq, --r.payeeSeq,
                  s.positionSeq, --r.positionSeq,
                  r.periodSeq,
                  r.objOutputName,
                  r.component,
                  sum(r.value) as aggValue,
                  sum(r.genericnumber2) as aggRecCnt,
                  s.businessSeq
             from sh_query_result r, sh_sequence s
            Where Component = '''||V_Componentvalue||'''
              and r.periodseq = '||gv_periodseq||'
              and genericBoolean1 = 1
              --version 7
              --and s.payeeSeq = r.payeeSeq
              --and s.positionSeq = r.positionSeq
              and ((s.payeeSeq = r.payeeSeq and s.positionSeq = r.positionSeq and r.Genericboolean3=0)
            -- v20 start
          -- or ( s.positionseq=r.Genericboolean2 and r.Genericboolean3=1) -- v20 original
          or (s.payeeSeq = r.payeeSeq and s.positionSeq = r.positionSeq and r.Genericboolean3=1 and r.Genericboolean4 <> r.Genericboolean5)
                  or (s.positionseq=r.Genericboolean2 and r.Genericboolean3=1 and r.Genericboolean4 = r.Genericboolean5)
          -- v20 end
                  )
              and s.periodSeq = r.periodSeq
              and s.seqType = ''PMSEQ''
            group by s.payeeSeq,    --r.payeeSeq,
                     s.positionSeq, --r.positionSeq,
                     r.periodSeq,
                     r.objOutputName,
                     r.component,
                     s.businessSeq) t
    on (t.businessSeq = cm.measurementSeq)
    when matched then
      update set cm.value = t.aggValue, cm.numberOfCredits = t.aggRecCnt';

    commit;
    Log('10');


    Log('11');
        --version 7 create PMCredit Trace

    execute immediate 'delete from cs_PMCreditTrace t
     where exists (select 1
              from sh_sequence scm
             where 1 = 1
               and scm.seqType = ''PMSEQ''
               and t.measurementSeq = scm.businessSeq)';

    Commit;
    Log('11');

    execute immediate 'insert into cs_pmCreditTrace
      (Tenantid,
       creditSeq,
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
     select  '''||v_tenantid||''',
             creditSeq,
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
        from (select
                 ROW_NUMBER() OVER (ORDER BY 0*0) as rn,
                 cc.creditSeq,
                 cm.measurementSeq,
                 cm.ruleSeq,
                 (Select Max(Pipelinerunseq) From Cs_Pipelinerun
                   Where Periodseq = Cc.Periodseq
                      and command = ''PipelineRun'') as pipelineRunSeq,
                  cm.periodSeq as targetPeriodSeq,
                  cc.periodSeq as sourcePeriodSeq,
                  cc.originTypeId as sourceOriginTypeId,
                  cc.value as contributionValue,
                  cc.unitTypeForValue as unitTypeForContributionValue,
                  cc.businessUnitMap,
                  cc.processingunitseq
                from cs_credit cc, cs_measurement cm,
                (select scc.businessSeq as ccseq,
                            scc.periodseq   as pseq,
                            scm.businessSeq as pmseq
                             from sh_sequence scc, sh_sequence scm
                            where scc.seqtype = ''CREDITSEQ''
                              and scm.seqtype = ''PMSEQ''
                              and (scc.positionseq = scm.positionseq
                                 OR scc.payeeseq = scm.positionseq) --PM credit old agency code
                              and scc.periodSeq = scm.periodSeq) csp
                 Where Cc.Processingunitseq = Gv_Processingunitseq
                   and Cm.Processingunitseq = Gv_Processingunitseq
                   And Cc.Origintypeid In (''calculated'', ''imported'')
                   and csp.pmseq = cm.measurementSeq
                   and csp.ccseq = cc.creditseq) t';


    commit;

--    comUpdPMCreditTrace('SP_UPDATE_NADOR');
    Log('11');

  end --SP_UPDATE_NADOR;