CREATE  procedure SP_TXA_NADOR_EXCEPTION(in i_periodSeq bigint) 
 sql security invoker 
 as
  begin
	  
	DECLARE gv_error VARCHAR(1000); /* package/session variable */
    DECLARE Gv_Setnumbernador BIGINT; /* package/session variable */
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE V_Periodenddate TIMESTAMP;  /* ORIGSQL: V_Periodenddate Date; */
    DECLARE V_Componentvalue VARCHAR(30) = 'NADOR_EXCEPTION';  /* ORIGSQL: V_Componentvalue Varchar2(30) := 'NADOR_EXCEPTION'; */
    DECLARE V_Fyceventtypeseq BIGINT;  /* ORIGSQL: V_Fyceventtypeseq Int; */
    DECLARE V_Sscpeventtypeseq BIGINT;  /* ORIGSQL: V_Sscpeventtypeseq Int; */
    DECLARE V_FYCTPGIEventtypeSeq BIGINT;  /* ORIGSQL: V_FYCTPGIEventtypeSeq Int; */

    -- version 24 add FY_TPGI for MAS86
    DECLARE vstartdate TIMESTAMP;  /* ORIGSQL: vstartdate date; */
    DECLARE venddate TIMESTAMP;  /* ORIGSQL: venddate date; */
    declare v_tenantid varchar(20) = gettenantid();

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [SP_TXA_NADOR_EXCEPTION]: ' || ::SQL_ERROR_MESSAGE; --|| ' - ' ||  /* ORIGSQL: sqlerrm */

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            /*dbms_utility.format_error_backtrace;*/
            /* Saving modified package/session variable 'gv_error': */ 
            SET SESSION 'DBMTK_GLOBVAR_DBMTK_USER_NAME_PK_STAGE_HOOK_GV_ERROR' = :gv_error;
            /* ORIGSQL: comDebugger('SP_TXA_NADOR_EXCEPTION DEBUGGER', 'ERROR' || gv_error) */
            comDebugger('SP_TXA_NADOR_EXCEPTION DEBUGGER', 'ERROR'|| IFNULL(:gv_error,''));
            ROLLBACK;
        END;

       SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_SETNUMBERNADOR') AS BIGINT) INTO Gv_Setnumbernador FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
        /* end of package/session variables */


    select startDate, endDate
      into v_periodStartDate, v_periodEndDate
      from cs_period
     where periodSeq = i_periodSeq;

    ---clean up temp table
    /*delete from sh_query_result
    where component='NADOR EXCEPTION';
    commit;*/

/*	comInitialPartition('NE', v_componentValue, i_periodSeq);*/

    V_Fyceventtypeseq  := Comgeteventtypeseq('FYC');
    v_SSCPEventtypeSeq := Comgeteventtypeseq('SSCP');
    -- version 24 add FY_TPGI for MAS86
    V_FYCTPGIEventtypeSeq := Comgeteventtypeseq('FYC_TPGI');

    comDebugger('SQL Performance','Stagehook[SP_TXA_NADOR_EXCEPTION]-SQL1 START:' || current_date);

    --identify the vaild data
    --DO NOT check the assignment position from nador exception table is new or old position in the query, because the assignment never reset

    Log('25');

    insert 
    into sh_query_result
      (component,
       periodseq,
       genericSequence1,
       genericSequence2,
       genericNumber1,
       genericDate1,
       genericAttribute1,
       genericAttribute2,
       genericAttribute3)
      Select distinct V_Componentvalue,
                      i_periodSeq,
                      cs.salestransactionSeq,
                      Cs.Salesorderseq,
                      Gv_Setnumbernador, --hard code assignment set number as 3
                      cs.compensationDate,
                      nd.txt_payee_agy,
                      nd.txt_payee_agt,
                      Cs.Businessunitmap
        from cs_salestransaction cs, in_nador_payee_setup nd
       where cs.tenantid='AIAS' and cs.processingunitseq = gv_processingunitseq
         and nd.dec_status = 1
         and nd.txt_payor_agy is not null
         and nd.txt_payor_agt is not null
         and nd.txt_payee_agy is not null
         and nd.txt_payee_agt is not null
         and nd.txt_payor_agy = cs.genericAttribute13
         and substr(nd.txt_payor_agt, 6) = cs.genericAttribute12
         and nd.dte_cycle >= v_periodStartDate
         and nd.dte_cycle < v_periodEndDate
         And Cs.Compensationdate >= V_Periodstartdate
         And Cs.Compensationdate < V_Periodenddate
         And Cs.Eventtypeseq in (V_Fyceventtypeseq, V_SSCPeventtypeseq, V_FYCTPGIEventtypeSeq); -- version 24
         --and cs.genericdate2 < to_date('12/1/2015', 'mm/dd/yyyy');


    Log('25 '||::ROWCOUNT);
commit;





    --delete the assignment ONLY has NADOR
    --txta deletion is happened in comCleanAssignment

    /*insert \*+APPEND*\
    into Sh_Query_Result
      (component,
       periodseq,
       genericSequence1,
       genericSequence2,
       genericNumber1,
       genericDate1,
       genericAttribute1,
       genericAttribute2,
       genericAttribute3,
       genericattribute11,
       genericattribute12,
       genericattribute13,
       genericattribute14,
       genericattribute15,
       genericattribute16,
       genericsequence4,
       genericnumber3,
       genericsequence5,
       genericnumber5)
      Select distinct V_Componentvalue,
                      i_periodSeq,
                      cs.salestransactionSeq,
                      Cs.Salesorderseq,
                      Gv_Setnumbernador, --hard code assignment set number as 5
                      cs.compensationDate,
                      nd.txt_payee_agy,
                      nd.txt_payee_agt,
                      Cs.Businessunitmap,
                      ts.genericattribute11,
                      ts.genericattribute12,
                      ts.genericattribute13,
                      substr(nd.txt_payee_agt, 6) as genericattribute14,
                      ts.genericattribute15,
                      ts.genericattribute16,
                      ts.unittypeforgenericnumber1 as genericsequence4,
                      ts.genericnumber2 as genericnumber3,
                      ts.unittypeforgenericnumber2 as genericsequence5,
                      ts.genericnumber1 as genericnumber5
        from
             cs_transactionassignment ts
        join in_nador_payee_setup nd
          on 'SGY' || nd.txt_payor_agy = ts.positionname
        join cs_salestransaction cs
          on ts.salestransactionseq = cs.salestransactionseq
         and cs.processingunitseq = gv_processingunitseq
         and nd.dec_status = 1
         and nd.txt_payor_agy is not null
         and nd.txt_payor_agt is not null
         and nd.txt_payee_agy is not null
         and nd.txt_payee_agt is not null
         and substr(nd.txt_payor_agt, 6) = ts.genericAttribute14
         and cs.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')
         and ((cs.productname In ('PA', 'HS', 'LF') and
             ts.genericattribute13 In ('W', 'WC')) or
             (cs.productname In ('CS', 'CL') and
             ts.genericattribute13 = 'C'))
         and nd.dte_cycle >= v_periodStartDate
         and nd.dte_cycle < v_periodEndDate
         And Cs.Compensationdate >= V_Periodstartdate
         And Cs.Compensationdate < V_Periodenddate
         And Cs.Eventtypeseq in (V_Fyceventtypeseq, V_SSCPeventtypeseq)
         where ts.tenantid=v_tenantid
           and cs.tenantid='AIAS'
           and ts.processingunitseq = gv_processingunitseq
           and cs.processingunitseq = gv_processingunitseq;



    Log('26 '||SQL%ROWCOUNT);
  commit;




    --for revamp begin
    update sh_query_result a
       set a.Genericnumber1 = Gv_Setnumbernador1
     where exists (select 1
              from (select genericSequence1,
                           genericSequence2,
                           genericAttribute1,
                           genericAttribute2
                      from (select t.genericSequence1,
                                   t.genericSequence2,
                                   t.Genericnumber1,
                                   t.genericAttribute1,
                                   t.genericAttribute2,
                                   row_number() over(partition by t.genericSequence1, t.genericSequence2, t.Genericnumber1 order by t.genericAttribute1, t.genericAttribute2) cnt
                              from sh_query_result t
                             Where t.Component = V_Componentvalue
                               and t.periodSeq = i_periodSeq)
                     where cnt = 2) b
             where a.genericSequence1 = b.genericSequence1
               and a.genericSequence2 = b.genericSequence2
               and a.genericAttribute1 = b.genericAttribute1
               and a.genericAttribute2 = b.genericAttribute2)
       and a.Component = V_Componentvalue
       and a.periodSeq = i_periodSeq;
Log('26a '||SQL%ROWCOUNT);
    commit;*/
    --for revamp end




    --reset assignment which contain NADOR and AOR asignment
    Update Cs_Transactionassignment
       set genericAttribute4 = substr(genericAttribute4, 7)
     where tenantid=v_tenantid and processingUnitSeq=gv_processingunitseq
       and compensationdate>=vstartdate and compensationdate<=venddate
       and (salestransactionSeq,salesorderSeq) in  (select genericSequence1 ,genericSequence2
              from sh_query_result
             where Component = v_componentValue
               and periodSeq = gv_periodseq)
       And Genericattribute4 like 'NADOR%';


    Log('26b '||::ROWCOUNT);

    commit;

--Added by Suresh 20180726
execute immediate 'truncate table tmp_sh_query_result_1';

insert into tmp_sh_query_result_1
  Select T1.Genericsequence1 as Salestransactionseq,
         T1.genericSequence2 as salesorderSeq,
         T1.Genericnumber1 as Setnumber,
         T1.Genericdate1 as Compensationdate,
         'SGY' || T1.Genericattribute1 as Positionname,
         null as payeeid,
         'NADOR' as genericAttribute4,
         T1.Genericattribute3 as Businessunitmap,
         T1.genericattribute11,
         T1.genericattribute12,
         T1.genericattribute13,
         T1.genericattribute14, ----agt
         T1.genericattribute15,
         T1.genericattribute16,
         T1.genericnumber5 as genericnumber1,
         T1.genericsequence4 as unittypeforgenericnumber1,
         T1.genericnumber3 as genericnumber2,
         T1.genericsequence5 as unittypeforgenericnumber2,
         row_number() over(partition by T1.Genericsequence1, 'SGY' || T1.Genericattribute1 order by T1.Genericnumber1) cnt
    From Sh_Query_Result T1
   Where 1 = 1
     And T1.Component = V_Componentvalue
     And t1.periodseq = gv_periodSeq;
commit;
--end Suresh 20180726

    Log('27');

       --for revamp begin
    Merge Into cs_transactionassignment Ta
    Using (
    /*commented by Suresh 20180726
      with tmp_sh_query_result as
       (Select T1.Genericsequence1 as Salestransactionseq,
               T1.genericSequence2 as salesorderSeq,
               T1.Genericnumber1 as Setnumber,
               T1.Genericdate1 as Compensationdate,
               'SGY' || T1.Genericattribute1 as Positionname,
               null as payeeid,
               'NADOR' as genericAttribute4,
               T1.Genericattribute3 as Businessunitmap,
               T1.genericattribute11,
               T1.genericattribute12,
               T1.genericattribute13,
               T1.genericattribute14, ----agt
               T1.genericattribute15,
               T1.genericattribute16,
               T1.genericnumber5 as genericnumber1,
               T1.genericsequence4 as unittypeforgenericnumber1,
               T1.genericnumber3 as genericnumber2,
               T1.genericsequence5 as unittypeforgenericnumber2,
               row_number() over(partition by T1.Genericsequence1, 'SGY' || T1.Genericattribute1 order by T1.Genericnumber1) cnt
          From Sh_Query_Result T1
         Where 1 = 1
           And T1.Component = V_Componentvalue
           And t1.periodseq = gv_periodSeq) ----tmp_sh_query_result
           */
      select a.Salestransactionseq,
             a.salesorderSeq,
             a.Setnumber,
             a.Compensationdate,
             a.Positionname,
             a.payeeid,
             a.genericAttribute4,
             a.Businessunitmap,
             a.genericattribute11,
             a.genericattribute12,
             a.genericattribute13,
             a.genericattribute14, ----agt
             ifnull(b.genericattribute11, a.genericattribute15) as genericattribute15,
             ifnull(b.genericattribute14, a.genericattribute16) as genericattribute16,
             a.genericnumber1,
             a.unittypeforgenericnumber1,
             ifnull(b.genericnumber1, a.genericnumber2) as genericnumber2,
             ifnull(b.unittypeforgenericnumber1, a.unittypeforgenericnumber2) as unittypeforgenericnumber2
        /*commented by Suresh 20180726
        from (select * from tmp_sh_query_result where cnt = 1) a
        left join (select * from tmp_sh_query_result where cnt = 2) b*/
        from (select * from tmp_sh_query_result_1 where cnt = 1) a
        left join (select * from tmp_sh_query_result_1 where cnt = 2) b
          on a.salestransactionseq = b.salestransactionseq
       ) R On (Ta.Salestransactionseq = R.Salestransactionseq and ta.positionName = r.Positionname) 
       When Matched Then
        Update --why need to merge ta.ga4 with r.ga4, beacasue need to accommodate with piaor stagehook
           Set Ta.Genericattribute4 = r.Genericattribute4 ||
                                      map(Ta.Genericattribute4,
                                             Null,
                                             '',
                                             '_') ||
                                      ifnull(Ta.Genericattribute4, '')
       When NOT MATCHED then 
        insert 
          (tenantid,
           Salestransactionseq,
           Salesorderseq,
           Setnumber,
           Compensationdate,
           Positionname,
           Payeeid,
           Genericattribute4,
           genericattribute11,
           genericattribute12,
           genericattribute13,
           genericattribute14,
           genericattribute15,
           genericattribute16,
           genericnumber1,
           unittypeforgenericnumber1,
           genericnumber2,
           unittypeforgenericnumber2,
           processingUnitSeq)
        Values
          (v_tenantid, R.Salestransactionseq,
           R.Salesorderseq,
           R.Setnumber,
           R.Compensationdate,
           R.Positionname,
           R.Payeeid,
           R.Genericattribute4,
           R.genericattribute11,
           R.genericattribute12,
           R.genericattribute13,
           R.genericattribute14,
           R.genericattribute15,
           R.genericattribute16,
           R.genericnumber1,
           R.unittypeforgenericnumber1,
           R.genericnumber2,
           R.unittypeforgenericnumber2,
           gv_processingunitseq);


    Log('27 '||::ROWCOUNT);
    commit;



    Comdebugger('SQL Performance','Stagehook[SP_TXA_NADOR_EXCEPTION]-SQL5 START:' || current_date);

    --update txn EB2

    update cs_gaSalestransaction
       set genericBoolean2 = 1
     where salestransactionSeq in
           (select genericSequence1
              from sh_query_result
             Where Component = V_Componentvalue
               and periodSeq = gv_periodseq
               ) AND PAGENUMBER = 0;

    commit;
    Log('28');



  end --SP_TXA_NADOR_EXCEPTION;
