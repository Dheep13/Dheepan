CREATE procedure SP_TXA_OVERRIDING_ASSIGNMENT(in i_periodSeq bigint) 
sql security invoker 
as

   

  begin
	  
	DECLARE gv_error VARCHAR(1000); /* package/session variable */
    DECLARE Gv_Setnumberordist BIGINT; /* package/session variable */
    DECLARE Gv_Setnumberoragy BIGINT; /* package/session variable */
    DECLARE Gv_Setnumberoragyanddist BIGINT; /* package/session variable */
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE V_Periodenddate TIMESTAMP;  /* ORIGSQL: V_Periodenddate Date; */
    DECLARE V_Componentvalue VARCHAR(30) = 'OVERRIDING_ASSIGNMENT';  /* ORIGSQL: V_Componentvalue Varchar2(30) := 'OVERRIDING_ASSIGNMENT'; */
    DECLARE V_Fyceventtypeseq BIGINT;  /* ORIGSQL: V_Fyceventtypeseq Int; */
    DECLARE V_Ryceventtypeseq BIGINT;  /* ORIGSQL: V_Ryceventtypeseq Int; */
    DECLARE V_ORyceventtypeseq BIGINT;  /* ORIGSQL: V_ORyceventtypeseq Int; */
    DECLARE V_Sscpeventtypeseq BIGINT;  /* ORIGSQL: V_Sscpeventtypeseq Int; */
    DECLARE V_Apieventtypeseq BIGINT;  /* ORIGSQL: V_Apieventtypeseq Int; */
    declare v_tenantid varchar(20)= gettenantid();

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [SP_OVERRIDING_ASSIGNMENT]: ' || ::SQL_ERROR_MESSAGE;-- || ' - ' ||  /* ORIGSQL: sqlerrm */

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            /*dbms_utility.format_error_backtrace;*/
            /* Saving modified package/session variable 'gv_error': */ 
            SET SESSION 'DBMTK_GLOBVAR_DBMTK_USER_NAME_PK_STAGE_HOOK_GV_ERROR' = :gv_error;
            ROLLBACK;
            comDebugger('SP_TXA_OVERRIDING_ASSIGNMENT', :gv_error);
            COMMIT;
        END;

        /* retrieve the package/session variables referenced in this procedure */
        SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_SETNUMBERORDIST') AS BIGINT) INTO Gv_Setnumberordist FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_SETNUMBERORAGY') AS BIGINT) INTO Gv_Setnumberoragy FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_SETNUMBERORAGYANDDIST') AS BIGINT) INTO Gv_Setnumberoragyanddist FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
        /* end of package/session variables */
	  

    select startDate, endDate
      into v_periodStartDate, v_periodEndDate
      from cs_period
     where periodSeq = i_periodSeq;



/*
    comInitialPartition('OA', v_componentValue, i_periodSeq);
*/

    V_Fyceventtypeseq  := Comgeteventtypeseq('FYC');
    V_Ryceventtypeseq  := Comgeteventtypeseq('RYC');
    V_ORyceventtypeseq  := Comgeteventtypeseq('ORYC');
    v_SSCPEventtypeSeq := Comgeteventtypeseq('SSCP');

    comDebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL1 START:' ||
                current_date);

    --identify the vaild data
    --Revised by Win Tan at 20151119
    --begin
    Execute Immediate 'TRUNCATE TABLE temp_override_assign';

    insert into temp_override_assign
      Select t.to_district      as to_unit, -- If to district is not null, than generate an assignment for this district
             t.to_agent,
             t.original_unit,
             t.effective_from,
             t.effective_to,
             t.policy_no,
             t.comp_code,
             Gv_Setnumberordist as setNumber
        from in_override_assign t
       where t.to_district is not null
         and t.to_district <> ifnull(t.to_agency, 'aa')
      union all
      Select t2.to_agency      as to_unit, -- If to agency is not null, than generate an assignment for this agency
             t2.to_agent,
             t2.original_unit,
             t2.effective_from,
             t2.effective_to,
             t2.policy_no,
             t2.comp_code,
             Gv_Setnumberoragy as setNumber
        from in_override_assign t2
       where t2.to_agency is not null
         and t2.to_agency <> ifnull(t2.to_district, 'aa')
      union all
      Select t3.to_agency             as to_unit, -- If to agency = to district, than generate only one assignment for this district
             t3.to_agent,
             t3.original_unit,
             t3.effective_from,
             t3.effective_to,
             t3.policy_no,
             t3.comp_code,
             Gv_Setnumberoragyanddist as setNumber
        from in_override_assign t3
       where t3.to_agency is not null
         and t3.to_district is not null
         and t3.to_district = t3.to_agency;

         log('SP_TXA_OVERRIDING_ASSIGNMENT 1 '||::ROWCOUNT);
    commit;
    --end

    insert into sh_query_result
      (component,
       periodseq,
       genericSequence1,
       genericSequence2,
       genericNumber1,
       genericDate1,
       genericAttribute1,
       genericAttribute2,
       genericAttribute3,
       genericAttribute11,
       genericAttribute12,
       genericAttribute13,
       genericAttribute14,
       genericNumber2,
       genericAttribute15,
       genericAttribute16,
       genericNumber3,
       genericSequence4,
       genericSequence5)
      Select V_Componentvalue,
             i_periodSeq,
             cs.salestransactionSeq,
             Cs.Salesorderseq,
             os.setNumber,
             cs.compensationDate,
             os.to_unit,
             os.to_agent,
             Cs.Businessunitmap,
             ts.genericattribute11,
             ts.genericattribute12,
             ts.genericattribute13,
             ts.genericattribute14,
             ts.genericnumber1,
             ts.genericattribute15,
             ts.genericattribute16,
             ts.genericnumber2,
             ts.unittypeforgenericnumber1,
             ts.unittypeforgenericnumber2
        from cs_transactionassignment ts
       inner join cs_salestransaction cs
          on ts.salestransactionseq = cs.salestransactionseq
       inner join temp_override_assign os
          on os.policy_no = cs.ponumber
         and os.comp_code = cs.productid
         and cs.compensationdate >= os.effective_from
         and cs.compensationdate < os.effective_to
       where cs.tenantid=v_tenantid and ts.tenantid=v_tenantid
         and cs.processingunitseq = gv_processingunitseq
         and ts.processingunitseq = gv_processingunitseq
         and 'SGY' || os.original_unit = ts.positionname
         and cs.compensationdate >= v_periodStartDate
         and cs.compensationdate < v_periodEndDate
         and ts.compensationdate >= v_periodStartDate
         and ts.compensationdate < v_periodEndDate
         and cs.eventtypeseq in
             (V_Fyceventtypeseq, V_Ryceventtypeseq,V_ORyceventtypeseq, v_SSCPEventtypeSeq)
         and cs.genericdate2 >= to_date('12/1/2015', 'mm/dd/yyyy')
         and ((cs.productname In ('PA', 'HS', 'LF') and
             ts.genericattribute13 In ('W', 'WC')) or
             (cs.productname In ('CS', 'CL') and
             ts.genericattribute13 = 'C'))
         and (ts.genericattribute4 is null -- do not overlap with PI, APR, NADOR transaction assignment updates
             or (ts.genericattribute4 not like '%NADOR%' and
             ts.genericattribute4 not like '%AOR%' and
             ts.genericattribute4 not like '%PI%'));

             log('SP_TXA_OVERRIDING_ASSIGNMENT 2 '||::ROWCOUNT);
    comDebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL1 END:' || current_date ||
                ' - ReusltNumber' || ::ROWCOUNT);
    commit;

    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL2 START:' ||
                current_date);

    Merge
    Into Cs_Transactionassignment Ta
    Using (Select distinct T1.Genericsequence1 As Salestransactionseq,
                  t1.genericSequence2 as salesorderSeq,
                  T1.Genericnumber1 As Setnumber,
                  t1.Genericdate1 As Compensationdate,
                  'SGY' || T1.Genericattribute1 As Positionname,
                  null as payeeid,
                  'ORASGMT' as genericAttribute4,
                  --T1.Genericattribute3 As Businessunitmap,
                  T1.genericattribute11 As ClassCodeIssueDate,
                  T1.genericattribute12 As LeaderTitleIssueDate,
                  T1.genericattribute13 As AgencyIndicator,
                  T1.genericattribute14 As WritingAgent,
                  T1.genericnumber2     As PercentSpilt,
                  T1.genericattribute15 As ClassCodeIssueDate2,
                  T1.genericattribute16 As WritingAgent2,
                  T1.genericnumber3     As PercentSpilt2,
                  T1.Genericsequence4   As unittypeforgenericnumber1,
                  T1.Genericsequence5   As unittypeforgenericnumber2,
                  gv_processingunitseq  as processingUnitSeq
             From sh_query_result T1
            Where 1 = 1
              And T1.Component = V_Componentvalue
              and t1.periodseq = gv_periodSeq) R
    On (Ta.Salestransactionseq = R.Salestransactionseq and ta.positionName = r.Positionname)
    When not Matched Then
      Insert
        (Salestransactionseq,
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
         Genericnumber1,
         genericattribute15,
         genericattribute16,
         Genericnumber2,
         unittypeforgenericnumber1,
         unittypeforgenericnumber2,
         PROCESSINGUNITSEQ)
      Values
        (R.Salestransactionseq,
         R.Salesorderseq,
         R.Setnumber,
         R.Compensationdate,
         R.Positionname,
         R.Payeeid,
         R.Genericattribute4,
         R.ClassCodeIssueDate,
         R.LeaderTitleIssueDate,
         R.AgencyIndicator,
         R.WritingAgent,
         R.PercentSpilt,
         R.ClassCodeIssueDate2,
         R.WritingAgent2,
         R.PercentSpilt2,
         R.unittypeforgenericnumber1,
         R.unittypeforgenericnumber2,
                  R.PROCESSINGUNITSEQ);


                  log('SP_TXA_OVERRIDING_ASSIGNMENT 3 '||::ROWCOUNT);
    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL2 END:' || current_date ||
                ' - ReusltNumber' || ::ROWCOUNT);
    commit;

    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL3 START:' ||
                current_date);

    --update txn EB5
    update cs_gaSalestransaction
       set genericBoolean5 = 1
     where salestransactionSeq in
           (select r.genericSequence1
              from sh_query_result r
             Where r.Component = V_Componentvalue
               and r.periodSeq = gv_periodseq
           )                AND PAGENUMBER = 0;

    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL3 END:' || current_date ||
                ' - ReusltNumber' || ::ROWCOUNT);

                log('SP_TXA_OVERRIDING_ASSIGNMENT 4 '||::ROWCOUNT);
    commit;

    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL4 START:' ||
                current_date);

    --update txn EA1, EA2

    update  cs_gaSalestransaction ga
       set ga.genericAttribute1 =
           (select 'SGY' || max(r.genericattribute1)
              from sh_query_result r
             where r.Component = V_Componentvalue
               and r.periodSeq = gv_periodseq
               and r.genericnumber1 in
                   (Gv_Setnumberoragy, Gv_Setnumberoragyanddist)
               and ga.salestransactionseq = r.genericsequence1
               and ga.PAGENUMBER = 0
               /*and rownum = 1*/)
     where ga.PAGENUMBER = 0 and exists (select 1
              from sh_query_result sh
             where sh.genericSequence1 = ga.salestransactionSeq
               and sh.genericnumber1 in
                   (Gv_Setnumberoragy, Gv_Setnumberoragyanddist)
               and sh.Component = v_componentValue
               and sh.periodSeq = gv_periodseq);


               log('SP_TXA_OVERRIDING_ASSIGNMENT 5 '||::ROWCOUNT);
    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL4 END:' || current_date ||
                ' - ReusltNumber' || ::ROWCOUNT);

    commit;

    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL5 START:' ||
                current_date);

    update  cs_gaSalestransaction ga
       set ga.genericAttribute2 =
           (select 'SGY' || max(r.genericattribute1)
              from sh_query_result r
             where r.Component = V_Componentvalue
               and r.periodSeq = gv_periodseq
               and r.genericnumber1 in
                   (Gv_Setnumberordist, Gv_Setnumberoragyanddist)
               and ga.salestransactionseq = r.genericsequence1
               and ga.PAGENUMBER = 0
               /*and rownum = 1*/)
     where exists (select 1
              from sh_query_result sh
             where sh.genericSequence1 = ga.salestransactionSeq
               and sh.genericnumber1 in
                   (Gv_Setnumberordist, Gv_Setnumberoragyanddist)
               and ga.PAGENUMBER = 0
               and sh.Component = v_componentValue
               and sh.periodSeq = gv_periodseq);

    Comdebugger('SQL Performance',
                'Stagehook[SP_OVERRIDING_ASSIGNMENT]-SQL5 END:' || current_date ||
                ' - ReusltNumber' || ::ROWCOUNT);
log('SP_TXA_OVERRIDING_ASSIGNMENT 6 '||::ROWCOUNT);
    commit;

  end ;