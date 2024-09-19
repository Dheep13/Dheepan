CREATE Procedure Comcleanassignment(in I_Periodseq bigInt) 
sql security invoker
as
  Begin
    DECLARE gv_calendarSeq BIGINT; /* package/session variable */
    DECLARE cdt_EndOfTime date:= to_date('2200-01-01','yyyy-mm-dd');
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */
    DECLARE gv_hryc BIGINT; /* package/session variable */

    DECLARE V_Periodstartdate TIMESTAMP;  /* ORIGSQL: V_Periodstartdate Date; */
    DECLARE v_periodenddate TIMESTAMP;  /* ORIGSQL: v_periodenddate date; */
    declare v_tenantid varchar(20);
   

    SELECT CAST(SESSION_CONTEXT('GV_CALENDARSEQ') AS BIGINT) INTO gv_calendarSeq FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
    SELECT CAST(SESSION_CONTEXT('GV_HRYC') AS BIGINT) INTO gv_hryc FROM SYS.DUMMY ;
   
    select tenantid into v_tenantid from cs_tenant;
   
   /* end of package/session variables */ 

	  
    Select Startdate, Enddate
      Into V_Periodstartdate, V_Periodenddate
      From Cs_Period
     Where Periodseq = I_Periodseq
       and calendarseq = gv_calendarseq
       and removedate = cdt_endoftime;

    Log('71');


update  Cs_Transactionassignment Ta
set setnumber=genericnumber6
where genericnumber6 is not null and genericnumber6<>setnumber
and tenantid=v_tenantid
and processingUnitseq = gv_ProcessingUnitSeq
and Compensationdate >= V_Periodstartdate
and Compensationdate < V_Periodenddate
--version 13 add by sammi
and not exists (select 1 from cs_salestransaction st
            where st.tenantid=v_tenantid
              and st.processingUnitseq = gv_ProcessingUnitSeq
              and st.Compensationdate >= V_Periodstartdate
              and st.Compensationdate < V_Periodenddate
              and st.EVENTTYPESEQ IN (select DATATYPESEQ from cs_eventtype where eventtypeid in ('FYC_INTRODUCER','OFYC_INTRODUCER') and removedate=cdt_EndOfTime)
              and st.salestransactionseq =ta.salestransactionseq
              )
--version 13 end
;

    Log('71 Update Setnumber '||::ROWCOUNT);
    commit;

    Delete From Cs_Transactionassignment Ta
     Where tenantid=v_tenantid
       and processingUnitseq=Gv_Processingunitseq
       and Compensationdate >= V_Periodstartdate
       And Compensationdate < V_Periodenddate
       And Setnumber > 2
       And Setnumber not in (41, 51)
       And (Genericattribute4 Is Not Null)
       And Exists
     (Select 1
              From Cs_Salestransaction St
             Where St.tenantid=v_tenantid  and St.Processingunitseq = Gv_Processingunitseq
               And St.Salestransactionseq = Ta.Salestransactionseq
               And St.Compensationdate >= V_Periodstartdate
               And st.Compensationdate < V_Periodenddate)
       And Not Exists
     (Select 1
              From Cs_Salestransaction
             Where tenantid=v_tenantid and Salestransactionseq = Ta.Salestransactionseq
             --version 13 add by sammi
             --And Eventtypeseq = gv_hryc
               And (Eventtypeseq = gv_hryc
                    OR
                    EVENTTYPESEQ IN (select DATATYPESEQ from cs_eventtype where eventtypeid in ('FYC_INTRODUCER','OFYC_INTRODUCER'
                    ,'FYC_TP','RYC_TP' --version 18
		    		,'FYC_TPGI','RYC_TPGI','FYP_TPGI','RYP_TPGI'-- version 22 For MAS Section86 project
                    ) and removedate=cdt_EndOfTime)
                    )
             --version 13 end
               and processingUnitseq = gv_ProcessingUnitSeq
               and Compensationdate >= V_Periodstartdate
               And Compensationdate < V_Periodenddate);
    Log('71 Deletion '||::ROWCOUNT);

    commit;

  end --comCleanAssignment;